package watcher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
)

// SageMakerLauncher submits the user's SageMaker job and tracks it across poll
// cycles. lagotto's model is "the launch is the capacity test": submit the job,
// and if SageMaker can't provision the requested ml.* capacity it fails with a
// CapacityError, which the poller treats as "retry next cycle" (bounded by TTL).
//
// SageMaker capacity failure is asynchronous — CreateTrainingJob returns
// immediately and the CapacityError only appears later via DescribeTrainingJob —
// so the flow spans two poll cycles, tracked by Watch.SageMakerJobName:
//
//	no job in flight  -> submit, store the job name, report "pending" (retry)
//	job in flight     -> describe:
//	                       Failed + CapacityError  -> clear name, retry
//	                       Failed + other reason   -> terminal
//	                       InProgress/Completed/... -> success (instances launched)
type SageMakerLauncher struct {
	cfg aws.Config
}

// NewSageMakerLauncher creates a launcher from a base AWS config; per-watch
// region clients are derived in Launch.
func NewSageMakerLauncher(cfg aws.Config) *SageMakerLauncher {
	return &SageMakerLauncher{cfg: cfg}
}

// sageMakerOutcome is returned by Launch so the poller can drive the watch's
// status without SageMaker-specific knowledge. Kind reuses FailureKind:
//   - FailureNone     -> success (m updated with the job ARN)
//   - FailureCapacity -> no capacity yet / job still pending; keep watch active
//   - FailureTerminal -> stop the watch as failed
type sageMakerOutcome struct {
	Kind FailureKind
	// JobName is the in-flight job name to persist on the watch (empty means
	// "clear it" — submit a fresh job next cycle).
	JobName string
	// ClearJobName signals the stored job name should be removed (a failed job
	// that should be re-submitted next cycle).
	ClearJobName bool
}

// Launch runs one step of the submit/track flow for a SageMaker watch.
func (l *SageMakerLauncher) Launch(ctx context.Context, w *Watch, m *MatchResult) (sageMakerOutcome, error) {
	region := firstRegion(w.Regions)
	cfg := l.cfg.Copy()
	if region != "" {
		cfg.Region = region
		m.Region = region
	}
	client := sagemaker.NewFromConfig(cfg)

	if w.SageMakerJobName == "" {
		return l.submit(ctx, client, w, m)
	}
	return l.check(ctx, client, w, m)
}

// submit creates the training job from the user's stored spec and records the
// generated job name. A successful create just means "submitted" — capacity is
// not yet known — so we report FailureCapacity to keep the watch active and
// check the job next cycle. A synchronous error (quota/validation) is terminal.
func (l *SageMakerLauncher) submit(ctx context.Context, client *sagemaker.Client, w *Watch, m *MatchResult) (sageMakerOutcome, error) {
	if len(w.SageMakerJobJSON) == 0 {
		// A SageMaker watch with no job spec can't be acted on — e.g. an old
		// proxy-era watch, or a notify-only watch routed here by mistake.
		return sageMakerOutcome{Kind: FailureTerminal}, fmt.Errorf("watch %s has no SageMaker job config", w.WatchID)
	}

	var input sagemaker.CreateTrainingJobInput
	if err := json.Unmarshal(w.SageMakerJobJSON, &input); err != nil {
		return sageMakerOutcome{Kind: FailureTerminal}, fmt.Errorf("unmarshal sagemaker job: %w", err)
	}

	// Generate a unique, traceable job name per attempt (SageMaker job names must
	// be unique within an account+region; reusing one would collide).
	jobName := fmt.Sprintf("lagotto-%s-%d", w.WatchID, w.MatchCount+1)
	input.TrainingJobName = aws.String(jobName)
	addLagottoTag(&input, w.WatchID)

	out, err := client.CreateTrainingJob(ctx, &input)
	if err != nil {
		// Synchronous errors are quota (ResourceLimitExceeded) or validation —
		// retrying won't help. Capacity is never reported synchronously.
		m.ActionTaken = "sagemaker_submit_failed"
		return sageMakerOutcome{Kind: FailureTerminal}, fmt.Errorf("create training job: %w", err)
	}

	if out.TrainingJobArn != nil {
		m.InstanceID = *out.TrainingJobArn // reuse InstanceID to carry the job ARN
	}
	m.InstanceType = w.InstanceTypePattern
	m.ActionTaken = "sagemaker_submitted"
	// Submitted — capacity outcome unknown yet. Stay active; check next cycle.
	return sageMakerOutcome{Kind: FailureCapacity, JobName: jobName}, nil
}

// check describes the in-flight job and maps its status to an outcome.
func (l *SageMakerLauncher) check(ctx context.Context, client *sagemaker.Client, w *Watch, m *MatchResult) (sageMakerOutcome, error) {
	out, err := client.DescribeTrainingJob(ctx, &sagemaker.DescribeTrainingJobInput{
		TrainingJobName: aws.String(w.SageMakerJobName),
	})
	if err != nil {
		// Can't read the job's state — treat as transient and retry; the watch
		// TTL bounds the loop.
		return sageMakerOutcome{Kind: FailureCapacity, JobName: w.SageMakerJobName}, fmt.Errorf("describe training job: %w", err)
	}

	status := string(out.TrainingJobStatus)
	failureReason := aws.ToString(out.FailureReason)
	if out.TrainingJobArn != nil {
		m.InstanceID = *out.TrainingJobArn
	}
	m.InstanceType = w.InstanceTypePattern

	switch ClassifySageMakerFailure(status, failureReason) {
	case FailureCapacity:
		// Job failed for lack of capacity — drop it and resubmit next cycle.
		m.ActionTaken = "sagemaker_capacity_unavailable"
		return sageMakerOutcome{Kind: FailureCapacity, ClearJobName: true}, nil
	case FailureTerminal:
		m.ActionTaken = "sagemaker_failed"
		return sageMakerOutcome{Kind: FailureTerminal}, fmt.Errorf("sagemaker job %s failed: %s", w.SageMakerJobName, failureReason)
	default:
		// InProgress/Completed/etc — instances were provisioned, capacity exists.
		m.ActionTaken = "sagemaker_launched"
		return sageMakerOutcome{Kind: FailureNone, JobName: w.SageMakerJobName}, nil
	}
}

// addLagottoTag stamps the job with the watch ID so lagotto-submitted probe/run
// jobs are filterable (job records are immutable and can't be deleted).
func addLagottoTag(input *sagemaker.CreateTrainingJobInput, watchID string) {
	input.Tags = append(input.Tags, smTag("lagotto:watch-id", watchID), smTag("lagotto:managed", "true"))
}

func smTag(key, value string) smtypes.Tag {
	return smtypes.Tag{Key: aws.String(key), Value: aws.String(value)}
}

func firstRegion(regions []string) string {
	if len(regions) > 0 {
		return regions[0]
	}
	return ""
}
