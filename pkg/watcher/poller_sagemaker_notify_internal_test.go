package watcher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// storedSageMakerNotifyWatch is a notify-only SageMaker watch as one created
// before #195 sits in DynamoDB: active, inside its TTL, with notify channels
// and even a job spec (the old `watch` didn't require one for notify, but a
// user could pass it). Nothing may ever submit that job.
func storedSageMakerNotifyWatch(t *testing.T) *Watch {
	t.Helper()
	spec, err := json.Marshal(map[string]interface{}{
		"RoleArn":                "arn:aws:iam::123456789012:role/SageMakerExecution",
		"AlgorithmSpecification": map[string]interface{}{"TrainingImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/probe:latest", "TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]interface{}{"S3OutputPath": "s3://bucket/out"},
		"ResourceConfig":         map[string]interface{}{"InstanceType": "ml.g5.2xlarge", "InstanceCount": 1, "VolumeSizeInGB": 1},
		"StoppingCondition":      map[string]interface{}{"MaxRuntimeInSeconds": 600},
	})
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	now := time.Now().UTC()
	return &Watch{
		WatchID:             "w-sm-notify",
		UserID:              "arn:alice",
		Status:              StatusActive,
		Service:             ServiceSageMaker,
		Action:              ActionNotify,
		InstanceTypePattern: "ml.g5.2xlarge",
		Regions:             []string{"us-east-1"},
		NotifyChannels:      []NotifyChannel{{Type: "email", Target: "alice@example.com"}},
		SageMakerJobJSON:    spec,
		CreatedAt:           now.Add(-time.Hour),
		UpdatedAt:           now.Add(-time.Hour),
		ExpiresAt:           now.Add(23 * time.Hour),
		TTLTimestamp:        now.Add(23 * time.Hour).Unix(),
	}
}

// TestPollAll_StoredSageMakerNotifyNotifiesOnce is the #195 poller gate (both
// the CLI daemon and the hosted Lambda run this Poller). A notify-only
// SageMaker watch used to be marked polled every cycle and never notify anyone
// until its TTL. Now its first poll tells the user exactly once why it can
// never fire and what works, retires it as failed, and submits nothing; a
// second sweep neither re-notifies nor submits.
func TestPollAll_StoredSageMakerNotifyNotifiesOnce(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, "smn-watches")
	env.CreateHistoryTable(t, "smn-history")
	store := NewStore(env.AWSConfig, "smn-watches", "smn-history")
	ctx := context.Background()

	w := storedSageMakerNotifyWatch(t)
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	type call struct {
		id     string
		status WatchStatus
		reason string
	}
	var calls []call
	p := NewPoller(nil, store, false, PollerOpts{SageMaker: NewSageMakerLauncher(env.AWSConfig)})
	p.notifyUnsupported = func(_ context.Context, got *Watch, reason string) error {
		calls = append(calls, call{got.WatchID, got.Status, reason})
		return nil
	}

	for cycle := 1; cycle <= 2; cycle++ {
		summary, err := p.PollAll(ctx)
		if err != nil {
			t.Fatalf("PollAll cycle %d: %v", cycle, err)
		}
		if len(summary.Matches) != 0 {
			t.Errorf("cycle %d: %d matches, want 0 (nothing was found or submitted)", cycle, len(summary.Matches))
		}
		if cycle == 1 && (summary.Failed != 1 || summary.Retrying != 0) {
			t.Errorf("cycle 1: Failed=%d Retrying=%d, want 1/0 (retired, not left ticking)", summary.Failed, summary.Retrying)
		}
	}

	if len(calls) != 1 {
		t.Fatalf("notifier called %d times over two sweeps, want exactly 1: %+v", len(calls), calls)
	}
	c := calls[0]
	if c.id != w.WatchID {
		t.Errorf("notified watch %q, want %q", c.id, w.WatchID)
	}
	if c.status != StatusFailed {
		t.Errorf("notified with status %q, want failed (the post-transition view)", c.status)
	}
	if !strings.Contains(c.reason, "--action spawn") {
		t.Errorf("notice does not name the mode that works: %q", c.reason)
	}

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil || got == nil {
		t.Fatalf("GetWatch: %v (%v)", got, err)
	}
	if got.Status != StatusFailed {
		t.Errorf("stored status = %q, want failed", got.Status)
	}
	if got.SageMakerJobName != "" {
		t.Errorf("a notify-only watch submitted SageMaker job %q", got.SageMakerJobName)
	}
}

// TestValidateServiceAction: creation refuses every SageMaker action except
// spawn, and the notify refusal names the mode that works instead of
// recommending another one that can't (#195). EC2 accepts all three.
func TestValidateServiceAction(t *testing.T) {
	for _, a := range []ActionMode{ActionNotify, ActionSpawn, ActionHold} {
		if err := ValidateServiceAction(ServiceEC2, a); err != nil {
			t.Errorf("ec2 + %s rejected: %v", a, err)
		}
		if err := ValidateServiceAction("", a); err != nil {
			t.Errorf("default service + %s rejected: %v", a, err)
		}
	}
	if err := ValidateServiceAction(ServiceSageMaker, ActionSpawn); err != nil {
		t.Errorf("sagemaker + spawn rejected: %v", err)
	}
	if err := ValidateServiceAction(ServiceSageMaker, ActionHold); err == nil {
		t.Error("sagemaker + hold accepted")
	}
	err := ValidateServiceAction(ServiceSageMaker, ActionNotify)
	if err == nil {
		t.Fatal("sagemaker + notify accepted; it can never fire")
	}
	if !strings.Contains(err.Error(), "--action spawn") || strings.Contains(err.Error(), "use --action notify") {
		t.Errorf("refusal should point at --action spawn, not notify: %v", err)
	}
}
