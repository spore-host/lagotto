// Package deploy stands up (and tears down) the hosted lagotto capacity poller in
// the caller's own AWS account (#48): it fetches the published poller Lambda zip
// (#29), uploads it to a bucket in the user's account, and then provisions the
// three poller resources — the SNS alerts topic, the poller Lambda and the
// EventBridge Scheduler schedule — with direct SDK calls. This is the "arm a
// watch, walk away, it's genuinely server-side" path that the laptop-bound
// `poll --daemon` can't be.
//
// # Why not CloudFormation (#154)
//
// It used to deploy the embedded CloudFormation/SAM template. Every deploy outage
// lagotto has had was CloudFormation-specific — a Function↔Role↔Schedule circular
// dependency (#67/#68), an AlreadyExists collision between `watch` and `deploy`
// (#59), a missing-capability refusal that wedged the stack in ROLLBACK_COMPLETE
// (#143), and a post-transform Early Validation failure that had to be bisected
// with no-execute change sets (#145). Two of the four were fixed by taking
// resources OUT of CloudFormation; by the end the stack held only three.
//
// Direct SDK calls make #143 and #145 structurally impossible, remove the
// ROLLBACK_COMPLETE wedge, surface each error at the call that failed, and make a
// `--version` bump a fast code-only update.
//
// The template is KEPT as an optional declarative artifact for IaC/enterprise
// consumers (deployment/cloudformation/lagotto-stack.yaml, documented in
// DEPLOYMENT.md), and everything needed to read, migrate away from, and — in
// tests — create a real stack stays in this package: StackOutputs, stackState,
// createOrUpdate, and MigrateFromStack (migrate.go).
package deploy

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

const (
	// lambdaAsset is the release asset name attached by GoReleaser (#29).
	lambdaAsset = "capacity-poller_lambda_linux_arm64.zip"
	releaseRepo = "spore-host/lagotto"
)

// Options configure a Deploy.
type Options struct {
	// StackName is NOT what deploy creates any more (#154). Every poller resource
	// has a fixed name; this is only the name of a LEGACY CloudFormation stack, used
	// to detect one that still exists (so the user can be warned) and as the target
	// of MigrateFromStack.
	StackName   string
	Region      string
	Version     string // release version to pull the Lambda zip from, e.g. "0.44.0" (no leading v)
	Environment string // SAM Environment param (default "production")
	Bucket      string // S3 bucket for the Lambda zip; empty → derive lagotto-lambda-<account>-<region>
	AccountID   string // caller account (for the derived bucket name + messaging)
	// The CLI-owned DynamoDB table names the poller is wired to (#59). deploy
	// never creates them; empty falls back to the standard names, which are also
	// the CFN template's parameter defaults (lagotto-watches / -match-history /
	// -scheduled-launches) so the two paths agree.
	WatchesTable   string
	HistoryTable   string
	ScheduledTable string
}

// Result is what a Deploy reports back.
//
// Outputs keeps the EXACT key names the CloudFormation stack used to export, so
// the block `lagotto deploy` prints is byte-identical to what people already
// recognize from the stack-based releases (and so anything scripted against those
// key names keeps working):
//
//	CapacityAlertsTopicArn, CapacityPollerFunctionArn, SchedulerInvokeRoleArn,
//	WatchesTableName, MatchHistoryTableName, ScheduledTableName
//
// Actions is one human-readable line per resource — "created", "code updated",
// "unchanged" — which is the thing a stack event log used to tell you and an
// idempotent Ensure* path otherwise wouldn't.
type Result struct {
	Outputs map[string]string
	Actions []string
}

// LambdaArtifactURL returns the GitHub Release download URL for the poller Lambda
// zip at the given version. Exposed (and pure) so it's unit-testable without network.
func LambdaArtifactURL(version string) string {
	v := strings.TrimPrefix(version, "v")
	return fmt.Sprintf("https://github.com/%s/releases/download/v%s/%s", releaseRepo, v, lambdaAsset)
}

// DefaultBucketName derives the per-account artifact bucket name when the caller
// doesn't supply one. Account + region keep it unique and discoverable.
func DefaultBucketName(accountID, region string) string {
	return fmt.Sprintf("lagotto-lambda-%s-%s", accountID, region)
}

// LambdaObjectKey is the S3 key the Lambda zip is uploaded under for a version.
func LambdaObjectKey(version string) string {
	return fmt.Sprintf("lagotto/capacity-poller-v%s.zip", strings.TrimPrefix(version, "v"))
}

// deployCapabilities are the CloudFormation capabilities the lagotto stack
// requires. Only CAPABILITY_AUTO_EXPAND (for the AWS::Serverless transform) is
// needed: the stack no longer defines any IAM resources — the poller execution
// role and the Scheduler invoke role are CLI-owned (created by pkg/runtimeiam,
// like the DynamoDB tables) and only referenced by ARN, so no CAPABILITY_IAM /
// CAPABILITY_NAMED_IAM is required and there is no named-IAM create collision
// under Early Validation (#143 added NAMED_IAM; #145 removed the IAM resources
// entirely).
var deployCapabilities = []cfntypes.Capability{
	cfntypes.CapabilityCapabilityAutoExpand,
}

// Deployer performs deploy/teardown against AWS. Every AWS dependency is held
// behind a narrow interface (see infra.go) and the two time/network-touching
// operations are function fields, so unit tests can drive the whole thing
// offline: httpGet stubs the release download, and sleep makes retry/poll loops
// instant.
type Deployer struct {
	cfn     cfnAPI
	s3      *s3.Client
	lambda  lambdaAPI
	sns     snsAPI
	sched   schedulerAPI
	httpGet func(url string) (*http.Response, error)
	sleep   func(time.Duration)
}

// New builds a Deployer from an AWS config.
func New(cfg aws.Config) *Deployer {
	return &Deployer{
		cfn:     cloudformation.NewFromConfig(cfg),
		s3:      s3.NewFromConfig(cfg),
		lambda:  lambda.NewFromConfig(cfg),
		sns:     sns.NewFromConfig(cfg),
		sched:   scheduler.NewFromConfig(cfg),
		httpGet: http.Get,
		sleep:   time.Sleep,
	}
}

// Deploy provisions the hosted poller with direct SDK calls (#154) and returns the
// resolved outputs plus what each step actually did.
//
// The order is load-bearing:
//
//  1. ensureBucket + uploadArtifact — the code has to be in S3 before anything can
//     point at it. uploadArtifact hands back the zip's base64 SHA-256 so a re-run
//     with unchanged code can skip the code push.
//  2. EnsureAlertsTopic — its ARN is an INPUT to the function's SNS_TOPIC_ARN, so
//     the topic must exist first. Using the ARN the topic call actually returned
//     (rather than a separately constructed guess) is what keeps a mismatch from
//     silently degrading the poller into a non-notifying one.
//  3. EnsurePollerFunction — converged onto the shape the SAM template declared.
//  4. EnsurePollerSchedule — targets the function and the CLI-owned Scheduler
//     invoke role. Created DISABLED; an existing schedule's state is passed
//     through untouched, so a deploy can never switch off a running poller.
//
// The DynamoDB tables and the two IAM roles are NOT touched here: they are ensured
// by the caller (cmd/deploy.go, via watcher.Store.EnsureTables and
// runtimeiam.EnsureRoles) before Deploy is called, exactly as before the cutover.
//
// Legacy-stack detection is deliberately NOT part of Deploy — see
// LegacyStackState: the caller runs it AFTER a successful deploy so a detection
// failure can never bury the real error.
func (d *Deployer) Deploy(ctx context.Context, opts Options) (*Result, error) {
	if opts.AccountID == "" {
		return nil, fmt.Errorf("deploy: AccountID is required (the poller's role and ARNs are derived from it)")
	}
	if opts.Region == "" {
		return nil, fmt.Errorf("deploy: Region is required (the poller's ARNs are derived from it)")
	}
	bucket := opts.Bucket
	if bucket == "" {
		bucket = DefaultBucketName(opts.AccountID, opts.Region)
	}
	key := LambdaObjectKey(opts.Version)

	if err := d.ensureBucket(ctx, bucket, opts.Region); err != nil {
		return nil, err
	}
	digest, err := d.uploadArtifact(ctx, bucket, key, opts.Version)
	if err != nil {
		return nil, err
	}

	env := opts.Environment
	if env == "" {
		env = "production"
	}

	topicARN, err := d.EnsureAlertsTopic(ctx, env)
	if err != nil {
		return nil, err
	}

	fnARN, fnAction, err := d.EnsurePollerFunction(ctx, PollerFunctionInput{
		RoleARN:    runtimeRoleARN(opts.AccountID),
		Bucket:     bucket,
		Key:        key,
		CodeSHA256: digest,
		EnvVars: PollerEnvVars(opts.Region, opts.AccountID,
			opts.WatchesTable, opts.HistoryTable, opts.ScheduledTable, topicARN),
		Tags: PollerTags(env, opts.Version),
	})
	if err != nil {
		return nil, err
	}
	// Fall back to the constructed ARN if the endpoint didn't report one. The two
	// are the same string — the name is fixed — so this only guards against an
	// empty Target.Arn on the schedule below, which would be an unhelpful failure.
	if fnARN == "" {
		fnARN = PollerFunctionARN(opts.Region, opts.AccountID)
	}

	schedRoleARN := schedulerInvokeRoleARN(opts.AccountID)
	schedAction, err := d.EnsurePollerSchedule(ctx, fnARN, schedRoleARN)
	if err != nil {
		return nil, err
	}

	return &Result{
		Outputs: map[string]string{
			"CapacityAlertsTopicArn":    topicARN,
			"CapacityPollerFunctionArn": fnARN,
			"SchedulerInvokeRoleArn":    schedRoleARN,
			"WatchesTableName":          orDefault(opts.WatchesTable, DefaultWatchesTable),
			"MatchHistoryTableName":     orDefault(opts.HistoryTable, DefaultHistoryTable),
			"ScheduledTableName":        orDefault(opts.ScheduledTable, DefaultScheduledTable),
		},
		Actions: []string{
			fmt.Sprintf("SNS topic %s: ensured", AlertsTopicName),
			fmt.Sprintf("Lambda %s: %s", PollerFunctionName, fnAction),
			fmt.Sprintf("schedule %s: %s", PollerScheduleName, schedAction),
		},
	}, nil
}

// Teardown deletes the three poller resources explicitly, in the order
// schedule → function → topic, and returns a description of each one it actually
// removed.
//
// The order matters: the schedule goes FIRST so nothing can fire into a
// half-deleted poller. Every step tolerates an already-absent resource, so a
// Teardown is idempotent — running it twice, or after a partial failure, succeeds
// and simply reports fewer deletions.
//
// Deliberately NOT deleted (each is stated in the CLI's confirmation prompt):
//
//   - The three DynamoDB tables. They hold watches and match history; `lagotto
//     teardown` owns those.
//   - Both IAM roles. runtimeiam owns them since #146 and the Scheduler invoke
//     role is shared with the #49 per-launch schedules, so deleting it here would
//     break a pending `lagotto launch --at`.
//   - The artifact bucket. ensureBucket creates it OUTSIDE the stack, so even the
//     old DeleteStack never removed it; changing that silently would be a
//     surprising data deletion.
//   - Per-launch lagotto-launch-sl-* schedules, same as before the cutover.
func (d *Deployer) Teardown(ctx context.Context, region, accountID string) ([]string, error) {
	if region == "" || accountID == "" {
		return nil, fmt.Errorf("teardown: region and accountID are required (the topic ARN is derived from them)")
	}
	var deleted []string

	// 1. Schedule first: an enabled schedule firing into a deleted function just
	// produces invocation errors, and a schedule outliving its target is worse than
	// a target outliving its schedule.
	if exists, err := d.pollerScheduleExists(ctx); err != nil {
		return deleted, err
	} else if exists {
		if err := d.deletePollerSchedule(ctx); err != nil {
			return deleted, err
		}
		deleted = append(deleted, "schedule "+PollerScheduleName)
	}

	// 2. The function.
	if exists, err := d.pollerFunctionExists(ctx); err != nil {
		return deleted, err
	} else if exists {
		if err := d.deletePollerFunction(ctx); err != nil {
			return deleted, err
		}
		deleted = append(deleted, "function "+PollerFunctionName)
	}

	// 3. The topic last: it's the only resource whose deletion loses something
	// (subscriptions), so it goes after the things that publish to it are gone.
	if exists, err := d.alertsTopicExists(ctx, region, accountID); err != nil {
		return deleted, err
	} else if exists {
		if err := d.deleteAlertsTopic(ctx, region, accountID); err != nil {
			return deleted, err
		}
		deleted = append(deleted, "SNS topic "+AlertsTopicName)
	}

	return deleted, nil
}

// orDefault returns v, or d when v is empty.
func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func (d *Deployer) ensureBucket(ctx context.Context, bucket, region string) error {
	_, err := d.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil // already exists (and we can reach it)
	}
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	// us-east-1 must NOT set a LocationConstraint; every other region must.
	if region != "" && region != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	if _, err := d.s3.CreateBucket(ctx, in); err != nil {
		return fmt.Errorf("create artifact bucket %s: %w", bucket, err)
	}
	return nil
}

// uploadArtifact downloads the published poller zip and puts it in the artifact
// bucket, returning the base64-std-encoded SHA-256 of the bytes — the same
// convention Lambda reports in FunctionConfiguration.CodeSha256 — so
// EnsurePollerFunction can skip an UpdateFunctionCode that would be a no-op.
//
// Treat that hash strictly as an OPTIMIZATION, never as correctness: we hash the
// bytes we already hold in memory, so it costs nothing, but if the convention
// ever fails to line up with what a given endpoint reports (substrate, for
// instance, reports an S3-sourced package's ETag instead), the only consequence
// is an unconditional UpdateFunctionCode — which is still completely correct,
// just not free. Never gate correctness on this value matching.
func (d *Deployer) uploadArtifact(ctx context.Context, bucket, key, version string) (string, error) {
	url := LambdaArtifactURL(version)
	resp, err := d.httpGet(url)
	if err != nil {
		return "", fmt.Errorf("download poller Lambda %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download poller Lambda %s: HTTP %d (is v%s released?)", url, resp.StatusCode, strings.TrimPrefix(version, "v"))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read poller Lambda zip: %w", err)
	}
	if _, err := d.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(body)),
	}); err != nil {
		return "", fmt.Errorf("upload poller Lambda to s3://%s/%s: %w", bucket, key, err)
	}
	sum := sha256.Sum256(body)
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

// failedCreateStates are terminal states a stack can be left in by a failed
// CreateStack. Such a stack can never be updated — it must be deleted and
// recreated. `lagotto deploy` does this automatically so a prior failure (e.g.
// the #59 AlreadyExists rollback) doesn't wedge every subsequent retry.
var failedCreateStates = map[cfntypes.StackStatus]bool{
	cfntypes.StackStatusRollbackComplete: true,
	cfntypes.StackStatusRollbackFailed:   true,
	cfntypes.StackStatusReviewInProgress: true,
	cfntypes.StackStatusCreateFailed:     true,
	cfntypes.StackStatusDeleteFailed:     true,
}

// createOrUpdate drives the CloudFormation path. KEEP IT: it has no production
// caller since the #154 cutover, but it is the only offline way to produce a
// genuinely CFN-created stack — i.e. the fixture the adopt-an-existing-stack
// migration test needs. Do not delete it as "dead code".
func (d *Deployer) createOrUpdate(ctx context.Context, stackName string, params []cfntypes.Parameter, caps []cfntypes.Capability) error {
	exists, status, err := d.stackState(ctx, stackName)
	if err != nil {
		return err
	}
	// A stack stranded in a failed-create state can't be updated; delete it first
	// so the CreateStack below starts clean (#59).
	if exists && failedCreateStates[status] {
		if err := d.deleteStack(ctx, stackName); err != nil {
			return fmt.Errorf("delete prior failed stack %s (status %s) before redeploy: %w", stackName, status, err)
		}
		exists = false
	}
	if exists {
		_, err := d.cfn.UpdateStack(ctx, &cloudformation.UpdateStackInput{
			StackName:    aws.String(stackName),
			TemplateBody: aws.String(cfn.StackTemplate),
			Parameters:   params,
			Capabilities: caps,
		})
		if err != nil {
			// "No updates are to be performed" is a benign no-op, not a failure.
			if strings.Contains(err.Error(), "No updates are to be performed") {
				return nil
			}
			return fmt.Errorf("update stack %s: %w", stackName, err)
		}
		w := cloudformation.NewStackUpdateCompleteWaiter(d.cfn)
		if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
			return fmt.Errorf("waiting for stack %s update: %w", stackName, err)
		}
		return nil
	}
	_, err = d.cfn.CreateStack(ctx, &cloudformation.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfn.StackTemplate),
		Parameters:   params,
		Capabilities: caps,
	})
	if err != nil {
		return fmt.Errorf("create stack %s: %w", stackName, err)
	}
	w := cloudformation.NewStackCreateCompleteWaiter(d.cfn)
	if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
		return fmt.Errorf("waiting for stack %s creation: %w", stackName, err)
	}
	return nil
}

// deleteStack removes a CloudFormation stack and waits for completion. The only
// remaining CFN-deleting paths are createOrUpdate's failed-create recovery and
// MigrateFromStack's final detach step; `lagotto deploy --teardown` no longer
// touches CloudFormation at all.
func (d *Deployer) deleteStack(ctx context.Context, stackName string) error {
	if _, err := d.cfn.DeleteStack(ctx, &cloudformation.DeleteStackInput{
		StackName: aws.String(stackName),
	}); err != nil {
		return fmt.Errorf("delete stack %s: %w", stackName, err)
	}
	w := cloudformation.NewStackDeleteCompleteWaiter(d.cfn)
	if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
		return fmt.Errorf("waiting for stack %s deletion: %w", stackName, err)
	}
	return nil
}

// stackState reports whether the stack exists and, if so, its current status.
// A failed-create status (see failedCreateStates) tells createOrUpdate to delete
// and recreate rather than attempt an impossible UpdateStack (#59).
func (d *Deployer) stackState(ctx context.Context, stackName string) (bool, cfntypes.StackStatus, error) {
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)})
	if err != nil {
		// CloudFormation returns a ValidationError "does not exist" for an absent stack.
		if strings.Contains(err.Error(), "does not exist") {
			return false, "", nil
		}
		return false, "", fmt.Errorf("describe stack %s: %w", stackName, err)
	}
	for _, s := range out.Stacks {
		if aws.ToString(s.StackName) == stackName {
			return true, s.StackStatus, nil
		}
	}
	return false, "", nil
}

// StackOutputs returns the deployed stack's outputs (e.g. the poller function
// ARN and scheduler-invoke-role ARN that `lagotto launch` needs to wire a
// per-launch EventBridge schedule, #49). Errors if the stack isn't deployed.
func (d *Deployer) StackOutputs(ctx context.Context, stackName string) (map[string]string, error) {
	return d.stackOutputs(ctx, stackName)
}

func (d *Deployer) stackOutputs(ctx context.Context, stackName string) (map[string]string, error) {
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)})
	if err != nil {
		return nil, fmt.Errorf("describe stack %s: %w", stackName, err)
	}
	res := map[string]string{}
	for _, s := range out.Stacks {
		for _, o := range s.Outputs {
			res[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
		}
	}
	return res, nil
}
