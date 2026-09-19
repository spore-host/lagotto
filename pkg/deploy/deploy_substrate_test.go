package deploy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// fakeZipBody is a stand-in for the poller Lambda zip bytes; its content
// never matters to these tests since Substrate does not execute the Lambda,
// only stores/deploys it.
const fakeZipBody = "fake-lambda-zip-bytes"

// fakeHTTPGet returns a stub httpGet that always answers with fakeZipBody at
// 200 OK, so uploadArtifact's download step succeeds without a network call.
func fakeHTTPGet(*testing.T) func(string) (*http.Response, error) {
	return func(string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(fakeZipBody))),
		}, nil
	}
}

// TestStackState_NoSuchStack verifies the "stack does not exist" branch of
// stackState, which createOrUpdate relies on to decide CreateStack vs
// UpdateStack (#1: pkg/deploy had 0% coverage before this file).
func TestStackState_NoSuchStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)

	exists, status, err := d.stackState(context.Background(), "no-such-stack")
	if err != nil {
		t.Fatalf("stackState: %v", err)
	}
	if exists {
		t.Errorf("exists = true for a stack never created, want false (status %v)", status)
	}
}

// TestDeploy_CreatesStackAndReturnsOutputs exercises Deploy end to end against
// Substrate: bucket creation, artifact upload (via a stubbed httpGet, so no
// real network/GitHub-release fetch), and CreateStack — then asserts the
// SAM-transformed stack's outputs come back, since callers (cmd/deploy.go,
// cmd/launch.go's scheduled-launch wiring) depend on those keys existing.
func TestDeploy_CreatesStackAndReturnsOutputs(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	outs, err := d.Deploy(context.Background(), Options{
		StackName: "lagotto-test", Region: "us-east-1", Version: "0.44.0",
		AccountID: "123456789012",
	})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	for _, key := range []string{
		"CapacityPollerFunctionArn", "SchedulerInvokeRoleArn",
		"WatchesTableName", "MatchHistoryTableName", "ScheduledTableName",
	} {
		if outs[key] == "" {
			t.Errorf("stack outputs missing %q (got %v)", key, outs)
		}
	}
}

// TestDeploy_RedeployIsAnUpdateNotAFailure verifies the createOrUpdate branch
// where the stack already exists in a healthy state: a second Deploy call
// must go through UpdateStack (not error out, not recreate) and still return
// outputs — this is the ordinary "run `lagotto deploy` again" path.
func TestDeploy_RedeployIsAnUpdateNotAFailure(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	opts := Options{StackName: "lagotto-redeploy", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012"}

	if _, err := d.Deploy(ctx, opts); err != nil {
		t.Fatalf("first Deploy: %v", err)
	}
	outs, err := d.Deploy(ctx, opts)
	if err != nil {
		t.Fatalf("second Deploy (update path): %v", err)
	}
	if outs["CapacityPollerFunctionArn"] == "" {
		t.Error("outputs missing CapacityPollerFunctionArn after redeploy")
	}
}

// TestTeardown_DeletesStack verifies Teardown removes a deployed stack and
// waits for completion — after it returns, stackState must report the stack
// gone, so a subsequent Deploy takes the CreateStack path rather than trying
// (and failing) an UpdateStack against nothing.
func TestTeardown_DeletesStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if _, err := d.Deploy(ctx, Options{
		StackName: "lagotto-teardown", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012",
	}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	if err := d.Teardown(ctx, "lagotto-teardown"); err != nil {
		t.Fatalf("Teardown: %v", err)
	}

	exists, _, err := d.stackState(ctx, "lagotto-teardown")
	if err != nil {
		t.Fatalf("stackState after teardown: %v", err)
	}
	if exists {
		t.Error("stack still exists after Teardown")
	}
}

// TestStackOutputs_ReadsDeployedStack is the direct regression guard for the
// public StackOutputs wrapper (used by cmd/launch.go to find the poller/
// scheduler ARNs for a scheduled launch) — distinct from Deploy's own return
// value, since a caller may query outputs for a stack deployed in a PRIOR
// process/invocation.
func TestStackOutputs_ReadsDeployedStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if _, err := d.Deploy(ctx, Options{
		StackName: "lagotto-outputs", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012",
	}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	outs, err := d.StackOutputs(ctx, "lagotto-outputs")
	if err != nil {
		t.Fatalf("StackOutputs: %v", err)
	}
	if outs["WatchesTableName"] == "" {
		t.Errorf("StackOutputs missing WatchesTableName (got %v)", outs)
	}
}

// TestStackOutputs_ErrorsForUndeployedStack: querying outputs for a stack that
// was never deployed must error, not return an empty map silently — a caller
// (e.g. a scheduled launch trying to wire an EventBridge target) needs to
// know deployment hasn't happened rather than proceed with nothing.
func TestStackOutputs_ErrorsForUndeployedStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)

	if _, err := d.StackOutputs(context.Background(), "never-deployed"); err == nil {
		t.Error("expected an error querying outputs for an undeployed stack")
	}
}

// TestEnsureBucket_IdempotentWhenAlreadyExists verifies the HeadBucket-first
// short-circuit: calling Deploy (which calls ensureBucket) twice for the same
// bucket must not error the second time around, since the bucket persists
// across the two calls and CreateBucket on an already-owned bucket would
// otherwise be a spurious failure on every redeploy.
func TestEnsureBucket_IdempotentWhenAlreadyExists(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if err := d.ensureBucket(ctx, "lagotto-lambda-123456789012-us-east-1", "us-east-1"); err != nil {
		t.Fatalf("first ensureBucket: %v", err)
	}
	// Second call against the SAME bucket must be a no-op success, not a
	// BucketAlreadyOwnedByYou failure.
	if err := d.ensureBucket(ctx, "lagotto-lambda-123456789012-us-east-1", "us-east-1"); err != nil {
		t.Fatalf("second ensureBucket (already exists): %v", err)
	}
}

// TestUploadArtifact_HTTPErrorPropagates verifies a network-level httpGet
// failure (as opposed to a non-200 response) surfaces as an error rather
// than a nil-body panic or a silently-empty upload.
func TestUploadArtifact_HTTPErrorPropagates(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = func(string) (*http.Response, error) {
		return nil, errors.New("connection refused")
	}

	_, err := d.uploadArtifact(context.Background(), "some-bucket", "some-key", "0.44.0")
	if err == nil {
		t.Fatal("expected an error when httpGet fails")
	}
}

// TestUploadArtifact_Non200StatusPropagates verifies a non-OK download (e.g.
// the requested version was never released, a 404) is reported with the
// status code and version — not treated as a successful upload of whatever
// GitHub's error page body happened to contain.
func TestUploadArtifact_Non200StatusPropagates(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = func(string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	}

	_, err := d.uploadArtifact(context.Background(), "some-bucket", "some-key", "0.44.0")
	if err == nil {
		t.Fatal("expected an error for a 404 download")
	}
}

// --- SDK-native Ensure* path (#154) -----------------------------------------
//
// Substrate v0.109.0 emulates every Lambda / SNS / EventBridge-Scheduler
// operation these functions call, so the three Ensures can be exercised end to
// end against a real wire protocol (not just in-package fakes) with zero real
// AWS and zero launches.

// substrateDeployer returns a Deployer against a fresh Substrate server, with the
// release download stubbed and sleeps instant.
func substrateDeployer(t *testing.T) *Deployer {
	t.Helper()
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)
	d.sleep = func(time.Duration) {}
	d.sns = snsTagQuirk{d.sns}
	d.lambda = lambdaTagQuirk{d.lambda}
	return d
}

// lambdaTagQuirk works around a second Substrate emulator bug, again not a
// lagotto one: substrate's parseLambdaOperation only strips the `/2015-03-31`
// API-date prefix, but Lambda serializes its three tag operations to
// `/2017-03-31/tags/{Resource}`, so TagResource is unroutable and answers
// UnknownOperationException even though the handler exists and works. Filed as
// scttfrdmn/substrate#1142.
//
// Scoped to that one message on that one operation; any other Lambda tag failure
// still surfaces, and production code has no such tolerance. The tag call's
// arguments are asserted exactly by TestEnsurePollerFunction_* against the
// in-package fake.
type lambdaTagQuirk struct{ lambdaAPI }

func (l lambdaTagQuirk) TagResource(ctx context.Context, in *lambda.TagResourceInput, optFns ...func(*lambda.Options)) (*lambda.TagResourceOutput, error) {
	out, err := l.lambdaAPI.TagResource(ctx, in, optFns...)
	if err != nil && strings.Contains(err.Error(), "UnknownOperationException") {
		return &lambda.TagResourceOutput{}, nil
	}
	return out, err
}

// snsTagQuirk works around a Substrate emulator bug, NOT a lagotto one:
// substrate's SNS TagResource answers 200 with a body that omits the
// `<TagResourceResult/>` element the SNS query protocol requires, so the real
// aws-sdk-go-v2 deserializer rejects an otherwise-successful call with
// "TagResourceResult node not found". The tag IS applied server-side. Filed as
// scttfrdmn/substrate#1141.
//
// Scoped as narrowly as possible: it swallows ONLY that one deserialization
// message, on TagResource, in tests. A genuine tag failure (AccessDenied,
// NotFound) still surfaces, and production code has no such tolerance.
type snsTagQuirk struct{ snsAPI }

func (s snsTagQuirk) TagResource(ctx context.Context, in *sns.TagResourceInput, optFns ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	out, err := s.snsAPI.TagResource(ctx, in, optFns...)
	if err != nil && strings.Contains(err.Error(), "TagResourceResult node not found") {
		return &sns.TagResourceOutput{}, nil
	}
	return out, err
}

// TestSubstrate_EnsureAlertsTopic asserts the topic is created with the
// attributes the template declared, and that they are observable afterwards.
func TestSubstrate_EnsureAlertsTopic(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	arn, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	if !strings.HasSuffix(arn, ":"+AlertsTopicName) {
		t.Errorf("topic ARN %q does not name %q", arn, AlertsTopicName)
	}

	got, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
	if err != nil {
		t.Fatalf("GetTopicAttributes: %v", err)
	}
	if got.Attributes["DisplayName"] != "Lagotto Capacity Alerts" {
		t.Errorf("DisplayName = %q", got.Attributes["DisplayName"])
	}
	if got.Attributes["KmsMasterKeyId"] != "alias/aws/sns" {
		t.Errorf("KmsMasterKeyId = %q, want alias/aws/sns", got.Attributes["KmsMasterKeyId"])
	}
}

// TestSubstrate_EnsureAlertsTopic_IsIdempotent: a second run returns the SAME ARN
// (CreateTopic is idempotent by name) and, since the attributes now already
// match, writes nothing.
func TestSubstrate_EnsureAlertsTopic_IsIdempotent(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	first, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("first EnsureAlertsTopic: %v", err)
	}
	second, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("second EnsureAlertsTopic: %v", err)
	}
	if first != second {
		t.Errorf("ARN changed between runs: %q then %q", first, second)
	}
}

// TestSubstrate_EnsurePollerFunction covers the create path end to end and
// asserts the function's env wiring — specifically that SNS_TOPIC_ARN is the ARN
// of the topic that was ACTUALLY created, not a separately constructed guess. A
// mismatch here is exactly the silent degradation (#154): the poller comes up
// fine and simply never notifies.
func TestSubstrate_EnsurePollerFunction(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	topicARN, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}

	bucket := DefaultBucketName("123456789012", "us-east-1")
	key := LambdaObjectKey("0.55.1")
	if err := d.ensureBucket(ctx, bucket, "us-east-1"); err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	sha, err := d.uploadArtifact(ctx, bucket, key, "0.55.1")
	if err != nil {
		t.Fatalf("uploadArtifact: %v", err)
	}
	if sha == "" {
		t.Error("uploadArtifact returned no code digest")
	}

	arn, action, err := d.EnsurePollerFunction(ctx, PollerFunctionInput{
		RoleARN:    runtimeRoleARN("123456789012"),
		Bucket:     bucket,
		Key:        key,
		CodeSHA256: sha,
		EnvVars:    PollerEnvVars("us-east-1", "123456789012", "", "", "", topicARN),
		Tags:       PollerTags("production"),
	})
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if action != ActionCreated {
		t.Errorf("action = %q, want %q", action, ActionCreated)
	}
	if !strings.HasSuffix(arn, ":function:"+PollerFunctionName) {
		t.Errorf("function ARN %q does not name %q", arn, PollerFunctionName)
	}

	out, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	cfg := out.Configuration
	if string(cfg.Runtime) != "provided.al2023" {
		t.Errorf("Runtime = %q", cfg.Runtime)
	}
	if len(cfg.Architectures) != 1 || cfg.Architectures[0] != lambdatypes.ArchitectureArm64 {
		t.Errorf("Architectures = %v, want [arm64]", cfg.Architectures)
	}
	if aws.ToString(cfg.Handler) != "bootstrap" {
		t.Errorf("Handler = %q", aws.ToString(cfg.Handler))
	}
	if aws.ToInt32(cfg.Timeout) != 900 || aws.ToInt32(cfg.MemorySize) != 512 {
		t.Errorf("Timeout/MemorySize = %d/%d, want 900/512", aws.ToInt32(cfg.Timeout), aws.ToInt32(cfg.MemorySize))
	}
	if aws.ToString(cfg.Role) != runtimeRoleARN("123456789012") {
		t.Errorf("Role = %q", aws.ToString(cfg.Role))
	}
	if cfg.Environment == nil {
		t.Fatal("function has no Environment")
	}
	if got := cfg.Environment.Variables["SNS_TOPIC_ARN"]; got != topicARN {
		t.Errorf("SNS_TOPIC_ARN = %q, want the ARN of the topic actually created (%q)", got, topicARN)
	}
	for _, k := range []string{"WATCHES_TABLE", "HISTORY_TABLE", "SCHEDULED_TABLE", "SCHEDULE_NAME",
		"POLLER_FUNCTION_ARN", "SCHEDULER_INVOKE_ROLE_ARN"} {
		if cfg.Environment.Variables[k] == "" {
			t.Errorf("env var %s is empty — a missing var silently degrades the poller", k)
		}
	}
}

// TestSubstrate_EnsurePollerFunction_IsIdempotent: a second run must succeed and
// leave the function converged.
//
// NOTE: it deliberately does NOT assert action == "unchanged". Substrate reports
// an S3-sourced package's CodeSha256 as the object's ETag rather than the
// base64 SHA-256 real Lambda reports, so the digest comparison never matches
// here and an UpdateFunctionCode is issued. That is precisely the caveat
// uploadArtifact documents: the digest is an OPTIMIZATION, and a convention
// mismatch costs a redundant (but still correct) code push — never correctness.
// The configuration comparison, which is the expensive one, does converge: zero
// UpdateFunctionConfiguration on the second run.
func TestSubstrate_EnsurePollerFunction_IsIdempotent(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	topicARN, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	bucket := DefaultBucketName("123456789012", "us-east-1")
	key := LambdaObjectKey("0.55.1")
	if err := d.ensureBucket(ctx, bucket, "us-east-1"); err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	sha, err := d.uploadArtifact(ctx, bucket, key, "0.55.1")
	if err != nil {
		t.Fatalf("uploadArtifact: %v", err)
	}
	in := PollerFunctionInput{
		RoleARN:    runtimeRoleARN("123456789012"),
		Bucket:     bucket,
		Key:        key,
		CodeSHA256: sha,
		EnvVars:    PollerEnvVars("us-east-1", "123456789012", "", "", "", topicARN),
		Tags:       PollerTags("production"),
	}
	if _, _, err := d.EnsurePollerFunction(ctx, in); err != nil {
		t.Fatalf("first EnsurePollerFunction: %v", err)
	}
	_, action, err := d.EnsurePollerFunction(ctx, in)
	if err != nil {
		t.Fatalf("second EnsurePollerFunction: %v", err)
	}
	if action == ActionConfigUpdated {
		t.Errorf("action = %q on a re-run: the configuration should already be converged", action)
	}
	out, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if out.Configuration.Environment.Variables["SNS_TOPIC_ARN"] != topicARN {
		t.Error("the re-run disturbed the env wiring")
	}
}

// TestSubstrate_EnsurePollerSchedule covers create + idempotence.
func TestSubstrate_EnsurePollerSchedule(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()
	fnARN := PollerFunctionARN("us-east-1", "123456789012")
	roleARN := schedulerInvokeRoleARN("123456789012")

	action, err := d.EnsurePollerSchedule(ctx, fnARN, roleARN)
	if err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if action != ActionCreated {
		t.Errorf("action = %q, want %q", action, ActionCreated)
	}

	got, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if got.State != schedulertypes.ScheduleStateDisabled {
		t.Errorf("State = %q, want DISABLED on create (nothing is watching yet)", got.State)
	}
	if aws.ToString(got.ScheduleExpression) != "rate(5 minutes)" {
		t.Errorf("ScheduleExpression = %q", aws.ToString(got.ScheduleExpression))
	}
	if got.FlexibleTimeWindow == nil || got.FlexibleTimeWindow.Mode != schedulertypes.FlexibleTimeWindowModeOff {
		t.Errorf("FlexibleTimeWindow = %+v, want Mode OFF", got.FlexibleTimeWindow)
	}
	if got.Target == nil || aws.ToString(got.Target.Arn) != fnARN {
		t.Errorf("Target = %+v, want arn %q", got.Target, fnARN)
	}
	// Target.RoleArn is deliberately NOT asserted here. Substrate v0.109.0 decodes
	// CreateSchedule's target into a struct whose JSON tag is `role_arn`, which
	// does not case-insensitively match the wire key `RoleArn`, so it silently
	// drops the invoke role (fixed upstream in a later substrate; lagotto still
	// pins v0.109.0). The role wiring IS asserted, exactly, by
	// TestEnsurePollerSchedule_* against the in-package fake. For the same reason
	// a re-run here can never report "unchanged" — scheduleDiffers correctly sees
	// the missing RoleArn as drift — so zero-churn convergence is asserted against
	// the fake too, and only re-runnability is checked here.
	if _, err := d.EnsurePollerSchedule(ctx, fnARN, roleARN); err != nil {
		t.Fatalf("second EnsurePollerSchedule: %v", err)
	}
}

// TestSubstrate_EnsurePollerSchedule_PreservesEnabledState is the end-to-end form
// of the #154 central guarantee, reproducing the real sequence:
//
//	lagotto deploy  → schedule created DISABLED
//	lagotto watch   → enablePollingSchedule flips it to ENABLED
//	lagotto deploy  → MUST NOT turn the running poller back off
func TestSubstrate_EnsurePollerSchedule_PreservesEnabledState(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()
	fnARN := PollerFunctionARN("us-east-1", "123456789012")
	roleARN := schedulerInvokeRoleARN("123456789012")

	if _, err := d.EnsurePollerSchedule(ctx, fnARN, roleARN); err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}

	// Exactly what cmd/watch.go's enablePollingSchedule does, out of band.
	cur, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if _, err := d.sched.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
		Name:               cur.Name,
		ScheduleExpression: cur.ScheduleExpression,
		FlexibleTimeWindow: cur.FlexibleTimeWindow,
		Target:             cur.Target,
		State:              schedulertypes.ScheduleStateEnabled,
	}); err != nil {
		t.Fatalf("enabling the schedule: %v", err)
	}

	// Now re-deploy.
	if _, err := d.EnsurePollerSchedule(ctx, fnARN, roleARN); err != nil {
		t.Fatalf("re-deploy EnsurePollerSchedule: %v", err)
	}

	after, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)})
	if err != nil {
		t.Fatalf("GetSchedule after redeploy: %v", err)
	}
	if after.State != schedulertypes.ScheduleStateEnabled {
		t.Fatalf("State = %q after a redeploy, want ENABLED — `lagotto deploy` must never be able to turn off a running poller", after.State)
	}

	// And again after a redeploy that DOES have to converge something.
	if _, err := d.EnsurePollerSchedule(ctx, "arn:aws:lambda:us-east-1:123456789012:function:other", roleARN); err != nil {
		t.Fatalf("converging EnsurePollerSchedule: %v", err)
	}
	after, err = d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)})
	if err != nil {
		t.Fatalf("GetSchedule after converging redeploy: %v", err)
	}
	if after.State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("State = %q after a converging redeploy, want ENABLED preserved", after.State)
	}
}

// TestSubstrate_DeleteHelpersTolerateAbsence exercises the three delete helpers
// (both against live resources and against nothing), so PR 3's Teardown cutover
// is pure wiring — and so they aren't dead code in the meantime.
func TestSubstrate_DeleteHelpersTolerateAbsence(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	// Nothing deployed yet: every delete is a no-op success.
	if err := d.deleteAlertsTopic(ctx, "us-east-1", "123456789012"); err != nil {
		t.Errorf("deleteAlertsTopic with no topic: %v", err)
	}
	if err := d.deletePollerFunction(ctx); err != nil {
		t.Errorf("deletePollerFunction with no function: %v", err)
	}
	if err := d.deletePollerSchedule(ctx); err != nil {
		t.Errorf("deletePollerSchedule with no schedule: %v", err)
	}

	// Now stand the resources up and delete them for real.
	topicARN, err := d.EnsureAlertsTopic(ctx, "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	bucket := DefaultBucketName("123456789012", "us-east-1")
	key := LambdaObjectKey("0.55.1")
	if err := d.ensureBucket(ctx, bucket, "us-east-1"); err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	sha, err := d.uploadArtifact(ctx, bucket, key, "0.55.1")
	if err != nil {
		t.Fatalf("uploadArtifact: %v", err)
	}
	fnARN, _, err := d.EnsurePollerFunction(ctx, PollerFunctionInput{
		RoleARN: runtimeRoleARN("123456789012"), Bucket: bucket, Key: key, CodeSHA256: sha,
		EnvVars: PollerEnvVars("us-east-1", "123456789012", "", "", "", topicARN),
		Tags:    PollerTags("production"),
	})
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if _, err := d.EnsurePollerSchedule(ctx, fnARN, schedulerInvokeRoleARN("123456789012")); err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}

	if err := d.deletePollerSchedule(ctx); err != nil {
		t.Errorf("deletePollerSchedule: %v", err)
	}
	if err := d.deletePollerFunction(ctx); err != nil {
		t.Errorf("deletePollerFunction: %v", err)
	}
	if err := d.deleteAlertsTopic(ctx, "us-east-1", "123456789012"); err != nil {
		t.Errorf("deleteAlertsTopic: %v", err)
	}

	// Re-running each delete must still be a no-op success (Teardown is re-runnable).
	if err := d.deletePollerSchedule(ctx); err != nil {
		t.Errorf("second deletePollerSchedule: %v", err)
	}
	if err := d.deletePollerFunction(ctx); err != nil {
		t.Errorf("second deletePollerFunction: %v", err)
	}
	if err := d.deleteAlertsTopic(ctx, "us-east-1", "123456789012"); err != nil {
		t.Errorf("second deleteAlertsTopic: %v", err)
	}
}
