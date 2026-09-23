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
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

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

// TestStackOutputs_ReadsDeployedStack is the direct regression guard for the
// public StackOutputs wrapper, which is retained for the optional CloudFormation
// path (reading the outputs of an already-CFN-deployed stack). Since the #154
// cutover `Deploy` no longer creates a stack, so the fixture is built with the
// retained createOrUpdate — the reason that function is kept.
func TestStackOutputs_ReadsDeployedStack(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()
	createLegacyStack(t, d, "lagotto-outputs")

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

// legacyStackVersion is the artifact version the legacy-stack fixture points at.
const legacyStackVersion = "0.44.0"

// createLegacyStack builds a genuinely CloudFormation-created stack via the
// retained createOrUpdate — the only offline way to produce one now that Deploy
// doesn't. This is the fixture the migration tests need: you cannot test detaching
// a stack without a stack.
func createLegacyStack(t *testing.T, d *Deployer, stackName string) {
	t.Helper()
	ctx := context.Background()
	bucket := DefaultBucketName("123456789012", "us-east-1")
	key := LambdaObjectKey(legacyStackVersion)
	if err := d.ensureBucket(ctx, bucket, "us-east-1"); err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	if _, err := d.uploadArtifact(ctx, bucket, key, legacyStackVersion); err != nil {
		t.Fatalf("uploadArtifact: %v", err)
	}
	params := []cfntypes.Parameter{
		{ParameterKey: aws.String("Environment"), ParameterValue: aws.String("production")},
		{ParameterKey: aws.String("LambdaCodeBucket"), ParameterValue: aws.String(bucket)},
		{ParameterKey: aws.String("LambdaCodeKey"), ParameterValue: aws.String(key)},
	}
	if err := d.createOrUpdate(ctx, stackName, params, deployCapabilities); err != nil {
		t.Fatalf("createOrUpdate (legacy stack fixture): %v", err)
	}
}

// substrateDeployOptions is the Options a substrate Deploy uses.
func substrateDeployOptions() Options {
	return Options{
		StackName: "lagotto", Region: "us-east-1", Version: "0.55.1",
		AccountID: "123456789012", Environment: "production",
	}
}

// TestSubstrate_Deploy_CreatesAllThreeResources is the SDK path end to end: bucket,
// artifact upload (stubbed httpGet, no network), topic, function, schedule — and
// the output keys the CLI prints.
func TestSubstrate_Deploy_CreatesAllThreeResources(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	res, err := d.Deploy(ctx, substrateDeployOptions())
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	// The exact key set the stack used to export. Callers and humans read these.
	want := []string{
		"CapacityAlertsTopicArn", "CapacityPollerFunctionArn", "SchedulerInvokeRoleArn",
		"WatchesTableName", "MatchHistoryTableName", "ScheduledTableName",
	}
	for _, k := range want {
		if res.Outputs[k] == "" {
			t.Errorf("outputs missing %q (got %v)", k, res.Outputs)
		}
	}
	if len(res.Outputs) != len(want) {
		t.Errorf("outputs has %d keys, want exactly %d (%v)", len(res.Outputs), len(want), res.Outputs)
	}
	if len(res.Actions) != 3 {
		t.Errorf("Actions = %v, want one line per resource", res.Actions)
	}

	// All three resources are really there.
	if _, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)}); err != nil {
		t.Errorf("GetFunction after Deploy: %v", err)
	}
	if _, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)}); err != nil {
		t.Errorf("GetSchedule after Deploy: %v", err)
	}
	if _, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(res.Outputs["CapacityAlertsTopicArn"]),
	}); err != nil {
		t.Errorf("GetTopicAttributes after Deploy: %v", err)
	}

	// SNS_TOPIC_ARN must be the ARN the topic call actually returned.
	fn, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)})
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if got := fn.Configuration.Environment.Variables["SNS_TOPIC_ARN"]; got != res.Outputs["CapacityAlertsTopicArn"] {
		t.Errorf("SNS_TOPIC_ARN = %q, want the reported topic ARN %q", got, res.Outputs["CapacityAlertsTopicArn"])
	}
}

// TestSubstrate_Deploy_IsIdempotent: a second Deploy converges rather than
// failing. This is the structural death of the whole ROLLBACK_COMPLETE class of
// bug (#59/#143) — there is no stack to strand, so a re-run is just three more
// idempotent Ensure calls.
func TestSubstrate_Deploy_IsIdempotent(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()
	opts := substrateDeployOptions()

	first, err := d.Deploy(ctx, opts)
	if err != nil {
		t.Fatalf("first Deploy: %v", err)
	}
	second, err := d.Deploy(ctx, opts)
	if err != nil {
		t.Fatalf("second Deploy: %v", err)
	}
	for k, v := range first.Outputs {
		if second.Outputs[k] != v {
			t.Errorf("output %s changed between runs: %q then %q", k, v, second.Outputs[k])
		}
	}
}

// TestSubstrate_Deploy_PreservesEnabledSchedule is the #154 central guarantee at
// the Deploy level (EnsurePollerSchedule has its own narrower version):
//
//	lagotto deploy  → schedule created DISABLED
//	lagotto watch   → enablePollingSchedule flips it to ENABLED
//	lagotto deploy  → MUST NOT turn the running poller back off
func TestSubstrate_Deploy_PreservesEnabledSchedule(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()
	opts := substrateDeployOptions()

	if _, err := d.Deploy(ctx, opts); err != nil {
		t.Fatalf("first Deploy: %v", err)
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

	if _, err := d.Deploy(ctx, opts); err != nil {
		t.Fatalf("second Deploy: %v", err)
	}

	after, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)})
	if err != nil {
		t.Fatalf("GetSchedule after redeploy: %v", err)
	}
	if after.State != schedulertypes.ScheduleStateEnabled {
		t.Fatalf("State = %q after a redeploy, want ENABLED — `lagotto deploy` must never be able to turn off a running poller", after.State)
	}
}

// TestSubstrate_Teardown_RemovesAllThreeAndIsRepeatable covers the cutover from
// DeleteStack to explicit deletes: all three go, and a second Teardown is a no-op
// success rather than a pile of NotFound errors.
func TestSubstrate_Teardown_RemovesAllThreeAndIsRepeatable(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	res, err := d.Deploy(ctx, substrateDeployOptions())
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	deleted, err := d.Teardown(ctx, "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if len(deleted) != 3 {
		t.Errorf("Teardown deleted %v, want all three resources", deleted)
	}

	if _, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)}); err == nil {
		t.Error("the poller Lambda is still there after Teardown")
	}
	if _, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)}); err == nil {
		t.Error("the poller schedule is still there after Teardown")
	}
	if _, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(res.Outputs["CapacityAlertsTopicArn"]),
	}); err == nil {
		t.Error("the alerts topic is still there after Teardown")
	}

	again, err := d.Teardown(ctx, "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("second Teardown: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("second Teardown reported deleting %v; there was nothing left to delete", again)
	}
}

// TestSubstrate_MigrateFromStack_DetachesWithoutDeletingResources is the whole
// point of migrate.go: an account with a real CloudFormation stack ends up with no
// stack and a fully intact poller.
//
// What this DOES cover: the whole five-step sequence against a real wire protocol
// — DescribeStacks, the retain UpdateStack, GetTemplate(Original) read back and
// gated (substrate stores the body we send and returns it, so the gate genuinely
// has to pass), DeleteStack + waiter, and the post-delete verification.
//
// What it does NOT cover, and the reason the safety gate's own test uses fakes:
// substrate's CloudFormation emulator does not MATERIALIZE template resources at
// all (a stack create leaves no Lambda behind), so its DeleteStack cannot model
// DeletionPolicy: Retain either. The resources survive here because the SDK path
// created them, not because CloudFormation was told to retain them. Retain
// semantics are only provable against real AWS; the offline guarantees are the
// overlay's shape invariant (TestRetainOverlay_AddsExactlySixLines) and the gate's
// refusal to delete (TestMigrateFromStack_RefusesToDeleteWithoutTheRetainGate).
func TestSubstrate_MigrateFromStack_DetachesWithoutDeletingResources(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	createLegacyStack(t, d, "lagotto")

	// The SDK path adopts what the stack created (fixed names), which is the state
	// a user is in after upgrading and re-running deploy.
	if _, err := d.Deploy(ctx, substrateDeployOptions()); err != nil {
		t.Fatalf("Deploy (adopting the stack's resources): %v", err)
	}

	if err := d.MigrateFromStack(ctx, "lagotto"); err != nil {
		t.Fatalf("MigrateFromStack: %v", err)
	}

	// (1) the stack is gone.
	exists, _, err := d.stackState(ctx, "lagotto")
	if err != nil {
		t.Fatalf("stackState after migrate: %v", err)
	}
	if exists {
		t.Error("the CloudFormation stack still exists after MigrateFromStack")
	}
	// (2) all three resources survived. MigrateFromStack verifies this itself, so a
	// failure above would already have errored — assert independently anyway, since
	// "the verification is the feature" is exactly the kind of thing that rots.
	if _, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)}); err != nil {
		t.Errorf("the poller Lambda did not survive the migration: %v", err)
	}
	if _, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(PollerScheduleName)}); err != nil {
		t.Errorf("the poller schedule did not survive the migration: %v", err)
	}
	if _, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: aws.String(AlertsTopicARN("us-east-1", "123456789012")),
	}); err != nil {
		t.Errorf("the alerts topic did not survive the migration: %v", err)
	}
}

// TestSubstrate_LegacyStackState covers the automatic detection that drives the
// warning: present-and-live before the migration, absent after.
func TestSubstrate_LegacyStackState(t *testing.T) {
	d := substrateDeployer(t)
	ctx := context.Background()

	if exists, _, err := d.LegacyStackState(ctx, "lagotto"); err != nil || exists {
		t.Fatalf("LegacyStackState with no stack = (%v, %v), want (false, nil)", exists, err)
	}

	createLegacyStack(t, d, "lagotto")
	exists, status, err := d.LegacyStackState(ctx, "lagotto")
	if err != nil {
		t.Fatalf("LegacyStackState: %v", err)
	}
	if !exists {
		t.Errorf("LegacyStackState reports no stack, but one was just created (status %q)", status)
	}
}

// --- Deploy / Teardown wiring, against the in-package fakes -----------------
//
// These use Substrate only for S3 (d.s3 is a concrete *s3.Client, not a seam, so
// ensureBucket/uploadArtifact need a real endpoint) and the recording fakes for
// Lambda/SNS/Scheduler, where exact arguments are the thing being asserted.
// Substrate v0.109.0 drops a schedule's Target.RoleArn on the wire, so the role
// wiring in particular can ONLY be asserted here.
func hybridDeployer(t *testing.T) (*Deployer, *fakeLambda, *fakeSNS, *fakeScheduler) {
	t.Helper()
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)
	d.sleep = func(time.Duration) {}
	// A deliberately WRONG-LOOKING topic ARN: if the function's SNS_TOPIC_ARN ends
	// up matching this, it can only have come from the topic call's return value
	// rather than from a separately constructed guess.
	sn := &fakeSNS{arn: "arn:aws:sns:us-east-1:123456789012:lagotto-capacity-alerts-RETURNED-BY-SNS"}
	l := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}}
	sc := &fakeScheduler{getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("Schedule not found")}}
	d.sns, d.lambda, d.sched = sn, l, sc
	return d, l, sn, sc
}

// TestDeploy_WiresTheTopicARNAndScheduleTarget asserts the three things a silent
// mis-wiring would break without any error being raised: the output key set, the
// function's SNS_TOPIC_ARN, and what the schedule targets.
func TestDeploy_WiresTheTopicARNAndScheduleTarget(t *testing.T) {
	d, l, sn, sc := hybridDeployer(t)

	res, err := d.Deploy(context.Background(), Options{
		Region: "us-east-1", AccountID: "123456789012", Version: "0.55.1", Environment: "production",
	})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	// The output key set is a compatibility surface: it's what the CLI prints and
	// what the stack used to export.
	wantKeys := map[string]string{
		"CapacityAlertsTopicArn":    sn.arn,
		"CapacityPollerFunctionArn": PollerFunctionARN("us-east-1", "123456789012"),
		"SchedulerInvokeRoleArn":    schedulerInvokeRoleARN("123456789012"),
		"WatchesTableName":          DefaultWatchesTable,
		"MatchHistoryTableName":     DefaultHistoryTable,
		"ScheduledTableName":        DefaultScheduledTable,
	}
	if len(res.Outputs) != len(wantKeys) {
		t.Errorf("outputs = %v, want exactly the %d stack-compatible keys", res.Outputs, len(wantKeys))
	}
	for k, want := range wantKeys {
		if res.Outputs[k] != want {
			t.Errorf("output %s = %q, want %q", k, res.Outputs[k], want)
		}
	}

	// SNS_TOPIC_ARN is the ARN EnsureAlertsTopic actually returned.
	if len(l.createCalls) != 1 {
		t.Fatalf("CreateFunction called %d times, want 1", len(l.createCalls))
	}
	if got := l.createCalls[0].Environment.Variables["SNS_TOPIC_ARN"]; got != sn.arn {
		t.Errorf("SNS_TOPIC_ARN = %q, want the ARN EnsureAlertsTopic returned (%q)", got, sn.arn)
	}
	// Empty table options fall back to the same names the template defaulted to.
	for k, want := range map[string]string{
		"WATCHES_TABLE": DefaultWatchesTable, "HISTORY_TABLE": DefaultHistoryTable,
		"SCHEDULED_TABLE": DefaultScheduledTable,
	} {
		if got := l.createCalls[0].Environment.Variables[k]; got != want {
			t.Errorf("env %s = %q, want the template's default %q", k, got, want)
		}
	}
	if got := aws.ToString(l.createCalls[0].Role); got != runtimeRoleARN("123456789012") {
		t.Errorf("function Role = %q, want the runtimeiam-owned role", got)
	}

	// The schedule targets the derived function ARN and the CLI-owned invoke role.
	if len(sc.createCalls) != 1 {
		t.Fatalf("CreateSchedule called %d times, want 1", len(sc.createCalls))
	}
	target := sc.createCalls[0].Target
	if target == nil {
		t.Fatal("the schedule was created with no target")
	}
	if got := aws.ToString(target.Arn); got != PollerFunctionARN("us-east-1", "123456789012") {
		t.Errorf("schedule target = %q, want the poller function ARN %q", got, PollerFunctionARN("us-east-1", "123456789012"))
	}
	if got := aws.ToString(target.RoleArn); got != schedulerInvokeRoleARN("123456789012") {
		t.Errorf("schedule role = %q, want the Scheduler invoke role %q", got, schedulerInvokeRoleARN("123456789012"))
	}
	if sc.createCalls[0].State != schedulertypes.ScheduleStateDisabled {
		t.Errorf("schedule created in state %q, want DISABLED (nothing is watching yet)", sc.createCalls[0].State)
	}
}

// TestDeploy_RequiresAccountAndRegion: every poller name and ARN is derived from
// them, so a missing one has to fail up front rather than produce a function whose
// role ARN is "arn:aws:iam:::role/…".
func TestDeploy_RequiresAccountAndRegion(t *testing.T) {
	d, _, _, _ := hybridDeployer(t)
	if _, err := d.Deploy(context.Background(), Options{Region: "us-east-1", Version: "0.55.1"}); err == nil {
		t.Error("Deploy accepted an empty AccountID")
	}
	if _, err := d.Deploy(context.Background(), Options{AccountID: "123456789012", Version: "0.55.1"}); err == nil {
		t.Error("Deploy accepted an empty Region")
	}
}

// TestTeardown_DeletesInTheRightOrder: schedule → function → topic, so nothing can
// fire into a half-deleted poller.
func TestTeardown_DeletesInTheRightOrder(t *testing.T) {
	log := &callLog{}
	l := &fakeLambda{log: log, getOut: &lambda.GetFunctionOutput{
		Configuration: &lambdatypes.FunctionConfiguration{
			FunctionArn: aws.String(PollerFunctionARN("us-east-1", "123456789012")),
			State:       lambdatypes.StateActive,
		},
	}}
	sn := &fakeSNS{log: log, arn: AlertsTopicARN("us-east-1", "123456789012"),
		attributes: map[string]string{"DisplayName": "Lagotto Capacity Alerts"}}
	sc := &fakeScheduler{log: log, getOut: &scheduler.GetScheduleOutput{Name: aws.String(PollerScheduleName)}}
	d, _ := testDeployer(l, sn, sc)

	deleted, err := d.Teardown(context.Background(), "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	want := []string{"DeleteSchedule", "DeleteFunction", "DeleteTopic"}
	if len(log.calls) != len(want) {
		t.Fatalf("delete calls = %v, want %v", log.calls, want)
	}
	for i, w := range want {
		if log.calls[i] != w {
			t.Errorf("delete call %d = %s, want %s (order: %v, want %v)", i, log.calls[i], w, log.calls, want)
		}
	}
	if len(deleted) != 3 {
		t.Errorf("Teardown reported %v, want all three resources", deleted)
	}
}

// TestTeardown_AbsentEverythingIsSuccess: a teardown against an account where the
// poller was never deployed (or has already been torn down) must be a no-op
// success, not a pile of NotFound errors.
func TestTeardown_AbsentEverythingIsSuccess(t *testing.T) {
	log := &callLog{}
	l := &fakeLambda{log: log, getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}}
	sn := &fakeSNS{log: log, getErr: &snstypes.NotFoundException{Message: aws.String("Topic not found")}}
	sc := &fakeScheduler{log: log, getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("Schedule not found")}}
	d, _ := testDeployer(l, sn, sc)

	deleted, err := d.Teardown(context.Background(), "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("Teardown with nothing deployed: %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("Teardown reported deleting %v, but nothing existed", deleted)
	}
	if len(log.calls) != 0 {
		t.Errorf("Teardown issued delete calls %v for resources that don't exist", log.calls)
	}
}

// TestTeardown_ProbeErrorIsNotSilentlyTreatedAsAbsence: an AccessDenied on the
// existence probe must fail the teardown. Reporting "nothing to delete" there
// would tell the user their poller is gone when it is still running and billing.
func TestTeardown_ProbeErrorIsNotSilentlyTreatedAsAbsence(t *testing.T) {
	log := &callLog{}
	sc := &fakeScheduler{log: log, getErr: errors.New("AccessDeniedException: not authorized to perform scheduler:GetSchedule")}
	d, _ := testDeployer(&fakeLambda{log: log}, &fakeSNS{log: log}, sc)

	if _, err := d.Teardown(context.Background(), "us-east-1", "123456789012"); err == nil {
		t.Error("Teardown swallowed an AccessDenied on the schedule probe")
	}
	if len(log.calls) != 0 {
		t.Errorf("Teardown kept deleting (%v) after a probe failed", log.calls)
	}
}

// TestTeardown_RequiresRegionAndAccount: the topic ARN is derived from them.
func TestTeardown_RequiresRegionAndAccount(t *testing.T) {
	d, _ := testDeployer(&fakeLambda{}, &fakeSNS{}, &fakeScheduler{})
	if _, err := d.Teardown(context.Background(), "us-east-1", ""); err == nil {
		t.Error("Teardown accepted an empty account ID")
	}
	if _, err := d.Teardown(context.Background(), "", "123456789012"); err == nil {
		t.Error("Teardown accepted an empty region")
	}
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
		Tags:       PollerTags("production", "0.44.0"),
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
		Tags:       PollerTags("production", "0.44.0"),
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
		Tags:    PollerTags("production", "0.44.0"),
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
