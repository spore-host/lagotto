package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

const testAccount = "123456789012"
const testRegion = "us-east-1"

func testPollerInput() PollerFunctionInput {
	return PollerFunctionInput{
		RoleARN:    runtimeRoleARN(testAccount),
		Bucket:     "lagotto-lambda-123456789012-us-east-1",
		Key:        LambdaObjectKey("0.55.1"),
		CodeSHA256: "sha-of-the-zip",
		EnvVars:    PollerEnvVars(testRegion, testAccount, "", "", "", AlertsTopicARN(testRegion, testAccount)),
		Tags:       PollerTags("production"),
	}
}

// liveConfig returns a FunctionConfiguration matching the desired shape for the
// given input — the "steady state" a converged deploy should leave behind.
func liveConfig(in PollerFunctionInput) *lambdatypes.FunctionConfiguration {
	return &lambdatypes.FunctionConfiguration{
		FunctionArn: strptr(PollerFunctionARN(testRegion, testAccount)),
		Role:        strptr(in.RoleARN),
		Handler:     strptr(pollerHandler),
		Runtime:     lambdatypes.Runtime(pollerRuntime),
		Timeout:     aws.Int32(pollerTimeout),
		MemorySize:  aws.Int32(pollerMemorySize),
		CodeSha256:  strptr(in.CodeSHA256),
		State:       lambdatypes.StateActive,
		Environment: &lambdatypes.EnvironmentResponse{Variables: in.EnvVars},
	}
}

// TestPollerEnvVars_ContractMatchesPoller pins the env-var contract the hosted
// poller reads (lambda/capacity-poller/main.go). A missing variable does not fail
// the poller — it silently DEGRADES it (no notifications, no boundary re-arm) —
// so the set is asserted rather than trusted.
func TestPollerEnvVars_ContractMatchesPoller(t *testing.T) {
	env := PollerEnvVars(testRegion, testAccount, "", "", "", "arn:aws:sns:us-east-1:123456789012:lagotto-capacity-alerts")
	want := map[string]string{
		"WATCHES_TABLE":             "lagotto-watches",
		"HISTORY_TABLE":             "lagotto-match-history",
		"SCHEDULED_TABLE":           "lagotto-scheduled-launches",
		"SNS_TOPIC_ARN":             "arn:aws:sns:us-east-1:123456789012:lagotto-capacity-alerts",
		"SCHEDULE_NAME":             "lagotto-capacity-poller",
		"POLLER_FUNCTION_ARN":       "arn:aws:lambda:us-east-1:123456789012:function:lagotto-capacity-poller",
		"SCHEDULER_INVOKE_ROLE_ARN": "arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke",
	}
	if len(env) != len(want) {
		t.Errorf("env var set has %d keys, want %d: %v", len(env), len(want), env)
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("env[%q] = %q, want %q", k, env[k], v)
		}
	}
	// Custom table names must override the defaults.
	custom := PollerEnvVars(testRegion, testAccount, "w", "h", "s", "")
	if custom["WATCHES_TABLE"] != "w" || custom["HISTORY_TABLE"] != "h" || custom["SCHEDULED_TABLE"] != "s" {
		t.Errorf("custom table names not wired through: %v", custom)
	}
	// AUTO_DELETE_TABLES is an opt-in a human sets by hand; deploy must not set it.
	if _, ok := env["AUTO_DELETE_TABLES"]; ok {
		t.Error("deploy must not set AUTO_DELETE_TABLES — it is a deliberate manual opt-in")
	}
}

// TestEnsurePollerFunction_CreatesWhenAbsent asserts the full create contract
// against the CFN template's function properties.
func TestEnsurePollerFunction_CreatesWhenAbsent(t *testing.T) {
	in := testPollerInput()
	fl := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{}}
	d, _ := testDeployer(fl, nil, nil)

	arn, action, err := d.EnsurePollerFunction(context.Background(), in)
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if action != ActionCreated {
		t.Errorf("action = %q, want %q", action, ActionCreated)
	}
	if arn != PollerFunctionARN(testRegion, testAccount) {
		t.Errorf("arn = %q", arn)
	}
	if len(fl.createCalls) != 1 {
		t.Fatalf("CreateFunction called %d times, want 1", len(fl.createCalls))
	}
	c := fl.createCalls[0]
	if aws.ToString(c.FunctionName) != PollerFunctionName {
		t.Errorf("FunctionName = %q", aws.ToString(c.FunctionName))
	}
	if string(c.Runtime) != "provided.al2023" {
		t.Errorf("Runtime = %q, want provided.al2023", c.Runtime)
	}
	if len(c.Architectures) != 1 || c.Architectures[0] != lambdatypes.ArchitectureArm64 {
		t.Errorf("Architectures = %v, want [arm64]", c.Architectures)
	}
	if aws.ToString(c.Handler) != "bootstrap" {
		t.Errorf("Handler = %q, want bootstrap", aws.ToString(c.Handler))
	}
	if aws.ToInt32(c.Timeout) != 900 {
		t.Errorf("Timeout = %d, want 900", aws.ToInt32(c.Timeout))
	}
	if aws.ToInt32(c.MemorySize) != 512 {
		t.Errorf("MemorySize = %d, want 512", aws.ToInt32(c.MemorySize))
	}
	if aws.ToString(c.Role) != in.RoleARN {
		t.Errorf("Role = %q, want %q", aws.ToString(c.Role), in.RoleARN)
	}
	if c.PackageType != lambdatypes.PackageTypeZip {
		t.Errorf("PackageType = %q, want Zip", c.PackageType)
	}
	if c.Code == nil || aws.ToString(c.Code.S3Bucket) != in.Bucket || aws.ToString(c.Code.S3Key) != in.Key {
		t.Errorf("Code = %+v, want bucket %q key %q", c.Code, in.Bucket, in.Key)
	}
	if c.Environment == nil || c.Environment.Variables["SNS_TOPIC_ARN"] != in.EnvVars["SNS_TOPIC_ARN"] {
		t.Errorf("Environment.Variables not wired: %+v", c.Environment)
	}
	if c.Tags["Application"] != "lagotto" || c.Tags["Component"] != "capacity-poller" {
		t.Errorf("Tags = %v", c.Tags)
	}
}

// TestEnsurePollerFunction_RetriesIAMRolePropagation is the #154 create-race
// guard: `lagotto deploy` creates the execution role (runtimeiam.EnsureRoles)
// seconds before creating the function, and Lambda commonly refuses the create
// with "The role defined for the function cannot be assumed by Lambda" until IAM
// propagates. That's transient, so it must be retried, not surfaced.
func TestEnsurePollerFunction_RetriesIAMRolePropagation(t *testing.T) {
	notAssumable := &lambdatypes.InvalidParameterValueException{
		Message: aws.String("The role defined for the function cannot be assumed by Lambda."),
	}
	fl := &fakeLambda{
		getErr:     &lambdatypes.ResourceNotFoundException{},
		createErrs: []error{notAssumable, notAssumable, nil},
	}
	d, sl := testDeployer(fl, nil, nil)

	_, action, err := d.EnsurePollerFunction(context.Background(), testPollerInput())
	if err != nil {
		t.Fatalf("EnsurePollerFunction should have survived IAM propagation: %v", err)
	}
	if action != ActionCreated {
		t.Errorf("action = %q, want %q", action, ActionCreated)
	}
	if len(fl.createCalls) != 3 {
		t.Errorf("CreateFunction called %d times, want exactly 3 (2 failures + 1 success)", len(fl.createCalls))
	}
	if len(sl.calls) != 2 {
		t.Errorf("slept %d times, want 2 (one between each retry)", len(sl.calls))
	}
}

// TestEnsurePollerFunction_IAMPropagationExhaustionNamesTheRole: when the retries
// run out, the error must tell the user WHAT is wrong (the role) and that
// re-running is the fix — otherwise "InvalidParameterValue" alone sends people
// hunting a template bug.
func TestEnsurePollerFunction_IAMPropagationExhaustionNamesTheRole(t *testing.T) {
	notAssumable := &lambdatypes.InvalidParameterValueException{
		Message: aws.String("The role defined for the function cannot be assumed by Lambda."),
	}
	errs := make([]error, rolePropagationAttempts)
	for i := range errs {
		errs[i] = notAssumable
	}
	fl := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{}, createErrs: errs}
	d, _ := testDeployer(fl, nil, nil)

	in := testPollerInput()
	_, _, err := d.EnsurePollerFunction(context.Background(), in)
	if err == nil {
		t.Fatal("expected an error after exhausting the propagation retries")
	}
	if !strings.Contains(err.Error(), in.RoleARN) {
		t.Errorf("error does not name the role %q: %v", in.RoleARN, err)
	}
	if !strings.Contains(err.Error(), "propagation") {
		t.Errorf("error does not mention propagation (so the user won't know to re-run): %v", err)
	}
	if len(fl.createCalls) != rolePropagationAttempts {
		t.Errorf("CreateFunction called %d times, want %d", len(fl.createCalls), rolePropagationAttempts)
	}
}

// TestEnsurePollerFunction_OtherInvalidParameterIsNotRetried guards the NARROW
// matcher. InvalidParameterValueException covers a whole family of permanent
// mistakes; only the "cannot be assumed" flavour is transient. A different one
// must fail on the first call rather than burn a minute of backoff.
func TestEnsurePollerFunction_OtherInvalidParameterIsNotRetried(t *testing.T) {
	bad := &lambdatypes.InvalidParameterValueException{
		Message: aws.String("Uploaded file must be a non-empty zip"),
	}
	fl := &fakeLambda{
		getErr:     &lambdatypes.ResourceNotFoundException{},
		createErrs: []error{bad, bad, bad, nil},
	}
	d, sl := testDeployer(fl, nil, nil)

	if _, _, err := d.EnsurePollerFunction(context.Background(), testPollerInput()); err == nil {
		t.Fatal("expected a non-retryable InvalidParameterValueException to fail immediately")
	}
	if len(fl.createCalls) != 1 {
		t.Errorf("CreateFunction called %d times, want exactly 1 (fail fast)", len(fl.createCalls))
	}
	if len(sl.calls) != 0 {
		t.Errorf("slept %v — a non-transient error must not back off", sl.calls)
	}
}

// TestEnsurePollerFunction_SteadyStateIssuesNoUpdates is what makes a re-run
// cheap: a function whose code digest and configuration already match must
// produce ZERO UpdateFunctionCode and ZERO UpdateFunctionConfiguration calls (no
// revision churn, no needless settle waits).
func TestEnsurePollerFunction_SteadyStateIssuesNoUpdates(t *testing.T) {
	in := testPollerInput()
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: liveConfig(in)}}
	d, _ := testDeployer(fl, nil, nil)

	arn, action, err := d.EnsurePollerFunction(context.Background(), in)
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if action != ActionUnchanged {
		t.Errorf("action = %q, want %q", action, ActionUnchanged)
	}
	if arn != PollerFunctionARN(testRegion, testAccount) {
		t.Errorf("arn = %q", arn)
	}
	if len(fl.codeCalls) != 0 {
		t.Errorf("UpdateFunctionCode called %d times in steady state, want 0", len(fl.codeCalls))
	}
	if len(fl.configCalls) != 0 {
		t.Errorf("UpdateFunctionConfiguration called %d times in steady state, want 0", len(fl.configCalls))
	}
	// Tags are not part of UpdateFunctionConfiguration, so the idempotent upsert
	// still runs — that's how a changed --environment retags an existing function.
	if len(fl.tagCalls) != 1 {
		t.Errorf("TagResource called %d times, want 1", len(fl.tagCalls))
	}
}

// TestEnsurePollerFunction_DifferentSHAUpdatesCode checks the code fast path:
// a new digest means push the new bucket/key — and pass Architectures, which is
// the only way to correct a function that ended up x86_64.
func TestEnsurePollerFunction_DifferentSHAUpdatesCode(t *testing.T) {
	in := testPollerInput()
	cur := liveConfig(in)
	cur.CodeSha256 = strptr("some-older-digest")
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: cur}}
	d, _ := testDeployer(fl, nil, nil)

	_, action, err := d.EnsurePollerFunction(context.Background(), in)
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if action != ActionCodeUpdated {
		t.Errorf("action = %q, want %q", action, ActionCodeUpdated)
	}
	if len(fl.codeCalls) != 1 {
		t.Fatalf("UpdateFunctionCode called %d times, want 1", len(fl.codeCalls))
	}
	c := fl.codeCalls[0]
	if aws.ToString(c.S3Bucket) != in.Bucket || aws.ToString(c.S3Key) != in.Key {
		t.Errorf("UpdateFunctionCode targeted s3://%s/%s, want s3://%s/%s",
			aws.ToString(c.S3Bucket), aws.ToString(c.S3Key), in.Bucket, in.Key)
	}
	if len(c.Architectures) != 1 || c.Architectures[0] != lambdatypes.ArchitectureArm64 {
		t.Errorf("Architectures = %v, want [arm64] (the only way to fix an x86_64 function)", c.Architectures)
	}
	if len(fl.configCalls) != 0 {
		t.Errorf("configuration matched but UpdateFunctionConfiguration was called %d times", len(fl.configCalls))
	}
}

// TestEnsurePollerFunction_EmptySHAAlwaysUpdatesCode: an absent digest means
// "can't prove it's current", so push unconditionally. The digest is an
// optimization, never correctness.
func TestEnsurePollerFunction_EmptySHAAlwaysUpdatesCode(t *testing.T) {
	in := testPollerInput()
	in.CodeSHA256 = ""
	cur := liveConfig(in)
	cur.CodeSha256 = strptr("")
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: cur}}
	d, _ := testDeployer(fl, nil, nil)

	if _, action, err := d.EnsurePollerFunction(context.Background(), in); err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	} else if action != ActionCodeUpdated {
		t.Errorf("action = %q, want %q", action, ActionCodeUpdated)
	}
	if len(fl.codeCalls) != 1 {
		t.Errorf("UpdateFunctionCode called %d times with no digest, want 1", len(fl.codeCalls))
	}
}

// TestEnsurePollerFunction_ChangedEnvUpdatesFullMap: Lambda replaces
// Environment.Variables wholesale, so the update must resupply the FULL desired
// map — not just the differing key.
func TestEnsurePollerFunction_ChangedEnvUpdatesFullMap(t *testing.T) {
	in := testPollerInput()
	cur := liveConfig(in)
	cur.Environment = &lambdatypes.EnvironmentResponse{Variables: map[string]string{
		"WATCHES_TABLE": "lagotto-watches", // only one of seven survives
	}}
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: cur}}
	d, _ := testDeployer(fl, nil, nil)

	_, action, err := d.EnsurePollerFunction(context.Background(), in)
	if err != nil {
		t.Fatalf("EnsurePollerFunction: %v", err)
	}
	if action != ActionConfigUpdated {
		t.Errorf("action = %q, want %q", action, ActionConfigUpdated)
	}
	if len(fl.configCalls) != 1 {
		t.Fatalf("UpdateFunctionConfiguration called %d times, want 1", len(fl.configCalls))
	}
	got := fl.configCalls[0]
	if got.Environment == nil {
		t.Fatal("UpdateFunctionConfiguration sent no Environment")
	}
	if len(got.Environment.Variables) != len(in.EnvVars) {
		t.Errorf("sent %d env vars, want the full desired map of %d: %v",
			len(got.Environment.Variables), len(in.EnvVars), got.Environment.Variables)
	}
	for k, v := range in.EnvVars {
		if got.Environment.Variables[k] != v {
			t.Errorf("env %q = %q, want %q", k, got.Environment.Variables[k], v)
		}
	}
	// The other converged fields must be resupplied too.
	if aws.ToString(got.Handler) != pollerHandler || aws.ToInt32(got.Timeout) != pollerTimeout {
		t.Errorf("UpdateFunctionConfiguration dropped handler/timeout: %+v", got)
	}
	if len(fl.codeCalls) != 0 {
		t.Errorf("digest matched but UpdateFunctionCode was called %d times", len(fl.codeCalls))
	}
}

// TestEnsurePollerFunction_ConfigDiffTriggers exercises each individually-diffed
// configuration field, so a future edit that drops one from the comparison fails
// here rather than silently leaving a drifted function alone.
func TestEnsurePollerFunction_ConfigDiffTriggers(t *testing.T) {
	base := testPollerInput()
	cases := map[string]func(*lambdatypes.FunctionConfiguration){
		"role":    func(c *lambdatypes.FunctionConfiguration) { c.Role = strptr("arn:aws:iam::1:role/other") },
		"handler": func(c *lambdatypes.FunctionConfiguration) { c.Handler = strptr("main") },
		"runtime": func(c *lambdatypes.FunctionConfiguration) { c.Runtime = lambdatypes.RuntimeProvidedal2 },
		"timeout": func(c *lambdatypes.FunctionConfiguration) { c.Timeout = aws.Int32(60) },
		"memory":  func(c *lambdatypes.FunctionConfiguration) { c.MemorySize = aws.Int32(128) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cur := liveConfig(base)
			mutate(cur)
			if !pollerConfigDiffers(base, cur) {
				t.Errorf("a drifted %s was not detected as a difference", name)
			}
		})
	}
	if pollerConfigDiffers(base, liveConfig(base)) {
		t.Error("a matching configuration was reported as differing")
	}
}

// TestEnsurePollerFunction_RetriesResourceConflictOnUpdate: a function that is
// still settling answers an update with ResourceConflictException ("An update is
// in progress"). That's transient — retry, bounded.
func TestEnsurePollerFunction_RetriesResourceConflictOnUpdate(t *testing.T) {
	in := testPollerInput()
	cur := liveConfig(in)
	cur.CodeSha256 = strptr("older")
	conflict := &lambdatypes.ResourceConflictException{
		Message: aws.String("The operation cannot be performed at this time. An update is in progress for resource"),
	}
	fl := &fakeLambda{
		getOut:   &lambda.GetFunctionOutput{Configuration: cur},
		codeErrs: []error{conflict, conflict, nil},
	}
	d, sl := testDeployer(fl, nil, nil)

	if _, _, err := d.EnsurePollerFunction(context.Background(), in); err != nil {
		t.Fatalf("EnsurePollerFunction should have retried past the conflict: %v", err)
	}
	if len(fl.codeCalls) != 3 {
		t.Errorf("UpdateFunctionCode called %d times, want 3", len(fl.codeCalls))
	}
	if len(sl.calls) == 0 {
		t.Error("expected a backoff sleep between conflict retries")
	}
}

// TestEnsurePollerFunction_ConflictRetryIsBounded: an endpoint that never stops
// conflicting must fail, not loop forever.
func TestEnsurePollerFunction_ConflictRetryIsBounded(t *testing.T) {
	in := testPollerInput()
	cur := liveConfig(in)
	cur.CodeSha256 = strptr("older")
	conflict := &lambdatypes.ResourceConflictException{Message: aws.String("An update is in progress")}
	errs := make([]error, conflictAttempts+5)
	for i := range errs {
		errs[i] = conflict
	}
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: cur}, codeErrs: errs}
	d, _ := testDeployer(fl, nil, nil)

	if _, _, err := d.EnsurePollerFunction(context.Background(), in); err == nil {
		t.Fatal("expected an error once the conflict retries are exhausted")
	}
	if len(fl.codeCalls) != conflictAttempts {
		t.Errorf("UpdateFunctionCode called %d times, want the bound of %d", len(fl.codeCalls), conflictAttempts)
	}
}

// TestWaitFunctionSettled_AbsentLastUpdateStatusSettlesImmediately is why this
// package does NOT use lambda.FunctionUpdatedV2Waiter. That SDK waiter's
// retryable DEFAULTS TO RETRY when no matcher matches, so a response that omits
// LastUpdateStatus (substrate's Lambda emulator never returns it) makes it spin
// its whole max-wait budget and then fail "exceeded max wait time". Real Lambda
// always populates the field once something has been applied, so an absent value
// means nothing is in flight — settled, zero sleeps.
func TestWaitFunctionSettled_AbsentLastUpdateStatusSettlesImmediately(t *testing.T) {
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{
		Configuration: &lambdatypes.FunctionConfiguration{State: lambdatypes.StateActive},
	}}
	d, sl := testDeployer(fl, nil, nil)

	if err := d.waitFunctionSettled(context.Background(), PollerFunctionName); err != nil {
		t.Fatalf("waitFunctionSettled: %v", err)
	}
	if fl.getCalls != 1 {
		t.Errorf("GetFunction called %d times, want exactly 1", fl.getCalls)
	}
	if len(sl.calls) != 0 {
		t.Errorf("slept %v — an absent LastUpdateStatus must settle immediately", sl.calls)
	}

	// Same when State is empty too (an endpoint that reports neither field).
	fl2 := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{}}}
	d2, sl2 := testDeployer(fl2, nil, nil)
	if err := d2.waitFunctionSettled(context.Background(), PollerFunctionName); err != nil {
		t.Fatalf("waitFunctionSettled with no state fields: %v", err)
	}
	if len(sl2.calls) != 0 {
		t.Errorf("slept %v with no state fields at all", sl2.calls)
	}
}

func TestWaitFunctionSettled_WaitsThroughPendingAndInProgress(t *testing.T) {
	pending := &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{State: lambdatypes.StatePending}}
	inProgress := &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
		State: lambdatypes.StateActive, LastUpdateStatus: lambdatypes.LastUpdateStatusInProgress,
	}}
	done := &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
		State: lambdatypes.StateActive, LastUpdateStatus: lambdatypes.LastUpdateStatusSuccessful,
	}}
	fl := &fakeLambda{getSeq: []getResult{{out: pending}, {out: inProgress}, {out: done}}}
	d, sl := testDeployer(fl, nil, nil)

	if err := d.waitFunctionSettled(context.Background(), PollerFunctionName); err != nil {
		t.Fatalf("waitFunctionSettled: %v", err)
	}
	if fl.getCalls != 3 {
		t.Errorf("GetFunction called %d times, want 3", fl.getCalls)
	}
	if len(sl.calls) != 2 {
		t.Errorf("slept %d times, want 2", len(sl.calls))
	}
}

func TestWaitFunctionSettled_ReportsFailureReasons(t *testing.T) {
	t.Run("last update failed", func(t *testing.T) {
		fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
			State:                  lambdatypes.StateActive,
			LastUpdateStatus:       lambdatypes.LastUpdateStatusFailed,
			LastUpdateStatusReason: strptr("InvalidSecurityGroupID"),
		}}}
		d, _ := testDeployer(fl, nil, nil)
		err := d.waitFunctionSettled(context.Background(), PollerFunctionName)
		if err == nil || !strings.Contains(err.Error(), "InvalidSecurityGroupID") {
			t.Errorf("error should carry LastUpdateStatusReason, got %v", err)
		}
	})
	t.Run("state failed", func(t *testing.T) {
		fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
			State:       lambdatypes.StateFailed,
			StateReason: strptr("ImageDeleted"),
		}}}
		d, _ := testDeployer(fl, nil, nil)
		err := d.waitFunctionSettled(context.Background(), PollerFunctionName)
		if err == nil || !strings.Contains(err.Error(), "ImageDeleted") {
			t.Errorf("error should carry StateReason, got %v", err)
		}
	})
	t.Run("never settles", func(t *testing.T) {
		fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
			State: lambdatypes.StateActive, LastUpdateStatus: lambdatypes.LastUpdateStatusInProgress,
		}}}
		d, _ := testDeployer(fl, nil, nil)
		if err := d.waitFunctionSettled(context.Background(), PollerFunctionName); err == nil {
			t.Error("expected a timeout error for a function that never settles")
		}
	})
}

// TestEnsurePollerFunction_GetFunctionErrorPropagates: an error that isn't
// ResourceNotFound (AccessDenied, throttling) must surface, not be mistaken for
// "absent" and trigger a create.
func TestEnsurePollerFunction_GetFunctionErrorPropagates(t *testing.T) {
	fl := &fakeLambda{getErr: errors.New("AccessDeniedException: not authorized")}
	d, _ := testDeployer(fl, nil, nil)

	if _, _, err := d.EnsurePollerFunction(context.Background(), testPollerInput()); err == nil {
		t.Fatal("expected the AccessDenied to propagate")
	}
	if len(fl.createCalls) != 0 {
		t.Error("a non-NotFound GetFunction error must not be treated as absent")
	}
}

// TestDeletePollerFunction_AbsentIsNotAnError: Teardown must be re-runnable.
func TestDeletePollerFunction_AbsentIsNotAnError(t *testing.T) {
	fl := &fakeLambda{deleteErr: &lambdatypes.ResourceNotFoundException{}}
	d, _ := testDeployer(fl, nil, nil)
	if err := d.deletePollerFunction(context.Background()); err != nil {
		t.Errorf("deleting an absent function should be a no-op success, got %v", err)
	}

	fl2 := &fakeLambda{}
	d2, _ := testDeployer(fl2, nil, nil)
	if err := d2.deletePollerFunction(context.Background()); err != nil {
		t.Errorf("deletePollerFunction: %v", err)
	}
	if fl2.deleteCalls != 1 {
		t.Errorf("DeleteFunction called %d times, want 1", fl2.deleteCalls)
	}

	fl3 := &fakeLambda{deleteErr: errors.New("AccessDeniedException")}
	d3, _ := testDeployer(fl3, nil, nil)
	if err := d3.deletePollerFunction(context.Background()); err == nil {
		t.Error("a real delete failure must surface")
	}
}

// TestRequirePollerDeployed_PresentIsOK: a function that exists is simply fine.
func TestRequirePollerDeployed_PresentIsOK(t *testing.T) {
	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{
		Configuration: liveConfig(testPollerInput()),
	}}
	d, _ := testDeployer(fl, nil, nil)

	if err := d.RequirePollerDeployed(context.Background(), testRegion, testAccount); err != nil {
		t.Fatalf("RequirePollerDeployed: %v", err)
	}
	if fl.getCalls != 1 {
		t.Errorf("GetFunction called %d times, want 1", fl.getCalls)
	}
}

// TestRequirePollerDeployed_AbsentNamesWhereToLook: the whole point of this check
// is the error message `lagotto launch` shows when the poller was never deployed
// (or was deleted out from under a stack that still reports its outputs), so pin
// the pieces a user needs: the function name, the account, the region, and the
// command to run.
func TestRequirePollerDeployed_AbsentNamesWhereToLook(t *testing.T) {
	fl := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{}}
	d, _ := testDeployer(fl, nil, nil)

	err := d.RequirePollerDeployed(context.Background(), testRegion, testAccount)
	if err == nil {
		t.Fatal("expected an error when the poller function is absent")
	}
	for _, want := range []string{PollerFunctionName, testAccount, testRegion, "lagotto deploy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// TestRequirePollerDeployed_NonNotFoundIsWrapped is the important guard: an
// AccessDenied on GetFunction means we don't KNOW whether the poller is deployed.
// Telling the user to run `lagotto deploy` would send them to fix the wrong
// problem, so the underlying error must be wrapped and surfaced instead.
func TestRequirePollerDeployed_NonNotFoundIsWrapped(t *testing.T) {
	denied := errors.New("AccessDeniedException: User is not authorized to perform lambda:GetFunction")
	fl := &fakeLambda{getErr: denied}
	d, _ := testDeployer(fl, nil, nil)

	err := d.RequirePollerDeployed(context.Background(), testRegion, testAccount)
	if err == nil {
		t.Fatal("expected the AccessDenied to surface")
	}
	if !errors.Is(err, denied) {
		t.Errorf("error must wrap the underlying failure, got %v", err)
	}
	if strings.Contains(err.Error(), "isn't deployed") || strings.Contains(err.Error(), "lagotto deploy") {
		t.Errorf("a permissions failure must not be reported as an undeployed poller: %v", err)
	}
}
