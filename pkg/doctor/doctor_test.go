package doctor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

const (
	testRegion  = "us-west-2"
	testAccount = "123456789012"
	testVersion = "0.60.0"
)

func expectedPolicy(t *testing.T) string {
	t.Helper()
	doc, err := runtimeiam.PolicyDocument(testRegion, testAccount)
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	return doc
}

// healthyState is an account where everything is right. Each test mutates the one
// thing it is about, so a failure names the rule that broke rather than the setup.
func healthyState(t *testing.T) State {
	t.Helper()
	return State{
		Region:          testRegion,
		AccountID:       testAccount,
		CLIVersion:      testVersion,
		LegacyStackName: "lagotto",
		PolicyDoc:       expectedPolicy(t),
		Roles: map[string]bool{
			runtimeiam.RoleName:                true,
			runtimeiam.SchedulerInvokeRoleName: true,
		},
		RoleErrs: map[string]error{},
		Tables: map[string]bool{
			"lagotto-watches": true, "lagotto-match-history": true, "lagotto-scheduled-launches": true,
		},
		TableOrder:    []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"},
		TableErrs:     map[string]error{},
		Poller:        deploy.PollerInfo{Exists: true, Version: testVersion, RoleARN: runtimeiam.RoleARN(testAccount)},
		Schedule:      deploy.ScheduleInfo{Exists: true, State: "ENABLED", Expression: "rate(5 minutes)"},
		TopicPresent:  true,
		ActiveWatches: 2,
	}
}

// find returns the named check. Fails the test when it's absent, since every
// assertion below is about a specific check's outcome.
func find(t *testing.T, r *Report, name string) Check {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("report has no %q check (checks: %v)", name, checkNames(r))
	return Check{}
}

func absent(t *testing.T, r *Report, name string) {
	t.Helper()
	for _, c := range r.Checks {
		if c.Name == name {
			t.Fatalf("report should not contain a %q check here, got %+v", name, c)
		}
	}
}

func checkNames(r *Report) []string {
	var out []string
	for _, c := range r.Checks {
		out = append(out, c.Name)
	}
	return out
}

// TestEvaluate_HealthyAccount: nothing failing, nothing warning, exit code zero.
func TestEvaluate_HealthyAccount(t *testing.T) {
	r := Evaluate(healthyState(t))
	if r.Failed() {
		t.Errorf("a healthy account reported a failure: %+v", r.Checks)
	}
	pass, warn, fail := r.Counts()
	if warn != 0 || fail != 0 {
		t.Errorf("counts = %d pass / %d warn / %d fail, want no warnings or failures: %+v", pass, warn, fail, r.Checks)
	}
	if got := find(t, r, CheckRuntimePolicy).Status; got != StatusPass {
		t.Errorf("runtime-policy = %s, want PASS", got)
	}
	// No legacy stack in the healthy state → the check is omitted, not passed.
	absent(t, r, CheckLegacyStack)
}

// --- the headline: runtime-policy drift -------------------------------------

// TestEvaluate_PolicyMissingGrantFails is the class this command exists for: the
// deployed policy predates a grant the current code needs (#151's pricing shape).
// It must FAIL, name the missing action, and tell the user to run `lagotto setup`.
func TestEvaluate_PolicyMissingGrantFails(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc = policyWithout(t, "pricing:GetProducts")

	c := find(t, Evaluate(st), CheckRuntimePolicy)
	if c.Status != StatusFail {
		t.Fatalf("status = %s, want FAIL (the poller cannot do what the code assumes)", c.Status)
	}
	body := c.Summary + "\n" + strings.Join(c.Detail, "\n")
	if !strings.Contains(body, "pricing:GetProducts") {
		t.Errorf("the finding does not name the missing action:\n%s", body)
	}
	if !strings.Contains(strings.Join(c.Fix, " "), "lagotto setup") {
		t.Errorf("Fix = %v, want it to say 'lagotto setup'", c.Fix)
	}
	if !Evaluate(st).Failed() {
		t.Error("a missing grant must make the report fail (non-zero exit)")
	}
}

// TestEvaluate_PolicyExtraGrantWarns is the other half: the deployed policy is
// NEWER than this binary. Nothing is broken, so it must be a WARN — and it must say
// that running THIS binary's setup would revert those grants, which is the trap the
// beta tester hit during #148.
func TestEvaluate_PolicyExtraGrantWarns(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc = policyPlus(t, "future:NewApi")

	r := Evaluate(st)
	c := find(t, r, CheckRuntimePolicy)
	if c.Status != StatusWarn {
		t.Fatalf("status = %s, want WARN (the deployed policy is ahead; nothing is broken)", c.Status)
	}
	if r.Failed() {
		t.Error("an extra grant must NOT make the report fail")
	}
	body := c.Summary + "\n" + strings.Join(c.Detail, "\n")
	for _, want := range []string{"future:NewApi", "REVERT"} {
		if !strings.Contains(body, want) {
			t.Errorf("the finding does not mention %q:\n%s", want, body)
		}
	}
}

// TestEvaluate_PolicyMissingAndExtraFails: when BOTH halves are present the
// failure wins, because the missing half is actively degrading the poller.
func TestEvaluate_PolicyMissingAndExtraFails(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc = policyPlus(t, "future:NewApi")
	st.PolicyDoc = mutatePolicy(t, st.PolicyDoc, func(statements []map[string]interface{}) []map[string]interface{} {
		for _, s := range statements {
			dropAction(s, "pricing:GetProducts")
		}
		return statements
	})

	c := find(t, Evaluate(st), CheckRuntimePolicy)
	if c.Status != StatusFail {
		t.Fatalf("status = %s, want FAIL", c.Status)
	}
	body := strings.Join(c.Detail, "\n")
	if !strings.Contains(body, "future:NewApi") {
		t.Errorf("the extra grants should still be reported alongside the failure:\n%s", body)
	}
}

// TestEvaluate_PolicyNormalizedNoFalseAlarm: the deployed document arrives
// URL-encoded, re-indented and re-keyed. It must PASS. This is the false-alarm
// guard at the Evaluate level (the grant-level normalization is tested in
// pkg/runtimeiam) — a doctor that reports drift on every healthy account is worse
// than no doctor.
func TestEvaluate_PolicyNormalizedNoFalseAlarm(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc = url.PathEscape(mutatePolicy(t, expectedPolicy(t), nil))

	c := find(t, Evaluate(st), CheckRuntimePolicy)
	if c.Status != StatusPass {
		t.Fatalf("status = %s, want PASS for a semantically identical policy: %s\n%v", c.Status, c.Summary, c.Detail)
	}
}

// TestEvaluate_PolicyAbsence: the two absences need different next steps, because
// "run setup" doesn't help someone who never deployed.
func TestEvaluate_PolicyAbsence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantFix string
	}{
		{"no role at all", runtimeiam.ErrRoleNotFound, "lagotto deploy"},
		{"role without the inline policy", runtimeiam.ErrPolicyNotFound, "lagotto setup"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := healthyState(t)
			st.PolicyDoc, st.PolicyErr = "", tc.err

			c := find(t, Evaluate(st), CheckRuntimePolicy)
			if c.Status != StatusFail {
				t.Fatalf("status = %s, want FAIL", c.Status)
			}
			if !strings.Contains(strings.Join(c.Fix, " "), tc.wantFix) {
				t.Errorf("Fix = %v, want it to include %q", c.Fix, tc.wantFix)
			}
		})
	}
}

// TestEvaluate_PolicyReadFailureWarns: an AccessDenied means "can't tell", not
// "drifted". Telling someone their policy is wrong when the real problem is a
// missing iam:GetRolePolicy sends them off applying the wrong fix.
func TestEvaluate_PolicyReadFailureWarns(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc, st.PolicyErr = "", errors.New("AccessDenied: iam:GetRolePolicy")

	r := Evaluate(st)
	c := find(t, r, CheckRuntimePolicy)
	if c.Status != StatusWarn {
		t.Fatalf("status = %s, want WARN for an unreadable policy", c.Status)
	}
	if r.Failed() {
		t.Error("an unreadable policy must not fail the report")
	}
}

// TestEvaluate_PolicyDriftIsMachineReadable: -o json consumers get the exact
// grants, not just the prose.
func TestEvaluate_PolicyDriftIsMachineReadable(t *testing.T) {
	st := healthyState(t)
	st.PolicyDoc = policyWithout(t, "pricing:GetProducts")
	r := Evaluate(st)
	if r.PolicyDrift == nil || len(r.PolicyDrift.Missing) == 0 {
		t.Fatalf("PolicyDrift = %+v, want the missing grants attached", r.PolicyDrift)
	}
	blob, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	if !strings.Contains(string(blob), "pricing:GetProducts") {
		t.Errorf("the JSON report does not carry the missing action: %s", blob)
	}
}

// --- poller version --------------------------------------------------------

func TestEvaluate_PollerVersion(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deployed   string
		cli        string
		want       Status
		wantInText string
	}{
		{"same version", "0.60.0", "0.60.0", StatusPass, "matches"},
		{"deployed older", "0.58.2", "0.60.0", StatusWarn, "OLDER"},
		{"deployed newer", "0.61.0", "0.60.0", StatusWarn, "NEWER"},
		{"no version tag", "", "0.60.0", StatusWarn, "UNKNOWN"},
		{"dev CLI build", "0.60.0", "dev", StatusWarn, "cannot compare"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := healthyState(t)
			st.Poller.Version = tc.deployed
			st.CLIVersion = tc.cli

			r := Evaluate(st)
			c := find(t, r, CheckPollerVersion)
			if c.Status != tc.want {
				t.Errorf("status = %s, want %s (%s)", c.Status, tc.want, c.Summary)
			}
			if !strings.Contains(c.Summary, tc.wantInText) {
				t.Errorf("summary = %q, want it to contain %q", c.Summary, tc.wantInText)
			}
			// A stale poller is drift, but never a failure: it's fixed with a
			// deploy and nothing is broken in the meantime.
			if r.Failed() {
				t.Errorf("a version finding must not fail the report: %+v", c)
			}
		})
	}
}

// TestEvaluate_PollerVersionOlderNamesTheDeployCommand: the remediation must carry
// THIS CLI's version, so it's copy-pasteable.
func TestEvaluate_PollerVersionOlderNamesTheDeployCommand(t *testing.T) {
	st := healthyState(t)
	st.Poller.Version = "0.58.2"
	c := find(t, Evaluate(st), CheckPollerVersion)
	if want := "lagotto deploy --version " + testVersion; !strings.Contains(strings.Join(c.Fix, " "), want) {
		t.Errorf("Fix = %v, want %q", c.Fix, want)
	}
}

// TestEvaluate_NoPollerOmitsVersionCheck: with no function deployed there is no
// version to discuss, and the poller-lambda check already says so. A second line
// repeating it is noise.
func TestEvaluate_NoPollerOmitsVersionCheck(t *testing.T) {
	st := healthyState(t)
	st.Poller = deploy.PollerInfo{}
	r := Evaluate(st)
	absent(t, r, CheckPollerVersion)
	if got := find(t, r, CheckPollerLambda).Status; got != StatusFail {
		t.Errorf("poller-lambda = %s, want FAIL", got)
	}
}

// --- resources exist -------------------------------------------------------

func TestEvaluate_MissingResourcesFailWithRemediation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*State)
		check   string
		wantFix string
		wantIn  string
	}{
		{"a missing table", func(st *State) { st.Tables["lagotto-match-history"] = false },
			CheckTables, "lagotto setup", "lagotto-match-history"},
		{"a missing IAM role", func(st *State) { st.Roles[runtimeiam.SchedulerInvokeRoleName] = false },
			CheckIAMRoles, "lagotto deploy", runtimeiam.SchedulerInvokeRoleName},
		{"no poller Lambda", func(st *State) { st.Poller = deploy.PollerInfo{} },
			CheckPollerLambda, "lagotto deploy", deploy.PollerFunctionName},
		{"no SNS topic", func(st *State) { st.TopicPresent = false },
			CheckAlertsTopic, "lagotto deploy", deploy.AlertsTopicName},
		{"no schedule", func(st *State) { st.Schedule = deploy.ScheduleInfo{} },
			CheckSchedule, "lagotto deploy", deploy.PollerScheduleName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := healthyState(t)
			tc.mutate(&st)
			r := Evaluate(st)
			c := find(t, r, tc.check)
			if c.Status != StatusFail {
				t.Fatalf("%s = %s, want FAIL", tc.check, c.Status)
			}
			if !strings.Contains(c.Summary, tc.wantIn) {
				t.Errorf("summary = %q, want it to name %q", c.Summary, tc.wantIn)
			}
			if !strings.Contains(strings.Join(c.Fix, " "), tc.wantFix) {
				t.Errorf("Fix = %v, want %q", c.Fix, tc.wantFix)
			}
			if !r.Failed() {
				t.Error("a missing resource must make the report fail")
			}
		})
	}
}

// TestEvaluate_UnreadableResourceWarns: an AccessDenied on a probe is "can't
// tell", not "missing" — the same rule as for the policy.
func TestEvaluate_UnreadableResourceWarns(t *testing.T) {
	st := healthyState(t)
	st.TableErrs["lagotto-watches"] = errors.New("AccessDenied")
	st.RoleErrs[runtimeiam.RoleName] = errors.New("AccessDenied")
	st.PollerErr = errors.New("AccessDenied")
	st.TopicErr = errors.New("AccessDenied")
	st.ScheduleErr = errors.New("AccessDenied")

	r := Evaluate(st)
	for _, name := range []string{CheckTables, CheckIAMRoles, CheckPollerLambda, CheckAlertsTopic, CheckSchedule} {
		if got := find(t, r, name).Status; got != StatusWarn {
			t.Errorf("%s = %s, want WARN when the read failed", name, got)
		}
	}
	if r.Failed() {
		t.Error("unreadable resources must not be reported as missing (no failure)")
	}
}

// --- schedule state vs reality ---------------------------------------------

// TestEvaluate_ScheduleStateMatrix covers the invariant "ENABLED iff a watch is
// active", including the silent-death case.
func TestEvaluate_ScheduleStateMatrix(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   string
		active  int
		want    Status
		wantFix bool
	}{
		{"enabled with active watches", "ENABLED", 3, StatusPass, false},
		{"enabled with no watches", "ENABLED", 0, StatusWarn, false},
		{"DISABLED with active watches (silent death)", "DISABLED", 2, StatusFail, true},
		{"disabled with no watches", "DISABLED", 0, StatusPass, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := healthyState(t)
			st.Schedule.State = tc.state
			st.ActiveWatches = tc.active

			r := Evaluate(st)
			c := find(t, r, CheckScheduleState)
			if c.Status != tc.want {
				t.Fatalf("status = %s, want %s (%s)", c.Status, tc.want, c.Summary)
			}
			if got := len(c.Fix) > 0; got != tc.wantFix {
				t.Errorf("Fix = %v, want fix-present=%v", c.Fix, tc.wantFix)
			}
			if tc.want == StatusFail && !r.Failed() {
				t.Error("a DISABLED schedule with active watches must fail the report — " +
					"this is the case where nothing is polling and nothing reports an error")
			}
		})
	}
}

// TestEvaluate_ScheduleStateSilentDeathSaysWhatIsHappening: the FAIL text has to
// spell out the consequence, because the symptom is the ABSENCE of symptoms.
func TestEvaluate_ScheduleStateSilentDeathSaysWhatIsHappening(t *testing.T) {
	st := healthyState(t)
	st.Schedule.State = "DISABLED"
	st.ActiveWatches = 2
	c := find(t, Evaluate(st), CheckScheduleState)
	body := c.Summary + "\n" + strings.Join(c.Detail, "\n")
	for _, want := range []string{"DISABLED", "nothing is polling"} {
		if !strings.Contains(body, want) {
			t.Errorf("the finding does not mention %q:\n%s", want, body)
		}
	}
}

// TestEvaluate_ScheduleStateUnknownWatchCountWarns: without the watch count the
// invariant can't be evaluated, so it must not be guessed either way.
func TestEvaluate_ScheduleStateUnknownWatchCountWarns(t *testing.T) {
	st := healthyState(t)
	st.Schedule.State = "DISABLED"
	st.ActiveWatchesErr = errors.New("AccessDenied: dynamodb:Query")

	r := Evaluate(st)
	if got := find(t, r, CheckScheduleState).Status; got != StatusWarn {
		t.Errorf("schedule-state = %s, want WARN when the watch count is unknown", got)
	}
	if r.Failed() {
		t.Error("an unknown watch count must not fail the report")
	}
}

// TestEvaluate_NoScheduleOmitsStateCheck: evalSchedule already reports the absence.
func TestEvaluate_NoScheduleOmitsStateCheck(t *testing.T) {
	st := healthyState(t)
	st.Schedule = deploy.ScheduleInfo{}
	absent(t, Evaluate(st), CheckScheduleState)
}

// --- legacy CloudFormation stack -------------------------------------------

func TestEvaluate_LegacyStackWarns(t *testing.T) {
	st := healthyState(t)
	st.LegacyStack = true
	st.LegacyStackStatus = "UPDATE_COMPLETE"

	r := Evaluate(st)
	c := find(t, r, CheckLegacyStack)
	if c.Status != StatusWarn {
		t.Fatalf("status = %s, want WARN", c.Status)
	}
	if r.Failed() {
		t.Error("a leftover stack must not fail the report — the poller works fine")
	}
	body := c.Summary + "\n" + strings.Join(c.Detail, "\n") + "\n" + strings.Join(c.Fix, "\n")
	for _, want := range []string{"lagotto", "--migrate-from-cloudformation"} {
		if !strings.Contains(body, want) {
			t.Errorf("the finding does not mention %q:\n%s", want, body)
		}
	}
}

// TestEvaluate_LegacyStackDetectionFailureIsSilent: it's advisory, exactly as in
// `lagotto deploy`, and must not become what the user reads instead of the checks
// they came for.
func TestEvaluate_LegacyStackDetectionFailureIsSilent(t *testing.T) {
	st := healthyState(t)
	st.LegacyStackErr = errors.New("AccessDenied: cloudformation:DescribeStacks")
	absent(t, Evaluate(st), CheckLegacyStack)
}

// --- exit code -------------------------------------------------------------

// TestReportFailed_OnlyFailuresCount is the CI contract: warnings must not make
// the command exit non-zero, or people stop running it.
func TestReportFailed_OnlyFailuresCount(t *testing.T) {
	warnOnly := &Report{Checks: []Check{
		{Name: "a", Status: StatusPass}, {Name: "b", Status: StatusWarn}, {Name: "c", Status: StatusWarn},
	}}
	if warnOnly.Failed() {
		t.Error("warnings alone must not fail")
	}
	withFail := &Report{Checks: append(warnOnly.Checks, Check{Name: "d", Status: StatusFail})}
	if !withFail.Failed() {
		t.Error("a single FAIL must fail the report")
	}
	pass, warn, fail := withFail.Counts()
	if pass != 1 || warn != 2 || fail != 1 {
		t.Errorf("Counts() = %d/%d/%d, want 1/2/1", pass, warn, fail)
	}
}

// --- Gather (the AWS-touching half, against fakes) -------------------------

// TestGather_ReadsEverythingAndMutatesNothing is the read-only guarantee: the IAM
// fake panics on any write call, so a Gather that ever grew a PutRolePolicy would
// fail here rather than in someone's account.
func TestGather_ReadsEverythingAndMutatesNothing(t *testing.T) {
	policy := expectedPolicy(t)
	f := &fakeIAM{
		t:        t,
		roles:    map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true},
		policies: map[string]string{runtimeiam.RoleName + "/" + runtimeiam.PolicyName: url.PathEscape(policy)},
	}
	tables := &fakeTables{present: map[string]bool{"lagotto-watches": true, "lagotto-match-history": true}}
	infra := &fakeInfra{
		poller:      deploy.PollerInfo{Exists: true, Version: testVersion},
		schedule:    deploy.ScheduleInfo{Exists: true, State: "ENABLED"},
		topic:       true,
		legacyStack: true, legacyStatus: "UPDATE_COMPLETE",
	}

	st := Gather(context.Background(), Options{
		Region:          testRegion,
		AccountID:       testAccount,
		CLIVersion:      testVersion,
		LegacyStackName: "lagotto",
		Tables:          []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"},
		IAM:             f,
		DynamoDB:        tables,
		Infra:           infra,
		ActiveWatches:   func(context.Context) (int, error) { return 4, nil },
	})

	if st.PolicyErr != nil {
		t.Fatalf("PolicyErr = %v, want the policy read back", st.PolicyErr)
	}
	if !json.Valid([]byte(st.PolicyDoc)) {
		t.Errorf("PolicyDoc is not decoded JSON: %.60q", st.PolicyDoc)
	}
	if !st.Roles[runtimeiam.RoleName] || !st.Roles[runtimeiam.SchedulerInvokeRoleName] {
		t.Errorf("Roles = %v, want both present", st.Roles)
	}
	if st.Tables["lagotto-scheduled-launches"] {
		t.Error("an absent table must be recorded as absent, not present")
	}
	if len(st.TableOrder) != 3 {
		t.Errorf("TableOrder = %v, want all three names echoed for reporting", st.TableOrder)
	}
	if !st.Poller.Exists || !st.Schedule.Exists || !st.TopicPresent || !st.LegacyStack {
		t.Errorf("infra not gathered: %+v", st)
	}
	if st.ActiveWatches != 4 {
		t.Errorf("ActiveWatches = %d, want 4", st.ActiveWatches)
	}

	// And the whole point: one read per resource, zero writes.
	if f.writes != 0 {
		t.Errorf("Gather made %d mutating IAM call(s); doctor must be strictly read-only", f.writes)
	}
	if tables.describes != 3 {
		t.Errorf("DescribeTable called %d times, want 3", tables.describes)
	}

	// The gathered state must evaluate to the finding we'd expect end-to-end.
	r := Evaluate(st)
	if got := find(t, r, CheckRuntimePolicy).Status; got != StatusPass {
		t.Errorf("runtime-policy = %s, want PASS (the deployed policy IS this binary's)", got)
	}
	if got := find(t, r, CheckTables).Status; got != StatusFail {
		t.Errorf("tables = %s, want FAIL (one table absent)", got)
	}
}

// TestGather_PerReadErrorsAreIsolated: one AccessDenied must cost one check, not
// the whole report.
func TestGather_PerReadErrorsAreIsolated(t *testing.T) {
	denied := errors.New("AccessDenied")
	f := &fakeIAM{
		t:            t,
		roles:        map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true},
		getPolicyErr: denied,
	}
	tables := &fakeTables{err: denied}
	infra := &fakeInfra{pollerErr: denied, scheduleErr: denied, topicErr: denied, legacyErr: denied}

	st := Gather(context.Background(), Options{
		Region: testRegion, AccountID: testAccount, CLIVersion: testVersion,
		LegacyStackName: "lagotto",
		Tables:          []string{"lagotto-watches"},
		IAM:             f, DynamoDB: tables, Infra: infra,
		ActiveWatches: func(context.Context) (int, error) { return 0, denied },
	})

	if st.PolicyErr == nil || errors.Is(st.PolicyErr, runtimeiam.ErrPolicyNotFound) {
		t.Errorf("PolicyErr = %v, want the AccessDenied surfaced (not classified as absence)", st.PolicyErr)
	}
	if st.TableErrs["lagotto-watches"] == nil {
		t.Error("a table read failure must be recorded as an error, not as absence")
	}
	if st.PollerErr == nil || st.ScheduleErr == nil || st.TopicErr == nil || st.LegacyStackErr == nil || st.ActiveWatchesErr == nil {
		t.Errorf("infra read failures not recorded: %+v", st)
	}

	// Every one of those degrades to a WARN, and the command still exits zero.
	if r := Evaluate(st); r.Failed() {
		t.Errorf("an account we simply cannot read must not be reported as broken: %+v", r.Checks)
	}
}

// TestGather_NilSeamsAreTolerated: a partially-wired Options must not panic (it's
// the shape tests and future callers will use).
func TestGather_NilSeamsAreTolerated(t *testing.T) {
	st := Gather(context.Background(), Options{Region: testRegion, AccountID: testAccount})
	if r := Evaluate(st); len(r.Checks) == 0 {
		t.Error("Evaluate produced no checks at all")
	}
}

// --- rendering -------------------------------------------------------------

// TestReportText covers what a user actually sees: a marker per line, the detail
// indented under it, and the failing commands repeated at the end.
func TestReportText(t *testing.T) {
	st := healthyState(t)
	st.Schedule.State = "DISABLED" // one FAIL, with a fix
	text := Evaluate(st).Text(PlainSymbols)

	for _, want := range []string{
		"lagotto doctor", testAccount, testRegion,
		"[✓] PASS", "[✗] FAIL", CheckScheduleState, "To fix:",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered report is missing %q:\n%s", want, text)
		}
	}
}

// TestReportText_HonorsTheSymbolFunction: --no-emoji / --accessibility are root
// flags, and a diagnostic that ignores them is one somebody can't read. The
// markers therefore come from the injected symboler, never from a literal.
func TestReportText_HonorsTheSymbolFunction(t *testing.T) {
	r := Evaluate(healthyState(t))
	emoji := r.Text(func(name string) string {
		if name == "success" {
			return "✅"
		}
		return "?"
	})
	if !strings.Contains(emoji, "✅") {
		t.Errorf("the injected symboler was not used:\n%s", emoji)
	}
	plain := r.Text(PlainSymbols)
	if strings.Contains(plain, "✅") {
		t.Errorf("the plain marker set leaked an emoji:\n%s", plain)
	}
	// A nil symboler must still render (the CLI falls back when i18n is uninitialized).
	if got := r.Text(nil); !strings.Contains(got, "PASS") {
		t.Errorf("Text(nil) did not render: %s", got)
	}
}

// TestReportText_NoFixSectionWhenHealthy keeps the happy path quiet.
func TestReportText_NoFixSectionWhenHealthy(t *testing.T) {
	text := Evaluate(healthyState(t)).Text(PlainSymbols)
	if strings.Contains(text, "To fix:") {
		t.Errorf("a healthy report should not print a fix section:\n%s", text)
	}
}

// --- version comparison ----------------------------------------------------

func TestCompareVersions(t *testing.T) {
	for _, tc := range []struct {
		a, b   string
		want   int
		wantOK bool
	}{
		{"0.60.0", "0.60.0", 0, true},
		{"v0.60.0", "0.60.0", 0, true},
		{"0.58.2", "0.60.0", -1, true},
		{"0.61.0", "0.60.0", 1, true},
		{"1.0.0", "0.99.9", 1, true},
		{"0.60.1", "0.60.0", 1, true},
		{"0.60.0-rc1", "0.60.0", 0, true}, // pre-release suffix ignored
		{"dev", "0.60.0", 0, false},
		{"", "0.60.0", 0, false},
		{"not-a-version", "0.60.0", 0, false},
	} {
		got, ok := compareVersions(tc.a, tc.b)
		if ok != tc.wantOK {
			t.Errorf("compareVersions(%q, %q) ok = %v, want %v", tc.a, tc.b, ok, tc.wantOK)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestDisplayVersion(t *testing.T) {
	if got := displayVersion("dev"); got != "dev build" {
		t.Errorf("displayVersion(dev) = %q", got)
	}
	if got := displayVersion("0.60.0"); got != "v0.60.0" {
		t.Errorf("displayVersion(0.60.0) = %q", got)
	}
}

// --- fakes -----------------------------------------------------------------

// fakeIAM implements runtimeiam.IAMAPI. The three MUTATING methods fail the test
// on sight: doctor is read-only, and this is where that is enforced.
type fakeIAM struct {
	t            *testing.T
	roles        map[string]bool
	policies     map[string]string // "role/policy" → document
	getPolicyErr error
	writes       int
}

func (f *fakeIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if f.roles[aws.ToString(in.RoleName)] {
		return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName}}, nil
	}
	return nil, &iamtypes.NoSuchEntityException{}
}

func (f *fakeIAM) GetRolePolicy(_ context.Context, in *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	if f.getPolicyErr != nil {
		return nil, f.getPolicyErr
	}
	doc, ok := f.policies[aws.ToString(in.RoleName)+"/"+aws.ToString(in.PolicyName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(doc)}, nil
}

func (f *fakeIAM) CreateRole(context.Context, *iam.CreateRoleInput, ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	f.writes++
	f.t.Error("doctor called CreateRole — it must be strictly read-only")
	return nil, fmt.Errorf("read-only")
}

func (f *fakeIAM) AttachRolePolicy(context.Context, *iam.AttachRolePolicyInput, ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	f.writes++
	f.t.Error("doctor called AttachRolePolicy — it must be strictly read-only")
	return nil, fmt.Errorf("read-only")
}

func (f *fakeIAM) PutRolePolicy(context.Context, *iam.PutRolePolicyInput, ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	f.writes++
	f.t.Error("doctor called PutRolePolicy — it must be strictly read-only")
	return nil, fmt.Errorf("read-only")
}

// fakeTables implements TableAPI over a set of present table names.
type fakeTables struct {
	present   map[string]bool
	err       error
	describes int
}

func (f *fakeTables) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.describes++
	if f.err != nil {
		return nil, f.err
	}
	if !f.present[aws.ToString(in.TableName)] {
		return nil, &dynamodbtypes.ResourceNotFoundException{}
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

// fakeInfra implements Infra with canned answers.
type fakeInfra struct {
	poller      deploy.PollerInfo
	pollerErr   error
	schedule    deploy.ScheduleInfo
	scheduleErr error
	topic       bool
	topicErr    error

	legacyStack  bool
	legacyStatus string
	legacyErr    error
}

func (f *fakeInfra) InspectPoller(context.Context) (deploy.PollerInfo, error) {
	return f.poller, f.pollerErr
}

func (f *fakeInfra) InspectPollerSchedule(context.Context) (deploy.ScheduleInfo, error) {
	return f.schedule, f.scheduleErr
}

func (f *fakeInfra) AlertsTopicPresent(context.Context, string, string) (bool, error) {
	return f.topic, f.topicErr
}

func (f *fakeInfra) LegacyStackState(context.Context, string) (bool, string, error) {
	return f.legacyStack, f.legacyStatus, f.legacyErr
}

// --- policy fixtures -------------------------------------------------------

// mutatePolicy round-trips a policy document through generic maps (which changes
// key order and whitespace) applying an optional statement mutation.
func mutatePolicy(t *testing.T, doc string, mutate func([]map[string]interface{}) []map[string]interface{}) string {
	t.Helper()
	var envelope struct {
		Version   string                   `json:"Version"`
		Statement []map[string]interface{} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &envelope); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if mutate != nil {
		envelope.Statement = mutate(envelope.Statement)
	}
	b, err := json.MarshalIndent(map[string]interface{}{
		"Version": envelope.Version, "Statement": envelope.Statement,
	}, "", "  ")
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	return string(b)
}

// policyWithout is this binary's policy minus one action — the "deployed policy
// predates the grant" fixture.
func policyWithout(t *testing.T, action string) string {
	t.Helper()
	return mutatePolicy(t, expectedPolicy(t), func(statements []map[string]interface{}) []map[string]interface{} {
		for _, st := range statements {
			dropAction(st, action)
		}
		return statements
	})
}

// policyPlus is this binary's policy plus an action it doesn't know about — the
// "a newer lagotto applied this" fixture.
func policyPlus(t *testing.T, action string) string {
	t.Helper()
	return mutatePolicy(t, expectedPolicy(t), func(statements []map[string]interface{}) []map[string]interface{} {
		return append(statements, map[string]interface{}{
			"Effect": "Allow", "Action": []interface{}{action}, "Resource": "*",
		})
	})
}

func dropAction(st map[string]interface{}, action string) {
	actions, ok := st["Action"].([]interface{})
	if !ok {
		return
	}
	kept := make([]interface{}, 0, len(actions))
	for _, a := range actions {
		if s, _ := a.(string); s == action {
			continue
		}
		kept = append(kept, a)
	}
	st["Action"] = kept
}
