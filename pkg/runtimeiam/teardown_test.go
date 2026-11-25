package runtimeiam

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

var updateTeardownPolicyGolden = flag.Bool("update-teardown-policy", false, "rewrite "+teardownPolicyGolden)

// teardownPolicyGolden is SchedulerTeardownPolicyDocument for a fixed account,
// pretty-printed.
const teardownPolicyGolden = "testdata/teardown-policy.golden.json"

const tdAccount = "123456789012"

// TestSchedulerTeardownPolicyGolden pins the invoke role's teardown policy
// (#325). Regenerate with
// `go test ./pkg/runtimeiam -run TestSchedulerTeardownPolicyGolden -args -update-teardown-policy`.
func TestSchedulerTeardownPolicyGolden(t *testing.T) {
	doc, err := SchedulerTeardownPolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(doc), "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')
	if *updateTeardownPolicyGolden {
		if err := os.WriteFile(teardownPolicyGolden, pretty.Bytes(), 0o644); err != nil { //nolint:gosec // a test fixture in the repository
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(teardownPolicyGolden)
	if err != nil {
		t.Fatalf("read %s: %v (generate it with -update-teardown-policy)", teardownPolicyGolden, err)
	}
	if !bytes.Equal(pretty.Bytes(), want) {
		t.Errorf("the teardown policy changed; review and regenerate %s with -update-teardown-policy.\ngot:\n%s", teardownPolicyGolden, pretty.String())
	}
}

func TestSchedulerTeardownPolicyNeedsAccount(t *testing.T) {
	if _, err := SchedulerTeardownPolicyDocument(""); err == nil {
		t.Error("an empty account was accepted")
	}
}

// allowsOn returns, for each action the doc Allows, the sorted resources it is
// allowed on.
func allowsOn(t *testing.T, doc string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, g := range mustGrants(t, doc) {
		if strings.EqualFold(g.Effect, "Allow") {
			out[g.Action] = append(out[g.Action], g.Resource)
		}
	}
	for a := range out {
		sort.Strings(out[a])
	}
	return out
}

func hasAll(have []string, want ...string) bool {
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

// TestPollerTeardownGrants (#325): every grant the poller needs to tear itself
// down, on exactly lagotto's resources.
func TestPollerTeardownGrants(t *testing.T) {
	doc, err := PolicyDocument(tdAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := allowsOn(t, doc)
	table := func(n string) string { return RegionalARN("dynamodb", tdAccount, "table/"+n) }
	tables := []string{table("lagotto-watches"), table("lagotto-match-history"), table("lagotto-scheduled-launches")}
	lease := table("spore-host-lifecycle")
	bucket := "arn:aws:s3:::lagotto-lambda-" + tdAccount + "-*"
	topic := RegionalARN("sns", tdAccount, "lagotto-capacity-alerts")

	want := map[string][]string{
		"dynamodb:DeleteTable":        append(append([]string{}, tables...), lease),
		"dynamodb:ListTagsOfResource": append(append([]string{}, tables...), lease),
		"dynamodb:DescribeTable":      append(append([]string{}, tables...), lease),
		"dynamodb:CreateTable":        {lease},
		"dynamodb:TagResource":        {lease},
		"dynamodb:GetItem":            {lease},
		"dynamodb:PutItem":            {lease},
		"dynamodb:UpdateItem":         {lease},
		"dynamodb:DeleteItem":         {lease},
		"dynamodb:ConditionCheckItem": {lease},
		"dynamodb:Scan":               {lease},
		"s3:ListBucketVersions":       {bucket},
		"s3:DeleteBucket":             {bucket},
		"s3:DeleteObject":             {bucket + "/*"},
		"s3:DeleteObjectVersion":      {bucket + "/*"},
		"sns:DeleteTopic":             {topic},
		"scheduler:DeleteSchedule": {
			RegionalARN("scheduler", tdAccount, "schedule/lagotto/*"),
			RegionalARN("scheduler", tdAccount, "schedule/lagotto-teardown/*"),
		},
		"scheduler:DeleteScheduleGroup": {RegionalARN("scheduler", tdAccount, "schedule-group/lagotto")},
		"scheduler:CreateSchedule":      {RegionalARN("scheduler", tdAccount, "schedule/lagotto-teardown/*")},
		"scheduler:CreateScheduleGroup": {RegionalARN("scheduler", tdAccount, "schedule-group/lagotto-teardown")},
		"scheduler:TagResource":         {RegionalARN("scheduler", tdAccount, "schedule-group/lagotto-teardown")},
	}
	for action, resources := range want {
		if !hasAll(got[action], resources...) {
			t.Errorf("%s is granted on %v, want at least %v", action, got[action], resources)
		}
	}

	// The destructive ones reach nothing else.
	exact := map[string]bool{
		"dynamodb:DeleteTable": true, "dynamodb:CreateTable": true, "s3:DeleteBucket": true, "s3:DeleteObject": true,
		"s3:DeleteObjectVersion": true, "sns:DeleteTopic": true, "scheduler:DeleteScheduleGroup": true,
		"scheduler:CreateScheduleGroup": true,
	}
	for action := range exact {
		w := append([]string{}, want[action]...)
		sort.Strings(w)
		if fmt.Sprint(got[action]) != fmt.Sprint(w) {
			t.Errorf("%s is granted on %v, want exactly %v", action, got[action], w)
		}
	}
	// PassRole for the teardown jobs is the existing grant: the invoke role
	// only, to the Scheduler service only.
	for _, g := range mustGrants(t, doc) {
		if g.Action == "iam:PassRole" && g.Resource == SchedulerInvokeRoleARN(tdAccount) &&
			!strings.Contains(g.Condition, `"scheduler.amazonaws.com"`) {
			t.Errorf("PassRole on the invoke role lost its Scheduler condition: %s", g)
		}
	}
}

// TestPollerCannotDeleteAnythingAccountWide: no IAM role, profile or policy
// of lagotto's is in reach of the poller's IAM writes, nor of its teardown
// jobs' (the invoke role's) (lagotto#340): the CLI removes them.
func TestPollerCannotDeleteAnythingAccountWide(t *testing.T) {
	doc, err := PolicyDocument(tdAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := SchedulerTeardownPolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	iamARN := func(kind, name string) string { return "arn:aws:iam::" + tdAccount + ":" + kind + "/" + name }
	targets := []string{RoleARN(tdAccount), SchedulerInvokeRoleARN(tdAccount), iamARN("role", InstanceRoleName),
		iamARN("instance-profile", InstanceRoleName), iamARN("policy", "lagotto-cli-user")}
	for who, d := range map[string]string{"the poller": doc, "the teardown jobs": jobs} {
		grants := mustGrants(t, d)
		for _, r := range targets {
			for _, a := range []string{"iam:DeleteRole", "iam:DeleteRolePolicy", "iam:DetachRolePolicy", "iam:PutRolePolicy",
				"iam:RemoveRoleFromInstanceProfile", "iam:DeleteInstanceProfile", "iam:DeletePolicy", "iam:DeletePolicyVersion"} {
				if Allows(grants, a, r) {
					t.Errorf("%s is allowed %s on %s", who, a, r)
				}
			}
		}
	}
}

// TestSchedulerTeardownGrants: the invoke role's teardown policy deletes the
// poller function and its log group, and nothing else.
func TestSchedulerTeardownGrants(t *testing.T) {
	doc, err := SchedulerTeardownPolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	got := allowsOn(t, doc)
	logGroup := "arn:aws:logs:*:" + tdAccount + ":log-group:/aws/lambda/lagotto-capacity-poller"
	want := map[string][]string{
		"lambda:DeleteFunction": {"arn:aws:lambda:*:" + tdAccount + ":function:lagotto-capacity-poller"},
		"logs:DeleteLogGroup":   {logGroup, logGroup + ":*"},
	}
	if len(got) != len(want) {
		t.Errorf("the teardown policy allows %v, want exactly %v", got, want)
	}
	for action, resources := range want {
		if fmt.Sprint(got[action]) != fmt.Sprint(resources) {
			t.Errorf("%s is granted on %v, want exactly %v", action, got[action], resources)
		}
	}
	if Allows(mustGrants(t, doc), "iam:DeleteRole", SchedulerInvokeRoleARN(tdAccount)) {
		t.Error("the teardown policy lets the invoke role delete itself")
	}
}

// TestGuardDeniesOnBothRoles (#325): both roles carry the explicit Deny on
// spore-host:role=service resources and on writes to spawn's permissions
// boundary, with a key of the global aws: namespace (valid on every service).
func TestGuardDeniesOnBothRoles(t *testing.T) {
	runtime, err := PolicyDocumentFor(tdAccount, Authorizations{InstanceRoles: []string{"my-role"}})
	if err != nil {
		t.Fatal(err)
	}
	teardown, err := SchedulerTeardownPolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	boundary := "arn:aws:iam::" + tdAccount + ":policy/spore-host/spawn-instance-boundary"
	for name, doc := range map[string]string{RoleName: runtime, SchedulerInvokeRoleName + "/" + SchedulerTeardownPolicyName: teardown} {
		var service, boundaryWrites int
		for _, g := range mustGrants(t, doc) {
			if !strings.EqualFold(g.Effect, "Deny") {
				continue
			}
			switch {
			case isServiceGuard(g):
				service++
				if g.Condition != `{"StringEquals":{"aws:ResourceTag/spore-host:role":"service"}}` {
					t.Errorf("%s: service guard condition = %s", name, g.Condition)
				}
			case g.Resource == boundary && g.Condition == "":
				boundaryWrites++
			default:
				t.Errorf("%s: unexpected Deny %s", name, g)
			}
		}
		if service != 1 {
			t.Errorf("%s: %d spore-host:role=service Deny statements, want 1", name, service)
		}
		grants := mustGrants(t, doc)
		for _, a := range []string{"iam:CreatePolicyVersion", "iam:DeletePolicy", "iam:DeletePolicyVersion",
			"iam:SetDefaultPolicyVersion", "iam:TagPolicy", "iam:UntagPolicy"} {
			if !deniedOn(grants, a, boundary) {
				t.Errorf("%s: %s on the boundary is not denied", name, a)
			}
		}
		if deniedOn(grants, "iam:GetPolicy", boundary) || deniedOn(grants, "iam:GetPolicyVersion", boundary) {
			t.Errorf("%s: reading the boundary is denied; only writes should be", name)
		}
		if boundaryWrites == 0 {
			t.Errorf("%s: no boundary Deny", name)
		}
	}
}

// deniedOn reports whether an unconditioned Deny in grants matches.
func deniedOn(grants []Grant, action, resource string) bool {
	for _, g := range grants {
		if strings.EqualFold(g.Effect, "Deny") && g.Condition == "" &&
			wildcardMatchFold(g.Action, action) && wildcardMatch(g.Resource, resource) {
			return true
		}
	}
	return false
}

// TestServiceGuardDoesNotBlindTheEvaluators: the guard is Action "*" on
// Resource "*", so read as an unconditional Deny it would make doctor and
// watch's role check report every grant missing. It only matches resources
// lagotto never tags; any other Deny still wins (the revert case).
func TestServiceGuardDoesNotBlindTheEvaluators(t *testing.T) {
	doc, err := PolicyDocument(tdAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	grants := mustGrants(t, doc)
	table := RegionalARN("dynamodb", tdAccount, "table/lagotto-watches")
	if !Allows(grants, "dynamodb:PutItem", table) {
		t.Error("the service guard is read as denying lagotto's own table")
	}
	if missing := MissingInstanceRoleGrants(grants, tdAccount, InstanceRoleName); len(missing) != 0 {
		t.Errorf("the service guard blocks passing lagotto's instance role: %v", missing)
	}
	diff, err := DiffRuntimePolicy(doc, "us-east-1", tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	if !diff.Empty() {
		t.Errorf("the policy drifts from itself: missing %v, extra %v", diff.MissingSummary(), diff.ExtraSummary())
	}

	other := append(append([]Grant{}, grants...), Grant{Effect: "Deny", Action: "*", Resource: "*",
		Condition: `{"StringEquals":{"aws:ResourceTag/other":"x"}}`})
	if Allows(other, "dynamodb:PutItem", table) {
		t.Error("a Deny that is not the service guard no longer wins")
	}
}

// maxRoleInlineChars is IAM's limit on the combined size of a role's inline
// policies, counted without whitespace.
const maxRoleInlineChars = 10240

// TestRolePoliciesFitTheInlineLimit: PutRolePolicy fails once a role's inline
// policies together pass 10,240 characters. The poller role, with a generous
// set of authorizations, and the invoke role's two policies, each leave room.
func TestRolePoliciesFitTheInlineLimit(t *testing.T) {
	var roles, sm []string
	for i := 0; i < 10; i++ {
		roles = append(roles, fmt.Sprintf("authorized-instance-role-%02d", i))
		sm = append(sm, fmt.Sprintf("arn:aws:iam::%s:role/service-role/sagemaker-execution-%02d", tdAccount, i))
	}
	runtime, err := PolicyDocumentFor(tdAccount, Authorizations{InstanceRoles: roles, SageMakerRoles: sm})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(runtime); n > maxRoleInlineChars-1500 {
		t.Errorf("the runtime policy is %d characters with 10 instance and 10 SageMaker roles, within 1500 of IAM's %d", n, maxRoleInlineChars)
	}
	invoke, err := SchedulerInvokePolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	teardown, err := SchedulerTeardownPolicyDocument(tdAccount)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(invoke) + len(teardown); n > maxRoleInlineChars-1500 {
		t.Errorf("the invoke role's policies are %d characters, within 1500 of IAM's %d", n, maxRoleInlineChars)
	}
	t.Logf("runtime policy %d chars; invoke role %d + %d chars", len(runtime), len(invoke), len(teardown))
}
