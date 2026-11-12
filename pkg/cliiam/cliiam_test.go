package cliiam

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

var updateGolden = flag.Bool("update-cli-user-policy", false, "rewrite "+goldenPath)

// goldenPath is the policy for a fixed account and one config bucket,
// pretty-printed.
const goldenPath = "testdata/cli-user-policy.golden.json"

func goldenDoc(t *testing.T) string {
	t.Helper()
	doc, err := PolicyDocument(testAccount, []string{"my-config-bucket"})
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

// TestPolicyDocumentGolden pins the exact policy: any change to what a CLI user
// may do is a reviewed diff of the golden file. Regenerate with
// `go test ./pkg/cliiam -run TestPolicyDocumentGolden -args -update-cli-user-policy`.
func TestPolicyDocumentGolden(t *testing.T) {
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(goldenDoc(t)), "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, pretty.Bytes(), 0o644); err != nil { //nolint:gosec // a test fixture in the repository
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read %s: %v (generate it with -update-cli-user-policy)", goldenPath, err)
	}
	if !bytes.Equal(pretty.Bytes(), want) {
		t.Errorf("the CLI-user policy changed; review and regenerate %s with -update-cli-user-policy.\ngot:\n%s", goldenPath, pretty.String())
	}
}

// TestPolicyHasNoPolicyVariables: ${aws:PrincipalAccount} and friends in a
// Resource pass Access Analyzer and the simulator but fail a real
// CreatePolicy ("failed legacy parsing", spawn#1069).
//
// The one variable allowed is ${aws:userid}, and only in a Condition value
// (the owner-scoped item access), which IAM resolves per request and a real
// CreatePolicy accepts.
func TestPolicyHasNoPolicyVariables(t *testing.T) {
	doc := goldenDoc(t)
	for _, st := range parseStatements(t, doc) {
		if strings.Contains(string(st.Resource), "${") {
			t.Errorf("%s: a policy variable in a Resource: %s", st.Sid, st.Resource)
		}
	}
	if rest := strings.ReplaceAll(doc, "${aws:userid}", ""); strings.Contains(rest, "${") {
		t.Errorf("the CLI-user policy contains a policy variable other than ${aws:userid}:\n%s", doc)
	}
}

func TestPolicyDocumentRejectsABadAccount(t *testing.T) {
	for _, acct := range []string{"", "12345", "${aws:PrincipalAccount}", "*"} {
		if _, err := PolicyDocument(acct, nil); err == nil {
			t.Errorf("PolicyDocument(%q) succeeded", acct)
		}
	}
}

// TestPolicyFitsTheManagedPolicyLimit: IAM caps a managed policy at 6,144
// characters without whitespace; CreatePolicy fails past it. Leave room for a
// handful of config buckets.
func TestPolicyFitsTheManagedPolicyLimit(t *testing.T) {
	buckets := []string{"bucket-one", "bucket-two", "bucket-three", "bucket-four", "bucket-five"}
	doc, err := PolicyDocument(testAccount, buckets)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc) > maxManagedPolicyChars-1000 {
		t.Errorf("the policy is %d characters, within 1000 of IAM's %d limit", len(doc), maxManagedPolicyChars)
	}
}

type parsedStatement struct {
	Sid       string
	Action    []string
	Resource  json.RawMessage
	Condition map[string]map[string]json.RawMessage
}

func parseStatements(t *testing.T, doc string) []parsedStatement {
	t.Helper()
	var p struct{ Statement []parsedStatement }
	if err := json.Unmarshal([]byte(doc), &p); err != nil {
		t.Fatal(err)
	}
	return p.Statement
}

// TestConditionKeysMatchActionService: a condition key belongs to the ACTION's
// service. An ec2: key on an ssm: action never matches: a silent implicit deny
// that CreatePolicy accepts (spawn#665).
func TestConditionKeysMatchActionService(t *testing.T) {
	known := map[string]bool{"iam:PassedToService": true, "sns:Protocol": true, "ec2:ResourceTag/" + holdTagKey: true,
		"dynamodb:LeadingKeys": true}
	// The caller check models StringEquals, and the owner-scoped statements'
	// LeadingKeys operators through their own context (ownerScoped).
	ops := map[string]bool{"StringEquals": true, "ForAllValues:StringLike": true, "ForAllValues:StringEquals": true, "Null": true}
	n := 0
	for _, st := range parseStatements(t, goldenDoc(t)) {
		for op, kv := range st.Condition {
			if !ops[op] {
				t.Errorf("%s: condition operator %s is not one the caller check models", st.Sid, op)
			}
			if op != "StringEquals" {
				for key := range kv {
					if key != "dynamodb:LeadingKeys" {
						t.Errorf("%s: %s on %s; only dynamodb:LeadingKeys is owner-scoped", st.Sid, op, key)
					}
				}
			}
			for key := range kv {
				n++
				if !known[key] {
					t.Errorf("%s: condition key %q is not one this test knows; check it in the Service Authorization "+
						"Reference for %v and add it", st.Sid, key, st.Action)
				}
				svc, _, _ := strings.Cut(key, ":")
				for _, a := range st.Action {
					if svc != "aws" && !strings.HasPrefix(a, svc+":") {
						t.Errorf("%s: %s key %q gates %s, an action of another service", st.Sid, svc, key, a)
					}
				}
			}
		}
	}
	if n != 6 {
		t.Errorf("found %d condition keys, want 6 (PassRole, Subscribe, CancelCapacityReservation, and LeadingKeys x3)", n)
	}
}

// noResourceLevel are the only actions granted on Resource "*": IAM does not
// authorize them at the resource level.
var noResourceLevel = map[string]bool{
	"ec2:DescribeInstanceTypeOfferings": true, "ec2:DescribeInstances": true, "ec2:DescribeRegions": true,
	"ec2:DescribeSubnets": true, "ec2:DescribeCapacityReservations": true,
	"servicequotas:GetServiceQuota": true, "scheduler:ListSchedules": true,
	"s3:ListAllMyBuckets": true, "cloudwatch:GetMetricStatistics": true,
}

// TestWildcardResourcesAreOnlyWhereRequired: every other grant names lagotto's
// own resources in this account.
func TestWildcardResourcesAreOnlyWhereRequired(t *testing.T) {
	grants, err := runtimeiam.ParsePolicy(goldenDoc(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range grants {
		if g.Resource == "*" {
			if !noResourceLevel[g.Action] {
				t.Errorf("%s is granted on \"*\"; scope it, or add it to noResourceLevel with the reason", g.Action)
			}
			continue
		}
		if noResourceLevel[g.Action] {
			t.Errorf("%s is in noResourceLevel but granted on %s", g.Action, g.Resource)
		}
		if !strings.Contains(g.Resource, testAccount) && !strings.HasPrefix(g.Resource, "arn:aws:s3:::my-config-bucket/") {
			t.Errorf("%s on %s is not scoped to the account", g.Action, g.Resource)
		}
		if strings.HasSuffix(g.Resource, ":*") && !strings.Contains(g.Resource, alertsTopicName) {
			t.Errorf("%s on %s ends in a bare wildcard", g.Action, g.Resource)
		}
	}
}

// TestNoPrivilegeEscalation: the policy must never let a CLI user write IAM,
// launch or end instances, or pass a role anywhere but to Scheduler.
func TestNoPrivilegeEscalation(t *testing.T) {
	grants, err := runtimeiam.ParsePolicy(goldenDoc(t))
	if err != nil {
		t.Fatal(err)
	}
	forbidden := []string{"iam:Create", "iam:Put", "iam:Attach", "iam:Delete", "iam:Detach", "iam:Update", "iam:Tag",
		"iam:Add", "ec2:Run", "ec2:Terminate", "ec2:CreateCapacityReservation", "lambda:Update", "lambda:Create",
		"lambda:Invoke", "dynamodb:Delete", "sns:Publish", "sns:Create", "sns:Delete", "sns:Set"}
	for _, g := range grants {
		if strings.Contains(g.Action, "*") {
			t.Errorf("wildcard action %s", g.Action)
		}
		for _, f := range forbidden {
			if strings.HasPrefix(g.Action, f) {
				t.Errorf("the CLI-user policy grants %s on %s", g.Action, g.Resource)
			}
		}
		if g.Action == "iam:PassRole" {
			if g.Resource != runtimeiam.SchedulerInvokeRoleARN(testAccount) ||
				!strings.Contains(g.Condition, "iam:PassedToService") || !strings.Contains(g.Condition, "scheduler.amazonaws.com") {
				t.Errorf("iam:PassRole must be on the scheduler invoke role, to Scheduler only: %s when %s", g.Resource, g.Condition)
			}
		}
		// `cancel sl-…` deletes per-launch schedules (#279), and nothing else: never
		// the poller's own schedule, a group, or another schedule in the account.
		if strings.HasPrefix(g.Action, "scheduler:Delete") &&
			(g.Action != "scheduler:DeleteSchedule" ||
				g.Resource != "arn:aws:scheduler:*:"+testAccount+":schedule/default/"+launchSchedulePrefix+"*") {
			t.Errorf("the CLI-user policy grants %s on %s; only DeleteSchedule on the per-launch schedules is allowed", g.Action, g.Resource)
		}
		if g.Action == "ec2:CancelCapacityReservation" && !strings.Contains(g.Condition, "ec2:ResourceTag/lagotto:managed") {
			t.Errorf("ec2:CancelCapacityReservation is not gated on lagotto's tag: %q", g.Condition)
		}
		if g.Action == "sns:Subscribe" && !strings.Contains(g.Condition, `"email"`) {
			t.Errorf("sns:Subscribe is not limited to email: %q", g.Condition)
		}
	}
}

// TestNamesMatchTheirOwners: the names this package scopes to are owned
// elsewhere; a rename there must not silently narrow the policy to nothing.
func TestNamesMatchTheirOwners(t *testing.T) {
	if pollerFunctionName != deploy.PollerFunctionName {
		t.Errorf("poller function %q != deploy.PollerFunctionName %q", pollerFunctionName, deploy.PollerFunctionName)
	}
	if launchSchedulePrefix != deploy.LaunchSchedulePrefix {
		t.Errorf("launch schedule prefix %q != deploy.LaunchSchedulePrefix %q", launchSchedulePrefix, deploy.LaunchSchedulePrefix)
	}
	if b := deploy.DefaultBucketName(testAccount, "us-west-2"); !strings.HasPrefix(b, artifactBucketPrefix+testAccount+"-") {
		t.Errorf("artifact bucket %q does not match %s<account>-*", b, artifactBucketPrefix)
	}
	doc := goldenDoc(t)
	for _, want := range []string{"table/lagotto-watches", "table/lagotto-match-history", "table/lagotto-scheduled-launches",
		runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName, "schedule/default/lagotto-capacity-poller"} {
		if !strings.Contains(doc, want) {
			t.Errorf("the policy does not name %s", want)
		}
	}
}

func TestValidateConfigBucketName(t *testing.T) {
	for _, ok := range []string{"my-bucket", "a.b.c", "abc"} {
		if err := ValidateConfigBucketName(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "*", "my-*", "s3://b", "arn:aws:s3:::b", "b/key", "UPPER", "ab"} {
		if err := ValidateConfigBucketName(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// TestConfigBucketsRoundTrip: what setup writes, setup reads back, so a re-run
// carries the buckets forward instead of revoking them.
func TestConfigBucketsRoundTrip(t *testing.T) {
	doc, err := PolicyDocument(testAccount, []string{"zeta", " alpha ", "alpha", "bad*"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ConfigBuckets(doc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "alpha,zeta" {
		t.Errorf("ConfigBuckets = %v, want [alpha zeta]", got)
	}
	none, _ := PolicyDocument(testAccount, nil)
	if strings.Contains(none, "s3:GetObject") {
		t.Error("s3:GetObject is granted with no config bucket named")
	}
	if got, _ := ConfigBuckets(none); len(got) != 0 {
		t.Errorf("ConfigBuckets of a bucketless policy = %v", got)
	}
}

func TestGrantedActionsMatchesTheDocument(t *testing.T) {
	grants, err := runtimeiam.ParsePolicy(goldenDoc(t))
	if err != nil {
		t.Fatal(err)
	}
	want := runtimeiam.DistinctActions(grants)
	got := GrantedActions(testAccount, []string{"my-config-bucket"})
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("GrantedActions = %v\nwant %v", got, want)
	}
}

func TestPolicyARN(t *testing.T) {
	if got := PolicyARN(testAccount); got != "arn:aws:iam::123456789012:policy/lagotto-cli-user" {
		t.Errorf("PolicyARN = %s", got)
	}
}
