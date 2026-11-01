package runtimeiam

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// regionalResources are lagotto's fixed-name regional resources and the
// actions the poller performs on each, as concrete ARNs in one region.
func regionalResources(region, account string) map[string][]string {
	a := func(svc, res string) string { return fmt.Sprintf("arn:aws:%s:%s:%s:%s", svc, region, account, res) }
	ddb := []string{"dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
		"dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem",
		"dynamodb:DescribeTable", "dynamodb:ConditionCheckItem"}
	return map[string][]string{
		a("dynamodb", "table/lagotto-watches"):                        ddb,
		a("dynamodb", "table/lagotto-watches/index/by-user"):          ddb,
		a("dynamodb", "table/lagotto-match-history"):                  ddb,
		a("dynamodb", "table/lagotto-match-history/index/x"):          ddb,
		a("dynamodb", "table/lagotto-scheduled-launches"):             ddb,
		a("dynamodb", "table/lagotto-scheduled-launches/index/x"):     ddb,
		a("sns", "lagotto-capacity-alerts"):                           {"sns:Publish"},
		a("scheduler", "schedule/default/lagotto-capacity-poller"):    {"scheduler:UpdateSchedule", "scheduler:GetSchedule"},
		a("scheduler", "schedule/default/lagotto-launch-sl-1234abcd"): {"scheduler:CreateSchedule", "scheduler:DeleteSchedule"},
	}
}

// TestEnsureRuntimeRole_PolicyConvergesAcrossRegions is the #219 gate, through
// the real setup write path. The execution role is IAM-global and shared by
// every region's poller, so `setup` in A, then B, then C must write the same
// document, and that document must serve every region's tables, topic and
// schedules — while still reaching nothing outside lagotto's names and account.
func TestEnsureRuntimeRole_PolicyConvergesAcrossRegions(t *testing.T) {
	const account = "123456789012"
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	roles := []string{"my-gpu-role"} // operator-authorized roles ride along unchanged
	key := RoleName + "/" + PolicyName

	f := &fakeIAM{}
	var written []string
	for _, region := range regions {
		if err := EnsureRuntimeRole(context.Background(), f, region, account, roles); err != nil {
			t.Fatalf("EnsureRuntimeRole(%s): %v", region, err)
		}
		written = append(written, f.inline[key])
	}
	for i := 1; i < len(written); i++ {
		if written[i] != written[0] {
			t.Errorf("the runtime policy depends on the region setup ran in — %s and %s wrote different documents:\n  %s\n  %s",
				regions[0], regions[i], written[0], written[i])
		}
	}

	grants, err := ParsePolicy(written[len(written)-1])
	if err != nil {
		t.Fatal(err)
	}
	for _, region := range regions {
		for res, actions := range regionalResources(region, account) {
			for _, act := range actions {
				if !Allows(grants, act, res) {
					t.Errorf("after setup in %v, the %s poller may not %s on %s", regions, region, act, res)
				}
			}
		}
		// Every region's diff is clean: doctor agrees from each region.
		diff, err := DiffRuntimePolicy(written[len(written)-1], region, account)
		if err != nil {
			t.Fatal(err)
		}
		if !diff.Empty() {
			t.Errorf("doctor in %s reports drift on the policy setup just wrote: missing=%v extra=%v regionOnly=%v",
				region, diff.MissingSummary(), diff.ExtraSummary(), Summarize(diff.RegionOnly))
		}
	}
	if missing := MissingInstanceRoleGrants(grants, account, "my-gpu-role"); len(missing) != 0 {
		t.Errorf("the authorized instance role lost %v", missing)
	}

	// Still name- and account-scoped.
	for _, c := range []struct{ act, res string }{
		{"dynamodb:PutItem", "arn:aws:dynamodb:us-east-1:999999999999:table/lagotto-watches"},
		{"dynamodb:PutItem", "arn:aws:dynamodb:us-east-1:123456789012:table/someone-elses-table"},
		{"sns:Publish", "arn:aws:sns:us-east-1:123456789012:other-topic"},
		{"sns:Publish", "arn:aws:sns:us-east-1:999999999999:lagotto-capacity-alerts"},
		{"scheduler:UpdateSchedule", "arn:aws:scheduler:us-east-1:123456789012:schedule/default/other"},
		{"scheduler:DeleteSchedule", "arn:aws:scheduler:us-east-1:123456789012:schedule/default/other"},
		{"scheduler:CreateSchedule", "arn:aws:scheduler:us-east-1:999999999999:schedule/default/lagotto-launch-x"},
	} {
		if Allows(grants, c.act, c.res) {
			t.Errorf("the runtime policy is broader than lagotto's own resources: it allows %s on %s", c.act, c.res)
		}
	}
}

// TestDiffRuntimePolicy_SingleRegionPolicy: what a pre-v0.65.1 `setup` wrote.
// Seen from the region it was written in, it covers this region only
// (RegionOnly, not Missing); seen from any other region, the regional grants
// are Missing. Neither side reports the old concrete-region grants as Extra:
// re-running setup loses nothing.
func TestDiffRuntimePolicy_SingleRegionPolicy(t *testing.T) {
	const account = "123456789012"
	wildcard, err := PolicyDocument(account, nil)
	if err != nil {
		t.Fatal(err)
	}
	old := regionPinned(t, wildcard, "us-west-2")

	here, err := DiffRuntimePolicy(old, "us-west-2", account)
	if err != nil {
		t.Fatal(err)
	}
	if len(here.Missing) != 0 || len(here.Extra) != 0 || len(here.RegionOnly) == 0 {
		t.Errorf("from us-west-2: missing=%d extra=%d regionOnly=%d, want 0/0/>0 (%v)",
			len(here.Missing), len(here.Extra), len(here.RegionOnly), here.ExtraSummary())
	}

	other, err := DiffRuntimePolicy(old, "us-east-1", account)
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Missing) == 0 || len(other.Extra) != 0 || len(other.RegionOnly) != 0 {
		t.Errorf("from us-east-1: missing=%d extra=%d regionOnly=%d, want >0/0/0", len(other.Missing), len(other.Extra), len(other.RegionOnly))
	}
	required, _ := PartitionByCriticality(other.Missing)
	if len(required) == 0 {
		t.Error("us-east-1's poller losing its DynamoDB grants must count as required")
	}
}

// regionPinned rewrites every any-region ARN in doc to region — the
// pre-v0.65.1 shape of the policy.
func regionPinned(t *testing.T, doc, region string) string {
	t.Helper()
	grants, err := ParsePolicy(doc)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, g := range grants {
		res := g.Resource
		if pinned, ok := arnInRegion(res, region); ok {
			res = pinned
		}
		cond := ""
		if g.Condition != "" {
			cond = `,"Condition":` + g.Condition
		}
		out = append(out, fmt.Sprintf(`{"Effect":%q,"Action":%q,"Resource":%q%s}`, g.Effect, g.Action, res, cond))
	}
	return `{"Version":"2012-10-17","Statement":[` + strings.Join(out, ",") + `]}`
}
