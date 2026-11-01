package doctor

import (
	"strings"
	"testing"
)

// regionPinnedPolicy is this binary's runtime policy with every any-region ARN
// pinned to region — what `lagotto setup` wrote before v0.65.1 (#219).
func regionPinnedPolicy(t *testing.T, region string) string {
	t.Helper()
	return strings.ReplaceAll(expectedPolicy(t), ":*:"+testAccount+":", ":"+region+":"+testAccount+":")
}

// TestEvaluate_RuntimePolicyRegionCoverage is the #219 doctor gate. The
// execution role is shared by every region's poller, so runtime-policy evaluates
// coverage for THIS region (testRegion is us-west-2): FAIL when this region's
// tables/topic/schedules are not covered, WARN when only this region is, PASS
// for the any-region policy this lagotto writes.
func TestEvaluate_RuntimePolicyRegionCoverage(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want Status
	}{
		{"pinned to another region (setup ran elsewhere)", regionPinnedPolicy(t, "us-east-1"), StatusFail},
		{"pinned to this region (an older setup ran here)", regionPinnedPolicy(t, testRegion), StatusWarn},
		{"the any-region policy this lagotto writes", expectedPolicy(t), StatusPass},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.doc == expectedPolicy(t) && tc.want != StatusPass {
				t.Fatal("fixture did not change the policy")
			}
			st := healthyState(t)
			st.PolicyDoc = tc.doc
			r := Evaluate(st)
			c := find(t, r, CheckRuntimePolicy)
			if c.Status != tc.want {
				t.Fatalf("runtime-policy = %s (%s), want %s; detail=%v", c.Status, c.Summary, tc.want, c.Detail)
			}
			detail := strings.Join(c.Detail, "\n")
			switch tc.want {
			case StatusFail:
				if !strings.Contains(detail, "dynamodb:") || !strings.Contains(detail, "lagotto#219") {
					t.Errorf("FAIL detail should name the missing DynamoDB grants and the cause:\n%s", detail)
				}
				if len(c.Fix) == 0 || !strings.HasPrefix(c.Fix[0], "lagotto setup") {
					t.Errorf("fix = %v, want lagotto setup", c.Fix)
				}
			case StatusWarn:
				if r.Failed() {
					t.Error("a policy that serves this region must not fail doctor here")
				}
				if strings.Contains(detail, "AHEAD") || strings.Contains(c.Summary, "AHEAD") {
					t.Errorf("the old region-pinned grants must not be reported as a newer lagotto's: %s / %s", c.Summary, detail)
				}
				if len(c.Fix) == 0 || !strings.HasPrefix(c.Fix[0], "lagotto setup") {
					t.Errorf("fix = %v, want lagotto setup", c.Fix)
				}
			case StatusPass:
				if r.PolicyDrift == nil || !r.PolicyDrift.Empty() {
					t.Errorf("drift = %+v, want empty", r.PolicyDrift)
				}
			}
		})
	}
}
