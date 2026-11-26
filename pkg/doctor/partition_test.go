package doctor

import (
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// TestEvaluate_PoliciesInEachPartition (lagotto#347): in aws-us-gov and
// aws-cn doctor compares the deployed policies with the ones for the region's
// partition: those pass, and the aws-partition ones (which name nothing there)
// fail as drift.
func TestEvaluate_PoliciesInEachPartition(t *testing.T) {
	for region, p := range map[string]string{"us-gov-west-1": awsarn.GovCloud, "cn-northwest-1": awsarn.China} {
		st := healthyState(t)
		st.Region = region
		st.PolicyDoc, _ = runtimeiam.PolicyDocument(p, testAccount, nil)
		st.InvokePolicyDoc, _ = runtimeiam.SchedulerInvokePolicyDocument(p, testAccount)
		st.Poller.RoleARN = runtimeiam.RoleARN(p, testAccount)
		r := Evaluate(st)
		for _, name := range []string{CheckRuntimePolicy, CheckInvokePolicy} {
			if c := find(t, r, name); c.Status != StatusPass {
				t.Errorf("%s: %s = %s: %s %v", region, name, c.Status, c.Summary, c.Detail)
			}
		}

		st.PolicyDoc = expectedPolicy(t) // the aws partition's
		st.InvokePolicyDoc = expectedInvokePolicy(t)
		r = Evaluate(st)
		c := find(t, r, CheckRuntimePolicy)
		if c.Status != StatusFail || !strings.Contains(strings.Join(c.Detail, "\n"), "arn:"+p+":dynamodb:*:"+testAccount+":table/"+runtimeiam.LifecycleTableName) {
			t.Errorf("%s: an aws-partition runtime policy = %s: %s %v", region, c.Status, c.Summary, c.Detail)
		}
		if c := find(t, r, CheckInvokePolicy); c.Status != StatusFail {
			t.Errorf("%s: an aws-partition invoke policy = %s: %s", region, c.Status, c.Summary)
		}
	}
}

// TestAttachFixInTheCallersPartition: the fix names lagotto-cli-user by its
// ARN in the caller's partition; with no principal, the aws one.
func TestAttachFixInTheCallersPartition(t *testing.T) {
	c := EvalCallerPermissions(cliiam.CallerResult{PrincipalARN: "arn:aws-us-gov:iam::111122223333:role/r",
		Status: cliiam.CallerMissing, Missing: []string{"dynamodb:GetItem on x"}}, "111122223333")
	if fix := strings.Join(c.Fix, "\n"); !strings.Contains(fix, "--role-name r --policy-arn arn:aws-us-gov:iam::111122223333:policy/lagotto-cli-user") {
		t.Errorf("GovCloud fix:\n%s", fix)
	}
	c = EvalCallerPermissions(cliiam.CallerResult{Status: cliiam.CallerMissing, Missing: []string{"dynamodb:GetItem on x"}}, "111122223333")
	if fix := strings.Join(c.Fix, "\n"); !strings.Contains(fix, "attach arn:aws:iam::111122223333:policy/lagotto-cli-user") {
		t.Errorf("no principal:\n%s", fix)
	}
}
