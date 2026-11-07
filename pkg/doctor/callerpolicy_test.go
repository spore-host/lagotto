package doctor

import (
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/cliiam"
)

func TestEvalCallerPermissions(t *testing.T) {
	ns := []string{"ec2:CancelCapacityReservation", "sns:Unsubscribe"}

	c := EvalCallerPermissions(cliiam.CallerResult{PrincipalARN: "arn:aws:iam::1:role/r", Status: cliiam.CallerAllowed, Checked: 40, NotSimulated: ns}, "1")
	if c.Status != StatusPass {
		t.Fatalf("allowed → %s, want PASS", c.Status)
	}
	if len(c.Detail) != 1 || !strings.Contains(c.Detail[0], "not simulated: ec2:CancelCapacityReservation, sns:Unsubscribe") {
		t.Errorf("PASS detail = %q, want the not-simulated line", c.Detail)
	}

	c = EvalCallerPermissions(cliiam.CallerResult{PrincipalARN: "arn:aws:iam::1:role/r", Status: cliiam.CallerMissing, Missing: []string{"dynamodb:GetItem on x"}, NotSimulated: ns}, "1")
	if c.Status != StatusWarn || c.Detail[0] != "dynamodb:GetItem on x" || !strings.HasPrefix(c.Detail[len(c.Detail)-1], "not simulated:") {
		t.Errorf("missing → %+v", c)
	}

	c = EvalCallerPermissions(cliiam.CallerResult{Status: cliiam.CallerUnknown, Reason: "denied", NotSimulated: ns}, "1")
	if c.Status != StatusWarn || len(c.Detail) != 0 || !strings.HasPrefix(c.Summary, "unknown:") {
		t.Errorf("unknown → %+v (must not claim anything was checked)", c)
	}
}
