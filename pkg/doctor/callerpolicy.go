package doctor

import (
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/cliiam"
)

// CheckCallerPermissions names the caller check (#232).
const CheckCallerPermissions = "caller-permissions"

// EvalCallerPermissions reports whether the identity running doctor has what
// the everyday commands need, as the IAM policy simulator judges it against
// the lagotto-cli-user policy's actions and resources. Pure, like Evaluate.
//
// Every outcome is WARN at most. It describes the caller's credentials, not
// the account's lagotto deployment (an administrator may run doctor with
// credentials that are not a watch user's), and the simulator is a model:
// each finding is a lead. An unknown result (the caller may not simulate, the
// identity is the root user, ...) is a WARN that says so, never a list of
// missing actions.
func EvalCallerPermissions(res cliiam.CallerResult, accountID string) Check {
	c := Check{Name: CheckCallerPermissions}
	who := res.PrincipalARN
	if who == "" {
		who = res.CallerARN
	}
	switch res.Status {
	case cliiam.CallerAllowed:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("%s may run the everyday commands (%d action/resource pairs simulated)", who, res.Checked)
	case cliiam.CallerMissing:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("%s lacks %d permission(s) the everyday commands need (IAM policy simulator)", who, len(res.Missing))
		c.Detail = append(c.Detail, res.Missing...)
		c.Fix = attachFix(res.PrincipalARN, accountID)
	default:
		c.Status = StatusWarn
		c.Summary = "unknown: " + res.Reason
		return c
	}
	if len(res.NotSimulated) > 0 {
		c.Detail = append(c.Detail, fmt.Sprintf("not simulated: %s (%s)",
			strings.Join(res.NotSimulated, ", "), cliiam.SimulatorBlindSpot))
	}
	return c
}

// attachFix is how an administrator gives principal the policy.
func attachFix(principal, accountID string) []string {
	arn := cliiam.PolicyARN(accountID)
	setup := "if the policy does not exist yet, an administrator runs: lagotto setup"
	_, resource, _ := strings.Cut(principal, ":iam::"+accountID+":")
	name := resource[strings.LastIndex(resource, "/")+1:]
	switch {
	case strings.HasPrefix(resource, "user/"):
		return []string{fmt.Sprintf("aws iam attach-user-policy --user-name %s --policy-arn %s", name, arn), setup}
	case strings.HasPrefix(resource, "role/aws-reserved/sso.amazonaws.com/"), strings.HasPrefix(name, "AWSReservedSSO_"):
		return []string{fmt.Sprintf("in IAM Identity Center, add the customer managed policy %s to this permission set", cliiam.PolicyName), setup}
	case strings.HasPrefix(resource, "role/"):
		return []string{fmt.Sprintf("aws iam attach-role-policy --role-name %s --policy-arn %s", name, arn), setup}
	}
	return []string{fmt.Sprintf("attach %s to this identity's group, user or role", arn), setup}
}
