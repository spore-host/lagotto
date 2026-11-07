package cliiam

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/smithy-go"
)

// SimulateAPI is the slice of the IAM API [CheckCaller] needs. *iam.Client
// satisfies it. Both calls are reads.
type SimulateAPI interface {
	SimulatePrincipalPolicy(ctx context.Context, in *iam.SimulatePrincipalPolicyInput, optFns ...func(*iam.Options)) (*iam.SimulatePrincipalPolicyOutput, error)
	GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
}

// CallerStatus is the outcome of the caller check.
type CallerStatus string

const (
	// CallerAllowed: the simulator allows every everyday action.
	CallerAllowed CallerStatus = "allowed"
	// CallerMissing: the simulator denies at least one.
	CallerMissing CallerStatus = "missing"
	// CallerUnknown: the check could not be made (for instance the caller may
	// not call iam:SimulatePrincipalPolicy). Nothing is known to be missing.
	CallerUnknown CallerStatus = "unknown"
)

// CallerResult is what [CheckCaller] found.
type CallerResult struct {
	// CallerARN is the identity STS reported.
	CallerARN string `json:"caller_arn"`
	// PrincipalARN is the IAM principal simulated: for an assumed role, the
	// role, not the STS session.
	PrincipalARN string       `json:"principal_arn,omitempty"`
	Status       CallerStatus `json:"status"`
	// Missing are the denied grants, "action on resource", sorted.
	Missing []string `json:"missing,omitempty"`
	// Reason explains an unknown result.
	Reason string `json:"reason,omitempty"`
	// Checked is the number of action/resource pairs simulated.
	Checked int `json:"checked"`
}

// errNotSimulable is a caller identity IAM cannot simulate.
var errNotSimulable = errors.New("not an IAM user or role")

// PrincipalARN converts the ARN STS reports for the caller into the IAM
// principal ARN SimulatePrincipalPolicy takes. roleName is set for an assumed
// role, whose role path the session ARN does not carry:
//
//	arn:aws:sts::123456789012:assumed-role/Dev/alice -> arn:aws:iam::123456789012:role/Dev
//	arn:aws:iam::123456789012:user/ops/bob           -> unchanged
//
// The account root user and federated users are not simulable.
func PrincipalARN(callerARN string) (arn, roleName string, err error) {
	parts := strings.SplitN(callerARN, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" {
		return "", "", fmt.Errorf("%q is not an ARN", callerARN)
	}
	partition, service, account, resource := parts[1], parts[2], parts[4], parts[5]
	switch {
	case service == "sts" && strings.HasPrefix(resource, "assumed-role/"):
		name, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
		if name == "" {
			return "", "", fmt.Errorf("%q names no role", callerARN)
		}
		return fmt.Sprintf("arn:%s:iam::%s:role/%s", partition, account, name), name, nil
	case service == "iam" && (strings.HasPrefix(resource, "user/") || strings.HasPrefix(resource, "role/")):
		return callerARN, "", nil
	case service == "iam" && resource == "root":
		return "", "", fmt.Errorf("the account root user: %w (it is allowed everything an SCP does not deny)", errNotSimulable)
	default:
		return "", "", fmt.Errorf("%s: %w", callerARN, errNotSimulable)
	}
}

// simRequest is one SimulatePrincipalPolicy call: a statement's actions against
// concrete stand-ins for its resources, with its conditions as context.
type simRequest struct {
	actions []string
	// resources maps each concrete resource simulated to the policy pattern it
	// stands for (which is what a report names). Empty means "*".
	resources map[string]string
	context   []iamtypes.ContextEntry
}

// probeName stands in for a `*` in a resource pattern.
const probeName = "lagotto-doctor-probe"

// concreteARN turns a policy resource pattern into an ARN the simulator can
// evaluate: a `*` region becomes the caller's region, any other `*` a probe
// name. (The simulator treats a `*` in a request resource literally.)
func concreteARN(pattern, region string) string {
	parts := strings.SplitN(pattern, ":", 6)
	if len(parts) == 6 && parts[3] == "*" {
		parts[3] = region
	}
	return strings.ReplaceAll(strings.Join(parts, ":"), "*", probeName)
}

// simRequests builds the caller check's requests from the policy's statements,
// so what doctor checks is exactly what setup grants.
func simRequests(accountID, region string) []simRequest {
	var out []simRequest
	for _, st := range statements(accountID, nil) {
		if st.noSimulate {
			continue
		}
		patterns := st.sim
		if len(patterns) == 0 {
			switch r := st.Resource.(type) {
			case string:
				patterns = []string{r}
			case []string:
				patterns = r
			}
		}
		req := simRequest{actions: st.Action, resources: map[string]string{}}
		for _, p := range patterns {
			if p != "*" {
				req.resources[concreteARN(p, region)] = p
			}
		}
		if cond, ok := st.Condition.(map[string]map[string]string); ok {
			keys := make([]string, 0, len(cond["StringEquals"]))
			for k := range cond["StringEquals"] {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				req.context = append(req.context, iamtypes.ContextEntry{
					ContextKeyName: aws.String(k), ContextKeyType: iamtypes.ContextKeyTypeEnumString,
					ContextKeyValues: []string{cond["StringEquals"][k]},
				})
			}
		}
		out = append(out, req)
	}
	return out
}

func apiErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func isAccessDenied(err error) bool {
	switch apiErrorCode(err) {
	case "AccessDenied", "AccessDeniedException", "UnauthorizedOperation":
		return true
	}
	return false
}

// CheckCaller simulates the caller (callerARN, as STS reports it) against
// every action and resource the CLI-user policy grants, in region, and
// reports what is denied. It never fails: a check that cannot be made is
// [CallerUnknown] with the reason, in particular when the caller may not call
// iam:SimulatePrincipalPolicy, which says nothing about the other actions.
//
// For an assumed role it simulates the role. The session ARN carries no role
// path, so when IAM does not know the path-less ARN (a role under a path, such
// as every IAM Identity Center role) it reads the role's real ARN with
// iam:GetRole and retries once.
//
// The simulator is a model, not IAM itself: it ignores session policies, and it
// has mis-evaluated tag and SNS conditions before. A "missing" here is a lead
// to check, not proof.
func CheckCaller(ctx context.Context, client SimulateAPI, callerARN, accountID, region string) CallerResult {
	res := CallerResult{CallerARN: callerARN, Status: CallerUnknown}
	principal, roleName, err := PrincipalARN(callerARN)
	if err != nil {
		res.Reason = fmt.Sprintf("cannot simulate %s", err)
		return res
	}
	res.PrincipalARN = principal

	missing, checked, err := simulateAll(ctx, client, principal, accountID, region)
	if err != nil && roleName != "" && isNoSuchEntity(err) {
		role, gerr := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
		if gerr != nil || role.Role == nil {
			res.Reason = fmt.Sprintf("IAM does not know %s (the role may have a path the session ARN omits), "+
				"and its real ARN could not be read with iam:GetRole: %v", principal, gerr)
			return res
		}
		principal = aws.ToString(role.Role.Arn)
		res.PrincipalARN = principal
		missing, checked, err = simulateAll(ctx, client, principal, accountID, region)
	}
	res.Checked = checked
	switch {
	case err != nil && isAccessDenied(err):
		res.Reason = fmt.Sprintf("not permitted to call iam:SimulatePrincipalPolicy on %s, so the caller's permissions "+
			"are unknown (the %s policy grants it): %v", principal, PolicyName, err)
		return res
	case err != nil:
		res.Reason = fmt.Sprintf("could not simulate %s: %v", principal, err)
		return res
	}
	res.Missing = missing
	if len(missing) == 0 {
		res.Status = CallerAllowed
	} else {
		res.Status = CallerMissing
	}
	return res
}

// simulateAll runs every request and returns the denied "action on pattern"
// grants, sorted, and how many pairs were evaluated.
func simulateAll(ctx context.Context, client SimulateAPI, principal, accountID, region string) ([]string, int, error) {
	seen := map[string]bool{}
	checked := 0
	for _, req := range simRequests(accountID, region) {
		in := &iam.SimulatePrincipalPolicyInput{
			PolicySourceArn: aws.String(principal),
			ActionNames:     req.actions,
			ContextEntries:  req.context,
		}
		for r := range req.resources {
			in.ResourceArns = append(in.ResourceArns, r)
		}
		sort.Strings(in.ResourceArns)
		pattern := func(r string) string {
			if p, ok := req.resources[r]; ok {
				return p
			}
			return "*"
		}
		for pg := iam.NewSimulatePrincipalPolicyPaginator(client, in); pg.HasMorePages(); {
			page, err := pg.NextPage(ctx)
			if err != nil {
				return nil, checked, err
			}
			for _, ev := range page.EvaluationResults {
				action := aws.ToString(ev.EvalActionName)
				if len(ev.ResourceSpecificResults) > 0 {
					for _, rr := range ev.ResourceSpecificResults {
						checked++
						if rr.EvalResourceDecision != iamtypes.PolicyEvaluationDecisionTypeAllowed {
							seen[action+" on "+pattern(aws.ToString(rr.EvalResourceName))] = true
						}
					}
					continue
				}
				checked++
				if ev.EvalDecision != iamtypes.PolicyEvaluationDecisionTypeAllowed {
					seen[action+" on "+pattern(aws.ToString(ev.EvalResourceName))] = true
				}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, checked, nil
}
