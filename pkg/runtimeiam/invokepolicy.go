package runtimeiam

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// ErrInvokeRoleNotFound / ErrInvokePolicyNotFound are the two definitive
// "absent" answers from [ReadSchedulerInvokePolicy], told apart for the same
// reason as ErrRoleNotFound / ErrPolicyNotFound: IAM answers both with
// NoSuchEntity, and the remediation text differs.
var (
	ErrInvokeRoleNotFound   = errors.New("runtimeiam: scheduler invoke role not found")
	ErrInvokePolicyNotFound = errors.New("runtimeiam: invoke policy not attached to the scheduler invoke role")
)

// ReadSchedulerInvokePolicy returns the inline InvokeLambda policy on the
// Scheduler invoke role, decoded to plain JSON. Read-only (iam:GetRolePolicy, plus
// iam:GetRole only to tell a missing role from a missing policy). Any error other
// than a definitive absence is returned as-is: an AccessDenied must never be
// reported as "the policy isn't there".
func ReadSchedulerInvokePolicy(ctx context.Context, client IAMAPI) (string, error) {
	out, err := client.GetRolePolicy(ctx, &iam.GetRolePolicyInput{
		RoleName:   aws.String(SchedulerInvokeRoleName),
		PolicyName: aws.String(SchedulerInvokePolicyName),
	})
	if err == nil {
		return decodePolicyDocument(aws.ToString(out.PolicyDocument)), nil
	}
	var notFound *iamtypes.NoSuchEntityException
	if !errors.As(err, &notFound) {
		return "", fmt.Errorf("runtimeiam: get inline policy %s on role %s: %w", SchedulerInvokePolicyName, SchedulerInvokeRoleName, err)
	}
	exists, rErr := roleExists(ctx, client, SchedulerInvokeRoleName)
	if rErr != nil {
		return "", fmt.Errorf("runtimeiam: check role %s: %w", SchedulerInvokeRoleName, rErr)
	}
	if !exists {
		return "", ErrInvokeRoleNotFound
	}
	return "", ErrInvokePolicyNotFound
}

// InvokeCoverage is what a Scheduler invoke policy permits, evaluated locally.
type InvokeCoverage struct {
	// ThisRegion: lambda:InvokeFunction is allowed on the function ARN asked about.
	ThisRegion bool
	// AllRegions: the grant that allows it has a wildcard region, so a poller in
	// any other region of the account is covered too. A policy that allows only
	// one region is the #218 shape: correct here, and silently broken everywhere
	// else the account runs a poller.
	AllRegions bool
}

// EvaluateInvokePolicy evaluates doc for lambda:InvokeFunction on functionARN
// (arn:aws:lambda:<region>:<account>:function:<name>) without calling IAM's
// policy simulator: IAM-style `*`/`?` globbing on action and resource, actions
// case-insensitive, explicit Deny beating any Allow.
//
// It is conservative where it cannot be exact: an Allow carrying a Condition is
// NOT counted (this cannot evaluate the condition, and claiming coverage that
// isn't there is the failure this check exists to catch), while a matching Deny
// counts whatever its condition. Statements using NotAction / NotResource /
// Principal are opaque to [ParsePolicy] and match nothing.
//
// AllRegions is answered by evaluating the same ARN with its region replaced by
// a literal `*`: only a pattern whose region segment is itself a wildcard (or a
// bare `*`) matches that.
func EvaluateInvokePolicy(doc, functionARN string) (InvokeCoverage, error) {
	grants, err := ParsePolicy(doc)
	if err != nil {
		return InvokeCoverage{}, err
	}
	usable := grants[:0:0]
	for _, g := range grants {
		if strings.EqualFold(g.Effect, "Allow") && g.Condition != "" {
			continue
		}
		usable = append(usable, g)
	}
	cov := InvokeCoverage{ThisRegion: Allows(usable, SchedulerInvokeAction, functionARN)}
	if anyRegion, ok := withWildcardRegion(functionARN); ok {
		cov.AllRegions = cov.ThisRegion && Allows(usable, SchedulerInvokeAction, anyRegion)
	}
	return cov, nil
}

// withWildcardRegion replaces the region segment (the 4th) of an ARN with "*".
func withWildcardRegion(arn string) (string, bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" {
		return "", false
	}
	parts[3] = "*"
	return strings.Join(parts, ":"), true
}
