package cliiam

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
)

// DeleteAPI is the slice of the IAM API [DeletePolicy] needs. *iam.Client
// satisfies it. Separate from [PolicyAPI] so setup's create path cannot reach
// a delete.
type DeleteAPI interface {
	GetPolicy(ctx context.Context, in *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error)
	ListEntitiesForPolicy(ctx context.Context, in *iam.ListEntitiesForPolicyInput, optFns ...func(*iam.Options)) (*iam.ListEntitiesForPolicyOutput, error)
	ListPolicyVersions(ctx context.Context, in *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error)
	DeletePolicyVersion(ctx context.Context, in *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error)
	DeletePolicy(ctx context.Context, in *iam.DeletePolicyInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyOutput, error)
}

// Deletion is what [DeletePolicy] did.
type Deletion struct {
	// Deleted: the policy existed and is gone.
	Deleted bool
	// Absent: there was no policy to delete.
	Absent bool
	// Unmanaged: a policy of that name exists without lagotto's tag and was
	// left in place.
	Unmanaged bool
	// Attached are the principals it is attached to, as "user/NAME",
	// "group/NAME" or "role/NAME", when DeletePolicy refused for them.
	Attached []string
	// DeletedVersions are the non-default versions deleted before the policy.
	DeletedVersions []string
}

// ErrAttached is DeletePolicy's refusal of a policy that something is
// attached to (Deletion.Attached says what).
var ErrAttached = errors.New("cliiam: the policy is attached")

// DeletePolicy is the named deleter for [PolicyName] (#232), only once
// nothing is attached to it (an owner decision, lagotto#340): it lists what
// the policy is attached to immediately before the delete, and refuses
// (ErrAttached) if anything is, so a policy attached since a footprint was
// read is never taken from anyone. It never detaches. Otherwise it deletes
// each non-default version, then the policy. Idempotent: an absent policy is
// reported as Absent. A same-named policy without lagotto's tag is left
// alone.
func DeletePolicy(ctx context.Context, client DeleteAPI, accountID string) (Deletion, error) {
	var d Deletion
	arn := aws.String(PolicyARN(accountID))
	p, err := client.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: arn})
	if err != nil {
		if isNoSuchEntity(err) {
			d.Absent = true
			return d, nil
		}
		return d, fmt.Errorf("cliiam: get policy %s: %w", PolicyName, err)
	}
	if p.Policy == nil || !hasManagedTag(p.Policy.Tags) {
		d.Unmanaged = true
		return d, nil
	}

	// 1. Nothing may be attached to it, now.
	for pg := iam.NewListEntitiesForPolicyPaginator(client, &iam.ListEntitiesForPolicyInput{PolicyArn: arn}); pg.HasMorePages(); {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return d, fmt.Errorf("cliiam: list what %s is attached to: %w", PolicyName, err)
		}
		for _, u := range page.PolicyUsers {
			d.Attached = append(d.Attached, "user/"+aws.ToString(u.UserName))
		}
		for _, g := range page.PolicyGroups {
			d.Attached = append(d.Attached, "group/"+aws.ToString(g.GroupName))
		}
		for _, r := range page.PolicyRoles {
			d.Attached = append(d.Attached, "role/"+aws.ToString(r.RoleName))
		}
	}
	if len(d.Attached) > 0 {
		return d, fmt.Errorf("cliiam: %s left in place: attached to %s: %w", PolicyName, strings.Join(d.Attached, ", "), ErrAttached)
	}

	// 2. Delete the non-default versions (the default goes with the policy).
	versions, err := listVersions(ctx, client, *arn)
	if err != nil {
		return d, fmt.Errorf("cliiam: list versions of %s: %w", PolicyName, err)
	}
	for _, v := range versions {
		if v.IsDefaultVersion {
			continue
		}
		id := aws.ToString(v.VersionId)
		if _, err := client.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{PolicyArn: arn, VersionId: aws.String(id)}); err != nil && !isNoSuchEntity(err) {
			return d, fmt.Errorf("cliiam: delete version %s of %s: %w", id, PolicyName, err)
		}
		d.DeletedVersions = append(d.DeletedVersions, id)
	}

	// 3. The policy.
	if _, err := client.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: arn}); err != nil {
		if isNoSuchEntity(err) {
			d.Absent = true
			return d, nil
		}
		return d, fmt.Errorf("cliiam: delete policy %s: %w", PolicyName, err)
	}
	d.Deleted = true
	return d, nil
}
