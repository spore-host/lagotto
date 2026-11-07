package cliiam

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
)

// DeleteAPI is the slice of the IAM API [DeletePolicy] needs. *iam.Client
// satisfies it. Separate from [PolicyAPI] so setup's create path cannot reach
// a delete.
type DeleteAPI interface {
	GetPolicy(ctx context.Context, in *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error)
	ListEntitiesForPolicy(ctx context.Context, in *iam.ListEntitiesForPolicyInput, optFns ...func(*iam.Options)) (*iam.ListEntitiesForPolicyOutput, error)
	DetachUserPolicy(ctx context.Context, in *iam.DetachUserPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error)
	DetachGroupPolicy(ctx context.Context, in *iam.DetachGroupPolicyInput, optFns ...func(*iam.Options)) (*iam.DetachGroupPolicyOutput, error)
	DetachRolePolicy(ctx context.Context, in *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error)
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
	// Detached are the principals it was detached from, as "user/NAME",
	// "group/NAME" or "role/NAME".
	Detached []string
	// DeletedVersions are the non-default versions deleted before the policy.
	DeletedVersions []string
}

// DeletePolicy is the named deleter for [PolicyName] (#232). IAM refuses
// DeletePolicy while the policy is attached to anything or has non-default
// versions, so it detaches it from every user, group and role, deletes each
// non-default version, then deletes the policy. Idempotent: an absent policy is
// reported as Absent. A same-named policy without lagotto's tag is left alone.
//
// Detaching removes the policy's permissions from everyone it was attached to.
// That is the point of a teardown, and the caller says so.
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

	// 1. Detach from every principal.
	for pg := iam.NewListEntitiesForPolicyPaginator(client, &iam.ListEntitiesForPolicyInput{PolicyArn: arn}); pg.HasMorePages(); {
		page, err := pg.NextPage(ctx)
		if err != nil {
			return d, fmt.Errorf("cliiam: list what %s is attached to: %w", PolicyName, err)
		}
		for _, u := range page.PolicyUsers {
			if _, err := client.DetachUserPolicy(ctx, &iam.DetachUserPolicyInput{UserName: u.UserName, PolicyArn: arn}); err != nil && !isNoSuchEntity(err) {
				return d, fmt.Errorf("cliiam: detach %s from user %s: %w", PolicyName, aws.ToString(u.UserName), err)
			}
			d.Detached = append(d.Detached, "user/"+aws.ToString(u.UserName))
		}
		for _, g := range page.PolicyGroups {
			if _, err := client.DetachGroupPolicy(ctx, &iam.DetachGroupPolicyInput{GroupName: g.GroupName, PolicyArn: arn}); err != nil && !isNoSuchEntity(err) {
				return d, fmt.Errorf("cliiam: detach %s from group %s: %w", PolicyName, aws.ToString(g.GroupName), err)
			}
			d.Detached = append(d.Detached, "group/"+aws.ToString(g.GroupName))
		}
		for _, r := range page.PolicyRoles {
			if _, err := client.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{RoleName: r.RoleName, PolicyArn: arn}); err != nil && !isNoSuchEntity(err) {
				return d, fmt.Errorf("cliiam: detach %s from role %s: %w", PolicyName, aws.ToString(r.RoleName), err)
			}
			d.Detached = append(d.Detached, "role/"+aws.ToString(r.RoleName))
		}
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
