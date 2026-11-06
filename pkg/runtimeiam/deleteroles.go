package runtimeiam

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// RoleDeleteAPI is the slice of the IAM API [DeleteRoles] needs. *iam.Client
// satisfies it. It is separate from [IAMAPI] so the create paths cannot reach
// a delete.
type RoleDeleteAPI interface {
	ListRolePolicies(ctx context.Context, in *iam.ListRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error)
	DeleteRolePolicy(ctx context.Context, in *iam.DeleteRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error)
	ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error)
	DetachRolePolicy(ctx context.Context, in *iam.DetachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error)
	DeleteRole(ctx context.Context, in *iam.DeleteRoleInput, optFns ...func(*iam.Options)) (*iam.DeleteRoleOutput, error)
}

// CLIOwnedRoles are the IAM roles lagotto creates (setup and deploy), in the
// order [DeleteRoles] deletes them: the invoke role first, so a schedule can
// no longer invoke a poller whose role is being removed.
var CLIOwnedRoles = []string{SchedulerInvokeRoleName, RoleName}

// DeleteRoles is the named deleter for [CLIOwnedRoles] (#229): for each role it
// deletes the inline policies, detaches the managed ones, and deletes the role.
// It returns the roles it deleted; an absent role is skipped, so it is
// idempotent.
//
// The roles are IAM-global and shared by the poller in every region of the
// account, so deleting them while any region still runs a poller breaks it.
// The caller must establish that none does (`lagotto teardown --iam-roles`
// checks every enabled region first). It deletes only these fixed names.
func DeleteRoles(ctx context.Context, client RoleDeleteAPI) ([]string, error) {
	var deleted []string
	for _, name := range CLIOwnedRoles {
		gone, err := deleteRole(ctx, client, name)
		if err != nil {
			return deleted, err
		}
		if gone {
			deleted = append(deleted, name)
		}
	}
	return deleted, nil
}

func isNoSuchEntity(err error) bool {
	var nse *iamtypes.NoSuchEntityException
	return errors.As(err, &nse)
}

// deleteRole empties and deletes one role. deleted is false when the role was
// already absent.
func deleteRole(ctx context.Context, client RoleDeleteAPI, name string) (deleted bool, err error) {
	var inline []string
	for p := iam.NewListRolePoliciesPaginator(client, &iam.ListRolePoliciesInput{RoleName: aws.String(name)}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if err != nil {
			if isNoSuchEntity(err) {
				return false, nil
			}
			return false, fmt.Errorf("runtimeiam: list inline policies of %s: %w", name, err)
		}
		inline = append(inline, page.PolicyNames...)
	}
	for _, pol := range inline {
		if _, err := client.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
			RoleName: aws.String(name), PolicyName: aws.String(pol),
		}); err != nil && !isNoSuchEntity(err) {
			return false, fmt.Errorf("runtimeiam: delete inline policy %s of %s: %w", pol, name, err)
		}
	}

	var attached []string
	for p := iam.NewListAttachedRolePoliciesPaginator(client, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(name)}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if err != nil {
			if isNoSuchEntity(err) {
				return false, nil
			}
			return false, fmt.Errorf("runtimeiam: list managed policies of %s: %w", name, err)
		}
		for _, ap := range page.AttachedPolicies {
			attached = append(attached, aws.ToString(ap.PolicyArn))
		}
	}
	for _, arn := range attached {
		if _, err := client.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
			RoleName: aws.String(name), PolicyArn: aws.String(arn),
		}); err != nil && !isNoSuchEntity(err) {
			return false, fmt.Errorf("runtimeiam: detach %s from %s: %w", arn, name, err)
		}
	}

	if _, err := client.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(name)}); err != nil {
		if isNoSuchEntity(err) {
			return false, nil
		}
		return false, fmt.Errorf("runtimeiam: delete role %s: %w", name, err)
	}
	return true, nil
}
