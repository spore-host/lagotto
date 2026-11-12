package runtimeiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// errIAM fails one IAM call (by name, optionally for one role) and delegates
// the rest to fakeIAM.
type errIAM struct {
	*fakeIAM
	call, role string
}

func (e *errIAM) fails(call string, role *string) error {
	if e.call == call && (e.role == "" || e.role == aws.ToString(role)) {
		return errors.New(call + " denied")
	}
	return nil
}

func (e *errIAM) GetRole(ctx context.Context, in *iam.GetRoleInput, o ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if err := e.fails("GetRole", in.RoleName); err != nil {
		return nil, err
	}
	return e.fakeIAM.GetRole(ctx, in, o...)
}

func (e *errIAM) CreateRole(ctx context.Context, in *iam.CreateRoleInput, o ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	if err := e.fails("CreateRole", in.RoleName); err != nil {
		return nil, err
	}
	return e.fakeIAM.CreateRole(ctx, in, o...)
}

func (e *errIAM) AttachRolePolicy(ctx context.Context, in *iam.AttachRolePolicyInput, o ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	if err := e.fails("AttachRolePolicy", in.RoleName); err != nil {
		return nil, err
	}
	return e.fakeIAM.AttachRolePolicy(ctx, in, o...)
}

func (e *errIAM) PutRolePolicy(ctx context.Context, in *iam.PutRolePolicyInput, o ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	if err := e.fails("PutRolePolicy", in.RoleName); err != nil {
		return nil, err
	}
	return e.fakeIAM.PutRolePolicy(ctx, in, o...)
}

// TestEnsureSetupRoles_ReportsEachFailure: every IAM step's failure stops setup
// with an error naming the role, and a role already created is still reported.
func TestEnsureSetupRoles_ReportsEachFailure(t *testing.T) {
	for _, tc := range []struct {
		call, role  string
		wantCreated []string
	}{
		{"GetRole", RoleName, nil},
		{"CreateRole", RoleName, nil},
		{"AttachRolePolicy", RoleName, nil},
		{"PutRolePolicy", RoleName, []string{RoleName}},
		{"GetRole", SchedulerInvokeRoleName, []string{RoleName}},
		{"CreateRole", SchedulerInvokeRoleName, []string{RoleName}},
		{"PutRolePolicy", SchedulerInvokeRoleName, []string{RoleName, SchedulerInvokeRoleName}},
	} {
		f := &errIAM{fakeIAM: &fakeIAM{}, call: tc.call, role: tc.role}
		created, err := EnsureSetupRoles(context.Background(), f, "us-west-2", "123456789012", Authorizations{})
		if err == nil || !strings.Contains(err.Error(), tc.role) {
			t.Errorf("%s on %s: err = %v, want one naming the role", tc.call, tc.role, err)
		}
		if strings.Join(created, ",") != strings.Join(tc.wantCreated, ",") {
			t.Errorf("%s on %s: created = %v, want %v", tc.call, tc.role, created, tc.wantCreated)
		}
	}
}

// EnsureRoles and EnsureRuntimeRole keep failing loudly after the refactor.
func TestEnsureRolesAndRuntimeRole_PropagateFailures(t *testing.T) {
	if err := EnsureRoles(context.Background(), &errIAM{fakeIAM: &fakeIAM{}, call: "CreateRole", role: RoleName}, "us-west-2", "123456789012"); err == nil {
		t.Error("EnsureRoles ignored a poller-role failure")
	}
	if err := EnsureRuntimeRole(context.Background(), &errIAM{fakeIAM: &fakeIAM{}, call: "AttachRolePolicy"}, "us-west-2", "123456789012", Authorizations{}); err == nil {
		t.Error("EnsureRuntimeRole ignored a poller-role failure")
	}
	if err := EnsureRuntimeRole(context.Background(), &errIAM{fakeIAM: &fakeIAM{}, call: "PutRolePolicy"}, "us-west-2", "123456789012", Authorizations{}); err == nil ||
		!strings.Contains(err.Error(), "put role policy") {
		t.Errorf("EnsureRuntimeRole PutRolePolicy failure: %v", err)
	}
}

// errDeleter fails one step of a role deletion.
type errDeleter struct {
	*fakeRoleDeleter
	call string
	err  error
}

func (e *errDeleter) ListRolePolicies(ctx context.Context, in *iam.ListRolePoliciesInput, o ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	if e.call == "ListRolePolicies" {
		return nil, e.err
	}
	return e.fakeRoleDeleter.ListRolePolicies(ctx, in, o...)
}

func (e *errDeleter) DeleteRolePolicy(ctx context.Context, in *iam.DeleteRolePolicyInput, o ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	if e.call == "DeleteRolePolicy" {
		return nil, e.err
	}
	return e.fakeRoleDeleter.DeleteRolePolicy(ctx, in, o...)
}

func (e *errDeleter) ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput, o ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	if e.call == "ListAttachedRolePolicies" {
		return nil, e.err
	}
	return e.fakeRoleDeleter.ListAttachedRolePolicies(ctx, in, o...)
}

func (e *errDeleter) DetachRolePolicy(ctx context.Context, in *iam.DetachRolePolicyInput, o ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	if e.call == "DetachRolePolicy" {
		return nil, e.err
	}
	return e.fakeRoleDeleter.DetachRolePolicy(ctx, in, o...)
}

func (e *errDeleter) DeleteRole(ctx context.Context, in *iam.DeleteRoleInput, o ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	if e.call == "DeleteRole" {
		return nil, e.err
	}
	return e.fakeRoleDeleter.DeleteRole(ctx, in, o...)
}

func TestDeleteRoles_EachStepFailsLoudly(t *testing.T) {
	for _, call := range []string{"ListRolePolicies", "DeleteRolePolicy", "ListAttachedRolePolicies", "DetachRolePolicy", "DeleteRole"} {
		f := &errDeleter{fakeRoleDeleter: setUpFakeRoles(), call: call, err: errors.New("AccessDenied")}
		f.attached[SchedulerInvokeRoleName] = []string{lambdaBasicExecutionARN} // so both roles detach
		deleted, err := DeleteRoles(context.Background(), f)
		if err == nil || !strings.Contains(err.Error(), "AccessDenied") || len(deleted) != 0 {
			t.Errorf("%s fails: deleted %v, err %v; want nothing deleted and the error", call, deleted, err)
		}
	}
}

// A role that disappears mid-deletion (another teardown racing) is "already
// gone", not an error: it is skipped, not reported as deleted.
func TestDeleteRoles_RoleVanishingIsNotAnError(t *testing.T) {
	for _, call := range []string{"ListAttachedRolePolicies", "DeleteRole"} {
		f := &errDeleter{fakeRoleDeleter: setUpFakeRoles(), call: call, err: &iamtypes.NoSuchEntityException{}}
		deleted, err := DeleteRoles(context.Background(), f)
		if err != nil || len(deleted) != 0 {
			t.Errorf("%s NoSuchEntity: deleted %v, err %v; want none, nil", call, deleted, err)
		}
	}
}
