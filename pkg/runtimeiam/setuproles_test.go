package runtimeiam

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// TestEnsureSetupRoles_CreatesBothOnAFreshAccount is #229: setup must create the
// Scheduler invoke role as well as the poller role, and say so. Before, setup
// created only the poller role and a CloudFormation-path account had no command
// short of `lagotto deploy` that created the role its stack's schedule assumes.
func TestEnsureSetupRoles_CreatesBothOnAFreshAccount(t *testing.T) {
	f := &fakeIAM{}
	created, err := EnsureSetupRoles(context.Background(), f, "us-west-2", "123456789012", Authorizations{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{RoleName, SchedulerInvokeRoleName}
	if !reflect.DeepEqual(created, want) {
		t.Errorf("created = %v, want %v", created, want)
	}
	if !reflect.DeepEqual(f.created, want) {
		t.Errorf("CreateRole calls = %v, want %v", f.created, want)
	}
	runtimeDoc, _ := PolicyDocument("123456789012", nil)
	invokeDoc, _ := SchedulerInvokePolicyDocument("123456789012")
	if f.inline[RoleName+"/"+PolicyName] != runtimeDoc {
		t.Error("the runtime policy was not written onto the poller role")
	}
	if f.inline[SchedulerInvokeRoleName+"/"+SchedulerInvokePolicyName] != invokeDoc {
		t.Error("the invoke policy was not written onto the Scheduler invoke role")
	}
	if !contains(f.attached, RoleName) {
		t.Error("basic execution was not attached to the poller role")
	}
}

// TestEnsureSetupRoles_Idempotent: a re-run creates nothing, reports nothing,
// and rewrites both policies in place.
func TestEnsureSetupRoles_Idempotent(t *testing.T) {
	f := &fakeIAM{existing: map[string]bool{RoleName: true, SchedulerInvokeRoleName: true}}
	created, err := EnsureSetupRoles(context.Background(), f, "us-east-1", "123456789012", Authorizations{InstanceRoles: []string{"my-role"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(created) != 0 || len(f.created) != 0 {
		t.Errorf("existing roles were re-created: reported %v, CreateRole %v", created, f.created)
	}
	if f.calls != 2 || !contains(f.puts, RoleName+"/"+PolicyName) || !contains(f.puts, SchedulerInvokeRoleName+"/"+SchedulerInvokePolicyName) {
		t.Errorf("PutRolePolicy calls = %v, want both policies once", f.puts)
	}
	if !strings.Contains(f.inline[RoleName+"/"+PolicyName], ":role/my-role") {
		t.Error("the authorized instance role was dropped from the runtime policy")
	}
}

// TestEnsureSetupRoles_OnlyTheMissingRoleIsReported: an account that has run
// setup before #229 has the poller role only.
func TestEnsureSetupRoles_OnlyTheMissingRoleIsReported(t *testing.T) {
	f := &fakeIAM{existing: map[string]bool{RoleName: true}}
	created, err := EnsureSetupRoles(context.Background(), f, "us-west-2", "123456789012", Authorizations{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(created, []string{SchedulerInvokeRoleName}) {
		t.Errorf("created = %v, want only the invoke role", created)
	}
}

func TestEnsureSetupRoles_RequiresRegionAndAccount(t *testing.T) {
	for _, tc := range [][2]string{{"", "123456789012"}, {"us-west-2", ""}} {
		f := &fakeIAM{}
		if _, err := EnsureSetupRoles(context.Background(), f, tc[0], tc[1], Authorizations{}); err == nil {
			t.Errorf("EnsureSetupRoles(%q, %q) did not fail", tc[0], tc[1])
		}
		if len(f.created) != 0 || f.calls != 0 {
			t.Errorf("EnsureSetupRoles(%q, %q) wrote to IAM before validating", tc[0], tc[1])
		}
	}
}

// fakeRoleDeleter models the roles' inline and managed policies, enforcing
// IAM's rule that a role with policies cannot be deleted.
type fakeRoleDeleter struct {
	inline   map[string][]string
	attached map[string][]string
	exists   map[string]bool
	order    []string
	failOn   string
}

func (f *fakeRoleDeleter) ListRolePolicies(_ context.Context, in *iam.ListRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	name := aws.ToString(in.RoleName)
	if !f.exists[name] {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.ListRolePoliciesOutput{PolicyNames: f.inline[name]}, nil
}

func (f *fakeRoleDeleter) DeleteRolePolicy(_ context.Context, in *iam.DeleteRolePolicyInput, _ ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	name := aws.ToString(in.RoleName)
	f.order = append(f.order, "delete-policy "+name+"/"+aws.ToString(in.PolicyName))
	var keep []string
	for _, p := range f.inline[name] {
		if p != aws.ToString(in.PolicyName) {
			keep = append(keep, p)
		}
	}
	f.inline[name] = keep
	return &iam.DeleteRolePolicyOutput{}, nil
}

func (f *fakeRoleDeleter) ListAttachedRolePolicies(_ context.Context, in *iam.ListAttachedRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	name := aws.ToString(in.RoleName)
	if !f.exists[name] {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	var out []iamtypes.AttachedPolicy
	for _, a := range f.attached[name] {
		out = append(out, iamtypes.AttachedPolicy{PolicyArn: aws.String(a)})
	}
	return &iam.ListAttachedRolePoliciesOutput{AttachedPolicies: out}, nil
}

func (f *fakeRoleDeleter) DetachRolePolicy(_ context.Context, in *iam.DetachRolePolicyInput, _ ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	name := aws.ToString(in.RoleName)
	f.order = append(f.order, "detach "+name)
	f.attached[name] = nil
	return &iam.DetachRolePolicyOutput{}, nil
}

func (f *fakeRoleDeleter) DeleteRole(_ context.Context, in *iam.DeleteRoleInput, _ ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	name := aws.ToString(in.RoleName)
	if name == f.failOn {
		return nil, errors.New("AccessDenied")
	}
	if !f.exists[name] {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	if len(f.inline[name]) > 0 || len(f.attached[name]) > 0 {
		return nil, &iamtypes.DeleteConflictException{}
	}
	f.order = append(f.order, "delete-role "+name)
	delete(f.exists, name)
	return &iam.DeleteRoleOutput{}, nil
}

func setUpFakeRoles() *fakeRoleDeleter {
	return &fakeRoleDeleter{
		exists:   map[string]bool{RoleName: true, SchedulerInvokeRoleName: true},
		inline:   map[string][]string{RoleName: {PolicyName}, SchedulerInvokeRoleName: {SchedulerInvokePolicyName}},
		attached: map[string][]string{RoleName: {lambdaBasicExecutionARN}},
	}
}

// TestDeleteRoles_DeletesWhatSetupCreates is the named deleter (#229): every
// role setup and deploy create, emptied of its policies and deleted, the
// invoke role first.
func TestDeleteRoles_DeletesWhatSetupCreates(t *testing.T) {
	// Build the account with the real create path, then delete it.
	created := &fakeIAM{}
	if _, err := EnsureSetupRoles(context.Background(), created, "us-west-2", "123456789012", Authorizations{}); err != nil {
		t.Fatal(err)
	}
	f := setUpFakeRoles()
	for _, name := range created.created {
		if !f.exists[name] {
			t.Fatalf("setup creates %s, which DeleteRoles' model of the account lacks; it has no deleter", name)
		}
	}

	deleted, err := DeleteRoles(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deleted, []string{SchedulerInvokeRoleName, RoleName}) {
		t.Errorf("deleted = %v", deleted)
	}
	if len(f.exists) != 0 {
		t.Errorf("roles left behind: %v", f.exists)
	}
	if f.order[0] != "delete-policy "+SchedulerInvokeRoleName+"/"+SchedulerInvokePolicyName {
		t.Errorf("the invoke role was not removed first: %v", f.order)
	}

	// Idempotent: nothing left, nothing reported, no error.
	again, err := DeleteRoles(context.Background(), f)
	if err != nil || len(again) != 0 {
		t.Errorf("second DeleteRoles = %v, %v; want none, nil", again, err)
	}
}

// TestDeleteRoles_StopsOnAnError: a failed delete is reported with what was
// already deleted, never as success.
func TestDeleteRoles_StopsOnAnError(t *testing.T) {
	f := setUpFakeRoles()
	f.failOn = RoleName
	deleted, err := DeleteRoles(context.Background(), f)
	if err == nil || !strings.Contains(err.Error(), RoleName) {
		t.Errorf("err = %v, want one naming %s", err, RoleName)
	}
	if !reflect.DeepEqual(deleted, []string{SchedulerInvokeRoleName}) {
		t.Errorf("deleted = %v, want only the invoke role", deleted)
	}
}
