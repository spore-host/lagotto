package runtimeiam

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

func TestPolicyDocument_ValidAndScoped(t *testing.T) {
	doc, err := PolicyDocument("us-west-2", "123456789012")
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}

	// Valid JSON with the right shape.
	var parsed struct {
		Version   string
		Statement []struct {
			Effect   string
			Action   []string
			Resource interface{}
		}
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatalf("policy is not valid JSON: %v", err)
	}
	if parsed.Version != "2012-10-17" {
		t.Errorf("Version = %q, want 2012-10-17", parsed.Version)
	}
	if len(parsed.Statement) == 0 {
		t.Fatal("policy has no statements")
	}

	// Every statement Allows and names at least one action.
	actions := map[string]bool{}
	for i, s := range parsed.Statement {
		if s.Effect != "Allow" {
			t.Errorf("statement %d Effect = %q, want Allow", i, s.Effect)
		}
		if len(s.Action) == 0 {
			t.Errorf("statement %d has no actions", i)
		}
		for _, a := range s.Action {
			actions[a] = true
		}
	}

	// The permissions each watch action needs must be present.
	for _, want := range []string{
		"dynamodb:PutItem", "sns:Publish", "ec2:DescribeInstanceTypes",
		"ec2:RunInstances", "ec2:CreateCapacityReservation",
		"sagemaker:CreateTrainingJob", "scheduler:CreateSchedule", "iam:PassRole",
		// #148: spawn's launcher needs the Pricing API to enforce --cost-limit.
		"pricing:GetProducts",
	} {
		if !actions[want] {
			t.Errorf("policy missing required action %q", want)
		}
	}

	// Region + account scoping shows up in the ARNs.
	if !strings.Contains(doc, "us-west-2") {
		t.Error("policy doesn't reference the region in any ARN")
	}
	if !strings.Contains(doc, "123456789012") {
		t.Error("policy doesn't reference the account ID in any ARN")
	}
	// PassRole must be conditioned (never an unconditioned pass-any-role).
	if !strings.Contains(doc, "iam:PassedToService") {
		t.Error("iam:PassRole is not scoped by PassedToService condition")
	}
	// Wide describe reads are Resource "*", but the instance role-setup must be
	// scoped (not "*") — guard against an over-broad regression.
	if strings.Contains(doc, `"iam:CreateRole"`) && !strings.Contains(doc, ":role/spored*") {
		t.Error("iam:CreateRole is present but not scoped to spored*")
	}
	// #148: spawn's launcher creates "spawn-instance-<hash>" roles/profiles, so the
	// poller must be authorized on spawn-instance* too — else auto-spawn dies at
	// "set up IAM instance profile" (AccessDenied → terminal) and no watch retries.
	for _, want := range []string{":role/spawn-instance*", ":instance-profile/spawn-instance*"} {
		if !strings.Contains(doc, want) {
			t.Errorf("policy missing %q (poller can't set up spawn's instance profile — lagotto#148)", want)
		}
	}
}

// fakeIAM simulates the subset of IAM used by the package: a set of roles that
// already exist, plus recorders for the create/attach/put calls.
type fakeIAM struct {
	existing map[string]bool
	created  []string
	attached []string
	// last PutRolePolicy (kept for existing assertions) + a per-call log.
	role, policy, doc string
	calls             int      // PutRolePolicy calls
	puts              []string // "role/policy" per PutRolePolicy
}

func (f *fakeIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if f.existing[aws.ToString(in.RoleName)] {
		return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName}}, nil
	}
	return nil, &iamtypes.NoSuchEntityException{}
}

func (f *fakeIAM) CreateRole(_ context.Context, in *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	name := aws.ToString(in.RoleName)
	f.created = append(f.created, name)
	if f.existing == nil {
		f.existing = map[string]bool{}
	}
	f.existing[name] = true
	return &iam.CreateRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName}}, nil
}

func (f *fakeIAM) AttachRolePolicy(_ context.Context, in *iam.AttachRolePolicyInput, _ ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	f.attached = append(f.attached, aws.ToString(in.RoleName))
	return &iam.AttachRolePolicyOutput{}, nil
}

func (f *fakeIAM) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	f.calls++
	f.role = aws.ToString(in.RoleName)
	f.policy = aws.ToString(in.PolicyName)
	f.doc = aws.ToString(in.PolicyDocument)
	f.puts = append(f.puts, f.role+"/"+f.policy)
	return &iam.PutRolePolicyOutput{}, nil
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// TestEnsureRoles_CreatesBothWhenAbsent is the #145 core: deploy creates both
// CLI-owned roles (so the stack can reference them by ARN instead of creating
// named roles that may already exist).
func TestEnsureRoles_CreatesBothWhenAbsent(t *testing.T) {
	f := &fakeIAM{}
	if err := EnsureRoles(context.Background(), f, "us-west-2", "123456789012"); err != nil {
		t.Fatalf("EnsureRoles: %v", err)
	}
	if !contains(f.created, RoleName) {
		t.Errorf("did not create %s (created=%v)", RoleName, f.created)
	}
	if !contains(f.created, SchedulerInvokeRoleName) {
		t.Errorf("did not create %s (created=%v)", SchedulerInvokeRoleName, f.created)
	}
	if !contains(f.attached, RoleName) {
		t.Errorf("did not attach basic-execution to %s (attached=%v)", RoleName, f.attached)
	}
	if !contains(f.puts, SchedulerInvokeRoleName+"/InvokeLambda") {
		t.Errorf("did not put InvokeLambda on %s (puts=%v)", SchedulerInvokeRoleName, f.puts)
	}
}

// TestEnsureRoles_IdempotentWhenPresent: pre-existing roles are not re-created.
func TestEnsureRoles_IdempotentWhenPresent(t *testing.T) {
	f := &fakeIAM{existing: map[string]bool{RoleName: true, SchedulerInvokeRoleName: true}}
	if err := EnsureRoles(context.Background(), f, "us-west-2", "123456789012"); err != nil {
		t.Fatalf("EnsureRoles: %v", err)
	}
	if len(f.created) != 0 {
		t.Errorf("created roles that already existed: %v", f.created)
	}
}

func TestEnsureRoles_RequiresRegionAndAccount(t *testing.T) {
	f := &fakeIAM{}
	if err := EnsureRoles(context.Background(), f, "", "123456789012"); err == nil {
		t.Error("want error for empty region")
	}
	if err := EnsureRoles(context.Background(), f, "us-west-2", ""); err == nil {
		t.Error("want error for empty account ID")
	}
	if len(f.created) != 0 {
		t.Errorf("no role should be created on validation failure (created=%v)", f.created)
	}
}

func TestEnsureRuntimeRole(t *testing.T) {
	f := &fakeIAM{}
	if err := EnsureRuntimeRole(context.Background(), f, "us-east-1", "123456789012"); err != nil {
		t.Fatalf("EnsureRuntimeRole: %v", err)
	}
	if f.calls != 1 {
		t.Errorf("PutRolePolicy called %d times, want 1", f.calls)
	}
	if f.role != RoleName {
		t.Errorf("role = %q, want %q", f.role, RoleName)
	}
	if f.policy != PolicyName {
		t.Errorf("policy = %q, want %q", f.policy, PolicyName)
	}
	if !json.Valid([]byte(f.doc)) {
		t.Error("attached policy document is not valid JSON")
	}
}

func TestEnsureRuntimeRole_RequiresRegionAndAccount(t *testing.T) {
	f := &fakeIAM{}
	if err := EnsureRuntimeRole(context.Background(), f, "", "123456789012"); err == nil {
		t.Error("want error for empty region")
	}
	if err := EnsureRuntimeRole(context.Background(), f, "us-east-1", ""); err == nil {
		t.Error("want error for empty account ID")
	}
	if f.calls != 0 {
		t.Errorf("PutRolePolicy should not be called on validation failure (called %d)", f.calls)
	}
}
