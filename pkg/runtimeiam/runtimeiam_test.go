package runtimeiam

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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
	// Wide describe reads are Resource "*", but the spored role-setup must be
	// scoped to spored* (not "*") — guard against an over-broad regression.
	if strings.Contains(doc, `"iam:CreateRole"`) && !strings.Contains(doc, ":role/spored*") {
		t.Error("iam:CreateRole is present but not scoped to spored*")
	}
}

// fakeIAM records the PutRolePolicy call.
type fakeIAM struct {
	role, policy, doc string
	calls             int
}

func (f *fakeIAM) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	f.calls++
	f.role = aws.ToString(in.RoleName)
	f.policy = aws.ToString(in.PolicyName)
	f.doc = aws.ToString(in.PolicyDocument)
	return &iam.PutRolePolicyOutput{}, nil
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
