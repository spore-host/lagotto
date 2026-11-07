package cliiam

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

func TestPrincipalARN(t *testing.T) {
	cases := []struct {
		in, want, role string
		err            bool
	}{
		{"arn:aws:sts::123456789012:assumed-role/Dev/alice", "arn:aws:iam::123456789012:role/Dev", "Dev", false},
		{"arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_abc/alice@example.org",
			"arn:aws:iam::123456789012:role/AWSReservedSSO_Admin_abc", "AWSReservedSSO_Admin_abc", false},
		{"arn:aws-us-gov:sts::123456789012:assumed-role/Dev/s", "arn:aws-us-gov:iam::123456789012:role/Dev", "Dev", false},
		{"arn:aws:iam::123456789012:user/ops/bob", "arn:aws:iam::123456789012:user/ops/bob", "", false},
		{"arn:aws:iam::123456789012:role/svc", "arn:aws:iam::123456789012:role/svc", "", false},
		{"arn:aws:iam::123456789012:root", "", "", true},
		{"arn:aws:sts::123456789012:federated-user/bob", "", "", true},
		{"arn:aws:sts::123456789012:assumed-role//s", "", "", true},
		{"not-an-arn", "", "", true},
	}
	for _, c := range cases {
		got, role, err := PrincipalARN(c.in)
		if (err != nil) != c.err || got != c.want || role != c.role {
			t.Errorf("PrincipalARN(%q) = %q, %q, %v; want %q, %q, err=%v", c.in, got, role, err, c.want, c.role, c.err)
		}
	}
}

// TestSimRequestsAreConcrete: the simulator reads a `*` in a request resource
// literally, so every simulated resource is concrete, in the caller's region,
// in the account; and the setup-only statements are not simulated.
func TestSimRequestsAreConcrete(t *testing.T) {
	reqs := simRequests(testAccount, "eu-west-1")
	if len(reqs) == 0 {
		t.Fatal("no requests")
	}
	actions := map[string]bool{}
	for _, r := range reqs {
		for _, a := range r.actions {
			actions[a] = true
		}
		for res, pattern := range r.resources {
			if strings.Contains(res, "*") {
				t.Errorf("simulated resource %s (for %s) has a wildcard", res, pattern)
			}
			if strings.Contains(pattern, ":*:") && !strings.Contains(res, ":eu-west-1:") {
				t.Errorf("%s: the region was not substituted (%s)", pattern, res)
			}
		}
	}
	if actions["iam:SimulatePrincipalPolicy"] {
		t.Error("the caller check simulates its own permission to simulate")
	}
	// Every other granted action is checked.
	for _, a := range GrantedActions(testAccount, nil) {
		if !actions[a] && a != "iam:SimulatePrincipalPolicy" && a != "iam:GetRole" {
			t.Errorf("%s is granted but not simulated", a)
		}
	}
	if !actions["iam:GetRole"] {
		t.Error("iam:GetRole on lagotto's roles is not simulated")
	}
}

func TestCheckCallerAllowed(t *testing.T) {
	f := &fakeIAM{}
	res := CheckCaller(context.Background(), f, "arn:aws:iam::123456789012:user/alice", testAccount, "us-east-1")
	if res.Status != CallerAllowed || len(res.Missing) != 0 || res.Checked < 20 {
		t.Errorf("result = %+v", res)
	}
}

// TestCheckCallerReportsWhatIsMissing, including conditions passed as context:
// a caller that may only subscribe non-email protocols lacks sns:Subscribe.
func TestCheckCallerReportsWhatIsMissing(t *testing.T) {
	f := &fakeIAM{simulate: func(_, action, resource string, ctx []iamtypes.ContextEntry) bool {
		switch action {
		case "scheduler:UpdateSchedule", "ec2:DescribeSubnets":
			return false
		case "sns:Subscribe":
			for _, c := range ctx {
				if aws.ToString(c.ContextKeyName) == "sns:Protocol" && c.ContextKeyValues[0] == "email" {
					return false // this caller's policy refuses email
				}
			}
			return true
		case "dynamodb:PutItem":
			return !strings.HasSuffix(resource, "table/lagotto-match-history")
		}
		return true
	}}
	res := CheckCaller(context.Background(), f, "arn:aws:iam::123456789012:user/alice", testAccount, "us-east-1")
	if res.Status != CallerMissing {
		t.Fatalf("status = %s", res.Status)
	}
	want := []string{
		"dynamodb:PutItem on arn:aws:dynamodb:*:123456789012:table/lagotto-match-history",
		"ec2:DescribeSubnets on *",
		"scheduler:UpdateSchedule on arn:aws:scheduler:*:123456789012:schedule/default/lagotto-capacity-poller",
		"sns:Subscribe on arn:aws:sns:*:123456789012:lagotto-capacity-alerts",
	}
	if strings.Join(res.Missing, "\n") != strings.Join(want, "\n") {
		t.Errorf("missing:\n%s\nwant:\n%s", strings.Join(res.Missing, "\n"), strings.Join(want, "\n"))
	}
}

// TestCheckCallerDeniedSimulateIsUnknown: not being allowed to simulate says
// nothing about the other permissions, so it is never reported as missing.
func TestCheckCallerDeniedSimulateIsUnknown(t *testing.T) {
	principal := "arn:aws:iam::123456789012:user/alice"
	f := &fakeIAM{simErr: map[string]error{principal: apiErr("AccessDenied")}}
	res := CheckCaller(context.Background(), f, principal, testAccount, "us-east-1")
	if res.Status != CallerUnknown || len(res.Missing) != 0 || !strings.Contains(res.Reason, "iam:SimulatePrincipalPolicy") {
		t.Errorf("result = %+v", res)
	}
	// Any other failure is unknown too.
	f = &fakeIAM{simErr: map[string]error{principal: apiErr("Throttling")}}
	if res := CheckCaller(context.Background(), f, principal, testAccount, "us-east-1"); res.Status != CallerUnknown {
		t.Errorf("a throttled simulation is %s", res.Status)
	}
}

// TestCheckCallerSimulatesTheRoleNotTheSession: an assumed role is simulated
// as its IAM role, and a role under a path (every IAM Identity Center role) is
// resolved with iam:GetRole when the path-less ARN is unknown.
func TestCheckCallerSimulatesTheRoleNotTheSession(t *testing.T) {
	f := &fakeIAM{}
	res := CheckCaller(context.Background(), f, "arn:aws:sts::123456789012:assumed-role/Dev/alice", testAccount, "us-east-1")
	if res.Status != CallerAllowed || res.PrincipalARN != "arn:aws:iam::123456789012:role/Dev" {
		t.Errorf("result = %+v", res)
	}
	for _, p := range f.simulated {
		if strings.Contains(p, ":sts:") || strings.Contains(p, "alice") {
			t.Fatalf("simulated the session %s", p)
		}
	}

	pathless := "arn:aws:iam::123456789012:role/AWSReservedSSO_Admin_abc"
	full := "arn:aws:iam::123456789012:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_Admin_abc"
	f = &fakeIAM{roleARN: full, simErr: map[string]error{pathless: noSuchEntity()}}
	res = CheckCaller(context.Background(), f, "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_abc/alice", testAccount, "us-east-1")
	if res.Status != CallerAllowed || res.PrincipalARN != full {
		t.Errorf("SSO role: %+v", res)
	}
	if !strings.Contains(strings.Join(f.calls, ","), "GetRole AWSReservedSSO_Admin_abc") {
		t.Errorf("GetRole was not used to resolve the path: %v", f.calls)
	}

	// The role cannot be read: unknown, not missing.
	f = &fakeIAM{simErr: map[string]error{pathless: noSuchEntity()}, errs: map[string]error{"GetRole": apiErr("AccessDenied")}}
	res = CheckCaller(context.Background(), f, "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_abc/alice", testAccount, "us-east-1")
	if res.Status != CallerUnknown || !strings.Contains(res.Reason, "iam:GetRole") {
		t.Errorf("unreadable role: %+v", res)
	}
}

func TestCheckCallerRootIsUnknown(t *testing.T) {
	f := &fakeIAM{}
	res := CheckCaller(context.Background(), f, "arn:aws:iam::123456789012:root", testAccount, "us-east-1")
	if res.Status != CallerUnknown || len(f.simulated) != 0 || !strings.Contains(res.Reason, "root") {
		t.Errorf("result = %+v", res)
	}
}
