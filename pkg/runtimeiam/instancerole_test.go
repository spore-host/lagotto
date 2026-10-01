package runtimeiam

import (
	"strings"
	"testing"
)

const (
	irAccount = "123456789012"
	irRegion  = "us-west-2"
	// The role from lagotto#170 — three watches matched g6.8xlarge and died
	// because the poller could not iam:GetRole it.
	irRole = "loop-node-role"
)

func irPolicy(t *testing.T, roles []string) string {
	t.Helper()
	doc, err := PolicyDocument(irRegion, irAccount, roles)
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	return doc
}

func mustGrants(t *testing.T, doc string) []Grant {
	t.Helper()
	g, err := ParsePolicy(doc)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	return g
}

// TestStockPolicyDoesNotCoverANamedRole is lagotto#170 itself: without an explicit
// authorization, the launcher's very first IAM call on a user-named role is denied,
// after the watch has already matched capacity.
func TestStockPolicyDoesNotCoverANamedRole(t *testing.T) {
	grants := mustGrants(t, irPolicy(t, nil))

	if Allows(grants, "iam:GetRole", "arn:aws:iam::"+irAccount+":role/"+irRole) {
		t.Error("stock policy should NOT cover a user-named role")
	}
	missing := MissingInstanceRoleGrants(grants, irAccount, irRole)
	if len(missing) == 0 {
		t.Fatal("expected the stock policy to be missing grants for a user-named role")
	}
	// The whole set must be reported, not just the one that happened to fail
	// first — granting only iam:GetRole moves the failure to the next call.
	for _, want := range append(append([]string{}, InstanceRoleActions...), InstanceRolePassAction) {
		if !containsStr(missing, want) {
			t.Errorf("missing-grant report omits %s; it reported %v", want, missing)
		}
	}

	// The built-in prefixes must still be covered, or the stock policy is broken.
	if !Allows(grants, "iam:GetRole", "arn:aws:iam::"+irAccount+":role/spawn-instance-abc123") {
		t.Error("stock policy must still cover spawn-instance* roles")
	}
}

// TestAuthorizedRoleIsFullyCovered is the fix: one authorization grants every
// action the launcher performs, on both the role and its identically-named profile.
func TestAuthorizedRoleIsFullyCovered(t *testing.T) {
	grants := mustGrants(t, irPolicy(t, []string{irRole}))

	if missing := MissingInstanceRoleGrants(grants, irAccount, irRole); len(missing) != 0 {
		t.Errorf("authorized role still missing grants: %v", missing)
	}
	// Profile-shaped actions must land on the instance-profile ARN specifically.
	if !Allows(grants, "iam:AddRoleToInstanceProfile", "arn:aws:iam::"+irAccount+":instance-profile/"+irRole) {
		t.Error("authorization must cover the identically-named instance profile")
	}
	// Authorizing one role must not authorize another.
	if missing := MissingInstanceRoleGrants(grants, irAccount, "some-other-role"); len(missing) == 0 {
		t.Error("authorizing one role must not cover a different role")
	}
}

// TestAuthorizedInstanceRolesRoundTrip is what lets the deployed policy be the
// source of truth: `setup` reads it back to preserve authorizations, and `doctor`
// reads it to avoid reporting them as drift.
func TestAuthorizedInstanceRolesRoundTrip(t *testing.T) {
	got, err := AuthorizedInstanceRoles(irPolicy(t, []string{irRole, "other-role"}))
	if err != nil {
		t.Fatalf("AuthorizedInstanceRoles: %v", err)
	}
	if len(got) != 2 || got[0] != "loop-node-role" || got[1] != "other-role" {
		t.Errorf("round-trip = %v, want [loop-node-role other-role] (sorted)", got)
	}

	// A stock policy has no authorizations. If this reported anything, `doctor`
	// would invent authorizations on every healthy deployment.
	stock, err := AuthorizedInstanceRoles(irPolicy(t, nil))
	if err != nil {
		t.Fatalf("AuthorizedInstanceRoles(stock): %v", err)
	}
	if len(stock) != 0 {
		t.Errorf("stock policy reported authorizations %v, want none "+
			"(the built-in spawn-instance*/spored*/scheduler-invoke roles must not be mistaken for authorizations)", stock)
	}
}

// TestDiffRuntimePolicyIgnoresAuthorizedRoles guards the trap that would make this
// feature self-defeating: if authorizations read as Extra drift, doctor would tell
// the user to run `setup`, which (before this change) would REVOKE them.
func TestDiffRuntimePolicyIgnoresAuthorizedRoles(t *testing.T) {
	deployed := irPolicy(t, []string{irRole})

	diff, err := DiffRuntimePolicy(deployed, irRegion, irAccount)
	if err != nil {
		t.Fatalf("DiffRuntimePolicy: %v", err)
	}
	if !diff.Empty() {
		t.Errorf("an authorized role must not read as drift; missing=%v extra=%v",
			diff.MissingActions(), diff.ExtraActions())
	}
}

// TestValidateInstanceRoleNameRefusesWildcards is security-critical: the name is
// interpolated into a Resource ARN, so `--instance-role '*'` would otherwise grant
// iam:PutRolePolicy and iam:PassRole on every role in the account.
func TestValidateInstanceRoleNameRefusesWildcards(t *testing.T) {
	for _, bad := range []string{"*", "loop-*", "?ole", "", "   ", "arn:aws:iam::1:role/x", "path/role"} {
		if err := ValidateInstanceRoleName(bad); err == nil {
			t.Errorf("ValidateInstanceRoleName(%q) = nil, want an error", bad)
		}
	}
	for _, ok := range []string{"loop-node-role", "My_Role.1", "a+b=c,d@e-f"} {
		if err := ValidateInstanceRoleName(ok); err != nil {
			t.Errorf("ValidateInstanceRoleName(%q) = %v, want nil", ok, err)
		}
	}

	// And the policy builder must drop a wildcard even if one reaches it, so a
	// bypass of the CLI-level check still cannot widen the policy.
	doc := irPolicy(t, []string{"*", "loop-*", irRole})
	if strings.Contains(doc, ":role/*\"") || strings.Contains(doc, "loop-*") {
		t.Errorf("PolicyDocument emitted a wildcard role resource:\n%s", doc)
	}
	roles, err := AuthorizedInstanceRoles(doc)
	if err != nil {
		t.Fatalf("AuthorizedInstanceRoles: %v", err)
	}
	if len(roles) != 1 || roles[0] != irRole {
		t.Errorf("authorized roles = %v, want only %q", roles, irRole)
	}
}

// TestPassRoleConditionIsNotIgnored is the trap this check would otherwise fall
// into. The stock policy grants iam:PassRole on "*" conditioned to
// sagemaker.amazonaws.com. A condition-blind match counts that as coverage, so an
// unauthorized role is reported usable and the watch dies at RunInstances anyway —
// the exact failure the create-time check exists to prevent.
func TestPassRoleConditionIsNotIgnored(t *testing.T) {
	grants := mustGrants(t, irPolicy(t, nil))
	roleARN := "arn:aws:iam::" + irAccount + ":role/" + irRole

	// Condition-blind: the sagemaker grant on "*" matches, which is the bug.
	if !Allows(grants, InstanceRolePassAction, roleARN) {
		t.Fatal("precondition: the stock policy does grant PassRole on * (for sagemaker)")
	}
	// Condition-aware: it must NOT count as permission to pass a role to EC2.
	if AllowsPassRoleTo(grants, roleARN, "ec2.amazonaws.com") {
		t.Error("PassRole conditioned to sagemaker must not count as PassRole to ec2")
	}
	if !AllowsPassRoleTo(grants, roleARN, "sagemaker.amazonaws.com") {
		t.Error("the sagemaker PassRole grant should still be recognised for sagemaker")
	}

	// An authorized role gets an ec2-conditioned grant, which must count.
	authorized := mustGrants(t, irPolicy(t, []string{irRole}))
	if !AllowsPassRoleTo(authorized, roleARN, "ec2.amazonaws.com") {
		t.Error("an authorized role must be passable to ec2")
	}
}

func TestConditionPermitsService(t *testing.T) {
	cases := []struct {
		condition, service string
		want               bool
		why                string
	}{
		{"", "ec2.amazonaws.com", true, "no condition permits any service"},
		{`{"StringEquals":{"iam:PassedToService":"ec2.amazonaws.com"}}`, "ec2.amazonaws.com", true, "exact match"},
		{`{"StringEquals":{"iam:PassedToService":"sagemaker.amazonaws.com"}}`, "ec2.amazonaws.com", false, "different service"},
		{`{"StringEquals":{"iam:PassedToService":["ec2.amazonaws.com","x"]}}`, "ec2.amazonaws.com", true, "list form"},
		{`{"StringEquals":{"aws:SourceVpc":"vpc-1"}}`, "ec2.amazonaws.com", false, "condition key we don't model: don't claim coverage"},
		{`{"DateGreaterThan":{"aws:CurrentTime":"2026-01-01"}}`, "ec2.amazonaws.com", false, "operator we don't model"},
		{`not json`, "ec2.amazonaws.com", false, "unparseable: don't claim coverage"},
	}
	for _, c := range cases {
		if got := conditionPermitsService(c.condition, c.service); got != c.want {
			t.Errorf("conditionPermitsService(%q, %q) = %v, want %v — %s", c.condition, c.service, got, c.want, c.why)
		}
	}
}

func TestAllowsWildcardSemantics(t *testing.T) {
	grants := []Grant{
		{Effect: "Allow", Action: "iam:GetRole", Resource: "arn:aws:iam::1:role/spawn-instance*"},
		{Effect: "Allow", Action: "ec2:*", Resource: "*"},
		{Effect: "Allow", Action: "s3:GetObject", Resource: "arn:aws:s3:::b/?.txt"},
	}
	cases := []struct {
		action, resource string
		want             bool
		why              string
	}{
		{"iam:GetRole", "arn:aws:iam::1:role/spawn-instance-abc", true, "prefix wildcard matches"},
		{"iam:GetRole", "arn:aws:iam::1:role/loop-node-role", false, "prefix wildcard must not match a different name"},
		{"IAM:GETROLE", "arn:aws:iam::1:role/spawn-instance-abc", true, "actions are case-insensitive"},
		{"ec2:RunInstances", "anything", true, "action wildcard + resource *"},
		{"iam:PassRole", "arn:aws:iam::1:role/spawn-instance-abc", false, "unrelated action"},
		{"s3:GetObject", "arn:aws:s3:::b/a.txt", true, "? matches one char"},
		{"s3:GetObject", "arn:aws:s3:::b/ab.txt", false, "? matches exactly one char"},
	}
	for _, c := range cases {
		if got := Allows(grants, c.action, c.resource); got != c.want {
			t.Errorf("Allows(%s, %s) = %v, want %v — %s", c.action, c.resource, got, c.want, c.why)
		}
	}
}

// TestAllowsDenyBeatsAllow — the policy lagotto writes has no Deny today, but an
// operator-edited one might, and reporting a denied role as usable would send a
// watch to its death.
func TestAllowsDenyBeatsAllow(t *testing.T) {
	grants := []Grant{
		{Effect: "Allow", Action: "iam:GetRole", Resource: "*"},
		{Effect: "Deny", Action: "iam:GetRole", Resource: "arn:aws:iam::1:role/loop-node-role"},
	}
	if Allows(grants, "iam:GetRole", "arn:aws:iam::1:role/loop-node-role") {
		t.Error("an explicit Deny must beat a wildcard Allow")
	}
	if !Allows(grants, "iam:GetRole", "arn:aws:iam::1:role/other") {
		t.Error("the Deny must not leak to unrelated resources")
	}
}

// TestGrantCriticality is the #170 doctor bug in miniature: servicequotas reads are
// reporting-only, everything else breaks a watch.
func TestGrantCriticality(t *testing.T) {
	if GrantIsRequired("servicequotas:GetServiceQuota") || GrantIsRequired("servicequotas:ListServiceQuotas") {
		t.Error("servicequotas reads are reporting-only — quotaReport fails open")
	}
	for _, required := range []string{"pricing:GetProducts", "iam:GetRole", "ec2:RunInstances", "dynamodb:PutItem"} {
		if !GrantIsRequired(required) {
			t.Errorf("%s must be treated as required", required)
		}
	}

	req, rep := PartitionByCriticality([]Grant{
		{Effect: "Allow", Action: "servicequotas:GetServiceQuota", Resource: "*"},
		{Effect: "Allow", Action: "iam:GetRole", Resource: "*"},
	})
	if len(req) != 1 || req[0].Action != "iam:GetRole" {
		t.Errorf("required = %v, want just iam:GetRole", req)
	}
	if len(rep) != 1 || rep[0].Action != "servicequotas:GetServiceQuota" {
		t.Errorf("reporting-only = %v, want just servicequotas:GetServiceQuota", rep)
	}
}

func containsStr(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
