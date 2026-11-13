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
	doc, err := PolicyDocument(irAccount, roles)
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

// TestStockPolicyDoesNotCoverANamedRole is lagotto#170 itself: without an
// explicit authorization the poller may not pass a user-named role to EC2.
func TestStockPolicyDoesNotCoverANamedRole(t *testing.T) {
	grants := mustGrants(t, irPolicy(t, nil))

	missing := MissingInstanceRoleGrants(grants, irAccount, irRole)
	if len(missing) != 1 || missing[0] != InstanceRolePassAction {
		t.Fatalf("stock policy: missing = %v, want [%s]", missing, InstanceRolePassAction)
	}
	// lagotto's own instance role is always passable.
	if missing := MissingInstanceRoleGrants(grants, irAccount, InstanceRoleName); len(missing) != 0 {
		t.Errorf("stock policy must let the poller pass %s: missing %v", InstanceRoleName, missing)
	}
}

// TestPollerAuthorsNoIAM is the confused-deputy fix: no grant in the poller's
// policy lets it create, tag, write or attach a policy to any role or instance
// profile, with or without authorized roles, and PassRole is never on a
// wildcard role.
func TestPollerAuthorsNoIAM(t *testing.T) {
	doc, err := PolicyDocumentFor(irAccount, Authorizations{
		InstanceRoles:  []string{irRole},
		SageMakerRoles: []string{"arn:aws:iam::" + irAccount + ":role/service-role/sm-exec"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range mustGrants(t, doc) {
		a := strings.ToLower(g.Action)
		switch a {
		case "iam:createrole", "iam:putrolepolicy", "iam:attachrolepolicy", "iam:tagrole",
			"iam:createinstanceprofile", "iam:addroletoinstanceprofile", "iam:taginstanceprofile",
			"iam:updateassumerolepolicy", "iam:deleterolepermissionsboundary", "iam:putrolepermissionsboundary":
			t.Errorf("poller policy grants %s", g)
		case "iam:passrole":
			if strings.Contains(g.Resource, "*") {
				t.Errorf("PassRole on a wildcard resource: %s", g)
			}
			if g.Condition == "" {
				t.Errorf("PassRole without iam:PassedToService: %s", g)
			}
		}
		if strings.HasPrefix(a, "iam:") && a != "iam:passrole" && a != "iam:createservicelinkedrole" {
			t.Errorf("unexpected IAM grant on the poller: %s", g)
		}
	}
}

// TestAuthorizedRoleIsFullyCovered: one authorization lets the poller pass
// exactly that role to EC2, and no other.
func TestAuthorizedRoleIsFullyCovered(t *testing.T) {
	grants := mustGrants(t, irPolicy(t, []string{irRole}))

	if missing := MissingInstanceRoleGrants(grants, irAccount, irRole); len(missing) != 0 {
		t.Errorf("authorized role still missing grants: %v", missing)
	}
	if AllowsPassRoleTo(grants, "arn:aws:iam::"+irAccount+":role/"+irRole, "sagemaker.amazonaws.com") {
		t.Error("an EC2 instance-role authorization must not let the poller pass the role to SageMaker")
	}
	// Authorizing one role must not authorize another.
	if missing := MissingInstanceRoleGrants(grants, irAccount, "some-other-role"); len(missing) == 0 {
		t.Error("authorizing one role must not cover a different role")
	}
	for _, r := range []string{"spored-instance-role", "spawn-instance-abc123"} {
		if AllowsPassRoleTo(grants, "arn:aws:iam::"+irAccount+":role/"+r, "ec2.amazonaws.com") {
			t.Errorf("the poller may pass %s, a role spawn users create and change", r)
		}
	}
}

// TestSageMakerRolesRoundTrip: SageMaker authorizations are recovered from the
// deployed policy separately from instance roles, and the old PassRole on "*"
// is not carried forward.
func TestSageMakerRolesRoundTrip(t *testing.T) {
	sm := "arn:aws:iam::" + irAccount + ":role/service-role/sm-exec"
	doc, err := PolicyDocumentFor(irAccount, Authorizations{InstanceRoles: []string{irRole}, SageMakerRoles: []string{sm}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := AuthorizedSageMakerRoles(doc)
	if err != nil || len(got) != 1 || got[0] != sm {
		t.Fatalf("AuthorizedSageMakerRoles = %v, %v; want [%s]", got, err, sm)
	}
	roles, err := AuthorizedInstanceRoles(doc)
	if err != nil || len(roles) != 1 || roles[0] != irRole {
		t.Fatalf("AuthorizedInstanceRoles = %v, %v; want [%s]", roles, err, irRole)
	}
	legacy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:PassRole","Resource":"*",` +
		`"Condition":{"StringEquals":{"iam:PassedToService":"sagemaker.amazonaws.com"}}}]}`
	if got, _ := AuthorizedSageMakerRoles(legacy); len(got) != 0 {
		t.Errorf("legacy PassRole on * carried forward as %v", got)
	}
	for _, bad := range []string{"arn:aws:iam::" + irAccount + ":role/*", "arn:aws:iam::999999999999:role/x",
		"arn:aws:iam::" + irAccount + ":role/spored-instance-role", "sm-exec"} {
		if ValidateSageMakerRoleARN(bad, irAccount) == nil {
			t.Errorf("ValidateSageMakerRoleARN(%q) accepted", bad)
		}
	}
}

// TestReservedInstanceRolesRefused: spawn's shared role, its nested-spawn
// roles and lagotto's own roles can never be authorized.
func TestReservedInstanceRolesRefused(t *testing.T) {
	for _, name := range []string{"spored-instance-role", "Spored-Custom", "lagotto-capacity-poller-role", InstanceRoleName} {
		if ValidateInstanceRoleName(name) == nil {
			t.Errorf("ValidateInstanceRoleName(%q) accepted a reserved role", name)
		}
	}
	doc := irPolicy(t, []string{"spored-instance-role", irRole})
	if strings.Contains(doc, "role/spored-instance-role") {
		t.Error("a reserved role reached the policy")
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
// into: a PassRole grant conditioned to sagemaker.amazonaws.com must not count
// as permission to pass the same role to EC2, or an unauthorized role is
// reported usable and the watch dies at RunInstances anyway.
func TestPassRoleConditionIsNotIgnored(t *testing.T) {
	roleARN := "arn:aws:iam::" + irAccount + ":role/" + irRole
	doc, err := PolicyDocumentFor(irAccount, Authorizations{SageMakerRoles: []string{roleARN}})
	if err != nil {
		t.Fatal(err)
	}
	grants := mustGrants(t, doc)

	// Condition-blind: the sagemaker grant matches.
	if !Allows(grants, InstanceRolePassAction, roleARN) {
		t.Fatal("precondition: the policy grants PassRole on the role (for sagemaker)")
	}
	// Condition-aware: it must NOT count as permission to pass a role to EC2.
	if AllowsPassRoleTo(grants, roleARN, "ec2.amazonaws.com") {
		t.Error("PassRole conditioned to sagemaker must not count as PassRole to ec2")
	}
	if !AllowsPassRoleTo(grants, roleARN, "sagemaker.amazonaws.com") {
		t.Error("the sagemaker PassRole grant should still be recognised for sagemaker")
	}
	if roles, _ := AuthorizedInstanceRoles(doc); len(roles) != 0 {
		t.Errorf("a SageMaker role was read back as an instance role: %v", roles)
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
