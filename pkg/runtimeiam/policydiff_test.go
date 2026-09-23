package runtimeiam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

const (
	testRegion  = "us-west-2"
	testAccount = "123456789012"
)

// mustPolicy returns this binary's expected policy document.
func mustPolicy(t *testing.T) string {
	t.Helper()
	doc, err := PolicyDocument(testRegion, testAccount)
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	return doc
}

// TestDiffPolicy_IdenticalIsEmpty: the happy path. A deployed policy that IS this
// binary's policy must produce no diff at all.
func TestDiffPolicy_IdenticalIsEmpty(t *testing.T) {
	doc := mustPolicy(t)
	diff, err := DiffPolicy(doc, doc)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if !diff.Empty() {
		t.Errorf("identical policies produced a diff: missing=%v extra=%v", diff.Missing, diff.Extra)
	}
}

// TestDiffPolicy_NormalizationGuard is the false-alarm guard, and the single most
// important test here.
//
// A deployed policy comes back from IAM re-serialized: URL-encoded, with its own
// key order, its own whitespace, and no guarantee that the statements are grouped
// or the Action/Resource arrays ordered the way this binary emitted them. A raw
// text diff would flag every one of those as drift and be useless. The comparison
// must see through ALL of it — otherwise doctor cries wolf on every healthy
// account, which is worse than not having it.
func TestDiffPolicy_NormalizationGuard(t *testing.T) {
	expected := mustPolicy(t)

	for _, tc := range []struct {
		name   string
		mangle func(t *testing.T, doc string) string
	}{
		{"url-encoded (as IAM returns it)", func(_ *testing.T, doc string) string {
			return url.PathEscape(doc)
		}},
		{"re-indented with different key order", func(t *testing.T, doc string) string {
			return reserialize(t, doc, nil)
		}},
		{"action and resource arrays reversed", func(t *testing.T, doc string) string {
			return reserialize(t, doc, func(statements []map[string]interface{}) []map[string]interface{} {
				for _, st := range statements {
					reverseField(st, "Action")
					reverseField(st, "Resource")
				}
				return statements
			})
		}},
		{"statements reordered", func(t *testing.T, doc string) string {
			return reserialize(t, doc, func(statements []map[string]interface{}) []map[string]interface{} {
				out := make([]map[string]interface{}, 0, len(statements))
				for i := len(statements) - 1; i >= 0; i-- {
					out = append(out, statements[i])
				}
				return out
			})
		}},
		{"one statement split into two with the same grants", func(t *testing.T, doc string) string {
			return reserialize(t, doc, splitFirstMultiActionStatement)
		}},
		{"a single-element Action array written as a bare string", func(t *testing.T, doc string) string {
			return reserialize(t, doc, func(statements []map[string]interface{}) []map[string]interface{} {
				for _, st := range statements {
					if as, ok := st["Action"].([]interface{}); ok && len(as) == 1 {
						st["Action"] = as[0]
					}
					if rs, ok := st["Resource"].([]interface{}); ok && len(rs) == 1 {
						st["Resource"] = rs[0]
					}
				}
				return statements
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deployed := tc.mangle(t, expected)
			diff, err := DiffPolicy(deployed, expected)
			if err != nil {
				t.Fatalf("DiffPolicy: %v", err)
			}
			if !diff.Empty() {
				t.Errorf("a semantically identical policy was reported as drifted.\nmissing=%v\nextra=%v",
					diff.MissingSummary(), diff.ExtraSummary())
			}
		})
	}
}

// TestDiffPolicy_MissingActionFails is the #151 shape: the deployed policy predates
// a grant the current code needs. The diff must NAME the missing action — "your
// policy is out of date" without saying which permission is gone is not actionable.
func TestDiffPolicy_MissingActionFails(t *testing.T) {
	expected := mustPolicy(t)
	deployed := reserialize(t, expected, func(statements []map[string]interface{}) []map[string]interface{} {
		for _, st := range statements {
			dropAction(st, "pricing:GetProducts")
		}
		return statements
	})

	diff, err := DiffPolicy(deployed, expected)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if len(diff.Missing) == 0 {
		t.Fatal("a policy missing pricing:GetProducts produced no missing grants")
	}
	if !contains(diff.MissingActions(), "pricing:GetProducts") {
		t.Errorf("MissingActions() = %v, want it to name pricing:GetProducts", diff.MissingActions())
	}
	if len(diff.Extra) != 0 {
		t.Errorf("removing a grant should produce no EXTRA grants, got %v", diff.ExtraSummary())
	}
	// The summary a human reads must mention the action and its resource.
	joined := strings.Join(diff.MissingSummary(), "\n")
	if !strings.Contains(joined, "pricing:GetProducts") || !strings.Contains(joined, "*") {
		t.Errorf("MissingSummary() = %q, want the action and its resource", joined)
	}
}

// TestDiffPolicy_MissingResourceOnlyFails is the #149 shape: the ACTION is present
// but not on the resource the launcher needs (spawn renamed its instance roles, so
// iam:GetRole et al. were granted on spored* but not spawn-instance*). An
// action-only comparison would have called that policy fine.
func TestDiffPolicy_MissingResourceOnlyFails(t *testing.T) {
	expected := mustPolicy(t)
	target := fmt.Sprintf("arn:aws:iam::%s:role/spawn-instance*", testAccount)
	deployed := reserialize(t, expected, func(statements []map[string]interface{}) []map[string]interface{} {
		for _, st := range statements {
			dropResource(st, target)
		}
		return statements
	})

	diff, err := DiffPolicy(deployed, expected)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if len(diff.Missing) == 0 {
		t.Fatalf("dropping resource %s produced no missing grants", target)
	}
	if !contains(diff.MissingActions(), "iam:GetRole") {
		t.Errorf("MissingActions() = %v, want iam:GetRole (granted, but not on %s)", diff.MissingActions(), target)
	}
	for _, g := range diff.Missing {
		if g.Resource != target {
			continue
		}
		return // found at least one grant naming the dropped resource
	}
	t.Errorf("no missing grant names the dropped resource %s: %v", target, diff.MissingSummary())
}

// TestDiffPolicy_ExtraActionIsNotMissing is the "deployed policy is NEWER" half:
// a future lagotto's setup granted something this binary doesn't know about. That
// must land in Extra (a warning) and never in Missing (a failure) — running this
// binary's setup would REVERT it, which is worth saying, but nothing is broken.
func TestDiffPolicy_ExtraActionIsNotMissing(t *testing.T) {
	expected := mustPolicy(t)
	deployed := reserialize(t, expected, func(statements []map[string]interface{}) []map[string]interface{} {
		return append(statements, map[string]interface{}{
			"Effect":   "Allow",
			"Action":   []interface{}{"future:NewApi"},
			"Resource": "*",
		})
	})

	diff, err := DiffPolicy(deployed, expected)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if len(diff.Missing) != 0 {
		t.Errorf("an EXTRA grant must not be reported as missing, got %v", diff.MissingSummary())
	}
	if !contains(diff.ExtraActions(), "future:NewApi") {
		t.Errorf("ExtraActions() = %v, want future:NewApi", diff.ExtraActions())
	}
	if diff.Empty() {
		t.Error("diff reports empty despite an extra grant")
	}
}

// TestDiffPolicy_ConditionIsPartOfIdentity: an unconditioned iam:PassRole is a
// completely different (and much more dangerous) permission than the same PassRole
// scoped by iam:PassedToService. Dropping the condition must show up as drift, not
// as a match.
func TestDiffPolicy_ConditionIsPartOfIdentity(t *testing.T) {
	expected := mustPolicy(t)
	deployed := reserialize(t, expected, func(statements []map[string]interface{}) []map[string]interface{} {
		for _, st := range statements {
			delete(st, "Condition")
		}
		return statements
	})

	diff, err := DiffPolicy(deployed, expected)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if len(diff.Missing) == 0 || len(diff.Extra) == 0 {
		t.Fatalf("dropping every Condition should show as both missing (conditioned) and extra "+
			"(unconditioned) PassRole grants; missing=%v extra=%v", diff.MissingSummary(), diff.ExtraSummary())
	}
	if !contains(diff.MissingActions(), "iam:PassRole") {
		t.Errorf("MissingActions() = %v, want iam:PassRole", diff.MissingActions())
	}
}

// TestDiffPolicy_ConditionKeyOrderIsNormalized: a condition block that differs
// only in key order is the same condition. Go maps marshal sorted, so this is
// asserted against a hand-written statement with the keys the other way round.
func TestDiffPolicy_ConditionKeyOrderIsNormalized(t *testing.T) {
	a := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:PassRole","Resource":"*",
	  "Condition":{"StringEquals":{"iam:PassedToService":"ec2.amazonaws.com","aws:RequestedRegion":"us-west-2"}}}]}`
	b := `{"Statement":[{"Condition":{"StringEquals":{"aws:RequestedRegion":"us-west-2","iam:PassedToService":"ec2.amazonaws.com"}},
	  "Resource":["*"],"Action":["iam:PassRole"],"Effect":"Allow"}],"Version":"2012-10-17"}`
	diff, err := DiffPolicy(a, b)
	if err != nil {
		t.Fatalf("DiffPolicy: %v", err)
	}
	if !diff.Empty() {
		t.Errorf("conditions differing only in key order were reported as drift: missing=%v extra=%v",
			diff.MissingSummary(), diff.ExtraSummary())
	}
}

// TestParsePolicy_ExpandsCrossProduct pins the normalization unit: one statement
// with 2 actions × 3 resources is 6 grants, which is what makes statement grouping
// invisible to the comparison.
func TestParsePolicy_ExpandsCrossProduct(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	  "Action":["s3:GetObject","s3:PutObject"],"Resource":["a","b","c"]}]}`
	grants, err := ParsePolicy(doc)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	if len(grants) != 6 {
		t.Errorf("got %d grants, want 6 (2 actions × 3 resources): %v", len(grants), grants)
	}
}

// TestParsePolicy_SingleStatementObject: IAM accepts a lone statement object as
// well as an array, and a hand-edited deployed policy may well use it.
func TestParsePolicy_SingleStatementObject(t *testing.T) {
	grants, err := ParsePolicy(`{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}`)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	if len(grants) != 1 || grants[0].Action != "s3:GetObject" {
		t.Errorf("got %v, want a single s3:GetObject grant", grants)
	}
}

// TestParsePolicy_NotActionStaysOpaque: a NotAction/NotResource statement inverts
// the match, so expanding it into grants would MISREPRESENT it. It must still be
// compared (a change to it is drift) but described honestly.
func TestParsePolicy_NotActionStaysOpaque(t *testing.T) {
	grants, err := ParsePolicy(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","NotAction":["s3:Get*"],"Resource":"*"}]}`)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	if len(grants) != 1 {
		t.Fatalf("got %d grants, want 1 opaque grant: %v", len(grants), grants)
	}
	if !strings.Contains(grants[0].Action, "unexpanded") {
		t.Errorf("grant %v should be marked unexpanded rather than described action-by-action", grants[0])
	}
}

func TestParsePolicy_RejectsGarbage(t *testing.T) {
	for _, doc := range []string{"", "   ", "not json at all", `{"Version":"2012-10-17"}`} {
		if _, err := ParsePolicy(doc); err == nil {
			t.Errorf("ParsePolicy(%q) = nil error, want a parse failure", doc)
		}
	}
}

// TestDiffRuntimePolicy_UsesThisBinarysPolicy wires the convenience form: the
// expected side must be PolicyDocument(region, account), so a deployed policy
// scoped to the WRONG account shows as drift rather than passing.
func TestDiffRuntimePolicy_UsesThisBinarysPolicy(t *testing.T) {
	diff, err := DiffRuntimePolicy(mustPolicy(t), testRegion, testAccount)
	if err != nil {
		t.Fatalf("DiffRuntimePolicy: %v", err)
	}
	if !diff.Empty() {
		t.Errorf("own policy reported as drifted: %v / %v", diff.MissingSummary(), diff.ExtraSummary())
	}

	otherAccount, err := PolicyDocument(testRegion, "999999999999")
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	diff, err = DiffRuntimePolicy(otherAccount, testRegion, testAccount)
	if err != nil {
		t.Fatalf("DiffRuntimePolicy: %v", err)
	}
	if diff.Empty() {
		t.Error("a policy scoped to a different account reported no drift")
	}
}

// --- reading the deployed policy --------------------------------------------

// TestReadRuntimePolicy_RoundTripsWhatSetupWrote: the read path must see exactly
// what EnsureRuntimeRole put there, so doctor's PASS means something.
func TestReadRuntimePolicy_RoundTripsWhatSetupWrote(t *testing.T) {
	f := &fakeIAM{}
	if err := EnsureRuntimeRole(context.Background(), f, testRegion, testAccount); err != nil {
		t.Fatalf("EnsureRuntimeRole: %v", err)
	}
	doc, err := ReadRuntimePolicy(context.Background(), f)
	if err != nil {
		t.Fatalf("ReadRuntimePolicy: %v", err)
	}
	diff, err := DiffRuntimePolicy(doc, testRegion, testAccount)
	if err != nil {
		t.Fatalf("DiffRuntimePolicy: %v", err)
	}
	if !diff.Empty() {
		t.Errorf("policy written by setup does not read back equal: %v / %v",
			diff.MissingSummary(), diff.ExtraSummary())
	}
}

// TestReadRuntimePolicy_URLEncoded: real IAM returns the document URL-encoded and
// the SDK does not decode it.
func TestReadRuntimePolicy_URLEncoded(t *testing.T) {
	plain := mustPolicy(t)
	f := &fakeIAM{
		existing: map[string]bool{RoleName: true},
		inline:   map[string]string{RoleName + "/" + PolicyName: url.PathEscape(plain)},
	}
	doc, err := ReadRuntimePolicy(context.Background(), f)
	if err != nil {
		t.Fatalf("ReadRuntimePolicy: %v", err)
	}
	if !json.Valid([]byte(doc)) {
		t.Fatalf("ReadRuntimePolicy returned a document that is not JSON: %.80q", doc)
	}
}

// TestReadRuntimePolicy_AbsenceIsDistinguished: IAM answers a missing ROLE and a
// missing POLICY with the same NoSuchEntity, but the fix differs (deploy vs
// setup), so the two must be reported as different sentinels.
func TestReadRuntimePolicy_AbsenceIsDistinguished(t *testing.T) {
	// No role at all → never deployed.
	if _, err := ReadRuntimePolicy(context.Background(), &fakeIAM{}); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("err = %v, want ErrRoleNotFound", err)
	}
	// Role present, no inline policy → deployed but never set up.
	f := &fakeIAM{existing: map[string]bool{RoleName: true}}
	if _, err := ReadRuntimePolicy(context.Background(), f); !errors.Is(err, ErrPolicyNotFound) {
		t.Errorf("err = %v, want ErrPolicyNotFound", err)
	}
}

// TestReadRuntimePolicy_AccessDeniedIsNotAbsence: an AccessDenied must NOT be
// reported as "the policy isn't there", or the user goes off running `setup` to
// fix a permissions problem.
func TestReadRuntimePolicy_AccessDeniedIsNotAbsence(t *testing.T) {
	denied := errors.New("AccessDenied: not authorized to perform iam:GetRolePolicy")
	f := &fakeIAM{existing: map[string]bool{RoleName: true}, getPolicyErr: denied}
	_, err := ReadRuntimePolicy(context.Background(), f)
	if err == nil {
		t.Fatal("want an error")
	}
	if errors.Is(err, ErrRoleNotFound) || errors.Is(err, ErrPolicyNotFound) {
		t.Errorf("AccessDenied was classified as absence: %v", err)
	}
	if !errors.Is(err, denied) {
		t.Errorf("err = %v, want it to wrap the underlying AccessDenied", err)
	}
}

func TestRoleExists(t *testing.T) {
	f := &fakeIAM{existing: map[string]bool{RoleName: true}}
	for name, want := range map[string]bool{RoleName: true, SchedulerInvokeRoleName: false} {
		got, err := RoleExists(context.Background(), f, name)
		if err != nil {
			t.Fatalf("RoleExists(%s): %v", name, err)
		}
		if got != want {
			t.Errorf("RoleExists(%s) = %v, want %v", name, got, want)
		}
	}
	// A non-NoSuchEntity error is an error, not "absent".
	boom := &fakeIAM{getRoleErr: errors.New("AccessDenied")}
	if _, err := RoleExists(context.Background(), boom, RoleName); err == nil {
		t.Error("want the AccessDenied surfaced rather than reported as absent")
	}
}

// TestGrantString covers the rendering used in the report.
func TestGrantString(t *testing.T) {
	g := Grant{Effect: "Allow", Action: "ec2:RunInstances", Resource: "*"}
	if got, want := g.String(), "ec2:RunInstances on *"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	g.Condition = `{"StringEquals":{"iam:PassedToService":"ec2.amazonaws.com"}}`
	if !strings.Contains(g.String(), "when") {
		t.Errorf("String() = %q, want it to surface the condition", g.String())
	}
	g = Grant{Effect: "Deny", Action: "ec2:RunInstances", Resource: "*"}
	if !strings.Contains(g.String(), "DENY") {
		t.Errorf("String() = %q, want the non-Allow effect made loud", g.String())
	}
}

// --- test helpers -----------------------------------------------------------

// reserialize round-trips a policy document through generic maps — which changes
// key order (Go marshals map keys sorted) and whitespace — applying an optional
// mutation to the statement list on the way through. Every normalization case is
// built with it, so the cases test the comparison rather than a string edit.
func reserialize(t *testing.T, doc string, mutate func([]map[string]interface{}) []map[string]interface{}) string {
	t.Helper()
	var envelope struct {
		Version   string                   `json:"Version"`
		Statement []map[string]interface{} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(doc), &envelope); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if mutate != nil {
		envelope.Statement = mutate(envelope.Statement)
	}
	out := map[string]interface{}{"Version": envelope.Version, "Statement": envelope.Statement}
	b, err := json.MarshalIndent(out, "  ", "    ")
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	return string(b)
}

func reverseField(st map[string]interface{}, field string) {
	vs, ok := st[field].([]interface{})
	if !ok {
		return
	}
	for i, j := 0, len(vs)-1; i < j; i, j = i+1, j-1 {
		vs[i], vs[j] = vs[j], vs[i]
	}
	st[field] = vs
}

// splitFirstMultiActionStatement splits one statement's action list across two
// statements with the same resources — a regrouping that changes nothing about
// what is permitted, and would rewrite every line of a text diff.
func splitFirstMultiActionStatement(statements []map[string]interface{}) []map[string]interface{} {
	for i, st := range statements {
		actions, ok := st["Action"].([]interface{})
		if !ok || len(actions) < 2 {
			continue
		}
		half := len(actions) / 2
		first := map[string]interface{}{}
		second := map[string]interface{}{}
		for k, v := range st {
			first[k] = v
			second[k] = v
		}
		first["Action"] = actions[:half]
		second["Action"] = actions[half:]
		out := append([]map[string]interface{}{}, statements[:i]...)
		out = append(out, first, second)
		out = append(out, statements[i+1:]...)
		return out
	}
	return statements
}

// dropAction removes one action from a statement's action list (the "deployed
// policy predates this grant" case).
func dropAction(st map[string]interface{}, action string) {
	actions, ok := st["Action"].([]interface{})
	if !ok {
		return
	}
	kept := make([]interface{}, 0, len(actions))
	for _, a := range actions {
		if s, _ := a.(string); s == action {
			continue
		}
		kept = append(kept, a)
	}
	st["Action"] = kept
}

// dropResource removes one resource ARN from a statement (the #149 case: the
// action is granted, just not on the ARN the launcher uses).
func dropResource(st map[string]interface{}, resource string) {
	resources, ok := st["Resource"].([]interface{})
	if !ok {
		return
	}
	kept := make([]interface{}, 0, len(resources))
	for _, r := range resources {
		if s, _ := r.(string); s == resource {
			continue
		}
		kept = append(kept, r)
	}
	st["Resource"] = kept
}
