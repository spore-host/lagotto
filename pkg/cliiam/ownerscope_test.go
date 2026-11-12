package cliiam

import (
	"encoding/json"
	"testing"

	substrate "github.com/scttfrdmn/substrate/emulator"
)

// TestOwnerScopedItemAccess evaluates the generated lagotto-cli-user policy
// with substrate's IAM evaluator (the AWS evaluation algorithm, policy
// variables and set operators included): a caller reaches only the records
// under its own principal id. The same cases are the live
// simulate-principal-policy checks in the release notes, since the evaluator
// is a model of IAM, not IAM.
func TestOwnerScopedItemAccess(t *testing.T) {
	doc, err := PolicyDocument(testAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	var pd substrate.PolicyDocument
	if err := json.Unmarshal([]byte(doc), &pd); err != nil {
		t.Fatal(err)
	}
	const (
		table = "arn:aws:dynamodb:us-east-1:" + testAccount + ":table/lagotto-watches"
		index = "arn:aws:dynamodb:us-east-1:" + testAccount + ":table/lagotto-watches/index/owner_id-index"
		hist  = "arn:aws:dynamodb:us-east-1:" + testAccount + ":table/lagotto-match-history"
		alice = "AIDAALICEEXAMPLE"
		role  = "AROAROLEEXAMPLE:alice"
	)
	cases := []struct {
		name, action, resource, userid string
		keys                           []string // nil = no partition key in the request
		allow                          bool
	}{
		{"own watch", "dynamodb:GetItem", table, alice, []string{alice + "/w-1"}, true},
		{"own write", "dynamodb:UpdateItem", table, alice, []string{alice + "/w-1"}, true},
		{"own create", "dynamodb:PutItem", table, alice, []string{alice + "/w-2"}, true},
		{"own history", "dynamodb:Query", hist, alice, []string{alice + "/w-1"}, true},
		{"another owner's watch", "dynamodb:GetItem", table, alice, []string{"AIDABOBEXAMPLE/w-1"}, false},
		{"another owner's write", "dynamodb:UpdateItem", table, alice, []string{"AIDABOBEXAMPLE/w-1"}, false},
		{"write under another prefix", "dynamodb:PutItem", table, alice, []string{"AIDABOBEXAMPLE/w-9"}, false},
		{"an id that extends the caller's", "dynamodb:GetItem", table, alice, []string{alice + "X/w-1"}, false},
		{"a legacy key", "dynamodb:GetItem", table, alice, []string{"w-1"}, false},
		{"a batch with one foreign key", "dynamodb:BatchGetItem", table, alice, []string{alice + "/w-1", "AIDABOBEXAMPLE/w-2"}, false},
		{"no partition key (scan-shaped)", "dynamodb:Query", table, alice, nil, false},
		{"scan", "dynamodb:Scan", table, alice, nil, false},
		{"role session, own", "dynamodb:GetItem", table, role, []string{role + "/w-1"}, true},
		{"role session, another session", "dynamodb:GetItem", table, role, []string{"AROAROLEEXAMPLE:bob/w-1"}, false},
		{"owner index, own id", "dynamodb:Query", index, alice, []string{alice}, true},
		{"owner index, another id", "dynamodb:Query", index, alice, []string{"AIDABOBEXAMPLE"}, false},
		{"delete own", "dynamodb:DeleteItem", table, alice, []string{alice + "/w-1"}, false},
	}
	for _, c := range cases {
		req := substrate.EvaluationRequest{
			Action: c.action, Resource: c.resource,
			Context: map[string]string{"aws:userid": c.userid},
		}
		if c.keys != nil {
			req.MultiContext = map[string][]string{"dynamodb:LeadingKeys": c.keys}
		}
		got := substrate.Evaluate([]substrate.PolicyDocument{pd}, req).Decision == substrate.DecisionAllow
		if got != c.allow {
			t.Errorf("%s: %s on %s as %s with keys %v: allowed = %v, want %v", c.name, c.action, c.resource, c.userid, c.keys, got, c.allow)
		}
	}
}
