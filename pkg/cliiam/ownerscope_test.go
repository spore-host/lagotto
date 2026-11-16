package cliiam

import (
	"encoding/json"
	"testing"

	substrate "github.com/scttfrdmn/substrate/emulator"
	"github.com/spore-host/lagotto/pkg/watcher"
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
		// The poller-control item (#323): UpdateItem of its epoch, nothing else.
		{"bump the poller epoch", "dynamodb:UpdateItem", table, alice, []string{watcher.PollerControlKey}, true},
		{"bump the poller epoch, role session", "dynamodb:UpdateItem", table, role, []string{watcher.PollerControlKey}, true},
		{"read the control item", "dynamodb:GetItem", table, alice, []string{watcher.PollerControlKey}, false},
		{"overwrite the control item", "dynamodb:PutItem", table, alice, []string{watcher.PollerControlKey}, false},
		{"delete the control item", "dynamodb:DeleteItem", table, alice, []string{watcher.PollerControlKey}, false},
		{"control key on another table", "dynamodb:UpdateItem", hist, alice, []string{watcher.PollerControlKey}, false},
		{"control key batched with a foreign key", "dynamodb:UpdateItem", table, alice, []string{watcher.PollerControlKey, "AIDABOBEXAMPLE/w-1"}, false},
	}
	for _, c := range cases {
		req := substrate.EvaluationRequest{
			Action: c.action, Resource: c.resource,
			Context: map[string]string{"aws:userid": c.userid},
		}
		if c.keys != nil {
			req.MultiContext = map[string][]string{"dynamodb:LeadingKeys": c.keys}
			if c.keys[0] == watcher.PollerControlKey {
				req.MultiContext["dynamodb:Attributes"] = []string{"watch_id", watcher.PollerEpochAttr}
			}
		}
		got := substrate.Evaluate([]substrate.PolicyDocument{pd}, req).Decision == substrate.DecisionAllow
		if got != c.allow {
			t.Errorf("%s: %s on %s as %s with keys %v: allowed = %v, want %v", c.name, c.action, c.resource, c.userid, c.keys, got, c.allow)
		}
	}
}

// TestPollerControlWritesOnlyTheEpoch: the PollerControl statement (#323) lets
// a CLI user increment the poller epoch, and nothing else on that item. In
// particular not a status, which would put the item in the status index the
// poller lists active watches from.
func TestPollerControlWritesOnlyTheEpoch(t *testing.T) {
	doc, err := PolicyDocument(testAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	var pd substrate.PolicyDocument
	if err := json.Unmarshal([]byte(doc), &pd); err != nil {
		t.Fatal(err)
	}
	table := "arn:aws:dynamodb:us-east-1:" + testAccount + ":table/lagotto-watches"
	for _, c := range []struct {
		name  string
		attrs []string
		allow bool
	}{
		{"the epoch", []string{"watch_id", watcher.PollerEpochAttr}, true},
		{"the epoch alone", []string{watcher.PollerEpochAttr}, true},
		{"a status", []string{"watch_id", watcher.PollerEpochAttr, "status"}, false},
		{"the poller's mode", []string{"watch_id", "mode"}, false},
	} {
		req := substrate.EvaluationRequest{
			Action: "dynamodb:UpdateItem", Resource: table,
			Context: map[string]string{"aws:userid": "AIDAALICEEXAMPLE"},
			MultiContext: map[string][]string{
				"dynamodb:LeadingKeys": {watcher.PollerControlKey},
				"dynamodb:Attributes":  c.attrs,
			},
		}
		got := substrate.Evaluate([]substrate.PolicyDocument{pd}, req).Decision == substrate.DecisionAllow
		if got != c.allow {
			t.Errorf("UpdateItem of %s (%v) on the control item: allowed = %v, want %v", c.name, c.attrs, got, c.allow)
		}
	}
}
