package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// TestReview4PlanStepGone (G5): a plan's account-wide step the account-wide
// teardown's fresh footprint no longer has is gone, not kept.
func TestReview4PlanStepGone(t *testing.T) {
	calls := 0
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		if calls++; calls > 2 {
			f.Account.Policy = nil // removed by hand since the plan
		}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New())
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	if err != nil || len(rec.calls) != 0 || !strings.Contains(out, "gone") || strings.Contains(out, "kept it") {
		t.Errorf("%v %v\n%s", err, rec.calls, out)
	}
}

// TestReview4WriteKept (G1): an explicit teardown prints what the
// account-wide teardown kept, with why, once per resource.
func TestReview4WriteKept(t *testing.T) {
	policy := fp.Resource{ARN: "arn:aws:iam::123456789012:policy/lagotto-cli-user", Type: inventory.TypePolicy, Name: "lagotto-cli-user"}
	role := fp.Resource{ARN: "arn:aws:iam::123456789012:role/x", Type: inventory.TypeRole, Name: "x"}
	var out bytes.Buffer
	writeAccountWide(&out, idle.AccountWideResult{Outcome: idle.AccountWideNothing,
		Kept: []inventory.Kept{{Resource: policy, Reason: "in use: attached to 1 user(s), group(s) or role(s)"}}})
	if got := out.String(); got != "Kept IAM policy lagotto-cli-user (in use: attached to 1 user(s), group(s) or role(s))\n" {
		t.Errorf("nothing else: %q", got)
	}
	out.Reset()
	writeAccountWide(&out, idle.AccountWideResult{Outcome: idle.AccountWideRemoved,
		Result: &fp.Result{Actions: []fp.Action{{ARN: role.ARN, Type: role.Type, Name: role.Name, Outcome: fp.OutcomeKept, Reason: "a dependency"}}},
		Kept:   []inventory.Kept{{Resource: role, Reason: "a dependency"}, {Resource: policy, Reason: "not lagotto's to delete: untagged"}}})
	if got := out.String(); strings.Count(got, " x ") != 1 || !strings.Contains(got, "Kept IAM policy lagotto-cli-user (not lagotto's to delete: untagged)") {
		t.Errorf("with actions: %q", got)
	}
}

// TestReview4IAMRolesReportsKept (G1): `teardown --iam-roles` says why
// lagotto-cli-user stays (something is attached to it), through the real
// account-wide teardown.
func TestReview4IAMRolesReportsKept(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		f.Account.PolicyAttachments = 1
		return f
	}
	stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New())
	r := runAccountWide(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
	var out bytes.Buffer
	writeAccountWide(&out, r)
	if r.Outcome != idle.AccountWideNothing || !strings.Contains(out.String(), "Kept IAM policy lagotto-cli-user (in use: attached to 1") {
		t.Errorf("%+v\n%s", r, out.String())
	}
}

// TestReview4LostMarkReplyEnded (libs finding 6, in lagotto): a creator's
// mark that committed though its answer was lost is ended when the creator
// is refused.
func TestReview4LostMarkReplyEnded(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return &fakeTeardownJobs{schedule: true} }
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	if _, err := c.SetIdleIfUnchanged(ctx, inventory.Tool, "us-west-2", lifecycle.Expect{}); err != nil {
		t.Fatal(err) // the table, and the item
	}
	lost := false
	table.Err = func(op string, in any) error {
		if tw, ok := in.(*dynamodb.TransactWriteItemsInput); ok && op == "TransactWriteItems" && !lost {
			lost = true
			table.Err = nil
			_, _ = table.TransactWriteItems(ctx, tw)
			return errors.New("connection reset")
		}
		return nil
	}
	if _, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "create watch", creatorHosted, &bytes.Buffer{}); err == nil {
		t.Fatal("a lost answer is a refusal")
	}
	if it, ok, _ := c.Get(ctx, inventory.Tool, "us-west-2"); !ok || len(it.Marks) != 0 || it.Epoch != 1 {
		t.Errorf("the orphan mark was left: %+v", it)
	}
}

// TestReview4FinishFailureGivesBack: `lagotto teardown` gives its region
// lease back when the finish fails, so no creator waits for it.
func TestReview4FinishFailureGivesBack(t *testing.T) {
	ctx := context.Background()
	tables := &fakeTables{empty: true}
	stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, false, false)
	table := lifecycletest.New()
	installFakeLeases(t, table)
	c, _ := newLifecycleClient(aws.Config{}, "deploy")
	if _, err := c.SetIdleIfUnchanged(ctx, inventory.Tool, "us-west-2", lifecycle.Expect{}); err != nil {
		t.Fatal(err)
	}
	table.Err = func(op string, _ any) error {
		if op == "DeleteItem" {
			return errors.New("network down")
		}
		return nil
	}
	out, err := runTeardownForTest(t)
	if err != nil || !strings.Contains(out, "was left in place") {
		t.Fatalf("%v\n%s", err, out)
	}
	table.Err = nil
	if it, ok, _ := c.Get(ctx, inventory.Tool, "us-west-2"); !ok || it.Lease != nil {
		t.Errorf("the lease was kept: %+v", it)
	}
}

// TestReview4LostGlobalMarkReplyEnded: the same for the global mark (setup).
func TestReview4LostGlobalMarkReplyEnded(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	if _, err := c.SetIdleIfUnchanged(ctx, inventory.Tool, lifecycle.ScopeGlobal, lifecycle.Expect{}); err != nil {
		t.Fatal(err)
	}
	table.Err = func(op string, in any) error {
		if tw, ok := in.(*dynamodb.TransactWriteItemsInput); ok && op == "TransactWriteItems" {
			table.Err = nil
			_, _ = table.TransactWriteItems(ctx, tw)
			return errors.New("connection reset")
		}
		return nil
	}
	if _, err := markGlobal(ctx, aws.Config{}, "", "set up lagotto's IAM", &bytes.Buffer{}); err == nil {
		t.Fatal("a lost answer is a refusal")
	}
	// setup marks its home region first (lagotto#351 review 5): that is the
	// write whose answer was lost here.
	if it, ok, _ := c.Get(ctx, inventory.Tool, "us-east-1"); !ok || len(it.Marks) != 0 || it.Epoch != 1 {
		t.Errorf("the orphan mark was left: %+v", it)
	}
	if it, _, _ := c.Get(ctx, inventory.Tool, lifecycle.ScopeGlobal); len(it.Marks) != 0 {
		t.Errorf("a global mark after the refusal: %+v", it)
	}
}
