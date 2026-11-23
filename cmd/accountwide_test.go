package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lastmile"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// TestMarkGlobal (lagotto#345): setup's mark of the global scope bumps its
// epoch and sets it ACTIVE; a caller whose IAM predates the table is warned
// and carries on; outside the aws partition there is no global scope; a
// client that cannot be built refuses.
func TestMarkGlobal(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	if err := markGlobal(ctx, aws.Config{}, "", "set up lagotto's IAM", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	if it, ok, _ := c.Get(ctx, inventory.Tool, lifecycle.ScopeGlobal); !ok || it.State != lifecycle.StateActive || it.Epoch != 1 {
		t.Errorf("global item %+v", it)
	}

	denied := lifecycletest.New()
	denied.Err = func(string, any) error { return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"} }
	installFakeLeases(t, denied)
	var out bytes.Buffer
	if err := markGlobal(ctx, aws.Config{}, "alice", "x", &out); err != nil || !strings.Contains(out.String(), "Continuing without the teardown lease") {
		t.Errorf("denied: %v %q", err, out.String())
	}

	installFakeLeases(t, lifecycletest.New(), func(o *lifecycle.Options) { o.ControlRegion = "" })
	if err := markGlobal(ctx, aws.Config{}, "alice", "x", &bytes.Buffer{}); err != nil {
		t.Errorf("no global scope: %v", err)
	}

	prev := newLifecycleClient
	t.Cleanup(func() { newLifecycleClient = prev })
	newLifecycleClient = func(aws.Config, string) (*lifecycle.Client, error) { return nil, errors.New("no client") }
	if err := markGlobal(ctx, aws.Config{}, "alice", "set up", &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "refusing to set up") {
		t.Errorf("no client: %v", err)
	}
	if r := runAccountWide(ctx, aws.Config{}, fpAcct, nil, func(string, ...any) {}); r.Outcome != idle.AccountWideFailed {
		t.Errorf("runAccountWide without a client: %+v", r)
	}
	if r := lastMileAccountWideGated(t, lastmile.ModeReap); r.Outcome != idle.AccountWideFailed {
		t.Errorf("the last mile's reap without a client: %+v", r)
	}
}

// lastMileAccountWideGated runs the last mile's account-wide teardown with
// the reap gate open.
func lastMileAccountWideGated(t *testing.T, mode lastmile.Mode) idle.AccountWideResult {
	t.Helper()
	prev := lastMileAccountWideReap
	lastMileAccountWideReap = true
	t.Cleanup(func() { lastMileAccountWideReap = prev })
	var logs []string
	return lastMileAccountWide(context.Background(), mode, aws.Config{}, fpAcct, []string{"us-east-1"}, nil,
		&lastMileFootprint{load: func(context.Context) (inventory.Facts, error) { return inventory.Facts{}, nil }},
		func(f string, a ...any) { logs = append(logs, f) })
}

// TestRunAccountWide: `teardown --iam-roles`'s path reaches the real
// account-wide teardown with the command's seams: it removes the unattached
// policy of an account with nothing else, under the global lease.
func TestRunAccountWide(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	table := lifecycletest.New()
	installFakeLeases(t, table)
	var logs []string
	r := runAccountWide(context.Background(), aws.Config{}, fpAcct, nil, func(f string, a ...any) { logs = append(logs, f) })
	if r.Outcome != idle.AccountWideRemoved || len(rec.calls) != 1 || rec.calls[0] != "global policy" || len(logs) == 0 {
		t.Fatalf("%+v %v", r, rec.calls)
	}
	var out bytes.Buffer
	writeAccountWide(&out, r)
	if !strings.Contains(out.String(), "Deleted IAM policy lagotto-cli-user") {
		t.Errorf("output %q", out.String())
	}
}

// TestWriteAccountWide: every outcome's line, and each resource type's name.
func TestWriteAccountWide(t *testing.T) {
	var out bytes.Buffer
	writeAccountWide(&out, idle.AccountWideResult{})
	if out.Len() != 0 {
		t.Errorf("no result: %q", out.String())
	}
	writeAccountWide(&out, idle.AccountWideResult{Result: &fp.Result{Actions: []fp.Action{
		{Type: inventory.TypeRole, Name: "r", Outcome: fp.OutcomeDeleted},
		{Type: inventory.TypeInstanceProfile, Name: "p", Outcome: fp.OutcomeGone},
		{Type: inventory.TypePolicy, Name: "c", Outcome: fp.OutcomeFailed, Reason: "denied", Err: errors.New("AccessDenied")},
		{Type: "AWS::Other::Thing", Name: "x", Outcome: fp.OutcomeNotRun},
	}}})
	for _, want := range []string{"Deleted IAM role r", "Already absent: instance profile p",
		"Not deleted: IAM policy c (failed: denied: AccessDenied)", "Not deleted: Other::Thing x (not-run)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if err := accountWideError(idle.AccountWideResult{Outcome: idle.AccountWideHeld, Why: "a delete did not complete", Err: errors.New("boom")}); err == nil ||
		!strings.Contains(err.Error(), "(held): a delete did not complete: boom") {
		t.Errorf("held: %v", err)
	}
	if accountWideError(idle.AccountWideResult{Outcome: idle.AccountWideNothing}) != nil {
		t.Error("nothing is an error")
	}
}

// TestTeardownPlanReapNoGlobalScope: outside the aws partition there is no
// global lease: the plan's account-wide steps are refused, and with
// --i-understand-no-lease run under the stand-in.
func TestTeardownPlanReapNoGlobalScope(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New(), func(o *lifecycle.Options) { o.ControlRegion = "" })
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	if err == nil || !strings.Contains(err.Error(), "(skipped)") || len(rec.calls) != 0 {
		t.Fatalf("no global scope: %v, deleted %v\n%s", err, rec.calls, out)
	}
	teardownNoLease = true
	if out, err := runTeardownPlanForTest(t, ""); err != nil || len(rec.calls) != 1 {
		t.Errorf("--i-understand-no-lease: %v, deleted %v\n%s", err, rec.calls, out)
	}
}
