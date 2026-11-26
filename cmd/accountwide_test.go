package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lastmile"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
)

// TestMarkGlobal (lagotto#345): setup's mark of the global scope bumps its
// epoch and sets it ACTIVE; a caller whose IAM predates the table is warned
// and carries on; outside aws, aws-us-gov and aws-cn there is no global scope; a
// client that cannot be built refuses.
func TestMarkGlobal(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	done, err := markGlobal(ctx, aws.Config{}, "", "set up lagotto's IAM", &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	if it, ok, _ := c.Get(ctx, inventory.Tool, lifecycle.ScopeGlobal); !ok || it.State != lifecycle.StateActive || it.Epoch != 1 || len(it.Marks) != 1 {
		t.Errorf("global item %+v", it)
	}
	// Setup's end: the scope is idle from then, as read (lagotto#340; no
	// writer idles it).
	done()
	if it, _, _ := c.Get(ctx, inventory.Tool, lifecycle.ScopeGlobal); len(it.Marks) != 0 || c.InUse(it, idle.CreatorMarkBound) {
		t.Errorf("after done: %+v", it)
	}
	// A done that fails is a warning.
	var warned bytes.Buffer
	done2, _ := markGlobal(ctx, aws.Config{}, "", "x", &warned)
	table.Err = func(string, any) error { return errors.New("network down") }
	done2()
	table.Err = nil
	if !strings.Contains(warned.String(), "could not end this command's mark") {
		t.Errorf("a failing done: %q", warned.String())
	}

	denied := lifecycletest.New()
	denied.Err = func(string, any) error { return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"} }
	installFakeLeases(t, denied)
	var out bytes.Buffer
	if _, err := markGlobal(ctx, aws.Config{}, "alice", "x", &out); err != nil || !strings.Contains(out.String(), "Continuing without the teardown lease") {
		t.Errorf("denied: %v %q", err, out.String())
	}

	installFakeLeases(t, lifecycletest.New(), func(o *lifecycle.Options) { o.ControlRegion = "" })
	if done, err := markGlobal(ctx, aws.Config{}, "alice", "x", &bytes.Buffer{}); err != nil {
		t.Errorf("no global scope: %v", err)
	} else {
		done()
	}

	prev := newLifecycleClient
	t.Cleanup(func() { newLifecycleClient = prev })
	newLifecycleClient = func(aws.Config, string) (*lifecycle.Client, error) { return nil, errors.New("no client") }
	if _, err := markGlobal(ctx, aws.Config{}, "alice", "set up", &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "refusing to set up") {
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

// TestTeardownPlanReapNoGlobalScope: outside aws, aws-us-gov and aws-cn there is no
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

// TestTeardownFinishesTheRegionItem: `lagotto teardown` finishes its region's
// lifecycle item (an ACTIVE one, left by deploy and watch) once the tables are
// gone and no poller is left, so it no longer keeps the account-wide
// resources (lagotto#340); with a poller there, or when it cannot tell, the
// item stays.
func TestTeardownFinishesTheRegionItem(t *testing.T) {
	ctx := context.Background()
	// What deploy and watch leave: their marks, ended.
	mark := func(table *lifecycletest.Fake) *lifecycle.Client {
		c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
		token, err := c.Mark(ctx, inventory.Tool, "us-west-2")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.MarkDone(ctx, inventory.Tool, "us-west-2", token); err != nil {
			t.Fatal(err)
		}
		return c
	}
	tables := &fakeTables{empty: true}
	stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, false, false)
	table := lifecycletest.New()
	installFakeLeases(t, table)
	c := mark(table)
	out, err := runTeardownForTest(t)
	if err != nil || !strings.Contains(out, "Finished lagotto's spore-host-lifecycle item in us-west-2") {
		t.Fatalf("%v\n%s", err, out)
	}
	if _, ok, _ := c.Get(ctx, inventory.Tool, "us-west-2"); ok {
		t.Error("the item is left")
	}
	// Nothing there: nothing written.
	table = lifecycletest.New()
	installFakeLeases(t, table)
	if _, err := runTeardownForTest(t); err != nil || table.Calls("CreateTable") != 0 {
		t.Errorf("no item: %v, %d CreateTable", err, table.Calls("CreateTable"))
	}

	for name, set := range map[string]func(){
		"a poller": func() {
			teardownPollerExists = func(context.Context, aws.Config) (bool, error) { return true, nil }
		},
		"unknown": func() {
			teardownPollerExists = func(context.Context, aws.Config) (bool, error) { return false, errors.New("AccessDenied") }
		},
		"no client": func() {
			newLifecycleClient = func(aws.Config, string) (*lifecycle.Client, error) { return nil, errors.New("no client") }
		},
		"a creator running (watch --local)": func() {
			other, _ := newLifecycleClient(aws.Config{}, "alice")
			if _, err := other.Mark(ctx, inventory.Tool, "us-west-2"); err != nil {
				t.Fatal(err)
			}
		},
		"a live lease": func() {
			other, _ := newLifecycleClient(aws.Config{}, "lagotto-poller/us-west-2")
			if _, _, err := other.Observe(ctx, inventory.Tool, "us-west-2", false); err != nil {
				t.Fatal(err)
			}
			if _, err := other.Claim(ctx, inventory.Tool, "us-west-2", 0); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, false, false)
			table := lifecycletest.New()
			installFakeLeases(t, table)
			c := mark(table)
			prev := newLifecycleClient
			t.Cleanup(func() { newLifecycleClient = prev })
			set()
			out, err := runTeardownForTest(t)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, _ := c.Get(ctx, inventory.Tool, "us-west-2"); !ok {
				t.Errorf("the item was finished:\n%s", out)
			}
			if name != "a poller" && !strings.Contains(out, "was left in place") {
				t.Errorf("no warning:\n%s", out)
			}
		})
	}
}

// TestCancelTeardownJobs (lagotto#340): deploy deletes a region's pending
// teardown jobs always, a watch or launch only while the poller's schedule
// is there; an old policy warns, any other failure refuses.
func TestCancelTeardownJobs(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	f := &fakeTeardownJobs{jobs: []string{"lagotto-teardown-log-group", "lagotto-teardown-function"}}
	if err := cancelTeardownJobs(ctx, f, "us-west-2", "deploy the poller", creatorDeploy, &out); err != nil || len(f.deleted) != 2 || f.pages != 2 ||
		!strings.Contains(out.String(), "Deleted the pending teardown job lagotto-teardown-function in us-west-2") {
		t.Errorf("deploy: %v %v (%d pages)\n%s", err, f.deleted, f.pages, out.String())
	}
	f = &fakeTeardownJobs{jobs: []string{"lagotto-teardown-function"}}
	if err := cancelTeardownJobs(ctx, f, "us-west-2", "create watch w-1", creatorHosted, &out); err == nil || !strings.Contains(err.Error(), "has been torn down") || len(f.deleted) != 0 {
		t.Errorf("a watch with the region's teardown finished: %v %v", err, f.deleted)
	}
	f.schedule = true
	if err := cancelTeardownJobs(ctx, f, "us-west-2", "create watch w-1", creatorHosted, &out); err != nil || len(f.deleted) != 1 {
		t.Errorf("a watch with the poller's schedule there: %v %v", err, f.deleted)
	}
	if err := cancelTeardownJobs(ctx, &fakeTeardownJobs{}, "us-west-2", "deploy", creatorDeploy, &out); err != nil {
		t.Errorf("no group: %v", err)
	}
	denied := &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"}
	for _, kind := range []creatorKind{creatorDeploy, creatorHosted} {
		out.Reset()
		if err := cancelTeardownJobs(ctx, &fakeTeardownJobs{err: denied}, "us-west-2", "x", kind, &out); err != nil ||
			!strings.Contains(out.String(), "warning: could not delete the pending lagotto-teardown jobs") {
			t.Errorf("denied (kind %d): %v %q", kind, err, out.String())
		}
		// A creator is refused with nothing created; a deploy (which settles
		// its jobs after it deployed) is told the poller is there and the
		// jobs will delete it.
		want := "Nothing was created"
		if kind == creatorDeploy {
			want = "the poller was deployed in us-west-2, but its pending lagotto-teardown jobs could not be deleted"
		}
		if err := cancelTeardownJobs(ctx, &fakeTeardownJobs{err: errors.New("network down")}, "us-west-2", "x", kind, &out); err == nil ||
			!strings.Contains(err.Error(), want) {
			t.Errorf("failing (kind %d): %v", kind, err)
		}
	}
	// A delete that fails, or is denied, after the listing.
	d := &deleteFails{fakeTeardownJobs: fakeTeardownJobs{jobs: []string{"lagotto-teardown-function"}}}
	for _, err := range []error{&schedulertypes.ResourceNotFoundException{Message: aws.String("gone")}, denied, errors.New("boom")} {
		d.err = err
		got := cancelTeardownJobs(ctx, d, "us-west-2", "x", creatorDeploy, &bytes.Buffer{})
		if (got != nil) != (err.Error() == "boom") {
			t.Errorf("delete %v: %v", err, got)
		}
	}

	// markCreating: for a deploy, only the marks (it settles the jobs
	// once its schedule stands, settleDeployJobs).
	installFakeLeases(t, lifecycletest.New())
	f = &fakeTeardownJobs{jobs: []string{"lagotto-teardown-function"}}
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return f }
	if done, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "deploy", creatorDeploy, &bytes.Buffer{}); err != nil || len(f.deleted) != 0 {
		t.Errorf("markCreating: %v %v", err, f.deleted)
	} else {
		done()
	}
	// A watch or launch in a region whose poller was torn down: refused,
	// before anything is written; once the region's table was marked and
	// the schedule went since, refused after the marks (lagotto#349).
	table := lifecycletest.New()
	installFakeLeases(t, table)
	f = &fakeTeardownJobs{jobs: []string{"lagotto-teardown-function"}}
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return f }
	if _, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "arm a scheduled launch", creatorHosted, &bytes.Buffer{}); err == nil ||
		!strings.Contains(err.Error(), "has been torn down") || len(f.deleted) != 0 || table.Calls("TransactWriteItems") != 0 {
		t.Errorf("a launch in a torn-down region: %v %v", err, f.deleted)
	}
	if err := cancelTeardownJobs(ctx, f, "us-west-2", "arm a scheduled launch", creatorHosted, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "has been torn down") {
		t.Errorf("after the marks: %v", err)
	}
	// watch --local: no hosted poller needed, and the jobs stay.
	if done, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "create watch", creatorLocal, &bytes.Buffer{}); err != nil || len(f.deleted) != 0 {
		t.Errorf("watch --local: %v %v", err, f.deleted)
	} else {
		done()
	}
	// A regional table the caller cannot reach still marks the global scope.
	regional := lifecycletest.New()
	regional.Err = func(string, any) error { return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"} }
	global := lifecycletest.New()
	prev := newLifecycleClient
	t.Cleanup(func() { newLifecycleClient = prev })
	newLifecycleClient = func(_ aws.Config, owner string) (*lifecycle.Client, error) {
		return lifecycle.New(lifecycle.Options{DynamoDB: func(r string) (lifecycle.API, error) {
			if r == "us-east-1" {
				return global, nil
			}
			return regional, nil
		}, ControlRegion: "us-east-1", Owner: owner, Sleep: func(context.Context, time.Duration) error { return nil }})
	}
	var out2 bytes.Buffer
	if done, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "deploy", creatorDeploy, &out2); err != nil || global.Calls("TransactWriteItems") == 0 {
		t.Errorf("regional table unreachable: %v (%d global writes)\n%s", err, global.Calls("TransactWriteItems"), out2.String())
	} else {
		done()
	}
}

// deleteFails fails each DeleteSchedule with err.
type deleteFails struct {
	fakeTeardownJobs
	err error
}

func (d *deleteFails) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	return nil, d.err
}

// TestRunOwnerIsUnique: two runs never share a lease owner.
func TestRunOwnerIsUnique(t *testing.T) {
	a, b := runOwner(teardownOwner), runOwner(teardownOwner)
	if a == b || !strings.HasPrefix(a, teardownOwner+"/") {
		t.Errorf("%q %q", a, b)
	}
}

// TestTeardownPlanReapKeptAccountWide: a plan's account-wide step the
// account-wide teardown did not delete (the policy was attached since the
// plan) is reported kept, with the reason.
func TestTeardownPlanReapKeptAccountWide(t *testing.T) {
	calls := 0
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		if calls++; calls > 2 {
			f.Account.PolicyAttachments = 1
		}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New())
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	if err != nil || len(rec.calls) != 0 || !strings.Contains(out, "the account-wide teardown kept it") {
		t.Errorf("%v %v\n%s", err, rec.calls, out)
	}
}

// TestTeardownPlanReapAccountWideReadFailure: account-wide steps blocked by
// a read that failed are not run, and fail the command; only an in-use
// blocker keeps them, with exit 0.
func TestTeardownPlanReapAccountWideReadFailure(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New())
	accountWideSource = func(aws.Config, string) inventory.Source {
		s := sourceOf(facts()).(*inventorytest.Source)
		s.Fail("us-east-1 Poller", errors.New("AccessDenied"))
		return s
	}
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	if err == nil || !strings.Contains(err.Error(), "could not") || len(rec.calls) != 0 || !strings.Contains(out, "not-run") {
		t.Errorf("%v %v\n%s", err, rec.calls, out)
	}
}

// TestRunTeardown_IAMRolesChecksFirst: `teardown --iam-roles` runs the
// account-wide check before any regional delete, so a refusal changes
// nothing.
func TestRunTeardown_IAMRolesChecksFirst(t *testing.T) {
	tables := &fakeTables{empty: true}
	calls := stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, true, false)
	stubPollerRegions(t, nil, nil)
	var region string
	teardownAccountWideCheck = func(_ context.Context, _ aws.Config, _, r string) idle.AccountWideResult {
		region = r
		return idle.AccountWideResult{Outcome: idle.AccountWideBlocked, Why: "the account-wide resources are still needed",
			Blockers: []string{"eu-west-1: its lifecycle item is ACTIVE"}}
	}
	_, err := runTeardownForTest(t)
	if err == nil || !strings.Contains(err.Error(), "not deleting anything") || tables.deleted || len(*calls) != 0 || region != "us-west-2" {
		t.Errorf("%v tables=%v calls=%v region=%q", err, tables.deleted, *calls, region)
	}
}

// TestCheckAccountWide: the check is report mode, excluding the caller's
// region's own item and resources, and writes nothing.
func TestCheckAccountWide(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1", Tables: fpTables("us-east-1")}}
		return f
	}
	stubPlanTeardown(t, facts)
	table := lifecycletest.New()
	installFakeLeases(t, table)
	c, _ := newLifecycleClient(aws.Config{}, "a deploy")
	tok, err := c.Mark(context.Background(), inventory.Tool, "us-east-1") // what a deploy leaves: its mark, ended
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.MarkDone(context.Background(), inventory.Tool, "us-east-1", tok); err != nil {
		t.Fatal(err)
	}
	writes := table.Calls("TransactWriteItems") + table.Calls("UpdateItem") + table.Calls("PutItem") + table.Calls("DeleteItem")
	r := checkAccountWide(context.Background(), aws.Config{}, fpAcct, "us-east-1")
	if r.Outcome != idle.AccountWideWould {
		t.Errorf("%+v", r)
	}
	if n := table.Calls("TransactWriteItems") + table.Calls("UpdateItem") + table.Calls("PutItem") + table.Calls("DeleteItem"); n != writes {
		t.Errorf("the check wrote %d times", n-writes)
	}
	if _, err := c.Mark(context.Background(), inventory.Tool, "us-east-1"); err != nil { // a creator running there
		t.Fatal(err)
	}
	if r := checkAccountWide(context.Background(), aws.Config{}, fpAcct, "eu-west-1"); r.Outcome != idle.AccountWideBlocked {
		t.Errorf("another region's item in use: %+v", r)
	}
	if r := checkAccountWide(context.Background(), aws.Config{}, fpAcct, "us-east-1"); r.Outcome != idle.AccountWideBlocked {
		t.Errorf("the caller's own region with a creator running (G2): %+v", r)
	}
}

// TestRunAccountWideUnfenced (lagotto#347): with no global scope, the
// account-wide resources not in use go under the stand-in lease.
func TestRunAccountWideUnfenced(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-iso-east-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
	if r.Outcome != idle.AccountWideRemoved || len(rec.calls) != 1 || rec.calls[0] != "global policy" {
		t.Fatalf("%+v %v", r, rec.calls)
	}
	// Nothing left, or the footprint unreadable.
	empty := func() inventory.Facts { f := facts(); f.Account = inventory.Account{}; return f }
	stubPlanTeardown(t, empty)
	if r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {}); r.Outcome != idle.AccountWideNothing {
		t.Errorf("nothing: %+v", r)
	}
	gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) {
		return inventory.Facts{}, errors.New("AccessDenied")
	}
	if r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {}); r.Outcome != idle.AccountWideFailed {
		t.Errorf("unreadable: %+v", r)
	}
}

// TestRunTeardown_IAMRolesWithoutAGlobalScope: outside aws, aws-us-gov and aws-cn
// --iam-roles still removes the account-wide resources (no lease there).
func TestRunTeardown_IAMRolesWithoutAGlobalScope(t *testing.T) {
	tables := &fakeTables{empty: true}
	calls := stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, true, false)
	stubPollerRegions(t, nil, nil)
	teardownAccountWideCheck = func(context.Context, aws.Config, string, string) idle.AccountWideResult {
		return idle.AccountWideResult{Outcome: idle.AccountWideSkipped, Why: "no control region for us-iso-east-1"}
	}
	unfenced := 0
	prev := teardownAccountWideUnfenced
	t.Cleanup(func() { teardownAccountWideUnfenced = prev })
	teardownAccountWideUnfenced = func(context.Context, aws.Config, string, map[string]bool, func(string, ...any)) idle.AccountWideResult {
		unfenced++
		return idle.AccountWideResult{Outcome: idle.AccountWideRemoved}
	}
	if _, err := runTeardownForTest(t); err != nil || unfenced != 1 || len(*calls) != 0 || !tables.deleted {
		t.Errorf("%v unfenced=%d calls=%v", err, unfenced, *calls)
	}
}

// TestMarkCreatingEndsMarksOnRefusal: a watch refused after its marks (the
// poller's schedule went while it waited) ends both marks, so nothing is
// left in use (lagotto#340).
func TestMarkCreatingEndsMarksOnRefusal(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	f := &flipSchedule{fakeTeardownJobs: fakeTeardownJobs{schedule: true, jobs: []string{"lagotto-teardown-function"}}}
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return f }
	if _, err := markCreating(ctx, aws.Config{}, "alice", "us-west-2", "create watch", creatorHosted, &bytes.Buffer{}); err == nil ||
		!strings.Contains(err.Error(), "has been torn down") {
		t.Fatalf("err = %v", err)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	for _, scope := range []string{"us-west-2", lifecycle.ScopeGlobal} {
		if it, ok, _ := c.Get(ctx, inventory.Tool, scope); !ok || len(it.Marks) != 0 || c.InUse(it, idle.CreatorMarkBound) {
			t.Errorf("%s after the refusal: %+v", scope, it)
		}
	}
	// A mark that cannot be ended is a warning.
	var out bytes.Buffer
	token, _ := c.Mark(ctx, inventory.Tool, "us-west-2")
	table.Err = func(string, any) error { return errors.New("network down") }
	endMark(c, "us-west-2", token, &out)
	table.Err = nil
	if !strings.Contains(out.String(), "could not end this command's mark") {
		t.Errorf("warning %q", out.String())
	}
}

// flipSchedule has the poller's schedule for the first look only.
type flipSchedule struct {
	fakeTeardownJobs
	looks int
}

func (f *flipSchedule) GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, o ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	if f.looks++; f.looks > 1 {
		f.schedule = false
	}
	return f.fakeTeardownJobs.GetSchedule(ctx, in, o...)
}
