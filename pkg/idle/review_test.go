package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// TestAccountWideMarkBeforeTheClaim: with no grace, a creator (setup, which
// marks only the global scope) that marks between the judgement and the
// claim is never erased: the actor's writes are conditional on what it
// judged, so it stops, with nothing deleted and the creator's mark intact.
func TestAccountWideMarkBeforeTheClaim(t *testing.T) {
	for _, existing := range []bool{false, true} {
		w := newAWWorld(t)
		if existing {
			if _, err := w.client("an earlier run").SetIdleIfUnchanged(context.Background(), Tool, lifecycle.ScopeGlobal, lifecycle.Expect{}); err != nil {
				t.Fatal(err)
			}
		}
		d := w.deps()
		d.Gather = func(ctx context.Context) (inventory.Facts, error) {
			f, err := inventory.Gather(ctx, w.src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
			if err := MarkActive(ctx, w.client("setup"), lifecycle.ScopeGlobal, "set up lagotto's IAM"); err != nil {
				t.Fatal(err)
			}
			return f, err
		}
		r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d)
		if r.Outcome != AccountWideBlocked || len(w.ev.list()) != 0 {
			t.Fatalf("existing=%t: %+v %v\n%s", existing, r, w.ev.list(), w.log())
		}
		if g, _ := w.global(); g.Lease != nil || g.State != lifecycle.StateActive || g.Mark == nil {
			t.Errorf("existing=%t: the creator's mark was erased: %+v", existing, g)
		}
	}
}

// TestCancelSkipsAnotherRunsLease: a reap run that cannot claim because
// another run holds the region's live lease leaves the jobs alone (they may
// be that run's commit); once that lease is stale, or gone, it deletes them.
func TestCancelSkipsAnotherRunsLease(t *testing.T) {
	ctx := context.Background()
	jobs := func(h *harness) {
		h.src.Update(region, func(r *inventory.Region) {
			r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName})
		})
	}
	h := newHarness(t)
	h.pastGrace(t, true)
	jobs(h)
	other, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil },
		Owner: "lagotto-poller/us-east-1 (an overlapping run)", Now: h.clock})
	if _, err := other.Claim(ctx, Tool, region, 0); err != nil {
		t.Fatal(err)
	}
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeNotClaimable || strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") {
		t.Errorf("under a live lease: %+v %v", r, h.ev.list())
	}
	if !strings.Contains(h.log(), "cancel=skipped") {
		t.Errorf("log:\n%s", h.log())
	}
	// The item cannot be read: the jobs stay too.
	h.table.Err = func(op string, _ any) error {
		if op == "GetItem" {
			return errors.New("network down")
		}
		return nil
	}
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps())
	if strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") {
		t.Errorf("deleted with the item unreadable: %v", h.ev.list())
	}
	h.table.Err = nil
	// In use, with no lease: they go.
	h = newHarness(t)
	jobs(h)
	h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 1 })
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeActive ||
		!strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot lagotto-teardown/"+FunctionJobName) {
		t.Errorf("in use: %+v %v", r, h.ev.list())
	}
	// The schedules cannot be read: only logged.
	h = newHarness(t)
	h.src.Fail(region+" Schedules", errors.New("throttled"))
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps())
	if !strings.Contains(h.log(), "cancel=failed") {
		t.Errorf("log:\n%s", h.log())
	}
}

// TestTeardownFootprintWithoutTheRegion: a footprint that does not hold the
// poller's own region gives the lease back.
func TestTeardownFootprintWithoutTheRegion(t *testing.T) {
	h := newHarness(t)
	h.pastGrace(t, true)
	prev := gatherFacts
	t.Cleanup(func() { gatherFacts = prev })
	gatherFacts = func(context.Context, inventory.Source, string, inventory.Scope, inventory.Options) (inventory.Facts, error) {
		return inventory.Facts{}, nil
	}
	if r := Tick(context.Background(), h.cfg(true), h.deps()); r.Outcome != OutcomeReleased || len(h.ev.list()) != 0 || h.item(t).Lease != nil {
		t.Errorf("%+v %v\n%s", r, h.ev.list(), h.log())
	}
}

// TestExecuteWithoutAResult: footprint.Execute returning (nil, err) (a plan
// it cannot run at all) ends the poller's run with the lease given back, in
// either mode, and leaves the account-wide run held, with nothing deleted.
func TestExecuteWithoutAResult(t *testing.T) {
	prev := execute
	t.Cleanup(func() { execute = prev })
	boom := errors.New("footprint: nil deleter")
	execute = func(context.Context, *fp.Plan, fp.Mode, fp.Deleter, *lifecycle.Lease) (*fp.Result, error) {
		return nil, boom
	}
	for _, reap := range []bool{false, true} {
		h := newHarness(t)
		h.pastGrace(t, reap)
		r := Tick(context.Background(), h.cfg(reap), h.deps())
		if r.Outcome != OutcomeFailed || !errors.Is(r.Err, boom) || h.item(t).Lease != nil || len(h.ev.list()) != 0 {
			t.Errorf("reap=%t: %+v %v", reap, r, h.ev.list())
		}
	}
	// The self-removal check alone.
	calls := 0
	execute = func(ctx context.Context, p *fp.Plan, m fp.Mode, d fp.Deleter, l *lifecycle.Lease) (*fp.Result, error) {
		if calls++; calls == 2 {
			return nil, boom
		}
		return prev(ctx, p, m, d, l)
	}
	h := newHarness(t)
	h.pastGrace(t, false)
	if r := Tick(context.Background(), h.cfg(false), h.deps()); r.Outcome != OutcomeFailed || h.item(t).Lease != nil {
		t.Errorf("self: %+v", r)
	}
	// The final deletes after the commit: nothing of them ran, so the jobs
	// are rolled back.
	calls = 0
	execute = func(ctx context.Context, p *fp.Plan, m fp.Mode, d fp.Deleter, l *lifecycle.Lease) (*fp.Result, error) {
		if calls++; calls == 3 {
			return nil, boom
		}
		return prev(ctx, p, m, d, l)
	}
	h = newHarness(t)
	h.pastGrace(t, true)
	if r := Tick(context.Background(), h.cfg(true), h.deps()); r.Outcome != OutcomeFailed ||
		!strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot lagotto-teardown/"+FunctionJobName) {
		t.Errorf("final: %+v %v", r, h.ev.list())
	}

	execute = func(context.Context, *fp.Plan, fp.Mode, fp.Deleter, *lifecycle.Lease) (*fp.Result, error) {
		return nil, boom
	}
	w := newAWWorld(t)
	if r := w.run(true, 0); r.Outcome != AccountWideFailed || !errors.Is(r.Err, boom) || len(w.ev.list()) != 0 {
		t.Errorf("account-wide: %+v", r)
	}
	if r := w.run(false, 0); r.Outcome != AccountWideWaiting && r.Outcome != AccountWideWould {
		t.Errorf("account-wide report: %+v", r)
	}
	if count(nil, fp.OutcomeFailed) != 0 {
		t.Error("count(nil)")
	}
}

// TestAccountWideLeftovers: an IDLE item in a region whose teardown stopped
// partway is dead when only regional resources are left (they need nothing
// account-wide), and blocks while its function is left (a poller).
func TestAccountWideLeftovers(t *testing.T) {
	idle := func(w *awWorld) {
		if _, _, err := w.client(PollerOwner("us-west-2")).Observe(context.Background(), Tool, "us-west-2", false); err != nil {
			t.Fatal(err)
		}
	}
	w := newAWWorld(t)
	idle(w)
	w.src.SetRegion(inventory.Region{Region: "us-west-2", Tables: inventorytest.Deployed("us-west-2", acct).Tables})
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Errorf("tables left: %+v", r)
	}
	w = newAWWorld(t)
	idle(w)
	w.src.SetRegion(inventory.Region{Region: "us-west-2", Function: inventorytest.Deployed("us-west-2", acct).Function})
	if r := w.run(true, 0); r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, ";"), "us-west-2: a poller is deployed") ||
		len(w.ev.list()) != 0 {
		t.Errorf("a function left: %+v", r)
	}
	_ = time.Second
}

// TestAccountWideSetupFinishesBeforeTheClaim: setup marks and ends its mark
// between the teardown recording the scope idle and its claim: the epoch
// moved since the judgement, so the claim is refused.
func TestAccountWideSetupFinishesBeforeTheClaim(t *testing.T) {
	w := newAWWorld(t)
	f := w.tables[region]
	wrote, ran := false, false
	f.Hook = func(ctx context.Context, op string) {
		switch {
		case op == "TransactWriteItems" && !ran:
			wrote = true
		case op == "GetItem" && wrote && !ran:
			ran = true
			setup := w.client("setup")
			token, err := Mark(ctx, setup, lifecycle.ScopeGlobal, "set up")
			if err != nil {
				t.Error(err)
				return
			}
			if done, err := setup.MarkDone(ctx, Tool, lifecycle.ScopeGlobal, token); !done || err != nil {
				t.Error(done, err)
			}
		}
	}
	r := w.run(true, 0)
	if !ran || r.Outcome != AccountWideBlocked || len(w.ev.list()) != 0 {
		t.Fatalf("ran=%t %+v %v\n%s", ran, r, w.ev.list(), w.log())
	}
}

// TestAccountWideDeadItemMarkedAgain: a dead regional item a watch --local
// marks again before it is finished is not finished: the run stops, the
// IAM stays, the lease goes back.
func TestAccountWideDeadItemMarkedAgain(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	c := w.client(PollerOwner("us-west-2"))
	if _, _, err := c.Observe(ctx, Tool, "us-west-2", false); err != nil {
		t.Fatal(err)
	}
	f := w.tables["us-west-2"]
	marked := false
	f.Hook = func(ctx context.Context, op string) {
		if op == "UpdateItem" && !marked { // the dead item's claim
			marked = true
			if err := w.client("watch --local").MarkActive(ctx, Tool, "us-west-2"); err != nil {
				t.Error(err)
			}
		}
	}
	r := w.run(true, 0)
	if !marked || r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, ";"), "changed since it was judged dead") || len(w.ev.list()) != 0 {
		t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
	}
	if it, ok, _ := w.client("x").Get(ctx, Tool, "us-west-2"); !ok || it.State != lifecycle.StateActive {
		t.Errorf("the watch's item: %+v", it)
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Errorf("the global lease was not given back: %+v", g)
	}
}

// TestCancelSkipsAFinishedRegion: a retried or duplicate invocation after
// the region committed (its rate schedule gone) leaves the jobs: they are
// the region's removal.
func TestCancelSkipsAFinishedRegion(t *testing.T) {
	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) {
		r.Schedules = []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}
	})
	if r := Tick(context.Background(), h.cfg(true), h.deps()); strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") {
		t.Errorf("%+v %v", r, h.ev.list())
	}
	if !strings.Contains(h.log(), "the poller's rate schedule is gone") {
		t.Errorf("log:\n%s", h.log())
	}
}

// TestAccountWideExcluding: `teardown --iam-roles`' check ignores the
// caller's own region's item and regional resources (it removes them
// first), but not its poller.
func TestAccountWideExcluding(t *testing.T) {
	w := newAWWorld(t)
	if err := w.client("deploy").MarkActive(context.Background(), Tool, "us-west-2"); err != nil {
		t.Fatal(err)
	}
	w.src.SetRegion(inventory.Region{Region: "us-west-2", Tables: inventorytest.Deployed("us-west-2", acct).Tables, ActiveWatches: 1})
	cfg := w.cfg(false, 0)
	if r := TearDownAccountWide(context.Background(), cfg, w.deps()); r.Outcome != AccountWideBlocked {
		t.Fatalf("not excluded: %+v", r)
	}
	cfg.Excluding = "us-west-2"
	before := w.writes()
	if r := TearDownAccountWide(context.Background(), cfg, w.deps()); r.Outcome != AccountWideWould || w.writes() != before {
		t.Errorf("excluded: %+v (%d writes)", r, w.writes()-before)
	}
	w.src.SetRegion(inventorytest.Deployed("us-west-2", acct))
	if r := TearDownAccountWide(context.Background(), cfg, w.deps()); r.Outcome != AccountWideBlocked {
		t.Errorf("its poller still blocks: %+v", r)
	}
}
