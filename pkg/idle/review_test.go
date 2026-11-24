package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/smithy-go"
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
		if g, _ := w.global(); g.Lease != nil || g.State != lifecycle.StateActive || len(g.Marks) == 0 {
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
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps(), nil)
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
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps(), nil)
	if !strings.Contains(h.log(), "cancel=skipped") || !strings.Contains(h.log(), "could not list") {
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

// TestAccountWideReleaseRetried: a release that fails once (a throttle) is
// tried again, so a creator after the run is not kept waiting (the model
// check found the single failure).
func TestAccountWideReleaseRetried(t *testing.T) {
	w := newAWWorld(t)
	w.acct.before = func(string) {
		if len(w.ev.list()) == 1 {
			w.src.SetRegion(inventorytest.Deployed("us-west-2", acct))
		}
	}
	failed := false
	w.tables[region].Err = func(op string, in any) error {
		if u, ok := in.(*dynamodb.UpdateItemInput); ok && op == "UpdateItem" && !failed &&
			strings.Contains(aws.ToString(u.UpdateExpression), "SET #state = :active REMOVE #lease") {
			failed = true
			return &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}
		}
		return nil
	}
	if r := w.run(true, 0); r.Outcome != AccountWideBlocked || !failed {
		t.Fatalf("%+v (failed %t)", r, failed)
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Errorf("the lease was left live: %+v", g)
	}
}

// TestClaimGlobalGivesBackItsOwn: a claim whose answer was lost, but which
// applied, leaves no live lease behind: it is adopted and given back.
func TestClaimGlobalGivesBackItsOwn(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	me := w.client(cliOwner)
	g, err := me.SetIdleIfUnchanged(ctx, Tool, lifecycle.ScopeGlobal, lifecycle.Expect{})
	if err != nil {
		t.Fatal(err)
	}
	judged := lifecycle.ExpectOf(g, true)
	w.tables[region].Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			w.tables[region].Err = nil
			if _, err := w.client(cliOwner).ClaimIfUnchanged(ctx, Tool, lifecycle.ScopeGlobal, 0, judged); err != nil {
				t.Error(err)
			}
			return errors.New("connection reset")
		}
		return nil
	}
	if _, err := claimGlobal(ctx, w.cfg(true, 0).withDefaults(), me, judged); err == nil || !strings.Contains(err.Error(), "it was given back") {
		t.Errorf("own lease: %v", err)
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Errorf("the lease was left: %+v", g)
	}
	// Gone already, or not given back: said so.
	if s := giveBackOwn(ctx, me, lifecycle.ScopeGlobal, func(context.Context, time.Duration) error { return nil }); !strings.Contains(s, "gone already") {
		t.Errorf("gone: %s", s)
	}
	if _, _, err := me.Observe(ctx, Tool, "us-west-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := me.Claim(ctx, Tool, "us-west-2", 0); err != nil {
		t.Fatal(err)
	}
	w.tables["us-west-2"].Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			return errors.New("network down")
		}
		return nil
	}
	if s := giveBackOwn(ctx, me, "us-west-2", func(context.Context, time.Duration) error { return nil }); !strings.Contains(s, "goes stale") {
		t.Errorf("not given back: %s", s)
	}
}

// TestCancelUnderTheLock: the cancel takes a lock on the region's item (a
// claim then refuses), refuses one that changed, stops when the lock is
// lost, and makes no call at all when the run's footprint shows no job.
func TestCancelUnderTheLock(t *testing.T) {
	ctx := context.Background()
	jobs := func(h *harness) {
		h.src.Update(region, func(r *inventory.Region) {
			r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName})
		})
	}
	// Locked while it deletes: a claim in between is refused.
	h := newHarness(t)
	h.pastGrace(t, true)
	jobs(h)
	claimed := false
	h.sched.e = &events{}
	other, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil }, Owner: "another run", Now: h.clock})
	cfg := h.cfg(true).withDefaults()
	d := h.deps()
	d.Scheduler = &hookScheduler{OneShotAPI: h.sched, before: func() {
		if _, err := other.Claim(ctx, Tool, region, 0); err == nil {
			claimed = true
		}
	}}
	cancelUncommittedJobs(ctx, cfg, d, nil)
	if claimed {
		t.Error("a claim got in while the cancel held the lock")
	}
	if it := h.item(t); it.Lease != nil {
		t.Errorf("the lock was left: %+v", it)
	}
	// The item changed between the read and the lock: nothing is deleted.
	h = newHarness(t)
	h.pastGrace(t, true)
	jobs(h)
	other2, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil }, Owner: "a creator", Now: h.clock})
	h.table.Hook = func(ctx context.Context, op string) {
		if op == "UpdateItem" && h.table.Hook != nil {
			h.table.Hook = nil
			if err := other2.MarkActive(ctx, Tool, region); err != nil {
				t.Error(err)
			}
		}
	}
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps(), nil)
	if strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") || !strings.Contains(h.log(), "could not be locked") {
		t.Errorf("changed: %v\n%s", h.ev.list(), h.log())
	}
	// The footprint the run read shows no job: no call at all.
	h = newHarness(t)
	reads := h.src.Reads(region+" Schedules") + h.table.Calls("GetItem")
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps(), &inventory.Region{Region: region})
	if n := h.src.Reads(region+" Schedules") + h.table.Calls("GetItem"); n != reads {
		t.Errorf("%d calls with no job seen", n-reads)
	}
	// No item to lock.
	h = newHarness(t)
	jobs(h)
	cancelUncommittedJobs(ctx, h.cfg(true).withDefaults(), h.deps(), nil)
	if !strings.Contains(h.log(), "no lifecycle item to lock") {
		t.Errorf("no item:\n%s", h.log())
	}
}

// hookScheduler runs before ahead of each DeleteSchedule.
type hookScheduler struct {
	OneShotAPI
	before func()
}

func (h *hookScheduler) DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, o ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	h.before()
	return h.OneShotAPI.DeleteSchedule(ctx, in, o...)
}

// TestRegionalMarksAge: a region's ACTIVE item with a creator's fresh mark
// blocks; one whose marks are all older than CreatorMarkBound, with no poller
// there, is a creator that died: dead, set idle as judged, and finished.
func TestRegionalMarksAge(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	if _, err := Mark(ctx, w.client("a watch"), "us-west-2", "create watch"); err != nil {
		t.Fatal(err)
	}
	if r := w.run(true, 0); r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, ";"), "a creator marked it") {
		t.Fatalf("fresh: %+v", r)
	}
	w.advance(CreatorMarkBound + time.Minute)
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Fatalf("stale: %+v\n%s", r, w.log())
	}
	if _, ok, _ := w.client("x").Get(ctx, Tool, "us-west-2"); ok {
		t.Error("the dead item is left")
	}
	// A dead ACTIVE item a creator marks again before it is idled: kept.
	w = newAWWorld(t)
	if _, err := Mark(ctx, w.client("a watch"), "us-west-2", "create watch"); err != nil {
		t.Fatal(err)
	}
	w.advance(CreatorMarkBound + time.Minute)
	f := w.tables["us-west-2"]
	f.Hook = func(ctx context.Context, op string) {
		if op == "TransactWriteItems" && f.Hook != nil {
			f.Hook = nil
			if _, err := Mark(ctx, w.client("another watch"), "us-west-2", "create watch"); err != nil {
				t.Error(err)
			}
		}
	}
	if r := w.run(true, 0); r.Outcome != AccountWideBlocked || len(w.ev.list()) != 0 {
		t.Errorf("marked again: %+v", r)
	}
}
