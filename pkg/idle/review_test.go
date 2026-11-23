package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// TestAccountWideMarkBeforeTheClaim: with no grace, a creator (setup, which
// marks only the global scope) that marks between the decision and the
// claim is seen by the epoch read after the claim: the lease goes back and
// nothing is deleted (it would delete the IAM setup just wrote).
func TestAccountWideMarkBeforeTheClaim(t *testing.T) {
	w := newAWWorld(t)
	d := w.deps()
	d.Gather = func(ctx context.Context) (inventory.Facts, error) {
		f, err := inventory.Gather(ctx, w.src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
		if err := MarkActive(ctx, w.client("setup"), lifecycle.ScopeGlobal, "set up lagotto's IAM"); err != nil {
			t.Fatal(err)
		}
		return f, err
	}
	r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d)
	if r.Outcome != AccountWideBlocked || !strings.Contains(r.Why, "a creator marked lagotto active since the decision") || len(w.ev.list()) != 0 {
		t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Errorf("the lease was not given back: %+v", g)
	}
	// A global item that cannot be read after the claim: the same.
	w = newAWWorld(t)
	d = w.deps()
	gets, claimed := 0, false
	global := func(in any) bool {
		switch x := in.(type) {
		case *dynamodb.GetItemInput:
			v, ok := x.Key["pk"].(*ddbtypes.AttributeValueMemberS)
			return ok && v.Value == lifecycle.ItemKey(Tool, lifecycle.ScopeGlobal)
		case *dynamodb.UpdateItemInput:
			v, ok := x.Key["pk"].(*ddbtypes.AttributeValueMemberS)
			return ok && v.Value == lifecycle.ItemKey(Tool, lifecycle.ScopeGlobal)
		}
		return false
	}
	w.tables[region].Err = func(op string, in any) error {
		switch {
		case op == "UpdateItem" && global(in):
			claimed = true
		case op == "GetItem" && claimed && global(in):
			if gets++; gets == 1 {
				return errors.New("network down")
			}
		}
		return nil
	}
	if r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d); r.Outcome != AccountWideBlocked || len(w.ev.list()) != 0 {
		t.Errorf("unreadable after the claim: %+v (%d reads)\n%s", r, gets, w.log())
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
	if r := w.run(true, 0); r.Outcome != AccountWideHeld || !errors.Is(r.Err, boom) || len(w.ev.list()) != 0 {
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
