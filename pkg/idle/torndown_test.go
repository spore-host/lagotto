package idle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// lagotto#350: a poller run in a region that was torn down (its rate
// schedule gone) leaves no lifecycle item, and no table, behind.

// noSchedule removes the poller's rate schedule, as a teardown does before
// it finishes the region's item.
func noSchedule(r *inventory.Region) { r.Schedules = nil }

// TestTickTornDownBeforeObserving: a run that finds its rate schedule gone
// records nothing, in either mode: no table, no item, no job cancel.
func TestTickTornDownBeforeObserving(t *testing.T) {
	ctx := context.Background()
	for _, reap := range []bool{false, true} {
		h := newHarness(t)
		h.src.Update(region, noSchedule)
		r := Tick(ctx, h.cfg(reap), h.deps())
		if r.Outcome != OutcomeTornDown || r.Err != nil || !strings.Contains(h.log(), "observe=skipped") || !strings.Contains(h.log(), tornDownWhy) {
			t.Errorf("reap=%t: %+v\n%s", reap, r, h.log())
		}
		if n := h.table.Calls("CreateTable") + h.table.Calls("TransactWriteItems") + h.table.Calls("UpdateItem"); n != 0 {
			t.Errorf("reap=%t: %d lifecycle writes", reap, n)
		}
		if n := h.src.Reads(region + " Schedules"); n != 1 {
			t.Errorf("reap=%t: %d schedule reads, want the one check", reap, n)
		}
	}
}

// TestTickTornDownWhileObserving: the region's teardown finishes (its rate
// schedule gone, its item finished, its table closed) between the run's
// check and its Observe, which makes the item, and the table, again. The
// run finishes the item it made and closes the empty table again, in
// either mode (#354 review G2).
func TestTickTornDownWhileObserving(t *testing.T) {
	ctx := context.Background()
	for _, reap := range []bool{false, true} {
		h := newHarness(t)
		h.table.Hook = func(_ context.Context, op string) {
			if op == "GetItem" {
				h.table.Hook = nil
				h.src.Update(region, noSchedule) // the CLI's teardown, done
			}
		}
		r := Tick(ctx, h.cfg(reap), h.deps())
		if r.Outcome != OutcomeTornDown || !strings.Contains(h.log(), "torn_down=true finish=ok") {
			t.Errorf("reap=%t: %+v\n%s", reap, r, h.log())
		}
		if _, ok, err := h.client.Get(ctx, Tool, region); ok || err != nil {
			t.Errorf("reap=%t: the item the run made is left (%v)", reap, err)
		}
		if h.table.Status(lifecycle.TableName) != "" {
			t.Errorf("reap=%t: the table the run made is left", reap)
		}
	}
}

// TestTickTornDownInUse: an item in use when the run finds its region torn
// down (a creator's live mark: watch --local needs no poller) is left.
func TestTickTornDownInUse(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	creator := h.other("watch --local")
	h.table.Hook = func(_ context.Context, op string) {
		if op == "TransactWriteItems" {
			h.table.Hook = nil
			h.src.Update(region, noSchedule)
			if _, err := creator.Mark(ctx, Tool, region); err != nil {
				t.Error(err)
			}
		}
	}
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeTornDown || !strings.Contains(h.log(), "finish=skipped") ||
		!strings.Contains(h.log(), "in use: a creator marked it") {
		t.Errorf("%+v\n%s", r, h.log())
	}
	if it := h.item(t); len(it.Marks) != 1 {
		t.Errorf("the creator's item: %+v", it)
	}
}

// TestTickTornDownFailures: a schedule read that fails observes as before;
// a finish that keeps failing is logged (and tried again), and so is a
// table that cannot be closed.
func TestTickTornDownFailures(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.src.Fail(region+" Schedules", errors.New("AccessDenied"))
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting || !strings.Contains(h.log(), "schedule_check=failed") {
		t.Errorf("a failing check: %+v\n%s", r, h.log())
	}

	h = newHarness(t)
	h.table.Hook = func(_ context.Context, op string) {
		if op == "TransactWriteItems" {
			h.table.Hook = nil
			h.src.Update(region, noSchedule)
			h.table.Err = func(op string, _ any) error {
				if op == "UpdateItem" {
					return errors.New("boom") // the claim
				}
				return nil
			}
		}
	}
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeTornDown || !strings.Contains(h.log(), "finish=failed") {
		t.Errorf("a finish that fails: %+v\n%s", r, h.log())
	}
	if n := h.table.Calls("UpdateItem"); n < readAttempts {
		t.Errorf("%d claims, want %d tries", n, readAttempts)
	}

	h = newHarness(t)
	h.table.Hook = func(_ context.Context, op string) {
		if op == "TransactWriteItems" {
			h.table.Hook = nil
			h.src.Update(region, noSchedule)
			h.table.Err = func(op string, _ any) error {
				if op == "PutItem" {
					return errors.New("boom") // the closing sentinel
				}
				return nil
			}
		}
	}
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeTornDown || !strings.Contains(h.log(), "close_table=failed") {
		t.Errorf("a table that cannot be closed: %+v\n%s", r, h.log())
	}
}

// TestFinishIfDead: an absent item (or table) is no write; a read that
// fails is an error; an item that changed since it was judged is left.
func TestFinishIfDead(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	if done, why, err := finishIfDead(ctx, h.client, region, h.cfg(true).Sleep); done || why != "no lifecycle item" || err != nil {
		t.Errorf("absent: %t %q %v", done, why, err)
	}
	if n := h.table.Calls("CreateTable") + h.table.Calls("TransactWriteItems"); n != 0 {
		t.Errorf("absent: %d writes", n)
	}
	h.table.Err = func(string, any) error { return errors.New("boom") }
	if _, _, err := finishIfDead(ctx, h.client, region, h.cfg(true).Sleep); err == nil {
		t.Error("a read that fails")
	}
	h.table.Err = nil

	// Changed between the read and the idle write: a creator's activity.
	if _, _, err := h.client.Observe(ctx, Tool, region, true); err != nil {
		t.Fatal(err)
	}
	h.advance(2 * CreatorMarkBound)
	other := h.other("watch --local")
	h.table.Hook = func(_ context.Context, op string) {
		if op == "TransactWriteItems" {
			h.table.Hook = nil
			h.advance(1)
			if _, _, err := other.Observe(ctx, Tool, region, true); err != nil {
				t.Error(err)
			}
		}
	}
	if done, why, err := finishIfDead(ctx, h.client, region, h.cfg(true).Sleep); done || err != nil || !strings.Contains(why, "changed") {
		t.Errorf("changed: %t %q %v", done, why, err)
	}
	if _, ok, _ := h.client.Get(ctx, Tool, region); !ok {
		t.Error("an item that changed was finished")
	}
}

// other is another participant on the harness's table and clock.
func (h *harness) other(owner string) *lifecycle.Client {
	c, err := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil }, Owner: owner, Now: h.clock})
	if err != nil {
		panic(err)
	}
	return c
}

// TestTickTornDownActive (#354 review G3): a run whose own verdict is
// active, in a region torn down while it observed, reads it again, and if
// it is still active leaves the item (ACTIVE, a record of that activity)
// for the account-wide teardown, in either mode; once the activity went
// with the region (its tables deleted), it finishes the item.
func TestTickTornDownActive(t *testing.T) {
	ctx := context.Background()
	for _, reap := range []bool{false, true} {
		h := newHarness(t)
		h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 1 })
		h.table.Hook = func(_ context.Context, op string) {
			if op == "TransactWriteItems" {
				h.table.Hook = nil
				h.src.Update(region, noSchedule)
			}
		}
		r := Tick(ctx, h.cfg(reap), h.deps())
		if r.Outcome != OutcomeTornDown || !strings.Contains(h.log(), "active=true torn_down=true finish=skipped") ||
			!strings.Contains(h.log(), "1 active watch(es)") {
			t.Errorf("reap=%t: %+v\n%s", reap, r, h.log())
		}
		if it := h.item(t); it.State != lifecycle.StateActive {
			t.Errorf("reap=%t: the item: %+v", reap, it)
		}
		if n := h.table.Calls("DeleteItem") + h.table.Calls("DeleteTable"); n != 0 {
			t.Errorf("reap=%t: %d deletes", reap, n)
		}
	}
	// The watch went with the region: the run's verdict, read again, is
	// idle, and the item it made is finished.
	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 1 })
	h.table.Hook = func(_ context.Context, op string) {
		if op == "TransactWriteItems" {
			h.table.Hook = nil
			h.src.Update(region, func(r *inventory.Region) { r.Schedules, r.ActiveWatches = nil, 0 })
		}
	}
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeTornDown || r.Verdict.Active || !strings.Contains(h.log(), "finish=ok") {
		t.Errorf("the activity gone with the region: %+v\n%s", r, h.log())
	}
	if _, ok, _ := h.client.Get(ctx, Tool, region); ok {
		t.Error("the activity gone with the region: the item is left")
	}
}

// TestTickReusesItsScheduleRead (#354 review G5): a reap run that does not
// commit reads the region's schedules twice (before and after observing),
// and its job cancel reuses the second read.
func TestTickReusesItsScheduleRead(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	if n := h.src.Reads(region + " Schedules"); n != 2 {
		t.Errorf("%d schedule reads, want 2", n)
	}
	// With an earlier run's job there, the cancel still lists again under
	// its lock before it deletes.
	h = newHarness(t)
	h.src.Update(region, func(r *inventory.Region) {
		r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName})
	})
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	if n := h.src.Reads(region + " Schedules"); n != 3 || !strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") {
		t.Errorf("%d schedule reads, want 3 (the re-list under the lock); events %v\n%s", n, h.ev.list(), h.log())
	}
}
