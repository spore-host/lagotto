package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The sixth review of lagotto#351.

// TestReview6FailedReadStillCancels (G1): a reap run whose post-claim
// footprint read fails, or reads its region only in part, does not take
// that for "no jobs": it lists the region's schedules and cancels a job an
// earlier run left.
func TestReview6FailedReadStillCancels(t *testing.T) {
	for name, gather := range map[string]func(ctx context.Context, src inventory.Source, acct string, sc inventory.Scope, o inventory.Options) (inventory.Facts, error){
		"a failed read": func(context.Context, inventory.Source, string, inventory.Scope, inventory.Options) (inventory.Facts, error) {
			return inventory.Facts{}, errors.New("throttled")
		},
		"an incomplete read": func(ctx context.Context, src inventory.Source, acct string, sc inventory.Scope, o inventory.Options) (inventory.Facts, error) {
			f, err := inventory.Gather(ctx, src, acct, sc, o)
			for i := range f.Regions {
				f.Regions[i].ActivityUnknown = true
				f.Regions[i].Schedules = nil
			}
			return f, err
		},
	} {
		h := newHarness(t)
		h.src.Update(region, func(r *inventory.Region) {
			r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName})
		})
		h.pastGrace(t, true)
		prev := gatherFacts
		gatherFacts = gather
		r := Tick(context.Background(), h.cfg(true), h.deps())
		gatherFacts = prev
		if r.Outcome != OutcomeReleased || r.facts != nil {
			t.Errorf("%s: %+v", name, r)
		}
		if !strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot lagotto-teardown/"+FunctionJobName) {
			t.Errorf("%s: the leftover job was not cancelled: %v\n%s", name, h.ev.list(), h.log())
		}
	}
}

// TestReview6InterruptedGivesBack (G2a): an account-wide run whose context
// is cancelled (the CLI interrupted) mid-delete still gives its lease back
// (suspended, as a teardown that stopped partway), with a context of its
// own, so no creator waits for it.
func TestReview6InterruptedGivesBack(t *testing.T) {
	w := newAWWorld(t)
	ctx, cancel := context.WithCancel(context.Background())
	deletes := 0
	w.acct.before = func(string) {
		if deletes++; deletes == 2 {
			cancel()
		}
	}
	r := TearDownAccountWide(ctx, w.cfg(true, 0), w.deps())
	if r.Outcome != AccountWideHeld && r.Outcome != AccountWideFailed {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Errorf("an interrupted run left its lease live: %+v", g)
	}
}

// TestReview6CreatorOutwaitsAKilledTeardown (G2b): a creator waits longer
// than a lease takes to go stale, so the lease of a teardown that was killed
// after its claim is taken over, not a refusal.
func TestReview6CreatorOutwaitsAKilledTeardown(t *testing.T) {
	if CreatorWait <= lifecycle.DefaultStaleAfter {
		t.Fatalf("CreatorWait %s does not cover the stale bound %s", CreatorWait, lifecycle.DefaultStaleAfter)
	}
	ctx := context.Background()
	w := newAWWorld(t)
	killed := w.client("a killed CLI")
	if _, err := killed.SetIdleIfUnchanged(ctx, Tool, lifecycle.ScopeGlobal, lifecycle.Expect{}); err != nil {
		t.Fatal(err)
	}
	g, _ := w.global()
	if _, err := killed.ClaimIfUnchanged(ctx, Tool, lifecycle.ScopeGlobal, 0, lifecycle.ExpectOf(g, true)); err != nil {
		t.Fatal(err)
	}
	creator, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return w.tables[r], nil },
		ControlRegion: region, Owner: "setup", Now: w.clock, WaitTimeout: CreatorWait,
		Sleep: func(context.Context, time.Duration) error { w.advance(10 * time.Second); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Mark(ctx, creator, lifecycle.ScopeGlobal, "set up"); err != nil {
		t.Errorf("a creator behind a killed teardown's lease: %v", err)
	}
	if g, _ := w.global(); g.Lease != nil || len(g.Marks) != 1 {
		t.Errorf("after: %+v", g)
	}
}

// TestReview6EndMarkPauses (nit): EndMark pauses between its tries.
func TestReview6EndMarkPauses(t *testing.T) {
	w := newAWWorld(t)
	c := w.client("creator")
	tok, err := Mark(context.Background(), c, "us-west-2", "x")
	if err != nil {
		t.Fatal(err)
	}
	w.tables["us-west-2"].Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			return errors.New("network down")
		}
		return nil
	}
	pauses := 0
	prev := endMarkSleep
	endMarkSleep = func(context.Context, time.Duration) error { pauses++; return nil }
	defer func() { endMarkSleep = prev }()
	if err := EndMark(context.Background(), c, "us-west-2", tok); err == nil || pauses != readAttempts-1 {
		t.Errorf("%v, %d pauses", err, pauses)
	}
}

// TestReview6BlockedLogsTheRelease (nit): a blocked run's log line carries
// the error of the release that followed it.
func TestReview6BlockedLogsTheRelease(t *testing.T) {
	w := newAWWorld(t)
	w.acct.before = func(string) {
		if _, err := Mark(context.Background(), w.client("setup"), "us-west-2", "x"); err != nil {
			t.Error(err)
		}
		w.tables[region].Err = func(op string, _ any) error {
			if op == "UpdateItem" {
				return errors.New("network down")
			}
			return nil
		}
	}
	r := w.run(true, 0)
	w.tables[region].Err = nil
	if r.Outcome != AccountWideBlocked || r.Err == nil {
		t.Fatalf("%+v", r)
	}
	if !strings.Contains(w.log(), `outcome=blocked`) || !strings.Contains(w.log(), `error="`) {
		t.Errorf("the blocked line lacks the release error:\n%s", w.log())
	}
}
