package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
)

// The fifth review of lagotto#351.

// makeDeadItem leaves us-west-2 with a dead regional item (an IDLE one, no
// poller there).
func makeDeadItem(t *testing.T, w *awWorld) {
	t.Helper()
	if _, _, err := w.client(PollerOwner("us-west-2")).Observe(context.Background(), Tool, "us-west-2", false); err != nil {
		t.Fatal(err)
	}
}

// TestReview5DeadItemFinishFails (G1): a dead item whose finish fails after
// the claim gives its lease back, so no creator in that region waits; a
// claim whose answer was lost is given back too.
func TestReview5DeadItemFinishFails(t *testing.T) {
	w := newAWWorld(t)
	makeDeadItem(t, w)
	w.tables["us-west-2"].Err = func(op string, _ any) error {
		if op == "DeleteItem" {
			return errors.New("network down")
		}
		return nil
	}
	if r := w.run(true, 0); r.Outcome != AccountWideFailed || !strings.Contains(r.Why, "could not finish us-west-2") {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	w.tables["us-west-2"].Err = nil
	it, ok, _ := w.client("reader").Get(context.Background(), Tool, "us-west-2")
	if !ok || it.Lease != nil {
		t.Errorf("the regional lease was left: %+v", it)
	}
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Errorf("the next run: %+v\n%s", r, w.log())
	}

	// The claim applies, its answer is lost: given back.
	w = newAWWorld(t)
	makeDeadItem(t, w)
	w.tables["us-west-2"].Err = func(op string, in any) error {
		if u, ok := in.(*dynamodb.UpdateItemInput); ok && op == "UpdateItem" && strings.Contains(*u.UpdateExpression, ":teardown") {
			w.tables["us-west-2"].Err = nil
			_, _ = w.tables["us-west-2"].UpdateItem(context.Background(), u)
			return errors.New("connection reset")
		}
		return nil
	}
	r := w.run(true, 0)
	if r.Outcome != AccountWideFailed || !strings.Contains(errString(r.Err), "it was given back") {
		t.Errorf("a lost claim answer: %+v", r)
	}
	if it, _, _ := w.client("reader").Get(context.Background(), Tool, "us-west-2"); it.Lease != nil {
		t.Errorf("the lost claim's lease was left: %+v", it)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// loseFinish makes f's first DeleteItem of an item apply, then answer with a
// dropped connection.
func loseFinish(f interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}, set func(func(string, any) error)) {
	set(func(op string, in any) error {
		if d, ok := in.(*dynamodb.DeleteItemInput); ok && op == "DeleteItem" {
			set(nil)
			_, _ = f.DeleteItem(context.Background(), d)
			return errors.New("connection reset")
		}
		return nil
	})
}

// TestReview5FinishAppliedAnswerLost (G2): a Finish whose delete applied but
// whose answer was lost is a finish, when the retry finds the lease gone
// and the item absent: the account-wide run reports removed and closes the
// table; so does the poller's regional teardown.
func TestReview5FinishAppliedAnswerLost(t *testing.T) {
	w := newAWWorld(t)
	f := w.tables[region]
	deleted := 0
	w.acct.before = func(string) {
		if deleted++; deleted == len(allAccountWide) {
			loseFinish(f, func(e func(string, any) error) { f.Err = e })
		}
	}
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Fatalf("account-wide: %+v\n%s", r, w.log())
	}
	if _, ok := w.global(); ok {
		t.Error("the global item is left")
	}

	h := newHarness(t)
	h.pastGrace(t, true)
	loseFinish(h.table, func(e func(string, any) error) { h.table.Err = e })
	if r := Tick(context.Background(), h.cfg(true), h.deps()); r.Outcome != OutcomeReaped {
		t.Errorf("poller: %+v\n%s", r, h.log())
	}
}

// TestReview5FunctionWithNoSchedule (G3): a function with no rate schedule
// and no job is named as such: a deploy that did not finish.
func TestReview5FunctionWithNoSchedule(t *testing.T) {
	w := newAWWorld(t)
	w.src.SetRegion(inventory.Region{Region: "us-west-2", Function: inventorytest.Deployed("us-west-2", acct).Function})
	r := w.run(false, 0)
	if r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, ";"), "a poller function with no rate schedule and no teardown job (a deploy that did not finish)") {
		t.Errorf("%+v", r)
	}
	// With its schedule, or a job: a poller, as before.
	w.src.SetRegion(inventorytest.Deployed("us-west-2", acct))
	if r := w.run(false, 0); !strings.Contains(strings.Join(r.Blockers, ";"), "us-west-2: a poller is deployed") {
		t.Errorf("%+v", r)
	}
}

// TestReview5SetupStopsADelete (G7): a creator (setup, in its home region)
// that marks while the account-wide teardown deletes stops it at its next
// re-check, and the lease is given back at once, so the creator's global
// mark, waiting for it, goes ahead.
func TestReview5SetupStopsADelete(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	setup := w.client("setup")
	var token string
	w.acct.before = func(string) {
		if token == "" {
			var err error
			if token, err = Mark(ctx, setup, region, "set up lagotto's IAM"); err != nil {
				t.Error(err)
			}
		}
	}
	r := w.run(true, 0)
	if r.Outcome != AccountWideHeld || len(w.ev.list()) != 1 {
		t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
	}
	if g, _ := w.global(); g.Lease != nil {
		t.Fatalf("the lease was kept: %+v", g)
	}
	if _, err := Mark(ctx, setup, lifecycle.ScopeGlobal, "set up lagotto's IAM"); err != nil {
		t.Errorf("setup's global mark: %v", err)
	}
	_ = time.Second
}
