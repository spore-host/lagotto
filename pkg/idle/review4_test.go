package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The fourth review of lagotto#351: writers never idle an item; lagotto
// judges each one (WhyInUse, lifecycle InUse and IdleSince).

// TestReview4WhyInUse: each reason an item is in use, and none once its
// creators ended or died.
func TestReview4WhyInUse(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	c := w.client("reader")
	read := func() lifecycle.Item {
		it, _, err := c.Get(ctx, Tool, "us-west-2")
		if err != nil {
			t.Fatal(err)
		}
		return it
	}
	creator := w.client("creator")
	token, err := Mark(ctx, creator, "us-west-2", "create")
	if err != nil {
		t.Fatal(err)
	}
	if why := WhyInUse(c, read()); !strings.HasPrefix(why, "a creator marked it at ") {
		t.Errorf("a live mark: %q", why)
	}
	if err := EndMark(ctx, creator, "us-west-2", token); err != nil {
		t.Fatal(err)
	}
	// Ended: not in use, with no write to idle it.
	if it := read(); WhyInUse(c, it) != "" || it.State != lifecycle.StateActive {
		t.Errorf("ended: %q %+v", WhyInUse(c, it), it)
	}
	if err := MarkActive(ctx, creator, "us-west-2", "fire a launch"); err != nil {
		t.Fatal(err)
	}
	if why := WhyInUse(c, read()); !strings.HasPrefix(why, "marked active at ") {
		t.Errorf("a pulse: %q", why)
	}
	w.advance(CreatorMarkBound)
	if why := WhyInUse(c, read()); why != "" {
		t.Errorf("a pulse past the bound: %q", why)
	}
	if err := creator.AddHold(ctx, Tool, "us-west-2", "spawn"); err != nil {
		t.Fatal(err)
	}
	if why := WhyInUse(c, read()); !strings.HasPrefix(why, "held by") {
		t.Errorf("a hold: %q", why)
	}
	if err := creator.RemoveHold(ctx, Tool, "us-west-2", "spawn"); err != nil {
		t.Fatal(err)
	}
	// An older lagotto's ACTIVE: no time, so in use until judged otherwise.
	older(t, w, "us-west-2")
	it := read()
	if why := WhyInUse(c, it); !strings.Contains(why, "older lagotto") || !untimedOnly(c, it) {
		t.Errorf("an older lagotto's: %q", why)
	}
}

// older stores scope's item as a lagotto from before the stamps left it:
// ACTIVE, with no time.
func older(t *testing.T, w *awWorld, scope string) {
	t.Helper()
	if err := w.tables[scope].PutRaw(lifecycle.TableName, map[string]ddbtypes.AttributeValue{
		"pk":    &ddbtypes.AttributeValueMemberS{Value: lifecycle.ItemKey(Tool, scope)},
		"state": &ddbtypes.AttributeValueMemberS{Value: "ACTIVE"}, "epoch": &ddbtypes.AttributeValueMemberN{Value: "3"},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestReview4ClaimJudged (G2): the CLI's region claim refuses a live
// creator mark and never erases it; one past the bound, or ended, is
// claimed; an absent item is made and claimed; an older lagotto's ACTIVE
// only with acceptOlder; a mark between the read and the write refuses it.
func TestReview4ClaimJudged(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	cli := w.client("lagotto teardown")
	creator := w.client("watch --local")
	if _, err := Mark(ctx, creator, "us-west-2", "create watch"); err != nil {
		t.Fatal(err)
	}
	var nc *lifecycle.NotClaimableError
	if _, err := ClaimJudged(ctx, cli, "us-west-2", true); !errors.As(err, &nc) || !strings.Contains(nc.Reason, "in use: a creator marked it") {
		t.Fatalf("a live mark: %v", err)
	}
	if it, _, _ := cli.Get(ctx, Tool, "us-west-2"); len(it.Marks) != 1 || it.State != lifecycle.StateActive {
		t.Errorf("the mark was touched: %+v", it)
	}
	w.advance(CreatorMarkBound)
	l, err := ClaimJudged(ctx, cli, "us-west-2", true)
	if err != nil {
		t.Fatalf("a dead creator's mark: %v", err)
	}
	if err := l.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	// Absent: made IDLE and claimed.
	if l, err = ClaimJudged(ctx, cli, "us-west-2", true); err != nil {
		t.Fatalf("absent: %v", err)
	}
	if err := l.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	// An older lagotto's ACTIVE.
	older(t, w, "us-west-2")
	if _, err := ClaimJudged(ctx, cli, "us-west-2", false); !errors.As(err, &nc) || !strings.Contains(nc.Reason, "older lagotto") {
		t.Errorf("an older lagotto's, not accepted: %v", err)
	}
	if l, err = ClaimJudged(ctx, cli, "us-west-2", true); err != nil {
		t.Fatalf("an older lagotto's, accepted: %v", err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// A creator marks between the judgement and the idle write.
	w.advance(CreatorMarkBound)
	marked := false
	w.tables["us-west-2"].Hook = func(ctx context.Context, op string) {
		if op == "TransactWriteItems" && !marked {
			marked = true
			if _, err := Mark(ctx, creator, "us-west-2", "create watch"); err != nil {
				t.Error(err)
			}
		}
	}
	if _, err := ClaimJudged(ctx, cli, "us-west-2", true); !errors.As(err, &nc) || nc.Reason != "changed since it was judged" {
		t.Errorf("a mark while claiming: %v", err)
	}
	w.tables["us-west-2"].Hook = nil
	if it, _, _ := cli.Get(ctx, Tool, "us-west-2"); len(it.Marks) != 1 || it.Lease != nil {
		t.Errorf("the creator's mark was erased: %+v", it)
	}
	// A read that fails is an error, not a refusal.
	w.tables["us-west-2"].Err = func(op string, _ any) error {
		if op == "GetItem" {
			return errors.New("network down")
		}
		return nil
	}
	if _, err := ClaimJudged(ctx, cli, "us-west-2", true); err == nil || errors.As(err, &nc) {
		t.Errorf("a failed read: %v", err)
	}
}

// TestReview4ClaimJudgedClosing: a closing sentinel a closer that died left
// is cleared, and the claim tried once more.
func TestReview4ClaimJudgedClosing(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	cli := w.client("lagotto teardown")
	// ACTIVE, past its bound: the idle write meets the sentinel.
	if err := MarkActive(ctx, w.client("x"), "us-west-2", "x"); err != nil {
		t.Fatal(err)
	}
	w.advance(CreatorMarkBound)
	if err := w.tables["us-west-2"].PutRaw(lifecycle.TableName, map[string]ddbtypes.AttributeValue{
		"pk":    &ddbtypes.AttributeValueMemberS{Value: lifecycle.SentinelKey},
		"since": &ddbtypes.AttributeValueMemberS{Value: "2026-11-16T00:00:00.000Z"},
	}); err != nil {
		t.Fatal(err)
	}
	if l, err := ClaimJudged(ctx, cli, "us-west-2", true); err != nil || l == nil {
		t.Errorf("after a dead closer: %v", err)
	}
}

// TestReview4KeptReported (G1): what the account-wide teardown keeps is
// returned with why: lagotto-cli-user while something is attached to it,
// and one that is not lagotto's to delete.
func TestReview4KeptReported(t *testing.T) {
	w := newAWWorld(t)
	a := inventorytest.Account(acct)
	a.PolicyAttachments = 2
	w.src.SetAccount(a)
	r := w.run(true, 0)
	if r.Outcome != AccountWideRemoved {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if len(r.Kept) != 1 || r.Kept[0].Resource.Type != inventory.TypePolicy || !strings.Contains(r.Kept[0].Reason, "in use: attached to 2") {
		t.Errorf("attached: %+v", r.Kept)
	}
	// Only it is left, and untagged: nothing to do, and it says why.
	a.Roles, a.InstanceProfiles = nil, nil
	a.PolicyAttachments = 0
	a.Policy.Tags = nil
	w.src.SetAccount(a)
	r = w.run(true, 0)
	if r.Outcome != AccountWideNothing || len(r.Kept) != 1 || !strings.Contains(r.Kept[0].Reason, "not lagotto's to delete") {
		t.Errorf("untagged: %+v", r)
	}
	// Report mode the same.
	if r := w.run(false, 0); len(r.Kept) != 1 {
		t.Errorf("report: %+v", r)
	}
	planned, kept := AccountWideResources(inventory.Facts{AllRegions: true, Account: a}, inventory.Options{}, t0, nil)
	if len(planned) != 0 || len(kept) != 1 {
		t.Errorf("AccountWideResources: %v %v", planned, kept)
	}
}

// TestReview4UnremovableFunction (G4): a function lagotto does not remove is
// named as such by the account-wide teardown, and its poller commits
// nothing (no job would remove it): it releases before any delete.
func TestReview4UnremovableFunction(t *testing.T) {
	w := newAWWorld(t)
	r := inventorytest.Deployed("us-west-2", acct)
	r.Function.Tags = nil
	w.src.SetRegion(r)
	res := w.run(false, 0)
	if res.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(res.Blockers, "\n"), "a function named lagotto-capacity-poller is there that lagotto does not remove") {
		t.Errorf("%+v", res)
	}

	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) { r.Function.Tags = nil })
	h.pastGrace(t, true)
	out := Tick(context.Background(), h.cfg(true), h.deps())
	if out.Outcome != OutcomeReleased || len(h.ev.list()) != 0 || len(h.sched.inputs) != 0 || out.committed {
		t.Errorf("%+v, deletes %v, jobs %d\n%s", out, h.ev.list(), len(h.sched.inputs), h.log())
	}
	if !strings.Contains(h.log(), "did not pass the delete guard") || !strings.Contains(h.log(), "not committing the region's removal") {
		t.Errorf("log:\n%s", h.log())
	}
	if it := h.item(t); it.Lease != nil {
		t.Errorf("the lease was kept: %+v", it)
	}
	// Report mode reports as before: nothing is committed there anyway.
	h = newHarness(t)
	h.src.Update(region, func(r *inventory.Region) { r.Function.Tags = nil })
	h.pastGrace(t, false)
	if out := Tick(context.Background(), h.cfg(false), h.deps()); out.Outcome != OutcomeReported {
		t.Errorf("report: %+v\n%s", out, h.log())
	}
}

// TestReview4CancelWithoutFacts (G6): a run that stops before it reads its
// region's footprint (within its grace) still lists the region's schedules,
// and cancels a job an earlier run left.
func TestReview4CancelWithoutFacts(t *testing.T) {
	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) {
		r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName})
	})
	if r := Tick(context.Background(), h.cfg(true), h.deps()); r.Outcome != OutcomeWaiting || r.facts != nil {
		t.Fatalf("%+v", r)
	}
	if !strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot lagotto-teardown/"+FunctionJobName) {
		t.Errorf("not cancelled: %v\n%s", h.ev.list(), h.log())
	}
}

// TestReview4EndFailureAgesOut (libs findings 1 and 6, in lagotto): a
// creator whose mark could not be ended keeps the global scope in use for
// CreatorMarkBound, and then not: the grace runs from the bound.
func TestReview4EndFailureAgesOut(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	creator := w.client("setup")
	token, err := Mark(ctx, creator, lifecycle.ScopeGlobal, "set up")
	if err != nil {
		t.Fatal(err)
	}
	w.tables[region].Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			return errors.New("network down")
		}
		return nil
	}
	if err := EndMark(ctx, creator, lifecycle.ScopeGlobal, token); err == nil {
		t.Fatal("the end did not fail")
	}
	w.tables[region].Err = nil
	if r := w.run(true, 0); r.Outcome != AccountWideBlocked {
		t.Fatalf("within the bound: %+v", r)
	}
	w.advance(CreatorMarkBound)
	if r := w.run(true, time.Hour); r.Outcome != AccountWideWaiting || !strings.Contains(r.Why, "idle for 0s of its 1h0m0s grace") {
		t.Errorf("at the bound: %+v", r)
	}
	w.advance(time.Hour)
	if r := w.run(true, time.Hour); r.Outcome != AccountWideRemoved {
		t.Errorf("a grace after it: %+v\n%s", r, w.log())
	}
}

// TestReview4GraceStartAsJudged: the write that starts the global grace (an
// absent item, reap mode) is conditional too: a creator that marks first
// refuses it, and a closing control table fails it.
func TestReview4GraceStartAsJudged(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	marked := false
	w.tables[region].Hook = func(ctx context.Context, op string) {
		if op == "TransactWriteItems" && !marked {
			marked = true
			if _, err := Mark(ctx, w.client("setup"), lifecycle.ScopeGlobal, "set up"); err != nil {
				t.Error(err)
			}
		}
	}
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideBlocked || !strings.Contains(r.Why, "changed since it was judged") {
		t.Errorf("a mark before the grace starts: %+v", r)
	}
	w.tables[region].Hook = nil
	if g, _ := w.global(); len(g.Marks) != 1 {
		t.Errorf("the mark: %+v", g)
	}

	w = newAWWorld(t)
	if _, err := w.client("x").SetIdleIfUnchanged(ctx, Tool, region, lifecycle.Expect{}); err != nil {
		t.Fatal(err) // the control table
	}
	if err := w.tables[region].PutRaw(lifecycle.TableName, map[string]ddbtypes.AttributeValue{
		"pk":    &ddbtypes.AttributeValueMemberS{Value: lifecycle.SentinelKey},
		"since": &ddbtypes.AttributeValueMemberS{Value: "2026-11-16T12:00:00.000Z"}, // live
	}); err != nil {
		t.Fatal(err)
	}
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideFailed || !strings.Contains(r.Why, "closing") {
		t.Errorf("a closing control table: %+v", r)
	}
}

// TestReview4DeadItemBehindASentinel (the long model check): a dead regional
// item in a table a closer that died left its sentinel in is still finished:
// the sentinel, once stale, is cleared and the idle write tried again.
func TestReview4DeadItemBehindASentinel(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	creator := w.client("watch --local")
	token, err := Mark(ctx, creator, "us-west-2", "create watch")
	if err != nil {
		t.Fatal(err)
	}
	if err := EndMark(ctx, creator, "us-west-2", token); err != nil {
		t.Fatal(err)
	}
	if err := w.tables["us-west-2"].PutRaw(lifecycle.TableName, map[string]ddbtypes.AttributeValue{
		"pk":    &ddbtypes.AttributeValueMemberS{Value: lifecycle.SentinelKey},
		"since": &ddbtypes.AttributeValueMemberS{Value: "2026-11-16T12:00:00.000Z"},
	}); err != nil {
		t.Fatal(err)
	}
	if r := w.run(true, 0); r.Outcome != AccountWideFailed {
		t.Fatalf("a live closer's sentinel: %+v", r)
	}
	w.advance(21 * time.Minute)
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Fatalf("a stale one: %+v\n%s", r, w.log())
	}
	if it, ok, _ := w.client("reader").Get(ctx, Tool, "us-west-2"); ok {
		t.Errorf("the dead item is left: %+v", it)
	}
}
