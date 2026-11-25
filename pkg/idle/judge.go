package idle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spore-host/libs/lifecycle"
)

// How lagotto judges a lifecycle item (lagotto#340). Writers never idle an
// item; whether one is in use, and since when it has been idle, is read from
// what it holds and its times (lifecycle InUse and IdleSince, with
// CreatorMarkBound), and every write the CLI makes to an item is conditional
// on it still being exactly what it judged (lifecycle SetIdleIfUnchanged
// and ClaimIfUnchanged with ExpectOf the item it read).

// WhyInUse says why lagotto's item it is in use (lifecycle InUse, with
// CreatorMarkBound), or "" when it is not.
func WhyInUse(lc *lifecycle.Client, it lifecycle.Item) string {
	if !lc.InUse(it, CreatorMarkBound) {
		return ""
	}
	switch {
	case it.Lease != nil && !lc.LeaseStale(it):
		return "a teardown holds its lease (" + it.Lease.Owner + ")"
	case len(it.Holds) > 0:
		return fmt.Sprintf("held by %v", it.Holds)
	case lc.InUse(lifecycle.Item{Marks: it.Marks, ActiveAt: it.ActiveAt, LastActivityAt: it.LastActivityAt}, CreatorMarkBound):
		// (The item's times too: a mark with no time is aged from them.)
		var at time.Time
		for _, m := range it.Marks {
			if m.At.After(at) {
				at = m.At
			}
		}
		if at.IsZero() {
			return "a creator marked it (a mark with no time, aged from the item's activation)"
		}
		return "a creator marked it at " + at.UTC().Format(time.RFC3339)
	case lc.InUse(lifecycle.Item{PulseAt: it.PulseAt}, CreatorMarkBound):
		return "marked active at " + it.PulseAt.UTC().Format(time.RFC3339)
	}
	return "it is ACTIVE from an older lagotto, which records no time to age it by"
}

// untimedOnly reports whether the only thing keeping it in use is an older
// lagotto's ACTIVE, which records no time.
func untimedOnly(lc *lifecycle.Client, it lifecycle.Item) bool {
	stamped := it
	stamped.StampState, stamped.StampEpoch = it.State, it.Epoch
	return lc.InUse(it, CreatorMarkBound) && !lc.InUse(stamped, CreatorMarkBound)
}

// idleAsJudged records scope's item idle, exactly as judged (e), when it is
// not IDLE or TEARDOWN already, and returns what the item then is: e made
// IDLE, with its marks (which the caller judged dead) and any stale lease
// removed. A creator that marked, or anything that changed it since, refuses
// it (a *lifecycle.NotClaimableError, "changed since it was judged"). A
// write that applied though its read-back failed (lifecycle ErrApplied)
// stands.
func idleAsJudged(ctx context.Context, lc *lifecycle.Client, scope string, e lifecycle.Expect) (lifecycle.Expect, error) {
	if e.Exists && (e.State == lifecycle.StateIdle || e.State == lifecycle.StateTeardown) {
		return e, nil
	}
	_, err := lc.SetIdleIfUnchanged(ctx, Tool, scope, e)
	switch {
	case errors.Is(err, lifecycle.ErrChanged):
		return e, &lifecycle.NotClaimableError{Tool: Tool, Scope: scope, Reason: "changed since it was judged"}
	case errors.Is(err, lifecycle.ErrInUse):
		return e, &lifecycle.NotClaimableError{Tool: Tool, Scope: scope, Reason: "in use: a hold or a live lease"}
	case err != nil && !errors.Is(err, lifecycle.ErrApplied):
		return e, err
	}
	// What the write made of the judged item, not a read after it (a
	// creator may have marked and finished in between).
	return lifecycle.Expect{Exists: true, Epoch: e.Epoch, State: lifecycle.StateIdle, LastActivity: e.LastActivity}, nil
}

// ClaimJudged claims lagotto's item in scope for a teardown the CLI runs
// (`lagotto teardown`, no grace), as judged: it reads the item, refuses (a
// *lifecycle.NotClaimableError) while it is in use (WhyInUse), records it
// idle as judged (creating it, and its table, when absent), and claims it as
// judged, so it never erases a creator's
// mark. acceptOlder is for an explicit `lagotto teardown`, whose own reading
// of the region is its verdict: an older lagotto's ACTIVE, which records no
// time and so never ages, does not refuse it. A table closing under a
// closer that died is cleared and tried once more.
func ClaimJudged(ctx context.Context, lc *lifecycle.Client, scope string, acceptOlder bool) (*lifecycle.Lease, error) {
	for attempt := 0; ; attempt++ {
		l, err := claimJudged(ctx, lc, scope, acceptOlder, sleep)
		if errors.Is(err, lifecycle.ErrTableClosing) && attempt == 0 {
			// A closer that died left its sentinel (libs#88):
			// CloseTableIfEmpty takes a stale one over (and drops it while
			// items remain); a live closer's it leaves alone.
			if _, cerr := lc.CloseTableIfEmpty(ctx, scope); cerr != nil {
				return nil, errors.Join(err, cerr)
			}
			continue
		}
		return l, err
	}
}

func claimJudged(ctx context.Context, lc *lifecycle.Client, scope string, acceptOlder bool, sleep func(context.Context, time.Duration) error) (*lifecycle.Lease, error) {
	it, ok, err := lc.Get(ctx, Tool, scope)
	if err != nil {
		return nil, err
	}
	// An absent item is judged absent: it is made IDLE (with its table)
	// only if it still is, and claimed.
	if why := WhyInUse(lc, it); why != "" && (!acceptOlder || !untimedOnly(lc, it)) {
		return nil, &lifecycle.NotClaimableError{Tool: Tool, Scope: scope, Reason: "in use: " + why}
	}
	e, err := idleAsJudged(ctx, lc, scope, lifecycle.ExpectOf(it, ok))
	if err != nil {
		return nil, err
	}
	return claimAsJudged(ctx, lc, scope, e, sleep)
}

// claimAsJudged claims scope exactly as e (with no grace: the judgement was
// the grace). A failure other than a refusal may have applied all the same
// (an answer lost): a live lease this very Client took is then given back
// (giveBackOwn), so it leaves nothing for a creator to wait for.
func claimAsJudged(ctx context.Context, lc *lifecycle.Client, scope string, e lifecycle.Expect, sleep func(context.Context, time.Duration) error) (*lifecycle.Lease, error) {
	l, err := lc.ClaimIfUnchanged(ctx, Tool, scope, 0, e)
	if err != nil && !errors.Is(err, lifecycle.ErrNotClaimable) {
		fctx, cancel := freshContext(ctx)
		defer cancel()
		if _, aerr := lc.AdoptLease(fctx, Tool, scope); aerr == nil {
			err = fmt.Errorf("%w (%s)", err, giveBackOwn(fctx, lc, scope, sleep))
		}
	}
	return l, err
}

// finishLease finishes l (lifecycle Finish), tried again on failure. A
// retry after a Finish that applied, but whose answer was lost, finds its
// lease gone (ErrLeaseLost): the item is then read, and an absent one is the
// finish done.
func finishLease(ctx context.Context, sleep func(context.Context, time.Duration) error, lc *lifecycle.Client, scope string, l *lifecycle.Lease) error {
	err := retryOp(ctx, Config{Sleep: sleep}, func() error {
		if err := l.Finish(ctx); errors.Is(err, lifecycle.ErrLeaseLost) {
			return &final{err}
		} else {
			return err
		}
	})
	if errors.Is(err, lifecycle.ErrLeaseLost) {
		if _, ok, rerr := lc.Get(ctx, Tool, scope); rerr == nil && !ok {
			return nil // finished: the first try applied
		}
	}
	return err
}

// releaseLease gives l back (lifecycle Release), tried again on failure; a
// lease already gone is not an error.
func releaseLease(ctx context.Context, sleep func(context.Context, time.Duration) error, l *lifecycle.Lease) error {
	ctx, cancel := freshContext(ctx)
	defer cancel()
	err := retryOp(ctx, Config{Sleep: sleep}, func() error {
		if err := l.Release(ctx); errors.Is(err, lifecycle.ErrLeaseLost) {
			return &final{err}
		} else {
			return err
		}
	})
	if errors.Is(err, lifecycle.ErrLeaseLost) {
		return nil
	}
	return err
}

// freshContext is a short context for giving a lease back, or ending a mark,
// after ctx was cancelled (the CLI interrupted): ctx's values, not its
// cancellation, and 30 s.
func freshContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}
