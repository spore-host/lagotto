package watcher

import (
	"context"
	"testing"
	"time"
)

// expiryTestStore builds a substrate-backed Store for driving PollAll's TTL-expiry
// branch. A sweep in which every watch is already past its TTL never reaches the
// capacity search, so a nil truffle client is safe here.
func expiryTestStore(t *testing.T) *Store {
	t.Helper()
	return capTestStore(t)
}

func expiredWatch(id string) *Watch {
	now := time.Now().UTC()
	return &Watch{
		WatchID:             id,
		UserID:              "arn:alice",
		Status:              StatusActive,
		Action:              ActionSpawn,
		InstanceTypePattern: "g7e.*",
		Regions:             []string{"us-east-1", "us-west-2"},
		CreatedAt:           now.Add(-48 * time.Hour),
		UpdatedAt:           now.Add(-48 * time.Hour),
		ExpiresAt:           now.Add(-1 * time.Minute), // TTL already elapsed
		TTLTimestamp:        RetentionTTL(now.Add(-1 * time.Minute)),
	}
}

// TestPollAll_ExpiryLeavesTombstoneAndNotifiesOnce covers #161 end to end: the TTL
// transition leaves a resolvable status=expired record (not a deleted one), the
// user is told once, and a second sweep neither re-notifies nor re-expires —
// notify-once is structural, since only ACTIVE watches are ever loaded.
func TestPollAll_ExpiryLeavesTombstoneAndNotifiesOnce(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()

	w := expiredWatch("w-expire")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	var notified []string
	p := NewPoller(nil, store, false)
	p.notifyExpired = func(_ context.Context, got *Watch) error {
		// The notifier derives the give-up duration from Status + UpdatedAt, so the
		// poller must hand it the POST-transition view, not the loaded record.
		if got.Status != StatusExpired {
			t.Errorf("notified with Status = %q, want expired", got.Status)
		}
		if _, ok := got.TimeToGiveUp(); !ok {
			t.Error("notified watch does not report TimeToGiveUp")
		}
		notified = append(notified, got.WatchID)
		return nil
	}

	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	if summary.Expired != 1 {
		t.Errorf("Expired = %d, want 1", summary.Expired)
	}
	if summary.Watched != 0 {
		t.Errorf("Watched = %d, want 0 (the watch was past its TTL)", summary.Watched)
	}
	if len(notified) != 1 || notified[0] != "w-expire" {
		t.Errorf("notified = %v, want [w-expire]", notified)
	}

	// The tombstone is present and terminal.
	got, err := store.GetWatch(ctx, "w-expire")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got == nil {
		t.Fatal("expired watch resolved to nil — no tombstone (#161)")
	}
	if got.Status != StatusExpired {
		t.Errorf("Status = %q, want expired", got.Status)
	}

	// A second sweep sees nothing: the watch is no longer active.
	notified = nil
	summary2, err := p.PollAll(ctx)
	if err != nil {
		t.Fatalf("second PollAll: %v", err)
	}
	if summary2.Expired != 0 {
		t.Errorf("second sweep Expired = %d, want 0", summary2.Expired)
	}
	if len(notified) != 0 {
		t.Errorf("second sweep notified %v, want none (notify-once)", notified)
	}
}

// TestPollAll_ExpiryNotifyFailureIsNonFatal confirms a failing notification never
// costs the tombstone or the sweep — the record is already durable when we notify.
func TestPollAll_ExpiryNotifyFailureIsNonFatal(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()

	if err := store.PutWatch(ctx, expiredWatch("w-expire-noisy")); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	p := NewPoller(nil, store, false)
	p.notifyExpired = func(context.Context, *Watch) error {
		return context.DeadlineExceeded
	}

	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	if summary.Expired != 1 {
		t.Errorf("Expired = %d, want 1", summary.Expired)
	}
	got, _ := store.GetWatch(ctx, "w-expire-noisy")
	if got == nil || got.Status != StatusExpired {
		t.Fatalf("tombstone not written despite notify failure: %+v", got)
	}
}

// TestExpiredNotifier_NoNotifierWired checks the nil-safe resolution order: with
// neither a seam nor a Notifier there is nothing to call, and the expiry path must
// still persist the tombstone rather than skip it.
func TestExpiredNotifier_NoNotifierWired(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()

	if err := store.PutWatch(ctx, expiredWatch("w-expire-quiet")); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	p := NewPoller(nil, store, false)
	if p.expiredNotifier() != nil {
		t.Error("expiredNotifier() non-nil with no notifier wired")
	}
	if _, err := p.PollAll(ctx); err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	got, _ := store.GetWatch(ctx, "w-expire-quiet")
	if got == nil || got.Status != StatusExpired {
		t.Fatalf("tombstone not written for a notify-less watch: %+v", got)
	}
}
