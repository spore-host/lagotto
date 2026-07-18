package watcher

import (
	"context"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// capTestStore builds a substrate-backed Store for driving recordOutcome's cap
// state machine directly (recordOutcome is unexported, so this lives in-package).
func capTestStore(t *testing.T) *Store {
	t.Helper()
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, "cap-watches")
	env.CreateHistoryTable(t, "cap-history")
	return NewStore(env.AWSConfig, "cap-watches", "cap-history")
}

func capWatch() *Watch {
	now := time.Now().UTC()
	return &Watch{
		WatchID:      "w-cap",
		UserID:       "arn:alice",
		Status:       StatusActive,
		Action:       ActionSpawn,
		CreatedAt:    now,
		UpdatedAt:    now,
		ExpiresAt:    now.Add(24 * time.Hour),
		TTLTimestamp: now.Add(24 * time.Hour).Unix(),
	}
}

// TestRecordOutcome_UnknownFailureCap verifies #41.3: consecutive FailureUnknown
// polls keep the watch active until the cap, then flip it to failed; a genuine
// FailureCapacity poll in between resets the streak and is never itself capped.
func TestRecordOutcome_UnknownFailureCap(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	p := NewPoller(nil, store, false)

	w := capWatch()
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	// First MaxConsecutiveFailures-1 unknown failures keep the watch active.
	for i := 1; i < MaxConsecutiveFailures; i++ {
		summary := &PollSummary{}
		p.recordOutcome(ctx, w, &MatchResult{WatchID: w.WatchID}, FailureUnknown, summary)
		if summary.Retrying != 1 || summary.Failed != 0 {
			t.Fatalf("failure %d: got Retrying=%d Failed=%d, want Retrying=1 Failed=0", i, summary.Retrying, summary.Failed)
		}
		got, _ := store.GetWatch(ctx, w.WatchID)
		if got.Status != StatusActive {
			t.Fatalf("failure %d: status = %q, want active", i, got.Status)
		}
		if got.ConsecutiveFailures != i {
			t.Fatalf("failure %d: ConsecutiveFailures = %d, want %d", i, got.ConsecutiveFailures, i)
		}
	}

	// The cap-reaching failure stops the watch as failed.
	summary := &PollSummary{}
	p.recordOutcome(ctx, w, &MatchResult{WatchID: w.WatchID}, FailureUnknown, summary)
	if summary.Failed != 1 {
		t.Fatalf("cap-reaching failure: Failed = %d, want 1", summary.Failed)
	}
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.Status != StatusFailed {
		t.Errorf("status after cap = %q, want failed", got.Status)
	}
}

// TestRecordOutcome_CapacityResetsStreak confirms a genuine capacity failure
// clears an accumulated unknown-failure streak (the cap counts only *consecutive*
// unclassified faults) and is itself uncapped.
func TestRecordOutcome_CapacityResetsStreak(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	p := NewPoller(nil, store, false)

	w := capWatch()
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	// Rack up some unknown failures short of the cap.
	for i := 0; i < MaxConsecutiveFailures-1; i++ {
		p.recordOutcome(ctx, w, &MatchResult{WatchID: w.WatchID}, FailureUnknown, &PollSummary{})
	}
	// A genuine capacity failure resets the streak.
	summary := &PollSummary{}
	p.recordOutcome(ctx, w, &MatchResult{WatchID: w.WatchID}, FailureCapacity, summary)
	if summary.Retrying != 1 {
		t.Errorf("capacity failure: Retrying = %d, want 1", summary.Retrying)
	}
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures after capacity failure = %d, want 0 (streak reset)", got.ConsecutiveFailures)
	}
	if got.Status != StatusActive {
		t.Errorf("status = %q, want active (capacity is uncapped)", got.Status)
	}

	// The in-memory watch must also reflect the reset so the next unknown streak
	// starts from 1, not from the stale count.
	if w.ConsecutiveFailures != 0 {
		t.Errorf("in-memory ConsecutiveFailures = %d, want 0", w.ConsecutiveFailures)
	}
}
