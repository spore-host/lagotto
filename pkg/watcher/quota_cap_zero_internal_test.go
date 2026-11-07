package watcher

import (
	"context"
	"strings"
	"testing"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

// Regression tests for #274: a fleet whose quota refuses the very FIRST worker
// is capped at 0, and 0 used to mean "not capped", so the fleet re-tried its
// doomed launch every cycle instead of backing off.

// TestQuotaCapAtZero_NextCycleBacksOff: the first-worker refusal is recorded
// as a cap (and notified once), and the next cycle makes no launch attempt.
func TestQuotaCapAtZero_NextCycleBacksOff(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-cap-zero")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p, calls := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		return nil, &capErr{quotaCode}
	})
	notified := 0
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { notified++; return nil }

	summary := &PollSummary{}
	p.pollFleetWatch(ctx, w, summary)
	if *calls != 1 {
		t.Fatalf("first cycle RunInstances attempts = %d, want 1", *calls)
	}
	if summary.QuotaCapped != 1 {
		t.Errorf("first cycle summary.QuotaCapped = %d, want 1", summary.QuotaCapped)
	}
	if notified != 1 {
		t.Errorf("notifications for a cap at 0 = %d, want 1", notified)
	}

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if !got.QuotaCapped || got.QuotaCappedCount != 0 || !got.IsQuotaCapped() {
		t.Fatalf("stored cap: marker=%v count=%d IsQuotaCapped=%v, want capped at 0",
			got.QuotaCapped, got.QuotaCappedCount, got.IsQuotaCapped())
	}
	if !strings.Contains(got.QuotaCapReason, "capped at 0/5") {
		t.Errorf("QuotaCapReason = %q, want it to say capped at 0/5", got.QuotaCapReason)
	}

	// Next cycle, from the record as a stateless (hosted) poller reads it.
	summary = &PollSummary{}
	p.pollFleetWatch(ctx, got, summary)
	if *calls != 1 {
		t.Errorf("RunInstances attempts after the next cycle = %d, want still 1 (backed off)", *calls)
	}
	if summary.QuotaCapped != 1 {
		t.Errorf("next cycle summary.QuotaCapped = %d, want 1 (suppressed, still capped)", summary.QuotaCapped)
	}
	if notified != 1 {
		t.Errorf("notifications after the suppressed cycle = %d, want still 1", notified)
	}

	// Past the re-probe interval: exactly one re-probe, refused again, and the
	// unchanged level is not re-notified.
	got, _ = store.GetWatch(ctx, w.WatchID)
	got.QuotaCappedAt = time.Now().UTC().Add(-(QuotaCapReprobeInterval + time.Minute))
	p.pollFleetWatch(ctx, got, &PollSummary{})
	if *calls != 2 {
		t.Errorf("RunInstances attempts after the interval = %d, want 2", *calls)
	}
	if notified != 1 {
		t.Errorf("notifications after a re-cap at the same level 0 = %d, want 1", notified)
	}
}

// TestQuotaCapAtZero_ClearedOnSuccess: a launch that succeeds after a cap at 0
// clears the cap, marker included, so a later cap notifies again.
func TestQuotaCapAtZero_ClearedOnSuccess(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-cap-zero-clear")
	w.QuotaCapped = true
	w.QuotaCappedAt = time.Now().UTC().Add(-(QuotaCapReprobeInterval + time.Minute))
	w.QuotaCapReason = "fleet capped at 0/5"
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p, _ := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})
	p.pollFleetWatch(ctx, w, &PollSummary{})

	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.QuotaCapped || got.IsQuotaCapped() || got.QuotaCapReason != "" {
		t.Errorf("cap at 0 not cleared by a successful launch: marker=%v reason=%q", got.QuotaCapped, got.QuotaCapReason)
	}
	if w.QuotaCapped || w.IsQuotaCapped() {
		t.Error("in-memory cap at 0 not cleared")
	}
}

// TestIsQuotaCapped_OldRecords: records written before the #274 marker keep
// exactly their old reading, and a marker left behind by a pre-#274 clear
// (which removes the anchor but not the marker) does not read as a cap at 0.
func TestIsQuotaCapped_OldRecords(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name string
		w    Watch
		want bool
	}{
		{"never capped", Watch{}, false},
		{"pre-#274 cap at 2", Watch{QuotaCappedCount: 2, QuotaCappedAt: now}, true},
		{"pre-#274 cap at 0 (no marker)", Watch{QuotaCappedAt: now}, false},
		{"#274 cap at 0", Watch{QuotaCapped: true, QuotaCappedAt: now}, true},
		{"#274 cap at 3", Watch{QuotaCapped: true, QuotaCappedCount: 3, QuotaCappedAt: now}, true},
		{"marker left by a pre-#274 clear", Watch{QuotaCapped: true}, false},
	}
	for _, tt := range tests {
		if got := tt.w.IsQuotaCapped(); got != tt.want {
			t.Errorf("%s: IsQuotaCapped() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestQuotaCapAtZero_OldRecordStillRetries: a pre-#274 record capped at 0 has
// no marker and reads as not capped, exactly as before: the next cycle tries
// the launch (and, on refusal, now records the cap with the marker).
func TestQuotaCapAtZero_OldRecordStillRetries(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-cap-zero-old")
	w.QuotaCappedAt = time.Now().UTC() // inside the interval, but no marker
	w.QuotaCapReason = "fleet capped at 0/5"
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p, calls := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		return nil, &capErr{quotaCode}
	})
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }
	p.pollFleetWatch(ctx, w, &PollSummary{})
	if *calls != 1 {
		t.Errorf("RunInstances attempts on an unmarked old record = %d, want 1 (reads as not capped)", *calls)
	}
	got, _ := store.GetWatch(ctx, w.WatchID)
	if !got.QuotaCapped {
		t.Error("the re-cap did not write the #274 marker")
	}
}
