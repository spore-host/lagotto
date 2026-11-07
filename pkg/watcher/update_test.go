package watcher_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// putVersioned stores w and reads it back with its raw version.
func putVersioned(t *testing.T, store *watcher.Store, w *watcher.Watch) (*watcher.Watch, string) {
	t.Helper()
	ctx := context.Background()
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	got, version, err := store.GetWatchVersioned(ctx, w.WatchID)
	if err != nil || got == nil {
		t.Fatalf("GetWatchVersioned: %v, %v", got, err)
	}
	if version == "" {
		t.Fatal("GetWatchVersioned returned no version for a stored watch")
	}
	return got, version
}

func TestGetWatchVersioned_Missing(t *testing.T) {
	store := setupStore(t)
	w, v, err := store.GetWatchVersioned(context.Background(), "w-none")
	if err != nil || w != nil || v != "" {
		t.Fatalf("GetWatchVersioned(missing) = %v, %q, %v; want nil, \"\", nil", w, v, err)
	}
}

// TestUpdateWatchFields_SearchChangeResetsDerivedState: the changed fields are
// written, derived search state is cleared, and the heartbeat is kept.
func TestUpdateWatchFields_SearchChangeResetsDerivedState(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-upd", "arn:alice")
	w.ConsecutiveFailures = 4
	w.Attempts = 9
	w.LastError = "capacity: InsufficientInstanceCapacity: none"
	w.MatchCount = 2
	w.QuotaCapped = true
	w.QuotaCappedCount = 3
	w.QuotaCappedAt = time.Now().UTC()
	w.QuotaCapReason = "capped"
	w.QuotaInfeasible = []watcher.QuotaInfeasibility{{InstanceType: "g5.xlarge", Region: "us-east-1", At: time.Now().UTC()}}
	w.Project = "old"
	got, version := putVersioned(t, store, w)

	nw := *got
	nw.Regions = []string{"us-east-1", "us-west-2"}
	nw.MaxPrice = 0 // omitempty: REMOVEd
	nw.Project = "" // omitempty: REMOVEd
	if err := store.UpdateWatchFields(ctx, &nw, watcher.WatchUpdate{
		Fields:  []string{watcher.FieldRegions, watcher.FieldMaxPrice, watcher.FieldProject},
		Version: version,
		Status:  watcher.StatusActive,
		Owner:   "t1",
	}, time.Now()); err != nil {
		t.Fatalf("UpdateWatchFields: %v", err)
	}

	after, _ := store.GetWatch(ctx, "w-upd")
	if strings.Join(after.Regions, ",") != "us-east-1,us-west-2" || after.MaxPrice != 0 || after.Project != "" {
		t.Errorf("fields not written: regions=%v max=%v project=%q", after.Regions, after.MaxPrice, after.Project)
	}
	if after.ConsecutiveFailures != 0 || after.QuotaCapped || after.QuotaCappedCount != 0 || after.QuotaCapReason != "" || len(after.QuotaInfeasible) != 0 || !after.QuotaCappedAt.IsZero() {
		t.Errorf("derived search state not reset: %+v", after)
	}
	if after.Attempts != 9 || after.LastError == "" || after.MatchCount != 2 {
		t.Errorf("heartbeat not kept: attempts=%d last_error=%q match_count=%d", after.Attempts, after.LastError, after.MatchCount)
	}
	if !strings.HasPrefix(after.LeaseOwner, watcher.UpdateLeasePrefix) || !after.LeaseExpiresAt.After(time.Now()) {
		t.Errorf("update did not fence the watch: lease %q until %v", after.LeaseOwner, after.LeaseExpiresAt)
	}
	if after.Status != watcher.StatusActive || after.InstanceTypePattern != "g5.xlarge" || after.Action != watcher.ActionNotify {
		t.Errorf("unrelated fields changed: %+v", after)
	}

	// A notify-only change keeps the derived state.
	got2, _, _ := store.GetWatchVersioned(ctx, "w-upd")
	got2.ConsecutiveFailures = 0
	if _, err := store.IncrementConsecutiveFailures(ctx, "w-upd"); err != nil {
		t.Fatal(err)
	}
	_, v2, _ := store.GetWatchVersioned(ctx, "w-upd")
	got2.NotifyChannels = []watcher.NotifyChannel{{Type: "webhook", Target: "https://example.com/h"}}
	if err := store.UpdateWatchFields(ctx, got2, watcher.WatchUpdate{
		Fields: []string{watcher.FieldNotifyChannels}, Version: v2, Status: watcher.StatusActive, Owner: "t2",
	}, time.Now()); err != nil {
		t.Fatalf("notify-only UpdateWatchFields: %v", err)
	}
	after2, _ := store.GetWatch(ctx, "w-upd")
	if after2.ConsecutiveFailures != 1 || len(after2.NotifyChannels) != 1 {
		t.Errorf("notify-only update: consecutive_failures=%d (want kept at 1), channels=%v", after2.ConsecutiveFailures, after2.NotifyChannels)
	}
}

// TestUpdateWatchFields_Conflicts: each condition refuses the write and
// leaves the record unchanged.
func TestUpdateWatchFields_Conflicts(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		setup func(t *testing.T, store *watcher.Store, w *watcher.Watch)
	}{
		{"version moved", func(t *testing.T, store *watcher.Store, w *watcher.Watch) {
			// Any write that moves updated_at after the read (ClaimLease does;
			// it writes a different string than PutWatch's nanosecond form).
			if err := store.ClaimLease(ctx, w.WatchID, "poller", time.Now().Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
		}},
		{"lease held", nil},
		{"status changed", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := setupStore(t)
			w := newTestWatch("w-c", "arn:alice")
			switch tc.name {
			case "lease held":
				w.LeaseOwner, w.LeaseExpiresAt = "hosted-lambda", time.Now().UTC().Add(time.Minute)
			}
			got, version := putVersioned(t, store, w)
			if tc.setup != nil {
				tc.setup(t, store, got)
			}
			status := watcher.StatusActive
			if tc.name == "status changed" {
				status = watcher.StatusExpired
			}
			nw := *got
			nw.Regions = []string{"eu-west-1"}
			err := store.UpdateWatchFields(ctx, &nw, watcher.WatchUpdate{
				Fields: []string{watcher.FieldRegions}, Version: version, Status: status, Owner: "t",
			}, time.Now())
			if !errors.Is(err, watcher.ErrUpdateConflict) {
				t.Fatalf("err = %v, want ErrUpdateConflict", err)
			}
			after, _ := store.GetWatch(ctx, "w-c")
			if strings.Join(after.Regions, ",") != "us-east-1" {
				t.Errorf("a refused update wrote regions %v", after.Regions)
			}
		})
	}
}

func TestUpdateWatchFields_LandingRefused(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-land", "arn:alice")
	w.Action = watcher.ActionSpawn
	w.PendingInstanceID, w.PendingRegion = "i-0abc", "us-east-1"
	got, version := putVersioned(t, store, w)
	nw := *got
	nw.Spot = false
	err := store.UpdateWatchFields(ctx, &nw, watcher.WatchUpdate{
		Fields: []string{watcher.FieldSpot}, Version: version, Status: watcher.StatusActive, Owner: "t",
	}, time.Now())
	if !errors.Is(err, watcher.ErrUpdateConflict) {
		t.Fatalf("err = %v, want ErrUpdateConflict for a landing watch", err)
	}
}

// TestUpdateWatchFields_StaleAndUpdateLeasesDoNotBlock: an expired lease and
// an earlier update's fence are not a live poller.
func TestUpdateWatchFields_StaleAndUpdateLeasesDoNotBlock(t *testing.T) {
	for _, lease := range []struct {
		owner string
		exp   time.Duration
	}{{"hosted-lambda", -time.Minute}, {watcher.UpdateLeasePrefix + "prev", time.Minute}} {
		store := setupStore(t)
		ctx := context.Background()
		w := newTestWatch("w-l", "arn:alice")
		w.LeaseOwner, w.LeaseExpiresAt = lease.owner, time.Now().UTC().Add(lease.exp)
		got, version := putVersioned(t, store, w)
		nw := *got
		nw.Spot = false
		if err := store.UpdateWatchFields(ctx, &nw, watcher.WatchUpdate{
			Fields: []string{watcher.FieldSpot}, Version: version, Status: watcher.StatusActive, Owner: "t",
		}, time.Now()); err != nil {
			t.Errorf("lease %q (%v): %v", lease.owner, lease.exp, err)
		}
	}
}

func TestUpdateWatchFields_Reactivate(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-re", "arn:alice")
	w.Status = watcher.StatusExpired
	w.ExpiredReason = watcher.ExpiredReasonUnacknowledged
	got, version := putVersioned(t, store, w)
	nw := *got
	nw.InstanceTypePattern = "g5.xlarge,g6.xlarge"
	if err := store.UpdateWatchFields(ctx, &nw, watcher.WatchUpdate{
		Fields: []string{watcher.FieldInstanceTypePattern}, Version: version, Status: watcher.StatusExpired, Reactivate: true, Owner: "t",
	}, time.Now()); err != nil {
		t.Fatalf("reactivate: %v", err)
	}
	after, _ := store.GetWatch(ctx, "w-re")
	if after.Status != watcher.StatusActive || after.ExpiredReason != "" || after.InstanceTypePattern != "g5.xlarge,g6.xlarge" {
		t.Errorf("reactivate: status=%s reason=%q pattern=%q", after.Status, after.ExpiredReason, after.InstanceTypePattern)
	}
	if after.TTLTimestamp != watcher.RetentionTTL(w.ExpiresAt) {
		t.Errorf("ttl_timestamp = %d, want the retention horizon of the expiry %d", after.TTLTimestamp, watcher.RetentionTTL(w.ExpiresAt))
	}
	if !after.ExpiresAt.Equal(w.ExpiresAt.Truncate(time.Second)) && !after.ExpiresAt.Equal(w.ExpiresAt) {
		t.Errorf("expires_at changed: %v -> %v", w.ExpiresAt, after.ExpiresAt)
	}
}

func TestUpdateWatchFields_RejectsImmutableField(t *testing.T) {
	store := setupStore(t)
	w := newTestWatch("w-imm", "arn:alice")
	for _, f := range [][]string{{"action"}, nil} {
		if err := store.UpdateWatchFields(context.Background(), w, watcher.WatchUpdate{Fields: f, Status: watcher.StatusActive}, time.Now()); err == nil {
			t.Errorf("fields %v: no error", f)
		}
	}
}

// TestRecordUpdate writes an `updated` row that a real match at the same
// instant neither overwrites nor is overwritten by.
func TestRecordUpdate(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-hist", "arn:alice")
	at := time.Now().UTC()
	m := &watcher.MatchResult{WatchID: w.WatchID, UserID: w.UserID, MatchedAt: at, ActionTaken: "notified", InstanceType: "g5.xlarge"}
	if err := store.RecordHistory(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpdate(ctx, w, []string{watcher.FieldRegions}, at); err != nil {
		t.Fatalf("RecordUpdate: %v", err)
	}
	rows, err := store.ListMatchHistory(ctx, w.WatchID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("%d history rows, want the match and the update", len(rows))
	}
	var upd, match int
	for _, r := range rows {
		switch r.ActionTaken {
		case watcher.ActionUpdated:
			upd++
			if strings.Join(r.ChangedFields, ",") != watcher.FieldRegions || r.MatchedAt.Equal(at) {
				t.Errorf("update row: fields %v at %v (the match's key is %v)", r.ChangedFields, r.MatchedAt, at)
			}
		case "notified":
			match++
		}
	}
	if upd != 1 || match != 1 {
		t.Errorf("rows: %d updated, %d matches", upd, match)
	}
}
