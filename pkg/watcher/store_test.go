package watcher_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	testWatchesTable = "test-watches"
	testHistoryTable = "test-history"
)

func setupStore(t *testing.T) *watcher.Store {
	t.Helper()
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, testWatchesTable)
	env.CreateHistoryTable(t, testHistoryTable)
	return watcher.NewStore(env.AWSConfig, testWatchesTable, testHistoryTable)
}

func newTestWatch(id, userID string) *watcher.Watch {
	now := time.Now().UTC()
	return &watcher.Watch{
		WatchID:             id,
		UserID:              userID,
		Status:              watcher.StatusActive,
		InstanceTypePattern: "g5.xlarge",
		Regions:             []string{"us-east-1"},
		Spot:                true,
		MaxPrice:            1.50,
		Action:              watcher.ActionNotify,
		CreatedAt:           now,
		UpdatedAt:           now,
		ExpiresAt:           now.Add(24 * time.Hour),
		TTLTimestamp:        now.Add(24 * time.Hour).Unix(),
	}
}

func TestPutAndGetWatch(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-test1", "arn:aws:iam::123456789012:user/alice")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	got, err := store.GetWatch(ctx, "w-test1")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got == nil {
		t.Fatal("GetWatch returned nil")
	}
	if got.WatchID != "w-test1" {
		t.Errorf("WatchID = %q, want w-test1", got.WatchID)
	}
	if got.InstanceTypePattern != "g5.xlarge" {
		t.Errorf("Pattern = %q, want g5.xlarge", got.InstanceTypePattern)
	}
	if got.Status != watcher.StatusActive {
		t.Errorf("Status = %q, want active", got.Status)
	}
}

func TestGetWatch_NotFound(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	got, err := store.GetWatch(ctx, "w-nonexistent")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for nonexistent watch, got %+v", got)
	}
}

func TestListWatchesByUser(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	alice := "arn:aws:iam::123456789012:user/alice"
	bob := "arn:aws:iam::123456789012:user/bob"

	_ = store.PutWatch(ctx, newTestWatch("w-a1", alice))
	_ = store.PutWatch(ctx, newTestWatch("w-a2", alice))
	_ = store.PutWatch(ctx, newTestWatch("w-b1", bob))

	watches, err := store.ListWatchesByUser(ctx, alice, "")
	if err != nil {
		t.Fatalf("ListWatchesByUser: %v", err)
	}
	if len(watches) != 2 {
		t.Errorf("got %d watches for alice, want 2", len(watches))
	}
}

func TestListActiveWatches(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	user := "arn:aws:iam::123456789012:user/test"
	w1 := newTestWatch("w-active", user)
	w2 := newTestWatch("w-cancelled", user)
	w2.Status = watcher.StatusCancelled

	_ = store.PutWatch(ctx, w1)
	_ = store.PutWatch(ctx, w2)

	active, err := store.ListActiveWatches(ctx)
	if err != nil {
		t.Fatalf("ListActiveWatches: %v", err)
	}
	if len(active) != 1 {
		t.Errorf("got %d active watches, want 1", len(active))
	}
	if len(active) > 0 && active[0].WatchID != "w-active" {
		t.Errorf("WatchID = %q, want w-active", active[0].WatchID)
	}
}

func TestUpdateWatchStatus(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-status", "arn:aws:iam::123456789012:user/test")
	_ = store.PutWatch(ctx, w)

	if err := store.UpdateWatchStatus(ctx, "w-status", watcher.StatusCancelled); err != nil {
		t.Fatalf("UpdateWatchStatus: %v", err)
	}

	got, _ := store.GetWatch(ctx, "w-status")
	if got.Status != watcher.StatusCancelled {
		t.Errorf("Status = %q, want cancelled", got.Status)
	}
}

func TestRecordMatch(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	user := "arn:aws:iam::123456789012:user/test"
	w := newTestWatch("w-match", user)
	_ = store.PutWatch(ctx, w)

	m := &watcher.MatchResult{
		WatchID:          "w-match",
		UserID:           user,
		Region:           "us-east-1",
		AvailabilityZone: "us-east-1a",
		InstanceType:     "g5.xlarge",
		Price:            0.75,
		IsSpot:           true,
		MatchedAt:        time.Now().UTC(),
		ActionTaken:      "notified",
	}

	if err := store.RecordMatch(ctx, w, m); err != nil {
		t.Fatalf("RecordMatch: %v", err)
	}

	// Verify watch was updated
	got, _ := store.GetWatch(ctx, "w-match")
	if got.MatchCount != 1 {
		t.Errorf("MatchCount = %d, want 1", got.MatchCount)
	}
	if got.LastMatch == nil {
		t.Fatal("LastMatch is nil")
	}
	if got.LastMatch.InstanceType != "g5.xlarge" {
		t.Errorf("LastMatch.InstanceType = %q, want g5.xlarge", got.LastMatch.InstanceType)
	}

	// Verify history was written
	history, err := store.ListMatchHistory(ctx, "w-match")
	if err != nil {
		t.Fatalf("ListMatchHistory: %v", err)
	}
	if len(history) != 1 {
		t.Errorf("got %d history records, want 1", len(history))
	}
}

// TestRecordMatch_ResetsWatchTTL verifies #41.4: a matched watch's own
// ttl_timestamp is bumped to the retention window (~90d out), not left at its
// original short expiry — so the record isn't DynamoDB-TTL-deleted early.
func TestRecordMatch_ResetsWatchTTL(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	user := "arn:aws:iam::123456789012:user/test"
	w := newTestWatch("w-ttl", user) // ExpiresAt/TTLTimestamp = now+24h
	origTTL := w.TTLTimestamp
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	m := &watcher.MatchResult{
		WatchID: "w-ttl", UserID: user, Region: "us-east-1",
		InstanceType: "g5.xlarge", MatchedAt: time.Now().UTC(), ActionTaken: "spawned",
	}
	if err := store.RecordMatch(ctx, w, m); err != nil {
		t.Fatalf("RecordMatch: %v", err)
	}

	got, _ := store.GetWatch(ctx, "w-ttl")
	// New TTL should be far past the original 24h expiry (retention is 90 days).
	if got.TTLTimestamp <= origTTL {
		t.Errorf("watch TTLTimestamp = %d, want > original %d (retention window)", got.TTLTimestamp, origTTL)
	}
	minExpected := time.Now().UTC().Add(60 * 24 * time.Hour).Unix()
	if got.TTLTimestamp < minExpected {
		t.Errorf("watch TTLTimestamp = %d, want at least ~90d out (>= %d)", got.TTLTimestamp, minExpected)
	}
}

// TestConsecutiveFailures covers #41.3's counter: Increment returns a running
// total and persists it; Reset zeroes it; an absent attribute starts at 0.
func TestConsecutiveFailures(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-fail", "arn:aws:iam::123456789012:user/test")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	// Absent attribute (pre-#41 record) increments from 0 → 1, 2, 3.
	for want := 1; want <= 3; want++ {
		n, err := store.IncrementConsecutiveFailures(ctx, "w-fail")
		if err != nil {
			t.Fatalf("IncrementConsecutiveFailures: %v", err)
		}
		if n != want {
			t.Errorf("Increment returned %d, want %d", n, want)
		}
	}
	got, _ := store.GetWatch(ctx, "w-fail")
	if got.ConsecutiveFailures != 3 {
		t.Errorf("persisted ConsecutiveFailures = %d, want 3", got.ConsecutiveFailures)
	}

	// Reset zeroes it.
	if err := store.ResetConsecutiveFailures(ctx, "w-fail"); err != nil {
		t.Fatalf("ResetConsecutiveFailures: %v", err)
	}
	got, _ = store.GetWatch(ctx, "w-fail")
	if got.ConsecutiveFailures != 0 {
		t.Errorf("after reset ConsecutiveFailures = %d, want 0", got.ConsecutiveFailures)
	}
}

func TestUpdateLastPolled(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-poll", "arn:aws:iam::123456789012:user/test")
	_ = store.PutWatch(ctx, w)

	if err := store.UpdateLastPolled(ctx, "w-poll"); err != nil {
		t.Fatalf("UpdateLastPolled: %v", err)
	}

	// Just verify no error — the timestamp is set server-side
}

// TestExtendWatch is the #161 REGRESSION GUARD, and the assertion below used to
// say the opposite: it required ttl_timestamp == newExpiry.Unix(), which is
// exactly the bug — DynamoDB then hard-deletes the record at the instant the watch
// expires, taking the status=expired tombstone with it. ttl_timestamp is a
// RETENTION horizon (expiry + 90d), never the watch's own expiry.
func TestExtendWatch(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-extend", "arn:aws:iam::123456789012:user/test")
	_ = store.PutWatch(ctx, w)

	newExpiry := time.Now().UTC().Add(48 * time.Hour)
	if err := store.ExtendWatch(ctx, "w-extend", newExpiry, false); err != nil {
		t.Fatalf("ExtendWatch: %v", err)
	}

	got, _ := store.GetWatch(ctx, "w-extend")
	if !got.ExpiresAt.Equal(newExpiry.Truncate(time.Second)) {
		t.Errorf("ExpiresAt = %s, want %s", got.ExpiresAt, newExpiry)
	}
	// The record must outlive the watch by the full retention window.
	if want := watcher.RetentionTTL(newExpiry); got.TTLTimestamp < want {
		t.Errorf("TTLTimestamp = %d, want >= %d (newExpiry + retention)", got.TTLTimestamp, want)
	}
	// And must NOT be the watch's own expiry — the precise shape of the bug, so it
	// can't quietly come back.
	if got.TTLTimestamp == newExpiry.Unix() {
		t.Errorf("TTLTimestamp = %d == newExpiry: ttl_timestamp was set to the watch expiry, so DynamoDB will delete the record the moment it expires (#161)", got.TTLTimestamp)
	}
	// Sanity: the retention horizon is ~90 days out, not ~48 hours.
	if minExpected := time.Now().UTC().Add(60 * 24 * time.Hour).Unix(); got.TTLTimestamp < minExpected {
		t.Errorf("TTLTimestamp = %d, want at least ~90d out (>= %d)", got.TTLTimestamp, minExpected)
	}
}

// TestPutWatch_RetentionTTL guards the OTHER direction: the creation path must
// arm ttl_timestamp at expiry + retention, so a watch that is never extended
// still leaves a tombstone (#161). This is the path the reporter hit — they never
// ran `lagotto extend`.
func TestPutWatch_RetentionTTL(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	expiry := time.Now().UTC().Add(48 * time.Hour)
	w := newTestWatch("w-retain", "arn:aws:iam::123456789012:user/test")
	w.ExpiresAt = expiry
	w.TTLTimestamp = watcher.RetentionTTL(expiry) // what cmd/watch.go now does
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	got, _ := store.GetWatch(ctx, "w-retain")
	if got.TTLTimestamp != watcher.RetentionTTL(expiry) {
		t.Errorf("TTLTimestamp = %d, want %d (expiry + retention)", got.TTLTimestamp, watcher.RetentionTTL(expiry))
	}
	if got.TTLTimestamp == expiry.Unix() {
		t.Errorf("TTLTimestamp == expiry (%d): the record would be deleted at expiry (#161)", expiry.Unix())
	}
}

// TestRetentionTTL_IsNotTheWatchExpiry pins the invariant itself, independent of
// any store call: the retention horizon is strictly later than the expiry it's
// derived from, by ~90 days.
func TestRetentionTTL_IsNotTheWatchExpiry(t *testing.T) {
	expiry := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	got := watcher.RetentionTTL(expiry)
	if got == expiry.Unix() {
		t.Fatal("RetentionTTL returned the expiry unchanged")
	}
	if want := expiry.Add(90 * 24 * time.Hour).Unix(); got != want {
		t.Errorf("RetentionTTL = %d, want %d (expiry + 90d)", got, want)
	}
}

// TestUpdateWatchStatus_ExpiredTombstoneSurvives is the #161 end state: after the
// TTL transition the record is still THERE, resolvable by ID, and says
// status=expired — distinguishable from a watch ID that never existed (which
// GetWatch reports as nil). It also covers the self-heal: this watch was stored
// with the old buggy ttl_timestamp == expiry, and the status write repairs it.
func TestUpdateWatchStatus_ExpiredTombstoneSurvives(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-1 * time.Hour)
	w := newTestWatch("w-tomb", "arn:aws:iam::123456789012:user/test")
	w.CreatedAt = past.Add(-48 * time.Hour)
	w.ExpiresAt = past
	w.TTLTimestamp = past.Unix() // the pre-fix value: delete-at-expiry
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	if err := store.UpdateWatchStatus(ctx, "w-tomb", watcher.StatusExpired); err != nil {
		t.Fatalf("UpdateWatchStatus: %v", err)
	}

	got, err := store.GetWatch(ctx, "w-tomb")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got == nil {
		t.Fatal("expired watch resolved to nil — the tombstone is gone (#161)")
	}
	if got.Status != watcher.StatusExpired {
		t.Errorf("Status = %q, want expired", got.Status)
	}
	// Self-heal: the doomed ttl_timestamp has been pushed out to the retention horizon.
	if minExpected := time.Now().UTC().Add(60 * 24 * time.Hour).Unix(); got.TTLTimestamp < minExpected {
		t.Errorf("TTLTimestamp = %d, want at least ~90d out (>= %d) — the tombstone is still armed to self-delete", got.TTLTimestamp, minExpected)
	}
	// And the give-up duration is now derivable from the tombstone (#139/#161).
	d, ok := got.TimeToGiveUp()
	if !ok {
		t.Fatal("TimeToGiveUp not reported for an expired watch")
	}
	if d < 47*time.Hour {
		t.Errorf("TimeToGiveUp = %s, want ~48h", d)
	}

	// Contrast: a watch ID that never existed still reads as absent.
	missing, err := store.GetWatch(ctx, "w-never-existed")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if missing != nil {
		t.Errorf("nonexistent watch resolved to %+v, want nil", missing)
	}
}

func TestExtendWatch_Reactivate(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-reactivate", "arn:aws:iam::123456789012:user/test")
	w.Status = watcher.StatusExpired
	_ = store.PutWatch(ctx, w)

	newExpiry := time.Now().UTC().Add(24 * time.Hour)
	if err := store.ExtendWatch(ctx, "w-reactivate", newExpiry, true); err != nil {
		t.Fatalf("ExtendWatch reactivate: %v", err)
	}

	got, _ := store.GetWatch(ctx, "w-reactivate")
	if got.Status != watcher.StatusActive {
		t.Errorf("Status = %q, want active", got.Status)
	}
}

// TestClaimLease_DoublePollerGuard covers the #47 lease: one poller claims it,
// a second poller is refused (ErrLeaseHeld), the owner can re-claim/extend, and
// once released the watch is claimable by anyone.
func TestClaimLease_DoublePollerGuard(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-lease", "arn:alice")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	future := time.Now().Add(2 * time.Minute)

	// Poller A claims it.
	if err := store.ClaimLease(ctx, "w-lease", "pollerA", future); err != nil {
		t.Fatalf("pollerA ClaimLease: %v", err)
	}
	// Poller B is refused while A's lease is live.
	if err := store.ClaimLease(ctx, "w-lease", "pollerB", future); !errors.Is(err, watcher.ErrLeaseHeld) {
		t.Fatalf("pollerB ClaimLease err = %v, want ErrLeaseHeld", err)
	}
	// Poller A can re-claim (extend) its own lease.
	if err := store.ClaimLease(ctx, "w-lease", "pollerA", future); err != nil {
		t.Fatalf("pollerA re-claim: %v", err)
	}
	// A releases; now B can claim.
	if err := store.ReleaseLease(ctx, "w-lease", "pollerA"); err != nil {
		t.Fatalf("pollerA ReleaseLease: %v", err)
	}
	if err := store.ClaimLease(ctx, "w-lease", "pollerB", future); err != nil {
		t.Fatalf("pollerB ClaimLease after release: %v", err)
	}
}

// TestClaimLease_StaleLeaseReclaimable confirms an expired lease (a crashed
// poller) doesn't block the watch forever — another poller can re-claim it.
func TestClaimLease_StaleLeaseReclaimable(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-stale", "arn:alice")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	// Poller A claims with an already-expired lease (simulating a crash mid-cycle).
	past := time.Now().Add(-1 * time.Minute)
	if err := store.ClaimLease(ctx, "w-stale", "pollerA", past); err != nil {
		t.Fatalf("pollerA ClaimLease: %v", err)
	}
	// Poller B claims it because A's lease is stale.
	if err := store.ClaimLease(ctx, "w-stale", "pollerB", time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("pollerB ClaimLease on stale lease err = %v, want success", err)
	}
}

// TestReleaseLease_OnlyOwnerClears confirms a non-owner's release is a no-op
// (doesn't steal the lease) — release is scoped to the holder.
func TestReleaseLease_OnlyOwnerClears(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	w := newTestWatch("w-rel", "arn:alice")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	future := time.Now().Add(2 * time.Minute)
	if err := store.ClaimLease(ctx, "w-rel", "pollerA", future); err != nil {
		t.Fatalf("pollerA ClaimLease: %v", err)
	}
	// B tries to release A's lease — no-op, no error.
	if err := store.ReleaseLease(ctx, "w-rel", "pollerB"); err != nil {
		t.Fatalf("pollerB ReleaseLease (non-owner) should be a no-op, got %v", err)
	}
	// A's lease must still hold against a B claim.
	if err := store.ClaimLease(ctx, "w-rel", "pollerB", future); !errors.Is(err, watcher.ErrLeaseHeld) {
		t.Fatalf("after non-owner release, pollerB claim err = %v, want ErrLeaseHeld (A still holds)", err)
	}
}

// TestRecordAndClearQuotaCap round-trips the three persisted quota-cap fields
// (#153) and confirms ClearQuotaCap removes all three while leaving the unrelated
// consecutive-failure counter alone.
func TestRecordAndClearQuotaCap(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	w := newTestWatch("w-quota-store", "arn:alice")
	w.DesiredCount = 5
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	// An unrelated counter that must survive the clear.
	if _, err := store.IncrementConsecutiveFailures(ctx, w.WatchID); err != nil {
		t.Fatalf("IncrementConsecutiveFailures: %v", err)
	}

	const reason = "fleet capped at 2/5 by the EC2 G-family On-Demand vCPU quota in us-east-1"
	before := time.Now().UTC().Add(-time.Second)
	if err := store.RecordQuotaCap(ctx, w.WatchID, 2, reason); err != nil {
		t.Fatalf("RecordQuotaCap: %v", err)
	}

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.QuotaCappedCount != 2 {
		t.Errorf("QuotaCappedCount = %d, want 2", got.QuotaCappedCount)
	}
	if got.QuotaCapReason != reason {
		t.Errorf("QuotaCapReason = %q, want %q", got.QuotaCapReason, reason)
	}
	if got.QuotaCappedAt.Before(before) {
		t.Errorf("QuotaCappedAt = %v, want >= %v", got.QuotaCappedAt, before)
	}

	// Re-recording at a different level overwrites (it's a level, not a counter).
	if err := store.RecordQuotaCap(ctx, w.WatchID, 3, reason); err != nil {
		t.Fatalf("RecordQuotaCap (relevel): %v", err)
	}
	got, _ = store.GetWatch(ctx, w.WatchID)
	if got.QuotaCappedCount != 3 {
		t.Errorf("QuotaCappedCount after re-record = %d, want 3 (overwritten, not summed)", got.QuotaCappedCount)
	}

	if err := store.ClearQuotaCap(ctx, w.WatchID); err != nil {
		t.Fatalf("ClearQuotaCap: %v", err)
	}
	got, err = store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch after clear: %v", err)
	}
	if got.QuotaCappedCount != 0 || got.QuotaCapReason != "" || !got.QuotaCappedAt.IsZero() {
		t.Errorf("cap state not cleared: count=%d at=%v reason=%q",
			got.QuotaCappedCount, got.QuotaCappedAt, got.QuotaCapReason)
	}
	if got.ConsecutiveFailures != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1 (untouched by the quota-cap clear)", got.ConsecutiveFailures)
	}
}

// TestListWatchesByUser_IncludesExpired is the `list --all` half of #161: the
// unfiltered per-user query (what --all issues) returns the expired tombstone
// alongside the active watch, while the default active-only filter still hides it.
// No new flag was needed — only a record that survives expiry.
func TestListWatchesByUser_IncludesExpired(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	alice := "arn:aws:iam::123456789012:user/alice"
	active := newTestWatch("w-live", alice)
	expired := newTestWatch("w-dead", alice)
	expired.Status = watcher.StatusExpired
	for _, w := range []*watcher.Watch{active, expired} {
		if err := store.PutWatch(ctx, w); err != nil {
			t.Fatalf("PutWatch %s: %v", w.WatchID, err)
		}
	}

	all, err := store.ListWatchesByUser(ctx, alice, "") // `list --all`
	if err != nil {
		t.Fatalf("ListWatchesByUser(all): %v", err)
	}
	seen := map[string]watcher.WatchStatus{}
	for _, w := range all {
		seen[w.WatchID] = w.Status
	}
	if seen["w-dead"] != watcher.StatusExpired {
		t.Errorf("--all did not surface the expired watch: %v", seen)
	}
	if seen["w-live"] != watcher.StatusActive {
		t.Errorf("--all did not surface the active watch: %v", seen)
	}

	activeOnly, err := store.ListWatchesByUser(ctx, alice, watcher.StatusActive) // default
	if err != nil {
		t.Fatalf("ListWatchesByUser(active): %v", err)
	}
	if len(activeOnly) != 1 || activeOnly[0].WatchID != "w-live" {
		t.Errorf("default filter returned %+v, want only w-live", activeOnly)
	}
}
