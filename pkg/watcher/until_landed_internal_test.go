package watcher

import (
	"context"
	"strings"
	"testing"
	"time"
)

func untilLandedWatch(id string, expiresAt time.Time) *Watch {
	w := expiredWatch(id)
	w.UntilLanded = true
	w.RemindEverySeconds = int64(DefaultRemindEvery / time.Second)
	w.RemindPauseAfter = DefaultRemindPauseAfter
	w.ExpiresAt = expiresAt
	w.TTLTimestamp = RetentionTTL(expiresAt)
	return w
}

var someChannel = []NotifyChannel{{Type: "sns", Target: "arn:aws:sns:us-east-1:123456789012:t"}}

func TestValidateUntilLanded(t *testing.T) {
	cases := []struct {
		name     string
		action   ActionMode
		desired  int
		channels []NotifyChannel
		remind   time.Duration
		pause    int
		want     string // substring of the error; "" = accepted
	}{
		{"spawn ok", ActionSpawn, 0, someChannel, DefaultRemindEvery, 2, ""},
		{"hold ok", ActionHold, 0, someChannel, MaxRemindEvery, 2, ""},
		{"notify refused", ActionNotify, 0, someChannel, DefaultRemindEvery, 2, "not supported for --action notify"},
		{"fleet refused", ActionSpawn, 3, someChannel, DefaultRemindEvery, 2, "--maintain"},
		{"no channel", ActionSpawn, 0, nil, DefaultRemindEvery, 2, "needs a --notify channel"},
		{"remind too long", ActionSpawn, 0, someChannel, MaxRemindEvery + time.Hour, 2, "capped at 30d"},
		{"remind too short", ActionSpawn, 0, someChannel, time.Minute, 2, "at least 1h"},
		{"negative pause", ActionSpawn, 0, someChannel, DefaultRemindEvery, -1, "pause-after"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateUntilLanded(c.action, c.desired, c.channels, c.remind, c.pause)
			if c.want == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want it to contain %q", err, c.want)
			}
		})
	}
}

func TestAckWindowAndDeadline(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := untilLandedWatch("w-ul", now.Add(21*24*time.Hour))
	if got := w.AckWindow(); got != 21*24*time.Hour {
		t.Errorf("AckWindow = %s, want 21d", got)
	}
	d, ok := w.AckDeadline(now)
	if !ok || !d.Equal(now.Add(21*24*time.Hour)) {
		t.Errorf("AckDeadline = %s, %v", d, ok)
	}
	if got := w.AckWindowStart(); !got.Equal(now) {
		t.Errorf("AckWindowStart = %s, want %s", got, now)
	}

	plain := expiredWatch("w-ttl")
	if plain.AckWindow() != 0 || !plain.AckWindowStart().IsZero() {
		t.Error("a TTL watch has an acknowledgement window")
	}
	if _, ok := plain.AckDeadline(now); ok {
		t.Error("a TTL watch has an acknowledgement deadline")
	}
	// An until-landed flag without an interval (a hand-written record) has no
	// window rather than a zero-length one that would pause it at once.
	odd := untilLandedWatch("w-odd", now)
	odd.RemindEverySeconds = 0
	if _, ok := odd.AckDeadline(now); ok {
		t.Error("an until-landed watch with no interval has a deadline")
	}
	odd.RemindEverySeconds = 3600
	odd.ExpiresAt = time.Time{}
	if !odd.AckWindowStart().IsZero() {
		t.Error("AckWindowStart with no ExpiresAt is not zero")
	}
}

func TestPausedAndExpiryReason(t *testing.T) {
	w := untilLandedWatch("w-ul", time.Now())
	if got := expiryReason(w); got != ExpiredReasonUnacknowledged {
		t.Errorf("expiryReason(until-landed) = %q", got)
	}
	if got := expiryReason(expiredWatch("w-ttl")); got != "" {
		t.Errorf("expiryReason(TTL) = %q, want none", got)
	}
	if w.Paused() {
		t.Error("an active watch reads as paused")
	}
	w.Status, w.ExpiredReason = StatusExpired, ExpiredReasonUnacknowledged
	if !w.Paused() {
		t.Error("an unacknowledged expired watch does not read as paused")
	}
}

func TestFormatSpan(t *testing.T) {
	for d, want := range map[time.Duration]string{
		21 * 24 * time.Hour: "21d",
		36 * time.Hour:      "36h",
		90 * time.Minute:    "1h30m0s",
		0:                   "0s",
	} {
		if got := FormatSpan(d); got != want {
			t.Errorf("FormatSpan(%s) = %q, want %q", d, got, want)
		}
	}
}

// TestPollAll_UntilLandedPausesUnacknowledged: an until-landed watch past its
// deadline is expired WITH the reason, the notifier sees the reason, and a TTL
// watch expiring in the same sweep gets none.
func TestPollAll_UntilLandedPausesUnacknowledged(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Minute)
	if err := store.PutWatch(ctx, untilLandedWatch("w-ul", past)); err != nil {
		t.Fatal(err)
	}
	if err := store.PutWatch(ctx, expiredWatch("w-ttl")); err != nil {
		t.Fatal(err)
	}

	reasons := map[string]string{}
	p := NewPoller(nil, store, true)
	p.notifyExpired = func(_ context.Context, w *Watch) error {
		reasons[w.WatchID] = w.ExpiredReason
		return nil
	}
	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Expired != 2 {
		t.Errorf("Expired = %d, want 2", summary.Expired)
	}
	if reasons["w-ul"] != ExpiredReasonUnacknowledged || reasons["w-ttl"] != "" {
		t.Errorf("notified reasons = %v", reasons)
	}

	got, _ := store.GetWatch(ctx, "w-ul")
	if got == nil || !got.Paused() {
		t.Fatalf("until-landed watch not paused: %+v", got)
	}
	// The record stays bounded: a finite retention horizon past the pause.
	if got.TTLTimestamp <= 0 || got.TTLTimestamp > RetentionTTL(time.Now().UTC().Add(time.Minute)) {
		t.Errorf("TTLTimestamp = %d, want a finite retention horizon from now", got.TTLTimestamp)
	}
	ttl, _ := store.GetWatch(ctx, "w-ttl")
	if ttl == nil || ttl.Status != StatusExpired || ttl.ExpiredReason != "" {
		t.Errorf("TTL watch = %+v, want expired with no reason", ttl)
	}

	// Resuming (what ack/extend does) reactivates the watch and clears the reason.
	next := time.Now().UTC().Add(21 * 24 * time.Hour)
	if err := store.ExtendWatch(ctx, "w-ul", next, true); err != nil {
		t.Fatalf("ExtendWatch: %v", err)
	}
	got, _ = store.GetWatch(ctx, "w-ul")
	if got.Status != StatusActive || got.ExpiredReason != "" || got.Paused() {
		t.Errorf("resumed watch = status %q reason %q", got.Status, got.ExpiredReason)
	}
	if !got.UntilLanded || got.RemindEverySeconds == 0 || got.RemindPauseAfter != DefaultRemindPauseAfter {
		t.Errorf("resume lost the until-landed settings: %+v", got)
	}
	if got.TTLTimestamp != RetentionTTL(next) {
		t.Errorf("TTLTimestamp = %d, want RetentionTTL(new deadline) %d", got.TTLTimestamp, RetentionTTL(next))
	}
}

// TestPollAll_UntilLandedBeforeDeadlineKeepsHunting: before the deadline an
// until-landed watch is polled like any other, not expired.
func TestPollAll_UntilLandedBeforeDeadlineKeepsHunting(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()
	w := untilLandedWatch("w-ul-live", time.Now().UTC().Add(time.Hour))
	// A SageMaker watch with no job takes the per-watch path, with no truffle
	// search, so the sweep needs no capacity client.
	w.Service = ServiceSageMaker
	w.InstanceTypePattern = "ml.g5.xlarge"
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	p := NewPoller(nil, store, false)
	p.notifyExpired = func(context.Context, *Watch) error {
		t.Error("an until-landed watch before its deadline was expired")
		return nil
	}
	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Expired != 0 || summary.Watched != 1 {
		t.Errorf("summary = %+v, want 1 watched, 0 expired", summary)
	}
}

func TestStoreExpireWatchNoReasonIsUpdateWatchStatus(t *testing.T) {
	store := expiryTestStore(t)
	ctx := context.Background()
	w := expiredWatch("w-plain")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := store.ExpireWatch(ctx, "w-plain", ""); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetWatch(ctx, "w-plain")
	if got.Status != StatusExpired || got.ExpiredReason != "" {
		t.Errorf("got status %q reason %q", got.Status, got.ExpiredReason)
	}
}

func TestPausedNotification(t *testing.T) {
	w := untilLandedWatch("w-ul", time.Now())
	w.Status, w.ExpiredReason, w.UpdatedAt = StatusExpired, ExpiredReasonUnacknowledged, time.Now().UTC()
	if s := expiredSubject(w); !strings.Contains(s, "paused") || !strings.Contains(s, "w-ul") {
		t.Errorf("subject = %q", s)
	}
	body := expiredBody(w)
	for _, want := range []string{"paused", "lagotto ack w-ul", "lagotto cancel w-ul", "Nothing was launched"} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "--ttl") {
		t.Errorf("paused body tells an until-landed watch to use --ttl:\n%s", body)
	}
	p := expiredPayload(w)
	if p["expired_reason"] != ExpiredReasonUnacknowledged || p["until_landed"] != true || p["event"] != "expired" {
		t.Errorf("payload = %v", p)
	}
	// A TTL expiry's payload keeps its old shape.
	plain := expiredPayload(expiredWatch("w-ttl"))
	if _, ok := plain["expired_reason"]; ok {
		t.Errorf("TTL payload gained expired_reason: %v", plain)
	}
	if _, ok := plain["until_landed"]; ok {
		t.Errorf("TTL payload gained until_landed: %v", plain)
	}
}
