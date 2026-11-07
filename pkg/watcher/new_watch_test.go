package watcher_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestNewWatch_TTL: the constructor sets the timestamps together, and the
// record's DynamoDB TTL is the RETENTION horizon past the expiry, never the
// expiry itself (#41, #161, spore-host-mcp#77).
func TestNewWatch_TTL(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.FixedZone("PST", -8*3600))
	w, err := watcher.NewWatch(now, watcher.Lifetime{TTL: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^w-[0-9a-f]{8}$`).MatchString(w.WatchID) {
		t.Errorf("WatchID = %q, want w- and 8 hex digits", w.WatchID)
	}
	if w.Status != watcher.StatusActive {
		t.Errorf("Status = %s, want active", w.Status)
	}
	if !w.CreatedAt.Equal(now) || !w.UpdatedAt.Equal(now) || w.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt/UpdatedAt = %v / %v, want %v in UTC", w.CreatedAt, w.UpdatedAt, now)
	}
	if want := now.Add(24 * time.Hour); !w.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", w.ExpiresAt, want)
	}
	if w.TTLTimestamp != watcher.RetentionTTL(w.ExpiresAt) {
		t.Errorf("TTLTimestamp = %d, want RetentionTTL(ExpiresAt) = %d", w.TTLTimestamp, watcher.RetentionTTL(w.ExpiresAt))
	}
	if w.TTLTimestamp <= w.ExpiresAt.Unix() {
		t.Errorf("TTLTimestamp %d is not after ExpiresAt %d: DynamoDB would delete the expired tombstone", w.TTLTimestamp, w.ExpiresAt.Unix())
	}
	if w.UntilLanded || w.RemindEverySeconds != 0 || w.RemindPauseAfter != 0 {
		t.Errorf("a TTL watch has until-landed fields: %+v", w)
	}

	w2, err := watcher.NewWatch(now, watcher.Lifetime{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if w2.WatchID == w.WatchID {
		t.Errorf("two watches got the same ID %s", w.WatchID)
	}
}

// TestNewWatch_UntilLanded: the expiry is the first acknowledgement deadline,
// (pause-after+1) × remind from creation, and the retention TTL follows it.
func TestNewWatch_UntilLanded(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w, err := watcher.NewWatch(now, watcher.Lifetime{
		UntilLanded: true, RemindEvery: watcher.DefaultRemindEvery, RemindPauseAfter: watcher.DefaultRemindPauseAfter,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !w.UntilLanded || w.RemindEverySeconds != int64(watcher.DefaultRemindEvery/time.Second) || w.RemindPauseAfter != watcher.DefaultRemindPauseAfter {
		t.Errorf("until-landed fields = %v / %d / %d", w.UntilLanded, w.RemindEverySeconds, w.RemindPauseAfter)
	}
	want := now.Add(time.Duration(watcher.DefaultRemindPauseAfter+1) * watcher.DefaultRemindEvery)
	if !w.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v (21 days)", w.ExpiresAt, want)
	}
	if w.TTLTimestamp != watcher.RetentionTTL(w.ExpiresAt) {
		t.Errorf("TTLTimestamp = %d, want RetentionTTL(ExpiresAt)", w.TTLTimestamp)
	}
	if !w.AckWindowStart().Equal(now) {
		t.Errorf("AckWindowStart = %v, want creation %v", w.AckWindowStart(), now)
	}
}

func TestNewWatch_RefusesABadLifetime(t *testing.T) {
	for name, life := range map[string]watcher.Lifetime{
		"zero TTL":              {},
		"negative TTL":          {TTL: -time.Hour},
		"until-landed with TTL": {UntilLanded: true, TTL: time.Hour, RemindEvery: watcher.DefaultRemindEvery},
		"remind too short":      {UntilLanded: true, RemindEvery: time.Second},
		"remind too long":       {UntilLanded: true, RemindEvery: watcher.MaxRemindEvery + time.Hour},
		"negative pause-after":  {UntilLanded: true, RemindEvery: watcher.DefaultRemindEvery, RemindPauseAfter: -1},
	} {
		if w, err := watcher.NewWatch(time.Now(), life); err == nil {
			t.Errorf("%s: accepted, got %+v", name, w)
		}
	}
}
