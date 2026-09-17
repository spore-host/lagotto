package watcher

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	ok := map[string]time.Duration{
		"1w":  7 * 24 * time.Hour,
		"2w":  14 * 24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"24h": 24 * time.Hour,
		"30m": 30 * time.Minute,
		"45s": 45 * time.Second,
	}
	for in, want := range ok {
		got, err := ParseDuration(in)
		if err != nil {
			t.Errorf("ParseDuration(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseDuration(%q) = %v, want %v", in, got, want)
		}
	}

	// Note: ParseDuration takes integer counts only; fractional values like
	// "1.5h" are truncated by Sscanf (pre-existing behavior), not errored.
	bad := []string{"", "h", "10", "10x", "abc"}
	for _, in := range bad {
		if _, err := ParseDuration(in); err == nil {
			t.Errorf("ParseDuration(%q) expected error, got nil", in)
		}
	}
}

func TestWaitToAcquire(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)

	// Matched watch: wait = matched_at − created_at.
	w := &Watch{
		Status:    StatusMatched,
		CreatedAt: created,
		LastMatch: &MatchResult{MatchedAt: created.Add(4*time.Minute + 12*time.Second)},
	}
	if d, ok := w.WaitToAcquire(); !ok || d != 4*time.Minute+12*time.Second {
		t.Errorf("WaitToAcquire() = %v, %v; want 4m12s, true", d, ok)
	}
	if got := FormatWait(4*time.Minute + 12*time.Second); got != "4m12s" {
		t.Errorf("FormatWait = %q, want 4m12s", got)
	}

	// No match → not available.
	if _, ok := (&Watch{Status: StatusActive, CreatedAt: created}).WaitToAcquire(); ok {
		t.Error("WaitToAcquire() ok=true for an unmatched watch, want false")
	}

	// Negative delta (clock skew / bad data) is treated as absent, not shown.
	skew := &Watch{Status: StatusMatched, CreatedAt: created, LastMatch: &MatchResult{MatchedAt: created.Add(-time.Minute)}}
	if _, ok := skew.WaitToAcquire(); ok {
		t.Error("WaitToAcquire() ok=true for a negative delta, want false")
	}
}

func TestTimeToGiveUp(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)

	// Failed watch: give-up = ended_at (UpdatedAt) − created_at.
	failed := &Watch{Status: StatusFailed, CreatedAt: created, UpdatedAt: created.Add(90 * time.Second)}
	if d, ok := failed.TimeToGiveUp(); !ok || d != 90*time.Second {
		t.Errorf("TimeToGiveUp() = %v, %v; want 1m30s, true", d, ok)
	}

	// A non-failed watch never yields a give-up time.
	if _, ok := (&Watch{Status: StatusMatched, CreatedAt: created, UpdatedAt: created.Add(time.Minute)}).TimeToGiveUp(); ok {
		t.Error("TimeToGiveUp() ok=true for a matched watch, want false")
	}
}

func TestComputeDurations(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)

	matched := &Watch{Status: StatusMatched, CreatedAt: created, LastMatch: &MatchResult{MatchedAt: created.Add(2 * time.Minute)}}
	matched.ComputeDurations()
	if matched.WaitToAcquireSeconds == nil || *matched.WaitToAcquireSeconds != 120 {
		t.Errorf("WaitToAcquireSeconds = %v, want 120", matched.WaitToAcquireSeconds)
	}
	if matched.TimeToGiveUpSeconds != nil {
		t.Errorf("TimeToGiveUpSeconds = %v, want nil for a matched watch", *matched.TimeToGiveUpSeconds)
	}

	failed := &Watch{Status: StatusFailed, CreatedAt: created, UpdatedAt: created.Add(30 * time.Second)}
	failed.ComputeDurations()
	if failed.TimeToGiveUpSeconds == nil || *failed.TimeToGiveUpSeconds != 30 {
		t.Errorf("TimeToGiveUpSeconds = %v, want 30", failed.TimeToGiveUpSeconds)
	}
	if failed.WaitToAcquireSeconds != nil {
		t.Errorf("WaitToAcquireSeconds = %v, want nil for a failed (never-matched) watch", *failed.WaitToAcquireSeconds)
	}
}
