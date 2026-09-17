package watcher

import (
	"fmt"
	"time"
)

// ParseDuration parses a short duration string of the form "<int><unit>" where
// unit is w (weeks), d (days), h (hours), m (minutes), or s (seconds) — e.g.
// "1w", "7d", "24h", "30m", "45s". It is the package-level home for the parser
// the CLI and the spawner both need (so neither has to import the other); the
// cmd layer falls back to this after time.ParseDuration (which doesn't
// understand "d"/"w"). Lifted from cmd/watch.go so pkg/watcher can
// default/validate launch TTLs without a cmd import cycle.
func ParseDuration(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration %q: expected <number><unit> where unit is one of w/d/h/m/s (e.g. 1w, 7d, 24h)", s)
	}
	unit := s[len(s)-1]
	val := s[:len(s)-1]
	var n int
	if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid duration %q: %q is not a number", s, val)
	}
	switch unit {
	case 'w':
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'm':
		return time.Duration(n) * time.Minute, nil
	case 's':
		return time.Duration(n) * time.Second, nil
	default:
		return 0, fmt.Errorf("invalid duration %q: unknown unit %q (use w/d/h/m/s)", s, string(unit))
	}
}

// FormatWait renders a derived wait as a short human string (e.g. "4m12s"),
// rounded to whole seconds for table output; raw seconds go to JSON. (#139)
func FormatWait(d time.Duration) string {
	return d.Round(time.Second).String()
}

// WaitToAcquire is the DERIVED time from a watch's creation until it matched —
// how long the user waited for capacity (#139). It's meaningful for a watch that
// reached a match (matched/spawned), i.e. one carrying a LastMatch. ok is false
// when the watch never matched or the timestamps are missing/inconsistent (a
// negative delta, which shouldn't happen, is treated as absent rather than shown).
func (w *Watch) WaitToAcquire() (time.Duration, bool) {
	if w.LastMatch == nil || w.LastMatch.MatchedAt.IsZero() || w.CreatedAt.IsZero() {
		return 0, false
	}
	d := w.LastMatch.MatchedAt.Sub(w.CreatedAt)
	if d < 0 {
		return 0, false
	}
	return d, true
}

// TimeToGiveUp is the DERIVED time from a watch's creation until it ended as
// StatusFailed — how long it kept trying before giving up (#139). The watch's
// last update (UpdatedAt) is the end time: RecordMatch stamps it on the terminal
// transition to failed. ok is false for any non-failed watch.
func (w *Watch) TimeToGiveUp() (time.Duration, bool) {
	if w.Status != StatusFailed || w.CreatedAt.IsZero() || w.UpdatedAt.IsZero() {
		return 0, false
	}
	d := w.UpdatedAt.Sub(w.CreatedAt)
	if d < 0 {
		return 0, false
	}
	return d, true
}

// ComputeDurations fills the display-only WaitToAcquireSeconds /
// TimeToGiveUpSeconds fields from the derived durations, so JSON output and the
// table share one computation. Idempotent; safe to call before any render (#139).
func (w *Watch) ComputeDurations() {
	if d, ok := w.WaitToAcquire(); ok {
		s := d.Seconds()
		w.WaitToAcquireSeconds = &s
	}
	if d, ok := w.TimeToGiveUp(); ok {
		s := d.Seconds()
		w.TimeToGiveUpSeconds = &s
	}
}
