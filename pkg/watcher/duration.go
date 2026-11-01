package watcher

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// ParseDuration parses lagotto's duration form: Go's time.ParseDuration syntax
// extended with d (24h) and w (7d), e.g. "1w", "7d", "24h", "30m", "45s",
// "1.5d", "1d12h". The grammar is one or more <decimal><unit> segments with
// unit one of ns, us, µs, ms, s, m, h, d, w. The WHOLE string must match:
// signs, spaces, a bare number, an unknown unit or any trailing input are an
// error naming the input (#201).
//
// Fractions are supported because time.ParseDuration supports them ("1.5h" is
// 90m), and every --ttl/--after caller tries time.ParseDuration first, so
// "1.5h" was always accepted; "1.5d" meaning 36h is the same rule, and it is
// what spawn's own TTL parser (spawn pkg/ttl) and spored compute for a "1.5d"
// spawn-config ttl. Before #201 this read the number with fmt.Sscanf("%d"),
// which stops at the first non-digit without an error, so "1.5d" became 24h,
// "2d12h" 2h and "5abcd" 5d, silently.
//
// It is the package-level home for the parser the CLI and the spawner both
// need (so neither has to import the other).
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("invalid duration %q: %s", s, durationForms)
	}
	var total time.Duration
	rest := s
	for rest != "" {
		i := 0
		for i < len(rest) && (rest[i] >= '0' && rest[i] <= '9' || rest[i] == '.') {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("invalid duration %q: expected a number at %q; %s", s, rest, durationForms)
		}
		num := rest[:i]
		rest = rest[i:]
		unit := ""
		for _, u := range durationUnitsLongestFirst {
			if strings.HasPrefix(rest, u) {
				unit = u
				break
			}
		}
		if unit == "" {
			if rest == "" {
				return 0, fmt.Errorf("invalid duration %q: %q has no unit; %s", s, num, durationForms)
			}
			return 0, fmt.Errorf("invalid duration %q: unknown unit at %q; %s", s, rest, durationForms)
		}
		rest = rest[len(unit):]

		var seg time.Duration
		switch unit {
		case "d", "w":
			// Parse as hours, then scale: time.ParseDuration does the exact
			// decimal arithmetic (fractions included) and the overflow check.
			h, err := time.ParseDuration(num + "h")
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q: %w", s, err)
			}
			scale := time.Duration(24)
			if unit == "w" {
				scale = 7 * 24
			}
			if h > math.MaxInt64/scale {
				return 0, fmt.Errorf("invalid duration %q: too large", s)
			}
			seg = h * scale
		default:
			d, err := time.ParseDuration(num + unit)
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q: %w", s, err)
			}
			seg = d
		}
		if seg > math.MaxInt64-total {
			return 0, fmt.Errorf("invalid duration %q: too large", s)
		}
		total += seg
	}
	return total, nil
}

// durationForms is the hint every ParseDuration error ends with.
const durationForms = "expected <number><unit>[<number><unit>…] with unit w, d, h, m or s (e.g. 1w, 7d, 1.5d, 24h, 1h30m)"

// durationUnitsLongestFirst lists every unit ParseDuration accepts, longest
// first so "ms" matches before "m". "µs" is U+00B5 and "μs" U+03BC, as in
// time.ParseDuration.
var durationUnitsLongestFirst = []string{"µs", "μs", "ns", "us", "ms", "s", "m", "h", "d", "w"}

// parseDurationLegacy is the pre-#201 parser, kept ONLY to re-read a value that
// was stored before ParseDuration became strict (see storedLaunchTTL). It reads
// a leading integer and the last byte as the unit, ignoring anything between
// them: "1.5d" -> 24h, "5abcd" -> 5d. Never use it for new input.
func parseDurationLegacy(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	var n int
	if _, err := fmt.Sscanf(s[:len(s)-1], "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	switch s[len(s)-1] {
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
	}
	return 0, fmt.Errorf("invalid duration %q", s)
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

// TimeToGiveUp is the DERIVED time from a watch's creation until it gave up
// without acquiring — how long it kept trying (#139). Two terminal statuses
// qualify: StatusFailed (a terminal launch error stopped it) and StatusExpired
// (its TTL elapsed with no match, #161) — "we hunted g7e for 48h and never got
// it" is the same measurement as "we tried for 20m and the AMI was bad", and both
// are the half of the wait distribution that a successful WaitToAcquire can't
// show. The watch's last update (UpdatedAt) is the end time: RecordMatch stamps it
// on the transition to failed, UpdateWatchStatus on the transition to expired.
// ok is false for any other status.
func (w *Watch) TimeToGiveUp() (time.Duration, bool) {
	if w.Status != StatusFailed && w.Status != StatusExpired {
		return 0, false
	}
	if w.CreatedAt.IsZero() || w.UpdatedAt.IsZero() {
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
