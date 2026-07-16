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
