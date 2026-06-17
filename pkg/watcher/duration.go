package watcher

import (
	"fmt"
	"time"
)

// ParseDuration parses a short duration string of the form "<int><unit>" where
// unit is d (days), h (hours), or m (minutes) — e.g. "7d", "24h", "30m". It is
// the package-level home for the parser the CLI and the spawner both need (so
// neither has to import the other); the cmd layer falls back to this after
// time.ParseDuration (which doesn't understand "d"). Lifted from cmd/watch.go
// so pkg/watcher can default/validate launch TTLs without a cmd import cycle.
func ParseDuration(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}
	unit := s[len(s)-1]
	val := s[:len(s)-1]
	var n int
	if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
		return 0, fmt.Errorf("invalid duration number: %s", val)
	}
	switch unit {
	case 'd':
		return time.Duration(n) * 24 * time.Hour, nil
	case 'h':
		return time.Duration(n) * time.Hour, nil
	case 'm':
		return time.Duration(n) * time.Minute, nil
	default:
		return 0, fmt.Errorf("unknown duration unit: %c", unit)
	}
}
