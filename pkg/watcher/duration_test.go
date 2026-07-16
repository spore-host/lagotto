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
