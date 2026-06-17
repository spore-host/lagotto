package watcher

import (
	"fmt"
	"strings"
	"time"
)

// ScheduleExpression resolves the --at / --after / --cron inputs into an
// EventBridge Scheduler expression and reports whether it's a one-shot (#49).
// Exactly one of at/after/cron must be non-empty.
//   - at "2026-07-01T08:00:00Z" → "at(2026-07-01T08:00:00)" (one-shot)
//   - after "6h"                → "at(<now+6h>)"            (one-shot)
//   - cron "0 9 ? * MON-FRI *"  → "cron(0 9 ? * MON-FRI *)" (recurring)
//
// now is passed in so callers (and tests) control the clock. EventBridge
// Scheduler at() expressions are in UTC with no zone suffix, so we format to
// "2006-01-02T15:04:05".
func ScheduleExpression(at, after, cron string, now time.Time) (expr string, oneShot bool, err error) {
	set := 0
	if at != "" {
		set++
	}
	if after != "" {
		set++
	}
	if cron != "" {
		set++
	}
	if set == 0 {
		return "", false, fmt.Errorf("one of --at, --after, or --cron is required")
	}
	if set > 1 {
		return "", false, fmt.Errorf("--at, --after, and --cron are mutually exclusive")
	}

	switch {
	case cron != "":
		c := strings.TrimSpace(cron)
		// Accept a bare cron expression or one already wrapped in cron(...).
		c = strings.TrimSuffix(strings.TrimPrefix(c, "cron("), ")")
		return fmt.Sprintf("cron(%s)", c), false, nil

	case after != "":
		d, perr := parseTTL(after) // reuse the 7d/24h/30m + Go-duration parser
		if perr != nil {
			return "", false, fmt.Errorf("invalid --after %q: %w", after, perr)
		}
		if d <= 0 {
			return "", false, fmt.Errorf("invalid --after %q: must be greater than zero", after)
		}
		return atExpr(now.UTC().Add(d)), true, nil

	default: // at != ""
		t, perr := time.Parse(time.RFC3339, at)
		if perr != nil {
			return "", false, fmt.Errorf("invalid --at %q (want RFC3339, e.g. 2026-07-01T08:00:00Z): %w", at, perr)
		}
		if !t.After(now) {
			return "", false, fmt.Errorf("--at %q is not in the future", at)
		}
		return atExpr(t.UTC()), true, nil
	}
}

// atExpr formats a time as an EventBridge Scheduler at() expression (UTC, no zone).
func atExpr(t time.Time) string {
	return fmt.Sprintf("at(%s)", t.UTC().Format("2006-01-02T15:04:05"))
}
