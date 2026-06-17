package watcher

import (
	"testing"
	"time"
)

func TestScheduleExpression(t *testing.T) {
	now := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	t.Run("at one-shot", func(t *testing.T) {
		expr, oneShot, err := ScheduleExpression("2026-07-01T08:00:00Z", "", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if expr != "at(2026-07-01T08:00:00)" || !oneShot {
			t.Errorf("got %q oneShot=%v", expr, oneShot)
		}
	})

	t.Run("after one-shot = now+delay", func(t *testing.T) {
		expr, oneShot, err := ScheduleExpression("", "6h", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if expr != "at(2026-07-01T06:00:00)" || !oneShot {
			t.Errorf("got %q oneShot=%v", expr, oneShot)
		}
	})

	t.Run("after accepts day form", func(t *testing.T) {
		expr, _, err := ScheduleExpression("", "2d", "", now)
		if err != nil {
			t.Fatal(err)
		}
		if expr != "at(2026-07-03T00:00:00)" {
			t.Errorf("got %q", expr)
		}
	})

	t.Run("cron recurring", func(t *testing.T) {
		expr, oneShot, err := ScheduleExpression("", "", "0 9 ? * MON-FRI *", now)
		if err != nil {
			t.Fatal(err)
		}
		if expr != "cron(0 9 ? * MON-FRI *)" || oneShot {
			t.Errorf("got %q oneShot=%v", expr, oneShot)
		}
	})

	t.Run("cron already wrapped is not double-wrapped", func(t *testing.T) {
		expr, _, err := ScheduleExpression("", "", "cron(5 0 * * ? *)", now)
		if err != nil {
			t.Fatal(err)
		}
		if expr != "cron(5 0 * * ? *)" {
			t.Errorf("got %q", expr)
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := []struct{ at, after, cron string }{
			{"", "", ""},                       // none
			{"2026-07-01T08:00:00Z", "6h", ""}, // two set
			{"", "6h", "0 9 ? * * *"},          // two set
			{"not-a-time", "", ""},             // bad RFC3339
			{"2025-01-01T00:00:00Z", "", ""},   // in the past
			{"", "-5h", ""},                    // negative delay
			{"", "garbage", ""},                // unparseable delay
		}
		for _, c := range cases {
			if _, _, err := ScheduleExpression(c.at, c.after, c.cron, now); err == nil {
				t.Errorf("expected error for at=%q after=%q cron=%q", c.at, c.after, c.cron)
			}
		}
	})
}
