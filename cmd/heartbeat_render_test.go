package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// #239 phase 1: the heartbeat in `list` and `status`, rendered against a fixed
// clock so the ages are exact.

var hbNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func hbWatches() []watcher.Watch {
	return []watcher.Watch{
		{ // healthy hunt, polled 3 minutes ago, last attempt short of capacity
			WatchID: "w-a660980c", Status: watcher.StatusActive, UserID: "arn:aws:iam::123456789012:user/alice",
			InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-west-2", "us-east-1"}, Action: watcher.ActionSpawn,
			CreatedAt: hbNow.Add(-72 * time.Hour), ExpiresAt: hbNow.Add(30 * 24 * time.Hour), LastPolledAt: hbNow.Add(-3 * time.Minute),
			Attempts: 812, LastAttemptAt: hbNow.Add(-3 * time.Minute),
			LastError:   "capacity: InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-west-2a).",
			LastErrorAt: hbNow.Add(-3 * time.Minute),
		},
		{ // stale: the search has failed for 2 hours, so LAST POLL is flagged
			WatchID: "w-4a1e61d6", Status: watcher.StatusActive, Project: "fieldwork", UserID: "arn:aws:iam::123456789012:user/alice",
			InstanceTypePattern: "g6e.*", Regions: []string{"us-east-1"}, Action: watcher.ActionNotify,
			CreatedAt: hbNow.Add(-5 * 24 * time.Hour), ExpiresAt: hbNow.Add(24 * time.Hour), LastPolledAt: hbNow.Add(-2 * time.Hour),
			LastError: "capacity: InsufficientInstanceCapacity", LastErrorAt: hbNow.Add(-3 * time.Hour),
			LastPollError: "search: operation error EC2: DescribeInstanceTypeOfferings, RequestLimitExceeded", LastPollErrorAt: hbNow.Add(-1 * time.Minute),
		},
		{ // a pre-#239 record: no heartbeat at all, never polled, created long ago
			WatchID: "w-0ld00000", Status: watcher.StatusActive, UserID: "arn:aws:iam::123456789012:user/alice",
			InstanceTypePattern: "c7g.large", Regions: []string{"us-west-2"}, Action: watcher.ActionNotify,
			CreatedAt: hbNow.Add(-1 * time.Hour), ExpiresAt: hbNow.Add(time.Hour),
		},
		{ // finished: polled long ago, but it isn't polled any more, so no flag
			WatchID: "w-done0000", Status: watcher.StatusMatched, UserID: "arn:aws:iam::123456789012:user/alice",
			InstanceTypePattern: "g7.24xlarge", Regions: []string{"eu-north-1", "us-west-2"}, Action: watcher.ActionSpawn,
			CreatedAt: hbNow.Add(-10 * 24 * time.Hour), ExpiresAt: hbNow.Add(-24 * time.Hour), LastPolledAt: hbNow.Add(-9 * 24 * time.Hour),
			Attempts: 2, LastAttemptAt: hbNow.Add(-9 * 24 * time.Hour),
			LastError: "quota: VcpuLimitExceeded: g7.24xlarge in eu-north-1; needs 96 vCPU", LastErrorAt: hbNow.Add(-9 * 24 * time.Hour),
		},
	}
}

func TestListTable_Heartbeat(t *testing.T) {
	var out bytes.Buffer
	writeWatchTable(&out, hbWatches(), hbNow, false)
	want := `WATCH ID     STATUS     PROJECT         OWNER                PATTERN              REGIONS                   SPOT   ACTION     EXPIRES              LAST POLL TRIES LAST ERR
w-a660980c   active     -               user/alice           p5.48xlarge          [us-west-2 us-east-1]     false  spawn      2026-12-06T12:00:00Z 3m          812 capacity
w-4a1e61d6   active     fieldwork       user/alice           g6e.*                [us-east-1]               false  notify     2026-11-07T12:00:00Z 2h!           0 search
w-0ld00000   active     -               user/alice           c7g.large            [us-west-2]               false  notify     2026-11-06T13:00:00Z never!        0 -
w-done0000   matched    -               user/alice           g7.24xlarge          [eu-north-1 us-west-2]    false  spawn      2026-11-05T12:00:00Z 9d            2 quota
`
	if got := out.String(); got != want {
		t.Errorf("list table:\n--- got\n%s--- want\n%s", got, want)
	}
	// Width stays reasonable: the heartbeat adds 25 columns to the old 147-wide row.
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if n := len([]rune(line)); n > 172 {
			t.Errorf("row is %d wide: %q", n, line)
		}
	}
}

func TestListTable_Wide(t *testing.T) {
	var out bytes.Buffer
	writeWatchTable(&out, hbWatches()[:3], hbNow, true)
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if !strings.HasSuffix(lines[0], "LAST ERR LAST ERROR") {
		t.Errorf("wide header = %q", lines[0])
	}
	for i, want := range []string{
		"812 capacity capacity: InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-west-2a). (3m ago)",
		// The newer of the two errors wins: the search error, a minute ago.
		"0 search   search: operation error EC2: DescribeInstanceTypeOfferings, RequestLimitExceeded (1m ago)",
		"never!        0 -",
	} {
		if !strings.HasSuffix(lines[i+1], want) {
			t.Errorf("wide row %d = %q, want suffix %q", i+1, lines[i+1], want)
		}
	}
}

func TestStatusHeartbeat(t *testing.T) {
	ws := hbWatches()
	cases := []struct {
		name string
		w    watcher.Watch
		want string
	}{
		{"healthy", ws[0], `Last polled: 2026-11-06T11:57:00Z (3m ago)
Attempts: 812 (last 2026-11-06T11:57:00Z, 3m ago)
Last error: capacity: InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity in the Availability Zone you requested (us-west-2a).
  at 2026-11-06T11:57:00Z (3m ago)
`},
		{"stale, search failing", ws[1], `Last polled: 2026-11-06T10:00:00Z (2h ago; stale: the poller may be disabled, or its capacity search failing)
Last error: capacity: InsufficientInstanceCapacity
  at 2026-11-06T09:00:00Z (3h ago)
Last search error: search: operation error EC2: DescribeInstanceTypeOfferings, RequestLimitExceeded
  at 2026-11-06T11:59:00Z (1m ago)
`},
		{"pre-#239 record", ws[2], ``},
		{"finished, not flagged", ws[3], `Last polled: 2026-10-28T12:00:00Z (9d ago)
Attempts: 2 (last 2026-10-28T12:00:00Z, 9d ago)
Last error: quota: VcpuLimitExceeded: g7.24xlarge in eu-north-1; needs 96 vCPU
  at 2026-10-28T12:00:00Z (9d ago)
`},
	}
	for _, c := range cases {
		var out bytes.Buffer
		writeHeartbeat(&out, &c.w, hbNow)
		if got := out.String(); got != c.want {
			t.Errorf("%s:\n--- got\n%s--- want\n%s", c.name, got, c.want)
		}
	}

	// A search error the poller has since recovered from says so.
	w := ws[1]
	w.LastPolledAt = hbNow
	w.LastPollErrorAt = hbNow.Add(-10 * time.Minute)
	var out bytes.Buffer
	writeHeartbeat(&out, &w, hbNow)
	if !strings.Contains(out.String(), "(10m ago; the search has succeeded since)") {
		t.Errorf("recovered search error:\n%s", out.String())
	}
}

func TestFormatAgeAndLastPoll(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Second: "0s", 0: "0s", 45 * time.Second: "45s", 15 * time.Minute: "15m",
		47 * time.Hour: "47h", 48 * time.Hour: "2d", 400 * 24 * time.Hour: "400d",
	} {
		if got := formatAge(d); got != want {
			t.Errorf("formatAge(%v) = %q, want %q", d, got, want)
		}
	}
	// Exactly at the threshold is not stale; just past it is.
	w := &watcher.Watch{Status: watcher.StatusActive, LastPolledAt: hbNow.Add(-stalePollAfter)}
	if got := lastPollCol(w, hbNow); got != "15m" {
		t.Errorf("at threshold = %q", got)
	}
	w.LastPolledAt = hbNow.Add(-stalePollAfter - time.Minute)
	if got := lastPollCol(w, hbNow); got != "16m!" {
		t.Errorf("past threshold = %q", got)
	}
	// Never polled, but only just created: not flagged yet.
	w = &watcher.Watch{Status: watcher.StatusActive, CreatedAt: hbNow.Add(-time.Minute)}
	if got := lastPollCol(w, hbNow); got != "never" {
		t.Errorf("new watch = %q", got)
	}
}
