package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

func expiredTestWatch(id string, waited time.Duration) watcher.Watch {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	return watcher.Watch{
		WatchID:             id,
		UserID:              "arn:aws:iam::123456789012:user/alice",
		Status:              watcher.StatusExpired,
		InstanceTypePattern: "g7e.*",
		Regions:             []string{"us-east-1", "us-west-2"},
		Action:              watcher.ActionSpawn,
		CreatedAt:           created,
		UpdatedAt:           created.Add(waited),
	}
}

// TestGaveUpWithoutAcquiring pins which watches count as a give-up (#161): the
// terminal never-acquired ones. A watch that matched is NOT a give-up — its
// outcome is already a row in the match history.
func TestGaveUpWithoutAcquiring(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	ended := created.Add(48 * time.Hour)

	cases := []struct {
		name string
		w    *watcher.Watch
		want bool
	}{
		{"expired, never matched", &watcher.Watch{Status: watcher.StatusExpired, CreatedAt: created, UpdatedAt: ended}, true},
		{"failed, never matched", &watcher.Watch{Status: watcher.StatusFailed, CreatedAt: created, UpdatedAt: ended}, true},
		{"expired but did match", &watcher.Watch{Status: watcher.StatusExpired, MatchCount: 1, CreatedAt: created, UpdatedAt: ended}, false},
		{"still active", &watcher.Watch{Status: watcher.StatusActive, CreatedAt: created, UpdatedAt: ended}, false},
		{"cancelled by the user", &watcher.Watch{Status: watcher.StatusCancelled, CreatedAt: created, UpdatedAt: ended}, false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		if got := gaveUpWithoutAcquiring(c.w); got != c.want {
			t.Errorf("%s: gaveUpWithoutAcquiring = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestRenderHistory_GaveUpSection is the #161 surfacing: an expired watch appears
// in `lagotto history` with its elapsed duration, labelled as a give-up rather
// than an acquisition.
func TestRenderHistory_GaveUpSection(t *testing.T) {
	var buf bytes.Buffer
	gaveUp := []watcher.Watch{expiredTestWatch("w-cf8e1a08", 48*time.Hour)}
	if err := renderHistory(&buf, nil, gaveUp); err != nil {
		t.Fatalf("renderHistory: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"Gave up without acquiring:",
		"WAITED",
		"w-cf8e1a08",
		"expired",
		"g7e.*",
		"48h0m0s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("renderHistory output missing %q:\n%s", want, out)
		}
	}
	// It must not be dressed up as a match.
	if strings.Contains(out, "MATCHED AT") {
		t.Errorf("gave-up-only output rendered the match header:\n%s", out)
	}
}

// TestRenderHistory_MatchesAndGaveUp: both halves of the wait distribution in one
// view — acquisitions with their WAIT, give-ups with their WAITED.
func TestRenderHistory_MatchesAndGaveUp(t *testing.T) {
	wait := 300.0
	matches := []watcher.MatchResult{{
		WatchID:              "w-got-it",
		InstanceType:         "g5.xlarge",
		Region:               "us-east-1",
		AvailabilityZone:     "us-east-1a",
		Price:                1.2,
		ActionTaken:          "spawned",
		MatchedAt:            time.Date(2026, 9, 16, 10, 5, 0, 0, time.UTC),
		WaitToAcquireSeconds: &wait,
	}}
	gaveUp := []watcher.Watch{expiredTestWatch("w-nope", 2*time.Hour)}

	var buf bytes.Buffer
	if err := renderHistory(&buf, matches, gaveUp); err != nil {
		t.Fatalf("renderHistory: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"MATCHED AT", "w-got-it", "5m0s", "Gave up without acquiring:", "w-nope", "2h0m0s"} {
		if !strings.Contains(out, want) {
			t.Errorf("renderHistory output missing %q:\n%s", want, out)
		}
	}
}

// TestRenderHistory_Empty keeps the old message for a genuinely empty history —
// the gave-up section must not turn "nothing to show" into a bare header.
func TestRenderHistory_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := renderHistory(&buf, nil, nil); err != nil {
		t.Fatalf("renderHistory: %v", err)
	}
	if got := strings.TrimSpace(buf.String()); got != "No match history found." {
		t.Errorf("empty output = %q, want \"No match history found.\"", got)
	}
}

// TestHistoryEntries_JSONShape: match rows keep the shape existing consumers
// parse (plus an additive "event":"match"), and a give-up is its OWN shape —
// event=gave_up with no instance_type/price, so it can never be misread as an
// acquisition (#161).
func TestHistoryEntries_JSONShape(t *testing.T) {
	matches := []watcher.MatchResult{{
		WatchID: "w-got-it", InstanceType: "g5.xlarge", Region: "us-east-1", Price: 1.2,
		ActionTaken: "spawned", MatchedAt: time.Date(2026, 9, 16, 10, 5, 0, 0, time.UTC),
	}}
	gaveUp := []watcher.Watch{expiredTestWatch("w-nope", 48*time.Hour)}

	data, err := json.Marshal(historyEntries(matches, gaveUp))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}

	if rows[0]["event"] != "match" {
		t.Errorf("row 0 event = %v, want match", rows[0]["event"])
	}
	if rows[0]["instance_type"] != "g5.xlarge" {
		t.Errorf("row 0 lost its MatchResult shape: %v", rows[0])
	}

	g := rows[1]
	if g["event"] != "gave_up" {
		t.Errorf("row 1 event = %v, want gave_up", g["event"])
	}
	if g["status"] != "expired" {
		t.Errorf("row 1 status = %v, want expired", g["status"])
	}
	if g["instance_type_pattern"] != "g7e.*" {
		t.Errorf("row 1 instance_type_pattern = %v, want g7e.*", g["instance_type_pattern"])
	}
	if g["time_to_give_up_seconds"] != 172800.0 {
		t.Errorf("row 1 time_to_give_up_seconds = %v, want 172800", g["time_to_give_up_seconds"])
	}
	for _, forbidden := range []string{"instance_type", "price", "matched_at"} {
		if _, ok := g[forbidden]; ok {
			t.Errorf("gave-up row has a %q key — it must not look like a match: %v", forbidden, g)
		}
	}
}
