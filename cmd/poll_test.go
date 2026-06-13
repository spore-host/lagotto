package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestPrintPollSummary_Table verifies the human-readable poll summary: the count
// line plus one line per match with its action. (printPollSummary is the shared
// renderer for both single-cycle and --daemon poll output.)
func TestPrintPollSummary_Table(t *testing.T) {
	outputFormat = "table" // package global read by getOutputFormat()
	cmd := &cobra.Command{}
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	summary := &watcher.PollSummary{
		Watched: 2, Launched: 1, Notified: 1,
		Matches: []watcher.MatchResult{
			{InstanceType: "g5.12xlarge", IsSpot: false, Region: "us-west-2",
				AvailabilityZone: "us-west-2a", Price: 5.672, WatchID: "w-abc123", ActionTaken: "spawned"},
		},
	}
	printPollSummary(cmd, summary)
	out := buf.String()

	for _, want := range []string{
		"2 watched", "1 launched", "1 notified",
		"g5.12xlarge", "us-west-2", "w-abc123", "spawned", "on-demand",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("poll summary missing %q\n---\n%s", want, out)
		}
	}
}

// TestPrintPollSummary_SpotLabel checks a spot match is labeled "spot".
func TestPrintPollSummary_SpotLabel(t *testing.T) {
	outputFormat = "table"
	cmd := &cobra.Command{}
	var buf bytes.Buffer
	cmd.SetOut(&buf)

	printPollSummary(cmd, &watcher.PollSummary{
		Watched: 1,
		Matches: []watcher.MatchResult{{InstanceType: "g5.xlarge", IsSpot: true, Region: "us-east-1", ActionTaken: "notified"}},
	})
	if !strings.Contains(buf.String(), "spot") {
		t.Errorf("expected 'spot' label for a spot match, got:\n%s", buf.String())
	}
}
