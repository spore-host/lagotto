package cmd

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
	libpricing "github.com/spore-host/libs/pricing"
)

// #239 phase 5: cost exposure in `list`, `status` and the until-landed prompt.

func expWatches(t *testing.T) []watcher.Watch {
	t.Helper()
	base := watcher.Watch{Status: watcher.StatusActive, UserID: "arn:aws:iam::123456789012:user/alice",
		Regions: []string{"us-east-1"}, CreatedAt: hbNow.Add(-time.Hour), ExpiresAt: hbNow.Add(time.Hour), LastPolledAt: hbNow.Add(-time.Minute)}
	mk := func(id string, action watcher.ActionMode, pattern string, f func(*watcher.Watch)) watcher.Watch {
		w := base
		w.WatchID, w.Action, w.InstanceTypePattern = id, action, pattern
		if f != nil {
			f(&w)
		}
		return w
	}
	return []watcher.Watch{
		mk("w-spawn000", watcher.ActionSpawn, "c7g.large", nil),
		mk("w-fleet000", watcher.ActionSpawn, "c7g.large", func(w *watcher.Watch) { w.DesiredCount = 4 }),
		mk("w-hold0000", watcher.ActionHold, "c7g.large", nil),
		mk("w-capped00", watcher.ActionSpawn, "p5.48xlarge", func(w *watcher.Watch) { w.MaxPrice = 40; w.Spot = true }),
		mk("w-p5000000", watcher.ActionSpawn, "p5.48xlarge", nil),
		mk("w-notify00", watcher.ActionNotify, "p5.48xlarge", nil),
	}
}

// TestListTable_Exposure is the EXPOSURE column golden, with the static note
// under the table. Rates are read from the static list so a refresh of the
// list does not break it; everything else is literal.
func TestListTable_Exposure(t *testing.T) {
	rate, err := libpricing.GetEC2HourlyRate("us-east-1", "c7g.large")
	if err != nil {
		t.Fatalf("static list lacks c7g.large in us-east-1: %v", err)
	}
	money := func(v float64) string {
		if v < 1 {
			return fmt.Sprintf("$%.4f", v)
		}
		return fmt.Sprintf("$%.2f", v)
	}
	var out bytes.Buffer
	writeWatchTable(&out, expWatches(t), hbNow, false)
	row := "%-12s %-10s %-15s %-12s %-20s %-20s %-25s %-6v %-10s %-16s %-25s %-9s %5s %-8s"
	line := func(id, pattern string, spot bool, action, exposure string) string {
		return strings.TrimRight(fmt.Sprintf(row, id, "active", "-", "-", "user/alice", pattern, "[us-east-1]", spot, action, exposure,
			"2026-11-06T13:00:00Z", "1m", "0", "-"), " ")
	}
	want := strings.Join([]string{
		strings.TrimRight(fmt.Sprintf(row, "WATCH ID", "STATUS", "PROJECT", "GROUP", "OWNER", "PATTERN", "REGIONS", "SPOT", "ACTION", "EXPOSURE", "EXPIRES",
			"LAST POLL", "TRIES", "LAST ERR"), " "),
		line("w-spawn000", "c7g.large", false, "spawn", "≤"+money(rate)+"/hr*"),
		line("w-fleet000", "c7g.large", false, "spawn×4", "≤"+money(4*rate)+"/hr*"),
		line("w-hold0000", "c7g.large", false, "hold", "30m@"+money(rate)+"/hr*"),
		line("w-capped00", "p5.48xlarge", true, "spawn", "≤$40.00/hr cap"),
		line("w-p5000000", "p5.48xlarge", false, "spawn", "unknown"),
		line("w-notify00", "p5.48xlarge", false, "notify", "none"),
		"",
		"EXPOSURE * on-demand rate, static, collected " + libpricing.PricesAsOf + ": " + watcher.CLIStaticPriceReason,
		`EXPOSURE is the cost if the watch lands: "lagotto status <watch-id>" shows its price basis.`,
		"",
	}, "\n")
	if got := out.String(); got != want {
		t.Errorf("list table:\n--- got\n%s--- want\n%s", got, want)
	}
}

// TestListExposureNotes_NoStaticNoNote: with no static rate shown, there is
// no static note; a partial maximum gets its own.
func TestListExposureNotes_NoStaticNoNote(t *testing.T) {
	ws := expWatches(t)
	var out bytes.Buffer
	writeWatchTable(&out, []watcher.Watch{ws[3], ws[4], ws[5]}, hbNow, false)
	if strings.Contains(out.String(), "EXPOSURE *") || strings.Contains(out.String(), "EXPOSURE ?") {
		t.Errorf("notes without a static or partial rate:\n%s", out.String())
	}
	w := ws[0]
	w.Regions = []string{"us-east-1", "af-south-1"}
	out.Reset()
	writeWatchTable(&out, []watcher.Watch{w}, hbNow, false)
	if !strings.Contains(out.String(), "/hr*?") || !strings.Contains(out.String(), "EXPOSURE ? ") {
		t.Errorf("partial exposure not marked:\n%s", out.String())
	}
}

// TestStatusExposure: status shows the summary and every detail line.
func TestStatusExposure(t *testing.T) {
	ws := expWatches(t)
	w := ws[2]
	var out bytes.Buffer
	writeExposure(&out, "Exposure: ", cliExposure(&w))
	got := out.String()
	for _, want := range []string{
		"Exposure: if this lands: 30 min reserved at up to $",
		"billed even if unused\n",
		"\n            price basis: on-demand rate\n",
		"\n            price source: static, collected " + libpricing.PricesAsOf + ": " + watcher.CLIStaticPriceReason + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status exposure lacks %q:\n%s", want, got)
		}
	}
	out.Reset()
	writeExposure(&out, "Exposure: ", nil)
	if out.Len() != 0 {
		t.Errorf("nil exposure printed %q", out.String())
	}
}

// TestConfirmUntilLanded_ShowsExposure: the prompt and the non-TTY refusal
// carry the exposure.
func TestConfirmUntilLanded_ShowsExposure(t *testing.T) {
	ul := untilLandedFlags{enabled: true, remindEvery: 7 * 24 * time.Hour, pauseAfter: 2}
	w := expWatches(t)[2]
	exp := cliExposure(&w)
	var errOut bytes.Buffer
	if err := confirmUntilLanded(strings.NewReader("y\n"), &errOut, true, false, watcher.ActionHold, "c7g.large", ul, exp); err != nil {
		t.Fatalf("TTY yes: %v", err)
	}
	for _, want := range []string{"billed for 30 minutes", "  " + exp.Summary + "\n", "collected " + libpricing.PricesAsOf} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("prompt lacks %q:\n%s", want, errOut.String())
		}
	}
	err := confirmUntilLanded(strings.NewReader(""), &errOut, false, false, watcher.ActionHold, "c7g.large", ul, exp)
	if err == nil || !strings.Contains(err.Error(), exp.Summary) || !strings.Contains(err.Error(), "--yes") {
		t.Errorf("non-TTY refusal = %v", err)
	}
}
