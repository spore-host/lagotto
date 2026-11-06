package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var (
	listAll     bool
	listProject string
	listMine    bool
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List your watches",
	Long: `List your watches.

Besides each watch's definition, the table shows its heartbeat:

  LAST POLL  how long ago the poller last checked it ("4m", "2h"). An active
             watch is flagged "!" when that is over 15 minutes: the poller is
             disabled or failing, or its capacity search keeps failing.
  TRIES      how many launch/hold/notify attempts it has made.
  LAST ERR   the class of its latest error: capacity, quota, terminal,
             unknown, or search (the capacity search itself failed).

Use -o wide for the full latest error, or -o json for every field.
"lagotto status <watch-id>" shows the full errors and when they happened.`,
	RunE: runList,
}

func init() {
	rootCmd.AddCommand(listCmd)
	// --all already means "no status filter", so it covers expired watches for free
	// once their records survive expiry (#161) — no separate --include-expired flag.
	// The help text just has to say so, since "all statuses" read as boilerplate
	// back when an expired watch had already been deleted.
	listCmd.Flags().BoolVar(&listAll, "all", false, "Show all statuses, including expired, cancelled, completed and failed watches (default: active only)")
	listCmd.Flags().StringVar(&listProject, "project", "", "Only show watches with this project label")
	listCmd.Flags().BoolVar(&listMine, "mine", false, "Only show watches you created (matches your caller ARN)")
}

func runList(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("get caller identity: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	watches, err := store.ListWatchesByUser(ctx, *identity.Arn, listStatusFilter(listAll))
	if err != nil {
		return fmt.Errorf("list watches: %w", err)
	}

	// Reuse the poller's WatchFilter matching logic (#1/#47) for --project/--mine
	// rather than re-implementing it. list already queries only the caller's own
	// watches, so --mine is a consistency guard (and self-documenting); --project
	// narrows to a single project label.
	filter := &watcher.WatchFilter{Project: listProject}
	if listMine {
		filter.Owner = *identity.Arn
	}
	if !filter.Empty() {
		kept := watches[:0]
		for _, w := range watches {
			if filter.Matches(&w) {
				kept = append(kept, w)
			}
		}
		watches = kept
	}

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(watches)
	}

	if len(watches) == 0 {
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "No watches found.")
		return nil
	}

	writeWatchTable(cmd.OutOrStdout(), watches, time.Now().UTC(), getOutputFormat() == "wide")
	return nil
}

// stalePollAfter is how old an active watch's last poll may be before `list`
// flags it with "!" (#239). The hosted poller runs every 5 minutes, so 15 is
// three missed cycles: the poller is disabled, broken, or its search is failing.
const stalePollAfter = 15 * time.Minute

// writeWatchTable renders the `list` table. now is a parameter so the ages are
// testable against a fixed clock. wide (`-o wide`) adds the full last error.
func writeWatchTable(out io.Writer, watches []watcher.Watch, now time.Time, wide bool) {
	const row = "%-12s %-10s %-15s %-20s %-20s %-25s %-6v %-10s %-20s %-9s %5s %-8s"
	header := fmt.Sprintf(row, "WATCH ID", "STATUS", "PROJECT", "OWNER", "PATTERN", "REGIONS", "SPOT", "ACTION", "EXPIRES",
		"LAST POLL", "TRIES", "LAST ERR")
	if wide {
		header += " LAST ERROR"
	}
	_, _ = fmt.Fprintln(out, strings.TrimRight(header, " "))
	for i := range watches {
		w := &watches[i]
		regions := displayRegions(w.Regions)
		if len(regions) > 25 {
			regions = regions[:22] + "..."
		}
		// A goal-driven fleet watch (#70) shows its target as "spawn×N" so the
		// maintain count is visible in the list without a new column.
		actionCol := string(w.Action)
		if w.DesiredCount > 0 {
			actionCol = fmt.Sprintf("%s×%d", w.Action, w.DesiredCount)
		}
		latest, latestAt := w.LatestError()
		line := fmt.Sprintf(row,
			w.WatchID,
			w.Status,
			dashIfEmpty(truncate(w.Project, 15)),
			truncate(shortOwner(w.UserID), 20),
			truncate(w.InstanceTypePattern, 20),
			regions,
			w.Spot,
			actionCol,
			w.ExpiresAt.Format(time.RFC3339),
			lastPollCol(w, now),
			strconv.Itoa(w.Attempts),
			dashIfEmpty(truncate(watcher.ErrorClass(latest), 8)),
		)
		if wide && latest != "" {
			line += fmt.Sprintf(" %s (%s ago)", latest, formatAge(now.Sub(latestAt)))
		}
		_, _ = fmt.Fprintln(out, strings.TrimRight(line, " "))
	}
}

// lastPollCol renders LAST POLL as an age ("4m", "2h", "3d"), or "never". An
// ACTIVE watch whose last poll is older than stalePollAfter is flagged "!"; so is
// an active one never polled though created longer ago than that. A finished
// watch is not polled any more, so its age is shown unflagged.
func lastPollCol(w *watcher.Watch, now time.Time) string {
	active := w.Status == watcher.StatusActive
	if w.LastPolledAt.IsZero() {
		if active && !w.CreatedAt.IsZero() && now.Sub(w.CreatedAt) > stalePollAfter {
			return "never!"
		}
		return "never"
	}
	age := now.Sub(w.LastPolledAt)
	s := formatAge(age)
	if active && age > stalePollAfter {
		s += "!"
	}
	return s
}

// formatAge renders a duration as one coarse unit: "45s", "12m", "5h", "3d".
// Negative durations (clock skew between the poller and this machine) read "0s".
func formatAge(d time.Duration) string {
	switch {
	case d < 0:
		return "0s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d/time.Second))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	default:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	}
}

// listStatusFilter maps --all to the store's status filter: the empty filter means
// "every status", so --all already covers expired watches (#161) now that their
// records survive expiry — hence no separate --include-expired flag. Extracted so
// that contract is unit-testable without AWS.
func listStatusFilter(all bool) watcher.WatchStatus {
	if all {
		return "" // no filter — every status, expired included
	}
	return watcher.StatusActive
}

// dashIfEmpty renders "-" for an empty column value so a blank field reads as
// "unset" rather than a formatting gap.
func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
