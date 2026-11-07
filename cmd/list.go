package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

EXPOSURE is what the watch can cost if it lands: "≤$X/hr" for spawn (a
fleet's total), "30m@$X/hr" for a hold's reservation (billed even if unused),
"none" for notify, "unknown" when lagotto has no price. "cap" means it is your
--max-price, "*" a rate from the static price list (the note under the table
says when it was collected and why), "?" that some types or regions have no
price. The CLI never calls the live Price List.

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

	// Email delivery state per channel (#240): pending confirmation, confirmed,
	// or not subscribed. One SNS read, best effort.
	ptrs := make([]*watcher.Watch, len(watches))
	for i := range watches {
		ptrs[i] = &watches[i]
	}
	annotateEmailStates(ctx, emailSubscriberFor(cfg, os.Getenv, aws.ToString(identity.Account)), ptrs)
	// Cost exposure per watch (#239 phase 5), for the EXPOSURE column and JSON.
	for _, w := range ptrs {
		cliExposure(w)
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
	writeListEmailStates(cmd.OutOrStdout(), watches)
	return nil
}

// writeListEmailStates prints, below the table, each active watch's email
// channels and their subscription state (#240).
func writeListEmailStates(out io.Writer, watches []watcher.Watch) {
	header := false
	for i := range watches {
		for _, ch := range watches[i].NotifyChannels {
			if ch.Type != "email" || ch.Subscription == "" {
				continue
			}
			if !header {
				_, _ = fmt.Fprintln(out, "\nEmail notifications:")
				header = true
			}
			_, _ = fmt.Fprintf(out, "  %-12s %s  %s\n", watches[i].WatchID, ch.Target, ch.Subscription.Describe())
		}
	}
}

// stalePollAfter is how old an active watch's last poll may be before `list`
// flags it with "!" (#239). The hosted poller runs every 5 minutes, so 15 is
// three missed cycles: the poller is disabled, broken, or its search is failing.
const stalePollAfter = 15 * time.Minute

// writeWatchTable renders the `list` table. now is a parameter so the ages are
// testable against a fixed clock. wide (`-o wide`) adds the full last error.
func writeWatchTable(out io.Writer, watches []watcher.Watch, now time.Time, wide bool) {
	const row = "%-12s %-10s %-15s %-20s %-20s %-25s %-6v %-10s %-16s %-25s %-9s %5s %-8s"
	header := fmt.Sprintf(row, "WATCH ID", "STATUS", "PROJECT", "OWNER", "PATTERN", "REGIONS", "SPOT", "ACTION", "EXPOSURE", "EXPIRES",
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
			statusCol(w),
			dashIfEmpty(truncate(w.Project, 15)),
			truncate(shortOwner(w.UserID), 20),
			truncate(w.InstanceTypePattern, 20),
			regions,
			w.Spot,
			actionCol,
			exposureCol(w),
			expiresCol(w, now),
			lastPollCol(w, now),
			strconv.Itoa(w.Attempts),
			// 9, not the column's 8: "no-subnet" (#256) is the one 9-byte class,
			// and "no-su..." would hide it. It only nudges a -o wide row by one.
			dashIfEmpty(truncate(watcher.ErrorClass(latest), 9)),
		)
		if wide && latest != "" {
			line += fmt.Sprintf(" %s (%s ago)", latest, formatAge(now.Sub(latestAt)))
		}
		if l := landingDesc(w, now); l != "" {
			line += "  " + l
		}
		_, _ = fmt.Fprintln(out, strings.TrimRight(line, " "))
	}
	writeExposureNotes(out, watches)
}

// exposureCol is the EXPOSURE column (#239 phase 5): what the watch can cost
// if it lands, per hour (a fleet's total), or a hold's reservation rate. See
// watcher.Exposure.Column for the markers.
func exposureCol(w *watcher.Watch) string {
	if w.Exposure == nil {
		cliExposure(w)
	}
	return truncate(w.Exposure.Column(), 16)
}

// writeExposureNotes explains the EXPOSURE markers under the table, once each:
// every static rate's label (static, collected <date>: why) for "*", and "?"
// for a maximum with unpriced combinations. The owner rule (truffle#184) is
// that a static price never appears without that label.
func writeExposureNotes(out io.Writer, watches []watcher.Watch) {
	seen := map[string]bool{}
	var notes []string
	partial := false
	for i := range watches {
		e := watches[i].Exposure
		if e == nil || e.Basis != watcher.BasisOnDemand {
			continue
		}
		for _, n := range e.StaticNotes {
			if !seen[n] {
				seen[n] = true
				notes = append(notes, n)
			}
		}
		partial = partial || len(e.Unpriced) > 0
	}
	if len(notes) == 0 && !partial {
		return
	}
	_, _ = fmt.Fprintln(out)
	for _, n := range notes {
		_, _ = fmt.Fprintf(out, "EXPOSURE * on-demand rate, %s\n", n)
	}
	if partial {
		_, _ = fmt.Fprintln(out, "EXPOSURE ? some of the watch's types or regions have no price, so the real maximum may be higher")
	}
	_, _ = fmt.Fprintln(out, "EXPOSURE is the cost if the watch lands: \"lagotto status <watch-id>\" shows its price basis.")
}

// expiresCol is the EXPIRES column: the expiry time, or for an until-landed
// watch (#239) its acknowledgement state, e.g. "until landed (ack in 4d)".
func expiresCol(w *watcher.Watch, now time.Time) string {
	if s, ok := untilLandedExpiresCol(w, now); ok {
		return s
	}
	return w.ExpiresAt.Format(time.RFC3339)
}

// statusCol is the STATUS column: the stored status, except that an active
// watch whose launch or hold is waiting to land (#239) reads "landing". The
// stored status (and JSON) stays "active".
func statusCol(w *watcher.Watch) string {
	if w.Status == watcher.StatusActive && w.Landing() {
		return "landing"
	}
	return string(w.Status)
}

// landingDesc describes a landing watch's pending resource, e.g.
// "landing i-0abc (pending 3m)", or "" for a watch that is not landing.
func landingDesc(w *watcher.Watch, now time.Time) string {
	if w.Status != watcher.StatusActive || !w.Landing() {
		return ""
	}
	if w.PendingSince == nil {
		return "landing " + w.PendingResourceID()
	}
	return fmt.Sprintf("landing %s (pending %s)", w.PendingResourceID(), formatAge(now.Sub(*w.PendingSince)))
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
