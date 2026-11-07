package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var statusCmd = &cobra.Command{
	Use:   "status <watch-id>",
	Short: "Show details of a watch",
	Args:  cobra.ExactArgs(1),
	RunE:  runStatus,
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	watchID := args[0]
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	w, err := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), watchID)
	if err != nil {
		return err
	}

	// Fill the derived, display-only wait/give-up durations (#139) so both the
	// JSON output and the detail table below share one computation.
	w.ComputeDurations()

	// Email delivery state per channel (#240). Best effort.
	annotateEmailStates(ctx, emailSubscriberFor(cfg, os.Getenv, accountFromARN(w.UserID)), []*watcher.Watch{w})

	// An active until-landed watch is only reminded by a poller new enough to
	// send reminders (#239). Best effort, to stderr, so -o json stays clean.
	warnIfPollerPredatesReminders(ctx, cfg, os.Stderr, w)

	// Cost exposure (#239 phase 5), in full: summary, basis and source.
	cliExposure(w)

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(w)
	}

	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Watch:    %s\n", w.WatchID)
	_, _ = fmt.Fprintf(out, "Status:   %s\n", w.Status)
	writeLanding(out, w, time.Now().UTC())
	if w.Project != "" {
		_, _ = fmt.Fprintf(out, "Project:  %s\n", w.Project)
	}
	if w.UserID != "" {
		_, _ = fmt.Fprintf(out, "Owner:    %s\n", w.UserID)
	}
	_, _ = fmt.Fprintf(out, "Pattern:  %s\n", w.InstanceTypePattern)
	_, _ = fmt.Fprintf(out, "Regions:  %s\n", displayRegions(w.Regions))
	_, _ = fmt.Fprintf(out, "Spot:     %v\n", w.Spot)
	if w.MaxPrice > 0 {
		_, _ = fmt.Fprintf(out, "Max price: $%.4f/hr\n", w.MaxPrice)
	}
	_, _ = fmt.Fprintf(out, "Action:   %s\n", w.Action)
	writeExposure(out, "Exposure: ", w.Exposure)
	writeNotifyChannels(out, w)
	if w.DesiredCount > 0 {
		_, _ = fmt.Fprintf(out, "Fleet:    maintain %d worker(s)\n", w.DesiredCount)
		if w.CompletionCondition != "" {
			_, _ = fmt.Fprintf(out, "Until:    %s\n", w.CompletionCondition)
		}
		// The quota ceiling (#153). This is the PRIMARY user-visible surface for it:
		// most watches carry no --notify, so without this line a fleet stuck below
		// --maintain by an account quota is only explained in the poller's logs.
		if w.QuotaCappedCount > 0 {
			_, _ = fmt.Fprintf(out, "Quota cap: fleet capped at %d/%d since %s\n",
				w.QuotaCappedCount, w.DesiredCount, w.QuotaCappedAt.Format(time.RFC3339))
			if w.QuotaCapReason != "" {
				_, _ = fmt.Fprintf(out, "  %s\n", w.QuotaCapReason)
			}
			_, _ = fmt.Fprintf(out, "  The watch stays active and keeps its running workers; a granted quota increase is picked up automatically.\n")
		}
	}
	writeQuotaInfeasible(out, w, time.Now().UTC())
	_, _ = fmt.Fprintf(out, "Created:  %s\n", w.CreatedAt.Format(time.RFC3339))
	writeExpires(out, w, time.Now().UTC())
	writeHeartbeat(out, w, time.Now().UTC())
	_, _ = fmt.Fprintf(out, "Matches:  %d\n", w.MatchCount)

	// Derived timings (#139): how long this watch waited for capacity, or — if it
	// gave up — how long it tried before failing.
	if d, ok := w.WaitToAcquire(); ok {
		_, _ = fmt.Fprintf(out, "Wait to acquire: %s\n", watcher.FormatWait(d))
	}
	if d, ok := w.TimeToGiveUp(); ok {
		_, _ = fmt.Fprintf(out, "Time to give up: %s\n", watcher.FormatWait(d))
	}

	writeLastMatch(out, w)
	return nil
}

// writeLanding prints the #239 landing line of `lagotto status`: the launched
// instance (or created reservation) the watch is waiting on, how long it has
// been pending, and when the poller gives up on it.
func writeLanding(out io.Writer, w *watcher.Watch, now time.Time) {
	if l := landingDesc(w, now); l != "" {
		_, _ = fmt.Fprintf(out, "Landing:  %s in %s", strings.TrimPrefix(l, "landing "), dashIfEmpty(w.PendingRegion))
		if w.PendingSince != nil {
			_, _ = fmt.Fprintf(out, "; given up at %s if still pending",
				w.PendingSince.Add(watcher.PendingLandingTimeout).Format(time.RFC3339))
		}
		_, _ = fmt.Fprintln(out)
		if w.PendingReservationID != "" {
			_, _ = fmt.Fprintf(out, "  The watch reserves nothing more until this resolves: active is a match; failed resumes the hunt.\n")
		} else {
			_, _ = fmt.Fprintf(out, "  The watch launches nothing more until this resolves: running is a match; terminated resumes the hunt.\n")
		}
	}
}

// writeLastMatch prints the "Last match" block of the status table.
func writeLastMatch(out io.Writer, w *watcher.Watch) {
	if w.LastMatch != nil {
		m := w.LastMatch
		_, _ = fmt.Fprintf(out, "\nLast match:\n")
		_, _ = fmt.Fprintf(out, "  Instance: %s\n", m.InstanceType)
		_, _ = fmt.Fprintf(out, "  Region:   %s\n", m.Region)
		_, _ = fmt.Fprintf(out, "  AZ:       %s\n", m.AvailabilityZone)
		_, _ = fmt.Fprintf(out, "  Price:    $%.4f/hr\n", m.Price)
		_, _ = fmt.Fprintf(out, "  Spot:     %v\n", m.IsSpot)
		_, _ = fmt.Fprintf(out, "  Action:   %s\n", m.ActionTaken)
		if m.InstanceID != "" {
			_, _ = fmt.Fprintf(out, "  Instance ID: %s\n", m.InstanceID)
		}
		// A fired hold: the reservation is targeted, so its id is what the user
		// needs to launch into it, or to know what `lagotto cancel` releases (#203).
		if m.ReservationID != "" {
			_, _ = fmt.Fprintf(out, "  Reservation: %s (%s)\n", m.ReservationID, m.AvailabilityZone)
			if w.Status == watcher.StatusMatched {
				_, _ = fmt.Fprintf(out, "  Launch into it:  %s\n", watcher.HoldLaunchCommandFor(w, m))
				_, _ = fmt.Fprintf(out, "  Release it early with: lagotto cancel %s\n", w.WatchID)
			}
		}
	}
}

// writeQuotaInfeasible prints which single-shot candidates EC2 refused on quota
// (#238). For an active watch it lists the candidates currently skipped and when
// each is re-probed; for a watch that failed because every candidate was refused
// it prints the terminal message, which lists them all with their quotas.
func writeQuotaInfeasible(out io.Writer, w *watcher.Watch, now time.Time) {
	if w.DesiredCount > 0 || len(w.QuotaInfeasible) == 0 {
		return
	}
	if w.Status == watcher.StatusFailed && w.QuotaCapReason != "" {
		_, _ = fmt.Fprintf(out, "Quota:    watch failed: every candidate was refused by an EC2 quota\n")
		for _, line := range strings.Split(w.QuotaCapReason, "\n") {
			_, _ = fmt.Fprintf(out, "  %s\n", strings.TrimLeft(line, " "))
		}
		return
	}
	if w.Status != watcher.StatusActive {
		return
	}
	live := w.LiveQuotaInfeasible(now)
	if len(live) == 0 {
		_, _ = fmt.Fprintf(out, "Quota:    no candidate is currently blocked (earlier quota refusals are due for re-probe)\n")
		return
	}
	_, _ = fmt.Fprintf(out, "Quota:    %d candidate(s) skipped as quota-infeasible; the watch keeps trying the others\n", len(live))
	for _, q := range live {
		_, _ = fmt.Fprintf(out, "  %s (re-probe after %s)\n", q.Describe(), q.ReprobeAt().Format(time.RFC3339))
	}
}

// writeHeartbeat prints the #239 heartbeat block of `lagotto status`: when the
// watch was last polled, how many attempts it has made, and the full last error
// and last search error. now is a parameter so the ages are testable.
func writeHeartbeat(out io.Writer, w *watcher.Watch, now time.Time) {
	if !w.LastPolledAt.IsZero() {
		stale := ""
		if w.Status == watcher.StatusActive && now.Sub(w.LastPolledAt) > stalePollAfter {
			stale = "; stale: the poller may be disabled, or its capacity search failing"
		}
		_, _ = fmt.Fprintf(out, "Last polled: %s (%s ago%s)\n", w.LastPolledAt.Format(time.RFC3339), formatAge(now.Sub(w.LastPolledAt)), stale)
	}
	if w.Attempts > 0 || !w.LastAttemptAt.IsZero() {
		_, _ = fmt.Fprintf(out, "Attempts: %d", w.Attempts)
		if !w.LastAttemptAt.IsZero() {
			_, _ = fmt.Fprintf(out, " (last %s, %s ago)", w.LastAttemptAt.Format(time.RFC3339), formatAge(now.Sub(w.LastAttemptAt)))
		}
		_, _ = fmt.Fprintln(out)
	}
	if w.LastError != "" {
		_, _ = fmt.Fprintf(out, "Last error: %s\n", w.LastError)
		if !w.LastErrorAt.IsZero() {
			_, _ = fmt.Fprintf(out, "  at %s (%s ago)\n", w.LastErrorAt.Format(time.RFC3339), formatAge(now.Sub(w.LastErrorAt)))
		}
	}
	if w.LastPollError != "" {
		_, _ = fmt.Fprintf(out, "Last search error: %s\n", w.LastPollError)
		if !w.LastPollErrorAt.IsZero() {
			recovered := ""
			if w.LastPolledAt.After(w.LastPollErrorAt) {
				recovered = "; the search has succeeded since"
			}
			_, _ = fmt.Fprintf(out, "  at %s (%s ago%s)\n", w.LastPollErrorAt.Format(time.RFC3339), formatAge(now.Sub(w.LastPollErrorAt)), recovered)
		}
	}
}

// writeNotifyChannels prints the watch's notification channels, with each
// email channel's subscription state when it is known (#240).
func writeNotifyChannels(out io.Writer, w *watcher.Watch) {
	for i, ch := range w.NotifyChannels {
		label := "Notify:   "
		if i > 0 {
			label = "          "
		}
		line := ch.Type + ":" + ch.Target
		if ch.Type == "email" && ch.Subscription != "" {
			line += "  " + ch.Subscription.Describe()
		}
		_, _ = fmt.Fprintf(out, "%s%s\n", label, line)
	}
}
