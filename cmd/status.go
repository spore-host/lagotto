package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(w)
	}

	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Watch:    %s\n", w.WatchID)
	_, _ = fmt.Fprintf(out, "Status:   %s\n", w.Status)
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
	_, _ = fmt.Fprintf(out, "Created:  %s\n", w.CreatedAt.Format(time.RFC3339))
	_, _ = fmt.Fprintf(out, "Expires:  %s\n", w.ExpiresAt.Format(time.RFC3339))
	if !w.LastPolledAt.IsZero() {
		_, _ = fmt.Fprintf(out, "Last polled: %s\n", w.LastPolledAt.Format(time.RFC3339))
	}
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
