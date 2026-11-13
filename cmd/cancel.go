package cmd

import (
	"bufio"
	"context"
	"errors"
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

var (
	cancelYes    bool
	cancelRegion string
)

var cancelCmd = &cobra.Command{
	Use:         "cancel <watch-id | scheduled-launch-id | group>",
	Annotations: level(levelBasic),
	Short:       "Cancel an active watch, a watch group or a scheduled launch, or release a fired hold's capacity reservation",
	Long: `Cancel an active watch (w-…), a watch group (its name, from 'lagotto watch
--group'), or a scheduled launch (sl-…, from 'lagotto launch').

A group: the group ends, and every active member is cancelled as a watch is,
with the tombstone "group <name> cancelled". A member's launch still landing
is not terminated (its TTL applies); a member's pending reservation is
released.

A watch: any capacity reservation it recorded is released, and
each email address it notifies is unsubscribed from the alerts topic unless another
active watch still names it.

For a --action hold watch that already fired (status: matched), cancel releases
the reservation it holds. The reservation is targeted and bills at the On-Demand
rate until its 30-minute window ends, so release it as soon as you know you
won't launch into it. The watch keeps its matched status. Releasing a
reservation that already expired or was already released is not an error.

A scheduled launch: the launch is marked cancelled, then its EventBridge
schedule and any boundary-retry schedules (-r<n>) are deleted. A schedule that
is already gone is not an error. A one-shot launch that already fired is
refused: there is nothing left to cancel. A --cron launch can be cancelled at
any time; it stops future fires. Cancelling an already-cancelled launch deletes
any schedule left behind. The record is in the region 'launch' ran in; pass
--region if that is not your default.`,
	Args: cobra.ExactArgs(1),
	RunE: runCancel,
}

func init() {
	// --yes/-y matches the suite-wide confirmation convention (spawn#40).
	cancelCmd.Flags().BoolVarP(&cancelYes, "yes", "y", false, "Skip the confirmation prompt")
	cancelCmd.Flags().StringVar(&cancelRegion, "region", "", "AWS region of the watch or scheduled launch (default: from your AWS config)")
	rootCmd.AddCommand(cancelCmd)
}

func runCancel(cmd *cobra.Command, args []string) error {
	watchID := args[0]
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, cancelRegion)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// A scheduled launch's id starts sl- (#279); a watch's starts w-.
	if isScheduledLaunchID(watchID) {
		return runCancelScheduledLaunch(ctx, cmd.OutOrStdout(), os.Stderr, cfg, watchID)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// A watch group (#287): end it and cancel every member.
	if isGroupArg(watchID) {
		return runCancelGroup(ctx, cmd.OutOrStdout(), os.Stderr, store, sts.NewFromConfig(cfg), watcher.NewHolder(cfg), watchID,
			func(w *watcher.Watch) {
				cleanupWatchEmails(ctx, cmd.OutOrStdout(), os.Stderr, emailSubscriberFor(cfg, os.Getenv, accountFromARN(w.UserID)), store.ListActiveWatches, w)
			})
	}

	// Verify the watch exists AND the caller owns it (#41).
	w, err := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), watchID)
	if err != nil {
		return err
	}

	confirm := func(prompt string) bool { return cancelYes || confirmCancel(prompt) }
	cancelled := false
	setStatus := func(ctx context.Context, id string, st watcher.WatchStatus) error {
		if err := store.UpdateWatchStatus(ctx, id, st); err != nil {
			return err
		}
		cancelled = true
		return nil
	}
	if err := cancelWatch(ctx, cmd.OutOrStdout(), os.Stderr, w, watcher.NewHolder(cfg), setStatus, confirm); err != nil {
		return err
	}
	// The watch has ended: unsubscribe any of its email addresses that no other
	// active watch names (#240). The topic is the one `watch` subscribed to.
	// A group member frees its group's launch slot (#287), so a sibling can
	// launch at once instead of after the slot expires.
	if cancelled && w.Group != "" {
		if err := store.ReleaseGroupSlot(ctx, w.GroupRef(), w.WatchID); err != nil {
			fmt.Fprintf(os.Stderr, "Note: %v (it expires on its own)\n", err)
		}
	}
	if cancelled {
		cleanupWatchEmails(ctx, cmd.OutOrStdout(), os.Stderr, emailSubscriberFor(cfg, os.Getenv, accountFromARN(w.UserID)), store.ListActiveWatches, w)
	}
	return nil
}

// reservationReleaser is the Holder method cancel needs; a fake in tests.
type reservationReleaser interface {
	Release(ctx context.Context, region, reservationID string) (watcher.ReleaseOutcome, error)
}

// heldReservation returns the reservation a watch holds, or "": the one still
// waiting to land (#239), else the one its last match holds.
func heldReservation(w *watcher.Watch) string {
	if w.PendingReservationID != "" {
		return w.PendingReservationID
	}
	if w.LastMatch == nil {
		return ""
	}
	return w.LastMatch.ReservationID
}

// heldReservationWhere is the region and AZ of heldReservation's reservation.
func heldReservationWhere(w *watcher.Watch) (region, az string) {
	if w.PendingReservationID != "" {
		if w.PendingMatch != nil {
			az = w.PendingMatch.AvailabilityZone
		}
		return w.PendingRegion, az
	}
	if w.LastMatch == nil {
		return "", ""
	}
	return w.LastMatch.Region, w.LastMatch.AvailabilityZone
}

// cancelWatch is runCancel after the AWS wiring: it decides what cancelling w
// means, and does it.
//
//   - active: release any recorded reservation (best-effort), then mark the
//     watch cancelled.
//   - matched with a held reservation (a fired --action hold, #203): release
//     the reservation. The watch stays matched, since it did match. Releasing
//     is the whole point here, so a failure is an error, but an already-ended
//     reservation is success (Release is idempotent).
//   - anything else: refuse, as before.
func cancelWatch(ctx context.Context, out, errOut io.Writer, w *watcher.Watch, rel reservationReleaser,
	setStatus func(context.Context, string, watcher.WatchStatus) error, confirm func(string) bool) error {
	resID := heldReservation(w)

	switch {
	case w.Status == watcher.StatusActive:
		if !confirm(fmt.Sprintf("Cancel watch %s (%s)?", watcher.ShortID(w.WatchID), w.InstanceTypePattern)) {
			_, _ = fmt.Fprintln(out, "Aborted.")
			return nil
		}
		// Release a held capacity reservation so it stops billing instead of
		// waiting out its 30-minute auto-expiry (#41). Best-effort: a failure
		// here must not block cancelling the watch.
		if resID != "" {
			region, az := heldReservationWhere(w)
			outcome, rerr := rel.Release(ctx, region, resID)
			if rerr != nil {
				_, _ = fmt.Fprintf(errOut, "Note: could not release capacity reservation %s: %v\n", resID, rerr)
			} else {
				reportRelease(out, resID, az, outcome)
			}
		}
		if err := setStatus(ctx, w.WatchID, watcher.StatusCancelled); err != nil {
			return fmt.Errorf("cancel watch: %w", err)
		}
		_, _ = fmt.Fprintf(out, "Cancelled watch %s\n", watcher.ShortID(w.WatchID))
		// A launch still landing (#239) is the user's instance now: cancel does not
		// terminate it, and no poller checks it any more. Say so, and how.
		if w.PendingInstanceID != "" {
			_, _ = fmt.Fprintf(errOut, "Note: instance %s (%s) was launched for this watch and had not been confirmed running. "+
				"It was not terminated; its TTL still applies. To terminate it now: spawn terminate %s\n",
				w.PendingInstanceID, w.PendingRegion, w.PendingInstanceID)
		}
		return nil

	case w.Status == watcher.StatusMatched && resID != "":
		m := w.LastMatch
		if !confirm(fmt.Sprintf("Release capacity reservation %s (%s) held by watch %s?", resID, m.AvailabilityZone, watcher.ShortID(w.WatchID))) {
			_, _ = fmt.Fprintln(out, "Aborted.")
			return nil
		}
		outcome, err := rel.Release(ctx, m.Region, resID)
		if err != nil {
			if errors.Is(err, watcher.ErrReleaseNotPermitted) {
				return fmt.Errorf("%w\nYour AWS credentials need ec2:CancelCapacityReservation in %s to release it early. "+
					"Otherwise it expires on its own 30 minutes after it was created (matched at %s)",
					err, m.Region, m.MatchedAt.UTC().Format(time.RFC3339))
			}
			return fmt.Errorf("release capacity reservation for watch %s: %w", watcher.ShortID(w.WatchID), err)
		}
		reportRelease(out, resID, m.AvailabilityZone, outcome)
		_, _ = fmt.Fprintf(out, "Watch %s stays matched.\n", watcher.ShortID(w.WatchID))
		return nil

	default:
		return fmt.Errorf("watch %s is not active (status: %s)", watcher.ShortID(w.WatchID), w.Status)
	}
}

// reportRelease prints what Release did.
func reportRelease(out io.Writer, id, az string, o watcher.ReleaseOutcome) {
	if o.AlreadyEnded {
		_, _ = fmt.Fprintf(out, "Capacity reservation %s (%s) had already ended (%s); nothing to release\n", id, az, o.State)
		return
	}
	_, _ = fmt.Fprintf(out, "Released capacity reservation %s (%s)\n", id, az)
}

// confirmCancel prompts on stderr and returns true only on an explicit yes,
// reading the answer from stdin.
func confirmCancel(prompt string) bool {
	fmt.Fprintf(os.Stderr, "%s [y/N] ", prompt)
	return readYes(os.Stdin)
}

// readYes returns true only when r yields an explicit yes. A read error or EOF
// (e.g. a non-interactive/piped stdin) reads as "no", so a piped invocation
// without --yes aborts rather than cancelling silently.
func readYes(r io.Reader) bool {
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && line == "" {
		return false
	}
	resp := strings.ToLower(strings.TrimSpace(line))
	return resp == "y" || resp == "yes"
}
