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

var cancelYes bool

var cancelCmd = &cobra.Command{
	Use:   "cancel <watch-id>",
	Short: "Cancel an active watch, or release a fired hold's capacity reservation",
	Long: `Cancel an active watch. Any capacity reservation it recorded is released.

For a --action hold watch that already fired (status: matched), cancel releases
the reservation it holds. The reservation is targeted and bills at the On-Demand
rate until its 30-minute window ends, so release it as soon as you know you
won't launch into it. The watch keeps its matched status. Releasing a
reservation that already expired or was already released is not an error.`,
	Args: cobra.ExactArgs(1),
	RunE: runCancel,
}

func init() {
	// --yes/-y matches the suite-wide confirmation convention (spawn#40).
	cancelCmd.Flags().BoolVarP(&cancelYes, "yes", "y", false, "Skip the confirmation prompt")
	rootCmd.AddCommand(cancelCmd)
}

func runCancel(cmd *cobra.Command, args []string) error {
	watchID := args[0]
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// Verify the watch exists AND the caller owns it (#41).
	w, err := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), watchID)
	if err != nil {
		return err
	}

	confirm := func(prompt string) bool { return cancelYes || confirmCancel(prompt) }
	return cancelWatch(ctx, cmd.OutOrStdout(), os.Stderr, w, watcher.NewHolder(cfg), store.UpdateWatchStatus, confirm)
}

// reservationReleaser is the Holder method cancel needs; a fake in tests.
type reservationReleaser interface {
	Release(ctx context.Context, region, reservationID string) (watcher.ReleaseOutcome, error)
}

// heldReservation returns the reservation a watch's last match holds, or "".
func heldReservation(w *watcher.Watch) string {
	if w.LastMatch == nil {
		return ""
	}
	return w.LastMatch.ReservationID
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
		if !confirm(fmt.Sprintf("Cancel watch %s (%s)?", w.WatchID, w.InstanceTypePattern)) {
			_, _ = fmt.Fprintln(out, "Aborted.")
			return nil
		}
		// Release a held capacity reservation so it stops billing instead of
		// waiting out its 30-minute auto-expiry (#41). Best-effort: a failure
		// here must not block cancelling the watch.
		if resID != "" {
			outcome, rerr := rel.Release(ctx, w.LastMatch.Region, resID)
			if rerr != nil {
				_, _ = fmt.Fprintf(errOut, "Note: could not release capacity reservation %s: %v\n", resID, rerr)
			} else {
				reportRelease(out, resID, w.LastMatch.AvailabilityZone, outcome)
			}
		}
		if err := setStatus(ctx, w.WatchID, watcher.StatusCancelled); err != nil {
			return fmt.Errorf("cancel watch: %w", err)
		}
		_, _ = fmt.Fprintf(out, "Cancelled watch %s\n", w.WatchID)
		return nil

	case w.Status == watcher.StatusMatched && resID != "":
		m := w.LastMatch
		if !confirm(fmt.Sprintf("Release capacity reservation %s (%s) held by watch %s?", resID, m.AvailabilityZone, w.WatchID)) {
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
			return fmt.Errorf("release capacity reservation for watch %s: %w", w.WatchID, err)
		}
		reportRelease(out, resID, m.AvailabilityZone, outcome)
		_, _ = fmt.Fprintf(out, "Watch %s stays matched.\n", w.WatchID)
		return nil

	default:
		return fmt.Errorf("watch %s is not active (status: %s)", w.WatchID, w.Status)
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
