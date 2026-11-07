package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/libs/i18n"
)

var extendTTL string

var extendCmd = &cobra.Command{
	Use:   "extend <watch-id>",
	Short: "Extend a watch's TTL",
	Long: `Extend a watch's TTL to --ttl from now. An expired watch is reactivated.

For an --until-landed watch, which has no TTL, extend is the same as
'lagotto ack': it moves the acknowledgement deadline a full window ahead and
resumes the watch if it paused. --ttl does not apply to it.`,
	Args: cobra.ExactArgs(1),
	RunE: runExtend,
}

func init() {
	rootCmd.AddCommand(extendCmd)
	extendCmd.Flags().StringVar(&extendTTL, "ttl", "24h", "New TTL from now (e.g., 24h, 7d)")
}

func runExtend(cmd *cobra.Command, args []string) error {
	ttlSet := cmd.Flags().Changed("ttl")
	ttl, err := parseTTLFlag(extendTTL)
	if err != nil {
		return err
	}
	return extendOrAck(cmd, args[0], func(w *watcher.Watch, now time.Time) (time.Time, error) {
		return extendExpiry(w, now, ttl, ttlSet)
	})
}

// extendExpiry is the new expiry `extend` sets: now + ttl, or for an
// until-landed watch (#239) its next acknowledgement deadline. An explicit
// --ttl on an until-landed watch is refused rather than silently ignored.
func extendExpiry(w *watcher.Watch, now time.Time, ttl time.Duration, ttlSet bool) (time.Time, error) {
	if !w.UntilLanded {
		return now.Add(ttl), nil
	}
	if ttlSet {
		return time.Time{}, fmt.Errorf("watch %s is --until-landed and has no TTL: 'lagotto extend %s' (or 'lagotto ack %s') "+
			"moves its acknowledgement deadline %s ahead; drop --ttl", w.WatchID, w.WatchID, w.WatchID, watcher.FormatSpan(w.AckWindow()))
	}
	return ackExpiry(w, now)
}

// extendOrAck is the shared body of `extend` and `ack`: resolve the caller's
// watch, compute its new expiry, store it (reactivating an expired watch), and
// re-arm the poller and email subscriptions a reactivated watch needs.
func extendOrAck(cmd *cobra.Command, watchID string, expiry func(w *watcher.Watch, now time.Time) (time.Time, error)) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// Resolve the watch and authorize the caller as its owner (#41). A mismatch
	// (or missing watch) surfaces the same not-found message.
	w, err := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), watchID)
	if err != nil {
		return err
	}

	// Only active or expired watches can be extended
	if w.Status != watcher.StatusActive && w.Status != watcher.StatusExpired {
		return fmt.Errorf("%s", i18n.T("lagotto.cancel.not_active", map[string]interface{}{
			"WatchID": watchID, "Status": w.Status,
		}))
	}

	newExpiry, err := expiry(w, time.Now().UTC())
	if err != nil {
		return err
	}
	reactivate := w.Status == watcher.StatusExpired

	// A reactivated watch is only polled if the poller's schedule, which the
	// poller switched off when it had nothing to poll, is switched back on
	// (#231). If that cannot be done the watch is put back to expired rather than
	// left active and unpolled. See pollschedule.go.
	sched := scheduler.NewFromConfig(cfg)
	armAction := "reactivate watch " + watchID
	var pollSched *scheduler.GetScheduleOutput
	if reactivate {
		if pollSched, err = pollSchedulePreflight(ctx, sched, armAction); err != nil {
			return err
		}
	}

	// ExtendWatch moves ttl_timestamp with the expiry, so an acknowledged
	// until-landed watch's record keeps a finite, later retention horizon (#239).
	if err := store.ExtendWatch(ctx, watchID, newExpiry, reactivate); err != nil {
		return fmt.Errorf("extend watch: %w", err)
	}

	if reactivate {
		if err := enablePollScheduleIfOff(ctx, sched, os.Stderr, pollSched); err != nil {
			// Put it back as it was: paused if it was paused.
			restore := store.ExpireWatch(ctx, watchID, w.ExpiredReason)
			return pollScheduleUpdateRefusal(armAction, "the watch is still expired", restore, err)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), i18n.T("lagotto.extend.reactivated", map[string]interface{}{
			"WatchID": watchID,
		}))
	}

	writeExtended(cmd.OutOrStdout(), w, newExpiry)

	// The acknowledged watch is reminded before its new deadline only by a
	// poller new enough to send reminders (#239). Best effort.
	if w.UntilLanded {
		acked := *w
		acked.Status, acked.ExpiresAt = watcher.StatusActive, newExpiry
		warnIfPollerPredatesReminders(ctx, cfg, os.Stderr, &acked)
	}

	// A reactivated watch's addresses may have been unsubscribed when it
	// expired (#240): subscribe them again. Best effort: the watch is extended
	// either way, and this says so if its emails will not be delivered.
	if reactivate {
		resubscribeOnReactivate(ctx, cmd.OutOrStdout(), os.Stderr, emailSubscriberFor(cfg, os.Getenv, accountFromARN(w.UserID)), w)
	}
	return nil
}

// writeExtended prints the outcome of an extend or ack.
func writeExtended(out io.Writer, w *watcher.Watch, newExpiry time.Time) {
	if w.UntilLanded {
		_, _ = fmt.Fprintf(out, "Acknowledged watch %s: it keeps hunting until it lands, and pauses at %s unless acknowledged again\n",
			w.WatchID, newExpiry.Format(time.RFC3339))
		return
	}
	_, _ = fmt.Fprintln(out, i18n.T("lagotto.extend.extended", map[string]interface{}{
		"WatchID":   w.WatchID,
		"ExpiresAt": newExpiry.Format(time.RFC3339),
	}))
}

// resubscribeOnReactivate subscribes a reactivated watch's email addresses,
// reusing any subscription they still have, and prints each one's state.
// subs is nil when no alerts topic could be named.
func resubscribeOnReactivate(ctx context.Context, out, errOut io.Writer, subs emailSubscriber, w *watcher.Watch) {
	if len(watcher.EmailAddresses(w)) == 0 {
		return
	}
	topic := alertsTopic{Why: "no alerts topic could be resolved"}
	if subs != nil {
		topic.ARN = subs.TopicArn()
	}
	w.Status = watcher.StatusActive
	if err := subscribeWatchEmails(ctx, errOut, topic, subs, w); err != nil {
		_, _ = fmt.Fprintf(errOut, "Warning: email notifications for %s will not be delivered: %v\n", w.WatchID,
			strings.TrimSuffix(err.Error(), "; no watch was created"))
		return
	}
	writeEmailStates(out, "  Notify:   ", w)
}
