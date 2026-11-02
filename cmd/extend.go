package cmd

import (
	"context"
	"fmt"
	"os"
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
	Args:  cobra.ExactArgs(1),
	RunE:  runExtend,
}

func init() {
	rootCmd.AddCommand(extendCmd)
	extendCmd.Flags().StringVar(&extendTTL, "ttl", "24h", "New TTL from now (e.g., 24h, 7d)")
}

func runExtend(cmd *cobra.Command, args []string) error {
	watchID := args[0]
	ctx := context.Background()

	ttl, err := time.ParseDuration(extendTTL)
	if err != nil {
		ttl, err = parseDuration(extendTTL)
		if err != nil {
			return fmt.Errorf("%s", i18n.T("lagotto.error.invalid_ttl", map[string]interface{}{
				"TTL": extendTTL, "Error": err,
			}))
		}
	}

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

	newExpiry := time.Now().UTC().Add(ttl)
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

	if err := store.ExtendWatch(ctx, watchID, newExpiry, reactivate); err != nil {
		return fmt.Errorf("extend watch: %w", err)
	}

	if reactivate {
		if err := enablePollScheduleIfOff(ctx, sched, os.Stderr, pollSched); err != nil {
			return pollScheduleUpdateRefusal(armAction, "the watch is still expired",
				store.UpdateWatchStatus(ctx, watchID, watcher.StatusExpired), err)
		}
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), i18n.T("lagotto.extend.reactivated", map[string]interface{}{
			"WatchID": watchID,
		}))
	}

	_, _ = fmt.Fprintln(cmd.OutOrStdout(), i18n.T("lagotto.extend.extended", map[string]interface{}{
		"WatchID":   watchID,
		"ExpiresAt": newExpiry.Format(time.RFC3339),
	}))
	return nil
}
