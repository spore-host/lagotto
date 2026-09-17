package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var historyWatchID string

var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Show match history",
	RunE:  runHistory,
}

func init() {
	rootCmd.AddCommand(historyCmd)
	historyCmd.Flags().StringVar(&historyWatchID, "watch-id", "", "Filter by watch ID")
}

func runHistory(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	var matches []watcher.MatchResult

	// created maps a watch ID to its creation time, so each match row can surface
	// the derived wait-to-acquire (matched_at − created_at, #139) — MatchResult
	// itself doesn't carry the watch's created_at, so we join it here.
	created := map[string]time.Time{}

	if historyWatchID != "" {
		// Authorize: only the watch's owner may read its history by ID (#41).
		w, oerr := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), historyWatchID)
		if oerr != nil {
			return oerr
		}
		created[w.WatchID] = w.CreatedAt
		matches, err = store.ListMatchHistory(ctx, historyWatchID)
	} else {
		// Get user's history
		stsClient := sts.NewFromConfig(cfg)
		identity, err2 := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err2 != nil {
			return fmt.Errorf("get caller identity: %w", err2)
		}
		// Fetch the caller's watches (all statuses) to learn each watch's
		// created_at for the wait-to-acquire join (#139).
		if watches, werr := store.ListWatchesByUser(ctx, *identity.Arn, ""); werr == nil {
			for _, w := range watches {
				created[w.WatchID] = w.CreatedAt
			}
		}
		matches, err = store.ListMatchHistoryByUser(ctx, *identity.Arn)
	}
	if err != nil {
		return fmt.Errorf("list history: %w", err)
	}

	// Fill each row's derived wait-to-acquire from its watch's created_at (#139),
	// where we know it and the delta is sane (non-negative).
	for i := range matches {
		if c, ok := created[matches[i].WatchID]; ok && !c.IsZero() && !matches[i].MatchedAt.IsZero() {
			if d := matches[i].MatchedAt.Sub(c); d >= 0 {
				s := d.Seconds()
				matches[i].WaitToAcquireSeconds = &s
			}
		}
	}

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(matches)
	}

	if len(matches) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "No match history found.")
		return nil
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%-12s %-18s %-15s %-15s %-10s %-10s %-25s %s\n",
		"WATCH ID", "INSTANCE TYPE", "REGION", "AZ", "PRICE", "ACTION", "MATCHED AT", "WAIT")
	for _, m := range matches {
		wait := "-"
		if m.WaitToAcquireSeconds != nil {
			wait = watcher.FormatWait(time.Duration(*m.WaitToAcquireSeconds * float64(time.Second)))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%-12s %-18s %-15s %-15s $%-9.4f %-10s %-25s %s\n",
			m.WatchID,
			m.InstanceType,
			m.Region,
			m.AvailabilityZone,
			m.Price,
			m.ActionTaken,
			m.MatchedAt.Format(time.RFC3339),
			wait,
		)
	}
	return nil
}
