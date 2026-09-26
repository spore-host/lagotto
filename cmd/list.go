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

var (
	listAll     bool
	listProject string
	listMine    bool
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List your watches",
	RunE:  runList,
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
		fmt.Fprintln(cmd.OutOrStdout(), "No watches found.")
		return nil
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%-12s %-10s %-15s %-20s %-20s %-25s %-6s %-10s %s\n",
		"WATCH ID", "STATUS", "PROJECT", "OWNER", "PATTERN", "REGIONS", "SPOT", "ACTION", "EXPIRES")
	for _, w := range watches {
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
		fmt.Fprintf(cmd.OutOrStdout(), "%-12s %-10s %-15s %-20s %-20s %-25s %-6v %-10s %s\n",
			w.WatchID,
			w.Status,
			dashIfEmpty(truncate(w.Project, 15)),
			truncate(shortOwner(w.UserID), 20),
			truncate(w.InstanceTypePattern, 20),
			regions,
			w.Spot,
			actionCol,
			w.ExpiresAt.Format(time.RFC3339),
		)
	}
	return nil
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
