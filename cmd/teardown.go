package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var teardownForce bool

var teardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Delete lagotto's DynamoDB tables",
	Long: `Delete the DynamoDB tables lagotto uses (lagotto-watches and
lagotto-match-history by default).

lagotto already tears these down automatically once there are no active watches
and the tables have drained (watches and match history age out via DynamoDB TTL).
Use this to remove them explicitly.

By default it refuses to delete tables that still hold records, so you don't lose
match history; pass --force to delete regardless.`,
	RunE: runTeardown,
}

func init() {
	rootCmd.AddCommand(teardownCmd)
	teardownCmd.Flags().BoolVar(&teardownForce, "force", false, "Delete even if the tables still contain records")
}

func runTeardown(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)
	out := cmd.OutOrStdout()

	if !teardownForce {
		empty, err := store.TablesEmpty(ctx)
		if err != nil {
			return fmt.Errorf("check tables: %w", err)
		}
		if !empty {
			return fmt.Errorf("tables still contain records (watches and/or match history) — " +
				"re-run with --force to delete anyway, or wait for them to age out via TTL")
		}
	}

	deleted, err := store.DeleteTables(ctx)
	if err != nil {
		return fmt.Errorf("delete tables: %w", err)
	}
	if len(deleted) == 0 {
		fmt.Fprintf(out, "No lagotto tables to delete (%s, %s already absent).\n", watchesTable, historyTable)
		return nil
	}
	for _, name := range deleted {
		fmt.Fprintf(out, "Deleted table %s\n", name)
	}
	fmt.Fprintln(out, "Teardown complete.")
	return nil
}
