package cmd

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create the DynamoDB tables lagotto needs",
	Long: `Create the DynamoDB tables lagotto uses to store watches and match history
(lagotto-watches and lagotto-match-history by default; override with
--watches-table / --history-table).

This is idempotent — existing tables are left untouched. You don't normally need
to run it: 'lagotto watch' creates the tables automatically on first use. Run it
explicitly when you want to provision the backend ahead of time or confirm what
will be created.`,
	RunE: runSetup,
}

func init() {
	rootCmd.AddCommand(setupCmd)
}

func runSetup(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)
	created, err := store.EnsureTables(ctx)
	if err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}

	out := cmd.OutOrStdout()
	if len(created) == 0 {
		fmt.Fprintf(out, "All tables already exist (%s, %s). Nothing to do.\n", watchesTable, historyTable)
		return nil
	}
	for _, name := range created {
		fmt.Fprintf(out, "Created table %s\n", name)
	}
	fmt.Fprintln(out, "Setup complete.")
	return nil
}
