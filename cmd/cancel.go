package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var cancelYes bool

var cancelCmd = &cobra.Command{
	Use:   "cancel <watch-id>",
	Short: "Cancel an active watch",
	Args:  cobra.ExactArgs(1),
	RunE:  runCancel,
}

func init() {
	// --yes/-y matches the suite-wide confirmation convention (spawn#40).
	cancelCmd.Flags().BoolVarP(&cancelYes, "yes", "y", false, "Skip the confirmation prompt")
	rootCmd.AddCommand(cancelCmd)
}

func runCancel(cmd *cobra.Command, args []string) error {
	watchID := args[0]
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// Verify watch exists
	w, err := store.GetWatch(ctx, watchID)
	if err != nil {
		return fmt.Errorf("get watch: %w", err)
	}
	if w == nil {
		return fmt.Errorf("watch %s not found", watchID)
	}
	if w.Status != watcher.StatusActive {
		return fmt.Errorf("watch %s is not active (status: %s)", watchID, w.Status)
	}

	if !cancelYes && !confirmCancel(fmt.Sprintf("Cancel watch %s (%s)?", watchID, w.InstanceTypePattern)) {
		fmt.Fprintln(cmd.OutOrStdout(), "Aborted.")
		return nil
	}

	if err := store.UpdateWatchStatus(ctx, watchID, watcher.StatusCancelled); err != nil {
		return fmt.Errorf("cancel watch: %w", err)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Cancelled watch %s\n", watchID)
	return nil
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
