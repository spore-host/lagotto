package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
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
	if w.Status != watcher.StatusActive {
		return fmt.Errorf("watch %s is not active (status: %s)", watchID, w.Status)
	}

	if !cancelYes && !confirmCancel(fmt.Sprintf("Cancel watch %s (%s)?", watchID, w.InstanceTypePattern)) {
		fmt.Fprintln(cmd.OutOrStdout(), "Aborted.")
		return nil
	}

	// Release a held capacity reservation so it stops billing instead of waiting
	// out its 30-minute auto-expiry (#41). Best-effort: a failure here (e.g. the
	// reservation already expired) must not block cancelling the watch.
	if w.LastMatch != nil && w.LastMatch.ReservationID != "" {
		holder := watcher.NewHolder(cfg)
		if rerr := holder.Release(ctx, w.LastMatch.Region, w.LastMatch.ReservationID); rerr != nil {
			fmt.Fprintf(os.Stderr, "Note: could not release capacity reservation %s: %v\n", w.LastMatch.ReservationID, rerr)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Released capacity reservation %s\n", w.LastMatch.ReservationID)
		}
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
