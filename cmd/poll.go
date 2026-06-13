package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

var (
	pollDaemon   bool
	pollInterval time.Duration
)

var pollCmd = &cobra.Command{
	Use:   "poll",
	Short: "Poll active watches (one cycle, or --daemon to loop)",
	Long: `Poll all active watches for available capacity and take their action
(notify / hold / spawn).

By default runs a single cycle. With --daemon it loops in the foreground on
--interval, exactly as the hosted Lambda poller does — so a 'lagotto watch
--action spawn' works hands-off with no Lambda/EventBridge/CloudFormation: keep
this running (or under your own supervisor) and it launches when capacity
appears. The daemon exits cleanly once no active watches remain (all fired,
expired, or cancelled), or on Ctrl-C / SIGTERM.`,
	RunE: runPoll,
}

func init() {
	rootCmd.AddCommand(pollCmd)
	pollCmd.Flags().BoolVar(&pollDaemon, "daemon", false, "Loop in the foreground, polling on --interval, until no active watches remain")
	pollCmd.Flags().DurationVar(&pollInterval, "interval", 5*time.Minute, "Polling interval in --daemon mode (e.g. 30s, 5m)")
}

func runPoll(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	poller, err := buildPoller(ctx)
	if err != nil {
		return err
	}

	if pollDaemon {
		return runPollDaemon(ctx, cmd, poller)
	}

	summary, err := poller.PollAll(ctx)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}
	printPollSummary(cmd, summary)
	return nil
}

// buildPoller wires up the poller exactly as the hosted Lambda does (store,
// optional notifier/spawner, holder, sagemaker), so the CLI single-cycle and
// --daemon modes share one code path with the production poller.
func buildPoller(ctx context.Context) (*watcher.Poller, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	truffleClient, err := truffleaws.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create truffle client: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	var notifier *watcher.Notifier
	if topicArn := os.Getenv("LAGOTTO_SNS_TOPIC_ARN"); topicArn != "" {
		notifier = watcher.NewNotifier(cfg, topicArn)
	}

	spawner, err := watcher.NewSpawner(ctx)
	if err != nil && verbose {
		fmt.Fprintf(os.Stderr, "Note: auto-spawn unavailable: %v\n", err)
	}

	return watcher.NewPoller(truffleClient, store, verbose, watcher.PollerOpts{
		Notifier:  notifier,
		Spawner:   spawner,
		Holder:    watcher.NewHolder(cfg),
		SageMaker: watcher.NewSageMakerLauncher(cfg),
	}), nil
}

// runPollDaemon polls on pollInterval in the foreground until no active watches
// remain (all fired/expired/cancelled) or the process is interrupted. This is
// the infra-free alternative to the hosted Lambda poller (#30): keep it running
// and a `watch --action spawn` fires when capacity appears.
func runPollDaemon(ctx context.Context, cmd *cobra.Command, poller *watcher.Poller) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(cmd.OutOrStderr(), "lagotto poll daemon: every %s (Ctrl-C to stop)\n", pollInterval)

	// Poll immediately, then on each tick — don't make the user wait one full
	// interval for the first cycle.
	for {
		summary, err := poller.PollAll(ctx)
		if err != nil {
			// Transient errors (throttling, a flaky region) shouldn't kill the
			// daemon — log and keep looping.
			fmt.Fprintf(cmd.OutOrStderr(), "poll cycle error (continuing): %v\n", err)
		} else {
			printPollSummary(cmd, summary)
			if summary.Watched == 0 {
				fmt.Fprintf(cmd.OutOrStderr(), "No active watches remain — daemon exiting.\n")
				return nil
			}
		}

		select {
		case <-ctx.Done():
			fmt.Fprintf(cmd.OutOrStderr(), "\nStopped.\n")
			return nil
		case <-time.After(pollInterval):
		}
	}
}

func printPollSummary(cmd *cobra.Command, summary *watcher.PollSummary) {
	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		_ = enc.Encode(summary)
		return
	}

	fmt.Fprintf(cmd.OutOrStdout(),
		"Poll cycle: %d watched, %d launched, %d notified, %d retrying, %d failed, %d expired\n",
		summary.Watched, summary.Launched, summary.Notified, summary.Retrying, summary.Failed, summary.Expired)

	for _, m := range summary.Matches {
		spotLabel := "on-demand"
		if m.IsSpot {
			spotLabel = "spot"
		}
		fmt.Fprintf(cmd.OutOrStdout(), "  %s %s in %s (%s) $%.4f/hr [watch: %s] action: %s\n",
			m.InstanceType, spotLabel, m.Region, m.AvailabilityZone, m.Price, m.WatchID, m.ActionTaken)
	}
}
