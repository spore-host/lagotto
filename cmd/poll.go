package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

var (
	pollDaemon   bool
	pollInterval time.Duration
	pollProject  string
	pollMine     bool
	pollWatchIDs []string
	pollNoLease  bool
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
expired, or cancelled), or on Ctrl-C / SIGTERM.

In a shared account, scope a local daemon to your own watches so it doesn't
drive other projects' watches (#47): --project NAME, --mine (only watches you
created), or --watch w-aaa,w-bbb. When scoped, the daemon exits once no active
watches in scope remain. A short processing lease guards against two pollers
firing the same watch; --no-lease disables it.`,
	RunE: runPoll,
}

func init() {
	rootCmd.AddCommand(pollCmd)
	pollCmd.Flags().BoolVar(&pollDaemon, "daemon", false, "Loop in the foreground, polling on --interval, until no active watches remain")
	pollCmd.Flags().DurationVar(&pollInterval, "interval", 5*time.Minute, "Polling interval in --daemon mode (e.g. 30s, 5m)")
	pollCmd.Flags().StringVar(&pollProject, "project", "", "Only poll watches with this project label (default: $LAGOTTO_PROJECT)")
	pollCmd.Flags().BoolVar(&pollMine, "mine", false, "Only poll watches created by the calling identity")
	pollCmd.Flags().StringSliceVar(&pollWatchIDs, "watch", nil, "Only poll these watch IDs (comma-separated or repeated)")
	pollCmd.Flags().BoolVar(&pollNoLease, "no-lease", false, "Disable the per-watch processing lease (not recommended when multiple pollers run)")
}

func runPoll(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	poller, filter, err := buildPoller(ctx)
	if err != nil {
		return err
	}

	if pollDaemon {
		return runPollDaemon(ctx, cmd, poller, filter)
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
// --daemon modes share one code path with the production poller. It also
// resolves the watch-scoping filter and lease owner (#47) from the --project /
// --mine / --watch flags, returning the filter so the daemon can scope its exit.
func buildPoller(ctx context.Context) (*watcher.Poller, *watcher.WatchFilter, error) {
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return nil, nil, fmt.Errorf("load AWS config: %w", err)
	}

	truffleClient, err := truffleaws.NewClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create truffle client: %w", err)
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

	// Resolve the caller ARN once — it's the lease owner and, for --mine, the
	// owner filter.
	callerARN := callerIdentityARN(ctx, cfg)

	filter, err := resolvePollFilter(callerARN)
	if err != nil {
		return nil, nil, err
	}

	// Lease owner: a stable-ish id for this poller process so its own re-claims
	// succeed and other pollers are excluded. Empty disables leasing.
	leaseOwner := ""
	if !pollNoLease {
		leaseOwner = "cli:" + callerARN
	}

	return watcher.NewPoller(truffleClient, store, verbose, watcher.PollerOpts{
		Notifier:   notifier,
		Spawner:    spawner,
		Holder:     watcher.NewHolder(cfg),
		SageMaker:  watcher.NewSageMakerLauncher(cfg),
		Filter:     filter,
		LeaseOwner: leaseOwner,
	}), filter, nil
}

// resolvePollFilter builds the watch-scoping filter from the --project / --mine /
// --watch flags (#47). --project falls back to $LAGOTTO_PROJECT. --mine needs the
// caller ARN; if identity can't be resolved it's an error (better than silently
// polling nothing or everything).
func resolvePollFilter(callerARN string) (*watcher.WatchFilter, error) {
	project := pollProject
	if project == "" {
		project = os.Getenv("LAGOTTO_PROJECT")
	}

	owner := ""
	if pollMine {
		if callerARN == "" {
			return nil, fmt.Errorf("--mine needs your AWS identity, but GetCallerIdentity failed; check your credentials")
		}
		owner = callerARN
	}

	f := &watcher.WatchFilter{Project: project, Owner: owner, WatchIDs: pollWatchIDs}
	return f, nil
}

// callerIdentityARN returns the caller's ARN, or "" if it can't be resolved
// (leasing/--mine degrade gracefully rather than failing the whole poll).
func callerIdentityARN(ctx context.Context, cfg aws.Config) string {
	id, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || id.Arn == nil {
		return ""
	}
	return *id.Arn
}

// runPollDaemon polls on pollInterval in the foreground until no active watches
// remain (all fired/expired/cancelled) or the process is interrupted. This is
// the infra-free alternative to the hosted Lambda poller (#30): keep it running
// and a `watch --action spawn` fires when capacity appears.
func runPollDaemon(ctx context.Context, cmd *cobra.Command, poller *watcher.Poller, filter *watcher.WatchFilter) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	scope := "all account watches"
	if filter != nil && !filter.Empty() {
		scope = filter.Describe()
	}
	fmt.Fprintf(cmd.OutOrStderr(), "lagotto poll daemon: every %s, scope: %s (Ctrl-C to stop)\n", pollInterval, scope)

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
			// summary.Watched counts only in-scope watches (out-of-scope ones are
			// skipped before the tally), so a scoped daemon exits when ITS watches
			// are done, not the whole account's (#47).
			if summary.Watched == 0 {
				if filter != nil && !filter.Empty() {
					fmt.Fprintf(cmd.OutOrStderr(), "No active watches in scope (%s) remain — daemon exiting.\n", filter.Describe())
				} else {
					fmt.Fprintf(cmd.OutOrStderr(), "No active watches remain — daemon exiting.\n")
				}
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
