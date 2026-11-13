package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/arm"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Email channels and their subscriptions (#240). See pkg/watcher's
// subscriptions.go for the design: one filtered subscription per address on
// the alerts topic, created by `watch`, removed when no active watch names the
// address any more.

// emailSubscriber is the slice of watcher.EmailSubscriptions the commands use.
type emailSubscriber interface {
	TopicArn() string
	Ensure(ctx context.Context, addr string) (watcher.EnsureResult, error)
	States(ctx context.Context) (map[string]watcher.SubscriptionState, error)
	Reconcile(ctx context.Context, active []watcher.Watch, only []string) (watcher.ReconcileResult, error)
	Audit(ctx context.Context, active []watcher.Watch) (watcher.EmailAudit, error)
	MissingEmailSubscriptions(ctx context.Context, active []watcher.Watch) ([]string, error)
}

// newEmailSubscriber is how commands build the subscriber; a variable so
// tests substitute a fake.
var newEmailSubscriber = func(cfg aws.Config, topicArn string) emailSubscriber {
	return watcher.NewEmailSubscriptions(sns.NewFromConfig(cfg), topicArn)
}

// emailTopicByName is the alerts topic email channels use in this account and
// region, without an existence probe: LAGOTTO_SNS_TOPIC_ARN, else
// lagotto-capacity-alerts by name. For the read-only displays (list, status,
// doctor), where a missing topic shows up as SubNoTopic.
func emailTopicByName(getenv func(string) string, region, accountID string) string {
	if v := strings.TrimSpace(getenv(topicEnvVar)); v != "" {
		return v
	}
	if region == "" || accountID == "" {
		return ""
	}
	return deploy.AlertsTopicARN(region, accountID)
}

// emailSubscriberFor is the subscriber for the alerts topic by name
// (emailTopicByName), or nil when there is none to name.
func emailSubscriberFor(cfg aws.Config, getenv func(string) string, accountID string) emailSubscriber {
	arn := emailTopicByName(getenv, cfg.Region, accountID)
	if arn == "" {
		return nil
	}
	return newEmailSubscriber(cfg, arn)
}

// subscribeForNewWatch is `watch`'s subscribe step: resolve the alerts topic
// the way `poll` does (#241), then subscribeWatchEmails.
func subscribeForNewWatch(ctx context.Context, errOut io.Writer, cfg aws.Config, getenv func(string) string, accountID string, probe topicProber, w *watcher.Watch) error {
	if !watcher.NeedsAlertsTopic(w) {
		return nil
	}
	topic := resolveAlertsTopic(ctx, getenv, cfg.Region, accountID, probe)
	var subs emailSubscriber
	if topic.ARN != "" {
		subs = newEmailSubscriber(cfg, topic.ARN)
	}
	return subscribeWatchEmails(ctx, errOut, topic, subs, w)
}

// subscribeWatchEmails is arm.SubscribeWatchEmails, run before the watch is
// stored so a refusal leaves no watch behind (#240), printing its warnings to
// errOut.
func subscribeWatchEmails(ctx context.Context, errOut io.Writer, topic alertsTopic, subs emailSubscriber, w *watcher.Watch) error {
	warnings, err := arm.SubscribeWatchEmails(ctx, topic, subs, w)
	for _, line := range warnings {
		_, _ = fmt.Fprintf(errOut, "Warning: %s\n", line)
	}
	return err
}

// setChannelStates is arm.SetChannelStates: record each email channel's state
// on w; fallback, when not "", is used for every one.
func setChannelStates(w *watcher.Watch, states map[string]watcher.SubscriptionState, fallback watcher.SubscriptionState) {
	arm.SetChannelStates(w, states, fallback)
}

// annotateEmailStates fills in the subscription state of every email channel
// on the ACTIVE watches in ws (an ended watch's address may rightly be gone).
// Best effort and one SNS call: list and status never fail on it.
func annotateEmailStates(ctx context.Context, subs emailSubscriber, ws []*watcher.Watch) {
	var targets []*watcher.Watch
	for _, w := range ws {
		if w.Status == watcher.StatusActive && len(watcher.EmailAddresses(w)) > 0 {
			targets = append(targets, w)
		}
	}
	if len(targets) == 0 {
		return
	}
	if subs == nil || subs.TopicArn() == "" {
		for _, w := range targets {
			setChannelStates(w, nil, watcher.SubNoTopic)
		}
		return
	}
	states, err := subs.States(ctx)
	for _, w := range targets {
		switch {
		case err != nil && watcher.IsTopicNotFound(err):
			setChannelStates(w, nil, watcher.SubNoTopic)
		case err != nil:
			setChannelStates(w, nil, watcher.SubUnknown)
		default:
			setChannelStates(w, states, "")
		}
	}
}

// writeEmailStates prints one line per email channel that has a state.
func writeEmailStates(out io.Writer, indent string, w *watcher.Watch) {
	for _, ch := range w.NotifyChannels {
		if ch.Type == "email" && ch.Subscription != "" {
			_, _ = fmt.Fprintf(out, "%semail:%s  %s\n", indent, ch.Target, ch.Subscription.Describe())
		}
	}
}

// cleanupWatchEmails is cancel's half of the deleter: arm.CleanupWatchEmails,
// unsubscribing each of w's addresses that no other active watch names, with
// its result and any failure printed. Best effort: cancelling the watch already
// succeeded, and the poller's next sweep or `lagotto notify-gc` retries.
func cleanupWatchEmails(ctx context.Context, out, errOut io.Writer, subs emailSubscriber, listActive func(context.Context) ([]watcher.Watch, error), w *watcher.Watch) {
	res, err := arm.CleanupWatchEmails(ctx, subs, listActive, w)
	addrs := strings.Join(watcher.EmailAddresses(w), ", ")
	var listErr *arm.ListActiveError
	if errors.As(err, &listErr) {
		_, _ = fmt.Fprintf(errOut, "Note: could not check whether other watches still use %s (%v); the poller's next sweep or `lagotto notify-gc` will unsubscribe it if not\n",
			addrs, listErr.Err)
		return
	}
	reportReconcile(out, res)
	if err != nil {
		_, _ = fmt.Fprintf(errOut, "Note: could not unsubscribe %s: %v (the poller's next sweep or `lagotto notify-gc` retries)\n", addrs, err)
	}
}

// reportReconcile prints what Reconcile did.
func reportReconcile(out io.Writer, res watcher.ReconcileResult) {
	for _, a := range res.Removed {
		_, _ = fmt.Fprintf(out, "Unsubscribed %s from email notifications (no active watch names it)\n", a)
	}
	for _, a := range res.PendingUnused {
		_, _ = fmt.Fprintf(out, "%s's subscription is still pending confirmation, so it cannot be unsubscribed; SNS deletes it within 3 days if it is not confirmed (if it is, the next cleanup removes it)\n", a)
	}
}

var notifyGCResubscribe bool

var notifyGCCmd = &cobra.Command{
	Use:         "notify-gc",
	Annotations: level(levelAdvanced),
	Short:       "Remove email subscriptions no active watch needs",
	Long: `Reconcile the email subscriptions on the alerts topic (lagotto-capacity-alerts)
with the active watches in this account and region.

lagotto subscribes each --notify email: address to the alerts topic, with a filter
policy so the address receives only its own watches' events. When no active watch
names an address any more, its subscription is removed: by the hosted poller at
the start of each sweep, by 'lagotto cancel' for the cancelled watch, and by this
command. Run it if the poller cannot (its runtime policy predates the grant: re-run
'lagotto setup'), or to clean up at once.

Only subscriptions lagotto created (they carry its filter policy) are removed. A
subscription that is still pending confirmation has no ARN to unsubscribe by; SNS
deletes it on its own within 3 days.

With --resubscribe it also subscribes every address an active watch names that has
no subscription at all, for example one whose confirmation email was never clicked
and that SNS has since deleted. SNS sends each a new confirmation email.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runNotifyGC,
}

func init() {
	rootCmd.AddCommand(notifyGCCmd)
	notifyGCCmd.Flags().BoolVar(&notifyGCResubscribe, "resubscribe", false,
		"Also subscribe active watches' addresses that have no subscription (sends a confirmation email)")
}

func runNotifyGC(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	identity, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("get caller identity: %w", err)
	}
	topic := resolveAlertsTopic(ctx, os.Getenv, cfg.Region, aws.ToString(identity.Account), deploy.New(cfg))
	if topic.ARN == "" {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "No alerts topic (%s), so there are no email subscriptions to clean up.\n", topic.Why)
		return nil
	}
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	return notifyGC(ctx, cmd.OutOrStdout(), newEmailSubscriber(cfg, topic.ARN), store.ListActiveWatches, notifyGCResubscribe, getOutputFormat())
}

// notifyGCResult is notify-gc's JSON output.
type notifyGCResult struct {
	TopicArn string `json:"topic_arn"`
	watcher.ReconcileResult
	Subscribed []watcher.EnsureResult `json:"subscribed,omitempty"`
}

// notifyGC is runNotifyGC after the AWS wiring.
func notifyGC(ctx context.Context, out io.Writer, subs emailSubscriber, listActive func(context.Context) ([]watcher.Watch, error), resubscribe bool, format string) error {
	// Every active watch, unfiltered: an address a watch missing from this
	// list uses would be unsubscribed. A failed read stops here.
	active, err := listActive(ctx)
	if err != nil {
		if isAccessDenied(err) {
			return fmt.Errorf("list active watches (nothing was changed): notify-gc reads every owner's watches, which the "+
				"owner-scoped lagotto-cli-user policy does not allow; the hosted poller removes unused subscriptions on each "+
				"sweep, or run notify-gc with administrator credentials: %w", err)
		}
		return fmt.Errorf("list active watches (nothing was changed): %w", err)
	}
	res, rerr := subs.Reconcile(ctx, active, nil)
	result := notifyGCResult{TopicArn: subs.TopicArn(), ReconcileResult: res}
	var errs []error
	if rerr != nil {
		errs = append(errs, rerr)
	}
	if resubscribe {
		missing, merr := subs.MissingEmailSubscriptions(ctx, active)
		if merr != nil {
			errs = append(errs, merr)
		}
		for _, a := range missing {
			er, eerr := subs.Ensure(ctx, a)
			if eerr != nil {
				errs = append(errs, eerr)
				continue
			}
			result.Subscribed = append(result.Subscribed, er)
		}
	}

	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			return err
		}
	} else {
		reportReconcile(out, res)
		for _, s := range result.Subscribed {
			_, _ = fmt.Fprintf(out, "Subscribed %s: %s\n", s.Address, s.State.Describe())
		}
		if len(res.Removed)+len(res.PendingUnused)+len(result.Subscribed) == 0 {
			_, _ = fmt.Fprintf(out, "Nothing to remove: no unused email subscription on %s.\n", subs.TopicArn())
		}
	}
	return errors.Join(errs...)
}
