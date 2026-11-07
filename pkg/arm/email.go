package arm

import (
	"context"
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// TopicEnvVar overrides the alerts topic email notifications are published to
// (#241).
const TopicEnvVar = "LAGOTTO_SNS_TOPIC_ARN"

// AlertsTopic is the resolved alerts topic (#241).
type AlertsTopic struct {
	// ARN is the topic email channels are published to; "" means none
	// resolved, so email channels are not delivered.
	ARN string
	// Source says where ARN came from, for a start-up line.
	Source string
	// Why says why no topic resolved, when ARN is "".
	Why string
}

// TopicProber is the one check ResolveAlertsTopic makes: does the
// capacity-alerts topic exist in this account and region? *deploy.Deployer
// satisfies it with sns:GetTopicAttributes.
type TopicProber interface {
	AlertsTopicPresent(ctx context.Context, region, accountID string) (bool, error)
}

// ResolveAlertsTopic finds the alerts topic the way the hosted poller gets it
// (#241). `lagotto deploy` creates lagotto-capacity-alerts in the region it
// deploys to and wires that ARN into the poller's SNS_TOPIC_ARN; the legacy
// CloudFormation stack used the same name. So, in order:
//
//  1. getenv(TopicEnvVar), when set: an explicit override.
//  2. lagotto-capacity-alerts in the caller's account and region, when it
//     exists. If its existence can't be checked (for example AccessDenied on
//     sns:GetTopicAttributes) the ARN is used anyway, since a hosted-poller
//     role may publish to a topic it cannot describe; a wrong guess then fails
//     loudly at publish time.
//  3. Nothing: email channels are not delivered, and Why says why.
//
// getenv is os.Getenv in production; probe is deploy.New(cfg).
func ResolveAlertsTopic(ctx context.Context, getenv func(string) string, region, accountID string, probe TopicProber) AlertsTopic {
	if v := strings.TrimSpace(getenv(TopicEnvVar)); v != "" {
		return AlertsTopic{ARN: v, Source: TopicEnvVar}
	}
	if region == "" {
		return AlertsTopic{Why: "no AWS region is set, so lagotto cannot look for " + deploy.AlertsTopicName}
	}
	if accountID == "" {
		return AlertsTopic{Why: "your AWS account could not be resolved (sts:GetCallerIdentity failed), so lagotto cannot look for " + deploy.AlertsTopicName}
	}
	arn := deploy.AlertsTopicARN(region, accountID)
	present, err := probe.AlertsTopicPresent(ctx, region, accountID)
	switch {
	case err != nil:
		return AlertsTopic{ARN: arn, Source: fmt.Sprintf("%s in %s, by name (could not confirm it exists: %v)", deploy.AlertsTopicName, region, err)}
	case !present:
		return AlertsTopic{Why: fmt.Sprintf("%s does not exist in account %s, region %s (`lagotto deploy` creates it)", deploy.AlertsTopicName, accountID, region)}
	default:
		return AlertsTopic{ARN: arn, Source: fmt.Sprintf("%s in %s, created by `lagotto deploy`", deploy.AlertsTopicName, region)}
	}
}

// EmailSubscriber is the slice of *watcher.EmailSubscriptions that
// SubscribeWatchEmails uses.
type EmailSubscriber interface {
	TopicArn() string
	Ensure(ctx context.Context, addr string) (watcher.EnsureResult, error)
}

// EmailReconciler is the slice of *watcher.EmailSubscriptions that
// CleanupWatchEmails uses.
type EmailReconciler interface {
	TopicArn() string
	Reconcile(ctx context.Context, active []watcher.Watch, only []string) (watcher.ReconcileResult, error)
}

// SubscribeWatchEmails subscribes each email address on w to the alerts topic,
// reusing an address's existing subscription (#240). Run it BEFORE the watch is
// stored, so a refusal leaves no watch behind: an email channel that cannot be
// subscribed would deliver nothing, which is the silent failure #240 is about.
// It records each channel's state on w (NotifyChannel.Subscription, which is
// not stored) for the caller's output.
//
// subs is watcher.NewEmailSubscriptions(sns client, topic.ARN), or nil when
// topic.ARN is "". A watch with no email channel is a no-op, whatever topic and
// subs are. The refusal error says no watch was created.
//
// warnings has one line per address that already has a subscription with NO
// filter policy (it receives every user's email events). They are returned
// even with an error, for the addresses handled before it; the CLI prints each
// as "Warning: <line>".
func SubscribeWatchEmails(ctx context.Context, topic AlertsTopic, subs EmailSubscriber, w *watcher.Watch) (warnings []string, err error) {
	addrs := watcher.EmailAddresses(w)
	if len(addrs) == 0 {
		return nil, nil
	}
	if topic.ARN == "" || subs == nil {
		return nil, fmt.Errorf("email notifications need the alerts topic, and %s. Run `lagotto deploy` in this account and region "+
			"(or set %s), or drop the email channel; no watch was created", topic.Why, TopicEnvVar)
	}
	states := map[string]watcher.SubscriptionState{}
	for _, a := range addrs {
		res, err := subs.Ensure(ctx, a)
		if err != nil {
			return warnings, fmt.Errorf("subscribe %s to email notifications: %w; no watch was created", a, err)
		}
		states[a] = res.State
		if res.Unfiltered {
			warnings = append(warnings, fmt.Sprintf("%s already has a subscription on %s with NO filter policy: it receives every email "+
				"event on the topic, other users' watches included. lagotto did not create it and leaves it alone; "+
				"`lagotto doctor` names it.", a, subs.TopicArn()))
		}
	}
	SetChannelStates(w, states, "")
	return warnings, nil
}

// SetChannelStates records each email channel's subscription state on w.
// states is keyed by normalized address; fallback, when not "", is used for
// every email channel instead, and an address missing from states gets
// SubNone.
func SetChannelStates(w *watcher.Watch, states map[string]watcher.SubscriptionState, fallback watcher.SubscriptionState) {
	for i := range w.NotifyChannels {
		ch := &w.NotifyChannels[i]
		if ch.Type != "email" {
			continue
		}
		if fallback != "" {
			ch.Subscription = fallback
			continue
		}
		if s, ok := states[watcher.NormalizeEmail(ch.Target)]; ok {
			ch.Subscription = s
		} else {
			ch.Subscription = watcher.SubNone
		}
	}
}

// ListActiveError is CleanupWatchEmails's error when the active watches could
// not be listed, so nothing was unsubscribed.
type ListActiveError struct{ Err error }

func (e *ListActiveError) Error() string { return "list active watches: " + e.Err.Error() }

// Unwrap returns the listing error.
func (e *ListActiveError) Unwrap() error { return e.Err }

// CleanupWatchEmails is a cancel path's half of the email deleter (#240): it
// unsubscribes each of w's addresses that no OTHER active watch names. Run it
// after the watch's cancelled status is stored. It is best effort for the
// caller: cancelling already succeeded, and the hosted poller's next sweep (or
// `lagotto notify-gc`) retries whatever it leaves.
//
// listActive is the store's ListActiveWatches. w itself is excluded from what
// it returns, since the status index is eventually consistent and may still
// list the just-cancelled watch as active. Note that listActive's slice is
// filtered in place.
//
// A watch with no email channel, a nil subs, or a subs with no topic is a
// no-op. A missing topic (watcher.IsTopicNotFound) is not an error: there is
// nothing to unsubscribe from. If listActive fails the error is a
// *ListActiveError and nothing was done; any other error is Reconcile's, with
// the result of what it did before failing.
func CleanupWatchEmails(ctx context.Context, subs EmailReconciler, listActive func(context.Context) ([]watcher.Watch, error), w *watcher.Watch) (watcher.ReconcileResult, error) {
	addrs := watcher.EmailAddresses(w)
	if len(addrs) == 0 || subs == nil || subs.TopicArn() == "" {
		return watcher.ReconcileResult{}, nil
	}
	active, err := listActive(ctx)
	if err != nil {
		return watcher.ReconcileResult{}, &ListActiveError{Err: err}
	}
	others := active[:0]
	for _, a := range active {
		if a.WatchID != w.WatchID {
			others = append(others, a)
		}
	}
	res, err := subs.Reconcile(ctx, others, addrs)
	if err != nil && watcher.IsTopicNotFound(err) {
		err = nil
	}
	return res, err
}
