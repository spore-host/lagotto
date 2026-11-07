package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/arm"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// topicEnvVar overrides the alerts topic `lagotto poll` publishes email
// notifications to (#241).
const topicEnvVar = arm.TopicEnvVar

// pollStderr is where `lagotto poll` writes its start-up warnings. A variable
// so tests can read them.
var pollStderr io.Writer = os.Stderr

// alertsTopic is the resolved alerts topic (#241): arm.AlertsTopic.
type alertsTopic = arm.AlertsTopic

// topicProber is the one check resolveAlertsTopic makes. *deploy.Deployer
// satisfies it with sns:GetTopicAttributes, an action `lagotto deploy` and
// `lagotto doctor` already call.
type topicProber = arm.TopicProber

// resolveAlertsTopic finds the alerts topic the way the hosted poller gets it:
// LAGOTTO_SNS_TOPIC_ARN, else lagotto-capacity-alerts in the caller's account
// and region. See arm.ResolveAlertsTopic.
func resolveAlertsTopic(ctx context.Context, getenv func(string) string, region, accountID string, probe topicProber) alertsTopic {
	return arm.ResolveAlertsTopic(ctx, getenv, region, accountID, probe)
}

// buildPollNotifier always returns a Notifier (#241). Webhook and sns channels
// name their own target and need no alerts topic; before #241 the CLI built a
// notifier only when LAGOTTO_SNS_TOPIC_ARN was set, so without it no channel
// fired at all. Only email depends on topic.ARN.
func buildPollNotifier(cfg aws.Config, topic alertsTopic) *watcher.Notifier {
	return watcher.NewNotifier(cfg, topic.ARN)
}

// accountFromARN returns the account field of an ARN, or "".
func accountFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return ""
	}
	return parts[4]
}

// topicWarnings is what `lagotto poll` prints at start when no alerts topic
// resolved (#241): one line saying so, then one per in-scope active watch with
// an email channel, since those are the notifications that will be dropped.
// Nothing when a topic resolved.
func topicWarnings(topic alertsTopic, watches []watcher.Watch, filter *watcher.WatchFilter) []string {
	if topic.ARN != "" {
		return nil
	}
	out := []string{fmt.Sprintf("Warning: email notifications will NOT be delivered: %s. "+
		"Webhook and sns channels are unaffected. Run `lagotto deploy` in this account and region, "+
		"or set %s to an SNS topic ARN.", topic.Why, topicEnvVar)}
	for i := range watches {
		w := &watches[i]
		if !filter.Matches(w) || !watcher.NeedsAlertsTopic(w) {
			continue
		}
		var targets []string
		for _, ch := range w.NotifyChannels {
			if ch.Type == "email" {
				targets = append(targets, "email:"+ch.Target)
			}
		}
		out = append(out, fmt.Sprintf("Warning: watch %s: %s will not be notified (no alerts topic)",
			w.WatchID, strings.Join(targets, ", ")))
	}
	return out
}

// warnUnresolvedTopic prints topicWarnings to out. list loads the active
// watches; when it fails only the general warning is printed.
func warnUnresolvedTopic(ctx context.Context, out io.Writer, topic alertsTopic, filter *watcher.WatchFilter, list func(context.Context) ([]watcher.Watch, error)) {
	if topic.ARN != "" {
		if verbose {
			_, _ = fmt.Fprintf(out, "Alerts topic for email notifications: %s (%s)\n", topic.ARN, topic.Source)
		}
		return
	}
	watches, err := list(ctx)
	if err != nil {
		watches = nil
	}
	for _, line := range topicWarnings(topic, watches, filter) {
		_, _ = fmt.Fprintln(out, line)
	}
}
