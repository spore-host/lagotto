package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// topicEnvVar overrides the alerts topic `lagotto poll` publishes email
// notifications to (#241).
const topicEnvVar = "LAGOTTO_SNS_TOPIC_ARN"

// pollStderr is where `lagotto poll` writes its start-up warnings. A variable
// so tests can read them.
var pollStderr io.Writer = os.Stderr

// alertsTopic is the resolved alerts topic for a local poller (#241).
type alertsTopic struct {
	// ARN is the topic email channels are published to; "" means none
	// resolved, so email channels are not delivered.
	ARN string
	// Source says where ARN came from, for the start-up line.
	Source string
	// Why says why no topic resolved, when ARN is "".
	Why string
}

// topicProber is the one check resolveAlertsTopic makes: does the
// capacity-alerts topic exist in this account and region? *deploy.Deployer
// satisfies it with sns:GetTopicAttributes, an action `lagotto deploy` and
// `lagotto doctor` already call.
type topicProber interface {
	AlertsTopicPresent(ctx context.Context, region, accountID string) (bool, error)
}

// resolveAlertsTopic finds the alerts topic the way the hosted poller gets
// it (#241). `lagotto deploy` creates lagotto-capacity-alerts in the region it
// deploys to and wires that ARN into the poller's SNS_TOPIC_ARN; the legacy
// CloudFormation stack used the same name. So, in order:
//
//  1. LAGOTTO_SNS_TOPIC_ARN, when set: an explicit override.
//  2. lagotto-capacity-alerts in the caller's account and region, when it
//     exists. If its existence can't be checked (for example AccessDenied on
//     sns:GetTopicAttributes) the ARN is used anyway, since a hosted-poller
//     role may publish to a topic it cannot describe; a wrong guess then fails
//     loudly at publish time.
//  3. Nothing: email channels are not delivered, and Why says why.
func resolveAlertsTopic(ctx context.Context, getenv func(string) string, region, accountID string, probe topicProber) alertsTopic {
	if v := strings.TrimSpace(getenv(topicEnvVar)); v != "" {
		return alertsTopic{ARN: v, Source: topicEnvVar}
	}
	if region == "" {
		return alertsTopic{Why: "no AWS region is set, so lagotto cannot look for " + deploy.AlertsTopicName}
	}
	if accountID == "" {
		return alertsTopic{Why: "your AWS account could not be resolved (sts:GetCallerIdentity failed), so lagotto cannot look for " + deploy.AlertsTopicName}
	}
	arn := deploy.AlertsTopicARN(region, accountID)
	present, err := probe.AlertsTopicPresent(ctx, region, accountID)
	switch {
	case err != nil:
		return alertsTopic{ARN: arn, Source: fmt.Sprintf("%s in %s, by name (could not confirm it exists: %v)", deploy.AlertsTopicName, region, err)}
	case !present:
		return alertsTopic{Why: fmt.Sprintf("%s does not exist in account %s, region %s (`lagotto deploy` creates it)", deploy.AlertsTopicName, accountID, region)}
	default:
		return alertsTopic{ARN: arn, Source: fmt.Sprintf("%s in %s, created by `lagotto deploy`", deploy.AlertsTopicName, region)}
	}
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
