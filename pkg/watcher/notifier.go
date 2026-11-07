package watcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// newSafeHTTPClient returns an http.Client whose dialer re-validates the actual
// IP it is about to connect to. ValidateWebhookURL resolves+vets the host at
// validation time, but the default transport performs its OWN DNS resolution at
// connect time — so a DNS-rebinding attacker can return a public IP during
// validation and 169.254.169.254 (the EC2/Lambda metadata endpoint, which holds
// the function's credentials) at dial time. Vetting the post-resolution dial IP
// closes that TOCTOU window regardless of what DNS returns (#40).
func newSafeHTTPClient(timeout time.Duration) *http.Client {
	baseDialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("resolve %q: %w", host, err)
			}
			if len(ips) == 0 {
				return nil, fmt.Errorf("host %q resolved to no addresses", host)
			}
			// Every resolved IP must pass — reject if any is internal, so a
			// multi-record response can't slip a blocked address past us.
			for _, ipAddr := range ips {
				if err := checkIP(ipAddr.IP); err != nil {
					return nil, fmt.Errorf("refusing to dial %q: %w", host, err)
				}
			}
			// Pin the connection to a vetted IP so the kernel can't re-resolve to
			// a different (blocked) address between this check and connect.
			return baseDialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
		},
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

// Notifier dispatches match notifications to configured channels.
// snsPublisher is the SNS call the notifier makes; an interface so tests can
// capture what is published (#203). *sns.Client satisfies it.
type snsPublisher interface {
	Publish(ctx context.Context, in *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type Notifier struct {
	snsClient  snsPublisher
	httpClient *http.Client
	topicArn   string
	// subs manages the per-address subscriptions on topicArn (#240); nil
	// skips subscription cleanup.
	subs SubscriptionAPI
}

// ErrNoAlertsTopic is the error an email channel returns when the Notifier has
// no alerts topic (#241). Email is the one channel that needs it: an email
// notification is published to lagotto-capacity-alerts, whose subscriptions
// deliver it. Webhook and sns channels name their own target, so they are
// delivered without one.
var ErrNoAlertsTopic = errors.New("email channel not delivered: no alerts topic (lagotto-capacity-alerts) is configured; " +
	"run `lagotto deploy` in this account and region, or set LAGOTTO_SNS_TOPIC_ARN")

// NewNotifier creates a Notifier. topicArn is the alerts topic email
// notifications are published to. It may be empty: webhook and sns channels
// are still delivered, and an email channel returns ErrNoAlertsTopic (#241).
func NewNotifier(cfg aws.Config, topicArn string) *Notifier {
	client := sns.NewFromConfig(cfg)
	return &Notifier{
		snsClient:  client,
		httpClient: newSafeHTTPClient(10 * time.Second),
		topicArn:   topicArn,
		subs:       client,
	}
}

// HasAlertsTopic reports whether email channels can be delivered.
func (n *Notifier) HasAlertsTopic() bool { return n.topicArn != "" }

// publishEmail publishes an email notification for addr to the alerts topic,
// or returns ErrNoAlertsTopic when there is none. Every email send goes
// through here, so no Publish is ever made with an empty TopicArn, and every
// one carries the EmailAttribute message attribute set to addr (#240): each
// address's subscription has a filter policy matching only its own address,
// so only addr receives it. A publish without the attribute (an sns: channel
// a user pointed at the alerts topic, say) reaches no filtered subscriber.
func (n *Notifier) publishEmail(ctx context.Context, addr, subject, body string) error {
	if n.topicArn == "" {
		return ErrNoAlertsTopic
	}
	if NormalizeEmail(addr) == "" {
		return fmt.Errorf("email channel has no address")
	}
	_, err := n.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn:          aws.String(n.topicArn),
		Subject:           aws.String(subject),
		Message:           aws.String(body),
		MessageAttributes: emailMessageAttributes(addr),
	}, InARNRegion(n.topicArn))
	return err
}

// ReconcileEmailSubscriptions unsubscribes the lagotto-managed email
// subscriptions on the alerts topic that no watch in active names (#240).
// active must be every active watch, unfiltered. A no-op without a topic.
func (n *Notifier) ReconcileEmailSubscriptions(ctx context.Context, active []Watch) (ReconcileResult, error) {
	if n == nil || n.topicArn == "" || n.subs == nil {
		return ReconcileResult{}, nil
	}
	return NewEmailSubscriptions(n.subs, n.topicArn).Reconcile(ctx, active, nil)
}

// NeedsAlertsTopic reports whether w has a channel that can only be delivered
// through the alerts topic: an email channel.
func NeedsAlertsTopic(w *Watch) bool {
	for _, ch := range w.NotifyChannels {
		if ch.Type == "email" {
			return true
		}
	}
	return false
}

// Notify sends notifications for a match to all channels configured on the watch.
func (n *Notifier) Notify(ctx context.Context, w *Watch, m *MatchResult) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}

	var lastErr error
	for _, ch := range w.NotifyChannels {
		var err error
		switch ch.Type {
		case "email":
			err = n.sendEmail(ctx, ch.Target, w, m)
		case "webhook":
			// Defense-in-depth: re-validate at send time to catch watches created
			// before the URL validation fix was deployed.
			if verr := ValidateWebhookURL(ch.Target); verr != nil {
				err = fmt.Errorf("blocked unsafe webhook URL: %w", verr)
			} else {
				err = n.sendWebhook(ctx, ch.Target, w, m)
			}
		case "sns":
			err = n.sendSNS(ctx, ch.Target, w, m)
		default:
			continue
		}
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// NotifyQuotaCap tells the user that a fleet watch has hit its EC2 vCPU quota
// ceiling and can't reach --maintain N (#153). A sibling of Notify rather than a
// synthesized MatchResult: a quota cap is not a capacity match, and faking one
// would pollute match history and the --action semantics ("what did lagotto do
// when it found capacity?").
//
// Same channel fan-out and the same defence-in-depth webhook re-validation as
// Notify, for watches created before the URL validation fix shipped.
//
// It is also sent, once, when a single-shot watch fails because EC2 refused every
// candidate on quota (#238); running is 0 and the wording says so.
func (n *Notifier) NotifyQuotaCap(ctx context.Context, w *Watch, running int, reason string) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}

	subject := quotaCapSubject(w, running)
	body := quotaCapBody(w, running, reason)

	var lastErr error
	for _, ch := range w.NotifyChannels {
		var err error
		switch ch.Type {
		case "email":
			err = n.publishEmail(ctx, ch.Target, subject, body)
		case "webhook":
			if verr := ValidateWebhookURL(ch.Target); verr != nil {
				err = fmt.Errorf("blocked unsafe webhook URL: %w", verr)
			} else {
				err = n.sendQuotaCapWebhook(ctx, ch.Target, w, running, reason)
			}
		case "sns":
			// Mirrors sendSNS: a raw-topic channel gets the machine-readable payload,
			// not the human email body.
			var data []byte
			if data, err = json.Marshal(quotaCapPayload(w, running, reason)); err == nil {
				_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
					TopicArn: aws.String(ch.Target),
					Message:  aws.String(string(data)),
				}, InARNRegion(ch.Target))
			}
		default:
			continue
		}
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// quotaCapSubject renders the notification subject line, e.g.
// "[lagotto] fleet capped at 2/5 by EC2 quota". Pure, so it's testable without
// SNS (snsClient is a concrete *sns.Client).
//
// A single-shot watch (DesiredCount 0) only reaches this when every candidate was
// refused by an EC2 quota and the watch failed (#238), so it gets its own wording.
func quotaCapSubject(w *Watch, running int) string {
	if w.DesiredCount == 0 {
		return fmt.Sprintf("[lagotto] watch %s failed: every candidate refused by EC2 quota", w.WatchID)
	}
	return fmt.Sprintf("[lagotto] fleet capped at %d/%d by EC2 quota", running, w.DesiredCount)
}

// quotaCapBody renders the notification body: what happened, what lagotto will
// keep doing (it stays active and keeps the running workers), and the reason
// string (which carries the family, the numbers when known, and the
// quota-increase hint).
func quotaCapBody(w *Watch, running int, reason string) string {
	if w.DesiredCount == 0 {
		return fmt.Sprintf(`Your watch %s stopped: EC2 refused every candidate because of an account quota.

Pattern:  %s
Regions:  %v
Spot:     %v

%s

Nothing was launched. lagotto tried each candidate (instance type, region),
skipped the ones a quota refused, and stopped only when none was left. Raise the
quota (or narrow the watch to sizes that fit) and create the watch again.
`,
			w.WatchID, w.InstanceTypePattern, w.Regions, w.Spot, reason)
	}
	return fmt.Sprintf(`Your fleet watch %s is capped by an EC2 service quota.

Running:  %d of %d requested workers
Pattern:  %s
Regions:  %v
Spot:     %v

%s

The watch stays ACTIVE and keeps its running workers: it maintains as many as the
quota allows and picks up a granted quota increase automatically. While capped it
retries at most once every %s; when it can read the quota, it retries as soon as
the limit is raised and otherwise only once every %s.
`,
		w.WatchID, running, w.DesiredCount, w.InstanceTypePattern, w.Regions, w.Spot,
		reason, QuotaCapReprobeInterval, QuotaCapBackstopInterval)
}

// quotaCapPayload is the machine-readable quota-cap event. The "event" key is new
// to THIS payload only — the existing match payload is deliberately left
// untouched (no retrofitted "event":"match") so no existing webhook consumer
// changes shape.
//
// A single-shot watch that failed because every candidate was quota-refused
// (#238) sends event "quota_infeasible" with the per-candidate list instead.
func quotaCapPayload(w *Watch, running int, reason string) map[string]interface{} {
	if w.DesiredCount == 0 {
		return map[string]interface{}{
			"watch_id":   w.WatchID,
			"event":      "quota_infeasible",
			"status":     string(StatusFailed),
			"pattern":    w.InstanceTypePattern,
			"reason":     reason,
			"candidates": w.QuotaInfeasible,
		}
	}
	return map[string]interface{}{
		"watch_id": w.WatchID,
		"event":    "quota_capped",
		"running":  running,
		"desired":  w.DesiredCount,
		"pattern":  w.InstanceTypePattern,
		"reason":   reason,
	}
}

// sendQuotaCapWebhook posts the quota-cap event to a webhook target.
func (n *Notifier) sendQuotaCapWebhook(ctx context.Context, url string, w *Watch, running int, reason string) error {
	data, err := json.Marshal(quotaCapPayload(w, running, reason))
	if err != nil {
		return fmt.Errorf("marshal quota-cap webhook payload: %w", err)
	}
	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// NotifyExpired tells the user that a watch reached its TTL without ever
// acquiring capacity (#161) — the mirror image of the match notification, and the
// moment a human most wants to hear from an unattended hunt, especially for
// `--action spawn` where nothing was ever launched.
//
// A sibling of Notify rather than a synthesized MatchResult, for the same reason
// NotifyQuotaCap is: an expiry is the ABSENCE of a match, and faking one would
// pollute match history and the --action semantics.
//
// Notify-once is structural, not stateful: the poller only ever loads ACTIVE
// watches, so the active→expired transition happens exactly once and no
// "already notified" flag is needed (unlike the #153 quota cap, which recurs
// while the watch stays active).
func (n *Notifier) NotifyExpired(ctx context.Context, w *Watch) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}

	subject := expiredSubject(w)
	body := expiredBody(w)

	var lastErr error
	for _, ch := range w.NotifyChannels {
		var err error
		switch ch.Type {
		case "email":
			err = n.publishEmail(ctx, ch.Target, subject, body)
		case "webhook":
			// Same defence-in-depth re-validation as Notify, for watches created
			// before the URL validation fix shipped.
			if verr := ValidateWebhookURL(ch.Target); verr != nil {
				err = fmt.Errorf("blocked unsafe webhook URL: %w", verr)
			} else {
				err = n.sendExpiredWebhook(ctx, ch.Target, w)
			}
		case "sns":
			var data []byte
			if data, err = json.Marshal(expiredPayload(w)); err == nil {
				_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
					TopicArn: aws.String(ch.Target),
					Message:  aws.String(string(data)),
				}, InARNRegion(ch.Target))
			}
		default:
			continue
		}
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// expiredSubject renders the notification subject line, e.g.
// "[lagotto] watch w-cf8e1a08 expired without acquiring g7e.*". Pure, so it's
// testable without SNS (snsClient is a concrete *sns.Client).
func expiredSubject(w *Watch) string {
	if w.ExpiredReason == ExpiredReasonUnacknowledged {
		return fmt.Sprintf("[lagotto] watch %s paused: not acknowledged (hunting %s)", w.WatchID, w.InstanceTypePattern)
	}
	return fmt.Sprintf("[lagotto] watch %s expired without acquiring %s", w.WatchID, w.InstanceTypePattern)
}

// expiredBody renders the notification body: what was hunted, for how long, and
// the one action that matters next (re-arm with a longer TTL, or widen the search).
func expiredBody(w *Watch) string {
	waited := "unknown"
	if d, ok := w.TimeToGiveUp(); ok {
		waited = FormatWait(d)
	}
	if w.ExpiredReason == ExpiredReasonUnacknowledged {
		return pausedBody(w, waited)
	}
	return fmt.Sprintf(`Your watch %s reached its TTL without ever finding capacity.

Pattern:  %s
Regions:  %v
Spot:     %v
Action:   %s (never taken — no capacity was found)
Created:  %s
Expired:  %s
Waited:   %s

Nothing was launched. The watch record is kept as status=expired so you can see
this happened, rather than having to infer it from silence.

To keep hunting, re-arm it with a longer TTL:

  lagotto extend %s --ttl 7d

If it never matched at all, consider widening --regions or the instance-type
pattern, or dropping --max-price.
`,
		w.WatchID, w.InstanceTypePattern, w.Regions, w.Spot, w.Action,
		w.CreatedAt.Format(time.RFC3339), w.UpdatedAt.Format(time.RFC3339), waited,
		w.WatchID)
}

// pausedBody is the expiry body for an until-landed watch whose acknowledgement
// deadline passed (#239). It is not a give-up: the watch is paused, and one
// command resumes it.
func pausedBody(w *Watch, waited string) string {
	return fmt.Sprintf(`Your until-landed watch %s has paused: nobody acknowledged it before its
deadline, so lagotto stopped hunting rather than keep trying to launch
unattended.

Pattern:  %s
Regions:  %v
Spot:     %v
Action:   %s (never landed)
Created:  %s
Paused:   %s
Hunted:   %s

Nothing was launched. To resume the hunt (and move the deadline forward):

  lagotto ack %s

To stop for good, cancel it:

  lagotto cancel %s
`,
		w.WatchID, w.InstanceTypePattern, w.Regions, w.Spot, w.Action,
		w.CreatedAt.Format(time.RFC3339), w.UpdatedAt.Format(time.RFC3339), waited,
		w.WatchID, w.WatchID)
}

// expiredPayload is the machine-readable expiry event. Like the quota-cap
// payload, the "event" key is new to THIS payload only — the existing match
// payload is left untouched, so no existing webhook consumer changes shape.
func expiredPayload(w *Watch) map[string]interface{} {
	p := map[string]interface{}{
		"watch_id": w.WatchID,
		"event":    "expired",
		"pattern":  w.InstanceTypePattern,
		"regions":  w.Regions,
		"action":   string(w.Action),
		"acquired": false,
	}
	if d, ok := w.TimeToGiveUp(); ok {
		p["time_to_give_up_seconds"] = d.Seconds()
	}
	// New keys only on an until-landed pause (#239), so a TTL expiry's payload
	// keeps its shape.
	if w.ExpiredReason != "" {
		p["expired_reason"] = w.ExpiredReason
	}
	if w.UntilLanded {
		p["until_landed"] = true
	}
	return p
}

// sendExpiredWebhook posts the expiry event to a webhook target.
func (n *Notifier) sendExpiredWebhook(ctx context.Context, url string, w *Watch) error {
	data, err := json.Marshal(expiredPayload(w))
	if err != nil {
		return fmt.Errorf("marshal expiry webhook payload: %w", err)
	}
	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (n *Notifier) sendEmail(ctx context.Context, email string, w *Watch, m *MatchResult) error {
	if normalizeService(w.Service) == ServiceSageMaker {
		subject, body := sageMakerEmailSubjectBody(w, m)
		return n.publishEmail(ctx, email, subject, body)
	}

	subject, body := matchEmailSubjectBody(w, m)
	return n.publishEmail(ctx, email, subject, body)
}

// sageMakerLaunched is the one SageMaker outcome in which the user's job got
// capacity (SageMakerLauncher.check: InProgress/Completed/...).
const sageMakerLaunched = "sagemaker_launched"

// sageMakerEmailSubjectBody is the SageMaker match email's text. Its wording
// follows the outcome (#212): Notify is called for terminal failures too, and
// the email used to say "job launched ... has been provisioned" regardless,
// so a user whose CreateTrainingJob was rejected was told it ran.
func sageMakerEmailSubjectBody(w *Watch, m *MatchResult) (string, string) {
	details := fmt.Sprintf(`Instance Type: %s
Region: %s
Matched At: %s
Watch Pattern: %s
Action Taken: %s
`,
		m.InstanceType, m.Region,
		m.MatchedAt.Format(time.RFC3339), w.InstanceTypePattern, m.ActionTaken,
	)
	if m.InstanceID != "" {
		details += fmt.Sprintf("SageMaker Job ARN: %s\n", m.InstanceID)
	}

	if m.ActionTaken == sageMakerLaunched {
		subject := fmt.Sprintf("[lagotto] SageMaker job launched for %s in %s", m.InstanceType, m.Region)
		return subject, fmt.Sprintf("Your SageMaker job for watch %s has been provisioned.\n\n", w.WatchID) + details
	}

	// Anything else that reaches Notify is final and is not a launch: the
	// poller has stopped the watch as failed.
	var why string
	switch m.ActionTaken {
	case "sagemaker_submit_failed":
		why = "SageMaker rejected the job when lagotto submitted it (CreateTrainingJob failed: for example an invalid RoleArn, a validation error in the job definition, or a quota limit). Retrying would not help."
	case "sagemaker_failed":
		why = "The job was submitted but failed for a reason other than capacity, so lagotto did not resubmit it."
	default:
		why = "lagotto could not submit the job (for example the watch has no usable job definition)."
	}
	subject := fmt.Sprintf("[lagotto] SageMaker job NOT launched for %s in %s: watch stopped", m.InstanceType, m.Region)
	body := fmt.Sprintf(`Your SageMaker job for watch %s was not provisioned, and the watch has stopped (status: failed).

%s
Fix the job definition (--sagemaker-config) and create a new watch.

`, w.WatchID, why) + details
	return subject, body
}

// matchEmailSubjectBody is the EC2 match email's text, split out so the copy is
// tested without SNS (snsClient is a concrete *sns.Client).
func matchEmailSubjectBody(w *Watch, m *MatchResult) (string, string) {
	spotLabel := "On-Demand"
	if m.IsSpot {
		spotLabel = "Spot"
	}
	subject := fmt.Sprintf("[lagotto] %s capacity found in %s", m.InstanceType, m.Region)
	body := fmt.Sprintf(`Capacity found for your watch %s

Instance Type: %s
Region: %s
Availability Zone: %s
Price: $%.4f/hr (%s)
Matched At: %s

Watch Pattern: %s
Action Taken: %s
`,
		w.WatchID,
		m.InstanceType,
		m.Region,
		m.AvailabilityZone,
		m.Price, spotLabel,
		m.MatchedAt.Format(time.RFC3339),
		w.InstanceTypePattern,
		m.ActionTaken,
	)

	if m.InstanceID != "" {
		body += fmt.Sprintf("Instance ID: %s\n", m.InstanceID)
	}
	// A hold is useless to the person notified without the reservation id: it is
	// a TARGETED reservation, so only a launch that names it can use it (#203).
	if m.ReservationID != "" {
		body += fmt.Sprintf(`Capacity Reservation: %s in %s
  Targeted: only a launch that names it can use it, e.g.
    %s
  It bills at the On-Demand rate until it expires; release it early with: lagotto cancel %s
`, m.ReservationID, m.AvailabilityZone, HoldLaunchCommandFor(w, m), w.WatchID)
	}
	return subject, body
}

func (n *Notifier) sendWebhook(ctx context.Context, url string, w *Watch, m *MatchResult) error {
	data, err := json.Marshal(matchWebhookPayload(w, m))
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// addReservation puts a hold's reservation id and AZ on a match payload (#203).
// The AZ is the matched AZ: Holder.Hold creates the reservation there.
func addReservation(payload map[string]interface{}, m *MatchResult) {
	if m.ReservationID == "" {
		return
	}
	payload["reservation_id"] = m.ReservationID
	payload["reservation_availability_zone"] = m.AvailabilityZone
}

// matchWebhookPayload is the JSON body of a match webhook.
func matchWebhookPayload(w *Watch, m *MatchResult) map[string]interface{} {
	payload := map[string]interface{}{
		"watch_id":          w.WatchID,
		"instance_type":     m.InstanceType,
		"region":            m.Region,
		"availability_zone": m.AvailabilityZone,
		"price":             m.Price,
		"is_spot":           m.IsSpot,
		"matched_at":        m.MatchedAt.Format(time.RFC3339),
		"action_taken":      m.ActionTaken,
		"pattern":           w.InstanceTypePattern,
	}
	if normalizeService(w.Service) == ServiceSageMaker {
		payload["service"] = string(ServiceSageMaker)
	}
	if m.InstanceID != "" {
		payload["instance_id"] = m.InstanceID
	}
	addReservation(payload, m)
	return payload
}

// matchSNSPayload is the JSON message published to a user's own SNS topic.
func matchSNSPayload(w *Watch, m *MatchResult) map[string]interface{} {
	payload := map[string]interface{}{
		"watch_id":          w.WatchID,
		"instance_type":     m.InstanceType,
		"region":            m.Region,
		"availability_zone": m.AvailabilityZone,
		"price":             m.Price,
		"is_spot":           m.IsSpot,
		"matched_at":        m.MatchedAt.Format(time.RFC3339),
		"action_taken":      m.ActionTaken,
	}
	if m.InstanceID != "" {
		payload["instance_id"] = m.InstanceID
	}
	addReservation(payload, m)
	return payload
}

func (n *Notifier) sendSNS(ctx context.Context, topicArn string, w *Watch, m *MatchResult) error {
	data, err := json.Marshal(matchSNSPayload(w, m))
	if err != nil {
		return fmt.Errorf("marshal SNS message: %w", err)
	}

	_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(string(data)),
	}, InARNRegion(topicArn))
	return err
}

// NotifyUnsupported tells the user that the poller retired a watch because its
// service/action combination can never fire (#195): a notify-only SageMaker
// watch stored before `watch` refused the combination. A sibling of Notify, not
// a synthesized MatchResult, for the same reason as NotifyExpired: no capacity
// was found, and the SageMaker match email would say a job was provisioned.
// Same channel fan-out and webhook re-validation as Notify. Once is structural:
// the poller marks the watch failed first, and only active watches are loaded.
func (n *Notifier) NotifyUnsupported(ctx context.Context, w *Watch, reason string) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}
	subject := unsupportedSubject(w)
	body := unsupportedBody(w, reason)
	payload, err := json.Marshal(unsupportedPayload(w, reason))
	if err != nil {
		return fmt.Errorf("marshal retirement payload: %w", err)
	}

	var lastErr error
	for _, ch := range w.NotifyChannels {
		var err error
		switch ch.Type {
		case "email":
			err = n.publishEmail(ctx, ch.Target, subject, body)
		case "webhook":
			if verr := ValidateWebhookURL(ch.Target); verr != nil {
				err = fmt.Errorf("blocked unsafe webhook URL: %w", verr)
			} else {
				err = n.postWebhookJSON(ch.Target, payload)
			}
		case "sns":
			_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
				TopicArn: aws.String(ch.Target),
				Message:  aws.String(string(payload)),
			}, InARNRegion(ch.Target))
		default:
			continue
		}
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// unsupportedSubject renders the retirement subject line. Pure, for tests.
func unsupportedSubject(w *Watch) string {
	return fmt.Sprintf("[lagotto] watch %s stopped: it could never fire", w.WatchID)
}

// unsupportedBody renders the retirement body: what was stopped, why, and what
// to run instead.
func unsupportedBody(w *Watch, reason string) string {
	return fmt.Sprintf(`Your watch %s has been stopped (status=failed) because it could never fire.

Pattern:  %s
Service:  %s
Action:   %s

%s.

Nothing was submitted or launched. Create a new watch with a mode that works.
`,
		w.WatchID, w.InstanceTypePattern, normalizeService(w.Service), w.Action, reason)
}

// unsupportedPayload is the machine-readable retirement event. Like the expiry
// and quota-cap payloads, the "event" key is new to THIS payload only.
func unsupportedPayload(w *Watch, reason string) map[string]interface{} {
	return map[string]interface{}{
		"watch_id": w.WatchID,
		"event":    "unsupported",
		"pattern":  w.InstanceTypePattern,
		"service":  string(normalizeService(w.Service)),
		"action":   string(w.Action),
		"reason":   reason,
	}
}

// postWebhookJSON posts an already-marshaled event to a webhook target.
func (n *Notifier) postWebhookJSON(url string, data []byte) error {
	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
