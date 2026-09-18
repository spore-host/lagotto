package watcher

import (
	"bytes"
	"context"
	"encoding/json"
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
type Notifier struct {
	snsClient  *sns.Client
	httpClient *http.Client
	topicArn   string
}

// NewNotifier creates a Notifier. topicArn is the default SNS topic for email notifications.
func NewNotifier(cfg aws.Config, topicArn string) *Notifier {
	return &Notifier{
		snsClient:  sns.NewFromConfig(cfg),
		httpClient: newSafeHTTPClient(10 * time.Second),
		topicArn:   topicArn,
	}
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
			_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
				TopicArn: aws.String(n.topicArn),
				Subject:  aws.String(subject),
				Message:  aws.String(body),
			})
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
				})
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
func quotaCapSubject(w *Watch, running int) string {
	return fmt.Sprintf("[lagotto] fleet capped at %d/%d by EC2 quota", running, w.DesiredCount)
}

// quotaCapBody renders the notification body: what happened, what lagotto will
// keep doing (it stays active and keeps the running workers), and the reason
// string (which carries the family, the numbers when known, and the
// quota-increase hint).
func quotaCapBody(w *Watch, running int, reason string) string {
	return fmt.Sprintf(`Your fleet watch %s is capped by an EC2 service quota.

Running:  %d of %d requested workers
Pattern:  %s
Regions:  %v
Spot:     %v

%s

The watch stays ACTIVE and keeps its running workers: it maintains as many as the
quota allows, retries at most once every %s while capped, and picks up a granted
quota increase automatically.
`,
		w.WatchID, running, w.DesiredCount, w.InstanceTypePattern, w.Regions, w.Spot,
		reason, QuotaCapReprobeInterval)
}

// quotaCapPayload is the machine-readable quota-cap event. The "event" key is new
// to THIS payload only — the existing match payload is deliberately left
// untouched (no retrofitted "event":"match") so no existing webhook consumer
// changes shape.
func quotaCapPayload(w *Watch, running int, reason string) map[string]interface{} {
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

func (n *Notifier) sendEmail(ctx context.Context, email string, w *Watch, m *MatchResult) error {
	spotLabel := "On-Demand"
	if m.IsSpot {
		spotLabel = "Spot"
	}

	if normalizeService(w.Service) == ServiceSageMaker {
		// SageMaker: lagotto submitted the user's job and it got capacity.
		subject := fmt.Sprintf("[lagotto] SageMaker job launched for %s in %s", m.InstanceType, m.Region)
		body := fmt.Sprintf(`Your SageMaker job for watch %s has been provisioned.

Instance Type: %s
Region: %s
Matched At: %s
Watch Pattern: %s
Action Taken: %s
`,
			w.WatchID, m.InstanceType, m.Region,
			m.MatchedAt.Format(time.RFC3339), w.InstanceTypePattern, m.ActionTaken,
		)
		if m.InstanceID != "" {
			body += fmt.Sprintf("SageMaker Job ARN: %s\n", m.InstanceID)
		}
		_, err := n.snsClient.Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(n.topicArn),
			Subject:  aws.String(subject),
			Message:  aws.String(body),
		})
		return err
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

	_, err := n.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(n.topicArn),
		Subject:  aws.String(subject),
		Message:  aws.String(body),
	})
	return err
}

func (n *Notifier) sendWebhook(ctx context.Context, url string, w *Watch, m *MatchResult) error {
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

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("post to webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (n *Notifier) sendSNS(ctx context.Context, topicArn string, w *Watch, m *MatchResult) error {
	payload := map[string]interface{}{
		"watch_id":      w.WatchID,
		"instance_type": m.InstanceType,
		"region":        m.Region,
		"price":         m.Price,
		"is_spot":       m.IsSpot,
		"matched_at":    m.MatchedAt.Format(time.RFC3339),
		"action_taken":  m.ActionTaken,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal SNS message: %w", err)
	}

	_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(string(data)),
	})
	return err
}
