package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// Email delivery (#240). An email channel is delivered through the shared
// alerts topic (lagotto-capacity-alerts), and SNS has no per-message
// recipient: every subscription on a topic receives every message, unless it
// carries a filter policy. So each address gets its OWN subscription, with a
// filter policy on the EmailAttribute message attribute that matches only its
// own address, and every email publish sets that attribute to the watch's
// address. A subscriber then receives only the events of the watches that name
// it, never another user's.
//
// The lifecycle is:
//
//   - `lagotto watch` (and `extend` when it reactivates a watch) subscribes the
//     address, reusing an existing subscription, never adding a second one.
//     SNS sends a confirmation email; until it is clicked the subscription is
//     PendingConfirmation and nothing is delivered.
//   - the deleter is Reconcile: it unsubscribes every lagotto-managed
//     subscription whose address no active watch names. The poller runs it at
//     the start of every sweep, `lagotto cancel` runs it for the cancelled
//     watch's addresses, and `lagotto notify-gc` runs it on demand.
//   - a PendingConfirmation subscription has no ARN to unsubscribe by. SNS
//     deletes an unconfirmed subscription on its own within 3 days, so
//     Reconcile reports it and leaves it.

// EmailAttribute is the SNS message attribute an email publish carries, and
// the key of the filter policy on each address's subscription.
const EmailAttribute = "lagotto_email"

// pendingConfirmation is the SubscriptionArn SNS reports for a subscription
// that has not been confirmed yet.
const pendingConfirmation = "PendingConfirmation"

// SubscriptionAPI is the slice of SNS the email-subscription lifecycle uses.
// *sns.Client satisfies it; tests supply a fake.
type SubscriptionAPI interface {
	Subscribe(ctx context.Context, in *sns.SubscribeInput, optFns ...func(*sns.Options)) (*sns.SubscribeOutput, error)
	Unsubscribe(ctx context.Context, in *sns.UnsubscribeInput, optFns ...func(*sns.Options)) (*sns.UnsubscribeOutput, error)
	ListSubscriptionsByTopic(ctx context.Context, in *sns.ListSubscriptionsByTopicInput, optFns ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error)
	GetSubscriptionAttributes(ctx context.Context, in *sns.GetSubscriptionAttributesInput, optFns ...func(*sns.Options)) (*sns.GetSubscriptionAttributesOutput, error)
}

var _ SubscriptionAPI = (*sns.Client)(nil)

// NormalizeEmail is the canonical form of an address: trimmed and lower-cased.
// The filter policy and the message attribute are both built from it, so
// "Alice@Example.com" on one watch and "alice@example.com" on another are the
// same subscriber.
func NormalizeEmail(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// ValidateEmail rejects a target SNS could not subscribe: it needs one "@"
// with something on both sides and no whitespace. SNS does the real check.
func ValidateEmail(addr string) error {
	a := strings.TrimSpace(addr)
	at := strings.Index(a, "@")
	if a == "" || at <= 0 || at != strings.LastIndex(a, "@") || at == len(a)-1 || strings.ContainsAny(a, " \t\r\n") {
		return fmt.Errorf("invalid email address %q", addr)
	}
	return nil
}

// EmailFilterPolicy is the filter policy on addr's subscription: deliver only
// messages whose EmailAttribute is addr.
func EmailFilterPolicy(addr string) string {
	b, _ := json.Marshal(map[string][]string{EmailAttribute: {NormalizeEmail(addr)}})
	return string(b)
}

// emailMessageAttributes is what an email publish to addr carries.
func emailMessageAttributes(addr string) map[string]snstypes.MessageAttributeValue {
	return map[string]snstypes.MessageAttributeValue{
		EmailAttribute: {DataType: aws.String("String"), StringValue: aws.String(NormalizeEmail(addr))},
	}
}

// managedFilterAddress returns the address a lagotto filter policy selects,
// or "" when policy is not one lagotto wrote (absent, or any other shape).
func managedFilterAddress(policy string) string {
	if strings.TrimSpace(policy) == "" {
		return ""
	}
	var p map[string][]string
	if err := json.Unmarshal([]byte(policy), &p); err != nil || len(p) != 1 {
		return ""
	}
	vals := p[EmailAttribute]
	if len(vals) != 1 {
		return ""
	}
	return vals[0]
}

// SubscriptionState is an address's delivery state on the alerts topic.
type SubscriptionState string

const (
	// SubConfirmed: the address confirmed its subscription and receives email.
	SubConfirmed SubscriptionState = "confirmed"
	// SubPending: SNS sent a confirmation email that has not been clicked yet;
	// nothing is delivered until it is.
	SubPending SubscriptionState = "pending_confirmation"
	// SubNone: the address has no subscription, so nothing reaches it.
	SubNone SubscriptionState = "not_subscribed"
	// SubNoTopic: there is no alerts topic to subscribe to.
	SubNoTopic SubscriptionState = "no_alerts_topic"
	// SubUnknown: the subscriptions could not be read.
	SubUnknown SubscriptionState = "unknown"
)

// Describe is the human form of a state, as list/status/watch print it.
func (s SubscriptionState) Describe() string {
	switch s {
	case SubConfirmed:
		return "confirmed"
	case SubPending:
		return "pending confirmation: check your inbox and click the link; nothing is delivered until you do"
	case SubNone:
		return "NOT subscribed: nothing is delivered (run `lagotto notify-gc --resubscribe`)"
	case SubNoTopic:
		return "NOT delivered: no alerts topic (run `lagotto deploy`)"
	default:
		return "unknown (could not read the alerts topic's subscriptions)"
	}
}

// EmailSubscription is one email subscription on the alerts topic.
type EmailSubscription struct {
	// Address is the normalized endpoint.
	Address string `json:"address"`
	// SubscriptionArn is the ARN, or "PendingConfirmation".
	SubscriptionArn string `json:"subscription_arn"`
	// State is SubConfirmed or SubPending.
	State SubscriptionState `json:"state"`
	// Inspected is true when the filter policy was read (only possible for a
	// confirmed subscription). Managed and Unfiltered mean nothing otherwise.
	Inspected bool `json:"inspected,omitempty"`
	// Managed: the subscription carries lagotto's filter policy for Address,
	// so lagotto created it and may delete it.
	Managed bool `json:"managed,omitempty"`
	// Unfiltered: the subscription has no filter policy at all, so it receives
	// EVERY message on the topic, every user's email events included.
	Unfiltered bool `json:"unfiltered,omitempty"`
}

// EmailSubscriptions manages the per-address subscriptions on one alerts topic.
type EmailSubscriptions struct {
	api      SubscriptionAPI
	topicArn string
}

// NewEmailSubscriptions returns the manager for topicArn.
func NewEmailSubscriptions(api SubscriptionAPI, topicArn string) *EmailSubscriptions {
	return &EmailSubscriptions{api: api, topicArn: topicArn}
}

// TopicArn is the alerts topic this manager works on.
func (e *EmailSubscriptions) TopicArn() string { return e.topicArn }

// List returns every email subscription on the topic. With inspect, each
// confirmed one's filter policy is read too (sns:GetSubscriptionAttributes);
// a pending one has no ARN to read it by.
func (e *EmailSubscriptions) List(ctx context.Context, inspect bool) ([]EmailSubscription, error) {
	var out []EmailSubscription
	var token *string
	for {
		page, err := e.api.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn:  aws.String(e.topicArn),
			NextToken: token,
		}, InARNRegion(e.topicArn))
		if err != nil {
			return nil, fmt.Errorf("list subscriptions of %s: %w", e.topicArn, err)
		}
		for _, s := range page.Subscriptions {
			if aws.ToString(s.Protocol) != "email" {
				continue
			}
			sub := EmailSubscription{
				Address:         NormalizeEmail(aws.ToString(s.Endpoint)),
				SubscriptionArn: aws.ToString(s.SubscriptionArn),
				State:           SubConfirmed,
			}
			if sub.SubscriptionArn == pendingConfirmation || sub.SubscriptionArn == "" {
				sub.State = SubPending
			}
			out = append(out, sub)
		}
		token = page.NextToken
		if aws.ToString(token) == "" {
			break
		}
	}
	if inspect {
		for i := range out {
			if err := e.inspect(ctx, &out[i]); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// inspect reads a confirmed subscription's filter policy.
func (e *EmailSubscriptions) inspect(ctx context.Context, sub *EmailSubscription) error {
	if sub.State != SubConfirmed {
		return nil
	}
	attrs, err := e.api.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(sub.SubscriptionArn),
	}, InARNRegion(e.topicArn))
	if err != nil {
		return fmt.Errorf("read subscription %s: %w", sub.SubscriptionArn, err)
	}
	policy := attrs.Attributes["FilterPolicy"]
	sub.Inspected = true
	sub.Unfiltered = strings.TrimSpace(policy) == ""
	sub.Managed = managedFilterAddress(policy) == sub.Address &&
		attrs.Attributes["FilterPolicyScope"] != "MessageBody"
	return nil
}

// States maps each address with a subscription to its state. An address that
// is subscribed more than once (by hand, say) reports its best state.
func (e *EmailSubscriptions) States(ctx context.Context) (map[string]SubscriptionState, error) {
	subs, err := e.List(ctx, false)
	if err != nil {
		return nil, err
	}
	states := map[string]SubscriptionState{}
	for _, s := range subs {
		if states[s.Address] != SubConfirmed {
			states[s.Address] = s.State
		}
	}
	return states, nil
}

// EnsureResult is what Ensure found or did for one address.
type EnsureResult struct {
	Address string            `json:"address"`
	State   SubscriptionState `json:"state"`
	// Created: Ensure subscribed the address just now, so SNS sent it a
	// confirmation email.
	Created bool `json:"created,omitempty"`
	// Unfiltered: the address already has a subscription with no filter
	// policy, which receives every message on the topic. Ensure reuses it and
	// leaves it alone (lagotto did not create it); doctor reports it.
	Unfiltered bool `json:"unfiltered,omitempty"`
}

// Ensure makes sure addr has a subscription on the topic, creating one with
// lagotto's filter policy only when it has none. It never creates a second
// subscription for an address: SNS would refuse one with different
// attributes anyway ("Subscription already exists with different attributes").
func (e *EmailSubscriptions) Ensure(ctx context.Context, addr string) (EnsureResult, error) {
	addr = NormalizeEmail(addr)
	res := EnsureResult{Address: addr}
	subs, err := e.List(ctx, false)
	if err != nil {
		return res, err
	}
	var existing *EmailSubscription
	for i := range subs {
		if subs[i].Address != addr {
			continue
		}
		if existing == nil || subs[i].State == SubConfirmed {
			existing = &subs[i]
		}
	}
	if existing != nil {
		res.State = existing.State
		if existing.State == SubConfirmed {
			if err := e.inspect(ctx, existing); err != nil {
				return res, err
			}
			res.Unfiltered = existing.Unfiltered
		}
		return res, nil
	}
	if _, err := e.api.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(e.topicArn),
		Protocol: aws.String("email"),
		Endpoint: aws.String(addr),
		Attributes: map[string]string{
			"FilterPolicy":      EmailFilterPolicy(addr),
			"FilterPolicyScope": "MessageAttributes",
		},
		ReturnSubscriptionArn: true,
	}, InARNRegion(e.topicArn)); err != nil {
		return res, fmt.Errorf("subscribe %s to %s: %w", addr, e.topicArn, err)
	}
	res.State, res.Created = SubPending, true
	return res, nil
}

// ReconcileResult is what Reconcile did.
type ReconcileResult struct {
	// Removed are the addresses unsubscribed.
	Removed []string `json:"removed,omitempty"`
	// PendingUnused are addresses no active watch names whose subscription is
	// still PendingConfirmation: it has no ARN to unsubscribe by, and SNS
	// deletes it on its own within 3 days.
	PendingUnused []string `json:"pending_unused,omitempty"`
	// InUse are the managed addresses kept because an active watch names them.
	InUse []string `json:"in_use,omitempty"`
}

// EmailAddresses returns the normalized email addresses w's channels name.
func EmailAddresses(w *Watch) []string {
	var out []string
	seen := map[string]bool{}
	for _, ch := range w.NotifyChannels {
		if ch.Type != "email" {
			continue
		}
		a := NormalizeEmail(ch.Target)
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// Reconcile is the deleter: it unsubscribes every subscription lagotto
// created (it carries lagotto's filter policy for its own address) whose
// address no watch in active names. active MUST be every active watch the
// topic serves, unfiltered: an address used by a watch missing from it would
// be unsubscribed. When only is non-empty, just those addresses are
// considered (cancel uses it for the cancelled watch's addresses).
//
// Subscriptions lagotto did not create (no filter policy, or another one) are
// never touched. The filter policy is read only for unused candidates, so in
// steady state Reconcile is one ListSubscriptionsByTopic call.
func (e *EmailSubscriptions) Reconcile(ctx context.Context, active []Watch, only []string) (ReconcileResult, error) {
	var res ReconcileResult
	inUse := map[string]bool{}
	for _, a := range activeEmailAddresses(active) {
		inUse[a] = true
	}
	scope := map[string]bool{}
	for _, a := range only {
		scope[NormalizeEmail(a)] = true
	}

	subs, err := e.List(ctx, false)
	if err != nil {
		return res, err
	}
	var errs []error
	for i := range subs {
		s := &subs[i]
		if len(scope) > 0 && !scope[s.Address] {
			continue
		}
		if inUse[s.Address] {
			res.InUse = append(res.InUse, s.Address)
			continue
		}
		if s.State == SubPending {
			res.PendingUnused = append(res.PendingUnused, s.Address)
			continue
		}
		if err := e.inspect(ctx, s); err != nil {
			errs = append(errs, err)
			continue
		}
		if !s.Managed {
			continue
		}
		if _, err := e.api.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(s.SubscriptionArn)}, InARNRegion(e.topicArn)); err != nil {
			errs = append(errs, fmt.Errorf("unsubscribe %s (%s): %w", s.Address, s.SubscriptionArn, err))
			continue
		}
		res.Removed = append(res.Removed, s.Address)
	}
	sort.Strings(res.Removed)
	sort.Strings(res.PendingUnused)
	sort.Strings(res.InUse)
	return res, errors.Join(errs...)
}

// IsTopicNotFound reports whether err means the alerts topic does not exist.
func IsTopicNotFound(err error) bool {
	var nf *snstypes.NotFoundException
	if errors.As(err, &nf) {
		return true
	}
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NotFoundException":
			return true
		}
	}
	return false
}

// EmailAudit is the email-delivery picture doctor reports (#240): the state
// of every address an active watch names, and the subscriptions nothing
// needs or that leak.
type EmailAudit struct {
	TopicArn string `json:"topic_arn"`
	// NoTopic: the alerts topic does not exist.
	NoTopic bool `json:"no_topic,omitempty"`
	// Confirmed, Pending and Missing partition the addresses active watches
	// name by their subscription state.
	Confirmed []string `json:"confirmed,omitempty"`
	Pending   []string `json:"pending,omitempty"`
	Missing   []string `json:"missing,omitempty"`
	// Unused are confirmed lagotto-managed subscriptions no active watch
	// names: `lagotto notify-gc` (or the poller's next sweep) removes them.
	Unused []string `json:"unused,omitempty"`
	// PendingUnused are pending subscriptions no active watch names. They have
	// no ARN to unsubscribe by; SNS deletes them within 3 days.
	PendingUnused []string `json:"pending_unused,omitempty"`
	// Unfiltered are confirmed email subscriptions with NO filter policy: each
	// receives every message on the topic, other users' email events included.
	Unfiltered []EmailSubscription `json:"unfiltered,omitempty"`
}

// Audit reads every email subscription on the topic (with filter policies)
// and compares them with the addresses the active watches name.
func (e *EmailSubscriptions) Audit(ctx context.Context, active []Watch) (EmailAudit, error) {
	audit := EmailAudit{TopicArn: e.topicArn}
	subs, err := e.List(ctx, true)
	if err != nil {
		if IsTopicNotFound(err) {
			audit.NoTopic = true
			audit.Missing = activeEmailAddresses(active)
			return audit, nil
		}
		return audit, err
	}
	best := map[string]SubscriptionState{}
	for _, s := range subs {
		if best[s.Address] != SubConfirmed {
			best[s.Address] = s.State
		}
	}
	inUse := map[string]bool{}
	for _, a := range activeEmailAddresses(active) {
		inUse[a] = true
		switch best[a] {
		case SubConfirmed:
			audit.Confirmed = append(audit.Confirmed, a)
		case SubPending:
			audit.Pending = append(audit.Pending, a)
		default:
			audit.Missing = append(audit.Missing, a)
		}
	}
	for _, s := range subs {
		if s.Unfiltered {
			audit.Unfiltered = append(audit.Unfiltered, s)
		}
		if inUse[s.Address] {
			continue
		}
		switch {
		case s.State == SubPending:
			audit.PendingUnused = append(audit.PendingUnused, s.Address)
		case s.Managed:
			audit.Unused = append(audit.Unused, s.Address)
		}
	}
	return audit, nil
}

// activeEmailAddresses is the sorted set of addresses active watches name.
func activeEmailAddresses(watches []Watch) []string {
	seen := map[string]bool{}
	var out []string
	for i := range watches {
		if watches[i].Status != "" && watches[i].Status != StatusActive {
			continue
		}
		for _, a := range EmailAddresses(&watches[i]) {
			if !seen[a] {
				seen[a] = true
				out = append(out, a)
			}
		}
	}
	sort.Strings(out)
	return out
}

// MissingEmailSubscriptions returns the addresses active watches name that
// have no subscription at all on the topic (never subscribed, or an
// unconfirmed one SNS has since deleted).
func (e *EmailSubscriptions) MissingEmailSubscriptions(ctx context.Context, active []Watch) ([]string, error) {
	states, err := e.States(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range activeEmailAddresses(active) {
		if _, ok := states[a]; !ok {
			out = append(out, a)
		}
	}
	return out, nil
}

// InARNRegion makes one SNS call in the region named in arn (a topic or
// subscription ARN), whatever region the client was built for. SNS rejects a
// topic ARN from another region with "InvalidParameter: TopicArn", and the
// alerts topic (LAGOTTO_SNS_TOPIC_ARN) or an sns: channel's own topic may sit
// in a region other than the CLI's or the poller's. An ARN with no region
// leaves the client's region alone.
func InARNRegion(arn string) func(*sns.Options) {
	region := ARNRegion(arn)
	return func(o *sns.Options) {
		if region != "" {
			o.Region = region
		}
	}
}

// ARNRegion returns the region field of an ARN, or "".
func ARNRegion(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" {
		return ""
	}
	return parts[3]
}
