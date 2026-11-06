package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// FakeSNS is an in-memory SNS topic for the email-subscription tests (#240).
// It models what those tests depend on: email subscriptions that start
// PendingConfirmation (ListSubscriptionsByTopic reports their ARN as
// "PendingConfirmation", and they cannot be read or unsubscribed by ARN),
// SNS's refusal to subscribe one endpoint twice with different attributes,
// paginated listing, and filter-policy delivery (Deliveries) — the property
// that keeps one user's events out of another's inbox.
type FakeSNS struct {
	mu sync.Mutex
	// TopicArn is the one topic that exists; any other gets NotFound.
	TopicArn string
	// NoTopic makes every call answer NotFound, as with no `lagotto deploy`.
	NoTopic bool
	// PageSize bounds ListSubscriptionsByTopic pages (default 2), so paging is
	// exercised.
	PageSize int
	// UnsubscribeErr, when set, fails Unsubscribe for this endpoint.
	UnsubscribeErr map[string]error

	Subs       []*FakeSub
	Subscribes []*sns.SubscribeInput
	Unsubs     []string
	Published  []*sns.PublishInput
	nextID     int
}

// FakeSub is one subscription.
type FakeSub struct {
	Arn       string
	Protocol  string
	Endpoint  string
	Attrs     map[string]string
	Confirmed bool
}

// NewFakeSNS returns a fake with topicArn existing and no subscriptions.
func NewFakeSNS(topicArn string) *FakeSNS { return &FakeSNS{TopicArn: topicArn} }

func (f *FakeSNS) notFound(arn string) error {
	if f.NoTopic || (arn != f.TopicArn && !strings.HasPrefix(arn, f.TopicArn+":")) {
		return &snstypes.NotFoundException{Message: aws.String("Topic does not exist")}
	}
	return nil
}

// AddSub adds a subscription directly, as a user subscribing by hand would.
func (f *FakeSNS) AddSub(endpoint string, attrs map[string]string, confirmed bool) *FakeSub {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.add("email", endpoint, attrs, confirmed)
}

func (f *FakeSNS) add(protocol, endpoint string, attrs map[string]string, confirmed bool) *FakeSub {
	f.nextID++
	s := &FakeSub{Arn: fmt.Sprintf("%s:sub-%04d", f.TopicArn, f.nextID), Protocol: protocol,
		Endpoint: endpoint, Attrs: attrs, Confirmed: confirmed}
	f.Subs = append(f.Subs, s)
	return s
}

// Confirm marks endpoint's subscription confirmed, as clicking the link does.
func (f *FakeSNS) Confirm(endpoint string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.Subs {
		if s.Endpoint == endpoint {
			s.Confirmed = true
		}
	}
}

// Subscribe implements SNS Subscribe for the email protocol.
func (f *FakeSNS) Subscribe(_ context.Context, in *sns.SubscribeInput, _ ...func(*sns.Options)) (*sns.SubscribeOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.notFound(aws.ToString(in.TopicArn)); err != nil {
		return nil, err
	}
	f.Subscribes = append(f.Subscribes, in)
	for _, s := range f.Subs {
		if s.Protocol == aws.ToString(in.Protocol) && s.Endpoint == aws.ToString(in.Endpoint) {
			if fmt.Sprint(s.Attrs) != fmt.Sprint(in.Attributes) {
				return nil, &snstypes.InvalidParameterException{Message: aws.String("Invalid parameter: Attributes Reason: Subscription already exists with different attributes")}
			}
			return &sns.SubscribeOutput{SubscriptionArn: aws.String(s.Arn)}, nil
		}
	}
	s := f.add(aws.ToString(in.Protocol), aws.ToString(in.Endpoint), in.Attributes, false)
	return &sns.SubscribeOutput{SubscriptionArn: aws.String(s.Arn)}, nil
}

// Unsubscribe removes a confirmed subscription by ARN.
func (f *FakeSNS) Unsubscribe(_ context.Context, in *sns.UnsubscribeInput, _ ...func(*sns.Options)) (*sns.UnsubscribeOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	arn := aws.ToString(in.SubscriptionArn)
	if f.NoTopic {
		return nil, f.notFound(arn)
	}
	for i, s := range f.Subs {
		if s.Arn == arn && f.UnsubscribeErr[s.Endpoint] != nil {
			return nil, f.UnsubscribeErr[s.Endpoint]
		}
		if s.Arn == arn && s.Confirmed {
			f.Subs = append(f.Subs[:i], f.Subs[i+1:]...)
			f.Unsubs = append(f.Unsubs, s.Endpoint)
			return &sns.UnsubscribeOutput{}, nil
		}
	}
	return nil, &snstypes.InvalidParameterException{Message: aws.String("Invalid parameter: SubscriptionArn")}
}

// ListSubscriptionsByTopic pages through the topic's subscriptions.
func (f *FakeSNS) ListSubscriptionsByTopic(_ context.Context, in *sns.ListSubscriptionsByTopicInput, _ ...func(*sns.Options)) (*sns.ListSubscriptionsByTopicOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.notFound(aws.ToString(in.TopicArn)); err != nil {
		return nil, err
	}
	size := f.PageSize
	if size <= 0 {
		size = 2
	}
	start := 0
	if t := aws.ToString(in.NextToken); t != "" {
		_, _ = fmt.Sscanf(t, "%d", &start)
	}
	out := &sns.ListSubscriptionsByTopicOutput{}
	for i := start; i < len(f.Subs) && i < start+size; i++ {
		s := f.Subs[i]
		arn := s.Arn
		if !s.Confirmed {
			arn = "PendingConfirmation"
		}
		out.Subscriptions = append(out.Subscriptions, snstypes.Subscription{
			SubscriptionArn: aws.String(arn), Protocol: aws.String(s.Protocol),
			Endpoint: aws.String(s.Endpoint), TopicArn: aws.String(f.TopicArn),
		})
	}
	if start+size < len(f.Subs) {
		out.NextToken = aws.String(fmt.Sprintf("%d", start+size))
	}
	return out, nil
}

// GetSubscriptionAttributes reads a confirmed subscription's attributes.
func (f *FakeSNS) GetSubscriptionAttributes(_ context.Context, in *sns.GetSubscriptionAttributesInput, _ ...func(*sns.Options)) (*sns.GetSubscriptionAttributesOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.Subs {
		if s.Arn == aws.ToString(in.SubscriptionArn) {
			attrs := map[string]string{}
			for k, v := range s.Attrs {
				attrs[k] = v
			}
			return &sns.GetSubscriptionAttributesOutput{Attributes: attrs}, nil
		}
	}
	return nil, &snstypes.NotFoundException{Message: aws.String("Subscription does not exist")}
}

// Publish records a publish to the topic.
func (f *FakeSNS) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Published = append(f.Published, in)
	return &sns.PublishOutput{MessageId: aws.String("m")}, nil
}

// Deliveries returns the endpoints SNS would deliver in to: every CONFIRMED
// subscription on its topic whose filter policy (MessageAttributes scope)
// matches the message's attributes, or that has no filter policy at all.
// Only exact string matching is modelled, which is all lagotto's policy uses.
func (f *FakeSNS) Deliveries(in *sns.PublishInput) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if aws.ToString(in.TopicArn) != f.TopicArn {
		return nil
	}
	var out []string
	for _, s := range f.Subs {
		if !s.Confirmed {
			continue
		}
		policy := s.Attrs["FilterPolicy"]
		if policy == "" {
			out = append(out, s.Endpoint)
			continue
		}
		var p map[string][]string
		if err := json.Unmarshal([]byte(policy), &p); err != nil {
			continue
		}
		match := true
		for key, allowed := range p {
			attr, ok := in.MessageAttributes[key]
			if !ok || !contains(allowed, aws.ToString(attr.StringValue)) {
				match = false
				break
			}
		}
		if match {
			out = append(out, s.Endpoint)
		}
	}
	return out
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
