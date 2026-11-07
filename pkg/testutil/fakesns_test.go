package testutil

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// The #240 tests trust FakeSNS's model of SNS; these pin that model.

const topic = "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts"

func filtered(addr string) map[string]string {
	return map[string]string{"FilterPolicy": `{"lagotto_email":["` + addr + `"]}`, "FilterPolicyScope": "MessageAttributes"}
}

func listAll(t *testing.T, f *FakeSNS) []snstypes.Subscription {
	t.Helper()
	var out []snstypes.Subscription
	var tok *string
	for {
		page, err := f.ListSubscriptionsByTopic(context.Background(), &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topic), NextToken: tok})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, page.Subscriptions...)
		if tok = page.NextToken; tok == nil {
			return out
		}
	}
}

func TestFakeSNS_SubscribePendingConfirmAndPaging(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSNS(topic)
	for _, a := range []string{"a@x", "b@x", "c@x"} {
		if _, err := f.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic), Protocol: aws.String("email"),
			Endpoint: aws.String(a), Attributes: filtered(a)}); err != nil {
			t.Fatal(err)
		}
	}
	// Same endpoint, same attributes: the existing subscription.
	if _, err := f.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic), Protocol: aws.String("email"),
		Endpoint: aws.String("a@x"), Attributes: filtered("a@x")}); err != nil || len(f.Subs) != 3 {
		t.Errorf("re-subscribe: err = %v, subs = %d", err, len(f.Subs))
	}
	// Same endpoint, different attributes: refused, as SNS does.
	var inv *snstypes.InvalidParameterException
	if _, err := f.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic), Protocol: aws.String("email"),
		Endpoint: aws.String("a@x")}); !errors.As(err, &inv) {
		t.Errorf("different attributes: err = %v, want InvalidParameter", err)
	}

	subs := listAll(t, f)
	if len(subs) != 3 {
		t.Fatalf("listed %d, want 3 across pages", len(subs))
	}
	for _, s := range subs {
		if aws.ToString(s.SubscriptionArn) != "PendingConfirmation" {
			t.Errorf("unconfirmed %s listed as %s", aws.ToString(s.Endpoint), aws.ToString(s.SubscriptionArn))
		}
	}
	// A pending subscription cannot be unsubscribed.
	if _, err := f.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(f.Subs[0].Arn)}); err == nil {
		t.Error("unsubscribed a pending subscription")
	}
	f.Confirm("a@x")
	if got := aws.ToString(listAll(t, f)[0].SubscriptionArn); got != f.Subs[0].Arn {
		t.Errorf("confirmed subscription listed as %q", got)
	}
	attrs, err := f.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: aws.String(f.Subs[0].Arn)})
	if err != nil || attrs.Attributes["FilterPolicy"] == "" {
		t.Errorf("GetSubscriptionAttributes: %v %v", attrs, err)
	}
	if _, err := f.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: aws.String("nope")}); err == nil {
		t.Error("read a subscription that does not exist")
	}
	if _, err := f.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(f.Subs[0].Arn)}); err != nil || len(f.Subs) != 2 {
		t.Errorf("unsubscribe confirmed: err = %v, subs = %d", err, len(f.Subs))
	}
	f.UnsubscribeErr = map[string]error{"b@x": errors.New("boom")}
	f.Confirm("b@x")
	if _, err := f.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(f.Subs[0].Arn)}); err == nil {
		t.Error("UnsubscribeErr was not returned")
	}
}

func TestFakeSNS_DeliveriesFollowFilterPolicy(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSNS(topic)
	f.AddSub("a@x", filtered("a@x"), true)
	f.AddSub("b@x", filtered("b@x"), true)
	f.AddSub("all@x", nil, true)
	f.AddSub("pending@x", filtered("a@x"), false)
	f.AddSub("bad@x", map[string]string{"FilterPolicy": "not json"}, true)

	msg := &sns.PublishInput{TopicArn: aws.String(topic), Message: aws.String("m"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{"lagotto_email": {DataType: aws.String("String"), StringValue: aws.String("a@x")}}}
	if _, err := f.Publish(ctx, msg); err != nil || len(f.Published) != 1 {
		t.Fatal("Publish not recorded")
	}
	if got := f.Deliveries(msg); !reflect.DeepEqual(got, []string{"a@x", "all@x"}) {
		t.Errorf("deliveries = %v, want [a@x all@x]", got)
	}
	bare := &sns.PublishInput{TopicArn: aws.String(topic), Message: aws.String("m")}
	if got := f.Deliveries(bare); !reflect.DeepEqual(got, []string{"all@x"}) {
		t.Errorf("unattributed deliveries = %v, want only the unfiltered subscriber", got)
	}
	if got := f.Deliveries(&sns.PublishInput{TopicArn: aws.String("other")}); got != nil {
		t.Errorf("another topic's message delivered to %v", got)
	}
}

func TestFakeSNS_NoTopic(t *testing.T) {
	ctx := context.Background()
	f := NewFakeSNS(topic)
	f.NoTopic = true
	var nf *snstypes.NotFoundException
	if _, err := f.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(topic)}); !errors.As(err, &nf) {
		t.Errorf("list: %v", err)
	}
	if _, err := f.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic)}); !errors.As(err, &nf) {
		t.Errorf("subscribe: %v", err)
	}
	if _, err := f.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: aws.String(topic + ":x")}); !errors.As(err, &nf) {
		t.Errorf("unsubscribe: %v", err)
	}
	f.NoTopic = false
	if _, err := f.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String("arn:aws:sns:us-west-2:1:other")}); !errors.As(err, &nf) {
		t.Errorf("subscribe to another topic: %v", err)
	}
}
