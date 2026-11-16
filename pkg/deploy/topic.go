package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/spore-host/libs/tags/snstags"

	"github.com/spore-host/lagotto/pkg/managedtags"
)

const (
	// alertsTopicDisplayName is the topic's DisplayName — what shows up as the
	// sender on an SMS/email subscription.
	alertsTopicDisplayName = "Lagotto Capacity Alerts"
	// alertsTopicKMSKey is the AWS-managed SNS key. lagotto deliberately uses the
	// managed key rather than a CMK (matching the template's trivy:ignore note):
	// the topic carries capacity notifications, not customer data.
	alertsTopicKMSKey = "alias/aws/sns"
)

// EnsureAlertsTopic get-or-creates the lagotto-capacity-alerts SNS topic in
// region and converges its attributes and tags, returning its ARN. Idempotent:
// safe to call on every `lagotto deploy`.
//
// env is the *tag* value — production/staging/development, from `--environment`.
// Do not confuse it with a Lambda Environment.Variables map; see
// EnsurePollerFunction's note.
//
// Four steps, in this order:
//
//  1. Probe the topic (GetTopicAttributes on its fixed ARN). SNS answers a
//     CreateTopic for an existing topic with InvalidParameter when the tags it
//     carries differ from the topic's, and the spore.host tags include the time
//     lagotto first tagged it (lagotto#324), so CreateTopic is called only for
//     a topic that is not there.
//  2. CreateTopic, for an absent topic, with the attributes and every tag set
//     at create time: a brand-new topic is never briefly unencrypted or
//     untagged. A topic created by a racing deploy between the probe and the
//     create is returned by CreateTopic all the same.
//  3. Always converge the attributes: SetTopicAttributes only the keys whose
//     value differs. This repairs a topic left behind by an older (or
//     hand-rolled) path, and it is also the only mechanism a test can observe —
//     substrate's createTopic reads a flat Params["DisplayName"] that the SDK
//     never sends, so create-time attributes are invisible to it. In steady
//     state this is zero writes.
//  4. Converge the tags: read them, and TagResource (an upsert) only the ones
//     missing or different, so re-running with a different --environment
//     retags an existing topic and a topic an older lagotto created gains the
//     spore.host tags, while spore-host:created and spore-host:owner keep their
//     first values (managedtags.Converge).
func (d *Deployer) EnsureAlertsTopic(ctx context.Context, env, region, accountID string) (string, error) {
	if env == "" {
		env = "production"
	}
	want := d.alertsTopicTags(env, region)

	arn := AlertsTopicARN(region, accountID)
	cur, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}, inRegion(region))
	switch {
	case err == nil:
		if a := cur.Attributes["TopicArn"]; a != "" {
			arn = a
		}
	case isSNSNotFound(err):
		out, cErr := d.sns.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(AlertsTopicName),
			Attributes: map[string]string{
				"DisplayName":    alertsTopicDisplayName,
				"KmsMasterKeyId": alertsTopicKMSKey,
			},
			Tags: snstags.FromMap(want),
		}, inRegion(region))
		if cErr != nil {
			return "", fmt.Errorf("create SNS topic %s: %w", AlertsTopicName, cErr)
		}
		arn = aws.ToString(out.TopicArn)
		if arn == "" {
			return "", fmt.Errorf("create SNS topic %s: no ARN returned", AlertsTopicName)
		}
		cur = nil // read the new topic's attributes below
	default:
		return "", fmt.Errorf("check whether SNS topic %s exists: %w", AlertsTopicName, err)
	}

	if err := d.convergeTopicAttributes(ctx, arn, region, cur); err != nil {
		return "", err
	}
	if err := d.convergeTopicTags(ctx, arn, region, want); err != nil {
		return "", err
	}
	return arn, nil
}

// alertsTopicTags are the topic's tags: the template's three, plus the
// spore.host service tags (lagotto#324).
func (d *Deployer) alertsTopicTags(env, region string) map[string]string {
	return managedtags.With(map[string]string{
		"Environment": env,
		"Application": "lagotto",
		"Component":   "alerts",
	}, d.serviceTags(region, managedtags.ComponentAlerts))
}

// convergeTopicTags writes the desired tags the topic lacks or has with
// another value, and nothing when it has them all.
func (d *Deployer) convergeTopicTags(ctx context.Context, arn, region string, want map[string]string) error {
	cur, err := d.sns.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(arn)}, inRegion(region))
	if err != nil {
		return fmt.Errorf("read tags of SNS topic %s: %w", arn, err)
	}
	add := managedtags.Converge(snstags.ToMap(cur.Tags), want)
	if len(add) == 0 {
		return nil
	}
	if _, err := d.sns.TagResource(ctx, &sns.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags:        snstags.FromMap(add),
	}, inRegion(region)); err != nil {
		return fmt.Errorf("tag SNS topic %s: %w", arn, err)
	}
	return nil
}

// convergeTopicAttributes issues a SetTopicAttributes for each desired attribute
// whose current value differs — and none when they all match.
//
// cur is the topic's attributes when the caller has just read them, nil to
// read them here.
func (d *Deployer) convergeTopicAttributes(ctx context.Context, arn, region string, cur *sns.GetTopicAttributesOutput) error {
	if cur == nil {
		var err error
		if cur, err = d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}, inRegion(region)); err != nil {
			return fmt.Errorf("read attributes of SNS topic %s: %w", arn, err)
		}
	}
	// Iterate a fixed slice, not a map, so the call order is deterministic and a
	// test can assert on it.
	desired := []struct{ key, value string }{
		{"DisplayName", alertsTopicDisplayName},
		{"KmsMasterKeyId", alertsTopicKMSKey},
	}
	for _, want := range desired {
		if cur.Attributes[want.key] == want.value {
			continue
		}
		if _, err := d.sns.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
			TopicArn:       aws.String(arn),
			AttributeName:  aws.String(want.key),
			AttributeValue: aws.String(want.value),
		}); err != nil {
			return fmt.Errorf("set %s on SNS topic %s: %w", want.key, arn, err)
		}
	}
	return nil
}

// alertsTopicExists reports whether the capacity-alerts topic is there, so
// Teardown can report what it actually deleted (see pollerFunctionExists).
//
// This one needs its own probe more than the others do: SNS DeleteTopic on a
// missing topic succeeds silently, so the delete call itself carries no
// create-vs-absent signal at all.
func (d *Deployer) alertsTopicExists(ctx context.Context, region, accountID string) (bool, error) {
	arn := AlertsTopicARN(region, accountID)
	_, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}, inRegion(region))
	if err == nil {
		return true, nil
	}
	if isSNSNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("check whether SNS topic %s exists: %w", arn, err)
}

// deleteAlertsTopic removes the capacity-alerts topic, tolerating an already-gone
// topic.
func (d *Deployer) deleteAlertsTopic(ctx context.Context, region, accountID string) error {
	arn := AlertsTopicARN(region, accountID)
	_, err := d.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)}, inRegion(region))
	if err == nil || isSNSNotFound(err) {
		return nil
	}
	return fmt.Errorf("delete SNS topic %s: %w", arn, err)
}

// isSNSNotFound reports whether err means "that topic isn't there". SNS answers
// with NotFoundException for a missing topic; some endpoints use the bare
// "NotFound" query-protocol code, which the SDK surfaces as a generic API error,
// so fall back to matching the code.
func isSNSNotFound(err error) bool {
	var nf *snstypes.NotFoundException
	if errors.As(err, &nf) {
		return true
	}
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NotFoundException", "ResourceNotFoundException":
			return true
		}
	}
	return false
}

// inRegion makes one SNS call in region, the region of the topic ARN it names,
// whatever region the client was built for (#240): SNS rejects another
// region's topic ARN with "InvalidParameter: TopicArn".
func inRegion(region string) func(*sns.Options) {
	return func(o *sns.Options) {
		if region != "" {
			o.Region = region
		}
	}
}
