package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
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

// EnsureAlertsTopic get-or-creates the lagotto-capacity-alerts SNS topic and
// converges its attributes and tags, returning its ARN. Idempotent: safe to call
// on every `lagotto deploy`.
//
// env is the *tag* value — production/staging/development, from `--environment`.
// Do not confuse it with a Lambda Environment.Variables map; see
// EnsurePollerFunction's note.
//
// Three steps, in this order:
//
//  1. CreateTopic with the attributes set at create time. CreateTopic is
//     idempotent by name and returns the EXISTING ARN when the topic is already
//     there, so this never fails on a re-run. Setting KmsMasterKeyId in the
//     create means a brand-new topic is never briefly unencrypted.
//  2. Always converge: read GetTopicAttributes and SetTopicAttributes only the
//     keys whose value differs. This repairs a topic left behind by an older (or
//     hand-rolled) path, and it is also the only mechanism a test can observe —
//     substrate's createTopic reads a flat Params["DisplayName"] that the SDK
//     never sends, so create-time attributes are invisible to it. In steady
//     state this is one read and zero writes.
//  3. TagResource, an idempotent upsert, so re-running with a different
//     --environment actually retags an existing topic.
//
// There is deliberately no `created bool` return: CreateTopic gives no
// create-vs-already-existed discriminator, and adding a speculative
// GetTopicAttributes pre-check just to synthesize one would be an extra call
// that buys nothing.
func (d *Deployer) EnsureAlertsTopic(ctx context.Context, env string) (string, error) {
	if env == "" {
		env = "production"
	}
	out, err := d.sns.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(AlertsTopicName),
		Attributes: map[string]string{
			"DisplayName":    alertsTopicDisplayName,
			"KmsMasterKeyId": alertsTopicKMSKey,
		},
		Tags: alertsTopicTags(env),
	})
	if err != nil {
		return "", fmt.Errorf("create SNS topic %s: %w", AlertsTopicName, err)
	}
	arn := aws.ToString(out.TopicArn)
	if arn == "" {
		return "", fmt.Errorf("create SNS topic %s: no ARN returned", AlertsTopicName)
	}

	if err := d.convergeTopicAttributes(ctx, arn); err != nil {
		return "", err
	}

	if _, err := d.sns.TagResource(ctx, &sns.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags:        alertsTopicTags(env),
	}); err != nil {
		return "", fmt.Errorf("tag SNS topic %s: %w", arn, err)
	}
	return arn, nil
}

// alertsTopicTags mirrors the template's topic tags exactly.
func alertsTopicTags(env string) []snstypes.Tag {
	return []snstypes.Tag{
		{Key: aws.String("Environment"), Value: aws.String(env)},
		{Key: aws.String("Application"), Value: aws.String("lagotto")},
		{Key: aws.String("Component"), Value: aws.String("alerts")},
	}
}

// convergeTopicAttributes issues a SetTopicAttributes for each desired attribute
// whose current value differs — and none when they all match.
func (d *Deployer) convergeTopicAttributes(ctx context.Context, arn string) error {
	cur, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
	if err != nil {
		return fmt.Errorf("read attributes of SNS topic %s: %w", arn, err)
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
	_, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)})
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
	_, err := d.sns.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(arn)})
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
