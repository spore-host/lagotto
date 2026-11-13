package deploy

// The poller's CloudWatch Logs log group (#305).
//
// Lambda writes to /aws/lambda/<function>, and creates that group itself on the
// first invocation when it does not exist. A group created that way has no
// owner: nothing in lagotto created it, so nothing deleted it, and every
// `deploy --teardown` left it behind (found in spore-host-dev on 2026-11-12,
// after two teardowns had both reported success).
//
// So `lagotto deploy` creates the group itself, before the function, tagged
// ManagedTagKey=ManagedTagValue and with a retention, and `deploy --teardown`
// deletes it after the function. A group that already exists (one Lambda
// created implicitly before #305) is adopted: tagged, and given the retention
// if it has none.
//
// Teardown deletes the group whether or not it carries the tag. The name is
// bound to the poller function, which teardown deletes unconditionally by its
// fixed name in the same step; an untagged group of that name is the pre-#305
// implicit one, which is exactly the leftover this exists to remove.

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	logstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// PollerLogGroupName is the log group the poller Lambda writes to: Lambda's
// fixed /aws/lambda/<function name> convention.
const PollerLogGroupName = "/aws/lambda/" + PollerFunctionName

// PollerLogRetentionDays is the retention deploy sets on the poller's log
// group. The poller logs one evaluation per watch per tick; a month is enough
// to debug a watch that missed, and keeps the group from growing forever.
const PollerLogRetentionDays int32 = 30

// logsAPI is the subset of *cloudwatchlogs.Client pkg/deploy calls.
type logsAPI interface {
	CreateLogGroup(ctx context.Context, in *cloudwatchlogs.CreateLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	DescribeLogGroups(ctx context.Context, in *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
	PutRetentionPolicy(ctx context.Context, in *cloudwatchlogs.PutRetentionPolicyInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutRetentionPolicyOutput, error)
	TagResource(ctx context.Context, in *cloudwatchlogs.TagResourceInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.TagResourceOutput, error)
	DeleteLogGroup(ctx context.Context, in *cloudwatchlogs.DeleteLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DeleteLogGroupOutput, error)
}

// PollerLogGroupARN returns the ARN of the poller's log group, in the form
// TagResource takes: no trailing ":*".
func PollerLogGroupARN(region, accountID string) string {
	return fmt.Sprintf("arn:aws:logs:%s:%s:log-group:%s", region, accountID, PollerLogGroupName)
}

// pollerLogGroupTags are the tags on the poller's log group: the managed tag
// that names lagotto as its owner, and the function's Application/Component
// pair, so the group sorts with the function in a tag-based view.
func pollerLogGroupTags() map[string]string {
	return map[string]string{
		ManagedTagKey: ManagedTagValue,
		"Application": "lagotto",
		"Component":   "capacity-poller",
	}
}

// ensurePollerLogGroup creates the poller's log group, tagged and with
// PollerLogRetentionDays, and returns what it did. An existing group is
// adopted: tagged (an idempotent upsert), and given the retention only when it
// has none, so a retention someone set on purpose is never shortened (a
// shorter retention deletes logs).
//
// Every failure is an error. A group deploy cannot create, tag or bound is one
// no lagotto command would own, which is the leak this exists to close.
func (d *Deployer) ensurePollerLogGroup(ctx context.Context, region, accountID string) (string, error) {
	_, err := d.logs.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(PollerLogGroupName),
		Tags:         pollerLogGroupTags(),
	}, logsInRegion(region))
	if err == nil {
		if err := d.putPollerLogRetention(ctx, region); err != nil {
			return "", err
		}
		return fmt.Sprintf("created, %d-day retention, tagged %s=%s", PollerLogRetentionDays, ManagedTagKey, ManagedTagValue), nil
	}
	var exists *logstypes.ResourceAlreadyExistsException
	if !errors.As(err, &exists) {
		return "", fmt.Errorf("create log group %s: %w", PollerLogGroupName, err)
	}

	if _, err := d.logs.TagResource(ctx, &cloudwatchlogs.TagResourceInput{
		ResourceArn: aws.String(PollerLogGroupARN(region, accountID)),
		Tags:        pollerLogGroupTags(),
	}, logsInRegion(region)); err != nil {
		return "", fmt.Errorf("tag existing log group %s %s=%s (without the tag nothing marks lagotto as its owner): %w",
			PollerLogGroupName, ManagedTagKey, ManagedTagValue, err)
	}
	retention, err := d.pollerLogRetention(ctx, region)
	if err != nil {
		return "", err
	}
	if retention != nil {
		return fmt.Sprintf("exists, %d-day retention kept, tagged %s=%s", *retention, ManagedTagKey, ManagedTagValue), nil
	}
	if err := d.putPollerLogRetention(ctx, region); err != nil {
		return "", err
	}
	return fmt.Sprintf("exists, %d-day retention set (it had none), tagged %s=%s", PollerLogRetentionDays, ManagedTagKey, ManagedTagValue), nil
}

// pollerLogRetention returns the poller log group's retention in days, nil
// when it never expires. The group must exist.
func (d *Deployer) pollerLogRetention(ctx context.Context, region string) (*int32, error) {
	out, err := d.logs.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(PollerLogGroupName),
	}, logsInRegion(region))
	if err != nil {
		return nil, fmt.Errorf("read retention of log group %s: %w", PollerLogGroupName, err)
	}
	// A prefix match: /aws/lambda/lagotto-capacity-poller-x would match too.
	for _, g := range out.LogGroups {
		if aws.ToString(g.LogGroupName) == PollerLogGroupName {
			return g.RetentionInDays, nil
		}
	}
	return nil, fmt.Errorf("read retention of log group %s: not listed, although creating it reported that it exists", PollerLogGroupName)
}

func (d *Deployer) putPollerLogRetention(ctx context.Context, region string) error {
	if _, err := d.logs.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{
		LogGroupName:    aws.String(PollerLogGroupName),
		RetentionInDays: aws.Int32(PollerLogRetentionDays),
	}, logsInRegion(region)); err != nil {
		return fmt.Errorf("set %d-day retention on log group %s: %w", PollerLogRetentionDays, PollerLogGroupName, err)
	}
	return nil
}

// deletePollerLogGroup deletes the poller's log group in region and reports
// whether it was there. DeleteLogGroup answers ResourceNotFoundException for
// an absent group, so the delete itself is the existence probe; any other
// error (AccessDenied, deletion protection) is returned, never read as "gone".
func (d *Deployer) deletePollerLogGroup(ctx context.Context, region string) (bool, error) {
	_, err := d.logs.DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{
		LogGroupName: aws.String(PollerLogGroupName),
	}, logsInRegion(region))
	if err == nil {
		return true, nil
	}
	var notFound *logstypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, fmt.Errorf("delete log group %s: %w", PollerLogGroupName, err)
}

// logsInRegion makes one CloudWatch Logs call in region, the region of the
// poller being deployed or torn down, whatever region the client was built
// for (the same rule as inRegion for SNS, #240).
func logsInRegion(region string) func(*cloudwatchlogs.Options) {
	return func(o *cloudwatchlogs.Options) {
		if region != "" {
			o.Region = region
		}
	}
}
