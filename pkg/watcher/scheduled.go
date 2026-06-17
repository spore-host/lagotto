package watcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ScheduledLaunchStatus is the lifecycle of a scheduled launch.
type ScheduledLaunchStatus string

const (
	// ScheduledPending — armed, not yet fired.
	ScheduledPending ScheduledLaunchStatus = "pending"
	// ScheduledLaunched — fired and launched at least once (a one-shot is done).
	ScheduledLaunched ScheduledLaunchStatus = "launched"
	// ScheduledFailed — the launch hit a terminal error.
	ScheduledFailed ScheduledLaunchStatus = "failed"
	// ScheduledCancelled — the user cancelled it before firing.
	ScheduledCancelled ScheduledLaunchStatus = "cancelled"
)

// IfExists policies decide what a scheduled launch does when an instance with the
// same Name tag already exists at fire time (#49 overlap policy).
const (
	// IfExistsSkip — don't launch; treat the existing instance as the fulfillment.
	// Default for one-shots (a --at into a Capacity Block must not double-launch).
	IfExistsSkip = "skip"
	// IfExistsLaunch — launch regardless; each fire is a fresh box. Default for cron.
	IfExistsLaunch = "launch"
	// IfExistsReplace — terminate the existing instance, then launch a new one.
	IfExistsReplace = "replace"
)

// ScheduledLaunch is a time-triggered launch (#49): launch an instance at a
// clock time (`--at`), after a delay (`--after`), or on a cron (`--cron`) —
// distinct from a Watch, which fires on *capacity* appearing. The motivating
// case is launching into an EC2 Capacity Block at its reserved start time. The
// launch config is the same SpawnConfigFile JSON a watch stores, so it inherits
// the #38 TTL guarantee and the reservation/capacity-block passthrough.
type ScheduledLaunch struct {
	ScheduleID       string                `json:"schedule_id" dynamodbav:"schedule_id"`
	UserID           string                `json:"user_id" dynamodbav:"user_id"`
	Status           ScheduledLaunchStatus `json:"status" dynamodbav:"status"`
	Region           string                `json:"region" dynamodbav:"region"`
	AvailabilityZone string                `json:"availability_zone,omitempty" dynamodbav:"availability_zone,omitempty"`
	// CronExpr is set for recurring schedules; empty for a one-shot. LaunchAt is
	// the resolved fire time for one-shots (--at / now+--after).
	CronExpr         string    `json:"cron_expr,omitempty" dynamodbav:"cron_expr,omitempty"`
	LaunchAt         time.Time `json:"launch_at,omitempty" dynamodbav:"launch_at,omitempty"`
	LaunchConfigJSON []byte    `json:"launch_config_json" dynamodbav:"launch_config_json"`
	// InstanceName is the launch's Name tag — the dedup key for IfExists.
	InstanceName string `json:"instance_name,omitempty" dynamodbav:"instance_name,omitempty"`
	// IfExists is the overlap policy when an instance named InstanceName already
	// exists at fire time (#49): "skip" (one-shot default — don't double-launch,
	// e.g. a Capacity Block), "launch" (cron default — each fire is a fresh box),
	// or "replace" (terminate the existing instance, then launch).
	IfExists string `json:"if_exists,omitempty" dynamodbav:"if_exists,omitempty"`
	// EventBridge Scheduler schedule name created for this launch (so cancel can
	// delete it). One-shots use ActionAfterCompletion=DELETE and self-remove.
	ScheduleName string    `json:"schedule_name,omitempty" dynamodbav:"schedule_name,omitempty"`
	InstanceID   string    `json:"instance_id,omitempty" dynamodbav:"instance_id,omitempty"`
	CreatedAt    time.Time `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" dynamodbav:"updated_at"`
	// TTLTimestamp ages the record out of DynamoDB long after it has fired/failed
	// so the table self-cleans (mirrors watches/history).
	TTLTimestamp int64 `json:"ttl_timestamp,omitempty" dynamodbav:"ttl_timestamp,omitempty"`
}

// PutScheduledLaunch creates or updates a scheduled launch.
func (s *Store) PutScheduledLaunch(ctx context.Context, sl *ScheduledLaunch) error {
	sl.UpdatedAt = time.Now().UTC()
	item, err := attributevalue.MarshalMap(sl)
	if err != nil {
		return fmt.Errorf("marshal scheduled launch: %w", err)
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.scheduledTable,
		Item:      item,
	}); err != nil {
		return fmt.Errorf("put scheduled launch: %w", err)
	}
	return nil
}

// GetScheduledLaunch retrieves one by ID (nil, nil if absent).
func (s *Store) GetScheduledLaunch(ctx context.Context, id string) (*ScheduledLaunch, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.scheduledTable,
		Key:       map[string]types.AttributeValue{"schedule_id": &types.AttributeValueMemberS{Value: id}},
	})
	if err != nil {
		return nil, fmt.Errorf("get scheduled launch: %w", err)
	}
	if out.Item == nil {
		return nil, nil
	}
	var sl ScheduledLaunch
	if err := attributevalue.UnmarshalMap(out.Item, &sl); err != nil {
		return nil, fmt.Errorf("unmarshal scheduled launch: %w", err)
	}
	return &sl, nil
}

// UpdateScheduledLaunchStatus sets the status (and instance id, if launched).
func (s *Store) UpdateScheduledLaunchStatus(ctx context.Context, id string, status ScheduledLaunchStatus, instanceID string) error {
	upd := "SET #s = :s, updated_at = :u"
	names := map[string]string{"#s": "status"}
	vals := map[string]types.AttributeValue{
		":s": &types.AttributeValueMemberS{Value: string(status)},
		":u": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}
	if instanceID != "" {
		upd += ", instance_id = :i"
		vals[":i"] = &types.AttributeValueMemberS{Value: instanceID}
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.scheduledTable,
		Key:                       map[string]types.AttributeValue{"schedule_id": &types.AttributeValueMemberS{Value: id}},
		UpdateExpression:          aws.String(upd),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	})
	if err != nil {
		return fmt.Errorf("update scheduled launch status: %w", err)
	}
	return nil
}

// HasPendingScheduledLaunches reports whether any scheduled launch is still
// pending (armed but not yet fired/failed/cancelled). The poller uses this as
// part of its teardown refcount (#49): infra must stay alive while a future
// launch is armed, even when zero watches remain. A missing table = none.
func (s *Store) HasPendingScheduledLaunches(ctx context.Context) (bool, error) {
	out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
		TableName:                 &s.scheduledTable,
		FilterExpression:          aws.String("#s = :p"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": &types.AttributeValueMemberS{Value: string(ScheduledPending)}},
		Select:                    types.SelectCount,
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, nil // no table → nothing pending
		}
		return false, fmt.Errorf("scan scheduled launches: %w", err)
	}
	return out.Count > 0, nil
}

// scheduledTableSchema is the CreateTable input for the scheduled-launches table:
// hash key schedule_id, a user_id GSI, TTL on ttl_timestamp.
func scheduledTableSchema(name string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: types.BillingModePayPerRequest,
		Tags:        []types.Tag{managedTag},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("schedule_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("user_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("schedule_id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:  aws.String("user_id-index"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("user_id"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
}
