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
	// ScheduledRetrying — fired, but the launch hit a *retryable* boundary failure
	// (reservation not yet active / transient capacity) and the handler has
	// re-armed a tight-interval retry to keep trying through the window open (#62).
	ScheduledRetrying ScheduledLaunchStatus = "retrying"
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
	ScheduleID string `json:"schedule_id" dynamodbav:"schedule_id"`
	UserID     string `json:"user_id" dynamodbav:"user_id"`
	// OwnerID is the creator's unique principal id, as on [Watch.OwnerID].
	OwnerID          string                `json:"owner_id,omitempty" dynamodbav:"owner_id,omitempty"`
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

	// ── Capacity Block start-time launch (#62) ───────────────────────────────
	// ReservationID, when set, makes this a "fire at the reservation's window
	// open" launch: the start time is derived from the reservation (not --at), the
	// handler verifies the reservation is launchable, and a retryable failure at
	// the boundary re-arms a tight-interval retry instead of failing.
	ReservationID string `json:"reservation_id,omitempty" dynamodbav:"reservation_id,omitempty"`
	// RetryUntil bounds the boundary-retry loop: once now > RetryUntil the launch
	// gives up (failed) rather than re-arming. Zero = no boundary retry (the
	// plain #49 one-shot behavior). All Capacity Blocks end 11:30 UTC, so this is
	// set well before the window close so a doomed launch doesn't churn forever.
	RetryUntil time.Time `json:"retry_until,omitempty" dynamodbav:"retry_until,omitempty"`
	// RetryIntervalSeconds is how long the handler waits before the next boundary
	// retry (default 30s) — tight, to catch the window open promptly.
	RetryIntervalSeconds int `json:"retry_interval_seconds,omitempty" dynamodbav:"retry_interval_seconds,omitempty"`
	// Attempts counts boundary-retry firings, for observability.
	Attempts int `json:"attempts,omitempty" dynamodbav:"attempts,omitempty"`
}

// ErrScheduledLaunchCancelled is PutScheduledLaunch refusing to overwrite a
// record `lagotto cancel` has cancelled (#279).
var ErrScheduledLaunchCancelled = errors.New("scheduled launch was cancelled")

// ErrScheduledLaunchChanged is CancelScheduledLaunch finding the record no
// longer in the status the caller read: it fired, failed or was cancelled in
// between.
var ErrScheduledLaunchChanged = errors.New("scheduled launch changed status")

// PutScheduledLaunch creates or updates a scheduled launch. It never
// overwrites a cancelled record (ErrScheduledLaunchCancelled): the poller's
// boundary retry (#62) rewrites the whole record, and without the condition a
// retry that read the record just before `lagotto cancel` would put it back to
// retrying and re-arm it (#279).
func (s *Store) PutScheduledLaunch(ctx context.Context, sl *ScheduledLaunch) error {
	sl.UpdatedAt = time.Now().UTC()
	item, err := attributevalue.MarshalMap(sl)
	if err != nil {
		return fmt.Errorf("marshal scheduled launch: %w", err)
	}
	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                &s.scheduledTable,
		Item:                     item,
		ConditionExpression:      aws.String("attribute_not_exists(schedule_id) OR #s <> :c"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":c": &types.AttributeValueMemberS{Value: string(ScheduledCancelled)},
		},
	}); err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return fmt.Errorf("put scheduled launch %s: %w", sl.ScheduleID, ErrScheduledLaunchCancelled)
		}
		return fmt.Errorf("put scheduled launch: %w", err)
	}
	return nil
}

// CancelScheduledLaunch marks a scheduled launch cancelled (status and
// updated_at, as the poller records a fired or failed one), only if its status
// is still from, the one the caller read and decided on. Otherwise it returns
// ErrScheduledLaunchChanged and changes nothing. The record keeps its TTL, so
// it ages out like any other finished launch; a cancelled record no longer
// counts as pending (HasPendingScheduledLaunches) and the poller does not fire
// it.
func (s *Store) CancelScheduledLaunch(ctx context.Context, id string, from ScheduledLaunchStatus) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                &s.scheduledTable,
		Key:                      map[string]types.AttributeValue{"schedule_id": &types.AttributeValueMemberS{Value: id}},
		UpdateExpression:         aws.String("SET #s = :c, updated_at = :u"),
		ConditionExpression:      aws.String("#s = :from"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":c":    &types.AttributeValueMemberS{Value: string(ScheduledCancelled)},
			":u":    &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
			":from": &types.AttributeValueMemberS{Value: string(from)},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return fmt.Errorf("cancel scheduled launch %s: %w", id, ErrScheduledLaunchChanged)
		}
		return fmt.Errorf("cancel scheduled launch %s: %w", id, err)
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
	// Both "pending" (armed, not yet fired) and "retrying" (#62, mid boundary
	// retry) keep the infra alive — tearing the poller down under a retrying
	// Capacity-Block launch would abandon it right at the window open.
	//
	// The status test is a FilterExpression, applied after each 1 MB page is
	// read, so a pending launch can sit on any page; scanFindsAny follows them
	// all (#181). Reading only the first page could report "none pending" and
	// let the poller disable its schedule under an armed launch.
	found, err := scanFindsAny(ctx, s.client, &dynamodb.ScanInput{
		TableName:                aws.String(s.scheduledTable),
		FilterExpression:         aws.String("#s = :p OR #s = :r"),
		ExpressionAttributeNames: map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: string(ScheduledPending)},
			":r": &types.AttributeValueMemberS{Value: string(ScheduledRetrying)},
		},
		Select: types.SelectCount,
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, nil // no table → nothing pending
		}
		return false, fmt.Errorf("scan scheduled launches: %w", err)
	}
	return found, nil
}

// ScheduledLaunchScheduleNames returns the ScheduleName of every
// scheduled-launch record, whatever its status, reading every page. `lagotto
// teardown` deletes those schedules before it deletes the table, which is the
// only record of them (#260). A missing table returns none.
func (s *Store) ScheduledLaunchScheduleNames(ctx context.Context) ([]string, error) {
	p := dynamodb.NewScanPaginator(s.client, &dynamodb.ScanInput{
		TableName:                aws.String(s.scheduledTable),
		ProjectionExpression:     aws.String("#n"),
		ExpressionAttributeNames: map[string]string{"#n": "schedule_name"},
	})
	var names []string
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(err, &notFound) {
				return nil, nil
			}
			return nil, fmt.Errorf("scan %q for schedule names: %w", s.scheduledTable, err)
		}
		for _, item := range page.Items {
			if v, ok := item["schedule_name"].(*types.AttributeValueMemberS); ok && v.Value != "" {
				names = append(names, v.Value)
			}
		}
	}
	return names, nil
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
