package watcher

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// The poller-control item (#323).
//
// One item in the watches table, at PollerControlKey, carries the epoch that
// every arming of a watch increments and the hosted poller checks before and
// after it slows its schedule to the idle cadence (see pkg/pollcadence for the
// protocol). It is not a watch: it has no status, user_id or owner_id, so none
// of the table's indexes hold it, and it is never listed, polled or migrated.
//
// The key cannot collide with a record's key. Every record key is either
// "<principal id>/<id>" (OwnerKey) or a legacy "w-…" / "g-…" id, and "#"
// occurs in neither: an IAM unique id is upper-case letters and digits, and a
// role session name or federated user name is drawn from [\w+=,.@-]. For the
// same reason it never matches the lagotto-cli-user policy's owner pattern
// "${aws:userid}/*", which is why that policy grants it separately, by exact
// key and attribute (pkg/cliiam PollerControl). It contains "/" so that
// IsLegacyKey is false: `lagotto setup` (of this version or an older one) does
// not try to move it under an owner.
const PollerControlKey = "#lagotto/poller-control"

// Attributes of the poller-control item. The CLI writes only
// PollerEpochAttr; the policy's PollerControl statement allows nothing else.
const (
	PollerEpochAttr  = "epoch"
	pollerModeAttr   = "mode"
	pollerIdleAtAttr = "idle_at"
	// PollerModeIdle is the mode the poller records before it slows down.
	PollerModeIdle = "idle"
)

// IsPollerControlKey reports whether key is the poller-control item's.
func IsPollerControlKey(key string) bool { return key == PollerControlKey }

func pollerControlItemKey() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"watch_id": avS(PollerControlKey)}
}

// BumpPollerEpoch increments the poller epoch, creating the control item if
// need be. A program arming a watch the hosted poller must service calls it
// AFTER storing the watch and BEFORE setting the poller's schedule (pkg/arm).
func (s *Store) BumpPollerEpoch(ctx context.Context) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.watchesTable,
		Key:                       pollerControlItemKey(),
		UpdateExpression:          aws.String("ADD #e :one"),
		ExpressionAttributeNames:  map[string]string{"#e": PollerEpochAttr},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
	})
	if err != nil {
		return fmt.Errorf("bump the poller epoch: %w", err)
	}
	return nil
}

// PollerEpoch reads the poller epoch strongly consistently; 0 when the
// control item does not exist yet.
func (s *Store) PollerEpoch(ctx context.Context) (int64, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:                &s.watchesTable,
		Key:                      pollerControlItemKey(),
		ConsistentRead:           aws.Bool(true),
		ProjectionExpression:     aws.String("#e"),
		ExpressionAttributeNames: map[string]string{"#e": PollerEpochAttr},
	})
	if err != nil {
		return 0, fmt.Errorf("read the poller epoch: %w", err)
	}
	n, ok := out.Item[PollerEpochAttr].(*types.AttributeValueMemberN)
	if !ok {
		return 0, nil
	}
	v, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("read the poller epoch: %q is not an integer: %w", n.Value, err)
	}
	return v, nil
}

// MarkPollerIdle records mode=idle on the control item only if its epoch is
// still seen (absent, for 0). It reports false, with a nil error, when the
// epoch moved: a watch was armed after seen was read.
func (s *Store) MarkPollerIdle(ctx context.Context, seen int64) (bool, error) {
	names := map[string]string{"#e": PollerEpochAttr, "#m": pollerModeAttr, "#at": pollerIdleAtAttr}
	vals := map[string]types.AttributeValue{
		":idle": avS(PollerModeIdle),
		":now":  avS(time.Now().UTC().Format(time.RFC3339)),
	}
	cond := "attribute_not_exists(#e)"
	if seen != 0 {
		cond = "#e = :seen"
		vals[":seen"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(seen, 10)}
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.watchesTable,
		Key:                       pollerControlItemKey(),
		UpdateExpression:          aws.String("SET #m = :idle, #at = :now"),
		ConditionExpression:       aws.String(cond),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	})
	if err != nil {
		var failed *types.ConditionalCheckFailedException
		if errors.As(err, &failed) {
			return false, nil
		}
		return false, fmt.Errorf("record the poller's idle mode: %w", err)
	}
	return true, nil
}
