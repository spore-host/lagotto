package watcher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type captureUpdate struct {
	dynamoAPI
	in  *dynamodb.UpdateItemInput
	err error
}

func (c *captureUpdate) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	c.in = in
	return &dynamodb.UpdateItemOutput{}, c.err
}

// #309: the swap is conditional on BOTH the status and the exact config the
// caller validated against, so a launch that fired, was cancelled or was
// updated in between is never overwritten.
func TestUpdateScheduledLaunchConfigIsConditional(t *testing.T) {
	c := &captureUpdate{}
	s := &Store{client: c, scheduledTable: "s"}
	sl := &ScheduledLaunch{ScheduleID: "o/sl-1", Status: ScheduledRetrying, LaunchConfigJSON: []byte(`{"old":1}`)}
	if err := s.UpdateScheduledLaunchConfig(context.Background(), sl, []byte(`{"new":1}`)); err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(c.in.ConditionExpression); got != "#s = :s AND launch_config_json = :o" {
		t.Errorf("condition = %q", got)
	}
	if v := c.in.ExpressionAttributeValues; string(v[":o"].(*types.AttributeValueMemberB).Value) != `{"old":1}` ||
		string(v[":n"].(*types.AttributeValueMemberB).Value) != `{"new":1}` || v[":s"].(*types.AttributeValueMemberS).Value != "retrying" {
		t.Errorf("values = %+v", v)
	}
	if !strings.Contains(aws.ToString(c.in.UpdateExpression), "REMOVE last_error") {
		t.Errorf("update = %q: a new config should clear the old failure", aws.ToString(c.in.UpdateExpression))
	}

	c.err = &types.ConditionalCheckFailedException{}
	if err := s.UpdateScheduledLaunchConfig(context.Background(), sl, []byte(`{}`)); !errors.Is(err, ErrScheduledLaunchConfigChanged) {
		t.Errorf("conditional failure = %v, want ErrScheduledLaunchConfigChanged", err)
	}
	c.err = errors.New("boom")
	if err := s.UpdateScheduledLaunchConfig(context.Background(), sl, []byte(`{}`)); err == nil || errors.Is(err, ErrScheduledLaunchConfigChanged) {
		t.Errorf("other failure = %v", err)
	}
	if err := s.FailScheduledLaunch(context.Background(), "o/sl-1", "why"); err == nil {
		t.Error("FailScheduledLaunch swallowed an error")
	}
}
