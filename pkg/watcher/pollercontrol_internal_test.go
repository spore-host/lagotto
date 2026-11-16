package watcher

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeControlDynamo holds the poller-control item and evaluates the two
// conditions the Store writes on it (#323). Every other dynamoAPI method
// panics through the nil embedded interface.
type fakeControlDynamo struct {
	dynamoAPI
	item    map[string]types.AttributeValue // nil: no control item
	updates []*dynamodb.UpdateItemInput
	gets    []*dynamodb.GetItemInput
	// beforeUpdate runs once before the next UpdateItem is applied.
	beforeUpdate func()
}

func (f *fakeControlDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.gets = append(f.gets, in)
	return &dynamodb.GetItemOutput{Item: f.item}, nil
}

func (f *fakeControlDynamo) epoch() string {
	if n, ok := f.item[PollerEpochAttr].(*types.AttributeValueMemberN); ok {
		return n.Value
	}
	return ""
}

func (f *fakeControlDynamo) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if hook := f.beforeUpdate; hook != nil {
		f.beforeUpdate = nil
		hook()
	}
	f.updates = append(f.updates, in)
	if f.item == nil {
		f.item = map[string]types.AttributeValue{"watch_id": in.Key["watch_id"]}
	}
	switch aws.ToString(in.ConditionExpression) {
	case "":
	case "attribute_not_exists(#e)":
		if f.epoch() != "" {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("failed")}
		}
	case "#e = :seen":
		if f.epoch() != in.ExpressionAttributeValues[":seen"].(*types.AttributeValueMemberN).Value {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("failed")}
		}
	default:
		return nil, errors.New("unexpected condition " + aws.ToString(in.ConditionExpression))
	}
	expr := aws.ToString(in.UpdateExpression)
	switch {
	case strings.HasPrefix(expr, "ADD #e :one"):
		n := int64(0)
		if e := f.epoch(); e != "" {
			n = mustAtoi(e)
		}
		f.item[PollerEpochAttr] = &types.AttributeValueMemberN{Value: itoa(n + 1)}
	case strings.HasPrefix(expr, "SET #m = :idle"):
		f.item[pollerModeAttr] = in.ExpressionAttributeValues[":idle"]
	default:
		return nil, errors.New("unexpected update " + expr)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func mustAtoi(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// TestPollerControlKeyIsNoRecordKey: the control key cannot be a record's key
// or match the lagotto-cli-user owner pattern, and is not a legacy key (so
// MigrateOwnerKeys of any version leaves it alone).
func TestPollerControlKeyIsNoRecordKey(t *testing.T) {
	if IsLegacyKey(PollerControlKey) {
		t.Error("the control key is a legacy key: setup would try to move it under an owner")
	}
	owner := KeyOwner(PollerControlKey)
	// ${aws:userid} is an IAM unique id ([A-Z0-9]+), optionally ":" and a role
	// session or federated name ([\w+=,.@-]+); never with "#".
	if !strings.Contains(owner, "#") {
		t.Errorf("the control key's owner part %q could be an ${aws:userid}", owner)
	}
	for _, k := range []string{"w-1a2b3c4d", OwnerKey("AIDAEXAMPLE", "w-1"), OwnerKey("AROAEXAMPLE:alice", GroupKeyPrefix+"x")} {
		if IsPollerControlKey(k) {
			t.Errorf("%q is taken for the control key", k)
		}
	}
}

// TestBumpPollerEpoch: ADD on the control key, creating it, and nothing but
// the epoch (the lagotto-cli-user PollerControl statement allows no other
// attribute).
func TestBumpPollerEpoch(t *testing.T) {
	f := &fakeControlDynamo{}
	s := &Store{client: f, watchesTable: "lagotto-watches"}
	for i := 0; i < 2; i++ {
		if err := s.BumpPollerEpoch(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.epoch() != "2" {
		t.Errorf("epoch = %q, want 2", f.epoch())
	}
	in := f.updates[0]
	if aws.ToString(in.TableName) != "lagotto-watches" || in.Key["watch_id"].(*types.AttributeValueMemberS).Value != PollerControlKey {
		t.Errorf("bump wrote %s %v, want the control key in the watches table", aws.ToString(in.TableName), in.Key)
	}
	if len(in.ExpressionAttributeNames) != 1 || in.ExpressionAttributeNames["#e"] != PollerEpochAttr || in.ConditionExpression != nil {
		t.Errorf("bump touches %v (condition %v), want the epoch only", in.ExpressionAttributeNames, in.ConditionExpression)
	}
}

// TestPollerEpochRead: strongly consistent; a missing item is epoch 0.
func TestPollerEpochRead(t *testing.T) {
	f := &fakeControlDynamo{}
	s := &Store{client: f, watchesTable: "w"}
	if e, err := s.PollerEpoch(context.Background()); e != 0 || err != nil {
		t.Errorf("no item: got (%d, %v), want (0, nil)", e, err)
	}
	if !aws.ToBool(f.gets[0].ConsistentRead) {
		t.Error("the epoch read is not strongly consistent")
	}
	_ = s.BumpPollerEpoch(context.Background())
	if e, err := s.PollerEpoch(context.Background()); e != 1 || err != nil {
		t.Errorf("after one bump: got (%d, %v), want (1, nil)", e, err)
	}
}

// TestMarkPollerIdleFailsWhenArmedMidCheck: the poller's conditional
// mode=idle holds on an unchanged epoch, and fails, as (false, nil), when a
// watch was armed between the read and the write, from a missing item too.
func TestMarkPollerIdleFailsWhenArmedMidCheck(t *testing.T) {
	ctx := context.Background()
	for _, preBumps := range []int{0, 3} {
		f := &fakeControlDynamo{}
		s := &Store{client: f, watchesTable: "w"}
		for i := 0; i < preBumps; i++ {
			_ = s.BumpPollerEpoch(ctx)
		}
		seen, err := s.PollerEpoch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := s.MarkPollerIdle(ctx, seen)
		if !ok || err != nil {
			t.Fatalf("epoch %d unchanged: got (%v, %v), want (true, nil)", seen, ok, err)
		}

		// A watch arrives mid-check: its arming bumps the epoch after the read.
		seen, _ = s.PollerEpoch(ctx)
		f.beforeUpdate = func() {
			f.item[PollerEpochAttr] = &types.AttributeValueMemberN{Value: itoa(seen + 1)}
		}
		ok, err = s.MarkPollerIdle(ctx, seen)
		if ok || err != nil {
			t.Errorf("epoch %d moved: got (%v, %v), want (false, nil)", seen, ok, err)
		}
	}
	// From no item at all: the condition is attribute_not_exists, and a bump
	// that creates the item fails it.
	f := &fakeControlDynamo{}
	s := &Store{client: f, watchesTable: "w"}
	f.beforeUpdate = func() {
		f.item = map[string]types.AttributeValue{PollerEpochAttr: &types.AttributeValueMemberN{Value: "1"}}
	}
	if ok, err := s.MarkPollerIdle(ctx, 0); ok || err != nil {
		t.Errorf("no item, armed mid-check: got (%v, %v), want (false, nil)", ok, err)
	}
}

// TestNonEmptyTablesIgnoresPollerControl: a watches table holding only the
// poller-control item is empty, so teardown is not blocked by it.
func TestNonEmptyTablesIgnoresPollerControl(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["w"] = []fakePage{{count: 0}}
	f.pages["h"] = []fakePage{{count: 0}}
	f.pages["s"] = []fakePage{{count: 0}}
	if _, err := newPagedStore(f).NonEmptyTables(context.Background()); err != nil {
		t.Fatal(err)
	}
	in := f.last["w"].(*dynamodb.ScanInput)
	if aws.ToString(in.FilterExpression) != "watch_id <> :ctl" ||
		in.ExpressionAttributeValues[":ctl"].(*types.AttributeValueMemberS).Value != PollerControlKey {
		t.Errorf("watches scan filter = %q %v, want the control item excluded", aws.ToString(in.FilterExpression), in.ExpressionAttributeValues)
	}
	if in := f.last["h"].(*dynamodb.ScanInput); in.FilterExpression != nil {
		t.Errorf("history scan filter = %q, want none", aws.ToString(in.FilterExpression))
	}
}
