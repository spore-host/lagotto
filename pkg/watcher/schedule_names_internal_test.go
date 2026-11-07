package watcher

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func nameItem(name string) map[string]types.AttributeValue {
	if name == "" {
		return map[string]types.AttributeValue{}
	}
	return map[string]types.AttributeValue{"schedule_name": &types.AttributeValueMemberS{Value: name}}
}

// ScheduledLaunchScheduleNames reads every page, whatever the record's status,
// and skips records with no schedule name (#260).
func TestScheduledLaunchScheduleNames_AllPages(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["s"] = []fakePage{
		{items: []map[string]types.AttributeValue{nameItem("lagotto-launch-sl-aaaa"), nameItem("")}, count: -1},
		{items: []map[string]types.AttributeValue{nameItem("lagotto-launch-sl-bbbb")}, count: -1},
	}
	s := &Store{client: f, watchesTable: "w", historyTable: "h", scheduledTable: "s"}
	got, err := s.ScheduledLaunchScheduleNames(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-bbbb"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if f.calls["s"] != 2 {
		t.Errorf("scanned %d page(s), want 2", f.calls["s"])
	}
}

// A missing table names no schedules; any other failure is an error naming it.
func TestScheduledLaunchScheduleNames_Errors(t *testing.T) {
	f := newFakePagedDynamo()
	s := &Store{client: f, watchesTable: "w", historyTable: "h", scheduledTable: "s"}
	if got, err := s.ScheduledLaunchScheduleNames(context.Background()); err != nil || got != nil {
		t.Errorf("missing table: got %v, %v; want none", got, err)
	}

	s = &Store{client: scanFails{}, scheduledTable: "s"}
	if _, err := s.ScheduledLaunchScheduleNames(context.Background()); err == nil || !strings.Contains(err.Error(), `"s"`) {
		t.Errorf("err = %v, want a scan error naming table s", err)
	}
}

// scanFails fails every Scan; other dynamoAPI methods panic through the nil
// embedding.
type scanFails struct{ dynamoAPI }

func (scanFails) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, errors.New("AccessDeniedException")
}
