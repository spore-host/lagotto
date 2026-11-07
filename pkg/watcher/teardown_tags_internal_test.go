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

// tagFailDynamo has every table present, fails ListTagsOfResource, and records
// DeleteTable calls. Other dynamoAPI methods panic through the nil embedding.
type tagFailDynamo struct {
	dynamoAPI
	deleted []string
}

func (f *tagFailDynamo) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	arn := "arn:aws:dynamodb:us-west-2:123456789012:table/" + aws.ToString(in.TableName)
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableArn: aws.String(arn)}}, nil
}

func (f *tagFailDynamo) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, errors.New("AccessDeniedException: not authorized to perform dynamodb:ListTagsOfResource")
}

func (f *tagFailDynamo) DeleteTable(_ context.Context, in *dynamodb.DeleteTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	f.deleted = append(f.deleted, aws.ToString(in.TableName))
	return &dynamodb.DeleteTableOutput{}, nil
}

// An explicit `lagotto teardown` that cannot read the scheduled table's tags
// fails naming the table rather than silently leaving it behind (#250), and
// does not delete it.
func TestDeleteTables_UnreadableScheduledTagsIsAnError(t *testing.T) {
	f := &tagFailDynamo{}
	s := &Store{client: f, watchesTable: "w", historyTable: "h", scheduledTable: "s"}

	res, err := s.DeleteTables(context.Background())
	if err == nil || !strings.Contains(err.Error(), `"s"`) || !errors.Is(err, errTagsUnreadable) {
		t.Fatalf("err = %v, want an unreadable-tags error naming table s", err)
	}
	if strings.Join(res.Deleted, ",") != "w,h" || strings.Join(f.deleted, ",") != "w,h" {
		t.Errorf("deleted %v (calls %v), want w,h and never s", res.Deleted, f.deleted)
	}
}

// The poller's automatic teardown keeps its conservative behaviour: unreadable
// tags mean "not managed", never an error and never a delete.
func TestDeleteManagedTables_UnreadableTagsDeletesNothing(t *testing.T) {
	f := &tagFailDynamo{}
	s := &Store{client: f, watchesTable: "w", historyTable: "h", scheduledTable: "s"}

	deleted, err := s.DeleteManagedTables(context.Background())
	if err != nil || len(deleted) != 0 || len(f.deleted) != 0 {
		t.Errorf("DeleteManagedTables = %v, %v (calls %v); want nothing deleted, no error", deleted, err, f.deleted)
	}
}
