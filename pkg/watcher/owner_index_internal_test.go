package watcher

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeIndexAPI struct {
	missing bool
	status  types.IndexStatus // "" = no owner index
	updates int
}

func (f *fakeIndexAPI) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if f.missing {
		return nil, &types.ResourceNotFoundException{}
	}
	td := &types.TableDescription{GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{{IndexName: aws.String("status-index"), IndexStatus: types.IndexStatusActive}}}
	if f.status != "" {
		td.GlobalSecondaryIndexes = append(td.GlobalSecondaryIndexes, types.GlobalSecondaryIndexDescription{IndexName: aws.String(OwnerIndexName), IndexStatus: f.status})
	}
	return &dynamodb.DescribeTableOutput{Table: td}, nil
}

func (f *fakeIndexAPI) UpdateTable(_ context.Context, in *dynamodb.UpdateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	f.updates++
	if c := in.GlobalSecondaryIndexUpdates[0].Create; c == nil || aws.ToString(c.IndexName) != OwnerIndexName ||
		c.Projection.ProjectionType != types.ProjectionTypeKeysOnly {
		return nil, errors.New("unexpected index")
	}
	f.status = types.IndexStatusCreating
	return &dynamodb.UpdateTableOutput{}, nil
}

func TestEnsureOwnerIndex(t *testing.T) {
	ctx := context.Background()
	f := &fakeIndexAPI{}
	if created, err := EnsureOwnerIndex(ctx, f, "t"); !created || err != nil || f.updates != 1 {
		t.Fatalf("absent: created=%v err=%v updates=%d", created, err, f.updates)
	}
	if _, err := EnsureOwnerIndex(ctx, f, "t"); !errors.Is(err, ErrOwnerIndexNotReady) || f.updates != 1 {
		t.Errorf("building: %v (updates %d), want not ready and no second create", err, f.updates)
	}
	f.status = types.IndexStatusActive
	if created, err := EnsureOwnerIndex(ctx, f, "t"); created || err != nil {
		t.Errorf("active: created=%v err=%v", created, err)
	}
	if created, err := EnsureOwnerIndex(ctx, &fakeIndexAPI{missing: true}, "t"); created || err != nil {
		t.Errorf("no table: created=%v err=%v", created, err)
	}
	if idx := watchesTableSchema("t").GlobalSecondaryIndexes; aws.ToString(idx[len(idx)-1].IndexName) != OwnerIndexName {
		t.Error("a new watches table has no owner index")
	}
}
