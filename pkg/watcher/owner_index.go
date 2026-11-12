package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// OwnerIDAttr is the attribute holding a record's owner principal id.
	OwnerIDAttr = "owner_id"
	// OwnerIndexName is the watches table's GSI on owner_id, through which
	// `lagotto list` finds the caller's own watches.
	//
	// It projects KEYS_ONLY. The lagotto-cli-user policy limits a Query on it
	// to the caller's own owner id when IAM evaluates dynamodb:LeadingKeys for
	// an index query; if it does not, the most another caller could read from
	// it is keys, never a watch. The watches themselves are then read with
	// BatchGetItem, which IAM authorizes key by key on the table's own
	// partition key (pkg/cliiam).
	OwnerIndexName = "owner_id-index"
)

func ownerIndex() types.GlobalSecondaryIndex {
	return types.GlobalSecondaryIndex{
		IndexName:  aws.String(OwnerIndexName),
		KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String(OwnerIDAttr), KeyType: types.KeyTypeHash}},
		Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
	}
}

// ListWatchesByOwner returns the watches under owner's key prefix, optionally
// only those in statusFilter. Index entries outside that prefix are ignored:
// owner_id is an ordinary attribute anyone may write on their own record, so
// only the key decides whose a watch is. Group items carry no owner_id and
// are not in the index.
func (s *Store) ListWatchesByOwner(ctx context.Context, owner string, statusFilter WatchStatus) ([]Watch, error) {
	if owner == "" {
		return nil, fmt.Errorf("list watches: no owner id")
	}
	items, err := queryAll(ctx, s.client, &dynamodb.QueryInput{
		TableName:                 &s.watchesTable,
		IndexName:                 aws.String(OwnerIndexName),
		KeyConditionExpression:    aws.String("#o = :o"),
		ExpressionAttributeNames:  map[string]string{"#o": OwnerIDAttr},
		ExpressionAttributeValues: map[string]types.AttributeValue{":o": &types.AttributeValueMemberS{Value: owner}},
	})
	if err != nil {
		return nil, fmt.Errorf("list watches: %w", err)
	}
	var keys []string
	for _, it := range items {
		k, ok := it["watch_id"].(*types.AttributeValueMemberS)
		if !ok || KeyOwner(k.Value) != owner || !strings.HasPrefix(ShortID(k.Value), "w-") {
			continue
		}
		keys = append(keys, k.Value)
	}
	got, err := s.batchGetWatches(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([]Watch, 0, len(got))
	for _, w := range got {
		if statusFilter == "" || w.Status == statusFilter {
			out = append(out, w)
		}
	}
	return out, nil
}

// batchGetWatches reads watches by key, 100 per request, retrying unprocessed
// keys.
func (s *Store) batchGetWatches(ctx context.Context, keys []string) ([]Watch, error) {
	var out []Watch
	for start := 0; start < len(keys); start += 100 {
		end := min(start+100, len(keys))
		req := make([]map[string]types.AttributeValue, 0, end-start)
		for _, k := range keys[start:end] {
			req = append(req, map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: k}})
		}
		pending := map[string]types.KeysAndAttributes{s.watchesTable: {Keys: req, ConsistentRead: aws.Bool(true)}}
		for attempt := 0; len(pending) > 0; attempt++ {
			if attempt > 8 {
				return out, fmt.Errorf("list watches: DynamoDB left keys unprocessed after %d attempts", attempt)
			}
			res, err := s.client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: pending})
			if err != nil {
				return out, fmt.Errorf("list watches: %w", err)
			}
			for _, item := range res.Responses[s.watchesTable] {
				var w Watch
				if err := attributevalue.UnmarshalMap(item, &w); err != nil {
					return out, fmt.Errorf("unmarshal watch: %w", err)
				}
				out = append(out, w)
			}
			pending = res.UnprocessedKeys
			if len(pending) > 0 {
				time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
			}
		}
	}
	return out, nil
}

// ownerIndexAPI is what EnsureOwnerIndex needs.
type ownerIndexAPI interface {
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
}

// ErrOwnerIndexNotReady means the owner index exists but is still being
// built; re-run once DynamoDB reports it ACTIVE.
var ErrOwnerIndexNotReady = errors.New("owner index is still being built")

// EnsureOwnerIndex adds [OwnerIndexName] to an existing watches table that
// predates it (`lagotto setup`; a table the CLI creates has it already) and
// reports whether it is ACTIVE. created is true when this call started the
// build. A table that does not exist is not an error: whoever creates it
// creates the index with it.
func EnsureOwnerIndex(ctx context.Context, api ownerIndexAPI, table string) (created bool, err error) {
	out, err := api.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		var nf *types.ResourceNotFoundException
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, fmt.Errorf("describe %s: %w", table, err)
	}
	for _, g := range out.Table.GlobalSecondaryIndexes {
		if aws.ToString(g.IndexName) != OwnerIndexName {
			continue
		}
		if g.IndexStatus != types.IndexStatusActive {
			return false, fmt.Errorf("%s on %s: %w (status %s)", OwnerIndexName, table, ErrOwnerIndexNotReady, g.IndexStatus)
		}
		return false, nil
	}
	idx := ownerIndex()
	if _, err := api.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:            aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String(OwnerIDAttr), AttributeType: types.ScalarAttributeTypeS}},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Create: &types.CreateGlobalSecondaryIndexAction{
			IndexName: idx.IndexName, KeySchema: idx.KeySchema, Projection: idx.Projection,
		}}},
	}); err != nil {
		return false, fmt.Errorf("add %s to %s: %w", OwnerIndexName, table, err)
	}
	return true, nil
}
