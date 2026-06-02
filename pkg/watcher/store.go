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

// Store handles DynamoDB persistence for watches and match history.
type Store struct {
	client       *dynamodb.Client
	watchesTable string
	historyTable string
}

// NewStore creates a Store backed by DynamoDB.
func NewStore(cfg aws.Config, watchesTable, historyTable string) *Store {
	return &Store{
		client:       dynamodb.NewFromConfig(cfg),
		watchesTable: watchesTable,
		historyTable: historyTable,
	}
}

// PutWatch creates or updates a watch record.
func (s *Store) PutWatch(ctx context.Context, w *Watch) error {
	w.UpdatedAt = time.Now().UTC()
	item, err := attributevalue.MarshalMap(w)
	if err != nil {
		return fmt.Errorf("marshal watch: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.watchesTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("put watch: %w", err)
	}
	return nil
}

// GetWatch retrieves a single watch by ID.
func (s *Store) GetWatch(ctx context.Context, watchID string) (*Watch, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get watch: %w", err)
	}
	if result.Item == nil {
		return nil, nil
	}
	var w Watch
	if err := attributevalue.UnmarshalMap(result.Item, &w); err != nil {
		return nil, fmt.Errorf("unmarshal watch: %w", err)
	}
	return &w, nil
}

// ListWatchesByUser returns watches belonging to a user, optionally filtered by status.
func (s *Store) ListWatchesByUser(ctx context.Context, userID string, statusFilter WatchStatus) ([]Watch, error) {
	expr := "user_id = :uid"
	exprValues := map[string]types.AttributeValue{
		":uid": &types.AttributeValueMemberS{Value: userID},
	}
	if statusFilter != "" {
		expr += " AND #st = :status"
		exprValues[":status"] = &types.AttributeValueMemberS{Value: string(statusFilter)}
	}

	var exprNames map[string]string
	if statusFilter != "" {
		exprNames = map[string]string{"#st": "status"}
	}

	input := &dynamodb.QueryInput{
		TableName:                 &s.watchesTable,
		IndexName:                 aws.String("user_id-index"),
		KeyConditionExpression:    aws.String("user_id = :uid"),
		ExpressionAttributeValues: exprValues,
	}
	if statusFilter != "" {
		input.FilterExpression = aws.String("#st = :status")
		input.ExpressionAttributeNames = exprNames
	}

	result, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("list watches: %w", err)
	}

	watches := make([]Watch, 0, len(result.Items))
	for _, item := range result.Items {
		var w Watch
		if err := attributevalue.UnmarshalMap(item, &w); err != nil {
			return nil, fmt.Errorf("unmarshal watch: %w", err)
		}
		watches = append(watches, w)
	}
	return watches, nil
}

// ListActiveWatches returns all watches with status "active". Used by the poller.
func (s *Store) ListActiveWatches(ctx context.Context) ([]Watch, error) {
	input := &dynamodb.QueryInput{
		TableName:              &s.watchesTable,
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("#st = :status"),
		ExpressionAttributeNames: map[string]string{
			"#st": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(StatusActive)},
		},
	}

	var watches []Watch
	for {
		result, err := s.client.Query(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list active watches: %w", err)
		}
		for _, item := range result.Items {
			var w Watch
			if err := attributevalue.UnmarshalMap(item, &w); err != nil {
				return nil, fmt.Errorf("unmarshal watch: %w", err)
			}
			watches = append(watches, w)
		}
		if result.LastEvaluatedKey == nil {
			break
		}
		input.ExclusiveStartKey = result.LastEvaluatedKey
	}
	return watches, nil
}

// ExtendWatch updates the expiry time of a watch. If the watch was expired,
// it also resets the status to active.
func (s *Store) ExtendWatch(ctx context.Context, watchID string, newExpiry time.Time, reactivate bool) error {
	expr := "SET expires_at = :exp, ttl_timestamp = :ttl, updated_at = :now"
	values := map[string]types.AttributeValue{
		":exp": &types.AttributeValueMemberS{Value: newExpiry.Format(time.RFC3339)},
		":ttl": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", newExpiry.Unix())},
		":now": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
	}
	names := map[string]string{}

	if reactivate {
		expr += ", #st = :active"
		values[":active"] = &types.AttributeValueMemberS{Value: string(StatusActive)}
		names["#st"] = "status"
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		UpdateExpression:          aws.String(expr),
		ExpressionAttributeValues: values,
	}
	if len(names) > 0 {
		input.ExpressionAttributeNames = names
	}

	_, err := s.client.UpdateItem(ctx, input)
	if err != nil {
		return fmt.Errorf("extend watch: %w", err)
	}
	return nil
}

// UpdateWatchStatus atomically updates a watch's status.
func (s *Store) UpdateWatchStatus(ctx context.Context, watchID string, status WatchStatus) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		UpdateExpression: aws.String("SET #st = :status, updated_at = :now"),
		ExpressionAttributeNames: map[string]string{
			"#st": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(status)},
			":now":    &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		},
	})
	if err != nil {
		return fmt.Errorf("update watch status: %w", err)
	}
	return nil
}

// RecordMatch updates the watch with match info and writes a history record.
func (s *Store) RecordMatch(ctx context.Context, w *Watch, m *MatchResult) error {
	// Update the watch record
	now := time.Now().UTC()
	w.MatchCount++
	w.LastMatch = m
	w.UpdatedAt = now
	if err := s.PutWatch(ctx, w); err != nil {
		return fmt.Errorf("update watch with match: %w", err)
	}

	// Write to match history
	m.TTLTimestamp = now.Add(90 * 24 * time.Hour).Unix() // 90-day retention
	item, err := attributevalue.MarshalMap(m)
	if err != nil {
		return fmt.Errorf("marshal match: %w", err)
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &s.historyTable,
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("put match history: %w", err)
	}
	return nil
}

// UpdateLastPolled updates the last_polled_at timestamp on a watch.
func (s *Store) UpdateLastPolled(ctx context.Context, watchID string) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		UpdateExpression: aws.String("SET last_polled_at = :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		},
	})
	if err != nil {
		return fmt.Errorf("update last polled: %w", err)
	}
	return nil
}

// ListMatchHistory returns match history, optionally filtered by watch ID.
func (s *Store) ListMatchHistory(ctx context.Context, watchID string) ([]MatchResult, error) {
	if watchID != "" {
		return s.listMatchHistoryByWatch(ctx, watchID)
	}
	// Scan all history (for the user's history command — should be scoped by user in practice)
	return s.scanMatchHistory(ctx)
}

func (s *Store) listMatchHistoryByWatch(ctx context.Context, watchID string) ([]MatchResult, error) {
	result, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.historyTable,
		KeyConditionExpression: aws.String("watch_id = :wid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":wid": &types.AttributeValueMemberS{Value: watchID},
		},
		ScanIndexForward: aws.Bool(false), // newest first
	})
	if err != nil {
		return nil, fmt.Errorf("query match history: %w", err)
	}
	return unmarshalMatches(result.Items)
}

func (s *Store) scanMatchHistory(ctx context.Context) ([]MatchResult, error) {
	result, err := s.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: &s.historyTable,
		Limit:     aws.Int32(100),
	})
	if err != nil {
		return nil, fmt.Errorf("scan match history: %w", err)
	}
	return unmarshalMatches(result.Items)
}

// ListMatchHistoryByUser returns match history for a specific user.
func (s *Store) ListMatchHistoryByUser(ctx context.Context, userID string) ([]MatchResult, error) {
	result, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              &s.historyTable,
		IndexName:              aws.String("user_id-index"),
		KeyConditionExpression: aws.String("user_id = :uid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":uid": &types.AttributeValueMemberS{Value: userID},
		},
		ScanIndexForward: aws.Bool(false),
	})
	if err != nil {
		return nil, fmt.Errorf("query match history by user: %w", err)
	}
	return unmarshalMatches(result.Items)
}

// EnsureTables creates the watches and match-history tables if they don't yet
// exist, then waits until both are ACTIVE. It is idempotent: existing tables are
// left untouched. This lets lagotto own its backend with zero manual setup (#12).
//
// Returns the names of any tables it created (empty if both already existed).
func (s *Store) EnsureTables(ctx context.Context) ([]string, error) {
	var created []string

	madeWatches, err := s.ensureTable(ctx, s.watchesTable, watchesTableSchema(s.watchesTable))
	if err != nil {
		return created, fmt.Errorf("ensure watches table %q: %w", s.watchesTable, err)
	}
	if madeWatches {
		created = append(created, s.watchesTable)
	}

	madeHistory, err := s.ensureTable(ctx, s.historyTable, historyTableSchema(s.historyTable))
	if err != nil {
		return created, fmt.Errorf("ensure history table %q: %w", s.historyTable, err)
	}
	if madeHistory {
		created = append(created, s.historyTable)
	}

	// Wait for any newly-created tables to become ACTIVE before returning, so a
	// subsequent write doesn't race table creation.
	waiter := dynamodb.NewTableExistsWaiter(s.client)
	for _, name := range created {
		if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}, 2*time.Minute); err != nil {
			return created, fmt.Errorf("wait for table %q to become active: %w", name, err)
		}
		// Enable DynamoDB TTL on ttl_timestamp so resolved watches (at expiry)
		// and match history (90-day retention) age out on their own — the basis
		// for the account self-cleaning once activity stops (#12).
		if err := s.enableTTL(ctx, name); err != nil {
			return created, fmt.Errorf("enable TTL on %q: %w", name, err)
		}
	}
	return created, nil
}

// enableTTL turns on DynamoDB TTL for the ttl_timestamp attribute. Idempotent:
// a table that already has TTL enabled is left as-is.
func (s *Store) enableTTL(ctx context.Context, name string) error {
	desc, err := s.client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(name)})
	if err == nil && desc.TimeToLiveDescription != nil {
		switch desc.TimeToLiveDescription.TimeToLiveStatus {
		case types.TimeToLiveStatusEnabled, types.TimeToLiveStatusEnabling:
			return nil
		}
	}
	_, err = s.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(name),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("ttl_timestamp"),
		},
	})
	if err != nil {
		// Substrate / some emulators don't implement UpdateTimeToLive; treat as
		// non-fatal so table creation still succeeds.
		var notImpl *types.ResourceInUseException
		if errors.As(err, &notImpl) {
			return nil
		}
		return err
	}
	return nil
}

// TablesEmpty reports whether both the watches and history tables contain zero
// items. Used to decide when the account can be auto-torn-down (no litter).
func (s *Store) TablesEmpty(ctx context.Context) (bool, error) {
	for _, name := range []string{s.watchesTable, s.historyTable} {
		out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(name),
			Limit:     aws.Int32(1),
			Select:    types.SelectCount,
		})
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(err, &notFound) {
				continue // a missing table is trivially empty
			}
			return false, fmt.Errorf("scan %q: %w", name, err)
		}
		if out.Count > 0 {
			return false, nil
		}
	}
	return true, nil
}

// DeleteTables removes both lagotto tables unconditionally. Idempotent:
// already-absent tables are ignored. Returns the names actually deleted.
// Used by the explicit `lagotto teardown` command (user opted in).
func (s *Store) DeleteTables(ctx context.Context) ([]string, error) {
	var deleted []string
	for _, name := range []string{s.watchesTable, s.historyTable} {
		_, err := s.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(err, &notFound) {
				continue
			}
			return deleted, fmt.Errorf("delete table %q: %w", name, err)
		}
		deleted = append(deleted, name)
	}
	return deleted, nil
}

// DeleteManagedTables deletes only tables tagged as CLI-managed
// (lagotto:managed=cli). CloudFormation-managed tables lack the tag and are left
// untouched, so the lambda's automatic teardown never causes stack drift or
// trips on missing control-plane IAM. Returns the names actually deleted.
func (s *Store) DeleteManagedTables(ctx context.Context) ([]string, error) {
	var deleted []string
	for _, name := range []string{s.watchesTable, s.historyTable} {
		managed, err := s.isManaged(ctx, name)
		if err != nil {
			return deleted, err
		}
		if !managed {
			continue // CFN-managed or absent — don't touch
		}
		_, err = s.client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(name)})
		if err != nil {
			var notFound *types.ResourceNotFoundException
			if errors.As(err, &notFound) {
				continue
			}
			return deleted, fmt.Errorf("delete table %q: %w", name, err)
		}
		deleted = append(deleted, name)
	}
	return deleted, nil
}

// isManaged reports whether the named table carries the lagotto:managed=cli tag.
// A missing table returns false (nothing to delete).
func (s *Store) isManaged(ctx context.Context, name string) (bool, error) {
	desc, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("describe table %q: %w", name, err)
	}
	arn := desc.Table.TableArn
	if arn == nil {
		return false, nil
	}
	tags, err := s.client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: arn})
	if err != nil {
		// If we can't read tags, be conservative and treat as unmanaged so we
		// never delete something we shouldn't.
		return false, nil
	}
	for _, t := range tags.Tags {
		if aws.ToString(t.Key) == aws.ToString(managedTag.Key) && aws.ToString(t.Value) == aws.ToString(managedTag.Value) {
			return true, nil
		}
	}
	return false, nil
}

// ensureTable creates one table if it does not already exist. Returns true if it
// created the table, false if it already existed.
func (s *Store) ensureTable(ctx context.Context, name string, input *dynamodb.CreateTableInput) (bool, error) {
	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err == nil {
		return false, nil // already exists
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return false, fmt.Errorf("describe table: %w", err)
	}

	if _, err := s.client.CreateTable(ctx, input); err != nil {
		// Tolerate a concurrent creator (another lagotto invocation racing us).
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			return false, nil
		}
		return false, fmt.Errorf("create table: %w", err)
	}
	return true, nil
}

// managedTag marks a table as created/owned by the lagotto CLI (as opposed to
// the CloudFormation stack). Auto-teardown only deletes tables carrying this
// tag, so it never destroys CFN-managed tables (which would cause stack drift
// and lacks the IAM the deployed lambda holds).
var managedTag = types.Tag{Key: aws.String("lagotto:managed"), Value: aws.String("cli")}

// watchesTableSchema returns the CreateTable input for the watches table:
// hash key watch_id, GSIs user_id-index and status-index.
func watchesTableSchema(name string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: types.BillingModePayPerRequest,
		Tags:        []types.Tag{managedTag},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("watch_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("user_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("watch_id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:  aws.String("user_id-index"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("user_id"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName:  aws.String("status-index"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
}

// historyTableSchema returns the CreateTable input for the match-history table:
// hash key watch_id, range key matched_at, GSI user_id-index.
func historyTableSchema(name string) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: types.BillingModePayPerRequest,
		Tags:        []types.Tag{managedTag},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("watch_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("matched_at"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("user_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("watch_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("matched_at"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("user_id-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("user_id"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("matched_at"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
	}
}

func unmarshalMatches(items []map[string]types.AttributeValue) ([]MatchResult, error) {
	matches := make([]MatchResult, 0, len(items))
	for _, item := range items {
		var m MatchResult
		if err := attributevalue.UnmarshalMap(item, &m); err != nil {
			return nil, fmt.Errorf("unmarshal match: %w", err)
		}
		matches = append(matches, m)
	}
	return matches, nil
}
