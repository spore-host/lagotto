package watcher_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestEnsureTables_CreatesThenIdempotent verifies EnsureTables creates both
// tables on first call and is a no-op on the second (#12).
func TestEnsureTables_CreatesAndIsIdempotent(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := watcher.NewStore(env.AWSConfig, "et-watches", "et-history")
	ctx := context.Background()

	created, err := store.EnsureTables(ctx)
	if err != nil {
		t.Fatalf("EnsureTables (first): %v", err)
	}
	if len(created) != 2 {
		t.Errorf("first call created %v, want 2 tables", created)
	}

	created2, err := store.EnsureTables(ctx)
	if err != nil {
		t.Fatalf("EnsureTables (second): %v", err)
	}
	if len(created2) != 0 {
		t.Errorf("second call created %v, want none (idempotent)", created2)
	}
}

// TestTablesEmptyAndDelete verifies the empty-check + delete lifecycle that
// drives auto-teardown.
func TestTablesEmptyAndDelete(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := watcher.NewStore(env.AWSConfig, "td-watches", "td-history")
	ctx := context.Background()

	if _, err := store.EnsureTables(ctx); err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}

	// Freshly created → empty.
	empty, err := store.TablesEmpty(ctx)
	if err != nil {
		t.Fatalf("TablesEmpty (fresh): %v", err)
	}
	if !empty {
		t.Error("fresh tables should be empty")
	}

	// Add a watch → not empty.
	if err := store.PutWatch(ctx, newTestWatch("w-td", "arn:aws:iam::123456789012:user/al")); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	empty, err = store.TablesEmpty(ctx)
	if err != nil {
		t.Fatalf("TablesEmpty (populated): %v", err)
	}
	if empty {
		t.Error("tables with a watch should not be empty")
	}

	// Delete → both reported deleted.
	deleted, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables: %v", err)
	}
	if len(deleted) != 2 {
		t.Errorf("deleted %v, want 2 tables", deleted)
	}

	// Delete again → idempotent, nothing left.
	deleted2, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables (second): %v", err)
	}
	if len(deleted2) != 0 {
		t.Errorf("second delete removed %v, want none", deleted2)
	}
}

// createUntaggedTable creates a DynamoDB table WITHOUT the lagotto:managed=cli
// tag, standing in for a CloudFormation-managed (or otherwise externally-owned)
// table that lagotto must never delete. It deliberately bypasses
// Store.EnsureTables, which tags everything it creates as CLI-managed.
func createUntaggedTable(t *testing.T, ctx context.Context, cli *dynamodb.Client, name string) {
	t.Helper()
	_, err := cli.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(name),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("watch_id"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("watch_id"), KeyType: dynamodbtypes.KeyTypeHash},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable %q: %v", name, err)
	}
}

// TestDeleteManagedTables_ConservativeWithoutTags verifies the safety invariant:
// the tag-gated deleter must delete NOTHING when a table does not carry the
// lagotto:managed=cli tag (i.e. lagotto can't confirm it owns the table). The
// tables here are created WITHOUT the managed tag, standing in for CFN-managed
// or otherwise externally-owned tables — deleting them would be a destructive
// bug, so DeleteManagedTables must leave them untouched.
func TestDeleteManagedTables_ConservativeWithoutTags(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()

	raw := dynamodb.NewFromConfig(env.AWSConfig)
	createUntaggedTable(t, ctx, raw, "mgd-watches")
	createUntaggedTable(t, ctx, raw, "mgd-history")

	store := watcher.NewStore(env.AWSConfig, "mgd-watches", "mgd-history")

	deleted, err := store.DeleteManagedTables(ctx)
	if err != nil {
		t.Fatalf("DeleteManagedTables: %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("deleted %v; expected none when the managed tag can't be confirmed", deleted)
	}

	// Both untagged tables must still exist (not deleted).
	for _, name := range []string{"mgd-watches", "mgd-history"} {
		if _, err := raw.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}); err != nil {
			t.Errorf("table %q should still exist (was not tagged managed): %v", name, err)
		}
	}
}

// TestDeleteManagedTables_DeletesTaggedTables is the positive counterpart: tables
// that DO carry the lagotto:managed=cli tag (as Store.EnsureTables creates them)
// are correctly confirmed as CLI-managed and deleted.
func TestDeleteManagedTables_DeletesTaggedTables(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := watcher.NewStore(env.AWSConfig, "own-watches", "own-history")
	ctx := context.Background()

	if _, err := store.EnsureTables(ctx); err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}

	deleted, err := store.DeleteManagedTables(ctx)
	if err != nil {
		t.Fatalf("DeleteManagedTables: %v", err)
	}
	if len(deleted) != 2 {
		t.Errorf("deleted %v; expected both CLI-managed tables to be deleted", deleted)
	}
}
