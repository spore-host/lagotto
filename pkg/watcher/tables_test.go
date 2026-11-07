package watcher_test

import (
	"context"
	"errors"
	"strings"
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

	// Delete → both reported deleted; the never-created scheduled table is
	// reported absent, not an error.
	res, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables: %v", err)
	}
	if len(res.Deleted) != 2 {
		t.Errorf("deleted %v, want 2 tables", res.Deleted)
	}
	if len(res.Absent) != 1 || res.Absent[0] != store.ScheduledTable() {
		t.Errorf("absent %v, want just %s", res.Absent, store.ScheduledTable())
	}

	// Delete again → idempotent, nothing left.
	res2, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables (second): %v", err)
	}
	if len(res2.Deleted) != 0 || len(res2.Absent) != 3 {
		t.Errorf("second delete = %+v, want nothing deleted and all 3 absent", res2)
	}
}

// requireTableGone fails the test if the named table still exists.
func requireTableGone(t *testing.T, ctx context.Context, cli *dynamodb.Client, name string) {
	t.Helper()
	_, err := cli.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	var notFound *dynamodbtypes.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		t.Errorf("table %q still exists after teardown (DescribeTable err = %v)", name, err)
	}
}

// TestDeleteTables_DeletesEveryTableDeployCreates: `lagotto deploy` runs
// EnsureTables and EnsureScheduledTable; `lagotto teardown` must delete every
// table those created (#250 — the scheduled table used to be left behind).
func TestDeleteTables_DeletesEveryTableDeployCreates(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	store := watcher.NewStore(env.AWSConfig, "dep-watches", "dep-history")
	store.SetScheduledTable("dep-scheduled")

	created, err := store.EnsureTables(ctx)
	if err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}
	sched, err := store.EnsureScheduledTable(ctx)
	if err != nil {
		t.Fatalf("EnsureScheduledTable: %v", err)
	}
	created = append(created, sched)

	res, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables: %v", err)
	}
	if strings.Join(res.Deleted, ",") != strings.Join(created, ",") {
		t.Errorf("deleted %v, want every created table %v", res.Deleted, created)
	}
	if len(res.Absent) != 0 || len(res.Unmanaged) != 0 {
		t.Errorf("absent %v, unmanaged %v; want none", res.Absent, res.Unmanaged)
	}
	raw := dynamodb.NewFromConfig(env.AWSConfig)
	for _, name := range created {
		requireTableGone(t, ctx, raw, name)
	}
}

// TestDeleteTables_DeletesLaunchCreatedScheduledTable: the first `lagotto
// launch` creates only the scheduled table. Teardown deletes it and reports the
// other two as absent rather than failing (#250).
func TestDeleteTables_DeletesLaunchCreatedScheduledTable(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	store := watcher.NewStore(env.AWSConfig, "ln-watches", "ln-history")
	store.SetScheduledTable("ln-scheduled")

	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		t.Fatalf("EnsureScheduledTable: %v", err)
	}
	res, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables: %v", err)
	}
	if len(res.Deleted) != 1 || res.Deleted[0] != "ln-scheduled" {
		t.Errorf("deleted %v, want [ln-scheduled]", res.Deleted)
	}
	if strings.Join(res.Absent, ",") != "ln-watches,ln-history" {
		t.Errorf("absent %v, want [ln-watches ln-history]", res.Absent)
	}
	requireTableGone(t, ctx, dynamodb.NewFromConfig(env.AWSConfig), "ln-scheduled")
}

// TestDeleteTables_LeavesUntaggedScheduledTable: a scheduled-launches table
// without the lagotto:managed=cli tag was not made by lagotto, so teardown
// leaves it in place and says so (#250).
func TestDeleteTables_LeavesUntaggedScheduledTable(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	raw := dynamodb.NewFromConfig(env.AWSConfig)
	createUntaggedTable(t, ctx, raw, "ut-scheduled")

	store := watcher.NewStore(env.AWSConfig, "ut-watches", "ut-history")
	store.SetScheduledTable("ut-scheduled")
	res, err := store.DeleteTables(ctx)
	if err != nil {
		t.Fatalf("DeleteTables: %v", err)
	}
	if len(res.Deleted) != 0 {
		t.Errorf("deleted %v, want none", res.Deleted)
	}
	if len(res.Unmanaged) != 1 || res.Unmanaged[0] != "ut-scheduled" {
		t.Errorf("unmanaged %v, want [ut-scheduled]", res.Unmanaged)
	}
	if _, err := raw.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("ut-scheduled")}); err != nil {
		t.Errorf("untagged table was deleted: %v", err)
	}
}

// TestNonEmptyTables_NamesThePendingScheduledLaunch: a pending scheduled launch
// blocks teardown, and NonEmptyTables names the table holding it (#250).
func TestNonEmptyTables_NamesThePendingScheduledLaunch(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	store := watcher.NewStore(env.AWSConfig, "ne-watches", "ne-history")
	store.SetScheduledTable("ne-scheduled")
	if _, err := store.EnsureTables(ctx); err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}
	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		t.Fatalf("EnsureScheduledTable: %v", err)
	}
	if got, err := store.NonEmptyTables(ctx); err != nil || len(got) != 0 {
		t.Fatalf("fresh tables: NonEmptyTables = %v, %v; want none", got, err)
	}
	if err := store.PutScheduledLaunch(ctx, &watcher.ScheduledLaunch{
		ScheduleID: "sl-ne", UserID: "arn:aws:iam::123456789012:user/al", Status: watcher.ScheduledPending,
	}); err != nil {
		t.Fatalf("PutScheduledLaunch: %v", err)
	}
	got, err := store.NonEmptyTables(ctx)
	if err != nil {
		t.Fatalf("NonEmptyTables: %v", err)
	}
	if len(got) != 1 || got[0] != "ne-scheduled" {
		t.Errorf("NonEmptyTables = %v, want [ne-scheduled]", got)
	}
	if empty, err := store.TablesEmpty(ctx); err != nil || empty {
		t.Errorf("TablesEmpty = %v, %v; want false", empty, err)
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
