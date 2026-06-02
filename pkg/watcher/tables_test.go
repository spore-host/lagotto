package watcher_test

import (
	"context"
	"testing"

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

// TestDeleteManagedTables_ConservativeWithoutTags verifies the tag-gated
// deleter never deletes when it can't confirm the lagotto:managed=cli tag.
// (Substrate doesn't emulate ListTagsOfResource, so isManaged returns false —
// which is exactly the safe behavior we want: it must not delete CFN-managed
// or untagged tables.)
func TestDeleteManagedTables_ConservativeWithoutTags(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := watcher.NewStore(env.AWSConfig, "mgd-watches", "mgd-history")
	ctx := context.Background()

	if _, err := store.EnsureTables(ctx); err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}

	deleted, err := store.DeleteManagedTables(ctx)
	if err != nil {
		t.Fatalf("DeleteManagedTables: %v", err)
	}
	if len(deleted) != 0 {
		t.Errorf("deleted %v; expected none when the managed tag can't be confirmed", deleted)
	}

	// Tables must still exist (not deleted).
	empty, err := store.TablesEmpty(ctx)
	if err != nil {
		t.Fatalf("TablesEmpty: %v", err)
	}
	if !empty {
		t.Error("tables should still exist and be empty")
	}
}
