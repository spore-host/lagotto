package watcher_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// #309: a scheduled launch's config is swapped only while it is as the
// caller read it; a refused swap changes nothing.
func TestUpdateScheduledLaunchConfig(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	store := watcher.NewStore(env.AWSConfig, "su-watches", "su-history")
	store.SetScheduledTable("su-scheduled")
	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		t.Fatal(err)
	}
	sl := &watcher.ScheduledLaunch{ScheduleID: "o/sl-1", Status: watcher.ScheduledPending, Region: "us-east-1",
		LaunchConfigJSON: []byte(`{"iampolicydocument":"{}"}`)}
	if err := store.PutScheduledLaunch(ctx, sl); err != nil {
		t.Fatal(err)
	}
	// The stale-config half of the condition is asserted against a fake in
	// scheduled_update_internal_test.go: substrate v0.124.0 compares every
	// Binary value as equal in a ConditionExpression (scttfrdmn/substrate#1454).
	if err := store.UpdateScheduledLaunchConfig(ctx, sl, []byte(`{"iamrole":"r"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetScheduledLaunch(ctx, sl.ScheduleID)
	if err != nil || string(got.LaunchConfigJSON) != `{"iamrole":"r"}` || got.Status != watcher.ScheduledPending {
		t.Fatalf("after swap: %+v, %v", got, err)
	}

	if err := store.FailScheduledLaunch(ctx, sl.ScheduleID, "launch config refused: x"); err != nil {
		t.Fatal(err)
	}
	all, err := store.ListScheduledLaunches(ctx)
	if err != nil || len(all) != 1 || all[0].Status != watcher.ScheduledFailed || all[0].LastError != "launch config refused: x" {
		t.Fatalf("ListScheduledLaunches = %+v, %v", all, err)
	}
	// Failed since: the old swap no longer applies.
	if err := store.UpdateScheduledLaunchConfig(ctx, got, []byte(`{}`)); !errors.Is(err, watcher.ErrScheduledLaunchConfigChanged) {
		t.Errorf("swap after it failed: %v", err)
	}

	missing := watcher.NewStore(env.AWSConfig, "su-watches", "su-history")
	missing.SetScheduledTable("su-absent")
	if got, err := missing.ListScheduledLaunches(ctx); err != nil || got != nil {
		t.Errorf("missing table: %v, %v", got, err)
	}
}
