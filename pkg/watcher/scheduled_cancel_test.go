package watcher_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestCancelScheduledLaunch_StopsCountingAsPending: a cancelled launch no
// longer keeps the poller armed (#279), and the poller's boundary retry cannot
// write it back.
func TestCancelScheduledLaunch_StopsCountingAsPending(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	store := watcher.NewStore(env.AWSConfig, "cx-watches", "cx-history")
	store.SetScheduledTable("cx-scheduled")
	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		t.Fatalf("EnsureScheduledTable: %v", err)
	}

	for _, st := range []watcher.ScheduledLaunchStatus{watcher.ScheduledPending, watcher.ScheduledRetrying} {
		t.Run(string(st), func(t *testing.T) {
			id := "sl-" + string(st)
			sl := &watcher.ScheduledLaunch{ScheduleID: id, Status: st, Region: "us-east-1", Attempts: 2}
			if err := store.PutScheduledLaunch(ctx, sl); err != nil {
				t.Fatalf("PutScheduledLaunch: %v", err)
			}
			if got, err := store.HasPendingScheduledLaunches(ctx); err != nil || !got {
				t.Fatalf("before cancel: HasPendingScheduledLaunches = %v, %v; want true", got, err)
			}

			// The wrong expected status changes nothing.
			if err := store.CancelScheduledLaunch(ctx, id, watcher.ScheduledLaunched); !errors.Is(err, watcher.ErrScheduledLaunchChanged) {
				t.Fatalf("cancel from the wrong status: %v, want ErrScheduledLaunchChanged", err)
			}
			if got, _ := store.GetScheduledLaunch(ctx, id); got.Status != st {
				t.Fatalf("a refused cancel changed the status to %s", got.Status)
			}

			if err := store.CancelScheduledLaunch(ctx, id, st); err != nil {
				t.Fatalf("CancelScheduledLaunch: %v", err)
			}
			got, err := store.GetScheduledLaunch(ctx, id)
			if err != nil || got.Status != watcher.ScheduledCancelled || got.UpdatedAt.IsZero() {
				t.Fatalf("after cancel: %+v, %v; want status cancelled with updated_at", got, err)
			}
			if p, err := store.HasPendingScheduledLaunches(ctx); err != nil || p {
				t.Fatalf("after cancel: HasPendingScheduledLaunches = %v, %v; want false", p, err)
			}

			// The poller's boundary retry rewrites the whole record; it must not
			// resurrect a cancelled one.
			sl.Status = watcher.ScheduledRetrying
			sl.Attempts++
			if err := store.PutScheduledLaunch(ctx, sl); !errors.Is(err, watcher.ErrScheduledLaunchCancelled) {
				t.Fatalf("re-put over a cancelled record: %v, want ErrScheduledLaunchCancelled", err)
			}
			if p, _ := store.HasPendingScheduledLaunches(ctx); p {
				t.Fatal("a refused re-put made the launch pending again")
			}
		})
	}
}
