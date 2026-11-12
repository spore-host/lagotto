package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// fakeLaunchCanceller records what cancelScheduledLaunch did, in order.
type fakeLaunchCanceller struct {
	calls     []string
	markErr   error
	deleted   deploy.LaunchScheduleTeardown
	deleteErr error
	confirm   bool
}

func (f *fakeLaunchCanceller) canceller() scheduledLaunchCanceller {
	return scheduledLaunchCanceller{
		markCancelled: func(_ context.Context, id string, from watcher.ScheduledLaunchStatus) error {
			f.calls = append(f.calls, fmt.Sprintf("mark %s from %s", id, from))
			return f.markErr
		},
		deleteSchedules: func(_ context.Context, id, recorded string) (deploy.LaunchScheduleTeardown, error) {
			f.calls = append(f.calls, fmt.Sprintf("delete %s %s", id, recorded))
			return f.deleted, f.deleteErr
		},
		confirm: func(string) bool { f.calls = append(f.calls, "confirm"); return f.confirm },
	}
}

func pendingLaunch() *watcher.ScheduledLaunch {
	return &watcher.ScheduledLaunch{ScheduleID: "sl-aaaa", Status: watcher.ScheduledPending, Region: "us-east-1",
		ScheduleName: "lagotto-launch-sl-aaaa", InstanceName: "job"}
}

// Cancelling a pending or retrying launch marks it cancelled, then deletes its
// schedule and retries, and prints both.
func TestCancelScheduledLaunch_MarksThenDeletes(t *testing.T) {
	for _, st := range []watcher.ScheduledLaunchStatus{watcher.ScheduledPending, watcher.ScheduledRetrying} {
		f := &fakeLaunchCanceller{confirm: true, deleted: deploy.LaunchScheduleTeardown{
			Deleted: []string{"lagotto-launch-sl-aaaa-r2"},
			Absent:  []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-aaaa-r1"},
		}}
		sl := pendingLaunch()
		sl.Status = st
		var out bytes.Buffer
		if err := cancelScheduledLaunch(context.Background(), &out, &out, sl, f.canceller()); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		want := []string{"confirm", "mark sl-aaaa from " + string(st), "delete sl-aaaa lagotto-launch-sl-aaaa"}
		if strings.Join(f.calls, "|") != strings.Join(want, "|") {
			t.Errorf("%s: calls %v, want %v (mark before delete)", st, f.calls, want)
		}
		for _, line := range []string{"Cancelled scheduled launch sl-aaaa", "Deleted schedule lagotto-launch-sl-aaaa-r2",
			"Schedule lagotto-launch-sl-aaaa was already gone", "Schedule lagotto-launch-sl-aaaa-r1 was already gone"} {
			if !strings.Contains(out.String(), line) {
				t.Errorf("%s: output lacks %q:\n%s", st, line, out.String())
			}
		}
	}
}

// A one-shot that already fired is refused and nothing is touched; a cron that
// fired is still armed, so it is cancellable.
func TestCancelScheduledLaunch_FiredOneShotRefused(t *testing.T) {
	for _, st := range []watcher.ScheduledLaunchStatus{watcher.ScheduledLaunched, watcher.ScheduledFailed} {
		f := &fakeLaunchCanceller{confirm: true}
		sl := pendingLaunch()
		sl.Status = st
		if st == watcher.ScheduledLaunched {
			sl.InstanceID = "i-0abc"
		}
		err := cancelScheduledLaunch(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, sl, f.canceller())
		if err == nil || !strings.Contains(err.Error(), "already fired") {
			t.Errorf("%s one-shot: err = %v, want an 'already fired' refusal", st, err)
		}
		if st == watcher.ScheduledLaunched && !strings.Contains(err.Error(), "spawn terminate i-0abc") {
			t.Errorf("a launched one-shot's refusal should say how to terminate its instance: %v", err)
		}
		if len(f.calls) != 0 {
			t.Errorf("%s one-shot: refused, but did %v", st, f.calls)
		}

		f = &fakeLaunchCanceller{confirm: true}
		sl.CronExpr = "cron(0 9 ? * MON-FRI *)"
		if err := cancelScheduledLaunch(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, sl, f.canceller()); err != nil {
			t.Errorf("%s cron: %v, want it cancellable", st, err)
		}
		if len(f.calls) != 3 {
			t.Errorf("%s cron: calls %v", st, f.calls)
		}
	}
}

// An already-cancelled launch is not re-marked; its leftover schedules are
// deleted (the rerun a failed deletion asks for).
func TestCancelScheduledLaunch_AlreadyCancelledDeletesLeftovers(t *testing.T) {
	f := &fakeLaunchCanceller{deleted: deploy.LaunchScheduleTeardown{Deleted: []string{"lagotto-launch-sl-aaaa-r3"}}}
	sl := pendingLaunch()
	sl.Status = watcher.ScheduledCancelled
	var out bytes.Buffer
	if err := cancelScheduledLaunch(context.Background(), &out, &out, sl, f.canceller()); err != nil {
		t.Fatal(err)
	}
	if want := "delete sl-aaaa lagotto-launch-sl-aaaa"; len(f.calls) != 1 || f.calls[0] != want {
		t.Errorf("calls %v, want only %q", f.calls, want)
	}
	if !strings.Contains(out.String(), "already cancelled") || !strings.Contains(out.String(), "Deleted schedule lagotto-launch-sl-aaaa-r3") {
		t.Errorf("output:\n%s", out.String())
	}
}

// A launch that fired between the read and the cancel is not touched further,
// and an abort does nothing.
func TestCancelScheduledLaunch_ChangedOrAborted(t *testing.T) {
	f := &fakeLaunchCanceller{confirm: true, markErr: fmt.Errorf("x: %w", watcher.ErrScheduledLaunchChanged)}
	err := cancelScheduledLaunch(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, pendingLaunch(), f.canceller())
	if !errors.Is(err, watcher.ErrScheduledLaunchChanged) || !strings.Contains(err.Error(), "lagotto cancel sl-aaaa --region us-east-1") {
		t.Errorf("err = %v", err)
	}
	if len(f.calls) != 2 {
		t.Errorf("calls %v: nothing may be deleted when the cancel lost the race", f.calls)
	}

	f = &fakeLaunchCanceller{confirm: false}
	var out bytes.Buffer
	if err := cancelScheduledLaunch(context.Background(), &out, &out, pendingLaunch(), f.canceller()); err != nil || !strings.Contains(out.String(), "Aborted.") {
		t.Errorf("abort: %v, %q", err, out.String())
	}
	if len(f.calls) != 1 {
		t.Errorf("abort: calls %v", f.calls)
	}
}

// A failed deletion leaves the launch cancelled and says how to finish.
func TestCancelScheduledLaunch_DeleteFailure(t *testing.T) {
	f := &fakeLaunchCanceller{confirm: true, deleteErr: errors.New("AccessDeniedException")}
	err := cancelScheduledLaunch(context.Background(), &bytes.Buffer{}, &bytes.Buffer{}, pendingLaunch(), f.canceller())
	if err == nil || !strings.Contains(err.Error(), "is cancelled, but deleting its schedules failed") ||
		!strings.Contains(err.Error(), "lagotto cancel sl-aaaa --region us-east-1") {
		t.Errorf("err = %v", err)
	}
}

// `cancel` dispatches on the id: sl- is a scheduled launch, anything else is
// still a watch, exactly as before (#279).
func TestIsScheduledLaunchID(t *testing.T) {
	for id, want := range map[string]bool{"sl-3065113f": true, "w-3065113f": false, "3065113f": false, "": false} {
		if got := isScheduledLaunchID(id); got != want {
			t.Errorf("isScheduledLaunchID(%q) = %v, want %v", id, got, want)
		}
	}
}

// `launch` names the lagotto command to cancel, not a raw AWS call.
func TestCancelLaunchCmd(t *testing.T) {
	if got := cancelLaunchCmd("sl-3065113f", "us-east-1"); got != "lagotto cancel sl-3065113f --region us-east-1" {
		t.Errorf("cancelLaunchCmd = %q", got)
	}
}

type fixedIdentity string

func (f fixedIdentity) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Arn: aws.String(string(f))}, nil
}

// Another principal's scheduled launch reads as not found, as a watch does.
func TestAuthorizeLaunchOwner(t *testing.T) {
	ctx := context.Background()
	sl := pendingLaunch()
	sl.UserID = "arn:aws:iam::123456789012:user/alice"
	if got, err := authorizeLaunchOwner(ctx, fixedIdentity(sl.UserID), "sl-aaaa", "us-east-1", sl); err != nil || got != sl {
		t.Errorf("owner: %v, %v", got, err)
	}
	if _, err := authorizeLaunchOwner(ctx, fixedIdentity("arn:aws:iam::123456789012:user/bob"), "sl-aaaa", "us-east-1", sl); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Errorf("non-owner: %v, want not found", err)
	}
	if _, err := authorizeLaunchOwner(ctx, fixedIdentity(sl.UserID), "sl-aaaa", "us-east-1", nil); err == nil ||
		!strings.Contains(err.Error(), "--region") {
		t.Errorf("missing: %v, want not found naming --region", err)
	}
}

// TestRunCancelScheduledLaunch_Substrate drives `cancel sl-…` end to end
// against the emulator: the record is cancelled, the launch's schedule and its
// retry are deleted, another launch's schedule is left alone, and an unknown
// id (or a region with no table) reads as not found.
func TestRunCancelScheduledLaunch_Substrate(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	cfg := env.AWSConfig
	prev := cancelYes
	cancelYes = true
	t.Cleanup(func() { cancelYes = prev })

	var out bytes.Buffer
	if err := runCancelScheduledLaunch(ctx, &out, &out, cfg, "sl-nothere"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("no table: %v, want not found", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)
	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutScheduledLaunch(ctx, &watcher.ScheduledLaunch{ScheduleID: "sl-e2e1", UserID: aws.ToString(id.Arn),
		Status: watcher.ScheduledRetrying, Region: cfg.Region, ScheduleName: "lagotto-launch-sl-e2e1"}); err != nil {
		t.Fatal(err)
	}
	sc := scheduler.NewFromConfig(cfg)
	for _, n := range []string{"lagotto-launch-sl-e2e1-r1", "lagotto-launch-sl-e2e2"} {
		if _, err := sc.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
			Name: aws.String(n), ScheduleExpression: aws.String("at(2099-01-01T00:00:00)"),
			FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
			Target: &schedulertypes.Target{Arn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:lagotto-capacity-poller"),
				RoleArn: aws.String("arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke")},
		}); err != nil {
			t.Fatalf("CreateSchedule %s: %v", n, err)
		}
	}

	out.Reset()
	if err := runCancelScheduledLaunch(ctx, &out, &out, cfg, "sl-e2e1"); err != nil {
		t.Fatalf("cancel: %v\n%s", err, out.String())
	}
	got, err := store.GetScheduledLaunch(ctx, "sl-e2e1")
	if err != nil || got.Status != watcher.ScheduledCancelled {
		t.Fatalf("record after cancel: %+v, %v", got, err)
	}
	if pending, _ := store.HasPendingScheduledLaunches(ctx); pending {
		t.Error("the cancelled launch still counts as pending")
	}
	if !strings.Contains(out.String(), "Deleted schedule lagotto-launch-sl-e2e1-r1") {
		t.Errorf("output:\n%s", out.String())
	}
	if _, err := sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("lagotto-launch-sl-e2e1-r1")}); err == nil {
		t.Error("the retry schedule survived the cancel")
	}
	if _, err := sc.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String("lagotto-launch-sl-e2e2")}); err != nil {
		t.Errorf("another launch's schedule was touched: %v", err)
	}

	if err := runCancelScheduledLaunch(ctx, &out, &out, cfg, "sl-unknown"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("unknown id: %v, want not found", err)
	}
}

type fullIdentity struct{ arn, userID string }

func (f fullIdentity) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Arn: aws.String(f.arn), UserId: aws.String(f.userID)}, nil
}

// A record with an owner id is owned by that principal id alone; a legacy
// watch that passes on its ARN gets the caller's id noted for `update`.
func TestAuthorizeOwnerByPrincipalID(t *testing.T) {
	ctx := context.Background()
	const arn = "arn:aws:sts::123456789012:assumed-role/Dev/alice"
	sl := pendingLaunch()
	sl.UserID, sl.OwnerID = arn, "AROAOLD:alice"
	if _, err := authorizeLaunchOwner(ctx, fullIdentity{arn, "AROANEW:alice"}, "sl-aaaa", "us-east-1", sl); err == nil {
		t.Error("a different principal id with the same ARN owns the launch")
	}
	if _, err := authorizeLaunchOwner(ctx, fullIdentity{arn, "AROAOLD:alice"}, "sl-aaaa", "us-east-1", sl); err != nil {
		t.Errorf("owner: %v", err)
	}

	w := &watcher.Watch{WatchID: "w-1", UserID: arn}
	got, err := authorizeWatchOwner(ctx, fullIdentity{arn, "AROAOLD:alice"}, "w-1", w)
	if err != nil || got.OwnerID != "AROAOLD:alice" || !got.OwnerIDBackfill {
		t.Errorf("legacy watch: %+v, %v; want the owner id noted for backfill", got, err)
	}
	w2 := &watcher.Watch{WatchID: "w-2", UserID: arn, OwnerID: "AROAOLD:alice"}
	if _, err := authorizeWatchOwner(ctx, fullIdentity{arn, "AROANEW:alice"}, "w-2", w2); err == nil {
		t.Error("a different principal id with the same ARN owns the watch")
	}
}
