package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/spore-host/lagotto/pkg/deploy"
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
