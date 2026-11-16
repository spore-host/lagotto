package arm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/pollcadence"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// fakeSchedule is an in-process PollScheduleAPI.
type fakeSchedule struct {
	get       *scheduler.GetScheduleOutput
	getErr    error
	updateErr error
	gets      []string
	updates   []*scheduler.UpdateScheduleInput
}

func (f *fakeSchedule) GetSchedule(_ context.Context, in *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	f.gets = append(f.gets, aws.ToString(in.Name))
	return f.get, f.getErr
}

func (f *fakeSchedule) UpdateSchedule(_ context.Context, in *scheduler.UpdateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error) {
	f.updates = append(f.updates, in)
	return &scheduler.UpdateScheduleOutput{}, f.updateErr
}

var (
	_ PollScheduleAPI = (*scheduler.Client)(nil)
	_ PollScheduleAPI = (*fakeSchedule)(nil)
)

func denied() error {
	return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"}
}

func TestPollScheduleName(t *testing.T) {
	t.Setenv(ScheduleNameEnvVar, "")
	if got := PollScheduleName(); got != deploy.PollerScheduleName {
		t.Errorf("PollScheduleName() = %q, want deploy.PollerScheduleName", got)
	}
	t.Setenv(ScheduleNameEnvVar, "my-poller")
	if got := PollScheduleName(); got != "my-poller" {
		t.Errorf("PollScheduleName() with %s = %q, want my-poller", ScheduleNameEnvVar, got)
	}
}

// TestPreflightPollSchedule: not deployed is (nil, nil); any other error,
// AccessDenied included, is a refusal that names the call, the schedule and
// that nothing was changed.
func TestPreflightPollSchedule(t *testing.T) {
	t.Setenv(ScheduleNameEnvVar, "")
	ctx := context.Background()

	out := &scheduler.GetScheduleOutput{Name: aws.String("lagotto-capacity-poller"), State: schedulertypes.ScheduleStateEnabled}
	f := &fakeSchedule{get: out}
	got, err := PreflightPollSchedule(ctx, f, "create watch w-1")
	if err != nil || got != out {
		t.Fatalf("deployed: got (%v, %v), want the schedule", got, err)
	}
	if len(f.gets) != 1 || f.gets[0] != "lagotto-capacity-poller" {
		t.Errorf("GetSchedule calls = %v", f.gets)
	}

	f = &fakeSchedule{getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("no")}}
	if got, err := PreflightPollSchedule(ctx, f, "create watch w-1"); got != nil || err != nil {
		t.Errorf("not deployed: got (%v, %v), want (nil, nil)", got, err)
	}

	f = &fakeSchedule{getErr: denied()}
	_, err = PreflightPollSchedule(ctx, f, "create watch w-1")
	if err == nil {
		t.Fatal("AccessDenied: no refusal")
	}
	for _, want := range []string{
		"cannot create watch w-1: scheduler:GetSchedule on the hosted poller's schedule lagotto-capacity-poller was denied (nothing was changed)",
		"iam:PassRole on role " + runtimeiam.SchedulerInvokeRoleName,
		"docs/reference/cli-iam-actions.md",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("AccessDenied refusal lacks %q:\n%v", want, err)
		}
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "AccessDeniedException" {
		t.Errorf("refusal does not wrap the API error: %v", err)
	}

	f = &fakeSchedule{getErr: errors.New("throttled")}
	_, err = PreflightPollSchedule(ctx, f, "extend watch w-2")
	want := "cannot extend watch w-2: scheduler:GetSchedule on the hosted poller's schedule lagotto-capacity-poller failed, so lagotto cannot make sure the poller will poll it (nothing was changed): throttled"
	if err == nil || err.Error() != want {
		t.Errorf("other error = %v, want %q", err, want)
	}
}

// TestEnablePollScheduleIfOff (deprecated since #323): only a DISABLED or
// idle-cadence schedule is touched, and the full PUT round-trips every field
// with State ENABLED.
func TestEnablePollScheduleIfOff(t *testing.T) {
	ctx := context.Background()

	for name, cur := range map[string]*scheduler.GetScheduleOutput{
		"not deployed": nil,
		"enabled":      {Name: aws.String("p"), State: schedulertypes.ScheduleStateEnabled},
	} {
		f := &fakeSchedule{}
		enabled, err := EnablePollScheduleIfOff(ctx, f, cur)
		if enabled || err != nil || len(f.updates) != 0 {
			t.Errorf("%s: got (%v, %v) with %d updates, want (false, nil) and none", name, enabled, err, len(f.updates))
		}
	}

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cur := &scheduler.GetScheduleOutput{
		Name:                       aws.String("lagotto-capacity-poller"),
		Description:                aws.String("d"),
		ScheduleExpression:         aws.String("rate(5 minutes)"),
		ScheduleExpressionTimezone: aws.String("UTC"),
		FlexibleTimeWindow:         &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target:                     &schedulertypes.Target{Arn: aws.String("arn:aws:lambda:us-east-1:1:function:p"), RoleArn: aws.String("arn:aws:iam::1:role/r")},
		GroupName:                  aws.String("default"),
		KmsKeyArn:                  aws.String("arn:aws:kms:us-east-1:1:key/k"),
		StartDate:                  &start,
		ActionAfterCompletion:      schedulertypes.ActionAfterCompletionNone,
		State:                      schedulertypes.ScheduleStateDisabled,
	}
	f := &fakeSchedule{}
	enabled, err := EnablePollScheduleIfOff(ctx, f, cur)
	if !enabled || err != nil {
		t.Fatalf("disabled: got (%v, %v), want (true, nil)", enabled, err)
	}
	if len(f.updates) != 1 {
		t.Fatalf("%d UpdateSchedule calls, want 1", len(f.updates))
	}
	u := f.updates[0]
	if u.State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("State = %s, want ENABLED", u.State)
	}
	if aws.ToString(u.Name) != "lagotto-capacity-poller" || aws.ToString(u.Description) != "d" ||
		aws.ToString(u.ScheduleExpression) != "rate(5 minutes)" || aws.ToString(u.ScheduleExpressionTimezone) != "UTC" ||
		u.FlexibleTimeWindow != cur.FlexibleTimeWindow || u.Target != cur.Target || aws.ToString(u.GroupName) != "default" ||
		aws.ToString(u.KmsKeyArn) != aws.ToString(cur.KmsKeyArn) || u.StartDate != cur.StartDate ||
		u.ActionAfterCompletion != cur.ActionAfterCompletion {
		t.Errorf("UpdateSchedule did not round-trip the schedule: %+v", u)
	}

	boom := errors.New("boom")
	f = &fakeSchedule{updateErr: boom}
	cur.State = schedulertypes.ScheduleStateDisabled
	enabled, err = EnablePollScheduleIfOff(ctx, f, cur)
	if enabled || !errors.Is(err, boom) {
		t.Errorf("failed update: got (%v, %v), want (false, the raw error)", enabled, err)
	}
}

// TestPollScheduleUpdateRefusal: the refusal reports the caller's rollback
// state, or, when the rollback failed, that the watch is active and unpolled.
func TestPollScheduleUpdateRefusal(t *testing.T) {
	t.Setenv(ScheduleNameEnvVar, "")
	err := PollScheduleUpdateRefusal("create watch w-1", "no watch was created", nil, errors.New("boom"))
	want := "cannot create watch w-1: scheduler:UpdateSchedule on the hosted poller's schedule lagotto-capacity-poller failed, so lagotto cannot make sure the poller will poll it (no watch was created): boom"
	if err.Error() != want {
		t.Errorf("refusal = %q, want %q", err, want)
	}

	err = PollScheduleUpdateRefusal("create watch w-1", "no watch was created", errors.New("put failed"), denied())
	for _, want := range []string{
		"scheduler:UpdateSchedule on the hosted poller's schedule lagotto-capacity-poller was denied",
		"the watch could NOT be rolled back (put failed): it is active but will not be polled — cancel it with 'lagotto cancel'",
		"scheduler:GetSchedule and scheduler:UpdateSchedule on schedule lagotto-capacity-poller",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), "no watch was created") {
		t.Errorf("a failed rollback must not claim no watch was created:\n%v", err)
	}
}

// fullSchedule is a schedule carrying every field UpdateSchedule must
// round-trip.
func fullSchedule(expr string, state schedulertypes.ScheduleState) *scheduler.GetScheduleOutput {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &scheduler.GetScheduleOutput{
		Name:                       aws.String("lagotto-capacity-poller"),
		Description:                aws.String("Polls for EC2 capacity matching active lagotto watches"),
		ScheduleExpression:         aws.String(expr),
		ScheduleExpressionTimezone: aws.String("UTC"),
		FlexibleTimeWindow:         &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target:                     &schedulertypes.Target{Arn: aws.String("arn:aws:lambda:us-east-1:1:function:p"), RoleArn: aws.String("arn:aws:iam::1:role/r")},
		GroupName:                  aws.String("default"),
		KmsKeyArn:                  aws.String("arn:aws:kms:us-east-1:1:key/k"),
		StartDate:                  &start,
		ActionAfterCompletion:      schedulertypes.ActionAfterCompletionNone,
		State:                      state,
	}
}

// TestEnablePollScheduleIfOffIdleCadence: the deprecated helper also sets an
// idle-cadence schedule back to every 5 minutes, so a caller not yet on
// ArmPollSchedule (spore-host-mcp) still un-slows an idle poller.
func TestEnablePollScheduleIfOffIdleCadence(t *testing.T) {
	f := &fakeSchedule{}
	enabled, err := EnablePollScheduleIfOff(context.Background(), f, fullSchedule(pollcadence.IdleExpression, schedulertypes.ScheduleStateEnabled))
	if !enabled || err != nil || len(f.updates) != 1 {
		t.Fatalf("got (%v, %v) with %d updates, want one update", enabled, err, len(f.updates))
	}
	if u := f.updates[0]; aws.ToString(u.ScheduleExpression) != pollcadence.NormalExpression || u.State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("update = %s %s, want ENABLED %s", u.State, aws.ToString(u.ScheduleExpression), pollcadence.NormalExpression)
	}
}

// TestArmPollSchedule (#323): ArmPollSchedule re-reads the schedule (the
// preflight read predates the persist) and makes it ENABLED every 5 minutes:
// a legacy DISABLED schedule and an idle-cadence one are set back, with every
// field round-tripped; one already there is left alone; no poller, nothing.
func TestArmPollSchedule(t *testing.T) {
	ctx := context.Background()
	stale := fullSchedule(pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled)

	if changed, err := ArmPollSchedule(ctx, &fakeSchedule{}, nil); changed || err != nil {
		t.Errorf("not deployed: got (%v, %v), want (false, nil)", changed, err)
	}

	for name, fresh := range map[string]*scheduler.GetScheduleOutput{
		"legacy DISABLED": fullSchedule(pollcadence.NormalExpression, schedulertypes.ScheduleStateDisabled),
		"idle cadence":    fullSchedule(pollcadence.IdleExpression, schedulertypes.ScheduleStateEnabled),
		"disabled idle":   fullSchedule(pollcadence.IdleExpression, schedulertypes.ScheduleStateDisabled),
	} {
		// The preflight read said ENABLED every 5 minutes; the schedule changed
		// since. A helper trusting the preflight read would do nothing.
		f := &fakeSchedule{get: fresh}
		changed, err := ArmPollSchedule(ctx, f, stale)
		if !changed || err != nil || len(f.updates) != 1 {
			t.Fatalf("%s: got (%v, %v) with %d updates, want one update", name, changed, err, len(f.updates))
		}
		if len(f.gets) != 1 {
			t.Errorf("%s: %d GetSchedule calls, want the one re-read", name, len(f.gets))
		}
		u := f.updates[0]
		if aws.ToString(u.ScheduleExpression) != pollcadence.NormalExpression || u.State != schedulertypes.ScheduleStateEnabled {
			t.Errorf("%s: update = %s %s, want ENABLED %s", name, u.State, aws.ToString(u.ScheduleExpression), pollcadence.NormalExpression)
		}
		if aws.ToString(u.Description) != aws.ToString(fresh.Description) || aws.ToString(u.ScheduleExpressionTimezone) != "UTC" ||
			u.Target != fresh.Target || aws.ToString(u.GroupName) != "default" || aws.ToString(u.KmsKeyArn) != aws.ToString(fresh.KmsKeyArn) ||
			u.StartDate != fresh.StartDate || u.ActionAfterCompletion != fresh.ActionAfterCompletion {
			t.Errorf("%s: UpdateSchedule did not round-trip the schedule: %+v", name, u)
		}
	}

	f := &fakeSchedule{get: fullSchedule(pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled)}
	if changed, err := ArmPollSchedule(ctx, f, stale); changed || err != nil || len(f.updates) != 0 {
		t.Errorf("already armed: got (%v, %v) with %d updates, want no update", changed, err, len(f.updates))
	}

	boom := errors.New("boom")
	f = &fakeSchedule{get: fullSchedule(pollcadence.IdleExpression, schedulertypes.ScheduleStateEnabled), updateErr: boom}
	if changed, err := ArmPollSchedule(ctx, f, stale); changed || !errors.Is(err, boom) {
		t.Errorf("failed update: got (%v, %v), want (false, the raw error)", changed, err)
	}
	f = &fakeSchedule{getErr: boom}
	if changed, err := ArmPollSchedule(ctx, f, stale); changed || !errors.Is(err, boom) {
		t.Errorf("failed re-read: got (%v, %v), want (false, the raw error)", changed, err)
	}
}

type fakeBumper struct {
	err   error
	calls int
}

func (f *fakeBumper) BumpPollerEpoch(context.Context) error {
	f.calls++
	return f.err
}

// TestBumpPollerEpoch: with a poller deployed the epoch is bumped; a failure
// is a warning naming the policy to update, not a refusal; with no poller,
// nothing is written.
func TestBumpPollerEpoch(t *testing.T) {
	ctx := context.Background()
	cur := fullSchedule(pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled)

	b := &fakeBumper{}
	if msg := BumpPollerEpoch(ctx, b, nil); msg != "" || b.calls != 0 {
		t.Errorf("not deployed: %q with %d calls, want nothing", msg, b.calls)
	}
	if msg := BumpPollerEpoch(ctx, b, cur); msg != "" || b.calls != 1 {
		t.Errorf("deployed: %q with %d calls, want one silent bump", msg, b.calls)
	}
	b = &fakeBumper{err: denied()}
	msg := BumpPollerEpoch(ctx, b, cur)
	for _, want := range []string{"Warning:", "up to an hour", "lagotto-cli-user", "lagotto setup"} {
		if !strings.Contains(msg, want) {
			t.Errorf("warning lacks %q: %q", want, msg)
		}
	}
}
