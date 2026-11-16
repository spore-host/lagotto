package pollcadence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// fakeSchedule is one in-process EventBridge Scheduler schedule. UpdateSchedule
// is a full PUT, as in the service: the stored schedule becomes exactly the
// input, so a field the caller omits is lost.
type fakeSchedule struct {
	cur       scheduler.GetScheduleOutput
	getErr    error
	updateErr error
	updates   []*scheduler.UpdateScheduleInput
	// beforeUpdate, when set, runs once just before an UpdateSchedule is
	// applied: the point another program can act between this caller's read
	// and its write.
	beforeUpdate func()
}

func (f *fakeSchedule) GetSchedule(_ context.Context, _ *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	c := f.cur
	return &c, nil
}

func (f *fakeSchedule) UpdateSchedule(_ context.Context, in *scheduler.UpdateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error) {
	if hook := f.beforeUpdate; hook != nil {
		f.beforeUpdate = nil
		hook()
	}
	f.updates = append(f.updates, in)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	f.cur = scheduler.GetScheduleOutput{
		Name: in.Name, Description: in.Description, ScheduleExpression: in.ScheduleExpression,
		ScheduleExpressionTimezone: in.ScheduleExpressionTimezone, FlexibleTimeWindow: in.FlexibleTimeWindow,
		Target: in.Target, GroupName: in.GroupName, KmsKeyArn: in.KmsKeyArn, StartDate: in.StartDate,
		EndDate: in.EndDate, ActionAfterCompletion: in.ActionAfterCompletion, State: in.State,
	}
	return &scheduler.UpdateScheduleOutput{}, nil
}

// fakeEpoch is the poller-control item.
type fakeEpoch struct {
	epoch    int64
	mode     string
	readErr  error
	markErr  error
	reads    int
	afterGet func() // runs once after the first read, when set
}

func (f *fakeEpoch) PollerEpoch(context.Context) (int64, error) {
	f.reads++
	if f.readErr != nil {
		return 0, f.readErr
	}
	v := f.epoch
	if hook := f.afterGet; hook != nil {
		f.afterGet = nil
		hook()
	}
	return v, nil
}

func (f *fakeEpoch) MarkPollerIdle(_ context.Context, seen int64) (bool, error) {
	if f.markErr != nil {
		return false, f.markErr
	}
	if f.epoch != seen {
		return false, nil
	}
	f.mode = "idle"
	return true, nil
}

func (f *fakeEpoch) bump() { f.epoch++ }

func liveSchedule(expr string, state schedulertypes.ScheduleState) *fakeSchedule {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &fakeSchedule{cur: scheduler.GetScheduleOutput{
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
	}}
}

func idleIs(v bool) func(context.Context) (bool, error) {
	return func(context.Context) (bool, error) { return v, nil }
}

func wantSchedule(t *testing.T, f *fakeSchedule, expr string) {
	t.Helper()
	if !At(&f.cur, expr) {
		t.Errorf("schedule = %s %s, want ENABLED %s", f.cur.State, aws.ToString(f.cur.ScheduleExpression), expr)
	}
}

// TestReconcileIdleSlowsActiveRestores: idle switches the schedule to the idle
// cadence (still ENABLED), and activity switches it back.
func TestReconcileIdleSlowsActiveRestores(t *testing.T) {
	ctx := context.Background()
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	ep := &fakeEpoch{epoch: 3}

	res, err := Reconcile(ctx, sched, "lagotto-capacity-poller", ep, idleIs(true))
	if err != nil || res.Outcome != Idle || res.Changed != 1 {
		t.Fatalf("idle: got %+v, %v; want Idle with one update", res, err)
	}
	wantSchedule(t, sched, IdleExpression)
	if ep.mode != "idle" {
		t.Errorf("control item mode = %q, want idle", ep.mode)
	}
	if ep.reads != 2 {
		t.Errorf("epoch read %d times, want 2 (before counting, and after slowing)", ep.reads)
	}

	// Idle again: already slow, nothing to write.
	res, err = Reconcile(ctx, sched, "lagotto-capacity-poller", ep, idleIs(true))
	if err != nil || res.Outcome != Idle || res.Changed != 0 {
		t.Errorf("idle again: got %+v, %v; want Idle with no update", res, err)
	}

	res, err = Reconcile(ctx, sched, "lagotto-capacity-poller", ep, idleIs(false))
	if err != nil || res.Outcome != Active || res.Changed != 1 {
		t.Fatalf("active: got %+v, %v; want Active with one update", res, err)
	}
	wantSchedule(t, sched, NormalExpression)

	// Active at the normal cadence: no write.
	n := len(sched.updates)
	if _, err := Reconcile(ctx, sched, "lagotto-capacity-poller", ep, idleIs(false)); err != nil || len(sched.updates) != n {
		t.Errorf("active at the normal cadence wrote the schedule (%v)", err)
	}
}

// TestReconcileNeverDisables: across idle and active runs, no update ever
// carries DISABLED (#323: disabling is what lost watches).
func TestReconcileNeverDisables(t *testing.T) {
	ctx := context.Background()
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	ep := &fakeEpoch{}
	for _, idle := range []bool{true, true, false, true, false} {
		if _, err := Reconcile(ctx, sched, "p", ep, idleIs(idle)); err != nil {
			t.Fatal(err)
		}
	}
	for _, u := range sched.updates {
		if u.State != schedulertypes.ScheduleStateEnabled {
			t.Errorf("an update set State %s", u.State)
		}
	}
}

// TestReconcileEnablesLegacyDisabled: a schedule a poller older than #323
// disabled is enabled when there is work.
func TestReconcileEnablesLegacyDisabled(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateDisabled)
	res, err := Reconcile(context.Background(), sched, "p", &fakeEpoch{}, idleIs(false))
	if err != nil || res.Outcome != Active || res.Changed != 1 {
		t.Fatalf("got %+v, %v", res, err)
	}
	wantSchedule(t, sched, NormalExpression)
}

// TestReconcileRoundTripsTheSchedule: the slow-down is a full-round-trip
// UpdateSchedule. The old disableSchedule re-sent five fields and dropped the
// Description (and the timezone, group, key, dates, action after completion).
func TestReconcileRoundTripsTheSchedule(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	want := sched.cur
	if _, err := Reconcile(context.Background(), sched, "p", &fakeEpoch{}, idleIs(true)); err != nil {
		t.Fatal(err)
	}
	if len(sched.updates) != 1 {
		t.Fatalf("%d updates, want 1", len(sched.updates))
	}
	u := sched.updates[0]
	if aws.ToString(u.Description) != aws.ToString(want.Description) {
		t.Errorf("Description = %q, want %q kept", aws.ToString(u.Description), aws.ToString(want.Description))
	}
	if aws.ToString(u.Name) != aws.ToString(want.Name) || aws.ToString(u.ScheduleExpressionTimezone) != "UTC" ||
		u.FlexibleTimeWindow != want.FlexibleTimeWindow || u.Target != want.Target ||
		aws.ToString(u.GroupName) != "default" || aws.ToString(u.KmsKeyArn) != aws.ToString(want.KmsKeyArn) ||
		u.StartDate != want.StartDate || u.ActionAfterCompletion != want.ActionAfterCompletion {
		t.Errorf("UpdateSchedule did not round-trip the schedule: %+v", u)
	}
	if aws.ToString(u.ScheduleExpression) != IdleExpression || u.State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("update = %s %s, want ENABLED %s", u.State, aws.ToString(u.ScheduleExpression), IdleExpression)
	}
}

// TestReconcileConditionFailsWhenArmedMidCheck: a watch armed after the
// poller read epoch E and counted nothing moves the epoch, so the poller's
// conditional mode=idle on epoch = E fails and it does not slow down.
func TestReconcileConditionFailsWhenArmedMidCheck(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	ep := &fakeEpoch{epoch: 7}
	idle := func(context.Context) (bool, error) {
		// Counted zero; then a watch is stored and its arming bumps the epoch.
		ep.bump()
		return true, nil
	}
	res, err := Reconcile(context.Background(), sched, "p", ep, idle)
	if err != nil || res.Outcome != Armed || res.Changed != 0 {
		t.Fatalf("got %+v, %v; want Armed with no update", res, err)
	}
	wantSchedule(t, sched, NormalExpression)
	if ep.mode == "idle" {
		t.Error("mode=idle was recorded although the epoch moved")
	}
}

// TestReconcileReReadRestoresAfterOverwrite: the arming lands between the
// poller's conditional write and its slow-down, and the slow-down overwrites
// the arming's normal cadence. The re-read sees the epoch moved and restores
// it.
func TestReconcileReReadRestoresAfterOverwrite(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	ep := &fakeEpoch{epoch: 1}
	sched.beforeUpdate = func() {
		// The arming program: bump, then set the normal cadence (already
		// there, so its write is a no-op), all before the poller's write lands.
		ep.bump()
	}
	res, err := Reconcile(context.Background(), sched, "p", ep, idleIs(true))
	if err != nil || res.Outcome != Armed || res.Changed != 2 {
		t.Fatalf("got %+v, %v; want Armed after slowing and restoring", res, err)
	}
	wantSchedule(t, sched, NormalExpression)
}

// TestReconcileUnguardedLeavesSchedule: an idle poller that cannot read or
// conditionally write the epoch does not slow down (it could undo an arming),
// and says why.
func TestReconcileUnguardedLeavesSchedule(t *testing.T) {
	for name, ep := range map[string]*fakeEpoch{
		"read":  {readErr: errors.New("throttled")},
		"write": {markErr: errors.New("throttled")},
	} {
		sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
		res, err := Reconcile(context.Background(), sched, "p", ep, idleIs(true))
		if err == nil || res.Outcome != Unguarded || len(sched.updates) != 0 {
			t.Errorf("%s: got %+v, %v, %d updates; want Unguarded, an error, no update", name, res, err, len(sched.updates))
		}
	}
}

// TestReconcileFailSafeWhenReReadFails: when the re-read after slowing fails,
// the poller cannot tell whether it overwrote an arming, so it restores the
// normal cadence.
func TestReconcileFailSafeWhenReReadFails(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	ep := &fakeEpoch{}
	ep.afterGet = func() { ep.readErr = errors.New("throttled") }
	res, err := Reconcile(context.Background(), sched, "p", ep, idleIs(true))
	if err == nil || res.Outcome != Armed {
		t.Fatalf("got %+v, %v; want Armed and the re-read error", res, err)
	}
	wantSchedule(t, sched, NormalExpression)
}

// TestReconcileCountErrorLeavesSchedule: an unknown count changes nothing.
func TestReconcileCountErrorLeavesSchedule(t *testing.T) {
	sched := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	boom := errors.New("query failed")
	_, err := Reconcile(context.Background(), sched, "p", &fakeEpoch{}, func(context.Context) (bool, error) { return false, boom })
	if !errors.Is(err, boom) || len(sched.updates) != 0 {
		t.Errorf("got %v with %d updates; want the count error and no update", err, len(sched.updates))
	}
}

// TestSetFromIsIdempotent: a schedule already ENABLED at the expression is not
// written; anything else is.
func TestSetFromIsIdempotent(t *testing.T) {
	ctx := context.Background()
	f := liveSchedule(NormalExpression, schedulertypes.ScheduleStateEnabled)
	if changed, err := SetFrom(ctx, f, &f.cur, NormalExpression); changed || err != nil || len(f.updates) != 0 {
		t.Errorf("already there: changed=%v err=%v updates=%d", changed, err, len(f.updates))
	}
	for _, st := range []schedulertypes.ScheduleState{schedulertypes.ScheduleStateDisabled, schedulertypes.ScheduleStateEnabled} {
		f := liveSchedule(IdleExpression, st)
		if changed, err := SetFrom(ctx, f, &f.cur, NormalExpression); !changed || err != nil {
			t.Errorf("%s idle: changed=%v err=%v", st, changed, err)
		}
		wantSchedule(t, f, NormalExpression)
	}
	f = liveSchedule(IdleExpression, schedulertypes.ScheduleStateEnabled)
	boom := errors.New("denied")
	f.updateErr = boom
	if changed, err := SetFrom(ctx, f, &f.cur, NormalExpression); changed || !errors.Is(err, boom) {
		t.Errorf("failed update: changed=%v err=%v, want the raw error", changed, err)
	}
}
