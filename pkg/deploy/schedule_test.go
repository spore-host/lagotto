package deploy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

var (
	testFnARN   = PollerFunctionARN(testRegion, testAccount)
	testRoleARN = schedulerInvokeRoleARN(testAccount)
)

// liveSchedule returns a GetScheduleOutput matching the desired shape, in the
// given state.
func liveSchedule(state schedulertypes.ScheduleState) *scheduler.GetScheduleOutput {
	return &scheduler.GetScheduleOutput{
		Name:               strptr(PollerScheduleName),
		Description:        strptr(pollerScheduleDescription),
		ScheduleExpression: strptr(pollerScheduleExpression),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		GroupName:          strptr("default"),
		State:              state,
		Target: &schedulertypes.Target{
			Arn:     strptr(testFnARN),
			RoleArn: strptr(testRoleARN),
		},
	}
}

// TestEnsurePollerSchedule_CreatesDisabledWhenAbsent: on a fresh deploy nothing
// is watching yet, so the schedule must start DISABLED — otherwise the poller
// starts billing invocations immediately. `lagotto watch` enables it.
func TestEnsurePollerSchedule_CreatesDisabledWhenAbsent(t *testing.T) {
	fsc := &fakeScheduler{getErr: &schedulertypes.ResourceNotFoundException{}}
	d, _ := testDeployer(nil, nil, fsc)

	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if action != ActionCreated {
		t.Errorf("action = %q, want %q", action, ActionCreated)
	}
	if len(fsc.createCalls) != 1 {
		t.Fatalf("CreateSchedule called %d times, want 1", len(fsc.createCalls))
	}
	c := fsc.createCalls[0]
	if c.State != schedulertypes.ScheduleStateDisabled {
		t.Errorf("State = %q, want DISABLED on create", c.State)
	}
	if aws.ToString(c.Name) != PollerScheduleName {
		t.Errorf("Name = %q, want %q", aws.ToString(c.Name), PollerScheduleName)
	}
	if aws.ToString(c.ScheduleExpression) != "rate(5 minutes)" {
		t.Errorf("ScheduleExpression = %q, want rate(5 minutes)", aws.ToString(c.ScheduleExpression))
	}
	if c.FlexibleTimeWindow == nil || c.FlexibleTimeWindow.Mode != schedulertypes.FlexibleTimeWindowModeOff {
		t.Errorf("FlexibleTimeWindow = %+v, want Mode OFF", c.FlexibleTimeWindow)
	}
	if c.Target == nil || aws.ToString(c.Target.Arn) != testFnARN || aws.ToString(c.Target.RoleArn) != testRoleARN {
		t.Errorf("Target = %+v, want arn %q role %q", c.Target, testFnARN, testRoleARN)
	}
	if aws.ToString(c.Description) == "" {
		t.Error("Description is empty; the template set one")
	}
	if len(fsc.updateCalls) != 0 {
		t.Errorf("UpdateSchedule called %d times on a create, want 0", len(fsc.updateCalls))
	}
}

// TestEnsurePollerSchedule_EnabledAndConvergedIssuesNoCall is one of the two
// highest-value tests in this change. An ENABLED schedule (a live watch is being
// polled) whose every other field already matches must produce NO API call at
// all — in particular no UpdateSchedule that could carry a state.
func TestEnsurePollerSchedule_EnabledAndConvergedIssuesNoCall(t *testing.T) {
	fsc := &fakeScheduler{getOut: liveSchedule(schedulertypes.ScheduleStateEnabled)}
	d, _ := testDeployer(nil, nil, fsc)

	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if action != ActionUnchanged {
		t.Errorf("action = %q, want %q", action, ActionUnchanged)
	}
	if len(fsc.updateCalls) != 0 {
		t.Errorf("UpdateSchedule called %d times against a converged ENABLED schedule, want 0", len(fsc.updateCalls))
	}
	if len(fsc.createCalls) != 0 {
		t.Errorf("CreateSchedule called %d times against an existing schedule, want 0", len(fsc.createCalls))
	}
}

// TestEnsurePollerSchedule_PreservesEnabledStateOnUpdate is the other one, and
// the single most valuable guarantee in #154: `lagotto deploy` must NEVER be able
// to turn off a running poller. The state is owned out of band
// (cmd/watch.go's enablePollingSchedule, cmd/extend.go, and the poller's own
// self-disable), so a convergence update has to pass the CURRENT state through —
// never the template's literal DISABLED.
func TestEnsurePollerSchedule_PreservesEnabledStateOnUpdate(t *testing.T) {
	cur := liveSchedule(schedulertypes.ScheduleStateEnabled)
	cur.Target.Arn = strptr("arn:aws:lambda:us-east-1:123456789012:function:some-stale-target")
	fsc := &fakeScheduler{getOut: cur}
	d, _ := testDeployer(nil, nil, fsc)

	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if action != ActionConfigUpdated {
		t.Errorf("action = %q, want %q", action, ActionConfigUpdated)
	}
	if len(fsc.updateCalls) != 1 {
		t.Fatalf("UpdateSchedule called %d times, want 1", len(fsc.updateCalls))
	}
	u := fsc.updateCalls[0]
	if u.State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("UpdateSchedule State = %q, want ENABLED preserved — deploy must never disable a running poller", u.State)
	}
	if aws.ToString(u.Target.Arn) != testFnARN {
		t.Errorf("Target.Arn = %q, want the corrected %q", aws.ToString(u.Target.Arn), testFnARN)
	}
	if aws.ToString(u.Target.RoleArn) != testRoleARN {
		t.Errorf("Target.RoleArn = %q, want %q", aws.ToString(u.Target.RoleArn), testRoleARN)
	}
	// A full PUT: every required field must be resupplied or it is dropped.
	if aws.ToString(u.ScheduleExpression) != pollerScheduleExpression {
		t.Errorf("ScheduleExpression = %q", aws.ToString(u.ScheduleExpression))
	}
	if u.FlexibleTimeWindow == nil || u.FlexibleTimeWindow.Mode != schedulertypes.FlexibleTimeWindowModeOff {
		t.Errorf("FlexibleTimeWindow = %+v", u.FlexibleTimeWindow)
	}
}

// TestEnsurePollerSchedule_PreservesDisabledStateOnUpdate: the mirror image — an
// idle (self-disabled) poller must not be switched on by a deploy either.
func TestEnsurePollerSchedule_PreservesDisabledStateOnUpdate(t *testing.T) {
	cur := liveSchedule(schedulertypes.ScheduleStateDisabled)
	cur.ScheduleExpression = strptr("rate(1 hour)")
	fsc := &fakeScheduler{getOut: cur}
	d, _ := testDeployer(nil, nil, fsc)

	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if len(fsc.updateCalls) != 1 {
		t.Fatalf("UpdateSchedule called %d times, want 1", len(fsc.updateCalls))
	}
	if fsc.updateCalls[0].State != schedulertypes.ScheduleStateDisabled {
		t.Errorf("State = %q, want DISABLED preserved", fsc.updateCalls[0].State)
	}
}

// TestEnsurePollerSchedule_RoundTripsUserTweaks: UpdateSchedule is a full PUT, so
// anything not resupplied is silently dropped. A user's own target Input, retry
// policy, DLQ, timezone, group, CMK, dates and action-after-completion must
// survive a convergence update.
func TestEnsurePollerSchedule_RoundTripsUserTweaks(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	cur := liveSchedule(schedulertypes.ScheduleStateEnabled)
	cur.Target.Arn = strptr("arn:aws:lambda:us-east-1:123456789012:function:stale")
	cur.Target.Input = strptr(`{"hand":"written"}`)
	cur.Target.RetryPolicy = &schedulertypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(3)}
	cur.Target.DeadLetterConfig = &schedulertypes.DeadLetterConfig{Arn: strptr("arn:aws:sqs:us-east-1:123456789012:dlq")}
	cur.ScheduleExpressionTimezone = strptr("America/New_York")
	cur.GroupName = strptr("my-group")
	cur.KmsKeyArn = strptr("arn:aws:kms:us-east-1:123456789012:key/abc")
	cur.StartDate = &start
	cur.EndDate = &end
	cur.ActionAfterCompletion = schedulertypes.ActionAfterCompletionNone

	fsc := &fakeScheduler{getOut: cur}
	d, _ := testDeployer(nil, nil, fsc)
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if len(fsc.updateCalls) != 1 {
		t.Fatalf("UpdateSchedule called %d times, want 1", len(fsc.updateCalls))
	}
	u := fsc.updateCalls[0]
	if aws.ToString(u.Target.Input) != `{"hand":"written"}` {
		t.Errorf("Target.Input dropped: %q", aws.ToString(u.Target.Input))
	}
	if u.Target.RetryPolicy == nil || aws.ToInt32(u.Target.RetryPolicy.MaximumRetryAttempts) != 3 {
		t.Errorf("Target.RetryPolicy dropped: %+v", u.Target.RetryPolicy)
	}
	if u.Target.DeadLetterConfig == nil {
		t.Error("Target.DeadLetterConfig dropped")
	}
	if aws.ToString(u.ScheduleExpressionTimezone) != "America/New_York" {
		t.Errorf("ScheduleExpressionTimezone dropped: %q", aws.ToString(u.ScheduleExpressionTimezone))
	}
	if aws.ToString(u.GroupName) != "my-group" {
		t.Errorf("GroupName dropped: %q", aws.ToString(u.GroupName))
	}
	if aws.ToString(u.KmsKeyArn) == "" {
		t.Error("KmsKeyArn dropped")
	}
	if u.StartDate == nil || !u.StartDate.Equal(start) || u.EndDate == nil || !u.EndDate.Equal(end) {
		t.Errorf("StartDate/EndDate dropped: %v / %v", u.StartDate, u.EndDate)
	}
	if u.ActionAfterCompletion != schedulertypes.ActionAfterCompletionNone {
		t.Errorf("ActionAfterCompletion dropped: %q", u.ActionAfterCompletion)
	}
}

// TestEnsurePollerSchedule_ConflictOnCreateFallsThroughToUpdate: two deploys
// racing, or a GetSchedule that lost a read race, make CreateSchedule answer
// ConflictException. That means "it already exists" — converge, don't error.
func TestEnsurePollerSchedule_ConflictOnCreateFallsThroughToUpdate(t *testing.T) {
	fsc := &conflictOnceScheduler{
		existing: liveSchedule(schedulertypes.ScheduleStateEnabled),
	}
	// The existing schedule has a stale target, so convergence must actually fire.
	fsc.existing.Target.Arn = strptr("arn:aws:lambda:us-east-1:123456789012:function:stale")
	d, _ := testDeployer(nil, nil, fsc)

	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil {
		t.Fatalf("a raced create must fall through to the update path, got %v", err)
	}
	if action != ActionConfigUpdated {
		t.Errorf("action = %q, want %q", action, ActionConfigUpdated)
	}
	if len(fsc.updateCalls) != 1 {
		t.Fatalf("UpdateSchedule called %d times, want 1", len(fsc.updateCalls))
	}
	if fsc.updateCalls[0].State != schedulertypes.ScheduleStateEnabled {
		t.Errorf("State = %q — even the raced path must preserve ENABLED", fsc.updateCalls[0].State)
	}
}

// conflictOnceScheduler answers the first GetSchedule with ResourceNotFound (so
// the create path is taken), the CreateSchedule with ConflictException, and every
// later GetSchedule with the existing schedule.
type conflictOnceScheduler struct {
	fakeScheduler
	existing *scheduler.GetScheduleOutput
}

func (f *conflictOnceScheduler) GetSchedule(_ context.Context, _ *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	f.getCalls++
	if f.getCalls == 1 {
		return nil, &schedulertypes.ResourceNotFoundException{}
	}
	return f.existing, nil
}

func (f *conflictOnceScheduler) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	f.createCalls = append(f.createCalls, in)
	return nil, &schedulertypes.ConflictException{Message: aws.String("Schedule lagotto-capacity-poller already exists")}
}

func TestEnsurePollerSchedule_GetErrorPropagates(t *testing.T) {
	fsc := &fakeScheduler{getErr: errors.New("AccessDeniedException")}
	d, _ := testDeployer(nil, nil, fsc)
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err == nil {
		t.Fatal("expected the AccessDenied to propagate")
	}
	if len(fsc.createCalls) != 0 {
		t.Error("a non-NotFound GetSchedule error must not be treated as absent")
	}
}

func TestEnsurePollerSchedule_CreateErrorPropagates(t *testing.T) {
	fsc := &fakeScheduler{
		getErr:    &schedulertypes.ResourceNotFoundException{},
		createErr: errors.New("ValidationException: bad target"),
	}
	d, _ := testDeployer(nil, nil, fsc)
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err == nil {
		t.Error("expected the CreateSchedule failure to propagate")
	}
}

// TestScheduleDiffers_IgnoresState documents that State is deliberately outside
// the comparison — it is not deploy's to decide.
func TestScheduleDiffers_IgnoresState(t *testing.T) {
	for _, st := range []schedulertypes.ScheduleState{
		schedulertypes.ScheduleStateEnabled, schedulertypes.ScheduleStateDisabled,
	} {
		if scheduleDiffers(liveSchedule(st), testFnARN, testRoleARN) {
			t.Errorf("a converged schedule in state %s was reported as differing", st)
		}
	}
	cases := map[string]func(*scheduler.GetScheduleOutput){
		"expression":  func(s *scheduler.GetScheduleOutput) { s.ScheduleExpression = strptr("rate(1 hour)") },
		"description": func(s *scheduler.GetScheduleOutput) { s.Description = strptr("something else") },
		"window mode": func(s *scheduler.GetScheduleOutput) {
			s.FlexibleTimeWindow = &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeFlexible}
		},
		"nil window": func(s *scheduler.GetScheduleOutput) { s.FlexibleTimeWindow = nil },
		"target arn": func(s *scheduler.GetScheduleOutput) { s.Target.Arn = strptr("arn:aws:lambda:::function:other") },
		"role arn":   func(s *scheduler.GetScheduleOutput) { s.Target.RoleArn = strptr("arn:aws:iam::1:role/other") },
		"nil target": func(s *scheduler.GetScheduleOutput) { s.Target = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := liveSchedule(schedulertypes.ScheduleStateEnabled)
			mutate(s)
			if !scheduleDiffers(s, testFnARN, testRoleARN) {
				t.Errorf("a drifted %s was not detected", name)
			}
		})
	}
}

// TestDeletePollerSchedule_AbsentIsNotAnError: Teardown must be re-runnable.
func TestDeletePollerSchedule_AbsentIsNotAnError(t *testing.T) {
	fsc := &fakeScheduler{deleteErr: &schedulertypes.ResourceNotFoundException{}}
	d, _ := testDeployer(nil, nil, fsc)
	if err := d.deletePollerSchedule(context.Background()); err != nil {
		t.Errorf("deleting an absent schedule should be a no-op success, got %v", err)
	}

	fsc2 := &fakeScheduler{}
	d2, _ := testDeployer(nil, nil, fsc2)
	if err := d2.deletePollerSchedule(context.Background()); err != nil {
		t.Errorf("deletePollerSchedule: %v", err)
	}
	if fsc2.deleteCalls != 1 {
		t.Errorf("DeleteSchedule called %d times, want 1", fsc2.deleteCalls)
	}

	fsc3 := &fakeScheduler{deleteErr: errors.New("AccessDeniedException")}
	d3, _ := testDeployer(nil, nil, fsc3)
	if err := d3.deletePollerSchedule(context.Background()); err == nil {
		t.Error("a real delete failure must surface")
	}
}
