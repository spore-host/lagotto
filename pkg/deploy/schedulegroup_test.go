package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sporetags "github.com/spore-host/libs/tags"
)

// preGroupSchedule is the poller schedule a pre-#324 deploy left in the
// default group: ENABLED, slowed to the idle cadence, with a user's retry
// policy on its target.
func preGroupSchedule() *scheduler.GetScheduleOutput {
	return &scheduler.GetScheduleOutput{
		Name:               aws.String(PollerScheduleName),
		Description:        aws.String(pollerScheduleDescription),
		ScheduleExpression: aws.String("rate(1 hour)"),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		State:              schedulertypes.ScheduleStateEnabled,
		Target: &schedulertypes.Target{
			Arn: aws.String(testFnARN), RoleArn: aws.String(testRoleARN),
			RetryPolicy: &schedulertypes.RetryPolicy{MaximumRetryAttempts: aws.Int32(3)},
		},
	}
}

// The move creates the new schedule BEFORE deleting the old one, so the poller
// is never without a schedule; keeps its state, cadence and settings; and a
// re-run leaves the single schedule alone.
func TestEnsurePollerSchedule_MovesCreateThenDelete(t *testing.T) {
	log := &callLog{}
	sc := &fakeScheduler{log: log, schedules: map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()}}
	d, _ := testDeployer(nil, nil, sc)

	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if !strings.Contains(action, "moved from schedule group default to lagotto") {
		t.Errorf("action = %q", action)
	}
	if want := []string{"CreateSchedule lagotto", "DeleteSchedule default"}; strings.Join(log.calls, ",") != strings.Join(want, ",") {
		t.Fatalf("calls = %v, want %v (create the new one, then delete the old)", log.calls, want)
	}
	moved := sc.schedules["lagotto"]
	if moved == nil || sc.schedules["default"] != nil {
		t.Fatalf("after the move: %v", sc.schedules)
	}
	if moved.State != schedulertypes.ScheduleStateEnabled || aws.ToString(moved.ScheduleExpression) != "rate(1 hour)" {
		t.Errorf("moved schedule %s at %s: state and cadence must be kept", moved.State, aws.ToString(moved.ScheduleExpression))
	}
	if moved.Target == nil || moved.Target.RetryPolicy == nil || aws.ToInt32(moved.Target.RetryPolicy.MaximumRetryAttempts) != 3 {
		t.Errorf("the target's retry policy was dropped: %+v", moved.Target)
	}

	// Idempotent re-run: one schedule, nothing created or deleted.
	log.calls = nil
	action, err = d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil || action != ActionUnchanged || len(log.calls) != 0 {
		t.Errorf("re-run = %q, %v, calls %v; want unchanged with no calls", action, err, log.calls)
	}
}

// A move whose delete failed leaves both schedules, and says so; the re-run
// removes the default group's copy, so only one is ever left firing.
func TestEnsurePollerSchedule_InterruptedMoveIsRepairedByARerun(t *testing.T) {
	sc := &fakeScheduler{
		schedules:     map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()},
		deleteByGroup: map[string]error{"default/" + PollerScheduleName: errors.New("ThrottlingException")},
	}
	d, _ := testDeployer(nil, nil, sc)
	_, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err == nil || !strings.Contains(err.Error(), "both fire") {
		t.Fatalf("err = %v, want the failed delete reported with its consequence", err)
	}
	if sc.schedules["lagotto"] == nil || sc.schedules["default"] == nil {
		t.Fatalf("schedules = %v, want both (the create succeeded)", sc.schedules)
	}

	sc.deleteByGroup = nil
	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil || !strings.Contains(action, "removed the duplicate") {
		t.Fatalf("re-run = %q, %v", action, err)
	}
	if sc.schedules["default"] != nil || sc.schedules["lagotto"] == nil {
		t.Errorf("schedules after the re-run = %v, want only lagotto's", sc.schedules)
	}
	if n := len(sc.createCalls); n != 1 {
		t.Errorf("CreateSchedule called %d times, want 1 (the re-run must not create again)", n)
	}
}

// A failed create of the new schedule leaves the old one untouched.
func TestEnsurePollerSchedule_FailedMoveCreateDeletesNothing(t *testing.T) {
	sc := &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()},
		createErr: errors.New("ValidationException")}
	d, _ := testDeployer(nil, nil, sc)
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err == nil {
		t.Fatal("want the create error")
	}
	if sc.deleteCalls != 0 || sc.schedules["default"] == nil {
		t.Errorf("deleted the only schedule after the new one failed to create")
	}
}

// While HoldScheduleMove is set the schedule stays in the default group, and
// a missing one is created there.
func TestEnsurePollerSchedule_HoldKeepsTheDefaultGroup(t *testing.T) {
	sc := &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()}}
	d, _ := testDeployer(nil, nil, sc)
	d.holdScheduleMove = "the runtime policy predates it"
	action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN)
	if err != nil || !strings.Contains(action, "kept in schedule group default (the runtime policy predates it)") {
		t.Fatalf("action %q err %v", action, err)
	}
	if len(sc.createCalls) != 0 || sc.deleteCalls != 0 {
		t.Errorf("a held schedule was moved: creates %d deletes %d", len(sc.createCalls), sc.deleteCalls)
	}

	sc = &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{}}
	d, _ = testDeployer(nil, nil, sc)
	d.holdScheduleMove = "held"
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err != nil {
		t.Fatal(err)
	}
	if len(sc.createCalls) != 1 || aws.ToString(sc.createCalls[0].GroupName) != "default" {
		t.Errorf("held create went to %v", sc.createCalls)
	}
}

// A new schedule is created in lagotto's group.
func TestEnsurePollerSchedule_CreatesInLagottosGroup(t *testing.T) {
	sc := &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{}}
	d, _ := testDeployer(nil, nil, sc)
	if action, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err != nil || action != ActionCreated {
		t.Fatalf("action %q err %v", action, err)
	}
	if aws.ToString(sc.createCalls[0].GroupName) != ScheduleGroupName || sc.createCalls[0].State != schedulertypes.ScheduleStateDisabled {
		t.Errorf("created %+v, want DISABLED in %s", sc.createCalls[0], ScheduleGroupName)
	}
}

// A start date already past is dropped on the move; Scheduler refuses one.
func TestMovedScheduleInput_DropsAPastStartDate(t *testing.T) {
	old := preGroupSchedule()
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	old.StartDate = &past
	if in := movedScheduleInput(old, testFnARN, testRoleARN, time.Now); in.StartDate != nil {
		t.Errorf("StartDate %v kept", in.StartDate)
	}
	old.StartDate = &future
	if in := movedScheduleInput(old, testFnARN, testRoleARN, time.Now); in.StartDate == nil {
		t.Error("a future StartDate was dropped")
	}
}

// The group is created with the spore.host tags; an existing one gains those
// it lacks and keeps its created time.
func TestEnsureScheduleGroup(t *testing.T) {
	sc := &fakeScheduler{}
	d, _ := testDeployer(nil, nil, sc)
	d.owner = "AIDAOWNER"
	d.now = func() time.Time { return time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC) }
	if action, err := d.ensureScheduleGroup(context.Background(), testRegion, testAccount); err != nil || action != ActionCreated {
		t.Fatalf("action %q err %v", action, err)
	}
	tg := sc.groupTags
	if !sporetags.IsManagedBy(tg, sporetags.ToolLagotto) || !sporetags.IsService(tg) || tg[sporetags.Scope] != testRegion ||
		tg[sporetags.Owner] != "AIDAOWNER" || tg[sporetags.Created] != "2026-02-03T04:05:06Z" {
		t.Errorf("group tags = %v", tg)
	}

	// Re-run, later and by someone else: nothing to write.
	d.owner, d.now = "AIDAOTHER", time.Now
	if action, err := d.ensureScheduleGroup(context.Background(), testRegion, testAccount); err != nil || action != ActionUnchanged {
		t.Errorf("re-run action %q err %v", action, err)
	}
	if len(sc.groupTagCalls) != 0 {
		t.Errorf("re-run tagged the group: %v", sc.groupTagCalls)
	}

	// A group someone created untagged is adopted.
	sc = &fakeScheduler{groupExists: true}
	d, _ = testDeployer(nil, nil, sc)
	if action, err := d.ensureScheduleGroup(context.Background(), testRegion, testAccount); err != nil || action != "exists, tagged" {
		t.Fatalf("action %q err %v", action, err)
	}
	if !sporetags.IsManagedBy(sc.groupTags, sporetags.ToolLagotto) {
		t.Errorf("adopted group tags = %v", sc.groupTags)
	}
	if arn := aws.ToString(sc.groupTagCalls[0].ResourceArn); arn != "arn:aws:scheduler:us-east-1:123456789012:schedule-group/lagotto" {
		t.Errorf("tagged %q", arn)
	}
}

// Teardown deletes the group only when it is empty: a pending per-launch
// schedule (retained by deploy --teardown) keeps it.
func TestTeardown_KeepsANonEmptyScheduleGroup(t *testing.T) {
	sc := &fakeScheduler{groupExists: true, listByGroup: map[string][][]string{"lagotto": {{"lagotto-launch-sl-1"}}},
		schedules: map[string]*scheduler.GetScheduleOutput{}}
	d, _ := testDeployer(&fakeLambda{getErr: notFoundLambda()}, &fakeSNS{getErr: notFoundSNS()}, sc)
	deleted, err := d.Teardown(context.Background(), testRegion, testAccount)
	if err != nil {
		t.Fatal(err)
	}
	if sc.groupDeletes != 0 || len(deleted) != 0 {
		t.Errorf("deleted %v (group deletes %d) with a schedule still in the group", deleted, sc.groupDeletes)
	}
}

func notFoundLambda() error {
	return &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}
}

func notFoundSNS() error { return &snstypes.NotFoundException{Message: aws.String("Topic not found")} }

// A racing deploy that already created the copy in lagotto's group: the move
// converges it and still deletes the old one.
func TestEnsurePollerSchedule_MoveRaceConverges(t *testing.T) {
	sc := &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()}}
	d, _ := testDeployer(nil, nil, sc)
	// The other deploy's copy appears between our reads and our create.
	racer := &raceScheduler{fakeScheduler: sc}
	d.sched = racer
	if _, err := d.EnsurePollerSchedule(context.Background(), testFnARN, testRoleARN); err != nil {
		t.Fatalf("EnsurePollerSchedule: %v", err)
	}
	if sc.schedules["default"] != nil || sc.schedules["lagotto"] == nil {
		t.Errorf("after a raced move: %v", sc.schedules)
	}
}

// raceScheduler creates lagotto's copy itself just before the first create,
// as a concurrent deploy would.
type raceScheduler struct {
	*fakeScheduler
	raced bool
}

func (r *raceScheduler) CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, o ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	if !r.raced {
		r.raced = true
		r.schedules["lagotto"] = preGroupSchedule()
	}
	return r.fakeScheduler.CreateSchedule(ctx, in, o...)
}

// Errors stop the deploy and name what failed; nothing is deleted.
func TestScheduleGroupErrors(t *testing.T) {
	ctx := context.Background()
	denied := &smithyAccessDenied{}

	sc := &fakeScheduler{groupCreateErr: denied}
	d, _ := testDeployer(nil, nil, sc)
	if _, err := d.ensureScheduleGroup(ctx, testRegion, testAccount); err == nil || !strings.Contains(err.Error(), "create schedule group") {
		t.Errorf("CreateScheduleGroup error = %v", err)
	}

	sc = &fakeScheduler{groupExists: true, listErr: denied}
	d, _ = testDeployer(nil, nil, sc)
	if _, err := d.deleteScheduleGroupIfEmpty(ctx); err == nil || sc.groupDeletes != 0 {
		t.Errorf("a list failure must keep the group: err %v, deletes %d", err, sc.groupDeletes)
	}
	if _, err := d.ListLaunchSchedules(ctx); err == nil {
		t.Error("a list failure was swallowed")
	}

	sc = &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{}, getErr: denied}
	d, _ = testDeployer(nil, nil, sc)
	if _, err := d.getPollerSchedule(ctx, "default"); err == nil || !strings.Contains(err.Error(), "schedule group default") {
		t.Errorf("GetSchedule error = %v", err)
	}
	if _, err := d.pollerScheduleExists(ctx, "lagotto"); err == nil {
		t.Error("an AccessDenied probe read as absent")
	}

	sc = &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{"lagotto": preGroupSchedule()}, deleteErr: denied}
	d, _ = testDeployer(nil, nil, sc)
	if err := d.deletePollerSchedule(ctx, "lagotto"); err == nil {
		t.Error("a DeleteSchedule failure was swallowed")
	}
	// A failed delete of the duplicate fails the deploy, so the re-run retries it.
	sc.schedules["default"] = preGroupSchedule()
	if _, err := d.EnsurePollerSchedule(ctx, testFnARN, testRoleARN); err == nil {
		t.Error("a failed duplicate delete was swallowed")
	}
}

// The topic probe and tag read fail the deploy rather than being read as
// "absent" or "untagged".
func TestEnsureAlertsTopic_ProbeAndTagErrors(t *testing.T) {
	fs := &fakeSNS{arn: testTopicARN, getErr: &smithyAccessDenied{}}
	d, _ := testDeployer(nil, fs, nil)
	if _, err := d.EnsureAlertsTopic(context.Background(), "production", testRegion, testAccount); err == nil || len(fs.createCalls) != 0 {
		t.Errorf("probe error: err %v, creates %d", err, len(fs.createCalls))
	}
}
