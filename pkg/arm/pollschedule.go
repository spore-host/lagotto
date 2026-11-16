package arm

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/pollcadence"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// PollScheduleAPI is the slice of EventBridge Scheduler that arming a watch
// uses to make sure the hosted poller will actually poll it.
// *scheduler.Client satisfies it.
type PollScheduleAPI interface {
	GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
	UpdateSchedule(ctx context.Context, in *scheduler.UpdateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error)
}

// ScheduleNameEnvVar overrides the hosted poller's schedule name.
const ScheduleNameEnvVar = "LAGOTTO_SCHEDULE_NAME"

// PollScheduleName is the hosted poller's schedule: deploy.PollerScheduleName,
// unless LAGOTTO_SCHEDULE_NAME overrides it.
func PollScheduleName() string {
	if n := os.Getenv(ScheduleNameEnvVar); n != "" {
		return n
	}
	return deploy.PollerScheduleName
}

// Arming a watch the hosted poller must service (#231, #323). An idle hosted
// poller slows its schedule to the idle cadence (pollcadence.IdleExpression,
// hourly), and a poller older than #323 switched it off altogether, so a new
// or reactivated watch is polled promptly only if the program arming it puts
// the schedule back to the normal cadence. The CLI used to treat ANY
// GetSchedule error as "poller not deployed" and report an UpdateSchedule
// failure only under --verbose: a user without the Scheduler permissions got
// an active watch, a schedule left off, and no message.
//
// A caller (lagotto watch, extend, update; spore-host-mcp's lagotto_watch)
// sequences four steps, in this order:
//
//  1. PreflightPollSchedule, BEFORE persisting. ResourceNotFound really means
//     the hosted poller is not deployed (the watch is for 'lagotto poll
//     --daemon'), so the watch is stored as before. Any other error,
//     AccessDenied included, means we cannot know whether the watch will be
//     polled: refuse, and store nothing.
//  2. persist (PutWatch / ExtendWatch / UpdateWatch).
//  3. BumpPollerEpoch: increment the poller epoch, so a poller that counted no
//     watches before step 2 and is slowing down sees that one was armed and
//     undoes its slow-down (pkg/pollcadence has the protocol). Best effort: a
//     failure is a warning, because the schedule is never disabled and the
//     watch is polled within the hour regardless.
//  4. ArmPollSchedule: re-read the schedule and make it ENABLED at the normal
//     cadence. If it fails (scheduler:UpdateSchedule, or iam:PassRole on the
//     invoke role the schedule assumes), the caller undoes the persist, so no
//     active watch is left orphaned, and refuses with
//     PollScheduleUpdateRefusal.
//
// Storing first and setting the schedule last is what makes the protocol
// work: a poller run that counts after step 2 sees the watch, and one that
// counted before it either sees the epoch move (step 3) or has its slow-down
// overwritten by step 4.
//
// The steps are separate functions rather than one taking persist/rollback
// callbacks so that lagotto's generated IAM inventory (internal/iamcoverage),
// which cannot follow function values precisely, attributes each command only
// the calls it makes.

// PreflightPollSchedule reads the poller's schedule. (nil, nil) means it is not
// deployed. Any other error is the refusal to return to the user, and nothing
// should be stored. action names what is refused ("create watch w-1").
func PreflightPollSchedule(ctx context.Context, sched PollScheduleAPI, action string) (*scheduler.GetScheduleOutput, error) {
	name := PollScheduleName()
	// In lagotto's schedule group, or the default one before `lagotto deploy`
	// has moved it (lagotto#324). The result carries the group it was found in,
	// which ArmPollSchedule's re-read and UpdateSchedule then address.
	cur, err := schedgroup.Find(ctx, sched, name)
	if err != nil {
		if schedgroup.IsNotFound(err) {
			return nil, nil
		}
		return nil, pollScheduleError(action, "nothing was changed", "scheduler:GetSchedule", name, err)
	}
	return cur, nil
}

// PollerEpochBumper is the store half of step 3. *watcher.Store satisfies it.
type PollerEpochBumper interface {
	BumpPollerEpoch(ctx context.Context) error
}

// BumpPollerEpoch is step 3: increment the poller epoch after persisting and
// before ArmPollSchedule. cur is PreflightPollSchedule's result; with no
// hosted poller (nil) there is nothing to tell and nothing is written. It
// returns the warning to show when the increment failed (empty otherwise):
// the watch is still armed, only possibly up to an hour late.
func BumpPollerEpoch(ctx context.Context, ep PollerEpochBumper, cur *scheduler.GetScheduleOutput) string {
	if cur == nil {
		return ""
	}
	if err := ep.BumpPollerEpoch(ctx); err != nil {
		return fmt.Sprintf("Warning: could not tell the hosted poller that a watch was armed (%v). "+
			"If it is idle it may take up to an hour to poll this watch for the first time. "+
			"A %s policy written before lagotto#323 does not grant dynamodb:UpdateItem on the poller-control item; "+
			"an administrator can re-run 'lagotto setup' to update it.", err, cliiam.PolicyName)
	}
	return ""
}

// ArmPollSchedule is step 4: it re-reads the schedule and makes it ENABLED at
// the normal cadence (pollcadence.NormalExpression), reporting whether that
// took an UpdateSchedule. A schedule already there is left alone; one an
// idle poller slowed, or one a poller older than #323 disabled, is set back.
// cur is PreflightPollSchedule's result: nil (not deployed) does nothing. The
// re-read matters because cur was read before the persist, and a slow-down
// in between must not be mistaken for a schedule that needs nothing.
//
// The error is the raw Scheduler error: undo the persist, then pass it to
// PollScheduleUpdateRefusal.
func ArmPollSchedule(ctx context.Context, sched PollScheduleAPI, cur *scheduler.GetScheduleOutput) (bool, error) {
	if cur == nil {
		return false, nil
	}
	fresh, err := sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: cur.Name, GroupName: cur.GroupName})
	if err != nil {
		return false, err
	}
	return pollcadence.SetFrom(ctx, sched, fresh, pollcadence.NormalExpression)
}

// EnablePollScheduleIfOff puts a DISABLED or idle-cadence schedule back to
// ENABLED at the normal cadence, from cur, and reports whether it did. A nil
// (not deployed) schedule, or one already ENABLED at another cadence, is left
// alone.
//
// Deprecated: it does not re-read the schedule or bump the poller epoch, so
// it can lose the race #323 fixed. Use BumpPollerEpoch and ArmPollSchedule.
func EnablePollScheduleIfOff(ctx context.Context, sched PollScheduleAPI, cur *scheduler.GetScheduleOutput) (bool, error) {
	if cur == nil || (cur.State != schedulertypes.ScheduleStateDisabled && !pollcadence.IsIdle(cur)) {
		return false, nil
	}
	if _, err := sched.UpdateSchedule(ctx, pollcadence.UpdateInput(cur, pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled)); err != nil {
		return false, err
	}
	return true, nil
}

// PollScheduleUpdateRefusal is the refusal after EnablePollScheduleIfOff
// failed with err. undone describes the state the caller's rollback left ("no
// watch was created"); rollbackErr is that rollback's own failure, if any, in
// which case the refusal says the watch is active but unpolled instead.
func PollScheduleUpdateRefusal(action, undone string, rollbackErr, err error) error {
	state := undone
	if rollbackErr != nil {
		state = fmt.Sprintf("the watch could NOT be rolled back (%v): it is active but will not be polled — cancel it with 'lagotto cancel'", rollbackErr)
	}
	return pollScheduleError(action, state, "scheduler:UpdateSchedule", PollScheduleName(), err)
}

// pollScheduleError is the refusal: what failed, what state it left, and — for
// AccessDenied — exactly which permissions the caller is missing.
func pollScheduleError(action, state, call, name string, err error) error {
	var apiErr smithy.APIError
	denied := errors.As(err, &apiErr) && (apiErr.ErrorCode() == "AccessDeniedException" || apiErr.ErrorCode() == "AccessDenied")
	if !denied {
		return fmt.Errorf("cannot %s: %s on the hosted poller's schedule %s failed, so lagotto cannot make sure the poller will poll it (%s): %w",
			action, call, name, state, err)
	}
	return fmt.Errorf(`cannot %s: %s on the hosted poller's schedule %s was denied (%s): %w

An idle hosted poller slows its schedule to once an hour (an older poller
switches it off), so arming a watch must read the schedule and set it back to
every 5 minutes. Without that the watch could be stored as active and not be
polled. Your identity needs:
  - scheduler:GetSchedule and scheduler:UpdateSchedule on schedule %s
  - iam:PassRole on role %s (UpdateSchedule passes the role the schedule assumes)
See docs/reference/cli-iam-actions.md for every action each command needs. Ask an
administrator to grant these or to run the command; spore.host-provisioned CLI-user
access is planned in lagotto#232`,
		action, call, name, state, err, name, runtimeiam.SchedulerInvokeRoleName)
}
