package arm

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
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

// Arming a watch the hosted poller must service (#231). The hosted poller
// switches its own schedule off when no watches are active, so a new or
// reactivated watch is only polled if the program arming it switches it back on.
// The CLI used to treat ANY GetSchedule error as "poller not deployed" and
// report an UpdateSchedule failure only under --verbose: a user without the
// Scheduler permissions got an active watch, a schedule left off, and no
// message.
//
// A caller (lagotto watch, extend, update; spore-host-mcp's lagotto_watch)
// sequences three steps, in this order:
//
//  1. PreflightPollSchedule, BEFORE persisting. ResourceNotFound really means
//     the hosted poller is not deployed (the watch is for 'lagotto poll
//     --daemon'), so the watch is stored as before. Any other error,
//     AccessDenied included, means we cannot know whether the watch will be
//     polled: refuse, and store nothing.
//  2. persist (PutWatch / ExtendWatch / UpdateWatch). Storing first and
//     enabling second means a poller run that starts after the enable always
//     sees the watch; the other order lets a run that starts in between find
//     no watch and switch the schedule straight back off.
//  3. EnablePollScheduleIfOff. If it fails (scheduler:UpdateSchedule, or
//     iam:PassRole on the invoke role the schedule assumes), the caller undoes
//     the persist, so no active watch is left orphaned, and refuses with
//     PollScheduleUpdateRefusal.
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
	cur, err := sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(name)})
	if err != nil {
		var notFound *schedulertypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, pollScheduleError(action, "nothing was changed", "scheduler:GetSchedule", name, err)
	}
	return cur, nil
}

// EnablePollScheduleIfOff switches a DISABLED schedule on and reports whether
// it did. A nil (not deployed) or already-enabled schedule is left alone. cur
// is PreflightPollSchedule's result. The error is the raw Scheduler error: undo
// the persist, then pass it to PollScheduleUpdateRefusal.
func EnablePollScheduleIfOff(ctx context.Context, sched PollScheduleAPI, cur *scheduler.GetScheduleOutput) (bool, error) {
	if cur == nil || cur.State != schedulertypes.ScheduleStateDisabled {
		return false, nil
	}
	if err := enablePollSchedule(ctx, sched, cur); err != nil {
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

// enablePollSchedule switches the schedule on. UpdateSchedule is a full PUT,
// so every field the schedule carries is round-tripped (an omitted optional
// field is dropped), as pkg/deploy's updatePollerSchedule does.
func enablePollSchedule(ctx context.Context, sched PollScheduleAPI, cur *scheduler.GetScheduleOutput) error {
	_, err := sched.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
		Name:                       cur.Name,
		Description:                cur.Description,
		ScheduleExpression:         cur.ScheduleExpression,
		ScheduleExpressionTimezone: cur.ScheduleExpressionTimezone,
		FlexibleTimeWindow:         cur.FlexibleTimeWindow,
		Target:                     cur.Target,
		GroupName:                  cur.GroupName,
		KmsKeyArn:                  cur.KmsKeyArn,
		StartDate:                  cur.StartDate,
		EndDate:                    cur.EndDate,
		ActionAfterCompletion:      cur.ActionAfterCompletion,
		State:                      schedulertypes.ScheduleStateEnabled,
	})
	return err
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

The hosted poller switches its schedule off when no watches are active, so
arming a watch must read the schedule and switch it back on. Without that the
watch would be stored as active and never polled. Your identity needs:
  - scheduler:GetSchedule and scheduler:UpdateSchedule on schedule %s
  - iam:PassRole on role %s (UpdateSchedule passes the role the schedule assumes)
See docs/reference/cli-iam-actions.md for every action each command needs. Ask an
administrator to grant these or to run the command; spore.host-provisioned CLI-user
access is planned in lagotto#232`,
		action, call, name, state, err, name, runtimeiam.SchedulerInvokeRoleName)
}
