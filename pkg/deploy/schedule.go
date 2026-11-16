package deploy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"

	"github.com/spore-host/lagotto/pkg/pollcadence"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

const (
	// pollerScheduleExpression is the poll cadence, matching the template.
	pollerScheduleExpression = pollcadence.NormalExpression
	// pollerScheduleDescription matches the template's Description.
	pollerScheduleDescription = "Polls for EC2 capacity matching active lagotto watches"
)

// EnsurePollerSchedule get-or-creates the lagotto-capacity-poller EventBridge
// Scheduler schedule and converges it, returning ActionCreated,
// ActionConfigUpdated or ActionUnchanged, or a description of a move between
// schedule groups.
//
// THE CENTRAL GUARANTEE OF THIS FUNCTION: `lagotto deploy` must never be able to
// turn off a running poller.
//
// The schedule's State is owned OUT OF BAND, not by deploy: arming a watch
// (pkg/arm, from watch/extend/update) sets it ENABLED, and a poller older
// than #323 flipped it back to DISABLED when it went idle. A CloudFormation
// stack that declares `State: DISABLED` is therefore permanently drifted from
// reality, and any deploy that reasserted the template value would silently
// stop an active watch's poller. So: DISABLED is used ONLY on create (nothing
// is watching yet), and every update path passes through the CURRENT state
// verbatim.
//
// The cadence is shared the same way (#323): an idle poller slows the
// schedule to pollcadence.IdleExpression and arming a watch sets it back to
// the normal one. Deploy treats either as current and keeps the one it finds.
//
// # Schedule groups (#324)
//
// A schedule cannot be tagged, so the schedule lives in lagotto's tagged
// schedule group (schedgroup.Name). One created before #324 is in the default
// group, and deploy MOVES it: it creates the schedule in lagotto's group as a
// copy of the old one (same state, cadence, target input and settings), and
// only then deletes the old one, so there is never a moment with no schedule
// (for that moment both exist, and a tick may fire twice; the poller is safe
// to run concurrently). A re-run after a failure between the two steps finds
// both and deletes the one in the default group, so only one is ever left to
// fire. While Options.HoldScheduleMove is set the schedule stays in (or is
// created in) the default group.
//
// Convergence compares only the non-State fields; when they all match, it issues
// no API call at all.
func (d *Deployer) EnsurePollerSchedule(ctx context.Context, fnARN, roleARN string) (string, error) {
	inGroup, err := d.getPollerSchedule(ctx, schedgroup.Name)
	if err != nil {
		return "", err
	}
	inDefault, err := d.getPollerSchedule(ctx, schedgroup.Default)
	if err != nil {
		return "", err
	}

	switch {
	case inGroup != nil:
		action, err := d.convergePollerSchedule(ctx, inGroup, fnARN, roleARN)
		if err != nil || inDefault == nil {
			return action, err
		}
		// A move interrupted after its create, or a schedule re-created in the
		// default group by an older lagotto: two schedules would both fire.
		if err := d.deletePollerSchedule(ctx, schedgroup.Default); err != nil {
			return "", err
		}
		return action + ", removed the duplicate in schedule group " + schedgroup.Default, nil

	case inDefault != nil && d.holdScheduleMove == "":
		return d.movePollerSchedule(ctx, inDefault, fnARN, roleARN)

	case inDefault != nil:
		action, err := d.convergePollerSchedule(ctx, inDefault, fnARN, roleARN)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s, kept in schedule group %s (%s)", action, schedgroup.Default, d.holdScheduleMove), nil
	}

	group := schedgroup.Name
	if d.holdScheduleMove != "" {
		group = schedgroup.Default
	}
	created, err := d.createPollerSchedule(ctx, group, fnARN, roleARN)
	if err != nil {
		return "", err
	}
	if created {
		return ActionCreated, nil
	}
	// A concurrent deploy won the race; re-read and converge instead of failing.
	cur, err := d.getPollerSchedule(ctx, group)
	if err != nil {
		return "", err
	}
	if cur == nil {
		return "", fmt.Errorf("look up schedule %s after create conflict: not found in schedule group %s", PollerScheduleName, group)
	}
	return d.convergePollerSchedule(ctx, cur, fnARN, roleARN)
}

// getPollerSchedule reads the poller schedule from group. An absent schedule
// (or group) is (nil, nil). The result's GroupName is always set.
func (d *Deployer) getPollerSchedule(ctx context.Context, group string) (*scheduler.GetScheduleOutput, error) {
	out, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name:      aws.String(PollerScheduleName),
		GroupName: aws.String(group),
	})
	if err != nil {
		if schedgroup.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("look up schedule %s in schedule group %s: %w", PollerScheduleName, group, err)
	}
	if out.GroupName == nil {
		out.GroupName = aws.String(group)
	}
	return out, nil
}

// convergePollerSchedule updates cur when a field deploy owns differs.
func (d *Deployer) convergePollerSchedule(ctx context.Context, cur *scheduler.GetScheduleOutput, fnARN, roleARN string) (string, error) {
	if !scheduleDiffers(cur, fnARN, roleARN) {
		return ActionUnchanged, nil
	}
	if err := d.updatePollerSchedule(ctx, cur, fnARN, roleARN); err != nil {
		return "", err
	}
	return ActionConfigUpdated, nil
}

// movePollerSchedule moves old, the poller schedule in the default group,
// into lagotto's group: create the copy, then delete the original. A copy a
// concurrent deploy created first is converged instead.
func (d *Deployer) movePollerSchedule(ctx context.Context, old *scheduler.GetScheduleOutput, fnARN, roleARN string) (string, error) {
	_, err := d.sched.CreateSchedule(ctx, movedScheduleInput(old, fnARN, roleARN, d.now))
	if err != nil {
		var conflict *schedulertypes.ConflictException
		if !errors.As(err, &conflict) {
			return "", fmt.Errorf("create schedule %s in schedule group %s (moving it from %s; the original is untouched): %w",
				PollerScheduleName, schedgroup.Name, schedgroup.Default, err)
		}
		cur, gerr := d.getPollerSchedule(ctx, schedgroup.Name)
		if gerr != nil {
			return "", gerr
		}
		if cur == nil {
			return "", fmt.Errorf("look up schedule %s in schedule group %s after create conflict: not found", PollerScheduleName, schedgroup.Name)
		}
		if _, err := d.convergePollerSchedule(ctx, cur, fnARN, roleARN); err != nil {
			return "", err
		}
	}
	if err := d.deletePollerSchedule(ctx, schedgroup.Default); err != nil {
		return "", fmt.Errorf("%w (schedule group %s now has the poller schedule too, so both fire until a re-run of 'lagotto deploy' deletes the old one)",
			err, schedgroup.Name)
	}
	return fmt.Sprintf("moved from schedule group %s to %s (%s)", schedgroup.Default, schedgroup.Name, old.State), nil
}

// movedScheduleInput is the CreateSchedule that copies old into lagotto's
// group, converged onto the desired target and description. The state and
// cadence are old's (they are not deploy's to decide), and so is every
// optional setting UpdateSchedule would round-trip. A start date already past
// is dropped: Scheduler refuses one on create.
func movedScheduleInput(old *scheduler.GetScheduleOutput, fnARN, roleARN string, now func() time.Time) *scheduler.CreateScheduleInput {
	target := &schedulertypes.Target{Arn: aws.String(fnARN), RoleArn: aws.String(roleARN)}
	if old.Target != nil {
		target.Input = old.Target.Input
		target.RetryPolicy = old.Target.RetryPolicy
		target.DeadLetterConfig = old.Target.DeadLetterConfig
	}
	in := &scheduler.CreateScheduleInput{
		Name:                       aws.String(PollerScheduleName),
		GroupName:                  aws.String(schedgroup.Name),
		Description:                aws.String(pollerScheduleDescription),
		ScheduleExpression:         aws.String(currentCadence(old)),
		FlexibleTimeWindow:         &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target:                     target,
		State:                      old.State,
		ScheduleExpressionTimezone: old.ScheduleExpressionTimezone,
		KmsKeyArn:                  old.KmsKeyArn,
		StartDate:                  old.StartDate,
		EndDate:                    old.EndDate,
		ActionAfterCompletion:      old.ActionAfterCompletion,
	}
	if now == nil {
		now = time.Now
	}
	if in.StartDate != nil && in.StartDate.Before(now()) {
		in.StartDate = nil
	}
	return in
}

// createPollerSchedule creates the schedule DISABLED in group. It reports
// created=false (with a nil error) when the schedule already existed —
// EventBridge Scheduler answers a raced create with ConflictException, which
// is a signal to fall through to the update path rather than an error to
// surface.
func (d *Deployer) createPollerSchedule(ctx context.Context, group, fnARN, roleARN string) (bool, error) {
	_, err := d.sched.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               aws.String(PollerScheduleName),
		GroupName:          aws.String(group),
		Description:        aws.String(pollerScheduleDescription),
		ScheduleExpression: aws.String(pollerScheduleExpression),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target: &schedulertypes.Target{
			Arn:     aws.String(fnARN),
			RoleArn: aws.String(roleARN),
		},
		// DISABLED on create only: nothing is watching yet, so the poller must not
		// start billing invocations. `lagotto watch` enables it.
		State: schedulertypes.ScheduleStateDisabled,
	})
	if err == nil {
		return true, nil
	}
	var conflict *schedulertypes.ConflictException
	if errors.As(err, &conflict) {
		return false, nil
	}
	return false, fmt.Errorf("create schedule %s in schedule group %s: %w", PollerScheduleName, group, err)
}

// scheduleDiffers reports whether any field deploy owns differs from the desired
// value. State is deliberately NOT compared — it is not deploy's to decide.
func scheduleDiffers(cur *scheduler.GetScheduleOutput, fnARN, roleARN string) bool {
	if aws.ToString(cur.ScheduleExpression) != currentCadence(cur) {
		return true
	}
	if aws.ToString(cur.Description) != pollerScheduleDescription {
		return true
	}
	if cur.FlexibleTimeWindow == nil || cur.FlexibleTimeWindow.Mode != schedulertypes.FlexibleTimeWindowModeOff {
		return true
	}
	if cur.Target == nil {
		return true
	}
	return aws.ToString(cur.Target.Arn) != fnARN || aws.ToString(cur.Target.RoleArn) != roleARN
}

// currentCadence is the expression deploy converges cur to: the idle cadence
// when the poller has slowed to it (#323), else the normal one.
func currentCadence(cur *scheduler.GetScheduleOutput) string {
	if pollcadence.IsIdle(cur) {
		return pollcadence.IdleExpression
	}
	return pollerScheduleExpression
}

// updatePollerSchedule issues the UpdateSchedule. UpdateSchedule is a full PUT,
// not a patch: every required field must be resupplied (the same reason
// pollcadence.UpdateInput copies the current values across), and any
// optional field omitted here is DROPPED. So a user's own tweaks — a target
// Input, a retry policy, a DLQ, a timezone, a non-default group, a CMK, a
// start/end date, an action-after-completion — are round-tripped from the current
// schedule rather than silently discarded.
func (d *Deployer) updatePollerSchedule(ctx context.Context, cur *scheduler.GetScheduleOutput, fnARN, roleARN string) error {
	target := &schedulertypes.Target{
		Arn:     aws.String(fnARN),
		RoleArn: aws.String(roleARN),
	}
	if cur.Target != nil {
		target.Input = cur.Target.Input
		target.RetryPolicy = cur.Target.RetryPolicy
		target.DeadLetterConfig = cur.Target.DeadLetterConfig
	}
	_, err := d.sched.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
		Name:               aws.String(PollerScheduleName),
		Description:        aws.String(pollerScheduleDescription),
		ScheduleExpression: aws.String(currentCadence(cur)),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target:             target,
		// NEVER a literal DISABLED here. See EnsurePollerSchedule's doc comment:
		// the state belongs to `watch`/`extend`/the poller, and reasserting the
		// template's DISABLED would stop a live poller mid-watch.
		State: cur.State,
		// Round-trip the rest so an UpdateSchedule (a full PUT) doesn't drop it.
		ScheduleExpressionTimezone: cur.ScheduleExpressionTimezone,
		GroupName:                  cur.GroupName,
		KmsKeyArn:                  cur.KmsKeyArn,
		StartDate:                  cur.StartDate,
		EndDate:                    cur.EndDate,
		ActionAfterCompletion:      cur.ActionAfterCompletion,
	})
	if err != nil {
		return fmt.Errorf("update schedule %s: %w", PollerScheduleName, err)
	}
	return nil
}

// pollerScheduleExists reports whether the poller schedule is in group, so
// Teardown can report what it actually deleted (see pollerFunctionExists).
func (d *Deployer) pollerScheduleExists(ctx context.Context, group string) (bool, error) {
	_, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name:      aws.String(PollerScheduleName),
		GroupName: aws.String(group),
	})
	if err == nil {
		return true, nil
	}
	if schedgroup.IsNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("check whether schedule %s exists in schedule group %s: %w", PollerScheduleName, group, err)
}

// deletePollerSchedule removes the poller schedule from group, tolerating an
// absent one.
func (d *Deployer) deletePollerSchedule(ctx context.Context, group string) error {
	_, err := d.sched.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name:      aws.String(PollerScheduleName),
		GroupName: aws.String(group),
	})
	if err == nil || schedgroup.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("delete schedule %s from schedule group %s: %w", PollerScheduleName, group, err)
}
