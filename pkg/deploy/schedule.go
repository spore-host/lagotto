package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

const (
	// pollerScheduleExpression is the poll cadence, matching the template.
	pollerScheduleExpression = "rate(5 minutes)"
	// pollerScheduleDescription matches the template's Description.
	pollerScheduleDescription = "Polls for EC2 capacity matching active lagotto watches"
)

// EnsurePollerSchedule get-or-creates the lagotto-capacity-poller EventBridge
// Scheduler schedule and converges it, returning ActionCreated,
// ActionConfigUpdated or ActionUnchanged.
//
// THE CENTRAL GUARANTEE OF THIS FUNCTION: `lagotto deploy` must never be able to
// turn off a running poller.
//
// The schedule's State is owned OUT OF BAND, not by deploy:
// cmd/watch.go's enablePollingSchedule (also reached from cmd/extend.go) flips it
// to ENABLED when a watch is armed, and the poller itself flips it back to
// DISABLED when it goes idle. A CloudFormation stack that declares
// `State: DISABLED` is therefore permanently drifted from reality, and any
// deploy that reasserted the template value would silently stop an active
// watch's poller. So: DISABLED is used ONLY on create (nothing is watching yet),
// and every update path passes through the CURRENT state verbatim.
//
// Convergence compares only the non-State fields; when they all match, it issues
// no API call at all.
func (d *Deployer) EnsurePollerSchedule(ctx context.Context, fnARN, roleARN string) (string, error) {
	cur, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name: aws.String(PollerScheduleName),
	})
	if err != nil {
		var notFound *schedulertypes.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			return "", fmt.Errorf("look up schedule %s: %w", PollerScheduleName, err)
		}
		created, cErr := d.createPollerSchedule(ctx, fnARN, roleARN)
		if cErr != nil {
			return "", cErr
		}
		if created {
			return ActionCreated, nil
		}
		// A concurrent deploy won the race; re-read and converge instead of failing.
		cur, err = d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
			Name: aws.String(PollerScheduleName),
		})
		if err != nil {
			return "", fmt.Errorf("look up schedule %s after create conflict: %w", PollerScheduleName, err)
		}
	}

	if !scheduleDiffers(cur, fnARN, roleARN) {
		return ActionUnchanged, nil
	}
	if err := d.updatePollerSchedule(ctx, cur, fnARN, roleARN); err != nil {
		return "", err
	}
	return ActionConfigUpdated, nil
}

// createPollerSchedule creates the schedule DISABLED. It reports created=false
// (with a nil error) when the schedule already existed — EventBridge Scheduler
// answers a raced create with ConflictException, which is a signal to fall
// through to the update path rather than an error to surface.
func (d *Deployer) createPollerSchedule(ctx context.Context, fnARN, roleARN string) (bool, error) {
	_, err := d.sched.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               aws.String(PollerScheduleName),
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
	return false, fmt.Errorf("create schedule %s: %w", PollerScheduleName, err)
}

// scheduleDiffers reports whether any field deploy owns differs from the desired
// value. State is deliberately NOT compared — it is not deploy's to decide.
func scheduleDiffers(cur *scheduler.GetScheduleOutput, fnARN, roleARN string) bool {
	if aws.ToString(cur.ScheduleExpression) != pollerScheduleExpression {
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

// updatePollerSchedule issues the UpdateSchedule. UpdateSchedule is a full PUT,
// not a patch: every required field must be resupplied (the same reason
// cmd/watch.go's enablePollingSchedule copies the current values across), and any
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
		ScheduleExpression: aws.String(pollerScheduleExpression),
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

// deletePollerSchedule removes the poller schedule, tolerating an absent one.
// Unexported and not yet called by production code — it exists now so the
// Teardown cutover (#154, follow-up PR) is pure wiring.
func (d *Deployer) deletePollerSchedule(ctx context.Context) error {
	_, err := d.sched.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name: aws.String(PollerScheduleName),
	})
	if err == nil {
		return nil
	}
	var notFound *schedulertypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return nil
	}
	return fmt.Errorf("delete schedule %s: %w", PollerScheduleName, err)
}
