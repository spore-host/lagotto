// Package pollcadence owns the hosted poller's schedule cadence (#323): how
// often lagotto-capacity-poller runs, and the protocol that lets the poller
// slow itself down when it has nothing to do without ever losing a watch that
// is armed while it decides.
//
// # Why the poller no longer disables its schedule
//
// Before #323 the poller switched its schedule OFF when it found no active
// watch and no pending scheduled launch, and `lagotto watch` switched it back
// on only if it read it as DISABLED. Those two read-then-write sequences
// interleave badly:
//
//  1. the poller counts zero watches;
//  2. `lagotto watch` stores a watch, reads the schedule as ENABLED, and so
//     does nothing;
//  3. the poller disables the schedule.
//
// The new watch was never polled, and nothing said so.
//
// # The protocol
//
// The schedule is never disabled by the poller any more. It has two cadences,
// both ENABLED: [NormalExpression] while there is work, and [IdleExpression]
// (one invocation an hour, about 720 a month) while there is none. A control
// item in the watches table carries an epoch that every arming of a watch
// increments ([Epoch]).
//
// A program that arms a watch (pkg/arm) stores it, increments the epoch, and
// then sets the schedule to the normal cadence. The poller ([Reconcile]) reads
// the epoch E BEFORE counting. When idle it records mode=idle on the control
// item only if the epoch is still E, slows the schedule, and then re-reads the
// epoch: a change means a watch was armed while it was slowing (and its
// owner's update may have been overwritten), so it restores the normal cadence.
//
// Because each side writes in the order "record, then schedule", the last
// schedule write is always the right one, or is corrected by the re-read. And
// because nothing disables the schedule, even the residual window (the status
// index the poller counts through is eventually consistent, so a watch stored
// a moment before the count can be missed by it) costs at most one hour of
// delay, never a watch that is not polled: the next hourly run counts the
// watch and restores the normal cadence.
package pollcadence

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

const (
	// NormalExpression is the poll cadence while any watch is active or any
	// scheduled launch is pending. It is also the cadence `lagotto deploy`
	// creates the schedule with (pkg/deploy).
	NormalExpression = "rate(5 minutes)"
	// IdleExpression is the cadence of an idle poller: still ENABLED, so a
	// watch armed by any lagotto version is polled within the hour.
	IdleExpression = "rate(1 hour)"
)

// ScheduleAPI is the slice of EventBridge Scheduler the cadence needs.
// *scheduler.Client satisfies it.
type ScheduleAPI interface {
	GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
	UpdateSchedule(ctx context.Context, in *scheduler.UpdateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error)
}

var _ ScheduleAPI = (*scheduler.Client)(nil)

// UpdateInput is the UpdateSchedule that gives cur the expression expr and
// the state state. UpdateSchedule is a full PUT: an optional field it omits is
// DROPPED, so every field the schedule carries is round-tripped (the poller's
// old disable dropped the Description, the timezone, the group, the key, the
// dates and the action after completion).
func UpdateInput(cur *scheduler.GetScheduleOutput, expr string, state schedulertypes.ScheduleState) *scheduler.UpdateScheduleInput {
	return &scheduler.UpdateScheduleInput{
		Name:                       cur.Name,
		Description:                cur.Description,
		ScheduleExpression:         aws.String(expr),
		ScheduleExpressionTimezone: cur.ScheduleExpressionTimezone,
		FlexibleTimeWindow:         cur.FlexibleTimeWindow,
		Target:                     cur.Target,
		GroupName:                  cur.GroupName,
		KmsKeyArn:                  cur.KmsKeyArn,
		StartDate:                  cur.StartDate,
		EndDate:                    cur.EndDate,
		ActionAfterCompletion:      cur.ActionAfterCompletion,
		State:                      state,
	}
}

// At reports whether cur is ENABLED at expr.
func At(cur *scheduler.GetScheduleOutput, expr string) bool {
	return cur != nil && cur.State == schedulertypes.ScheduleStateEnabled && aws.ToString(cur.ScheduleExpression) == expr
}

// IsIdle reports whether cur is at the idle cadence (whatever its state).
func IsIdle(cur *scheduler.GetScheduleOutput) bool {
	return cur != nil && aws.ToString(cur.ScheduleExpression) == IdleExpression
}

// SetFrom makes the schedule cur describes ENABLED at expr, and reports
// whether that took an UpdateSchedule. It is idempotent: a schedule already
// there is left alone. The error is the raw Scheduler error.
func SetFrom(ctx context.Context, api ScheduleAPI, cur *scheduler.GetScheduleOutput, expr string) (bool, error) {
	if At(cur, expr) {
		return false, nil
	}
	if _, err := api.UpdateSchedule(ctx, UpdateInput(cur, expr, schedulertypes.ScheduleStateEnabled)); err != nil {
		return false, err
	}
	return true, nil
}

// Set reads the schedule name afresh and makes it ENABLED at expr (SetFrom).
// The read is part of the protocol: a read taken before the caller's record
// was written may predate a cadence change it must see.
func Set(ctx context.Context, api ScheduleAPI, name, expr string) (bool, error) {
	cur, err := api.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(name)})
	if err != nil {
		return false, fmt.Errorf("get schedule %s: %w", name, err)
	}
	return SetFrom(ctx, api, cur, expr)
}

// Epoch is the poller-control item (pkg/watcher PollerControlKey, in the
// watches table) as the poller uses it. *watcher.Store satisfies it.
type Epoch interface {
	// PollerEpoch is the current epoch (0 when the control item does not
	// exist), read strongly consistently.
	PollerEpoch(ctx context.Context) (int64, error)
	// MarkPollerIdle records mode=idle only if the epoch is still seen. It
	// reports false, with a nil error, when the epoch moved.
	MarkPollerIdle(ctx context.Context, seen int64) (bool, error)
}

// Outcome is what one Reconcile decided.
type Outcome string

const (
	// Active: there is work, and the schedule is at the normal cadence.
	Active Outcome = "active"
	// Idle: nothing to poll, and the schedule is at the idle cadence.
	Idle Outcome = "idle"
	// Armed: nothing was counted, but the epoch moved while the poller
	// checked, so a watch was armed in the meantime; the schedule is left at
	// (or put back to) the normal cadence.
	Armed Outcome = "armed"
	// Unguarded: nothing was counted, but the epoch could not be read or
	// conditionally written, so the schedule is left as it is (Reconcile
	// returns the error).
	Unguarded Outcome = "unguarded"
)

// Result is one Reconcile's outcome, and whether it changed the schedule.
type Result struct {
	Outcome Outcome
	// Changed counts the UpdateSchedule calls made.
	Changed int
}

// Reconcile is the poller's half of the protocol, run at the end of each poll
// cycle. idle counts the work left (active watches, pending scheduled
// launches) and reports whether there is none; it runs AFTER the epoch is
// read. An error from idle leaves the schedule as it is.
//
// A schedule found DISABLED (by a poller older than #323, or a fresh deploy)
// is enabled when there is work. An idle poller that cannot read or
// conditionally write the epoch does not slow down: without the guard it
// could undo a concurrent arming.
func Reconcile(ctx context.Context, sched ScheduleAPI, name string, ep Epoch, idle func(context.Context) (bool, error)) (Result, error) {
	var res Result
	seen, epochErr := ep.PollerEpoch(ctx)
	nothing, err := idle(ctx)
	if err != nil {
		return res, err
	}
	if !nothing {
		res.Outcome = Active
		changed, err := Set(ctx, sched, name, NormalExpression)
		if changed {
			res.Changed++
		}
		if err != nil {
			return res, fmt.Errorf("restore the normal cadence: %w", err)
		}
		return res, nil
	}

	if epochErr != nil {
		res.Outcome = Unguarded
		return res, fmt.Errorf("could not read the poller epoch, so not slowing the schedule: %w", epochErr)
	}
	ok, err := ep.MarkPollerIdle(ctx, seen)
	if err != nil {
		res.Outcome = Unguarded
		return res, fmt.Errorf("could not record idle mode, so not slowing the schedule: %w", err)
	}
	if !ok {
		// A watch was armed after the epoch was read; its owner is setting the
		// normal cadence. Nothing to undo: this run wrote no schedule.
		res.Outcome = Armed
		return res, nil
	}

	res.Outcome = Idle
	changed, err := Set(ctx, sched, name, IdleExpression)
	if err != nil {
		return res, fmt.Errorf("slow to the idle cadence: %w", err)
	}
	if !changed {
		// Already idle: this run overwrote nothing, so there is nothing to
		// re-check. An arming after the read above sets the normal cadence
		// itself, after this point.
		return res, nil
	}
	res.Changed++

	// The slow-down may have overwritten the normal cadence an arming set
	// between the conditional write and the update. The arming incremented
	// the epoch before it updated the schedule, so the epoch shows it.
	now, err := ep.PollerEpoch(ctx)
	if err == nil && now == seen {
		return res, nil
	}
	res.Outcome = Armed
	changed, rerr := Set(ctx, sched, name, NormalExpression)
	if changed {
		res.Changed++
	}
	if err != nil {
		// Unknown: fail safe, to the normal cadence. The next idle run slows
		// it again.
		if rerr != nil {
			return res, fmt.Errorf("re-read the poller epoch (%v), and restore the normal cadence: %w", err, rerr)
		}
		return res, fmt.Errorf("re-read the poller epoch after slowing (restored the normal cadence): %w", err)
	}
	if rerr != nil {
		return res, fmt.Errorf("a watch was armed while slowing; restore the normal cadence: %w", rerr)
	}
	return res, nil
}
