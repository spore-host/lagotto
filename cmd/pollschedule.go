package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/spore-host/lagotto/pkg/arm"
)

// Arming a watch the hosted poller must service (#231, #323). The steps and
// their order (preflight, persist, bump the poller epoch, set the schedule,
// roll back on failure) are pkg/arm's, which exports them for non-CLI callers
// (#266); see pkg/arm/pollschedule.go. These are the commands' thin wrappers:
// the only CLI-specific parts are the stderr warning when the epoch could not
// be bumped and the --verbose line when the schedule was changed.

// pollScheduleAPI is the slice of EventBridge Scheduler that watch, extend and
// update use.
type pollScheduleAPI = arm.PollScheduleAPI

// pollSchedulePreflight is arm.PreflightPollSchedule: (nil, nil) means the
// poller is not deployed; any other error is the refusal, before persisting.
func pollSchedulePreflight(ctx context.Context, sched pollScheduleAPI, action string) (*scheduler.GetScheduleOutput, error) {
	return arm.PreflightPollSchedule(ctx, sched, action)
}

// armPollSchedule is the steps after persisting (#323): arm.BumpPollerEpoch,
// whose failure is only a warning on stderr, then arm.ArmPollSchedule, saying
// so under --verbose when it changed the schedule. Its error is
// ArmPollSchedule's: undo the persist, then pollScheduleUpdateRefusal.
func armPollSchedule(ctx context.Context, sched pollScheduleAPI, ep arm.PollerEpochBumper, stderr io.Writer, cur *scheduler.GetScheduleOutput) error {
	if msg := arm.BumpPollerEpoch(ctx, ep, cur); msg != "" {
		_, _ = fmt.Fprintln(stderr, msg)
	}
	changed, err := arm.ArmPollSchedule(ctx, sched, cur)
	if err != nil {
		return err
	}
	if changed && verbose {
		_, _ = fmt.Fprintf(stderr, "Set polling schedule %s to every 5 minutes\n", aws.ToString(cur.Name))
	}
	return nil
}

// pollScheduleUpdateRefusal is arm.PollScheduleUpdateRefusal: the refusal after
// armPollSchedule failed and the caller rolled back.
func pollScheduleUpdateRefusal(action, undone string, rollbackErr, err error) error {
	return arm.PollScheduleUpdateRefusal(action, undone, rollbackErr, err)
}
