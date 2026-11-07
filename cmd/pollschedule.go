package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/spore-host/lagotto/pkg/arm"
)

// Arming a watch the hosted poller must service (#231). The steps and their
// order (preflight, persist, enable, roll back on failure) are pkg/arm's, which
// exports them for non-CLI callers (#266); see pkg/arm/pollschedule.go. These
// are the commands' thin wrappers: the only CLI-specific part is the --verbose
// line when the schedule was switched on.

// pollScheduleAPI is the slice of EventBridge Scheduler that watch, extend and
// update use.
type pollScheduleAPI = arm.PollScheduleAPI

// pollSchedulePreflight is arm.PreflightPollSchedule: (nil, nil) means the
// poller is not deployed; any other error is the refusal, before persisting.
func pollSchedulePreflight(ctx context.Context, sched pollScheduleAPI, action string) (*scheduler.GetScheduleOutput, error) {
	return arm.PreflightPollSchedule(ctx, sched, action)
}

// enablePollScheduleIfOff is arm.EnablePollScheduleIfOff, saying so under
// --verbose when it switched the schedule on.
func enablePollScheduleIfOff(ctx context.Context, sched pollScheduleAPI, stderr io.Writer, cur *scheduler.GetScheduleOutput) error {
	enabled, err := arm.EnablePollScheduleIfOff(ctx, sched, cur)
	if err != nil {
		return err
	}
	if enabled && verbose {
		_, _ = fmt.Fprintf(stderr, "Enabled polling schedule %s\n", aws.ToString(cur.Name))
	}
	return nil
}

// pollScheduleUpdateRefusal is arm.PollScheduleUpdateRefusal: the refusal after
// enablePollScheduleIfOff failed and the caller rolled back.
func pollScheduleUpdateRefusal(action, undone string, rollbackErr, err error) error {
	return arm.PollScheduleUpdateRefusal(action, undone, rollbackErr, err)
}
