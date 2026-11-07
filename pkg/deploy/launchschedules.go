package deploy

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// launchScheduleGroup is the EventBridge Scheduler group the per-launch
// schedules live in: `lagotto launch` and the poller's #62 boundary retry both
// create them without a GroupName, i.e. in "default".
const launchScheduleGroup = "default"

// LaunchSchedulePrefix starts the name of every per-launch schedule lagotto
// creates: `lagotto launch`'s lagotto-launch-sl-<id> and the poller's boundary
// retries, lagotto-launch-sl-<id>-r<n> (#62). The poller's own schedule
// (PollerScheduleName) does not match it.
const LaunchSchedulePrefix = "lagotto-launch-sl-"

// LaunchScheduleName is the schedule `lagotto launch` creates for the
// scheduled launch with this id ("sl-…"), and stores as its ScheduleName.
func LaunchScheduleName(scheduleID string) string {
	return "lagotto-launch-" + scheduleID
}

// IsLaunchSchedule reports whether name is a per-launch schedule lagotto
// created.
func IsLaunchSchedule(name string) bool {
	return strings.HasPrefix(name, LaunchSchedulePrefix)
}

// ListLaunchSchedules returns the name of every per-launch schedule in the
// default group, sorted, following every page.
func (d *Deployer) ListLaunchSchedules(ctx context.Context) ([]string, error) {
	var names []string
	in := &scheduler.ListSchedulesInput{
		GroupName:  aws.String(launchScheduleGroup),
		NamePrefix: aws.String(LaunchSchedulePrefix),
	}
	for {
		out, err := d.sched.ListSchedules(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list %s* schedules: %w", LaunchSchedulePrefix, err)
		}
		for _, s := range out.Schedules {
			// NamePrefix already filters; checking again keeps a scheduler that
			// ignores it (an emulator) from handing back a schedule we must not delete.
			if name := aws.ToString(s.Name); IsLaunchSchedule(name) {
				names = append(names, name)
			}
		}
		if aws.ToString(out.NextToken) == "" {
			break
		}
		in.NextToken = out.NextToken
	}
	sort.Strings(names)
	return names, nil
}

// LaunchScheduleTeardown is what DeleteLaunchSchedules did.
type LaunchScheduleTeardown struct {
	// Deleted holds the schedules DeleteSchedule removed.
	Deleted []string
	// Absent holds recorded schedules that no longer existed: typically a
	// one-shot that fired and removed itself (ActionAfterCompletion=DELETE).
	Absent []string
}

// DeleteLaunchSchedules deletes every per-launch schedule (#260): the ones
// named by scheduled-launch records (recorded) and every one ListLaunchSchedules
// finds, which catches the poller's retry schedules and any schedule whose
// record is gone. A recorded name without LaunchSchedulePrefix is skipped:
// lagotto never writes one, so it is not lagotto's to delete.
//
// The listing runs before any deletion, so a caller without
// scheduler:ListSchedules gets an error and nothing is deleted. A schedule that
// is already gone is reported in Absent, never as an error.
func (d *Deployer) DeleteLaunchSchedules(ctx context.Context, recorded []string) (LaunchScheduleTeardown, error) {
	var res LaunchScheduleTeardown
	listed, err := d.ListLaunchSchedules(ctx)
	if err != nil {
		return res, err
	}
	seen := map[string]bool{}
	var names []string
	for _, n := range append(append([]string{}, recorded...), listed...) {
		if IsLaunchSchedule(n) && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		_, err := d.sched.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
			Name:      aws.String(name),
			GroupName: aws.String(launchScheduleGroup),
		})
		var notFound *schedulertypes.ResourceNotFoundException
		switch {
		case err == nil:
			res.Deleted = append(res.Deleted, name)
		case errors.As(err, &notFound):
			res.Absent = append(res.Absent, name)
		default:
			return res, fmt.Errorf("delete schedule %s: %w", name, err)
		}
	}
	return res, nil
}
