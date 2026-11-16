package deploy

// Per-resource deleters for the plan-driven teardown (`lagotto teardown
// --plan`, lagotto#325). Each removes one resource of a lagotto deployment
// with the same calls Teardown makes for it, and reports whether it was
// there, so the footprint executor (libs/footprint.Execute) can delete a
// deployment one resource at a time, in its dependency order, and record what
// it did. They take no resource name they could be pointed elsewhere with,
// except the schedule deleters, which accept only lagotto's names.

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// SchedulePrefix starts the name of every schedule lagotto creates: the
// poller's (PollerScheduleName), the per-launch ones (LaunchSchedulePrefix)
// and the idle-teardown jobs (lagotto#326).
const SchedulePrefix = "lagotto-"

// ScheduleGroups are the schedule groups lagotto creates: its own
// (ScheduleGroupName) and the idle-teardown jobs' group
// (runtimeiam.TeardownScheduleGroup).
var ScheduleGroups = []string{ScheduleGroupName, runtimeiam.TeardownScheduleGroup}

// IsScheduleGroup reports whether name is one of ScheduleGroups.
func IsScheduleGroup(name string) bool {
	for _, g := range ScheduleGroups {
		if name == g {
			return true
		}
	}
	return false
}

// DeletePollerFunction deletes the poller Lambda in this Deployer's region
// and reports whether it was there.
func (d *Deployer) DeletePollerFunction(ctx context.Context) (bool, error) {
	exists, err := d.pollerFunctionExists(ctx)
	if err != nil || !exists {
		return false, err
	}
	return true, d.deletePollerFunction(ctx)
}

// DeleteAlertsTopic deletes the capacity-alerts topic in region and reports
// whether it was there (SNS DeleteTopic alone cannot tell).
func (d *Deployer) DeleteAlertsTopic(ctx context.Context, region, accountID string) (bool, error) {
	exists, err := d.alertsTopicExists(ctx, region, accountID)
	if err != nil || !exists {
		return false, err
	}
	return true, d.deleteAlertsTopic(ctx, region, accountID)
}

// DeletePollerLogGroup deletes the poller's log group in region and reports
// whether it was there. Lambda recreates the group on the function's next
// invocation, so it must run after the function is gone.
func (d *Deployer) DeletePollerLogGroup(ctx context.Context, region string) (bool, error) {
	return d.deletePollerLogGroup(ctx, region)
}

// DeleteSchedule deletes the schedule name from group and reports whether it
// was there. Only a lagotto-* schedule in lagotto's groups or the default one
// (where schedules from before lagotto#324 are) is accepted.
func (d *Deployer) DeleteSchedule(ctx context.Context, group, name string) (bool, error) {
	if !strings.HasPrefix(name, SchedulePrefix) || (!IsScheduleGroup(group) && group != schedgroup.Default) {
		return false, fmt.Errorf("delete schedule %s/%s: not a lagotto schedule", group, name)
	}
	_, err := d.sched.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name:      aws.String(name),
		GroupName: aws.String(group),
	})
	switch {
	case err == nil:
		return true, nil
	case schedgroup.IsNotFound(err):
		return false, nil
	}
	return false, fmt.Errorf("delete schedule %s from schedule group %s: %w", name, group, err)
}

// DeleteScheduleGroup deletes one of lagotto's schedule groups
// (ScheduleGroups) and reports whether it was there. One that still holds a
// schedule is kept, with an error wrapping ErrScheduleGroupNotEmpty:
// DeleteScheduleGroup would delete its schedules with it.
func (d *Deployer) DeleteScheduleGroup(ctx context.Context, name string) (bool, error) {
	if !IsScheduleGroup(name) {
		return false, fmt.Errorf("delete schedule group %s: not one of lagotto's (%s)", name, strings.Join(ScheduleGroups, ", "))
	}
	return d.deleteGroupIfEmpty(ctx, name)
}
