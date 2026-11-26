package deploy

// lagotto's EventBridge Scheduler schedule group (#324).
//
// A schedule cannot be tagged, so lagotto puts its schedules in a group of its
// own (schedgroup.Name) and tags the group: `lagotto deploy` creates it, and
// the teardowns delete it once nothing is left in it.

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/libs/tags/schedulertags"

	"github.com/spore-host/lagotto/pkg/managedtags"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// ScheduleGroupName is lagotto's schedule group.
const ScheduleGroupName = schedgroup.Name

// ensureScheduleGroup creates lagotto's schedule group in region, tagged, and
// returns what it did. An existing group is adopted: the spore.host tags it
// lacks are added (managedtags.Converge).
func (d *Deployer) ensureScheduleGroup(ctx context.Context, region, accountID string) (string, error) {
	want := d.infraTags(region, managedtags.ComponentPoller)
	_, err := d.sched.CreateScheduleGroup(ctx, &scheduler.CreateScheduleGroupInput{
		Name: aws.String(ScheduleGroupName),
		Tags: schedulertags.FromMap(want),
	})
	if err == nil {
		return ActionCreated, nil
	}
	var conflict *schedulertypes.ConflictException
	if !errors.As(err, &conflict) {
		return "", fmt.Errorf("create schedule group %s: %w", ScheduleGroupName, err)
	}

	arn := d.arn("scheduler", region, accountID, "schedule-group/"+schedgroup.Name)
	cur, err := d.sched.ListTagsForResource(ctx, &scheduler.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil {
		return "", fmt.Errorf("read tags of schedule group %s: %w", ScheduleGroupName, err)
	}
	add := managedtags.Converge(schedulertags.ToMap(cur.Tags), want)
	if len(add) == 0 {
		return ActionUnchanged, nil
	}
	if _, err := d.sched.TagResource(ctx, &scheduler.TagResourceInput{
		ResourceArn: aws.String(arn),
		Tags:        schedulertags.FromMap(add),
	}); err != nil {
		return "", fmt.Errorf("tag schedule group %s: %w", ScheduleGroupName, err)
	}
	return "exists, tagged", nil
}

// deleteScheduleGroupIfEmpty deletes lagotto's schedule group when no schedule
// is left in it, and reports whether it did. DeleteScheduleGroup deletes every
// schedule in the group with it, so a group that still holds one (a pending
// per-launch schedule, or the poller schedule of a poller still deployed) is
// kept. An absent group is (false, nil).
func (d *Deployer) deleteScheduleGroupIfEmpty(ctx context.Context) (bool, error) {
	gone, err := d.deleteGroupIfEmpty(ctx, ScheduleGroupName)
	if errors.Is(err, ErrScheduleGroupNotEmpty) {
		return false, nil
	}
	return gone, err
}

// ErrScheduleGroupNotEmpty is what deleting a schedule group that still holds
// a schedule returns: DeleteScheduleGroup would delete them all with it.
var ErrScheduleGroupNotEmpty = errors.New("the schedule group still holds a schedule")

// deleteGroupIfEmpty deletes the named schedule group if no schedule is left
// in it. A non-empty group is ErrScheduleGroupNotEmpty and is kept; an absent
// one is (false, nil).
func (d *Deployer) deleteGroupIfEmpty(ctx context.Context, name string) (bool, error) {
	out, err := d.sched.ListSchedules(ctx, &scheduler.ListSchedulesInput{
		GroupName:  aws.String(name),
		MaxResults: aws.Int32(1),
	})
	if err != nil {
		if schedgroup.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("list the schedules in schedule group %s: %w", name, err)
	}
	if len(out.Schedules) > 0 || aws.ToString(out.NextToken) != "" {
		return false, fmt.Errorf("schedule group %s: %w", name, ErrScheduleGroupNotEmpty)
	}
	if _, err := d.sched.DeleteScheduleGroup(ctx, &scheduler.DeleteScheduleGroupInput{
		Name: aws.String(name),
	}); err != nil {
		if schedgroup.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("delete schedule group %s: %w", name, err)
	}
	return true, nil
}
