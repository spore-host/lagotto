package deploy

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// LaunchSchedulePrefix starts the name of every per-launch schedule lagotto
// creates: `lagotto launch`'s lagotto-launch-sl-<id> and the poller's boundary
// retries, lagotto-launch-sl-<id>-r<n> (#62). The poller's own schedule
// (PollerScheduleName) does not match it.
const LaunchSchedulePrefix = "lagotto-launch-sl-"

// LaunchScheduleName is the schedule `lagotto launch` creates for the
// scheduled launch with this id ("sl-…"), and stores as its ScheduleName. An
// owner-scoped key ("<owner>/sl-…") names its schedule by the short id alone:
// schedule names allow only [0-9A-Za-z-_.].
func LaunchScheduleName(scheduleID string) string {
	if i := strings.Index(scheduleID, "/"); i >= 0 {
		scheduleID = scheduleID[i+1:]
	}
	return "lagotto-launch-" + scheduleID
}

// IsLaunchSchedule reports whether name is a per-launch schedule lagotto
// created.
func IsLaunchSchedule(name string) bool {
	return strings.HasPrefix(name, LaunchSchedulePrefix)
}

// IsScheduleOfLaunch reports whether name is one of the schedules of the
// scheduled launch scheduleID: the one `lagotto launch` created
// (LaunchScheduleName) or one of the poller's boundary retries of it,
// <that name>-r<n> (#62). The id is matched whole, so sl-0a1b2c3d does not
// claim the schedules of an sl-0a1b2c3d9.
func IsScheduleOfLaunch(name, scheduleID string) bool {
	base := LaunchScheduleName(scheduleID)
	if !IsLaunchSchedule(base) {
		return false
	}
	if name == base {
		return true
	}
	n, ok := strings.CutPrefix(name, base+"-r")
	if !ok || n == "" {
		return false
	}
	for _, c := range n {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ListLaunchSchedules returns the name of every per-launch schedule in
// lagotto's schedule group and the default one (#324), sorted and without
// duplicates, following every page.
func (d *Deployer) ListLaunchSchedules(ctx context.Context) ([]string, error) {
	refs, err := d.listSchedulesWithPrefix(ctx, LaunchSchedulePrefix, IsLaunchSchedule)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var names []string
	for _, r := range refs {
		if !seen[r.name] {
			seen[r.name] = true
			names = append(names, r.name)
		}
	}
	return names, nil
}

// scheduleRef is one schedule and the group it is in.
type scheduleRef struct{ group, name string }

// listSchedulesWithPrefix lists the schedules of both of lagotto's groups
// (schedgroup.Groups) whose name starts with prefix and satisfies keep,
// sorted by name, following every page. A group that does not exist (lagotto's,
// before a #324 deploy created it) has none.
func (d *Deployer) listSchedulesWithPrefix(ctx context.Context, prefix string, keep func(string) bool) ([]scheduleRef, error) {
	var refs []scheduleRef
	for _, group := range schedgroup.Groups {
		in := &scheduler.ListSchedulesInput{
			GroupName:  aws.String(group),
			NamePrefix: aws.String(prefix),
		}
		for {
			out, err := d.sched.ListSchedules(ctx, in)
			if err != nil {
				if group != schedgroup.Default && schedgroup.IsNotFound(err) {
					break
				}
				return nil, fmt.Errorf("list %s* schedules in schedule group %s: %w", prefix, group, err)
			}
			for _, s := range out.Schedules {
				// NamePrefix already filters; checking again keeps a scheduler that
				// ignores it (an emulator) from handing back a schedule we must not delete.
				if name := aws.ToString(s.Name); keep(name) {
					refs = append(refs, scheduleRef{group: group, name: name})
				}
			}
			if aws.ToString(out.NextToken) == "" {
				break
			}
			in.NextToken = out.NextToken
		}
	}
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].name < refs[j].name })
	return refs, nil
}

// LaunchScheduleTeardown is what DeleteLaunchSchedules did.
type LaunchScheduleTeardown struct {
	// Deleted holds the schedules DeleteSchedule removed.
	Deleted []string
	// Absent holds recorded schedules that no longer existed: typically a
	// one-shot that fired and removed itself (ActionAfterCompletion=DELETE).
	Absent []string
	// GroupDeleted reports that DeleteLaunchSchedules also deleted lagotto's
	// schedule group, which nothing was left in (#324).
	GroupDeleted bool
}

// DeleteLaunchSchedules deletes every per-launch schedule (#260): the ones
// named by scheduled-launch records (recorded) and every one ListLaunchSchedules
// finds, which catches the poller's retry schedules and any schedule whose
// record is gone. A recorded name without LaunchSchedulePrefix is skipped:
// lagotto never writes one, so it is not lagotto's to delete. Then, when no
// schedule is left in it, it deletes lagotto's schedule group (#324).
//
// The listing runs before any deletion, so a caller without
// scheduler:ListSchedules gets an error and nothing is deleted. A schedule that
// is already gone is reported in Absent, never as an error.
func (d *Deployer) DeleteLaunchSchedules(ctx context.Context, recorded []string) (LaunchScheduleTeardown, error) {
	listed, err := d.listSchedulesWithPrefix(ctx, LaunchSchedulePrefix, IsLaunchSchedule)
	if err != nil {
		return LaunchScheduleTeardown{}, err
	}
	res, err := d.deleteSchedules(ctx, recorded, listed, IsLaunchSchedule)
	if err != nil {
		return res, err
	}
	res.GroupDeleted, err = d.deleteScheduleGroupIfEmpty(ctx)
	return res, err
}

// DeleteScheduledLaunchSchedules deletes the schedules of one scheduled launch
// (`lagotto cancel sl-…`, #279): recorded, the ScheduleName its record holds,
// and every schedule IsScheduleOfLaunch matches in lagotto's and the default
// schedule group, which catches the poller's -r<n> boundary retries. A name
// that is not one of the launch's own is skipped, so a corrupt record cannot
// point cancel at another schedule.
//
// The listing runs before any deletion; an error there deletes nothing. A
// schedule that is already gone (a one-shot that fired and removed itself) is
// reported in Absent, never as an error.
func (d *Deployer) DeleteScheduledLaunchSchedules(ctx context.Context, scheduleID, recorded string) (LaunchScheduleTeardown, error) {
	mine := func(n string) bool { return IsScheduleOfLaunch(n, scheduleID) }
	listed, err := d.listSchedulesWithPrefix(ctx, LaunchScheduleName(scheduleID), mine)
	if err != nil {
		return LaunchScheduleTeardown{}, err
	}
	// The launch's own schedule is always tried, even when the record names none:
	// it is the one `lagotto launch` creates, and not-found is not an error.
	return d.deleteSchedules(ctx, []string{LaunchScheduleName(scheduleID), recorded}, listed, mine)
}

// deleteSchedules deletes, in name order, every listed schedule in the group
// it was listed in, and every recorded name the listing did not show, which
// it tries in the default group (where a schedule just created by an older
// lagotto, too new to be listed, would be) and in lagotto's. Names keep
// rejects are skipped. Each is reported as Deleted or, when no group had it,
// Absent.
//
// A recorded name not listed is gone, or too new to be listed. For it, an
// AccessDenied from lagotto's group (a caller whose policy predates #324) is
// read as absent there: a schedule can only be in that group if its creator
// was granted the group, and the listing, which covers it, did not show one.
func (d *Deployer) deleteSchedules(ctx context.Context, recorded []string, listed []scheduleRef, keep func(string) bool) (LaunchScheduleTeardown, error) {
	var res LaunchScheduleTeardown
	groupsOf := map[string][]string{}
	for _, r := range listed {
		if keep(r.name) {
			groupsOf[r.name] = append(groupsOf[r.name], r.group)
		}
	}
	unlisted := map[string]bool{}
	for _, n := range recorded {
		if keep(n) && groupsOf[n] == nil {
			unlisted[n] = true
		}
	}
	var names []string
	for n := range groupsOf {
		names = append(names, n)
	}
	for n := range unlisted {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		groups, tolerateDenied := groupsOf[name], false
		if groups == nil {
			groups, tolerateDenied = []string{schedgroup.Default, schedgroup.Name}, true
		}
		gone := false
		for _, group := range groups {
			_, err := d.sched.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
				Name:      aws.String(name),
				GroupName: aws.String(group),
			})
			switch {
			case err == nil:
				gone = true
			case schedgroup.IsNotFound(err):
			case tolerateDenied && group == schedgroup.Name && schedgroup.IsAccessDenied(err):
			default:
				return res, fmt.Errorf("delete schedule %s from schedule group %s: %w", name, group, err)
			}
		}
		if gone {
			res.Deleted = append(res.Deleted, name)
		} else {
			res.Absent = append(res.Absent, name)
		}
	}
	return res, nil
}
