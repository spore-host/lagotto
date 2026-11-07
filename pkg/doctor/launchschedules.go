package doctor

import (
	"fmt"
	"strings"
)

// CheckLaunchSchedules names the orphan per-launch schedule check (#260).
const CheckLaunchSchedules = "launch-schedule"

// EvalLaunchSchedules reports per-launch lagotto-launch-sl-* schedules that
// no scheduled-launch record names: left behind when the scheduled-launches
// table was deleted under them (a `lagotto teardown` before #260 did that), so
// nothing lists or cancels them, and a cron one keeps firing indefinitely. A
// poller boundary-retry schedule (<record's schedule>-r<n>, #62) belongs to
// its record.
//
// schedules is every per-launch schedule found; recorded is every
// ScheduleName in the table. Pure, like Evaluate. All outcomes are WARN at
// most: an orphan costs invocations, not correctness of the poller.
//
// ok is false when there is nothing to say: no per-launch schedule at all.
func EvalLaunchSchedules(region string, schedules []string, listErr error, recorded []string, recordsErr error) (Check, bool) {
	c := Check{Name: CheckLaunchSchedules}
	switch {
	case listErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not list per-launch lagotto-launch-sl-* schedules (scheduler:ListSchedules): %v", listErr)
		return c, true
	case len(schedules) == 0:
		return Check{}, false
	case recordsErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("%d per-launch schedule(s) exist, but the scheduled-launch records could not be read to match them: %v",
			len(schedules), recordsErr)
		return c, true
	}

	var orphans []string
	for _, s := range schedules {
		if !hasRecord(s, recorded) {
			orphans = append(orphans, s)
		}
	}
	if len(orphans) == 0 {
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("%d per-launch schedule(s), each with a scheduled-launch record", len(schedules))
		return c, true
	}
	c.Status = StatusWarn
	c.Summary = fmt.Sprintf("%d per-launch schedule(s) have no scheduled-launch record: nothing tracks or cancels them, and a cron one fires indefinitely", len(orphans))
	for _, o := range orphans {
		c.Detail = append(c.Detail, fmt.Sprintf("aws scheduler delete-schedule --name %s --region %s", o, region))
	}
	c.Detail = append(c.Detail, "or 'lagotto teardown' (deletes every per-launch schedule together with lagotto's tables)")
	return c, true
}

// hasRecord reports whether schedule is a recorded schedule or one of its
// boundary retries.
func hasRecord(schedule string, recorded []string) bool {
	for _, r := range recorded {
		if r != "" && (schedule == r || strings.HasPrefix(schedule, r+"-r")) {
			return true
		}
	}
	return false
}
