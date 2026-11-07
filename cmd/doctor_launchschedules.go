package cmd

import (
	"context"

	"github.com/spore-host/lagotto/pkg/doctor"
)

// launchScheduleLister lists the per-launch schedules; *deploy.Deployer
// satisfies it.
type launchScheduleLister interface {
	ListLaunchSchedules(ctx context.Context) ([]string, error)
}

// appendLaunchScheduleCheck adds doctor's orphan per-launch schedule check
// (#260) to report. The records are read only when there are schedules to
// match, so an account with none makes one ListSchedules call.
func appendLaunchScheduleCheck(ctx context.Context, report *doctor.Report, region string,
	sched launchScheduleLister, recorded func(context.Context) ([]string, error)) {
	names, listErr := sched.ListLaunchSchedules(ctx)
	var rec []string
	var recErr error
	if listErr == nil && len(names) > 0 {
		rec, recErr = recorded(ctx)
	}
	if c, ok := doctor.EvalLaunchSchedules(region, names, listErr, rec, recErr); ok {
		report.Checks = append(report.Checks, c)
	}
}
