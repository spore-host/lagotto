package cmd

import (
	"context"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/doctor"
)

// appendCallerCheck adds doctor's caller check (#232): does the identity
// running doctor have what the everyday commands need? callerARN is the ARN
// STS reported; for an assumed role cliiam simulates the role.
func appendCallerCheck(ctx context.Context, report *doctor.Report, client cliiam.SimulateAPI, callerARN, accountID, region string) {
	res := cliiam.CheckCaller(ctx, client, callerARN, accountID, region)
	report.Checks = append(report.Checks, doctor.EvalCallerPermissions(res, accountID))
}
