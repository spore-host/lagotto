package deploy

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// TestResourceARNsInEachPartition: the poller's ARNs are in its region's
// partition (lagotto#347), what the scheduler targets, the function's
// environment and the footprint compare against.
func TestResourceARNsInEachPartition(t *testing.T) {
	const acct = "123456789012"
	for region, p := range map[string]string{"us-gov-west-1": "aws-us-gov", "cn-northwest-1": "aws-cn", "eu-west-1": "aws"} {
		pre := "arn:" + p + ":"
		for got, want := range map[string]string{
			PollerFunctionARN(region, acct):      pre + "lambda:" + region + ":" + acct + ":function:" + PollerFunctionName,
			AlertsTopicARN(region, acct):         pre + "sns:" + region + ":" + acct + ":" + AlertsTopicName,
			PollerLogGroupARN(region, acct):      pre + "logs:" + region + ":" + acct + ":log-group:" + PollerLogGroupName,
			schedgroup.ARN(region, acct):         pre + "scheduler:" + region + ":" + acct + ":schedule-group/" + schedgroup.Name,
			runtimeRoleARN(region, acct):         pre + "iam::" + acct + ":role/lagotto-capacity-poller-role",
			schedulerInvokeRoleARN(region, acct): pre + "iam::" + acct + ":role/lagotto-capacity-poller-scheduler-invoke",
		} {
			if got != want {
				t.Errorf("%s: %q, want %q", region, got, want)
			}
		}
		env := PollerEnvVars(region, acct, "", "", "", "")
		if env["POLLER_FUNCTION_ARN"] != PollerFunctionARN(region, acct) || env["SCHEDULER_INVOKE_ROLE_ARN"] != schedulerInvokeRoleARN(region, acct) {
			t.Errorf("%s: the poller's environment %v", region, env)
		}
	}
}

// TestDeployRefusesUnsupportedPartitions: deploy creates nothing in a region
// lagotto does not support (the Deployer has no clients: any call panics).
func TestDeployRefusesUnsupportedPartitions(t *testing.T) {
	for _, region := range []string{"us-iso-east-1", "eusc-de-east-1"} {
		_, err := (&Deployer{}).Deploy(context.Background(), Options{AccountID: "123456789012", Region: region, Version: "0.73.0"})
		if !errors.Is(err, awsarn.ErrUnsupportedPartition) {
			t.Errorf("%s: %v", region, err)
		}
	}
}
