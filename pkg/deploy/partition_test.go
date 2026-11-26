package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"

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
// lagotto does not support, aws-cn included (the Deployer has no clients:
// any call panics).
func TestDeployRefusesUnsupportedPartitions(t *testing.T) {
	for region, want := range map[string]error{"us-iso-east-1": awsarn.ErrUnsupportedPartition,
		"eusc-de-east-1": awsarn.ErrUnsupportedPartition, "cn-north-1": awsarn.ErrPartitionNotYetSupported} {
		_, err := (&Deployer{}).Deploy(context.Background(), Options{AccountID: "123456789012", Region: region, Version: "0.73.0"})
		if !errors.Is(err, want) {
			t.Errorf("%s: %v", region, err)
		}
	}
}

// topicARNs records the topic ARN of every SNS call that names one.
type topicARNs struct {
	*fakeSNS
	arns []string
}

func (r *topicARNs) GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, o ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	r.arns = append(r.arns, aws.ToString(in.TopicArn))
	return r.fakeSNS.GetTopicAttributes(ctx, in, o...)
}

func (r *topicARNs) DeleteTopic(ctx context.Context, in *sns.DeleteTopicInput, o ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	r.arns = append(r.arns, aws.ToString(in.TopicArn))
	return r.fakeSNS.DeleteTopic(ctx, in, o...)
}

// TestSetPartition (lagotto#347): a Deployer given a partition names its
// topic in it, where the region's name gives none (an ISO region), and
// without one uses the region's.
func TestSetPartition(t *testing.T) {
	for _, c := range []struct{ region, partition, want string }{
		{"us-iso-east-1", "aws-iso", "arn:aws-iso:sns:us-iso-east-1:123456789012:" + AlertsTopicName},
		{"us-gov-west-1", "", "arn:aws-us-gov:sns:us-gov-west-1:123456789012:" + AlertsTopicName},
	} {
		log := &callLog{}
		sn := &topicARNs{fakeSNS: &fakeSNS{log: log, attributes: map[string]string{}}}
		d, _ := testDeployer(&fakeLambda{log: log, getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("none")}},
			sn, &fakeScheduler{log: log, getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("none")}})
		if c.partition != "" {
			d.SetPartition(c.partition)
		}
		if _, err := d.DeleteAlertsTopic(context.Background(), c.region, "123456789012"); err != nil {
			t.Fatal(err)
		}
		if len(sn.arns) != 2 || sn.arns[0] != c.want || sn.arns[1] != c.want {
			t.Errorf("%s: topic ARNs %v, want %s", c.region, sn.arns, c.want)
		}
		if got := d.arn("logs", c.region, "123456789012", "log-group:x"); !strings.HasPrefix(got, "arn:"+strings.Split(c.want, ":")[1]+":logs:") {
			t.Errorf("%s: log group ARN %s", c.region, got)
		}
	}
}
