package deploy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// Fixed resource names. Every one of the three poller resources has a fixed
// name, which is what makes the SDK-native Ensure* path (#154) able to ADOPT
// resources a prior CloudFormation deploy created instead of duplicating them —
// and what lets `lagotto launch` derive the ARNs it needs from account+region
// rather than reading stack outputs.
const (
	// PollerFunctionName is the hosted capacity-poller Lambda.
	PollerFunctionName = "lagotto-capacity-poller"
	// AlertsTopicName is the SNS topic the poller publishes capacity alerts to.
	AlertsTopicName = "lagotto-capacity-alerts"
	// PollerScheduleName is the EventBridge Scheduler schedule that invokes the
	// poller. Same string as the function name by design (the template does the
	// same); the poller also receives it as SCHEDULE_NAME so it can self-disable.
	PollerScheduleName = PollerFunctionName
)

// Lambda function shape — the contract the CFN/SAM template establishes and the
// SDK path must reproduce exactly (deployment/cloudformation/lagotto-stack.yaml).
const (
	pollerRuntime    = "provided.al2023"
	pollerHandler    = "bootstrap"
	pollerTimeout    = 900
	pollerMemorySize = 512
	pollerArch       = "arm64"
)

// PollerFunctionARN returns the constructed ARN of the poller Lambda.
//
// NOTE: pkg/runtimeiam builds this same ARN inline (in its scheduler-invoke
// inline policy and in PolicyDocument's scheduler statements). That duplication
// is deliberate for now — runtimeiam must not import pkg/deploy (deploy imports
// runtimeiam), and collapsing it would mean moving the names to a third package
// right on top of the just-landed #153 work. Left for a later cleanup.
func PollerFunctionARN(region, accountID string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", region, accountID, PollerFunctionName)
}

// AlertsTopicARN returns the constructed ARN of the capacity-alerts SNS topic.
func AlertsTopicARN(region, accountID string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, AlertsTopicName)
}

// --- Test seams -------------------------------------------------------------
//
// Narrow, UNEXPORTED per-service interfaces: only the operations pkg/deploy
// actually calls, so an in-package fake in a test file is a few methods rather
// than a whole service, and none of this leaks into the package's public API.
// The concrete *lambda.Client / *sns.Client / *scheduler.Client /
// *cloudformation.Client satisfy them.

type lambdaAPI interface {
	GetFunction(ctx context.Context, in *lambda.GetFunctionInput, optFns ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error)
	CreateFunction(ctx context.Context, in *lambda.CreateFunctionInput, optFns ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error)
	UpdateFunctionCode(ctx context.Context, in *lambda.UpdateFunctionCodeInput, optFns ...func(*lambda.Options)) (*lambda.UpdateFunctionCodeOutput, error)
	UpdateFunctionConfiguration(ctx context.Context, in *lambda.UpdateFunctionConfigurationInput, optFns ...func(*lambda.Options)) (*lambda.UpdateFunctionConfigurationOutput, error)
	DeleteFunction(ctx context.Context, in *lambda.DeleteFunctionInput, optFns ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error)
	TagResource(ctx context.Context, in *lambda.TagResourceInput, optFns ...func(*lambda.Options)) (*lambda.TagResourceOutput, error)
}

type snsAPI interface {
	CreateTopic(ctx context.Context, in *sns.CreateTopicInput, optFns ...func(*sns.Options)) (*sns.CreateTopicOutput, error)
	GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error)
	SetTopicAttributes(ctx context.Context, in *sns.SetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error)
	TagResource(ctx context.Context, in *sns.TagResourceInput, optFns ...func(*sns.Options)) (*sns.TagResourceOutput, error)
	DeleteTopic(ctx context.Context, in *sns.DeleteTopicInput, optFns ...func(*sns.Options)) (*sns.DeleteTopicOutput, error)
}

type schedulerAPI interface {
	GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
	CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
	UpdateSchedule(ctx context.Context, in *scheduler.UpdateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error)
	DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error)
}

type cfnAPI interface {
	DescribeStacks(ctx context.Context, in *cloudformation.DescribeStacksInput, optFns ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error)
	GetTemplate(ctx context.Context, in *cloudformation.GetTemplateInput, optFns ...func(*cloudformation.Options)) (*cloudformation.GetTemplateOutput, error)
	CreateStack(ctx context.Context, in *cloudformation.CreateStackInput, optFns ...func(*cloudformation.Options)) (*cloudformation.CreateStackOutput, error)
	UpdateStack(ctx context.Context, in *cloudformation.UpdateStackInput, optFns ...func(*cloudformation.Options)) (*cloudformation.UpdateStackOutput, error)
	DeleteStack(ctx context.Context, in *cloudformation.DeleteStackInput, optFns ...func(*cloudformation.Options)) (*cloudformation.DeleteStackOutput, error)
}

// --- Waiting / retrying -----------------------------------------------------

const (
	// settlePollInterval is how often waitFunctionSettled re-reads the function.
	settlePollInterval = 2 * time.Second
	// settleMaxWait bounds waitFunctionSettled.
	settleMaxWait = 5 * time.Minute
	// activeMaxWait bounds the post-create FunctionActiveV2Waiter.
	activeMaxWait = 2 * time.Minute
	// rolePropagationAttempts is how many times CreateFunction is retried while
	// an IAM role created moments earlier propagates. ~60s of backoff.
	rolePropagationAttempts = 10
	// conflictAttempts bounds the ResourceConflictException retry on updates.
	conflictAttempts = 6
)

// waitFunctionSettled blocks until the named function has no update in flight,
// so the next Update*/Tag call can't fail with ResourceConflictException
// ("...An update is in progress for resource...").
//
// This is deliberately HAND-ROLLED rather than lambda.FunctionUpdatedV2Waiter.
// DO NOT "simplify" it back to the SDK waiter: substrate's Lambda emulator never
// returns LastUpdateStatus at all, and the SDK's functionUpdatedV2StateRetryable
// DEFAULTS TO RETRY when no matcher matches. Against an emulator (or any
// response that omits the field) that waiter therefore spins its entire max-wait
// budget and then fails with "exceeded max wait time", turning every offline
// test into a multi-minute timeout. lambda.FunctionActiveV2Waiter is fine and IS
// used below — it matches on State == Active, which substrate does populate.
//
// Semantics, mirroring what real Lambda reports:
//   - State == Pending                 → still being created, wait.
//   - LastUpdateStatus == InProgress   → an update is in flight, wait.
//   - LastUpdateStatus == Failed       → error, carrying LastUpdateStatusReason.
//   - State == Failed                  → error, carrying StateReason.
//   - LastUpdateStatus empty AND State Active-or-empty → settled. Real Lambda
//     always populates LastUpdateStatus once anything has been applied, so an
//     absent value means nothing is in flight.
func (d *Deployer) waitFunctionSettled(ctx context.Context, name string) error {
	deadline := time.Now().Add(settleMaxWait)
	// Bound by poll COUNT as well as wall clock: d.sleep is injectable, so a test
	// (or a future caller) that supplies a no-op sleep must not be able to turn
	// this into a hot loop that spins for the full five minutes.
	maxPolls := int(settleMaxWait/settlePollInterval) + 1
	for i := 0; ; i++ {
		out, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(name)})
		if err != nil {
			return fmt.Errorf("check state of function %s: %w", name, err)
		}
		cfg := out.Configuration
		if cfg == nil {
			return nil // nothing to wait on
		}
		switch {
		case cfg.LastUpdateStatus == lambdatypes.LastUpdateStatusFailed:
			return fmt.Errorf("function %s last update failed: %s", name, aws.ToString(cfg.LastUpdateStatusReason))
		case cfg.State == lambdatypes.StateFailed:
			return fmt.Errorf("function %s is in state Failed: %s", name, aws.ToString(cfg.StateReason))
		case cfg.State == lambdatypes.StatePending,
			cfg.LastUpdateStatus == lambdatypes.LastUpdateStatusInProgress:
			// fall through to the sleep below
		default:
			return nil // settled
		}
		if time.Now().After(deadline) || i >= maxPolls {
			return fmt.Errorf("timed out after %s waiting for function %s to settle (state %s, last update %s)",
				settleMaxWait, name, cfg.State, cfg.LastUpdateStatus)
		}
		d.sleep(settlePollInterval)
	}
}

// retryTransient calls fn up to attempts times, sleeping with a bounded backoff
// (1s, 2s, 4s, 8s, 8s, ... — roughly a 60s total budget at attempts=10) between
// tries, but only while matches reports the error is worth retrying. Any other
// error, and the final error, are returned as-is.
//
// The sleep goes through the injected d.sleep so tests are instant.
func (d *Deployer) retryTransient(ctx context.Context, attempts int, matches func(error) bool, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	backoff := 1 * time.Second
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if !matches(err) {
			return err
		}
		if i == attempts-1 {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(err, ctxErr)
		}
		d.sleep(backoff)
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
	return err
}

// runtimeRoleARN / schedulerInvokeRoleARN re-export the runtimeiam helpers so
// callers inside pkg/deploy read consistently.
func runtimeRoleARN(accountID string) string { return runtimeiam.RoleARN(accountID) }

func schedulerInvokeRoleARN(accountID string) string {
	return runtimeiam.SchedulerInvokeRoleARN(accountID)
}
