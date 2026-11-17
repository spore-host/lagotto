package idle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/libs/tags/schedulertags"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/managedtags"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The self-removal plan. The poller never deletes its own function, log group
// or role from inside itself: deleting the function mid-run is the obvious
// failure, and its log group is worse, because Lambda's log delivery recreates
// a deleted group, untagged and with no retention, within seconds of a
// delete while the function is still flushing (seen live on 2026-11-16, dev
// account, us-east-1; lagotto#326). Instead it hands those deletes to
// one-shot EventBridge Scheduler jobs in the lagotto-teardown group, which
// run as the Scheduler invoke role (its LagottoTeardown policy grants exactly
// them) and delete themselves after they run.

// The offsets of the one-shot jobs from the moment the teardown plans them.
const (
	// FunctionOffset: the function goes once this run has long finished.
	FunctionOffset = 2 * time.Minute
	// RolePolicyOffset: the poller role is emptied after the function is
	// gone (DeleteRole refuses a role that still has policies)...
	RolePolicyOffset = 4 * time.Minute
	// RoleOffset: ...and deleted a minute later.
	RoleOffset = 5 * time.Minute
	// LogGroupOffset: the log group goes well after the function's last log
	// flush, so nothing recreates it.
	LogGroupOffset = 15 * time.Minute
)

// OneShot is one teardown job: at At (Offset after planning), EventBridge
// Scheduler calls the AWS API Target with Input, as the invoke role.
type OneShot struct {
	// Name is the schedule's name in TeardownScheduleGroup. It is fixed per
	// job, so a teardown that crashed and runs again creates the same jobs,
	// and a ConflictException means the job is already there.
	Name   string
	Offset time.Duration
	At     time.Time
	// Target is the universal target, arn:aws:scheduler:::aws-sdk:<service>:<action>.
	Target string
	Input  string
	// Resource is the ARN the job deletes, Why why it is a job.
	Resource string
	Why      string
}

// Expression is the job's schedule expression, at() in UTC.
func (o OneShot) Expression() string { return "at(" + o.At.UTC().Format("2006-01-02T15:04:05") + ")" }

// SelfRemoval says which of the poller's own resources the plan removes.
type SelfRemoval struct {
	Region, AccountID string
	// Function and LogGroup: the poller function and its log group pass the
	// delete guard. The log group is removed whenever the function is, since
	// the function's last flush can recreate it.
	Function, LogGroup bool
	// Role: this is the last region and the poller role passes the guard.
	Role bool
}

// sdkTarget is a Scheduler universal target.
func sdkTarget(service, action string) string {
	return "arn:aws:scheduler:::aws-sdk:" + service + ":" + action
}

func input(v map[string]string) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// Plan returns the one-shot jobs, in the order they run, planned at now.
func (s SelfRemoval) Plan(now time.Time) []OneShot {
	at := func(o OneShot) OneShot { o.At = now.UTC().Add(o.Offset).Truncate(time.Second); return o }
	var out []OneShot
	if s.Function {
		out = append(out, at(OneShot{
			Name: "lagotto-teardown-function", Offset: FunctionOffset,
			Target:   sdkTarget("lambda", "deleteFunction"),
			Input:    input(map[string]string{"FunctionName": deploy.PollerFunctionName}),
			Resource: deploy.PollerFunctionARN(s.Region, s.AccountID),
			Why:      "the poller cannot delete the function it runs in",
		}))
	}
	if s.Role {
		role := runtimeiam.RoleName
		out = append(out,
			at(OneShot{
				Name: "lagotto-teardown-role-inline", Offset: RolePolicyOffset,
				Target:   sdkTarget("iam", "deleteRolePolicy"),
				Input:    input(map[string]string{"RoleName": role, "PolicyName": runtimeiam.PolicyName}),
				Resource: runtimeiam.RoleARN(s.AccountID),
				Why:      "the poller role is emptied after the function that runs as it is gone",
			}),
			at(OneShot{
				Name: "lagotto-teardown-role-managed", Offset: RolePolicyOffset,
				Target:   sdkTarget("iam", "detachRolePolicy"),
				Input:    input(map[string]string{"RoleName": role, "PolicyArn": runtimeiam.BasicExecutionPolicyARN}),
				Resource: runtimeiam.RoleARN(s.AccountID),
				Why:      "the poller role is emptied after the function that runs as it is gone",
			}),
			at(OneShot{
				Name: "lagotto-teardown-role", Offset: RoleOffset,
				Target:   sdkTarget("iam", "deleteRole"),
				Input:    input(map[string]string{"RoleName": role}),
				Resource: runtimeiam.RoleARN(s.AccountID),
				Why:      "the poller cannot delete the role it runs as; this is the last region with a poller",
			}))
	}
	if s.Function || s.LogGroup {
		out = append(out, at(OneShot{
			Name: "lagotto-teardown-log-group", Offset: LogGroupOffset,
			Target:   sdkTarget("cloudwatchlogs", "deleteLogGroup"),
			Input:    input(map[string]string{"LogGroupName": deploy.PollerLogGroupName}),
			Resource: deploy.PollerLogGroupARN(s.Region, s.AccountID),
			Why:      "after the function's last log flush: deleted earlier, Lambda recreates it untagged",
		}))
	}
	return out
}

// OneShotAPI is the Scheduler API the jobs are created with; the SDK client
// satisfies it.
type OneShotAPI interface {
	CreateScheduleGroup(ctx context.Context, in *scheduler.CreateScheduleGroupInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleGroupOutput, error)
	CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
}

// ScheduleInput is the CreateSchedule request for one job.
func ScheduleInput(o OneShot, invokeRoleARN string) *scheduler.CreateScheduleInput {
	return &scheduler.CreateScheduleInput{
		Name:                  aws.String(o.Name),
		GroupName:             aws.String(runtimeiam.TeardownScheduleGroup),
		ScheduleExpression:    aws.String(o.Expression()),
		FlexibleTimeWindow:    &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		ActionAfterCompletion: schedulertypes.ActionAfterCompletionDelete,
		Description:           aws.String("lagotto idle teardown (lagotto#326): " + o.Why),
		Target: &schedulertypes.Target{
			Arn:     aws.String(o.Target),
			RoleArn: aws.String(invokeRoleARN),
			Input:   aws.String(o.Input),
		},
	}
}

func isConflict(err error) bool {
	var c *schedulertypes.ConflictException
	return errors.As(err, &c)
}

// CreateOneShots creates the lagotto-teardown schedule group, tagged, and
// then each job in it. Both are idempotent: a ConflictException (the group or
// job already exists, from a teardown that crashed after creating it) counts
// as success, so a resumed teardown carries on where it stopped. before, when
// set, runs before each job is created (the lease heartbeat); its error stops
// the creation.
func CreateOneShots(ctx context.Context, api OneShotAPI, region, accountID string, now time.Time, jobs []OneShot,
	before func(ctx context.Context, step string) error) (created []string, err error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	_, err = api.CreateScheduleGroup(ctx, &scheduler.CreateScheduleGroupInput{
		Name: aws.String(runtimeiam.TeardownScheduleGroup),
		Tags: schedulertags.FromMap(managedtags.Infrastructure(region, "", managedtags.ComponentPoller, now)),
	})
	if err != nil && !isConflict(err) {
		return nil, fmt.Errorf("create schedule group %s: %w", runtimeiam.TeardownScheduleGroup, err)
	}
	invoke := runtimeiam.SchedulerInvokeRoleARN(accountID)
	for _, j := range jobs {
		if before != nil {
			if err := before(ctx, "create one-shot "+j.Name); err != nil {
				return created, err
			}
		}
		if _, err := api.CreateSchedule(ctx, ScheduleInput(j, invoke)); err != nil && !isConflict(err) {
			return created, fmt.Errorf("create one-shot teardown job %s: %w", j.Name, err)
		}
		created = append(created, j.Name)
	}
	return created, nil
}
