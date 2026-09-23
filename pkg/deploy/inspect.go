package deploy

// READ-ONLY snapshots of the deployed poller, for `lagotto doctor` (#156).
//
// Everything in this file is a pure observation: it issues Get* calls and
// nothing else. No resource is created, converged, tagged or deleted. That is a
// property worth keeping — doctor is the one command a user runs when they
// already suspect their account is wrong, and it must never be the thing that
// changes it.
//
// These sit in pkg/deploy rather than in the doctor package because the narrow
// lambdaAPI / snsAPI / schedulerAPI seams are unexported (deliberately — they're
// test seams, not public API), so the only place that can read through them is
// here.

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// VersionTag is the Lambda tag `lagotto deploy` stamps the poller with, holding
// the lagotto release the poller artifact came from.
//
// THIS IS THE ONLY RELIABLE VERSION SIGNAL the deployed poller has, which is why
// it's a tag rather than anything else:
//
//   - GetFunction's Code.Location is a presigned URL into an AWS-internal
//     Lambda bucket, NOT the s3://…/lagotto/capacity-poller-v<version>.zip key
//     the artifact was deployed from, so the version cannot be recovered from it.
//   - CodeSha256 identifies the artifact but can't be mapped to a version without
//     downloading every release.
//   - An Environment variable would work, but Environment.Variables participates
//     in pollerConfigDiffers, so it would churn the function CONFIGURATION on
//     every version bump. Tags don't.
//   - Tags are re-upserted by EnsurePollerFunction on every deploy (create and
//     update alike), so the value cannot go stale while the code moves.
//
// A poller deployed before this tag existed simply has no Version tag, and
// PollerInfo.Version comes back empty. Doctor reports that as "unknown" rather
// than guessing — a wrong version claim is worse than no claim.
const VersionTag = "Version"

// PollerInfo is a read-only snapshot of the deployed poller Lambda.
type PollerInfo struct {
	// Exists is false (with a nil error) when the function isn't deployed.
	Exists bool   `json:"exists"`
	ARN    string `json:"arn,omitempty"`
	// Version is the VersionTag value, or "" when the tag is absent.
	Version    string `json:"version,omitempty"`
	RoleARN    string `json:"role_arn,omitempty"`
	Runtime    string `json:"runtime,omitempty"`
	CodeSHA256 string `json:"code_sha256,omitempty"`
}

// ScheduleInfo is a read-only snapshot of the poller's EventBridge schedule.
type ScheduleInfo struct {
	// Exists is false (with a nil error) when the schedule isn't there.
	Exists bool `json:"exists"`
	// State is the raw ENABLED/DISABLED string. The schedule's state is owned out
	// of band (see EnsurePollerSchedule): `watch`/`extend` enable it, the poller
	// disables itself when idle — which is exactly why a mismatch against "are
	// there active watches" is worth reporting.
	State      string `json:"state,omitempty"`
	Expression string `json:"expression,omitempty"`
	TargetARN  string `json:"target_arn,omitempty"`
}

// Enabled reports whether the schedule is in the ENABLED state.
func (s ScheduleInfo) Enabled() bool {
	return s.State == string(schedulertypes.ScheduleStateEnabled)
}

// InspectPoller reads the deployed poller Lambda. An absent function is
// (PollerInfo{Exists: false}, nil); any other error is returned, so an
// AccessDenied is never reported as "not deployed".
func (d *Deployer) InspectPoller(ctx context.Context) (PollerInfo, error) {
	out, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	})
	if err != nil {
		var notFound *lambdatypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return PollerInfo{}, nil
		}
		return PollerInfo{}, fmt.Errorf("look up function %s: %w", PollerFunctionName, err)
	}
	info := PollerInfo{Exists: true, Version: out.Tags[VersionTag]}
	if cfg := out.Configuration; cfg != nil {
		info.ARN = aws.ToString(cfg.FunctionArn)
		info.RoleARN = aws.ToString(cfg.Role)
		info.Runtime = string(cfg.Runtime)
		info.CodeSHA256 = aws.ToString(cfg.CodeSha256)
	}
	return info, nil
}

// InspectPollerSchedule reads the poller's schedule. An absent schedule is
// (ScheduleInfo{Exists: false}, nil); any other error is returned.
func (d *Deployer) InspectPollerSchedule(ctx context.Context) (ScheduleInfo, error) {
	out, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name: aws.String(PollerScheduleName),
	})
	if err != nil {
		var notFound *schedulertypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return ScheduleInfo{}, nil
		}
		return ScheduleInfo{}, fmt.Errorf("look up schedule %s: %w", PollerScheduleName, err)
	}
	info := ScheduleInfo{
		Exists:     true,
		State:      string(out.State),
		Expression: aws.ToString(out.ScheduleExpression),
	}
	if out.Target != nil {
		info.TargetARN = aws.ToString(out.Target.Arn)
	}
	return info, nil
}

// AlertsTopicPresent reports whether the capacity-alerts SNS topic exists. The
// exported, read-only counterpart of the probe Teardown uses.
func (d *Deployer) AlertsTopicPresent(ctx context.Context, region, accountID string) (bool, error) {
	return d.alertsTopicExists(ctx, region, accountID)
}
