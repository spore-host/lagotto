// Package schedgroup is where lagotto's EventBridge Scheduler schedules live
// (lagotto#324).
//
// A schedule cannot be tagged, but a schedule group can, so lagotto keeps its
// schedules in a group of its own, [Name] ("lagotto"), which `lagotto deploy`
// creates tagged spore-host:managed-by=lagotto. Leave-no-trace cleanup
// (spore-host/spore-host#752) then finds them through the group.
//
// Before lagotto#324 every schedule lived in the "default" group ([Default]):
// the poller schedule lagotto-capacity-poller and the per-launch
// lagotto-launch-sl-* schedules. Both groups are therefore read, in the order
// [Groups]:
//
//   - The poller schedule is looked up with [Find]: lagotto's group first,
//     then default. `lagotto deploy` moves it into lagotto's group (create the
//     new one, then delete the old, so the poller is never left without one)
//     once the deployed IAM policies grant the new group.
//   - A new per-launch schedule is created with [Create], in lagotto's group,
//     falling back to default where the group does not exist yet (a poller
//     deployed by an older lagotto) or the caller's policy predates it.
//   - Listing and deletion cover both groups (pkg/deploy).
package schedgroup

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"

	"github.com/spore-host/lagotto/pkg/awsarn"
)

const (
	// Name is lagotto's own schedule group, created tagged by `lagotto deploy`.
	Name = "lagotto"
	// Default is EventBridge Scheduler's default group, where every schedule
	// lagotto created before lagotto#324 lives.
	Default = "default"
)

// Groups are the groups lagotto's schedules may be in, in lookup order.
var Groups = []string{Name, Default}

// ARN returns the ARN of lagotto's schedule group in region and account, in
// region's partition.
func ARN(region, accountID string) string {
	return awsarn.InRegion("scheduler", region, accountID, "schedule-group/"+Name)
}

// GetAPI is the slice of EventBridge Scheduler [Find] needs.
type GetAPI interface {
	GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
}

// CreateAPI is the slice of EventBridge Scheduler [Create] needs.
type CreateAPI interface {
	CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
}

// IsNotFound reports whether err is Scheduler's ResourceNotFoundException,
// which it returns both for a missing schedule and for a missing group.
func IsNotFound(err error) bool {
	var nf *schedulertypes.ResourceNotFoundException
	return errors.As(err, &nf)
}

// IsAccessDenied reports whether err is an authorization failure.
func IsAccessDenied(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.ErrorCode() {
	case "AccessDeniedException", "AccessDenied":
		return true
	}
	return false
}

// Find reads the schedule name from lagotto's group, and from the default
// group when it is not there. The result's GroupName is always set, so an
// UpdateSchedule built from it (pollcadence.UpdateInput) addresses the group
// the schedule is really in.
//
// When neither group has it the error is the ResourceNotFoundException, so
// callers keep treating that as "not deployed". A failure to read lagotto's
// group other than not-found (an AccessDenied from a policy written before
// lagotto#324, say) does not hide a schedule in the default group: default is
// read, and only if the schedule is not there either is the first error
// returned, since then it cannot be told whether the schedule exists.
func Find(ctx context.Context, api GetAPI, name string) (*scheduler.GetScheduleOutput, error) {
	var firstErr error
	for _, g := range Groups {
		out, err := api.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(name), GroupName: aws.String(g)})
		if err == nil {
			if out.GroupName == nil {
				out.GroupName = aws.String(g)
			}
			return out, nil
		}
		if !IsNotFound(err) && firstErr == nil {
			firstErr = err
		}
		if g == Default {
			if firstErr != nil {
				return nil, firstErr
			}
			return nil, err
		}
	}
	return nil, firstErr // unreachable: Groups ends with Default
}

// Create creates the schedule in in.GroupName, lagotto's group when that is
// empty, and returns the group it was created in.
//
// It falls back to the default group, where every lagotto before lagotto#324
// created its schedules, when lagotto's group is not usable: it does not exist
// (ResourceNotFoundException: the poller was deployed by an older lagotto) or
// the caller may not create schedules in it (AccessDenied: its IAM policy was
// written by an older `lagotto setup`). Any other error, ConflictException
// included, is returned as it is.
func Create(ctx context.Context, api CreateAPI, in *scheduler.CreateScheduleInput) (string, error) {
	want := aws.ToString(in.GroupName)
	if want == "" {
		want = Name
	}
	first := *in
	first.GroupName = aws.String(want)
	_, err := api.CreateSchedule(ctx, &first)
	if err == nil {
		return want, nil
	}
	if want == Default || (!IsNotFound(err) && !IsAccessDenied(err)) {
		return "", err
	}
	fallback := *in
	fallback.GroupName = aws.String(Default)
	if _, ferr := api.CreateSchedule(ctx, &fallback); ferr != nil {
		return "", fmt.Errorf("create schedule %s in group %s: %w (group %s refused it first: %v)",
			aws.ToString(in.Name), Default, ferr, want, err)
	}
	return Default, nil
}
