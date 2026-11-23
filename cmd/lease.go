package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// newLifecycleClient builds the spore-host-lifecycle client a command leases
// with (lagotto#326). A seam: tests answer it from lifecycletest.
var newLifecycleClient = func(cfg aws.Config, owner string) (*lifecycle.Client, error) {
	return idle.NewClient(cfg, owner)
}

// markActive is every creating command's first write (lagotto#326): deploy,
// watch (and watch --local) and launch mark lagotto active in region, and
// then in the global scope (lagotto#345), before they create anything, so a
// teardown holding either lease is waited out and none can start deleting
// until a grace period after this.
//
// A teardown that keeps the lease for the whole wait refuses the command
// (non-zero exit, nothing created). A caller whose IAM predates the lease
// table is warned on errOut and carries on, as before lagotto#326.
func markActive(ctx context.Context, cfg aws.Config, owner, region, what string, errOut io.Writer) error {
	return markCreating(ctx, cfg, owner, region, what, false, errOut)
}

// markCreating is markActive, and then the region's pending one-shot
// teardown jobs go (lagotto#340): a poller that committed its removal leaves
// its function's job due 92 min later, which would delete a poller deployed
// in the meantime, whatever that poller's mode. deploying (deploy, which
// recreates the poller's schedule) deletes them always; a watch or launch
// only while the poller's rate schedule is still there (without it, the
// region's teardown has finished, lagotto#349, and its job is what removes
// the old function).
func markCreating(ctx context.Context, cfg aws.Config, owner, region, what string, deploying bool, errOut io.Writer) error {
	if err := markActiveOnly(ctx, cfg, owner, region, what, errOut); err != nil {
		return err
	}
	return cancelTeardownJobs(ctx, newTeardownJobAPI(cfg, region), region, what, deploying, errOut)
}

func markActiveOnly(ctx context.Context, cfg aws.Config, owner, region, what string, errOut io.Writer) error {
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	err = idle.MarkActive(ctx, c, region, what)
	if errors.Is(err, idle.ErrLeaseUnavailable) {
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
		return nil
	}
	if err != nil {
		return err
	}
	return markGlobalWith(ctx, c, what, errOut)
}

// markGlobal marks lagotto's global scope active (lagotto#345) before a
// command creates or changes anything account-wide: setup's IAM, and
// deploy, watch and launch, which use it. The CLI's account-wide teardown
// (idle.TearDownAccountWide) holds the global lease while it deletes, so a
// creator waits it out; one that marks after its decision moves the global
// item's epoch, which stops the teardown before its next delete, and starts
// the global grace period over.
func markGlobal(ctx context.Context, cfg aws.Config, owner, what string, errOut io.Writer) error {
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	return markGlobalWith(ctx, c, what, errOut)
}

func markGlobalWith(ctx context.Context, c *lifecycle.Client, what string, errOut io.Writer) error {
	if _, err := c.RegionFor(lifecycle.ScopeGlobal); err != nil {
		return nil // no global scope outside the aws partition (lagotto#347): nothing removes the account-wide IAM there
	}
	err := idle.MarkActive(ctx, c, lifecycle.ScopeGlobal, what)
	if errors.Is(err, idle.ErrLeaseUnavailable) {
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
		return nil
	}
	return err
}

// teardownJobAPI is the Scheduler API a creator cancels a region's one-shot
// teardown jobs with; the SDK client satisfies it.
type teardownJobAPI interface {
	GetSchedule(ctx context.Context, in *scheduler.GetScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error)
	ListSchedules(ctx context.Context, in *scheduler.ListSchedulesInput, optFns ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error)
	DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, optFns ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error)
}

// newTeardownJobAPI builds the Scheduler client of region. A seam for tests.
var newTeardownJobAPI = func(cfg aws.Config, region string) teardownJobAPI {
	c := cfg.Copy()
	c.Region = region
	return scheduler.NewFromConfig(c)
}

// cancelTeardownJobs deletes region's pending one-shot teardown jobs (see
// markCreating). A caller whose IAM predates the grant is warned and carries
// on; any other failure refuses, with nothing created.
func cancelTeardownJobs(ctx context.Context, api teardownJobAPI, region, what string, deploying bool, errOut io.Writer) error {
	warn := func(err error) {
		_, _ = fmt.Fprintf(errOut, "warning: could not delete the pending lagotto-teardown jobs in %s (%v); a job left there deletes the poller when it runs. "+
			"An administrator's 'lagotto setup' updates the lagotto-cli-user policy to grant it.\n", region, err)
	}
	refuse := func(err error) error {
		return fmt.Errorf("refusing to %s: could not delete the pending lagotto-teardown jobs in %s: %w. Nothing was created", what, region, err)
	}
	if !deploying {
		has, err := pollerScheduleExists(ctx, api)
		switch {
		case err != nil && isAccessDeniedErr(err):
			warn(err)
			return nil
		case err != nil:
			return refuse(err)
		case !has:
			return nil
		}
	}
	var names []string
	var token *string
	for {
		out, err := api.ListSchedules(ctx, &scheduler.ListSchedulesInput{GroupName: aws.String(runtimeiam.TeardownScheduleGroup), NextToken: token})
		var nf *schedulertypes.ResourceNotFoundException
		switch {
		case errors.As(err, &nf):
			return nil // no teardown group: no job
		case err != nil && isAccessDeniedErr(err):
			warn(err)
			return nil
		case err != nil:
			return refuse(err)
		}
		for _, s := range out.Schedules {
			names = append(names, aws.ToString(s.Name))
		}
		if token = out.NextToken; token == nil {
			break
		}
	}
	for _, n := range names {
		_, err := api.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String(n), GroupName: aws.String(runtimeiam.TeardownScheduleGroup)})
		var nf *schedulertypes.ResourceNotFoundException
		switch {
		case err == nil:
			_, _ = fmt.Fprintf(errOut, "Deleted the pending teardown job %s in %s (lagotto is in use there again)\n", n, region)
		case errors.As(err, &nf):
		case isAccessDeniedErr(err):
			warn(err)
			return nil
		default:
			return refuse(err)
		}
	}
	return nil
}

// pollerScheduleExists reports whether the poller's rate schedule is in
// lagotto's group or (before lagotto#324) the default one.
func pollerScheduleExists(ctx context.Context, api teardownJobAPI) (bool, error) {
	for _, g := range []string{schedgroup.Name, "default"} {
		_, err := api.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(deploy.PollerScheduleName), GroupName: aws.String(g)})
		var nf *schedulertypes.ResourceNotFoundException
		switch {
		case err == nil:
			return true, nil
		case errors.As(err, &nf):
		default:
			return false, err
		}
	}
	return false, nil
}

// isAccessDeniedErr reports whether err is IAM refusing the call.
func isAccessDeniedErr(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	code := ae.ErrorCode()
	return strings.HasPrefix(code, "AccessDenied") || code == "UnauthorizedOperation"
}
