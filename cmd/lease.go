package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

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

// creatorKind is what a creating command does to a region's poller, which
// decides what happens to the region's pending one-shot teardown jobs.
type creatorKind int

const (
	// creatorDeploy (deploy) recreates the poller and its schedule: the
	// region's teardown jobs always go first.
	creatorDeploy creatorKind = iota
	// creatorHosted (watch, launch) needs the hosted poller: refused once
	// its rate schedule is gone (the region was torn down; its function's
	// job is pending, lagotto#349), and otherwise the jobs go.
	creatorHosted
	// creatorLocal (watch --local) needs no hosted poller: the jobs stay.
	creatorLocal
)

// markCreating is every creating command's first write (lagotto#326): deploy,
// watch (and watch --local) and launch mark lagotto active in region, and
// then in the global scope (lagotto#345), before they create anything, and
// deal with the region's pending teardown jobs (lagotto#340, creatorKind).
// A teardown holding either lease is waited out (the CLI up to 10 min): it
// refuses the command if it keeps the lease that long (non-zero exit,
// nothing created). A caller whose IAM predates the lease table is warned on
// errOut and carries on, as before lagotto#326, for either scope.
//
// done ends the command's global mark (lifecycle MarkDone): call it when the
// command has finished, so the global scope goes idle from then (unless
// another creator has marked since). A creator that dies leaves its mark,
// which the account-wide teardown takes as in use for
// idle.CreatorMarkBound.
func markCreating(ctx context.Context, cfg aws.Config, owner, region, what string, kind creatorKind, errOut io.Writer) (done func(), err error) {
	done = func() {}
	if owner == "" {
		owner = "lagotto-cli"
	}
	if kind == creatorHosted {
		// Before any mark: a region whose poller was torn down is refused
		// with nothing written. (cancelTeardownJobs checks again after the
		// marks.)
		if has, err := pollerScheduleExists(ctx, newTeardownJobAPI(cfg, region)); err == nil && !has {
			return done, fmt.Errorf("refusing to %s: lagotto's poller in %s has been torn down (its rate schedule is gone, and its function "+
				"is due for deletion). Nothing was created. Deploy it again first: lagotto deploy --region %s", what, region, region)
		}
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return done, fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	err = idle.MarkActive(ctx, c, region, what)
	switch {
	case errors.Is(err, idle.ErrLeaseUnavailable):
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
	case err != nil:
		return done, err
	}
	// The global scope is marked even when the region's table was not
	// reachable: the account-wide teardown reads it on its own.
	if done, err = markGlobalWith(ctx, c, what, errOut); err != nil {
		return done, err
	}
	if kind == creatorLocal {
		return done, nil
	}
	if err := cancelTeardownJobs(ctx, newTeardownJobAPI(cfg, region), region, what, kind, errOut); err != nil {
		done()
		return func() {}, err
	}
	return done, nil
}

// markGlobal marks lagotto's global scope active (lagotto#345) before setup
// creates or changes anything account-wide. While the account-wide teardown
// (idle.TearDownAccountWide) holds the global lease, the mark waits for it
// (up to 10 min); once marked, the teardown, which never claims the scope
// while a creator's mark is fresh, waits for done (or for
// idle.CreatorMarkBound) and then the global grace from then.
func markGlobal(ctx context.Context, cfg aws.Config, owner, what string, errOut io.Writer) (done func(), err error) {
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return func() {}, fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	return markGlobalWith(ctx, c, what, errOut)
}

func markGlobalWith(ctx context.Context, c *lifecycle.Client, what string, errOut io.Writer) (func(), error) {
	noop := func() {}
	if _, err := c.RegionFor(lifecycle.ScopeGlobal); err != nil {
		return noop, nil // no global scope outside the aws partition (lagotto#347): nothing removes the account-wide IAM there
	}
	token, err := idle.Mark(ctx, c, lifecycle.ScopeGlobal, what)
	if errors.Is(err, idle.ErrLeaseUnavailable) {
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
		return noop, nil
	}
	if err != nil {
		return noop, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := c.MarkDone(ctx, idle.Tool, lifecycle.ScopeGlobal, token); err != nil {
			_, _ = fmt.Fprintf(errOut, "warning: could not end this command's mark on lagotto's global scope (%v); "+
				"the account-wide teardown takes it as in use for %s\n", err, idle.CreatorMarkBound)
		}
	}, nil
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
func cancelTeardownJobs(ctx context.Context, api teardownJobAPI, region, what string, kind creatorKind, errOut io.Writer) error {
	warn := func(err error) {
		_, _ = fmt.Fprintf(errOut, "warning: could not delete the pending lagotto-teardown jobs in %s (%v); a job left there deletes the poller when it runs. "+
			"An administrator's 'lagotto setup' updates the lagotto-cli-user policy to grant it.\n", region, err)
	}
	refuse := func(err error) error {
		return fmt.Errorf("refusing to %s: could not delete the pending lagotto-teardown jobs in %s: %w. Nothing was created", what, region, err)
	}
	if kind == creatorHosted {
		has, err := pollerScheduleExists(ctx, api)
		switch {
		case err != nil && isAccessDeniedErr(err):
			warn(err)
			return nil
		case err != nil:
			return refuse(err)
		case !has:
			// The region was torn down (or is being): its poller's function
			// is due for deletion, and nothing would poll or fire this.
			return fmt.Errorf("refusing to %s: lagotto's poller in %s has been torn down (its rate schedule is gone, and its function "+
				"is due for deletion). Nothing was created. Deploy it again first: lagotto deploy --region %s", what, region, region)
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
