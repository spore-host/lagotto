package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
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
// command has finished, so the global scope is idle from then, as read (unless
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
	if done, err = markScopes(ctx, c, region, what, errOut); err != nil {
		return done, err
	}
	if kind == creatorLocal || kind == creatorDeploy {
		// deploy settles the region's jobs once it has replaced the poller,
		// on every path (settleDeployJobs).
		return done, nil
	}
	if err := cancelTeardownJobs(ctx, newTeardownJobAPI(cfg, region), region, what, kind, errOut); err != nil {
		done()
		return func() {}, err
	}
	return done, nil
}

// markScopes marks region, then the global scope, each refreshed while it
// runs (keepMark) and ended by done (on a refusal too, so a creator that has
// returned leaves its region in use only as far as what it made is). A
// caller whose IAM predates the lease table is warned and carries on, for
// either scope.
func markScopes(ctx context.Context, c *lifecycle.Client, region, what string, errOut io.Writer) (done func(), err error) {
	done = func() {}
	endRegion := func() {}
	token, err := idle.Mark(ctx, c, region, what)
	switch {
	case errors.Is(err, idle.ErrLeaseUnavailable):
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
	case err != nil:
		// A mark that committed though its answer was lost is ended too
		// (one that did not commit is not there to end).
		endMark(c, region, token, io.Discard)
		return done, err
	default:
		stop := keepMark(c, region, token, errOut)
		endRegion = func() { stop(); endMark(c, region, token, errOut) }
	}
	// The global scope is marked even when the region's table was not
	// reachable: the account-wide teardown reads it on its own.
	endGlobal, err := markGlobalWith(ctx, c, what, errOut)
	done = func() { endRegion(); endGlobal() }
	if err != nil {
		done()
		return func() {}, err
	}
	return done, nil
}

// endMark ends a command's mark on scope (idle.EndMark), with a fresh
// context: the command's own may be done. A failure is a warning: the mark
// is in use for idle.CreatorMarkBound, then taken for a creator that died.
// No token (no mark was written) is nothing to end.
func endMark(c *lifecycle.Client, scope, token string, errOut io.Writer) {
	if token == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := idle.EndMark(ctx, c, scope, token); err != nil {
		_, _ = fmt.Fprintf(errOut, "warning: could not end this command's mark on lagotto's %s scope (%v); "+
			"the account-wide teardown takes it as in use for %s\n", scope, err, idle.CreatorMarkBound)
	}
}

// markGlobal marks setup's home region (cfg's region, else the control
// region) and then lagotto's global scope active (lagotto#345) before setup
// creates or changes anything account-wide, as every creator does. The
// regional mark is what stops an account-wide teardown already deleting: its
// re-check before each delete sees it and gives the global lease back, and
// the global mark, which waits for that lease (up to 10 min), then proceeds.
// Once marked, the teardown, which never claims the scope while a creator's
// mark is fresh, waits for done (or for idle.CreatorMarkBound) and then the
// global grace from then.
func markGlobal(ctx context.Context, cfg aws.Config, owner, what string, errOut io.Writer) (done func(), err error) {
	home := cfg.Region
	if home == "" {
		home, _ = idle.ControlRegion("")
	}
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return func() {}, fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	return markScopes(ctx, c, home, what, errOut)
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
		endMark(c, lifecycle.ScopeGlobal, token, io.Discard) // a mark whose answer was lost
		return noop, err
	}
	stop := keepMark(c, lifecycle.ScopeGlobal, token, errOut)
	return func() { stop(); endMark(c, lifecycle.ScopeGlobal, token, errOut) }, nil
}

// markRefresh is how often a running creator refreshes its marks
// (idle.RefreshMark): a third of idle.CreatorMarkBound, so a creator that
// runs for longer (a watch --local, a slow deploy) is never taken for one
// that died. A seam for tests.
var markRefresh = idle.CreatorMarkBound / 3

// keepMark refreshes a creator's mark on scope every markRefresh until stop
// is called. A mark that cannot be refreshed (gone, or a teardown holds the
// scope) is a warning, once.
func keepMark(c *lifecycle.Client, scope, token string, errOut io.Writer) (stop func()) {
	quit, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(markRefresh)
		defer t.Stop()
		warned := false
		for {
			select {
			case <-quit:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				ok, err := idle.RefreshMark(ctx, c, scope, token)
				cancel()
				if (!ok || err != nil) && !warned {
					warned = true
					_, _ = fmt.Fprintf(errOut, "warning: could not refresh this command's mark on lagotto's %s scope (%v); "+
						"after %s the account-wide teardown takes this command as one that died\n", scope, refreshWhy(ok, err), idle.CreatorMarkBound)
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(quit); <-finished }) }
}

func refreshWhy(ok bool, err error) string {
	if err != nil {
		return err.Error()
	}
	return "the mark is gone, or a teardown holds the scope"
}

// settleDeployJobs is deploy's last step once it has marked, on every path
// (lagotto#340): the region's pending teardown jobs and the poller's rate
// schedule never stand together. With the schedule there (the deploy got
// that far), the jobs go; without it (it failed before), they stay and
// finish the region's teardown, so a deploy that fails never leaves a
// function with no schedule and no job. After a deploy that succeeded
// (verify), a job that fired during it, and so removed the function, is an
// error: deploy again.
func settleDeployJobs(ctx context.Context, cfg aws.Config, region string, verify bool, errOut io.Writer) error {
	api := newTeardownJobAPI(cfg, region)
	has, err := pollerScheduleExists(ctx, api)
	switch {
	case err != nil && isAccessDeniedErr(err):
		_, _ = fmt.Fprintf(errOut, "warning: could not check the poller's schedule in %s (%v); pending lagotto-teardown jobs there are left\n", region, err)
		return nil
	case err != nil:
		return fmt.Errorf("could not check the poller's schedule in %s, to settle its pending teardown jobs: %w. Run 'lagotto deploy --region %s' again", region, err, region)
	case !has:
		return nil // the region's teardown, if any, finishes
	}
	if err := cancelTeardownJobs(ctx, api, region, "deploy the poller", creatorDeploy, errOut); err != nil {
		return err
	}
	if verify {
		c := cfg.Copy()
		c.Region = region
		if exists, err := teardownPollerExists(ctx, c); err == nil && !exists {
			return fmt.Errorf("a pending teardown job removed the poller in %s while it was being deployed; run 'lagotto deploy --region %s' again", region, region)
		}
	}
	return nil
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
