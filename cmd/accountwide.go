package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lastmile"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// The CLI is the single actor that removes lagotto's account-wide resources
// (lagotto#340): the poller role, the Scheduler invoke role,
// lagotto-instance-role and its profile, the profiles setup made for
// authorized roles, and lagotto-cli-user once nothing is attached to it. Each
// poller removes only its own region. Three paths run the one teardown,
// idle.TearDownAccountWide, under the global lease:
//
//   - the lazy last mile after a command (report-only in this release),
//     with the global grace period;
//   - `lagotto teardown --plan --mode reap`, for the plan's account-wide
//     steps, with no grace (lagotto#346);
//   - `lagotto teardown --iam-roles`, with no grace (lagotto#346).

// Seams. Tests replace them; production uses the values here.
var (
	// accountWideSource reads lagotto's footprint for the account-wide
	// teardown, and before each delete every region's poller and jobs.
	accountWideSource = func(cfg aws.Config, account string) inventory.Source {
		return inventory.NewAWSSource(cfg, account)
	}
	// lastMileAccountWideReap lets the last mile's account-wide teardown
	// delete under SPORE_LASTMILE=reap. False in this release: the last mile
	// is report-only (libs#79 and lagotto#328 decide when it may reap).
	lastMileAccountWideReap = false
	// teardownAccountWide is `teardown --iam-roles`'s account-wide teardown.
	teardownAccountWide = runAccountWide
)

// The lease owners the CLI's account-wide teardown writes.
const (
	lastMileOwner = "lagotto last mile"
	teardownOwner = "lagotto teardown"
)

// runAccountWide is an explicit `lagotto teardown`'s account-wide teardown:
// reap mode, no grace, limited to only when set. Its rows go to logf.
func runAccountWide(ctx context.Context, cfg aws.Config, account string, only map[string]bool, logf func(string, ...any)) idle.AccountWideResult {
	c, err := newLifecycleClient(cfg, teardownOwner)
	if err != nil {
		return idle.AccountWideResult{Outcome: idle.AccountWideFailed, Why: "no lifecycle client", Err: err}
	}
	return idle.TearDownAccountWide(ctx, idle.AccountWideConfig{
		AccountID: account, Owner: teardownOwner, Reap: true, Only: only,
		Options: footprintOptions(), Now: footprintNow, Logf: logf,
	}, idle.AccountWideDeps{Lifecycle: c, Source: accountWideSource(cfg, account), Deleters: newTeardownDeleters(cfg, account)})
}

// lastMileAccountWide is the last mile's account-wide teardown, after
// lastmile.Run, in whatever time the budget has left (ctx's deadline). In
// report mode it reads through the last mile's read-only lifecycle client
// and the footprint lastmile.Run read (live), and writes nothing.
func lastMileAccountWide(ctx context.Context, mode lastmile.Mode, cfg aws.Config, account string, regions []string,
	lc *lifecycle.Client, live *lastMileFootprint, logf func(string, ...any)) idle.AccountWideResult {
	acfg := idle.AccountWideConfig{AccountID: account, Grace: idle.DefaultGrace, Options: footprintOptions(), Now: lastMileNow,
		Logf: func(f string, a ...any) { logf("lastmile: "+f, a...) }}
	deps := idle.AccountWideDeps{Lifecycle: lc, Regions: regions, Gather: live.factsOf}
	if mode == lastmile.ModeReap && lastMileAccountWideReap {
		c, err := newLifecycleClient(cfg, lastMileOwner)
		if err != nil {
			logf("lastmile: account-wide skipped: %v", err)
			return idle.AccountWideResult{Outcome: idle.AccountWideFailed, Why: "no lifecycle client", Err: err}
		}
		acfg.Reap, acfg.Owner = true, lastMileOwner
		deps = idle.AccountWideDeps{Lifecycle: c, Regions: regions, Gather: live.factsOf,
			Source: accountWideSource(cfg, account), Deleters: newTeardownDeleters(cfg, account)}
	}
	return idle.TearDownAccountWide(ctx, acfg, deps)
}

// accountWideError is the error of an explicit teardown's account-wide run
// that did not remove what it was asked to, or nil.
func accountWideError(r idle.AccountWideResult) error {
	switch r.Outcome {
	case idle.AccountWideRemoved, idle.AccountWideNothing:
		return nil
	}
	return fmt.Errorf("the account-wide resources were not removed (%s): %s", r.Outcome, accountWideWhy(r))
}

// accountWideWhy says why a run decided what it did.
func accountWideWhy(r idle.AccountWideResult) string {
	why := r.Why
	if len(r.Blockers) > 0 {
		why += ": " + strings.Join(r.Blockers, "; ")
	}
	if r.Err != nil {
		why += ": " + r.Err.Error()
	}
	return why
}

// resourceKind is how a teardown line names a resource type.
func resourceKind(typ string) string {
	switch typ {
	case inventory.TypeRole:
		return "IAM role"
	case inventory.TypeInstanceProfile:
		return "instance profile"
	case inventory.TypePolicy:
		return "IAM policy"
	}
	return strings.TrimPrefix(typ, "AWS::")
}

// writeAccountWide prints what an explicit teardown's account-wide run did,
// one line per resource.
func writeAccountWide(out io.Writer, r idle.AccountWideResult) {
	if r.Result == nil {
		return
	}
	for _, a := range r.Result.Actions {
		switch a.Outcome {
		case fp.OutcomeDeleted:
			_, _ = fmt.Fprintf(out, "Deleted %s %s\n", resourceKind(a.Type), a.Name)
		case fp.OutcomeGone:
			_, _ = fmt.Fprintf(out, "Already absent: %s %s\n", resourceKind(a.Type), a.Name)
		default:
			line := fmt.Sprintf("Not deleted: %s %s (%s", resourceKind(a.Type), a.Name, a.Outcome)
			if a.Reason != "" {
				line += ": " + a.Reason
			}
			if a.Err != nil {
				line += ": " + a.Err.Error()
			}
			_, _ = fmt.Fprintln(out, line+")")
		}
	}
}
