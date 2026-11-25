package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The plan-driven teardown (lagotto#325): `lagotto teardown --plan <file|->`
// executes a `lagotto footprint -o json` report through the shared executor
// (libs/footprint.Execute). It is a second path beside the fixed one
// (runTeardown), which it does not change; the two share flags only where
// they mean the same thing, and none of --force and --iam-roles applies here.
var (
	teardownPlan    string
	teardownMode    string
	teardownYes     bool
	teardownNoLease bool
)

// noLeaseFlag is the hidden flag that lets reap mode go ahead where the
// teardown lease (lagotto#326) cannot be taken: it falls back, region by
// region, to an in-memory stand-in that fences nothing.
const noLeaseFlag = "i-understand-no-lease"

func init() {
	teardownCmd.Flags().StringVar(&teardownPlan, "plan", "",
		"Execute a footprint plan: the JSON 'lagotto footprint -o json' writes, from a file or - for stdin")
	teardownCmd.Flags().StringVar(&teardownMode, "mode", string(fp.ModeReport),
		"With --plan: report (say what would be deleted; the default) or reap (delete; needs --yes)")
	teardownCmd.Flags().BoolVar(&teardownYes, "yes", false, "With --plan --mode reap: confirm the deletion")
	teardownCmd.Flags().BoolVar(&teardownNoLease, noLeaseFlag, false,
		"With --plan --mode reap: where the teardown lease (lagotto#326) cannot be taken, delete without it, so nothing stops a concurrent deploy or watch there")
	if err := teardownCmd.Flags().MarkHidden(noLeaseFlag); err != nil {
		panic(err)
	}
}

// checkTeardownFlags rejects flags of one teardown path given to the other.
func checkTeardownFlags(cmd *cobra.Command) error {
	changed := func(names ...string) []string {
		var out []string
		for _, n := range names {
			if cmd.Flags().Changed(n) {
				out = append(out, "--"+n)
			}
		}
		return out
	}
	if teardownPlan == "" {
		if f := changed("mode", "yes", noLeaseFlag); len(f) > 0 {
			return fmt.Errorf("%s apply only with --plan", strings.Join(f, ", "))
		}
		return nil
	}
	if f := changed("force", "iam-roles"); len(f) > 0 {
		return fmt.Errorf("%s do not apply with --plan: the plan says what is deleted, and its classes decide what may be", strings.Join(f, ", "))
	}
	return nil
}

func runTeardownPlan(cmd *cobra.Command) error {
	mode := fp.Mode(teardownMode)
	switch {
	case mode != fp.ModeReport && mode != fp.ModeReap:
		return fmt.Errorf("--mode %q: use report or reap", teardownMode)
	case mode == fp.ModeReap && !teardownYes:
		return fmt.Errorf("--mode reap deletes what the plan lists; pass --yes to confirm (run --mode report first to see what)")
	}
	planned, err := readPlan(cmd.InOrStdin(), teardownPlan)
	if err != nil {
		return err
	}
	ctx, stop := commandContext(cmd)
	defer stop()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	acct, err := setupCallerAccount(ctx, cfg)
	if err != nil {
		return fmt.Errorf("not deleting anything: resolve the AWS account ID: %w", err)
	}
	return executePlan(ctx, cmd.OutOrStdout(), cfg, acct, planned, mode)
}

// readPlan decodes the plan from path, or stdin for "-".
func readPlan(stdin io.Reader, path string) (*fp.Report, error) {
	r := stdin
	if path != "-" {
		f, err := os.Open(path) // #nosec G304 -- the user names the plan file to read
		if err != nil {
			return nil, fmt.Errorf("read the plan: %w", err)
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	rep, err := fp.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("read the plan %s: %w", path, err)
	}
	return rep, nil
}

// planScope is the footprint a plan is re-checked against: the regions it
// names, and every region when it names an account-wide resource (which only
// a footprint of every region can classify).
func planScope(planned *fp.Report) inventory.Scope {
	var s inventory.Scope
	seen := map[string]bool{}
	for _, r := range planned.Resources {
		if r.Region == inventory.Global {
			s.AllRegions = true
			continue
		}
		if r.Region != "" && !seen[r.Region] {
			seen[r.Region] = true
			s.Regions = append(s.Regions, r.Region)
		}
	}
	sort.Strings(s.Regions)
	if s.AllRegions {
		s.Regions = nil
	}
	return s
}

// executePlan re-takes the footprint, keeps what is in use, and runs the rest
// of the plan through footprint.Execute.
func executePlan(ctx context.Context, out io.Writer, cfg aws.Config, acct string, planned *fp.Report, mode fp.Mode) error {
	if len(planned.Resources) == 0 {
		_, _ = fmt.Fprintln(out, "The plan lists nothing; nothing to do.")
		return nil
	}
	if planned.Account != "" && planned.Account != acct {
		return fmt.Errorf("not deleting anything: the plan is for account %s, but these credentials are for %s", planned.Account, acct)
	}
	live, err := takeFootprint(ctx, cfg, acct, planScope(planned))
	if err != nil {
		return fmt.Errorf("not deleting anything: re-check the plan against the account: %w", err)
	}
	plan, kept, err := inventory.PlanTeardown(planned, live, footprintOptions())
	if err != nil {
		return fmt.Errorf("not deleting anything: %w", err)
	}
	deleter := inventory.NewDeleter(live, newTeardownDeleters(cfg, acct))
	var res *fp.Result
	var execErr error
	if mode == fp.ModeReap {
		res, execErr = reapWithLeases(ctx, cfg, acct, plan, live, deleter, os.Stderr)
	} else {
		res, execErr = fp.Execute(ctx, plan, mode, deleter, nil)
	}
	if res == nil {
		return execErr
	}
	if err := writeTeardownResult(out, plan, kept, res, live.Warnings, getOutputFormat()); err != nil {
		return err
	}
	if execErr != nil {
		return fmt.Errorf("teardown --plan: %d step(s) failed: %w", res.Count(fp.OutcomeFailed)+res.Count(fp.OutcomeStopped), execErr)
	}
	return nil
}

// unfencedLease is the stand-in for the teardown lease (lagotto#326) that
// --i-understand-no-lease falls back to where the real one cannot be taken.
// footprint.Execute takes a lease in reap mode and heartbeats it before every
// delete, so that a teardown that lost the account to someone else's deploy
// or watch stops. This one is held on an in-memory table no other lagotto can
// see: its heartbeats always succeed and fence nothing, which is exactly what
// the flag acknowledges.
func unfencedLease(ctx context.Context) (*lifecycle.Lease, error) {
	table := lifecycletest.New()
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(string) (lifecycle.API, error) { return table, nil },
		ControlRegion: "local",
		Owner:         "lagotto teardown --plan (no lease)",
	})
	if err != nil {
		return nil, err
	}
	if _, err := c.SetIdleIfUnchanged(ctx, inventory.Tool, lifecycle.ScopeGlobal, lifecycle.Expect{}); err != nil {
		return nil, fmt.Errorf("take the local stand-in lease: %w", err)
	}
	it, ok, err := c.Get(ctx, inventory.Tool, lifecycle.ScopeGlobal)
	if err != nil {
		return nil, fmt.Errorf("take the local stand-in lease: %w", err)
	}
	lease, err := c.ClaimIfUnchanged(ctx, inventory.Tool, lifecycle.ScopeGlobal, 0, lifecycle.ExpectOf(it, ok))
	if err != nil {
		return nil, fmt.Errorf("take the local stand-in lease: %w", err)
	}
	return lease, nil
}

// regionLease is one region's teardown lease, real or the stand-in.
type regionLease struct {
	region string
	lease  *lifecycle.Lease
	real   bool
}

// reapWithLeases runs a reap plan under the real teardown lease (lagotto#326)
// of every region it deletes in: each region's steps with that region's
// lease, so a deploy, watch or launch there either waits for the teardown or
// stops it at its next step. The account-wide (IAM) steps are not run under
// those: once the regions are done, the CLI's account-wide teardown
// (idle.TearDownAccountWide, lagotto#340 and #346) deletes them under the
// global lease, the one actor that removes account-wide resources, only
// while no region has a poller, a pending one-shot job or a live lifecycle
// item, each re-checked before each delete.
//
// A region whose lease cannot be taken (something there is in use, or another
// teardown holds it) refuses the whole run before anything is deleted, unless
// --i-understand-no-lease, which falls back to the stand-in there (and, where
// there is no global lease, outside the aws partition, for the account-wide
// steps). Once a region's steps are done its lease is finished, and its
// lifecycle table closed if no tool is left in it, when nothing of lagotto's
// is left there; otherwise it is released, and the grace period starts over.
func reapWithLeases(ctx context.Context, cfg aws.Config, acct string, plan *fp.Plan, live *fp.Report, deleter fp.Deleter, errOut io.Writer) (*fp.Result, error) {
	byRegion := map[string][]fp.Resource{}
	var regions []string
	var global []fp.Resource
	for _, st := range plan.Steps {
		r := st.Resource
		if r.Region == inventory.Global || r.Region == "" {
			global = append(global, r)
			continue
		}
		if _, ok := byRegion[r.Region]; !ok {
			regions = append(regions, r.Region)
		}
		byRegion[r.Region] = append(byRegion[r.Region], r)
	}
	sort.Strings(regions)
	active := map[string]bool{}
	for _, r := range live.Resources {
		if r.Class == fp.ClassActive {
			active[r.Region] = true
		}
	}

	c, err := newLifecycleClient(cfg, runOwner("lagotto teardown --plan"))
	if err != nil {
		return nil, fmt.Errorf("not deleting anything: %w", err)
	}
	var leases []regionLease
	giveBack := func() {
		for _, l := range leases {
			if l.real {
				rctx, cancel := freshCtx(ctx)
				_ = l.lease.Release(rctx)
				cancel()
			}
		}
	}
	for _, region := range regions {
		l, err := claimRegion(ctx, c, region, active[region])
		if err != nil {
			if !teardownNoLease {
				giveBack()
				return nil, fmt.Errorf("not deleting anything: could not take the teardown lease in %s: %w. "+
					"Run --mode report to see what is in use, and try again once nothing is", region, err)
			}
			_, _ = fmt.Fprintf(errOut, "warning: no teardown lease in %s (%v); --%s: deleting there without one, so nothing stops a concurrent deploy or watch\n",
				region, err, noLeaseFlag)
			if l, err = unfencedLease(ctx); err != nil {
				giveBack()
				return nil, fmt.Errorf("not deleting anything: %w", err)
			}
			leases = append(leases, regionLease{region: region, lease: l})
			continue
		}
		leases = append(leases, regionLease{region: region, lease: l, real: true})
	}

	byARN := map[string]fp.Action{}
	var errs []error
	run := func(rs []fp.Resource, d fp.Deleter, lease *lifecycle.Lease) {
		sub, err := fp.NewPlan(plan.Tool, plan.Patterns, rs)
		if err != nil {
			errs = append(errs, err)
			return
		}
		res, err := fp.Execute(ctx, sub, fp.ModeReap, d, lease)
		if err != nil {
			errs = append(errs, err)
		}
		if res != nil {
			for _, a := range res.Actions {
				byARN[a.ARN] = a
			}
		}
	}
	for i, region := range regions {
		run(byRegion[region], deleter, leases[i].lease)
	}

	// Each region's lease: finished when nothing of lagotto's is left there.
	for _, l := range leases {
		if !l.real {
			continue
		}
		if regionCleared(live, l.region, byARN) {
			if err := l.lease.Finish(ctx); err != nil {
				errs = append(errs, err)
				continue
			}
			if _, err := c.CloseTableIfEmpty(ctx, l.region); err != nil {
				_, _ = fmt.Fprintf(errOut, "warning: could not remove the empty %s table in %s: %v\n", lifecycle.TableName, l.region, err)
			}
			continue
		}
		rctx, cancel := freshCtx(ctx)
		err := l.lease.Release(rctx)
		cancel()
		if err != nil && !errors.Is(err, lifecycle.ErrLeaseLost) {
			errs = append(errs, err)
		}
	}

	// The account-wide steps, by the one account-wide teardown.
	if len(global) > 0 {
		errs = append(errs, reapAccountWide(ctx, cfg, acct, global, byARN, errOut))
	}

	res := &fp.Result{Mode: fp.ModeReap, Tool: string(plan.Tool), Actions: make([]fp.Action, len(plan.Steps))}
	for i, st := range plan.Steps {
		a, ok := byARN[st.Resource.ARN]
		if !ok {
			a = fp.Action{ARN: st.Resource.ARN, Type: st.Resource.Type, Name: st.Resource.Name, Outcome: fp.OutcomeNotRun, Reason: "not run"}
		}
		res.Actions[i] = a
	}
	return res, errors.Join(errs...)
}

// reapAccountWide runs the plan's account-wide steps through the CLI's
// account-wide teardown, limited to them, and records each step's action in
// byARN. Outside the aws partition, where there is no global lease, it runs
// them under the stand-in only with --i-understand-no-lease.
func reapAccountWide(ctx context.Context, cfg aws.Config, acct string, global []fp.Resource, byARN map[string]fp.Action, errOut io.Writer) error {
	only := map[string]bool{}
	for _, r := range global {
		only[r.ARN] = true
	}
	logf := func(f string, a ...any) {
		if verbose {
			_, _ = fmt.Fprintf(errOut, "debug: "+f+"\n", a...)
		}
	}
	r := teardownAccountWide(ctx, cfg, acct, only, logf)
	if r.Outcome == idle.AccountWideSkipped && teardownNoLease {
		// The same checks as `teardown --iam-roles` there: no poller and no
		// pending lagotto-teardown job in any region, read in full.
		_, _ = fmt.Fprintf(errOut, "warning: no global teardown lease (%s); --%s: deleting the account-wide resources without one, "+
			"once no poller and no pending teardown job is left in any region\n", r.Why, noLeaseFlag)
		r = teardownAccountWideUnfenced(ctx, cfg, acct, only, logf)
	}
	if r.Result != nil {
		for _, a := range r.Result.Actions {
			byARN[a.ARN] = a
		}
	}
	err := accountWideError(r)
	outcome, reason := fp.OutcomeNotRun, accountWideWhy(r)
	finished := false
	switch r.Outcome {
	case idle.AccountWideBlocked:
		if !r.ReadFailed {
			// In use, as far as the account-wide teardown can tell: kept,
			// like any active resource of the plan, and not a failure.
			outcome, err = fp.OutcomeKept, nil
		}
		// A read that failed is not "in use": not run, and an error.
	case idle.AccountWideRemoved, idle.AccountWideNothing:
		finished = true
	}
	kept := map[string]string{}
	for _, k := range r.Kept {
		kept[k.Resource.ARN] = k.Reason
	}
	for _, g := range global {
		if _, ok := byARN[g.ARN]; ok {
			continue
		}
		a := fp.Action{ARN: g.ARN, Type: g.Type, Name: g.Name, Outcome: outcome, Reason: reason}
		if finished {
			// The run read the account afresh: what it kept, it says why;
			// what its footprint no longer had is gone.
			if why, ok := kept[g.ARN]; ok {
				a.Outcome, a.Reason = fp.OutcomeKept, "the account-wide teardown kept it: "+why
			} else {
				a.Outcome, a.Reason = fp.OutcomeGone, "no longer there when the account-wide teardown read the account"
			}
		}
		byARN[g.ARN] = a
	}
	return err
}

// claimRegion takes the teardown lease in region, with no grace period (the
// human path's decision being the plan itself), as judged: refused while
// the footprint says something there is in use, or the region's item is
// (idle.ClaimJudged: a creator's live mark, a hold, a live lease), and
// claimed only if the item is still exactly what was read, so a creator's
// mark is never erased.
func claimRegion(ctx context.Context, c *lifecycle.Client, region string, active bool) (*lifecycle.Lease, error) {
	if active {
		return nil, fmt.Errorf("something of lagotto's in %s is in use", region)
	}
	return idle.ClaimJudged(ctx, c, region, true)
}

// regionCleared reports whether every resource of lagotto's in region, per
// live, was removed.
func regionCleared(live *fp.Report, region string, actions map[string]fp.Action) bool {
	for _, r := range live.Resources {
		if r.Region != region {
			continue
		}
		a, ok := actions[r.ARN]
		if !ok || (a.Outcome != fp.OutcomeDeleted && a.Outcome != fp.OutcomeGone) {
			return false
		}
	}
	return true
}

// regionDeleter is one region's deleters: deploy --teardown's (the Deployer)
// and teardown's tables (the Store).
type regionDeleter struct {
	*deploy.Deployer
	*watcher.Store
}

// iamAccountDeleter is the account-wide deleters: `teardown --iam-roles`'s.
type iamAccountDeleter struct {
	client    *iam.Client
	accountID string
}

func (d iamAccountDeleter) DeleteRole(ctx context.Context, name string) (bool, error) {
	return runtimeiam.DeleteRole(ctx, d.client, name)
}

func (d iamAccountDeleter) DeleteInstanceProfile(ctx context.Context, name string) (bool, error) {
	return runtimeiam.DeleteInstanceProfile(ctx, d.client, name)
}

func (d iamAccountDeleter) DeletePolicy(ctx context.Context) (cliiam.Deletion, error) {
	return cliiam.DeletePolicy(ctx, d.client, d.accountID)
}

// newTeardownDeleters builds the deleters a plan is executed with. A seam for
// tests.
var newTeardownDeleters = func(cfg aws.Config, accountID string) inventory.Deleters {
	var mu sync.Mutex
	regions := map[string]inventory.RegionDeleter{}
	return inventory.Deleters{
		AccountID: accountID,
		Region: func(region string) inventory.RegionDeleter {
			mu.Lock()
			defer mu.Unlock()
			if d, ok := regions[region]; ok {
				return d
			}
			c := cfg.Copy()
			c.Region = region
			d := regionDeleter{Deployer: deploy.New(c), Store: watcher.NewStore(c, watchesTable, historyTable)}
			regions[region] = d
			return d
		},
		Account: iamAccountDeleter{client: iam.NewFromConfig(cfg), accountID: accountID},
	}
}

// teardownStep is one line of the plan's outcome.
type teardownStep struct {
	ARN     string `json:"arn"`
	Type    string `json:"type"`
	Region  string `json:"region"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
	Error   string `json:"error,omitempty"`
}

// teardownReport is `teardown --plan -o json`.
type teardownReport struct {
	Mode     fp.Mode        `json:"mode"`
	Kept     []teardownStep `json:"kept"`
	Steps    []teardownStep `json:"steps"`
	Warnings []string       `json:"warnings"`
}

func writeTeardownResult(out io.Writer, plan *fp.Plan, kept []inventory.Kept, res *fp.Result, warnings []string, format string) error {
	rep := teardownReport{Mode: res.Mode, Kept: []teardownStep{}, Steps: []teardownStep{}, Warnings: append([]string{}, warnings...)}
	for _, k := range kept {
		r := k.Resource
		rep.Kept = append(rep.Kept, teardownStep{ARN: r.ARN, Type: r.Type, Region: r.Region, Name: r.Name, Outcome: "kept", Reason: k.Reason})
	}
	for i, a := range res.Actions {
		st := teardownStep{ARN: a.ARN, Type: a.Type, Name: a.Name, Outcome: string(a.Outcome), Reason: a.Reason}
		if i < len(plan.Steps) {
			st.Region = plan.Steps[i].Resource.Region
		}
		if a.Err != nil {
			st.Error = a.Err.Error()
			if errors.Is(a.Err, inventory.ErrInUse) {
				st.Reason = "in use since the plan was made"
			}
		}
		rep.Steps = append(rep.Steps, st)
	}
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "OUTCOME\tREGION\tTYPE\tNAME\tREASON")
	line := func(s teardownStep) {
		reason := s.Reason
		if s.Error != "" {
			reason = strings.TrimSpace(reason + ": " + s.Error)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", s.Outcome, s.Region, strings.TrimPrefix(s.Type, "AWS::"), s.Name, reason)
	}
	for _, s := range rep.Kept {
		line(s)
	}
	for _, s := range rep.Steps {
		line(s)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	verb := "would delete"
	n := res.Count(fp.OutcomeWouldDelete)
	if res.Mode == fp.ModeReap {
		verb, n = "deleted", res.Count(fp.OutcomeDeleted)
	}
	_, _ = fmt.Fprintf(out, "\n%s: %s %d, kept %d in use, %d not deletable, %d failed.\n", res.Mode, verb, n,
		len(rep.Kept), res.Count(fp.OutcomeKept)+res.Count(fp.OutcomeRefused)+res.Count(fp.OutcomeSkipped),
		res.Count(fp.OutcomeFailed)+res.Count(fp.OutcomeStopped))
	for _, wn := range rep.Warnings {
		_, _ = fmt.Fprintf(out, "warning: %s\n", wn)
	}
	return nil
}
