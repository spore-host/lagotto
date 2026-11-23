package idle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

// DefaultGrace is how long a region must be idle before the poller tears
// lagotto down there (spore-host/spore-host#752: 24 h).
const DefaultGrace = 24 * time.Hour

// GraceEnv overrides [DefaultGrace] in the poller: a Go duration ("30m").
// For live checks of the report; lagotto deploy does not set it.
const GraceEnv = deploy.IdleGraceEnv

// GraceFrom reads the grace period from getenv. An unset value is
// [DefaultGrace]; an invalid or non-positive one is too, with a warning.
func GraceFrom(getenv func(string) string) (time.Duration, string) {
	v := strings.TrimSpace(getenv(GraceEnv))
	if v == "" {
		return DefaultGrace, ""
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return DefaultGrace, fmt.Sprintf("ignoring %s=%q (want a positive duration such as 30m): using %s", GraceEnv, v, DefaultGrace)
	}
	return d, ""
}

// Verdict is the poller's per-run judgment of one region.
type Verdict struct {
	Region string
	Active bool
	// Reasons say why the region is active.
	Reasons []string
	// Warnings are the reads that failed; any one makes the region active.
	Warnings []string
}

// ReadVerdict judges region from its activity signals alone
// (inventory.GatherActivity), the same rule the footprint classifies by. A
// read that fails makes the region active.
func ReadVerdict(ctx context.Context, rs inventory.RegionSource, region string, o inventory.Options) Verdict {
	r, warnings := inventory.GatherActivity(ctx, rs, region, o)
	return Verdict{Region: region, Active: r.Active(), Reasons: r.ActiveReasons(), Warnings: warnings}
}

// Config is one region's idle teardown.
type Config struct {
	Region, AccountID string
	// Grace is how long the region must be idle. Default DefaultGrace.
	Grace time.Duration
	// Reap deletes. False is report mode, the only mode this release's
	// poller runs (lagotto#328 turns reap on).
	Reap    bool
	Options inventory.Options
	// Now is the clock. Default time.Now.
	Now func() time.Time
	// Logf writes one log line. Default stderr.
	Logf func(format string, args ...any)
	// GlobalWait is how long the global step waits for another region to
	// give the global teardown lease back. Default DefaultGlobalWait.
	GlobalWait time.Duration
	// Sleep waits for d or until ctx is done. Default a timer.
	Sleep func(ctx context.Context, d time.Duration) error
}

func (c Config) withDefaults() Config {
	if c.Grace <= 0 {
		c.Grace = DefaultGrace
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logf == nil {
		c.Logf = func(format string, args ...any) { _, _ = fmt.Fprintf(os.Stderr, format+"\n", args...) }
	}
	if c.GlobalWait <= 0 {
		c.GlobalWait = DefaultGlobalWait
	}
	if c.Sleep == nil {
		c.Sleep = sleep
	}
	return c
}

func sleep(ctx context.Context, d time.Duration) error {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tm.C:
		return nil
	}
}

func (c Config) mode() fp.Mode {
	if c.Reap {
		return fp.ModeReap
	}
	return fp.ModeReport
}

// label is the log prefix of a teardown row: REPORT or REAP.
func (c Config) label() string { return strings.ToUpper(string(c.mode())) }

// Deps are what a teardown reads, deletes and leases with.
type Deps struct {
	Lifecycle *lifecycle.Client
	// Source reads the footprint (inventory.NewAWSSource in the poller).
	Source inventory.Source
	// Deleters delete in reap mode. Report mode never calls them.
	Deleters inventory.Deleters
	// Scheduler creates the one-shot jobs in reap mode.
	Scheduler OneShotAPI
}

// Outcome is what one run decided.
type Outcome string

// Outcomes.
const (
	// OutcomeActive: something is in use; nothing to do.
	OutcomeActive Outcome = "active"
	// OutcomeWaiting: idle, but for less than the grace period.
	OutcomeWaiting Outcome = "waiting"
	// OutcomeNotClaimable: the lease could not be taken (another teardown
	// holds it, or a creator ran since the item was read).
	OutcomeNotClaimable Outcome = "not-claimable"
	// OutcomeReleased: the post-claim re-check found activity, or the
	// footprint could not be read in full; the lease was given back.
	OutcomeReleased Outcome = "released"
	// OutcomeReported: report mode ran the whole plan and gave the lease back.
	OutcomeReported Outcome = "reported"
	// OutcomeReaped: reap mode deleted, handed off its own removal and
	// finished the lease.
	OutcomeReaped Outcome = "reaped"
	// OutcomeLeaseLost: a creator took the lease mid-teardown; it stopped.
	OutcomeLeaseLost Outcome = "lease-lost"
	// OutcomeFailed: something failed; the lease was given back (or, after
	// the poller committed its own removal, left to go stale).
	OutcomeFailed Outcome = "failed"
	// OutcomeKept: reap mode kept the poller: no region with a live poller
	// owes the account-wide resources, so this one goes on owing them, and
	// decides again a grace period later. Nothing was deleted.
	OutcomeKept Outcome = "kept"
)

// Phase is when, and by whom, a planned resource is removed.
type Phase string

// Phases.
const (
	// PhaseDirect: the poller deletes it at once (its tables, bucket, topic,
	// per-launch schedules and, in the last region, lagotto's instance role
	// and profile).
	PhaseDirect Phase = "direct"
	// PhaseSelf: the poller's own function, log group and role, which a
	// one-shot job deletes after the poller has finished (see OneShot).
	PhaseSelf Phase = "self"
	// PhaseFinal: its own rate schedule and lagotto's schedule group, which
	// the poller deletes last, once the jobs exist.
	PhaseFinal Phase = "final"
	// PhaseCLI: what the poller leaves for `lagotto teardown`: what it has
	// no grant to delete, and what its own jobs need.
	PhaseCLI Phase = "cli"
)

// PhaseOf says how the poller's teardown removes r, and for PhaseCLI why it
// does not.
func PhaseOf(r fp.Resource) (Phase, string) {
	switch r.Type {
	case inventory.TypeTable, inventory.TypeBucket, inventory.TypeTopic:
		return PhaseDirect, ""
	case inventory.TypeFunction, inventory.TypeLogGroup:
		return PhaseSelf, ""
	case inventory.TypeSchedule:
		if strings.Contains(r.ARN, ":schedule/"+runtimeiam.TeardownScheduleGroup+"/") {
			return PhaseCLI, "a one-shot teardown job still to run"
		}
		if r.Name == deploy.PollerScheduleName {
			return PhaseFinal, ""
		}
		return PhaseDirect, ""
	case inventory.TypeScheduleGroup:
		switch r.Name {
		case schedgroup.Name:
			return PhaseFinal, ""
		case runtimeiam.TeardownScheduleGroup:
			return PhaseCLI, "it holds the one-shot teardown jobs; 'lagotto teardown' removes it once they have run"
		}
	case inventory.TypeRole:
		switch r.Name {
		case runtimeiam.RoleName:
			return PhaseSelf, ""
		case runtimeiam.InstanceRoleName:
			return PhaseDirect, ""
		case runtimeiam.SchedulerInvokeRoleName:
			return PhaseCLI, "the one-shot teardown jobs run as it; 'lagotto teardown --iam-roles' removes it"
		}
	case inventory.TypeInstanceProfile:
		if r.Name == runtimeiam.InstanceRoleName {
			return PhaseDirect, ""
		}
		return PhaseCLI, "the poller may delete only lagotto's own instance profile"
	case inventory.TypePolicy:
		return PhaseCLI, "the poller has no grant on IAM policies; 'lagotto teardown --iam-roles' removes it"
	}
	return PhaseCLI, "the poller does not delete a " + r.Type
}

// Plan is a teardown as planned and checked.
type Plan struct {
	// Last reports that this region's run is the one that tears down the
	// account-wide resources (global.go): every other region is done, or in
	// report mode finishing too. LastWhy says why, or why not.
	Last    bool
	LastWhy string
	// Others are the other enabled regions as the global step judged them.
	Others []OtherRegion
	// Kept are left in place: in use, depended on by something in use, or
	// left for `lagotto teardown` (PhaseCLI).
	Kept []inventory.Kept
	// Direct, Self and Final are footprint.Execute's results for each phase
	// of the region's own resources. Self always runs in report mode: its
	// outcome only decides the jobs.
	Direct, Self, Final *fp.Result
	// Phases are the plans executed, by phase.
	Phases map[Phase]*fp.Plan
	// OneShots are the region's self-removal jobs.
	OneShots []OneShot
	// Global is the account-wide teardown, set when this run planned it.
	// Last is true when this run removes the account-wide resources; a fence
	// that stopped it after the account-wide deletes (a region elsewhere is
	// no longer done) leaves Last false and Global holding what ran.
	Global *GlobalPlan
}

func (p *Plan) results() []*fp.Result {
	rs := []*fp.Result{p.Direct, p.Self, p.Final}
	if p.Global != nil {
		rs = append(rs, p.Global.Direct, p.Global.Self)
	}
	return rs
}

func (p *Plan) globalOneShots() int {
	if p.Global == nil {
		return 0
	}
	return len(p.Global.OneShots)
}

// WouldDelete counts the resources the teardown removes (or, in reap mode,
// removed or handed to a job).
func (p *Plan) WouldDelete() int {
	n := 0
	for _, r := range p.results() {
		if r != nil {
			n += r.Count(fp.OutcomeWouldDelete) + r.Count(fp.OutcomeDeleted)
		}
	}
	return n
}

// KeptCount counts what stays: kept by the plan, and refused, skipped or kept
// by the delete guard.
func (p *Plan) KeptCount() int {
	n := len(p.Kept)
	if p.Global != nil {
		n += len(p.Global.Kept)
	}
	for _, r := range p.results() {
		if r != nil {
			n += r.Count(fp.OutcomeKept) + r.Count(fp.OutcomeRefused) + r.Count(fp.OutcomeSkipped)
		}
	}
	return n
}

// Failed counts the steps that failed or were stopped.
func (p *Plan) Failed() int {
	n := 0
	for _, r := range p.results() {
		if r != nil {
			n += r.Count(fp.OutcomeFailed) + r.Count(fp.OutcomeStopped) + r.Count(fp.OutcomeNotRun)
		}
	}
	return n
}

// Result is what one run did.
type Result struct {
	Outcome Outcome
	Verdict Verdict
	// IdleFor is how long the region had been idle, per the lease table.
	IdleFor time.Duration
	// Plan is set once the lease was claimed and the footprint read.
	Plan *Plan
	Err  error
}

// Tick runs one region's idle check: the verdict, Observe, and, once the
// region has been idle for the grace period, the teardown. It never disables
// or slows the poller's schedule (pkg/pollcadence owns the cadence), and in
// report mode it deletes nothing. Every step is logged.
func Tick(ctx context.Context, cfg Config, d Deps) Result {
	cfg = cfg.withDefaults()
	logf := cfg.Logf
	v := ReadVerdict(ctx, d.Source.Region(cfg.Region), cfg.Region, cfg.Options)
	res := Result{Verdict: v}
	for _, w := range v.Warnings {
		logf("idle-verdict region=%s warning=%q", cfg.Region, w)
	}

	_, idleFor, err := d.Lifecycle.Observe(ctx, Tool, cfg.Region, v.Active)
	res.IdleFor = idleFor
	if err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		if errors.Is(err, lifecycle.ErrTableClosing) {
			logf("idle-verdict region=%s active=%t observe=skipped reason=%q", cfg.Region, v.Active, "the lifecycle table is being deleted")
			if cfg.Reap {
				// A closer that died leaves its sentinel, and Observe never
				// clears one (MarkActive does; libs#88). CloseTableIfEmpty takes a
				// stale sentinel over, and drops it again while any tool's
				// item is left, so the next run observes; a live closer's
				// sentinel it leaves alone.
				if _, cerr := d.Lifecycle.CloseTableIfEmpty(ctx, cfg.Region); cerr != nil {
					logf("idle-verdict region=%s close_table=failed error=%q", cfg.Region, cerr.Error())
				}
			}
			return res
		}
		logf("idle-verdict region=%s active=%t observe=failed error=%q", cfg.Region, v.Active, err.Error())
		return res
	}
	line := fmt.Sprintf("idle-verdict region=%s active=%t idle_for=%s grace=%s", cfg.Region, v.Active, idleFor.Round(time.Second), cfg.Grace)
	if v.Active {
		logf("%s reasons=%q", line, strings.Join(v.Reasons, "; "))
		res.Outcome = OutcomeActive
		return res
	}
	logf("%s", line)
	if idleFor < cfg.Grace {
		res.Outcome = OutcomeWaiting
		return res
	}

	lease, err := d.Lifecycle.Claim(ctx, Tool, cfg.Region, cfg.Grace)
	if err != nil {
		res.Err = err
		res.Outcome = OutcomeNotClaimable
		if !errors.Is(err, lifecycle.ErrNotClaimable) {
			res.Outcome = OutcomeFailed
		}
		logf("idle-teardown %s region=%s claim=refused reason=%q", cfg.label(), cfg.Region, err.Error())
		return res
	}
	logf("idle-teardown %s region=%s claim=ok lease=%s", cfg.label(), cfg.Region, shortToken(lease.Token()))
	return teardown(ctx, cfg, d, lease, res)
}

func shortToken(t string) string {
	if len(t) > 8 {
		return t[:8]
	}
	return t
}

// release gives the lease back and logs why.
func release(ctx context.Context, cfg Config, lease *lifecycle.Lease, why string) error {
	err := lease.Release(ctx)
	status := "released"
	if err != nil {
		status = "release-failed"
	}
	if err != nil {
		cfg.Logf("idle-teardown %s region=%s lease=%s reason=%q error=%q", cfg.label(), cfg.Region, status, why, err.Error())
	} else {
		cfg.Logf("idle-teardown %s region=%s lease=%s reason=%q", cfg.label(), cfg.Region, status, why)
	}
	return err
}

// run is one teardown in progress: what Tick claimed, and the footprint the
// plan was made from.
type run struct {
	cfg   Config
	d     Deps
	lease *lifecycle.Lease
	facts inventory.Facts
}

// logRow logs each action of a phase's result.
func (t *run) logRow(ph string, p *fp.Plan, r *fp.Result) {
	if r == nil {
		return
	}
	for i, a := range r.Actions {
		region := ""
		if i < len(p.Steps) {
			region = p.Steps[i].Resource.Region
		}
		row := fmt.Sprintf("idle-teardown %s region=%s phase=%s outcome=%s type=%s resource_region=%s name=%q arn=%s",
			t.cfg.label(), t.cfg.Region, ph, a.Outcome, a.Type, region, a.Name, a.ARN)
		if a.Reason != "" {
			row += fmt.Sprintf(" reason=%q", a.Reason)
		}
		if a.Err != nil {
			row += fmt.Sprintf(" error=%q", a.Err.Error())
		}
		t.cfg.Logf("%s", row)
	}
}

// logOneShots logs the self-removal jobs.
func (t *run) logOneShots(jobs []OneShot) {
	for _, o := range jobs {
		t.cfg.Logf("idle-teardown %s region=%s phase=one-shot name=%s offset=+%s at=%s group=%s target=%s input=%s resource=%s",
			t.cfg.label(), t.cfg.Region, o.Name, o.Offset, o.At.UTC().Format(time.RFC3339), runtimeiam.TeardownScheduleGroup, o.Target, o.Input, o.Resource)
	}
}

// teardown runs with the lease held: re-check, plan, execute, hand off, and
// the global step.
func teardown(ctx context.Context, cfg Config, d Deps, lease *lifecycle.Lease, res Result) Result {
	// The post-claim re-check, and the footprint the plan is made from: every
	// enabled region (the global step judges the others from it), read now
	// that the lease keeps creators out of this region.
	facts, err := inventory.Gather(ctx, d.Source, cfg.AccountID, inventory.Scope{AllRegions: true}, cfg.Options)
	if err != nil {
		res.Outcome, res.Err = OutcomeReleased, err
		_ = release(ctx, cfg, lease, "could not read the footprint: "+err.Error())
		return res
	}
	here := facts.RegionFacts(cfg.Region)
	switch {
	case here == nil:
		res.Outcome = OutcomeReleased
		_ = release(ctx, cfg, lease, cfg.Region+" is not among the account's enabled regions")
		return res
	case here.Active():
		res.Outcome = OutcomeReleased
		why := strings.Join(here.ActiveReasons(), "; ")
		for _, w := range facts.Warnings {
			if strings.HasPrefix(w, cfg.Region+":") {
				why += "; " + w
			}
		}
		_ = release(ctx, cfg, lease, "active after the claim: "+why)
		return res
	}
	t := &run{cfg: cfg, d: d, lease: lease, facts: facts}
	if cfg.Reap {
		return t.reap(ctx, res)
	}
	return t.report(ctx, res)
}

// planRegion plans the region's own resources from facts (the account-wide
// ones are the global step's) into plan, and returns the deleter that
// re-checks against them.
func (t *run) planRegion(plan *Plan, facts inventory.Facts) (fp.Deleter, error) {
	cfg := t.cfg
	live := inventory.Classify(facts, cfg.Options, cfg.Now())
	scoped := *live
	scoped.Resources = nil
	for _, r := range live.Resources {
		if r.Region == cfg.Region {
			scoped.Resources = append(scoped.Resources, r)
		}
	}
	full, kept, err := inventory.PlanTeardown(&scoped, live, cfg.Options)
	if err != nil {
		return nil, err
	}
	plan.Kept = kept
	byPhase := map[Phase][]fp.Resource{}
	for _, st := range full.Steps {
		ph, why := PhaseOf(st.Resource)
		if ph == PhaseCLI {
			plan.Kept = append(plan.Kept, inventory.Kept{Resource: st.Resource, Reason: "left for the CLI: " + why})
			continue
		}
		byPhase[ph] = append(byPhase[ph], st.Resource)
	}
	for _, ph := range []Phase{PhaseDirect, PhaseSelf, PhaseFinal} {
		p, err := fp.NewPlan(full.Tool, full.Patterns, byPhase[ph])
		if err != nil {
			return nil, err
		}
		plan.Phases[ph] = p
	}
	for _, k := range plan.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}
	return inventory.NewDeleter(live, t.d.Deleters), nil
}

// selfRemoval is what the Self phase's guard let through: the function and
// the log group, as jobs.
func (t *run) selfRemoval(self *fp.Result) SelfRemoval {
	sr := SelfRemoval{Region: t.cfg.Region, AccountID: t.cfg.AccountID}
	for _, a := range self.Actions {
		if a.Outcome != fp.OutcomeWouldDelete {
			continue
		}
		switch a.Type {
		case inventory.TypeFunction:
			sr.Function = true
		case inventory.TypeLogGroup:
			sr.LogGroup = true
		}
	}
	return sr
}

func (t *run) summary(plan *Plan, lease string, g GlobalLease) {
	t.cfg.Logf("idle-teardown %s region=%s would_delete=%d kept=%d failed=%d one_shots=%d last_region=%t lease=%s global_lease=%s last_region_why=%q",
		t.cfg.label(), t.cfg.Region, plan.WouldDelete(), plan.KeptCount(), plan.Failed(), len(plan.OneShots)+plan.globalOneShots(), plan.Last, lease, g, plan.LastWhy)
}

// report is the teardown in report mode: every phase re-checked and logged,
// nothing deleted, no job created, and nothing written to the global scope.
// It releases the regional lease (the grace period starts over) and then
// reports the account-wide step (reportGlobal).
func (t *run) report(ctx context.Context, res Result) Result {
	cfg, lease := t.cfg, t.lease
	plan := &Plan{Phases: map[Phase]*fp.Plan{}}
	res.Plan = plan
	deleter, err := t.planRegion(plan, t.facts)
	if err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		_ = release(ctx, cfg, lease, "could not plan: "+err.Error())
		return res
	}
	plan.Direct, err = fp.Execute(ctx, plan.Phases[PhaseDirect], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseDirect), plan.Phases[PhaseDirect], plan.Direct)
	if stopped(ctx, cfg, lease, &res, err, "the direct deletes") {
		return res
	}
	plan.Self, err = fp.Execute(ctx, plan.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseSelf), plan.Phases[PhaseSelf], plan.Self)
	if stopped(ctx, cfg, lease, &res, err, "the self-removal check") {
		return res
	}
	plan.OneShots = t.selfRemoval(plan.Self).Plan(cfg.Now())
	t.logOneShots(plan.OneShots)
	plan.Final, err = fp.Execute(ctx, plan.Phases[PhaseFinal], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseFinal), plan.Phases[PhaseFinal], plan.Final)
	if stopped(ctx, cfg, lease, &res, err, "the final deletes") {
		return res
	}
	if err := lease.Release(ctx); err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		if errors.Is(err, lifecycle.ErrLeaseLost) {
			res.Outcome = OutcomeLeaseLost
		}
		plan.LastWhy = "could not release this region's lease: " + err.Error()
		t.summary(plan, "release-failed", GlobalNone)
		return res
	}
	if err := t.reportGlobal(ctx, plan); err != nil {
		res.Err = errors.Join(res.Err, err)
		cfg.Logf("idle-teardown %s region=%s phase=global error=%q", cfg.label(), cfg.Region, err.Error())
	}
	res.Outcome = OutcomeReported
	t.summary(plan, "released", GlobalNone)
	return res
}

// reap is the teardown in reap mode, by the invariant (global.go): the
// global lease before the first delete; the account-wide decision, and the
// account-wide deletes when this region is the last, before the poller
// commits its own removal; the global item finished while this region's
// poller, still intact, owes the decision; and only then the commit.
func (t *run) reap(ctx context.Context, res Result) Result {
	cfg, d, lease := t.cfg, t.d, t.lease
	plan := &Plan{Phases: map[Phase]*fp.Plan{}}
	res.Plan = plan
	g := GlobalNone

	// 0. The global lease, before anything is deleted.
	_, err := ControlRegion(cfg.Region)
	var gl *lifecycle.Lease
	if err == nil {
		gl, err = acquireGlobal(ctx, cfg, d.Lifecycle)
	}
	if err != nil {
		plan.LastWhy = "could not take the global teardown lease: " + err.Error()
		_ = stopped(ctx, cfg, lease, &res, fmt.Errorf("take the global teardown lease: %w", err), "taking the global teardown lease")
		return res
	}
	cfg.Logf("idle-teardown %s region=%s phase=global claim=ok lease=%s scope=%s", cfg.label(), cfg.Region, shortToken(gl.Token()), lifecycle.ScopeGlobal)

	// back gives everything back before the commit: the regional lease
	// (stopped's rules) and the global item, since this region's poller is
	// intact and still owes the decision. The next grace period tries again.
	back := func(err error, what string) Result {
		t.cancelLeftJobs(ctx)
		if !stopped(ctx, cfg, lease, &res, err, what) {
			res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, err)
		}
		g = t.finishGlobal(ctx, gl)
		t.summary(plan, "released", g)
		return res
	}
	// keep gives everything back without a failure: this region keeps its
	// poller, and decides again a grace period later.
	keep := func(outcome Outcome, why string) Result {
		t.cancelLeftJobs(ctx)
		_ = release(ctx, cfg, lease, why)
		g = t.finishGlobal(ctx, gl)
		res.Outcome = outcome
		t.summary(plan, "released", g)
		return res
	}

	// 1. The decision, on a footprint read with the global lease held.
	fresh, err := inventory.Gather(ctx, d.Source, cfg.AccountID, inventory.Scope{AllRegions: true}, cfg.Options)
	if err != nil {
		return back(err, "reading the footprint under the global lease")
	}
	if here := fresh.RegionFacts(cfg.Region); here == nil || here.Active() {
		return keep(OutcomeReleased, "active after the global lease was taken")
	}
	t.facts = fresh
	plan.Others = t.judgeAll(ctx, &fresh)
	last, why, owed := decideGlobal(&fresh, plan.Others, true)
	plan.LastWhy = why
	if !last && !owed {
		return keep(OutcomeKept, "kept the poller: "+why+"; no region with a live poller owes the account-wide resources, so this one keeps owing them")
	}
	var gp *GlobalPlan
	if last {
		if gp, err = t.planGlobals(&fresh); err != nil {
			return back(err, "planning the account-wide teardown")
		}
	}

	// Both plans before anything is deleted.
	deleter, err := t.planRegion(plan, fresh)
	if err != nil {
		return back(err, "planning")
	}

	// 2. The last region: the account-wide deletes, fenced, before anything
	// of its own is deleted.
	if gp != nil {
		plan.Last, plan.Global = true, gp
		if !t.fence(ctx, plan, &fresh, "the account-wide deletes") {
			plan.Last, plan.Global = false, nil
			return keep(OutcomeKept, "kept the poller: "+plan.LastWhy)
		}
		fenced := fencedDeleter{Deleter: gp.deleter, fence: func(ctx context.Context) bool {
			return t.fence(ctx, plan, &fresh, "an account-wide delete")
		}, lease: lease}
		gp.Direct, err = fp.Execute(ctx, gp.Phases[PhaseDirect], fp.ModeReap, fenced, gl)
		t.logRow("global-direct", gp.Phases[PhaseDirect], gp.Direct)
		if errors.Is(err, errFenced) {
			plan.Last = false
			return keep(OutcomeKept, "kept the poller: "+plan.LastWhy)
		}
		if err != nil {
			return back(err, "the account-wide deletes")
		}
		gp.Self, err = fp.Execute(ctx, gp.Phases[PhaseSelf], fp.ModeReport, gp.deleter, gl)
		t.logRow("global-self", gp.Phases[PhaseSelf], gp.Self)
		if err != nil {
			return back(err, "the poller role's check")
		}
	}

	// 3. Its own resources, directly.
	if err := gl.Step(ctx, "the region's direct deletes"); err != nil {
		return back(err, "heartbeating the global lease")
	}
	plan.Direct, err = fp.Execute(ctx, plan.Phases[PhaseDirect], fp.ModeReap, deleter, lease)
	t.logRow(string(PhaseDirect), plan.Phases[PhaseDirect], plan.Direct)
	if err != nil {
		return back(err, "the direct deletes")
	}

	// 4. What becomes a job: its function and log group, and in the last
	// region the poller role.
	plan.Self, err = fp.Execute(ctx, plan.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseSelf), plan.Phases[PhaseSelf], plan.Self)
	if err != nil {
		return back(err, "the self-removal check")
	}
	sr := t.selfRemoval(plan.Self)
	sr.Role = gp != nil && roleWouldGo(gp.Self)
	now := cfg.Now()
	jobs := sr.Plan(now)
	for _, j := range jobs {
		if gp != nil && strings.HasPrefix(j.Name, roleJobPrefix) {
			gp.OneShots = append(gp.OneShots, j)
		} else {
			plan.OneShots = append(plan.OneShots, j)
		}
	}
	t.logOneShots(jobs)

	// 5. The commit, still under the global lease, so no other poller can
	// decide in between: the jobs, the poller role's first and its
	// function's last. They all fire after the poller's next hourly run, so
	// a run that stops anywhere before its own schedule is deleted leaves a
	// poller that runs once more and finishes. A deploy or a watch since the
	// decision stops it, before the jobs and again after (rolled back).
	before := func(ctx context.Context, step string) error {
		if err := gl.Step(ctx, step); err != nil {
			return err
		}
		return lease.Step(ctx, step)
	}
	if !t.fence(ctx, plan, &fresh, "the poller's own removal") {
		plan.Last, plan.Global = false, nil
		return keep(OutcomeKept, "kept the poller: "+plan.LastWhy)
	}
	created, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, jobs, before)
	if err != nil {
		if rerr := t.rollback(ctx, created); rerr != nil {
			return t.strand(&res, plan, GlobalHeld, errors.Join(err, rerr), "creating the one-shot jobs")
		}
		return back(err, "creating the one-shot jobs")
	}
	if !t.fence(ctx, plan, &fresh, "the end of the poller's own removal") {
		// Rolling the commit back is this region's to do only while the
		// decision still is: another region that took the global lease over
		// (it went stale) has judged this one committed, and acts on that.
		if err := gl.Step(ctx, "roll the commit back"); err == nil {
			plan.Last, plan.Global = false, nil
			if rerr := t.rollback(ctx, created); rerr != nil {
				return t.strand(&res, plan, GlobalHeld, rerr, "rolling the one-shot jobs back")
			}
			return keep(OutcomeKept, "kept the poller: "+plan.LastWhy)
		}
		cfg.Logf("idle-teardown %s region=%s phase=global lease=lost reason=%q", cfg.label(), cfg.Region,
			"another region took the decision over and judged this one committed: it stays committed")
	}

	// 6. The decision is durable (the account-wide resources are gone or
	// handed to their jobs, or a region with a live poller owes them): the
	// global item goes. Then its own rate schedule and group.
	g = t.finishGlobal(ctx, gl)
	plan.Final, err = fp.Execute(ctx, plan.Phases[PhaseFinal], fp.ModeReap, deleter, lease)
	t.logRow(string(PhaseFinal), plan.Phases[PhaseFinal], plan.Final)
	if err != nil {
		if plan.Final.Count(fp.OutcomeDeleted) == 0 {
			// Nothing of its own schedule is gone yet (a creator took the
			// lease over, say): its jobs go, and its poller stays, owing the
			// decision again.
			if rerr := t.rollback(ctx, created); rerr == nil {
				_ = stopped(ctx, cfg, lease, &res, err, "the final deletes")
				t.summary(plan, "released", g)
				return res
			}
		}
		return t.strand(&res, plan, g, err, "the final deletes")
	}

	// 7. Its lease, and its table.
	if err := lease.Finish(ctx); err != nil {
		return t.strand(&res, plan, g, err, "finishing its lease")
	}
	t.closeTable(ctx, cfg.Region)
	res.Outcome = OutcomeReaped
	t.summary(plan, "finished", g)
	return res
}

// cancelLeftJobs deletes the one-shot jobs an earlier run of this region
// left (it crashed while committing) when this run does not commit: the
// poller stays, and so must its role.
func (t *run) cancelLeftJobs(ctx context.Context) {
	here := t.facts.RegionFacts(t.cfg.Region)
	if here == nil {
		return
	}
	var left []string
	for _, s := range here.Schedules {
		if s.Group == runtimeiam.TeardownScheduleGroup && strings.HasPrefix(s.Name, "lagotto-teardown-") {
			left = append(left, s.Name)
		}
	}
	_ = t.rollback(ctx, left)
}

// roleJobPrefix names the poller role's one-shot jobs.
const roleJobPrefix = "lagotto-teardown-role"

// rollback deletes the one-shot jobs a failed commit had created, so the
// poller stays.
func (t *run) rollback(ctx context.Context, created []string) error {
	var errs []error
	for _, name := range created {
		_, err := t.d.Scheduler.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
			Name: aws.String(name), GroupName: aws.String(runtimeiam.TeardownScheduleGroup)})
		if err != nil && !isNotFound(err) {
			errs = append(errs, fmt.Errorf("delete one-shot job %s: %w", name, err))
		}
	}
	if len(created) > 0 {
		t.cfg.Logf("idle-teardown %s region=%s phase=one-shot rollback=%d error=%v", t.cfg.label(), t.cfg.Region, len(created), errors.Join(errs...))
	}
	return errors.Join(errs...)
}

// strand ends a run that failed after the poller committed its own removal:
// it will not run again, and the regional lease is left held, to go stale
// (a stale teardown whose poller is gone counts as done, and the last mile
// reports it to resume). The account-wide decision is durable already.
func (t *run) strand(res *Result, plan *Plan, g GlobalLease, err error, what string) Result {
	res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, err)
	if errors.Is(err, lifecycle.ErrLeaseLost) {
		res.Outcome = OutcomeLeaseLost
	}
	t.cfg.Logf("idle-teardown %s region=%s lease=held during=%q error=%q reason=%q", t.cfg.label(), t.cfg.Region, what, err.Error(),
		"failed after the poller committed its own removal; the regional lease is left to go stale")
	t.summary(plan, "held", g)
	return *res
}

// closeTable removes region's spore-host-lifecycle table if no spore.host
// tool's item is left in it.
func (t *run) closeTable(ctx context.Context, region string) {
	if _, err := t.d.Lifecycle.CloseTableIfEmpty(ctx, region); err != nil {
		t.cfg.Logf("idle-teardown %s region=%s close_table=failed table_region=%s error=%q", t.cfg.label(), t.cfg.Region, region, err.Error())
	}
}

// stopped handles a phase's error. A lost lease stops the teardown at once,
// in either mode. In reap mode any other failure gives the lease back, so
// nothing the failed step's resources depend on (and none of the poller's own
// removal) goes ahead, and the next grace period tries again. In report mode
// a failed re-check is a row of the report like any other, and the report
// carries on.
func stopped(ctx context.Context, cfg Config, lease *lifecycle.Lease, res *Result, err error, what string) bool {
	if err == nil {
		return false
	}
	res.Err = errors.Join(res.Err, err)
	if errors.Is(err, lifecycle.ErrLeaseLost) {
		res.Outcome = OutcomeLeaseLost
		cfg.Logf("idle-teardown %s region=%s lease=lost during=%q reason=%q", cfg.label(), cfg.Region, what,
			"a creator marked lagotto active, or another teardown took the lease over; stopped")
		return true
	}
	if !cfg.Reap {
		return false
	}
	res.Outcome = OutcomeFailed
	_ = release(ctx, cfg, lease, what+" failed: "+err.Error())
	return true
}
