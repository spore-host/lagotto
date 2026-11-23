package idle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

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
	// OutcomeFailed: something failed; the lease was given back.
	OutcomeFailed Outcome = "failed"
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

	// The region's own resources. The account-wide ones are the global
	// step's (global.go), at the end.
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
		res.Outcome, res.Err = OutcomeFailed, err
		_ = release(ctx, cfg, lease, "could not plan: "+err.Error())
		return res
	}
	plan := &Plan{Kept: kept, Phases: map[Phase]*fp.Plan{}}
	res.Plan = plan
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
			res.Outcome, res.Err = OutcomeFailed, err
			_ = release(ctx, cfg, lease, "could not plan: "+err.Error())
			return res
		}
		plan.Phases[ph] = p
	}

	deleter := inventory.NewDeleter(live, d.Deleters)
	mode := cfg.mode()
	for _, k := range plan.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}

	// 0. In reap mode, the global lease BEFORE the first delete (the
	// invariant, global.go). Not taken, the poller gives its lease back
	// having deleted nothing, and tries again a grace period later.
	var gl *lifecycle.Lease
	if cfg.Reap {
		_, err := ControlRegion(cfg.Region)
		if err == nil {
			gl, err = acquireGlobal(ctx, cfg, d.Lifecycle)
		}
		if err != nil {
			plan.LastWhy = "could not take the global teardown lease: " + err.Error()
			_ = stopped(ctx, cfg, lease, &res, fmt.Errorf("take the global teardown lease: %w", err), "taking the global teardown lease")
			return res
		}
		cfg.Logf("idle-teardown %s region=%s phase=global claim=ok lease=%s scope=%s", cfg.label(), cfg.Region, shortToken(gl.Token()), lifecycle.ScopeGlobal)
	}
	// stop ends a run stopped BEFORE it committed its removal: stopped's
	// rules for the regional lease, and the global item goes, since this
	// region's poller is intact and still owes the decision.
	stop := func(err error, what string) bool {
		if !stopped(ctx, cfg, lease, &res, err, what) {
			return false
		}
		if gl != nil {
			t.finishGlobal(ctx, gl)
		}
		return true
	}
	// strand ends a run stopped AFTER it committed its removal: its poller
	// will not run again, so both leases stay held, to go stale and be
	// resumed (a stale teardown whose poller is gone counts as done).
	strand := func(err error, what string) Result {
		res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, err)
		if errors.Is(err, lifecycle.ErrLeaseLost) {
			res.Outcome = OutcomeLeaseLost
		}
		cfg.Logf("idle-teardown %s region=%s lease=held during=%q error=%q reason=%q", cfg.label(), cfg.Region, what, err.Error(),
			"failed after the poller committed its own removal; the regional lease is left to go stale")
		t.holdGlobal("this region failed after committing its removal")
		return res
	}

	// 1. What the poller deletes directly.
	plan.Direct, err = fp.Execute(ctx, plan.Phases[PhaseDirect], mode, deleter, lease)
	t.logRow(string(PhaseDirect), plan.Phases[PhaseDirect], plan.Direct)
	if stop(err, "the direct deletes") {
		return res
	}
	// 2. Its own function and log group: always report mode, only to apply
	// the delete guard; what passes becomes a one-shot job. (Its role is
	// account-wide: the global step's.)
	plan.Self, err = fp.Execute(ctx, plan.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseSelf), plan.Phases[PhaseSelf], plan.Self)
	if stop(err, "the self-removal check") {
		return res
	}
	sr := SelfRemoval{Region: cfg.Region, AccountID: cfg.AccountID}
	for _, a := range plan.Self.Actions {
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
	now := cfg.Now()
	plan.OneShots = sr.Plan(now)
	t.logOneShots(plan.OneShots)
	if cfg.Reap {
		created, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, plan.OneShots, lease.Step)
		switch {
		case err != nil && slices.Contains(created, FunctionJobName):
			return strand(err, "creating the one-shot jobs")
		case err != nil:
			if stop(err, "creating the one-shot jobs") {
				return res
			}
		}
	}
	// 3. Its own rate schedule and lagotto's schedule group. From its first
	// delete the poller will not run again: it has committed its removal.
	plan.Final, err = fp.Execute(ctx, plan.Phases[PhaseFinal], mode, deleter, lease)
	t.logRow(string(PhaseFinal), plan.Phases[PhaseFinal], plan.Final)
	if err != nil && cfg.Reap && !errors.Is(err, lifecycle.ErrLeaseLost) {
		return strand(err, "the final deletes")
	}
	if stopped(ctx, cfg, lease, &res, err, "the final deletes") {
		if gl != nil {
			t.holdGlobal("this region lost its lease after committing its removal")
		}
		return res
	}

	// 4. The global step: the end of the regional lease, whether this is the
	// last region, and the account-wide teardown if it is. Report mode never
	// finishes the regional lease or closes a table: it releases, the grace
	// period starts over, and the next report is a grace period away.
	g := t.global(ctx, plan, gl)
	if g.err != nil {
		res.Err = errors.Join(res.Err, g.err)
		cfg.Logf("idle-teardown %s region=%s phase=global error=%q", cfg.label(), cfg.Region, g.err.Error())
	}
	summary := func(lease string) {
		cfg.Logf("idle-teardown %s region=%s would_delete=%d kept=%d failed=%d one_shots=%d last_region=%t lease=%s global_lease=%s last_region_why=%q",
			cfg.label(), cfg.Region, plan.WouldDelete(), plan.KeptCount(), plan.Failed(), len(plan.OneShots)+plan.globalOneShots(), plan.Last, lease, g.lease, plan.LastWhy)
	}
	switch {
	case g.endErr != nil:
		res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, g.endErr)
		if errors.Is(g.endErr, lifecycle.ErrLeaseLost) {
			res.Outcome = OutcomeLeaseLost
		}
		if cfg.Reap {
			summary("finish-failed")
		} else {
			summary("release-failed")
		}
		return res
	case !cfg.Reap:
		res.Outcome = OutcomeReported
		summary("released")
		return res
	}
	t.closeTable(ctx, cfg.Region)
	res.Outcome = OutcomeReaped
	summary("finished")
	return res
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
