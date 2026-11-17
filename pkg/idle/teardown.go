package idle

import (
	"context"
	"errors"
	"fmt"
	"os"
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
const GraceEnv = "LAGOTTO_IDLE_GRACE"

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
	return c
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
	// Last reports that this is the last region with lagotto's tables or
	// poller, so the account-wide IAM is in the plan; LastWhy says why.
	Last    bool
	LastWhy string
	// Kept are left in place: in use, depended on by something in use, or
	// left for `lagotto teardown` (PhaseCLI).
	Kept []inventory.Kept
	// Direct, Self and Final are footprint.Execute's results for each phase.
	// Self always runs in report mode: its outcome only decides the jobs.
	Direct, Self, Final *fp.Result
	// Phases are the plans executed, by phase.
	Phases map[Phase]*fp.Plan
	// OneShots are the self-removal jobs.
	OneShots []OneShot
}

// WouldDelete counts the resources the teardown removes (or, in reap mode,
// removed or handed to a job).
func (p *Plan) WouldDelete() int {
	n := 0
	for _, r := range []*fp.Result{p.Direct, p.Self, p.Final} {
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
	for _, r := range []*fp.Result{p.Direct, p.Self, p.Final} {
		if r != nil {
			n += r.Count(fp.OutcomeKept) + r.Count(fp.OutcomeRefused) + r.Count(fp.OutcomeSkipped)
		}
	}
	return n
}

// Failed counts the steps that failed or were stopped.
func (p *Plan) Failed() int {
	n := 0
	for _, r := range []*fp.Result{p.Direct, p.Self, p.Final} {
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

// teardown runs with the lease held: re-check, plan, execute, hand off.
func teardown(ctx context.Context, cfg Config, d Deps, lease *lifecycle.Lease, res Result) Result {
	// The post-claim re-check, and the footprint the plan is made from: every
	// enabled region (only that can say whether this is the last region, and
	// classify the account-wide IAM), read now that the lease keeps creators
	// out of this region.
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

	live := inventory.Classify(facts, cfg.Options, cfg.Now())
	last, lastWhy := inventory.LastRegion(facts, cfg.Region)
	scoped := *live
	scoped.Resources = nil
	for _, r := range live.Resources {
		if r.Region == cfg.Region || (last && r.Region == inventory.Global) {
			scoped.Resources = append(scoped.Resources, r)
		}
	}
	full, kept, err := inventory.PlanTeardown(&scoped, live, cfg.Options)
	if err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		_ = release(ctx, cfg, lease, "could not plan: "+err.Error())
		return res
	}
	plan := &Plan{Last: last, LastWhy: lastWhy, Kept: kept, Phases: map[Phase]*fp.Plan{}}
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
	logRow := func(ph Phase, p *fp.Plan, r *fp.Result) {
		for i, a := range r.Actions {
			region := ""
			if i < len(p.Steps) {
				region = p.Steps[i].Resource.Region
			}
			row := fmt.Sprintf("idle-teardown %s region=%s phase=%s outcome=%s type=%s resource_region=%s name=%q arn=%s",
				cfg.label(), cfg.Region, ph, a.Outcome, a.Type, region, a.Name, a.ARN)
			if a.Reason != "" {
				row += fmt.Sprintf(" reason=%q", a.Reason)
			}
			if a.Err != nil {
				row += fmt.Sprintf(" error=%q", a.Err.Error())
			}
			cfg.Logf("%s", row)
		}
	}
	for _, k := range plan.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}

	// 1. What the poller deletes directly.
	plan.Direct, err = fp.Execute(ctx, plan.Phases[PhaseDirect], mode, deleter, lease)
	logRow(PhaseDirect, plan.Phases[PhaseDirect], plan.Direct)
	if stop := stopped(ctx, cfg, lease, &res, err, "the direct deletes"); stop {
		return res
	}
	// 2. Its own function, log group and role: always report mode, only to
	// apply the delete guard; what passes becomes a one-shot job.
	plan.Self, err = fp.Execute(ctx, plan.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	logRow(PhaseSelf, plan.Phases[PhaseSelf], plan.Self)
	if stop := stopped(ctx, cfg, lease, &res, err, "the self-removal check"); stop {
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
		case inventory.TypeRole:
			sr.Role = last
		}
	}
	now := cfg.Now()
	plan.OneShots = sr.Plan(now)
	for _, o := range plan.OneShots {
		cfg.Logf("idle-teardown %s region=%s phase=one-shot name=%s offset=+%s at=%s group=%s target=%s input=%s resource=%s",
			cfg.label(), cfg.Region, o.Name, o.Offset, o.At.UTC().Format(time.RFC3339), runtimeiam.TeardownScheduleGroup, o.Target, o.Input, o.Resource)
	}
	if cfg.Reap {
		if _, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, plan.OneShots, lease.Step); err != nil {
			if stop := stopped(ctx, cfg, lease, &res, err, "creating the one-shot jobs"); stop {
				return res
			}
		}
	}
	// 3. Last, its own rate schedule and lagotto's schedule group.
	plan.Final, err = fp.Execute(ctx, plan.Phases[PhaseFinal], mode, deleter, lease)
	logRow(PhaseFinal, plan.Phases[PhaseFinal], plan.Final)
	if stop := stopped(ctx, cfg, lease, &res, err, "the final deletes"); stop {
		return res
	}

	summary := func(lease string) {
		cfg.Logf("idle-teardown %s region=%s would_delete=%d kept=%d failed=%d one_shots=%d last_region=%t lease=%s last_region_why=%q",
			cfg.label(), cfg.Region, plan.WouldDelete(), plan.KeptCount(), plan.Failed(), len(plan.OneShots), last, lease, lastWhy)
	}
	if !cfg.Reap {
		// Report mode never finishes the lease or closes the table: the grace
		// period starts over, and the next report is a grace period away.
		if err := lease.Release(ctx); err != nil {
			res.Outcome, res.Err = OutcomeFailed, err
			if errors.Is(err, lifecycle.ErrLeaseLost) {
				res.Outcome = OutcomeLeaseLost
			}
			summary("release-failed")
			return res
		}
		res.Outcome = OutcomeReported
		summary("released")
		return res
	}
	if err := lease.Finish(ctx); err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		if errors.Is(err, lifecycle.ErrLeaseLost) {
			res.Outcome = OutcomeLeaseLost
		}
		summary("finish-failed")
		return res
	}
	if _, err := d.Lifecycle.CloseTableIfEmpty(ctx, cfg.Region); err != nil {
		cfg.Logf("idle-teardown %s region=%s close_table=failed error=%q", cfg.label(), cfg.Region, err.Error())
	}
	res.Outcome = OutcomeReaped
	summary("finished")
	return res
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
