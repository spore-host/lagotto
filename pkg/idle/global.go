package idle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The global teardown (lagotto#340). lagotto's account-wide resources (the
// poller role every region's poller runs as, lagotto-instance-role and its
// profile, and what the poller leaves for the CLI: the Scheduler invoke role
// and lagotto-cli-user) go once every region's teardown has finished, and
// exactly one region's poller plans them: the last one to finish.
//
// A region's teardown decides it at its very end, after its own deletes:
//
//  1. It reads every other enabled region's lagotto lifecycle item. If none
//     has one, and none still has a poller of its own, no other region can be
//     deciding at the same time (a deciding region holds its item until it
//     has decided), so it decides under its own regional lease.
//  2. Otherwise it takes the lease on lagotto's global scope (the item
//     tool#lagotto#global in ControlRegion's spore-host-lifecycle table), the
//     mutex every region's decision runs under, waiting up to
//     Config.GlobalWait while another region holds it. Under it, it ends its
//     own regional lease first (in reap mode Finish, which deletes its item),
//     and only then reads the other regions.
//  3. Each other region stands done (no lifecycle item, and no poller left
//     that is not already handed to its one-shot job), pending (tearing down
//     with a live lease, or idle past the grace: it will run this same step
//     after its own deletes), or active (in use, within its grace, held, a
//     stalled teardown its poller will resume, or anything that could not be
//     read: fail safe). Any active region keeps the globals. In reap mode a
//     pending one does too: it finishes after this region, takes the global
//     lease, finds this region's item gone, and is the one that decides.
//  4. The last region plans the globals from a footprint read again now, and
//     in reap mode deletes lagotto-instance-role and its profile and creates
//     the poller role's one-shot jobs, under the lease it decided with.
//
// Because a region ends its own regional lease before it gives the global
// lease back, and a region whose item still exists is never "done", of two
// regions deciding in turn the first always sees the second's item and
// defers, and the second always sees the first's gone: exactly one plans the
// globals, however close together the two went idle. A region that skips the
// mutex (step 1) saw no other item, so no other region can be deciding.
//
// Report mode cannot finish its regional lease: it releases it, which puts the
// item back to ACTIVE and restarts the region's idle clock, and the lifecycle
// item keeps no record of the release. A later region cannot tell that state
// from a region in use, so in report mode a pending region counts as done:
// the region that decides first reports the globals, the other then finds it
// ACTIVE and does not. That is exactly one report for regions that go idle
// within a poll interval of each other (the case seen live in lagotto#340);
// regions that cross the grace hours apart report no global teardown in
// report mode, while reap mode removes the globals in the last of them.

// ControlRegion is the region whose spore-host-lifecycle table holds lagotto's
// global-scope lifecycle item: the partition's IAM home region, so that every
// region's poller and every CLI read the same one.
func ControlRegion(region string) string {
	switch {
	case strings.HasPrefix(region, "us-gov-"):
		return "us-gov-west-1"
	case strings.HasPrefix(region, "cn-"):
		return "cn-north-1"
	}
	return "us-east-1"
}

// DefaultGlobalWait is how long a region's teardown waits for another
// region's global step to give the global lease back. A global step takes
// seconds; a holder that died leaves a lease that goes stale after 20 min,
// which is longer than this, so a region that cannot take it in time does not
// decide (see Tick).
const DefaultGlobalWait = 5 * time.Minute

// globalPoll is how often a waiting region tries the global lease again.
const globalPoll = 5 * time.Second

// Standing is how the global step judged another enabled region.
type Standing string

// Standings.
const (
	// StandingDone: no lagotto lifecycle item, and nothing there that still
	// needs the globals.
	StandingDone Standing = "done"
	// StandingPending: tearing down with a live lease, or idle past the
	// grace; it runs the global step itself after its own deletes.
	StandingPending Standing = "pending"
	// StandingActive: in use, within its grace, held, a stalled teardown,
	// or unreadable. It keeps the globals.
	StandingActive Standing = "active"
)

// OtherRegion is one other region's standing, and why.
type OtherRegion struct {
	Region   string
	Standing Standing
	Why      string
}

// judgeOthers reads the lifecycle item of every region in f other than self
// and judges each with f's facts of that region.
func judgeOthers(ctx context.Context, lc *lifecycle.Client, f *inventory.Facts, self string, grace time.Duration, now time.Time) []OtherRegion {
	var out []OtherRegion
	for i := range f.Regions {
		rf := &f.Regions[i]
		if rf.Region == self {
			continue
		}
		it, ok, err := lc.Get(ctx, Tool, rf.Region)
		out = append(out, judge(rf, it, ok, err, lc.LeaseStale(it), grace, now))
	}
	return out
}

// judge is one other region's standing from its lifecycle item (it, ok, err:
// lifecycle.Client.Get's answer) and its footprint facts rf.
func judge(rf *inventory.Region, it lifecycle.Item, ok bool, err error, stale bool, grace time.Duration, now time.Time) OtherRegion {
	o := OtherRegion{Region: rf.Region, Standing: StandingActive}
	switch {
	case err != nil:
		o.Why = "could not read its lifecycle item: " + err.Error()
	case rf.Active():
		o.Why = strings.Join(rf.ActiveReasons(), "; ")
	case ok && len(it.Holds) > 0:
		o.Why = fmt.Sprintf("held by %v", it.Holds)
	case ok && it.Lease != nil && stale:
		o.Why = "its teardown stalled (a stale lease held by " + it.Lease.Owner + "); its poller resumes it"
	case ok && it.Lease != nil:
		o.Standing, o.Why = StandingPending, "tearing down (lease held by "+it.Lease.Owner+")"
	case ok && it.State == lifecycle.StateIdle && !it.IdleSince.IsZero() && it.IdleFor(now) >= grace:
		o.Standing, o.Why = StandingPending, fmt.Sprintf("idle for %s, past the %s grace", it.IdleFor(now).Round(time.Minute), grace)
	case ok && it.State == lifecycle.StateIdle:
		o.Why = fmt.Sprintf("idle for %s of the %s grace", it.IdleFor(now).Round(time.Minute), grace)
	case ok:
		o.Why = "state " + string(it.State)
	case rf.Function != nil && !functionJobPending(rf):
		o.Why = "its poller has no lifecycle item (a poller from before lagotto#326)"
	default:
		o.Standing, o.Why = StandingDone, "no lifecycle item"
		if rf.Function != nil {
			o.Why += "; its poller is handed to its one-shot job"
		}
	}
	return o
}

// functionJobPending reports whether r's poller function has its one-shot
// delete job waiting in lagotto-teardown: the poller has finished its
// teardown and is going.
func functionJobPending(r *inventory.Region) bool {
	for _, s := range r.Schedules {
		if s.Group == runtimeiam.TeardownScheduleGroup && s.Name == FunctionJobName {
			return true
		}
	}
	return false
}

// decideGlobal says whether this region tears down the account-wide
// resources, given a footprint of every region and the other regions'
// standings, and why.
func decideGlobal(f *inventory.Facts, others []OtherRegion, reap bool) (bool, string) {
	if !f.AllRegions {
		return false, "the footprint did not read every enabled region"
	}
	if f.AccountIncomplete {
		return false, "the account-wide resources could not all be read"
	}
	var active, pending []string
	for _, o := range others {
		switch o.Standing {
		case StandingActive:
			active = append(active, o.Region+" ("+o.Why+")")
		case StandingPending:
			pending = append(pending, o.Region)
		}
	}
	sort.Strings(active)
	sort.Strings(pending)
	switch {
	case len(active) > 0:
		return false, "still active: " + strings.Join(active, ", ")
	case len(pending) > 0 && reap:
		return false, "finishing after this region, and the last of them tears the account-wide resources down: " + strings.Join(pending, ", ")
	case len(pending) > 0:
		return true, "report mode: also tearing down or idle past the grace, so this report stands for the last of them: " + strings.Join(pending, ", ")
	}
	return true, "no other enabled region has lagotto in use or still to tear down"
}

// acquireGlobal takes the global-scope lease, waiting while another region
// holds it.
func acquireGlobal(ctx context.Context, cfg Config, lc *lifecycle.Client) (*lifecycle.Lease, error) {
	tries := int(cfg.GlobalWait/globalPoll) + 1
	var last error
	for i := 0; i < tries; i++ {
		if i > 0 {
			if err := cfg.Sleep(ctx, globalPoll); err != nil {
				return nil, err
			}
		}
		_, _, err := lc.Observe(ctx, Tool, lifecycle.ScopeGlobal, false)
		if err == nil {
			var l *lifecycle.Lease
			if l, err = lc.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err == nil {
				return l, nil
			}
		}
		if !errors.Is(err, lifecycle.ErrNotClaimable) && !errors.Is(err, lifecycle.ErrTableClosing) {
			return nil, err
		}
		last = err
	}
	return nil, fmt.Errorf("another region held it for %s: %w", cfg.GlobalWait, last)
}

// GlobalPlan is the account-wide part of a teardown, in the run that planned
// it.
type GlobalPlan struct {
	// Kept are left in place: in use, or left for `lagotto teardown`.
	Kept []inventory.Kept
	// Phases are the plans executed: PhaseDirect (lagotto-instance-role and
	// its profile) and PhaseSelf (the poller role, removed by jobs).
	Phases map[Phase]*fp.Plan
	// Direct and Self are footprint.Execute's results; Self always runs in
	// report mode.
	Direct, Self *fp.Result
	// OneShots are the poller role's removal jobs.
	OneShots []OneShot
}

// globalOutcome is what the global step did to the leases.
type globalOutcome struct {
	// endErr is the regional lease's Finish (reap) or Release (report) error.
	endErr error
	// held: the regional lease was left held, because the global lease could
	// not be taken in reap mode (see Tick).
	held bool
	// mutex: the global lease was taken.
	mutex bool
	// err is a failure of the global step itself.
	err error
}

// global is the global step, run at the end of a region's teardown with its
// regional lease held. It always ends the regional lease (unless held), and
// records its decision in plan.
func (t *run) global(ctx context.Context, plan *Plan) globalOutcome {
	cfg, d := t.cfg, t.d
	var g globalOutcome
	end := func() {
		if cfg.Reap {
			g.endErr = t.lease.Finish(ctx)
		} else {
			g.endErr = t.lease.Release(ctx)
		}
	}
	decide := func(lease *lifecycle.Lease) {
		fresh, err := inventory.Gather(ctx, d.Source, cfg.AccountID, inventory.Scope{AllRegions: true}, cfg.Options)
		if err != nil {
			plan.Last, plan.LastWhy = false, "could not read the footprint again: "+err.Error()
			g.err = err
			return
		}
		plan.Others = judgeOthers(ctx, d.Lifecycle, &fresh, cfg.Region, cfg.Grace, cfg.Now())
		for _, o := range plan.Others {
			cfg.Logf("idle-teardown %s region=%s phase=global other_region=%s standing=%s why=%q",
				cfg.label(), cfg.Region, o.Region, o.Standing, o.Why)
		}
		plan.Last, plan.LastWhy = decideGlobal(&fresh, plan.Others, cfg.Reap)
		if plan.Last {
			g.err = t.globals(ctx, plan, &fresh, lease)
		}
	}

	pre := judgeOthers(ctx, d.Lifecycle, &t.facts, cfg.Region, cfg.Grace, cfg.Now())
	alone := true
	for _, o := range pre {
		if o.Standing != StandingDone {
			alone = false
		}
	}
	if alone {
		// No other region can be deciding: decide under the regional lease,
		// then end it.
		decide(t.lease)
		end()
		return g
	}

	gl, err := acquireGlobal(ctx, cfg, d.Lifecycle)
	if err != nil {
		plan.Last, plan.LastWhy = false, "could not take the global teardown lease: "+err.Error()
		g.err = err
		if cfg.Reap {
			// Ending the regional lease now could leave nobody to decide:
			// a region that deferred to this one is waiting for its item to
			// go. Left held, it goes stale; every other region then counts
			// this one active, and `lagotto teardown` or the last mile
			// resumes it.
			g.held = true
			return g
		}
		end()
		return g
	}
	g.mutex = true
	cfg.Logf("idle-teardown %s region=%s phase=global claim=ok lease=%s scope=%s", cfg.label(), cfg.Region, shortToken(gl.Token()), lifecycle.ScopeGlobal)
	end()
	if g.endErr == nil {
		decide(gl)
	} else {
		plan.Last, plan.LastWhy = false, "could not end this region's lease: "+g.endErr.Error()
	}
	if err := gl.Finish(ctx); err != nil {
		cfg.Logf("idle-teardown %s region=%s phase=global lease=finish-failed scope=%s error=%q", cfg.label(), cfg.Region, lifecycle.ScopeGlobal, err.Error())
		g.err = errors.Join(g.err, err)
	}
	return g
}

// globals plans the account-wide teardown from fresh, a footprint read after
// this region's own deletes, and in reap mode executes it under lease.
func (t *run) globals(ctx context.Context, plan *Plan, fresh *inventory.Facts, lease *lifecycle.Lease) error {
	cfg, d := t.cfg, t.d
	live := inventory.Classify(*fresh, cfg.Options, cfg.Now())
	scoped := *live
	scoped.Resources = nil
	for _, r := range live.Resources {
		if r.Region == inventory.Global {
			scoped.Resources = append(scoped.Resources, r)
		}
	}
	full, kept, err := inventory.PlanTeardown(&scoped, live, cfg.Options)
	if err != nil {
		return err
	}
	gp := &GlobalPlan{Kept: kept, Phases: map[Phase]*fp.Plan{}}
	plan.Global = gp
	byPhase := map[Phase][]fp.Resource{}
	for _, st := range full.Steps {
		ph, why := PhaseOf(st.Resource)
		if ph != PhaseDirect && ph != PhaseSelf { // PhaseCLI: nothing account-wide is PhaseFinal
			gp.Kept = append(gp.Kept, inventory.Kept{Resource: st.Resource, Reason: "left for the CLI: " + why})
			continue
		}
		byPhase[ph] = append(byPhase[ph], st.Resource)
	}
	for _, ph := range []Phase{PhaseDirect, PhaseSelf} {
		p, err := fp.NewPlan(full.Tool, full.Patterns, byPhase[ph])
		if err != nil {
			return err
		}
		gp.Phases[ph] = p
	}
	for _, k := range gp.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=global-kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}
	deleter := inventory.NewDeleter(live, d.Deleters)
	gp.Direct, err = fp.Execute(ctx, gp.Phases[PhaseDirect], cfg.mode(), deleter, lease)
	t.logRow("global-direct", gp.Phases[PhaseDirect], gp.Direct)
	if err != nil && (cfg.Reap || errors.Is(err, lifecycle.ErrLeaseLost)) {
		return err
	}
	gp.Self, err = fp.Execute(ctx, gp.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	t.logRow("global-self", gp.Phases[PhaseSelf], gp.Self)
	if err != nil && (cfg.Reap || errors.Is(err, lifecycle.ErrLeaseLost)) {
		return err
	}
	sr := SelfRemoval{Region: cfg.Region, AccountID: cfg.AccountID}
	for _, a := range gp.Self.Actions {
		if a.Outcome == fp.OutcomeWouldDelete && a.Type == inventory.TypeRole && a.Name == runtimeiam.RoleName {
			sr.Role = true
		}
	}
	now := cfg.Now()
	gp.OneShots = sr.Plan(now)
	t.logOneShots(gp.OneShots)
	if cfg.Reap {
		if _, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, gp.OneShots, lease.Step); err != nil {
			return err
		}
	}
	return nil
}
