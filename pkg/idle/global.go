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

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The global teardown (lagotto#340). lagotto's account-wide resources (the
// poller role every region's poller runs as, lagotto-instance-role and its
// profile, and what the poller leaves for the CLI: the Scheduler invoke role
// and lagotto-cli-user) go once every region's teardown has finished, and
// exactly one region's poller removes them: the last one to finish.
//
// # Reap mode
//
// The account-wide decision runs under the lease on lagotto's global scope
// (the item tool#lagotto#global in ControlRegion's spore-host-lifecycle
// table), the mutex every region's teardown tail runs under:
//
//  1. Before it commits its own removal (the one-shot jobs that delete its
//     function, and the delete of its own rate schedule), a region's
//     teardown takes the global lease, waiting up to Config.GlobalWait while
//     another region holds it and retrying failed reads and writes. If it
//     cannot take it, it gives its regional lease back with nothing
//     committed: its poller is intact and tries again a grace period later.
//  2. Holding it, the region creates its jobs, deletes its rate schedule,
//     and finishes its regional lease (its lifecycle item is deleted).
//  3. Only then does it read the footprint again and every other enabled
//     region's lifecycle item. Each other region stands done (no lifecycle
//     item and no live poller of its own, or a stalled teardown whose
//     poller is already gone), pending (tearing down with a live lease, or
//     idle past its own grace with a live poller to tear it down), or active
//     (anything else: in use, within its grace, just deployed, held, a
//     stalled teardown its poller will resume, a poller from before the
//     lease, or anything that could not be read). Any pending or active
//     region keeps the globals: it runs this same step itself once it
//     finishes, and finds this region's item gone.
//  4. The last region plans the globals and, re-reading the other regions'
//     items just before each change (a deploy elsewhere may have started),
//     deletes lagotto-instance-role and its profile and creates the poller
//     role's one-shot jobs, under the global lease.
//  5. It finishes the global lease once the decision is clean: the globals
//     removed, or deferred to a region that will decide later. Otherwise (a
//     failed delete or job, a footprint or item that could not be read) it
//     leaves the global lease held. In 20 min the lease is stale: the next
//     region's step takes it over and decides again, and the CLI's lazy last
//     mile (libs#78) reports tool#lagotto#global as a teardown to resume,
//     with the account-wide resources it would remove. While the item
//     exists, the last mile also keeps the invoke role.
//
// Because every region's step runs under one lease, and a region finishes its
// own lifecycle item before it reads the others, of two regions in turn the
// first always sees the second's item and defers, and the second always sees
// the first's gone: exactly one removes the globals, however close together
// the two went idle.
//
// # Report mode
//
// Report mode writes nothing to the global scope: it takes no global lease
// and creates no table. It releases its regional lease (which puts the item
// back to ACTIVE and restarts the region's idle clock; the lifecycle item
// keeps no record of the release), then reads the others as above, with one
// difference: a pending region counts as done. So the first region to report
// lists the globals, and a region reporting after it finds it ACTIVE and does
// not. Without the mutex, two reports at exactly the same moment can both
// find the other already released, and neither lists them; never both.
// Regions that cross the grace hours apart list no global teardown in report
// mode; reap mode removes it in the last of them.
//
// Creators of the account-wide resources (setup, deploy) do not yet mark the
// global scope active (lagotto#345); the re-reads in step 4 are what fence the
// deletes against a deploy in another region meanwhile.

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
// region's step to give the global lease back. A step takes seconds.
const DefaultGlobalWait = 5 * time.Minute

// globalPoll is how often a waiting region tries the global lease again.
const globalPoll = 5 * time.Second

// readAttempts is how many times the global step tries a lifecycle read
// before it counts the region unreadable.
const readAttempts = 3

// Standing is how the global step judged another enabled region.
type Standing string

// Standings.
const (
	// StandingDone: nothing there that needs the globals, or will decide
	// them.
	StandingDone Standing = "done"
	// StandingPending: tearing down with a live lease, or idle past its
	// grace with a live poller; it runs the global step itself after its
	// own deletes.
	StandingPending Standing = "pending"
	// StandingActive: in use, within its grace, held, a stalled teardown
	// its poller will resume, or unreadable. It keeps the globals.
	StandingActive Standing = "active"
)

// OtherRegion is one other region's standing, and why.
type OtherRegion struct {
	Region   string
	Standing Standing
	Why      string
	// Unknown: active only because something could not be read.
	Unknown bool
}

// judgeOthers reads the lifecycle item of every region in f other than self,
// trying a failed read readAttempts times, and judges each with f's facts of
// that region.
func judgeOthers(ctx context.Context, lc *lifecycle.Client, f *inventory.Facts, self string, now time.Time,
	sleep func(context.Context, time.Duration) error) []OtherRegion {
	var out []OtherRegion
	for i := range f.Regions {
		rf := &f.Regions[i]
		if rf.Region == self {
			continue
		}
		var (
			it  lifecycle.Item
			ok  bool
			err error
		)
		for a := 0; a < readAttempts; a++ {
			if a > 0 {
				if serr := sleep(ctx, time.Second); serr != nil {
					break
				}
			}
			if it, ok, err = lc.Get(ctx, Tool, rf.Region); err == nil {
				break
			}
		}
		out = append(out, judge(rf, it, ok, err, lc.LeaseStale(it), now))
	}
	return out
}

// judge is one other region's standing from its lifecycle item (it, ok, err:
// lifecycle.Client.Get's answer; stale: its lease is stale) and its footprint
// facts rf.
func judge(rf *inventory.Region, it lifecycle.Item, ok bool, err error, stale bool, now time.Time) OtherRegion {
	o := OtherRegion{Region: rf.Region, Standing: StandingActive}
	live := pollerLive(rf)
	switch {
	case err != nil:
		o.Why, o.Unknown = "could not read its lifecycle item: "+err.Error(), true
	case rf.ActivityUnknown:
		o.Why, o.Unknown = strings.Join(rf.ActiveReasons(), "; "), true
	case rf.Active():
		o.Why = strings.Join(rf.ActiveReasons(), "; ")
	case ok && len(it.Holds) > 0:
		o.Why = fmt.Sprintf("held by %v", it.Holds)
	case ok && it.Lease != nil && stale && live:
		o.Why = "its teardown stalled (a stale lease held by " + it.Lease.Owner + "); its poller resumes it at its next run"
	case ok && it.Lease != nil && stale:
		o.Standing = StandingDone
		o.Why = "its teardown stalled (a stale lease held by " + it.Lease.Owner + ") after its poller was handed to its jobs; the last mile reports it to resume"
	case ok && it.Lease != nil:
		o.Standing, o.Why = StandingPending, "tearing down (lease held by "+it.Lease.Owner+")"
	case ok && it.State == lifecycle.StateIdle && !live && !lagottoLeft(rf):
		o.Standing, o.Why = StandingDone, "an idle lifecycle item, and nothing of lagotto's left"
	case ok && it.State == lifecycle.StateIdle && !live:
		o.Why = "idle, with no poller to tear it down ('lagotto teardown --plan' removes what is left)"
	case ok && it.State == lifecycle.StateIdle:
		grace := graceOf(rf)
		if idle := it.IdleFor(now); !it.IdleSince.IsZero() && idle >= grace {
			o.Standing, o.Why = StandingPending, fmt.Sprintf("idle for %s, past its %s grace", idle.Round(time.Minute), grace)
		} else {
			o.Why = fmt.Sprintf("idle for %s of its %s grace", idle.Round(time.Minute), grace)
		}
	case ok:
		o.Why = "state " + string(it.State)
	case live:
		o.Why = "its poller has no lifecycle item (a poller from before lagotto#326)"
	default:
		o.Standing, o.Why = StandingDone, "no lifecycle item"
		if rf.Function != nil {
			o.Why += "; its poller is handed to its one-shot job"
		}
	}
	return o
}

// pollerLive reports whether r has a poller that will run again: its
// function, not yet handed to its one-shot delete job, and its rate
// schedule.
func pollerLive(r *inventory.Region) bool {
	if r.Function == nil || functionJobPending(r) {
		return false
	}
	for _, s := range r.Schedules {
		if s.Name == deploy.PollerScheduleName {
			return true
		}
	}
	return false
}

// lagottoLeft reports whether anything of lagotto's is left in r.
func lagottoLeft(r *inventory.Region) bool {
	return len(r.Tables) > 0 || r.Bucket != nil || r.Topic != nil || r.Function != nil || r.LogGroup != nil ||
		len(r.Groups) > 0 || len(r.Schedules) > 0 || r.Stack != nil
}

// graceOf is the grace r's own poller waits: its LAGOTTO_IDLE_GRACE, read
// as that poller reads it.
func graceOf(r *inventory.Region) time.Duration {
	v := ""
	if r.Function != nil {
		v = r.Function.IdleGrace
	}
	g, _ := GraceFrom(func(string) string { return v })
	return g
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

// decideGlobal says whether this region removes the account-wide resources,
// given a footprint of every region and the other regions' standings, and
// why. unknown: the answer is no only because something could not be read.
func decideGlobal(f *inventory.Facts, others []OtherRegion, reap bool) (last bool, why string, unknown bool) {
	if !f.AllRegions {
		return false, "the footprint did not read every enabled region", true
	}
	if f.AccountIncomplete {
		return false, "the account-wide resources could not all be read", true
	}
	var active, pending []string
	unknown = true
	for _, o := range others {
		switch o.Standing {
		case StandingActive:
			active = append(active, o.Region+" ("+o.Why+")")
			unknown = unknown && o.Unknown
		case StandingPending:
			pending = append(pending, o.Region)
		}
	}
	sort.Strings(active)
	sort.Strings(pending)
	switch {
	case len(active) > 0:
		return false, "still active: " + strings.Join(active, ", "), unknown && len(pending) == 0
	case len(pending) > 0 && reap:
		return false, "finishing after this region, and the last of them removes the account-wide resources: " + strings.Join(pending, ", "), false
	case len(pending) > 0:
		return true, "report mode: also tearing down or idle past their grace, so this report stands for the last of them: " + strings.Join(pending, ", "), false
	}
	return true, "no other enabled region has lagotto in use or still to tear down", false
}

// acquireGlobal takes the global-scope lease, waiting while another region
// holds it, and trying again after any failure, until cfg.GlobalWait has
// passed or ctx is done.
func acquireGlobal(ctx context.Context, cfg Config, lc *lifecycle.Client) (*lifecycle.Lease, error) {
	tries := int(cfg.GlobalWait/globalPoll) + 1
	var last error
	for i := 0; i < tries; i++ {
		if i > 0 {
			if err := cfg.Sleep(ctx, globalPoll); err != nil {
				return nil, errors.Join(err, last)
			}
		}
		_, _, err := lc.Observe(ctx, Tool, lifecycle.ScopeGlobal, false)
		if err == nil {
			var l *lifecycle.Lease
			if l, err = lc.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err == nil {
				return l, nil
			}
		}
		if ctx.Err() != nil {
			return nil, err
		}
		last = err
	}
	return nil, fmt.Errorf("not taken in %s: %w", cfg.GlobalWait, last)
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

// GlobalLease is what became of the global lease.
type GlobalLease string

// The global lease's fates, as the summary logs them (global_lease=).
const (
	// GlobalNone: not taken (report mode).
	GlobalNone GlobalLease = "none"
	// GlobalFinished: the decision was clean, and the item is gone.
	GlobalFinished GlobalLease = "finished"
	// GlobalHeld: left held, to go stale and be resumed.
	GlobalHeld GlobalLease = "held"
)

// globalOutcome is what the global step did.
type globalOutcome struct {
	// endErr is the regional lease's Finish (reap) or Release (report) error.
	endErr error
	// lease is what became of the global lease.
	lease GlobalLease
	// err is a failure of the global step itself.
	err error
}

// global is the global step, run at the end of a region's teardown with its
// regional lease held, and in reap mode the global lease gl too. It ends the
// regional lease and records its decision in plan.
func (t *run) global(ctx context.Context, plan *Plan, gl *lifecycle.Lease) globalOutcome {
	cfg := t.cfg
	g := globalOutcome{lease: GlobalNone}
	if !cfg.Reap {
		g.endErr = t.lease.Release(ctx)
		if g.endErr != nil {
			plan.LastWhy = "could not release this region's lease: " + g.endErr.Error()
			return g
		}
		// Report mode only reads the global scope: a global teardown that
		// holds or owes the lease is a reap-mode poller's.
		it, ok, err := t.d.Lifecycle.Get(ctx, Tool, lifecycle.ScopeGlobal)
		switch {
		case err != nil:
			plan.LastWhy = "could not read the global teardown lease: " + err.Error()
		case ok && it.Lease != nil:
			plan.LastWhy = "a global teardown holds or owes the global lease (" + it.Lease.Owner + ")"
		default:
			_, g.err = t.decide(ctx, plan, nil)
		}
		return g
	}

	g.endErr = t.lease.Finish(ctx)
	if g.endErr != nil {
		plan.LastWhy = "could not finish this region's lease: " + g.endErr.Error()
		g.lease = t.finishGlobal(ctx, gl)
		return g
	}
	clean, err := t.decide(ctx, plan, gl)
	g.err = err
	if clean {
		g.lease = t.finishGlobal(ctx, gl)
		return g
	}
	g.lease = GlobalHeld
	cfg.Logf("idle-teardown %s region=%s phase=global lease=held scope=%s reason=%q", cfg.label(), cfg.Region, lifecycle.ScopeGlobal,
		"the account-wide decision did not complete; the lease goes stale in 20 min, then the next region's teardown takes it over and the CLI's last mile reports it to resume")
	return g
}

// finishGlobal finishes the global lease.
func (t *run) finishGlobal(ctx context.Context, gl *lifecycle.Lease) GlobalLease {
	if err := gl.Finish(ctx); err != nil {
		t.cfg.Logf("idle-teardown %s region=%s phase=global lease=finish-failed scope=%s error=%q", t.cfg.label(), t.cfg.Region, lifecycle.ScopeGlobal, err.Error())
		return GlobalHeld
	}
	return GlobalFinished
}

// decide reads the footprint again and the other regions' items, decides,
// and if this region is the last plans (and in reap mode removes) the
// account-wide resources under gl. clean reports that the decision is final:
// done, or deferred to a region that will decide later.
func (t *run) decide(ctx context.Context, plan *Plan, gl *lifecycle.Lease) (clean bool, err error) {
	cfg, d := t.cfg, t.d
	fresh, err := inventory.Gather(ctx, d.Source, cfg.AccountID, inventory.Scope{AllRegions: true}, cfg.Options)
	if err != nil {
		plan.LastWhy = "could not read the footprint again: " + err.Error()
		return false, err
	}
	plan.Others = judgeOthers(ctx, d.Lifecycle, &fresh, cfg.Region, cfg.Now(), cfg.Sleep)
	for _, o := range plan.Others {
		cfg.Logf("idle-teardown %s region=%s phase=global other_region=%s standing=%s why=%q",
			cfg.label(), cfg.Region, o.Region, o.Standing, o.Why)
	}
	last, why, unknown := decideGlobal(&fresh, plan.Others, cfg.Reap)
	plan.LastWhy = why
	if !last {
		return !unknown, nil
	}
	return t.globals(ctx, plan, &fresh, gl)
}

// fence re-reads the other regions' items before a change to the
// account-wide resources: a region that is no longer done (a deploy or a
// watch since the decision) stops it. clean: the stop is final, that region
// decides later.
func (t *run) fence(ctx context.Context, plan *Plan, fresh *inventory.Facts, before string) (ok, clean bool) {
	for _, o := range judgeOthers(ctx, t.d.Lifecycle, fresh, t.cfg.Region, t.cfg.Now(), t.cfg.Sleep) {
		if o.Standing != StandingDone {
			plan.Last = false
			plan.LastWhy = fmt.Sprintf("%s is no longer done before %s: %s (%s)", o.Region, before, o.Standing, o.Why)
			t.cfg.Logf("idle-teardown %s region=%s phase=global fence=stopped before=%q other_region=%s standing=%s why=%q",
				t.cfg.label(), t.cfg.Region, before, o.Region, o.Standing, o.Why)
			return false, !o.Unknown
		}
	}
	return true, true
}

// globals plans the account-wide teardown from fresh, a footprint read after
// this region's own deletes, and in reap mode executes it under gl. clean:
// as for decide.
func (t *run) globals(ctx context.Context, plan *Plan, fresh *inventory.Facts, gl *lifecycle.Lease) (clean bool, err error) {
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
		plan.LastWhy = "could not plan the account-wide teardown: " + err.Error()
		return false, err
	}
	gp := &GlobalPlan{Kept: kept, Phases: map[Phase]*fp.Plan{}}
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
			plan.LastWhy = "could not plan the account-wide teardown: " + err.Error()
			return false, err
		}
		gp.Phases[ph] = p
	}
	// Planned: this run is the one that removes them.
	plan.Last, plan.Global = true, gp
	for _, k := range gp.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=global-kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}
	deleter := inventory.NewDeleter(live, d.Deleters)
	if cfg.Reap {
		if ok, clean := t.fence(ctx, plan, fresh, "the account-wide deletes"); !ok {
			return clean, nil
		}
	}
	gp.Direct, err = fp.Execute(ctx, gp.Phases[PhaseDirect], cfg.mode(), deleter, gl)
	t.logRow("global-direct", gp.Phases[PhaseDirect], gp.Direct)
	if err != nil && (cfg.Reap || errors.Is(err, lifecycle.ErrLeaseLost)) {
		return false, err
	}
	gp.Self, err = fp.Execute(ctx, gp.Phases[PhaseSelf], fp.ModeReport, deleter, gl)
	t.logRow("global-self", gp.Phases[PhaseSelf], gp.Self)
	if err != nil && (cfg.Reap || errors.Is(err, lifecycle.ErrLeaseLost)) {
		return false, err
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
	if cfg.Reap && len(gp.OneShots) > 0 {
		if ok, clean := t.fence(ctx, plan, fresh, "the poller role's jobs"); !ok {
			gp.OneShots = nil
			return clean, nil
		}
		if _, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, gp.OneShots, gl.Step); err != nil {
			return false, err
		}
	}
	return true, nil
}
