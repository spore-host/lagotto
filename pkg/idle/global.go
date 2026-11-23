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
// # The invariant (reap mode)
//
// A region's teardown takes the global lease (the item tool#lagotto#global
// in ControlRegion's spore-host-lifecycle table) BEFORE its first delete, and
// holds it until the account-wide decision is durable. The global item is
// deleted ONLY when
//
//   - the account-wide resources are gone (this region removed them and
//     handed the poller role to its jobs), or
//   - a region with a live poller still owes them: this region itself,
//     stopped before it committed its own removal, or another region whose
//     poller will run this same step when it tears down.
//
// Every other exit leaves the global lease held. It goes stale after 20 min;
// the next region's teardown then takes it over and decides again, and the
// CLI's lazy last mile (libs#78) reports tool#lagotto#global as a teardown to
// resume (and keeps the invoke role while the item exists).
//
// A region "commits its own removal" when it creates the one-shot job that
// deletes its function, or starts deleting its own rate schedule: from then
// on its poller will not run again. After that point it never gives its
// regional lease back to ACTIVE; a failure leaves it held, and a stale
// teardown whose poller is gone counts as done.
//
// # The decision
//
// Holding the global lease (taken before its first delete), the region reads
// the footprint again and every other enabled region's item, and judges each
// other region done (nothing there needs or will decide the globals),
// pending (its own poller holds its lease, or it is idle past its own grace,
// with a live poller either way), or active (anything else, including
// anything that could not be read). A non-done region with a live poller of
// its own owes the globals. The decision is last (every other region done:
// this one removes them), deferring (a region owes them), or neither (this
// region keeps its poller and decides again later). Then, in order: both
// plans; the account-wide deletes when last, each re-checked against the
// other regions; its own direct deletes; the commit (its one-shot jobs, the
// poller role's first and its function's last, every one due after the
// poller's next hourly run), fenced before and after; the global item, once
// the decision is durable; its own schedule and group; its lease. Because
// one lease serializes the regions' decisions, and a region's item shows it
// committed (its poller is handed to its job) before the lease is given up,
// of two regions the first always defers and the second always finds the
// first done: exactly one removes the globals.
//
// A region whose function's job is already pending (an earlier run
// committed, then stopped) finishes that commit: once another region is
// removing the account-wide resources it may have judged this one done, so
// this one never goes back to keeping its poller.
//
// Before each change to the account-wide resources the last region re-reads
// the other regions' items (a deploy elsewhere since the decision stops it;
// lagotto#345 will have setup and deploy mark the global scope themselves).
//
// The CLI's `lagotto teardown --plan --mode reap` does not join this decision
// (lagotto#346): a region whose lease the CLI holds counts as active and
// owes nothing, so the pollers keep their own.
//
// # Report mode
//
// Report mode writes nothing to the global scope: it takes no global lease
// and creates no table. It releases its regional lease (the item goes back to
// ACTIVE and the idle clock restarts; the item keeps no record of the
// release), then reads the others as above, with one difference: a pending
// region counts as done. So the first region to report lists the globals,
// and a region reporting after it finds it ACTIVE and does not. Two reports
// at exactly the same moment can each find the other already released, and
// then neither lists them; never both. Regions that cross the grace hours
// apart list no global teardown in report mode; reap mode removes it in the
// last of them.

// ControlRegion is the region whose spore-host-lifecycle table holds lagotto's
// global-scope lifecycle item: the commercial partition's IAM home region,
// us-east-1, so that every region's poller and every CLI read the same one.
// An empty region is the commercial partition's.
//
// Any other partition is an error, so the global teardown fails closed there:
// the ISO partitions and the European Sovereign Cloud cannot reach us-east-1,
// and in aws-us-gov and aws-cn lagotto's runtime policy cannot reach any
// lifecycle table at all (runtimeiam.RegionalARN names the aws partition
// only; lagotto#347).
func ControlRegion(region string) (string, error) {
	switch {
	case region == "":
		return "us-east-1", nil
	case strings.Contains(region, "-iso"), strings.HasPrefix(region, "eusc-"):
		return "", fmt.Errorf("no control region for %s: lagotto's global teardown lease works only in the aws partition", region)
	case strings.HasPrefix(region, "us-gov-"), strings.HasPrefix(region, "cn-"):
		return "", fmt.Errorf("no control region for %s: lagotto's runtime policy names only the aws partition's lifecycle tables (lagotto#347)", region)
	}
	for _, p := range []string{"us-", "eu-", "ap-", "ca-", "sa-", "me-", "af-", "il-", "mx-"} {
		if strings.HasPrefix(region, p) {
			return "us-east-1", nil
		}
	}
	return "", fmt.Errorf("no control region for %s: lagotto's global teardown lease works only in the aws partition", region)
}

// PollerOwner is the lease owner a region's poller writes: the only owner
// whose regional lease means "this region decides the globals itself".
func PollerOwner(region string) string { return "lagotto-poller/" + region }

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
	// Exists, Epoch and State are its lifecycle item as read: the fence
	// compares them.
	Exists bool
	Epoch  int64
	State  lifecycle.State
	// Owes: it is not done, and its own live poller will run the global
	// step when it tears down (it has a lifecycle item, its activity was
	// read, and no one but its poller holds its lease). Deferring to such a
	// region is final; to any other, the global lease stays held.
	Owes bool
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
	o := OtherRegion{Region: rf.Region, Standing: StandingActive, Exists: ok, Epoch: it.Epoch, State: it.State}
	live := pollerLive(rf)
	owner := PollerOwner(rf.Region)
	switch {
	case err != nil:
		o.Why = "could not read its lifecycle item: " + err.Error()
	case rf.ActivityUnknown, rf.Active():
		o.Why = strings.Join(rf.ActiveReasons(), "; ")
	case ok && len(it.Holds) > 0:
		o.Why = fmt.Sprintf("held by %v", it.Holds)
	case ok && it.Lease != nil && functionJobPending(rf):
		// Its poller is handed to its jobs: that run decided, and committed
		// its removal; what is left of it the last mile reports.
		o.Standing, o.Why = StandingDone, "its poller committed its own removal (lease held by "+it.Lease.Owner+")"
	case ok && it.Lease != nil && live && stale:
		o.Why = "its teardown stalled (a stale lease held by " + it.Lease.Owner + "); its poller resumes it at its next run"
	case ok && it.Lease != nil && live && it.Lease.Owner == owner:
		o.Standing, o.Why = StandingPending, "its poller is tearing it down"
	case ok && it.Lease != nil && live:
		o.Why = "a teardown by " + it.Lease.Owner + " holds its lease; only its own poller decides the account-wide resources"
	case ok && it.Lease != nil && needsGlobals(rf):
		o.Why = "a lease held by " + it.Lease.Owner + ", and a poller that needs the poller role but will not run to finish it"
	case ok && it.Lease != nil:
		o.Standing, o.Why = StandingDone, "a lease held by "+it.Lease.Owner+", and nothing there that needs the account-wide resources; the last mile reports it"
	case ok && it.State == lifecycle.StateActive:
		// Whatever the footprint says: a creator marks a region active
		// before it creates anything.
		o.Why = "state ACTIVE"
	case ok && !live && !needsGlobals(rf):
		o.Standing, o.Why = StandingDone, "a lifecycle item (state "+string(it.State)+"), and nothing there that needs the account-wide resources"
	case ok && !live:
		o.Why = "state " + string(it.State) + ", with a poller that will not run to tear it down ('lagotto teardown --plan' removes what is left)"
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
	case needsGlobals(rf):
		// A poller that will run once a watch arms its schedule needs the
		// poller role whatever the schedule's state (a region from before
		// lagotto#326, or deployed and never watched, lagotto#348).
		o.Why = "its poller has no lifecycle item, and its schedule is not enabled (a poller from before lagotto#326, or never watched)"
	default:
		o.Standing, o.Why = StandingDone, "no lifecycle item"
		if functionJobPending(rf) {
			o.Why += "; its poller is handed to its one-shot job"
		}
	}
	o.Owes = o.Standing != StandingDone && err == nil && !rf.ActivityUnknown && ok && live &&
		(it.Lease == nil || it.Lease.Owner == owner)
	return o
}

// pollerLive reports whether r has a poller that will run again: its
// function, not yet handed to its one-shot delete job, and its rate schedule
// ENABLED. `lagotto deploy` creates the schedule DISABLED, and only a watch
// enables it (idle mode keeps it ENABLED, at rate(1h)), so a region deployed
// and never watched has no poller that will ever run.
func pollerLive(r *inventory.Region) bool {
	if r.Function == nil || functionJobPending(r) {
		return false
	}
	for _, s := range r.Schedules {
		if s.Name == deploy.PollerScheduleName && s.State == "ENABLED" {
			return true
		}
	}
	return false
}

// needsGlobals reports whether r has a poller function, not handed to its
// delete job, that runs as the poller role (now, or once a watch arms its
// schedule). Tables and the rest of a region need none of the account-wide
// resources.
func needsGlobals(r *inventory.Region) bool {
	return r.Function != nil && !functionJobPending(r)
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
// why. owed: the answer is no, and a region that keeps them owes them (see
// OtherRegion.Owes), so deferring to it is final.
func decideGlobal(f *inventory.Facts, others []OtherRegion, reap bool) (last bool, why string, owed bool) {
	if !f.AllRegions {
		return false, "the footprint did not read every enabled region", false
	}
	if f.AccountIncomplete {
		return false, "the account-wide resources could not all be read", false
	}
	var active, pending []string
	for _, o := range others {
		switch o.Standing {
		case StandingActive:
			active = append(active, o.Region+" ("+o.Why+")")
		case StandingPending:
			pending = append(pending, o.Region)
		}
		owed = owed || o.Owes
	}
	sort.Strings(active)
	sort.Strings(pending)
	switch {
	case len(active) > 0:
		return false, "still active: " + strings.Join(active, ", "), owed
	case len(pending) > 0 && reap:
		return false, "finishing after this region, and the last of them removes the account-wide resources: " + strings.Join(pending, ", "), owed
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
		if errors.Is(err, lifecycle.ErrTableClosing) {
			// A closer that died left its sentinel, which Observe never
			// clears (libs#88): CloseTableIfEmpty takes a stale one over (and leaves
			// a live closer's alone). The next try observes again.
			if control, cerr := lc.RegionFor(lifecycle.ScopeGlobal); cerr == nil {
				_, _ = lc.CloseTableIfEmpty(ctx, control)
			}
		}
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

	live    *fp.Report
	deleter fp.Deleter
}

// GlobalLease is what became of the global lease.
type GlobalLease string

// The global lease's fates, as the summary logs them (global_lease=).
const (
	// GlobalNone: not taken (report mode, or no control region).
	GlobalNone GlobalLease = "none"
	// GlobalFinished: the item is gone.
	GlobalFinished GlobalLease = "finished"
	// GlobalHeld: left held, to go stale and be taken over.
	GlobalHeld GlobalLease = "held"
)

// finishGlobal finishes the global lease, which deletes the global item, and
// closes the control region's lifecycle table if nothing is left in it
// (acquireGlobal's Observe may have created it). A lease another region took
// over (it went stale) stays that region's.
func (t *run) finishGlobal(ctx context.Context, gl *lifecycle.Lease) GlobalLease {
	if gl == nil {
		return GlobalNone
	}
	if err := t.retry(ctx, func() error { return gl.Finish(ctx) }); err != nil {
		t.cfg.Logf("idle-teardown %s region=%s phase=global lease=finish-failed scope=%s error=%q", t.cfg.label(), t.cfg.Region, lifecycle.ScopeGlobal, err.Error())
		return GlobalHeld
	}
	if control, err := ControlRegion(t.cfg.Region); err == nil {
		t.closeTable(ctx, control)
	}
	return GlobalFinished
}

// judgeAll reads every other region's item, judges it with fresh's facts,
// and logs each.
func (t *run) judgeAll(ctx context.Context, fresh *inventory.Facts) []OtherRegion {
	others := judgeOthers(ctx, t.d.Lifecycle, fresh, t.cfg.Region, t.cfg.Now(), t.cfg.Sleep)
	for _, o := range others {
		t.cfg.Logf("idle-teardown %s region=%s phase=global other_region=%s standing=%s why=%q",
			t.cfg.label(), t.cfg.Region, o.Region, o.Standing, o.Why)
	}
	return others
}

// fence re-reads the other regions' items before a change that depends on
// the decision, and reports whether every one is as it was judged: the same
// standing, and the same item (present or not, at the same epoch). A deploy
// or a watch since then (an epoch moved, an item appeared) stops it, even
// before it has created anything the footprint would show; so does a
// region whose standing changed.
func (t *run) fence(ctx context.Context, plan *Plan, fresh *inventory.Facts, before string) bool {
	was := map[string]OtherRegion{}
	for _, o := range plan.Others {
		was[o.Region] = o
	}
	for _, o := range judgeOthers(ctx, t.d.Lifecycle, fresh, t.cfg.Region, t.cfg.Now(), t.cfg.Sleep) {
		w := was[o.Region]
		if o.Standing == w.Standing && o.Exists == w.Exists && o.Epoch == w.Epoch {
			continue // a pending region that has since claimed its lease is still pending
		}
		plan.LastWhy = fmt.Sprintf("%s changed before %s: %s (%s)", o.Region, before, o.Standing, o.Why)
		t.cfg.Logf("idle-teardown %s region=%s phase=global fence=stopped before=%q other_region=%s standing=%s why=%q",
			t.cfg.label(), t.cfg.Region, before, o.Region, o.Standing, o.Why)
		return false
	}
	return true
}

// fencedDeleter runs the fence (run.fence) before each account-wide
// delete's re-check, and heartbeats the regional lease immediately before
// the delete: a region that changed since the decision, or a creator that
// took this region's lease over, stops the deletes at once. (footprint.Execute
// heartbeats the global lease itself, between the two.)
type fencedDeleter struct {
	fp.Deleter
	fence func(ctx context.Context) bool
	lease *lifecycle.Lease
}

// errFenced is a fenced delete's refusal.
var errFenced = errors.New("another region changed since the account-wide decision")

func (f fencedDeleter) Describe(ctx context.Context, r fp.Resource) (fp.Resource, error) {
	if !f.fence(ctx) {
		return fp.Resource{}, errFenced
	}
	return f.Deleter.Describe(ctx, r)
}

// Delete heartbeats the regional lease last, immediately before the delete.
func (f fencedDeleter) Delete(ctx context.Context, r fp.Resource) error {
	if err := f.lease.Step(ctx, "delete "+r.Type+" "+r.ARN); err != nil {
		return err
	}
	return f.Deleter.Delete(ctx, r)
}

// planGlobals plans the account-wide teardown from fresh, a footprint read
// with the global lease held, and logs what it keeps.
func (t *run) planGlobals(fresh *inventory.Facts) (*GlobalPlan, error) {
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
		return nil, err
	}
	gp := &GlobalPlan{Kept: kept, Phases: map[Phase]*fp.Plan{}, live: live, deleter: inventory.NewDeleter(live, d.Deleters)}
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
			return nil, err
		}
		gp.Phases[ph] = p
	}
	for _, k := range gp.Kept {
		cfg.Logf("idle-teardown %s region=%s phase=global-kept outcome=kept type=%s resource_region=%s name=%q arn=%s reason=%q",
			cfg.label(), cfg.Region, k.Resource.Type, k.Resource.Region, k.Resource.Name, k.Resource.ARN, k.Reason)
	}
	return gp, nil
}

// reportGlobal is report mode's account-wide step, after it has released its
// regional lease: it reads the global item (and writes nothing there), the
// footprint and the other regions, decides as reap mode would (a pending
// region counting as done), and plans the account-wide teardown if this is
// the region that removes it.
func (t *run) reportGlobal(ctx context.Context, plan *Plan) error {
	it, ok, err := t.d.Lifecycle.Get(ctx, Tool, lifecycle.ScopeGlobal)
	switch {
	case err != nil:
		plan.LastWhy = "could not read the global teardown lease: " + err.Error()
		return nil
	case ok && it.Lease != nil:
		plan.LastWhy = "a global teardown holds or owes the global lease (" + it.Lease.Owner + ")"
		return nil
	}
	fresh, err := inventory.Gather(ctx, t.d.Source, t.cfg.AccountID, inventory.Scope{AllRegions: true}, t.cfg.Options)
	if err != nil {
		plan.LastWhy = "could not read the footprint again: " + err.Error()
		return err
	}
	plan.Others = t.judgeAll(ctx, &fresh)
	last, why, _ := decideGlobal(&fresh, plan.Others, false)
	plan.LastWhy = why
	if !last {
		return nil
	}
	gp, err := t.planGlobals(&fresh)
	if err != nil {
		plan.LastWhy = "could not plan the account-wide teardown: " + err.Error()
		return err
	}
	plan.Last, plan.Global = true, gp
	gp.Direct, err = fp.Execute(ctx, gp.Phases[PhaseDirect], fp.ModeReport, gp.deleter, nil)
	t.logRow("global-direct", gp.Phases[PhaseDirect], gp.Direct)
	if err != nil {
		return err
	}
	gp.Self, err = fp.Execute(ctx, gp.Phases[PhaseSelf], fp.ModeReport, gp.deleter, nil)
	t.logRow("global-self", gp.Phases[PhaseSelf], gp.Self)
	if err != nil {
		return err
	}
	if roleWouldGo(gp.Self) {
		gp.OneShots = SelfRemoval{Region: t.cfg.Region, AccountID: t.cfg.AccountID, Role: true}.Plan(t.cfg.Now())
		t.logOneShots(gp.OneShots)
	}
	return nil
}

// roleWouldGo reports whether the poller role passed the delete guard.
func roleWouldGo(r *fp.Result) bool {
	if r == nil {
		return false
	}
	for _, a := range r.Actions {
		if a.Outcome == fp.OutcomeWouldDelete && a.Type == inventory.TypeRole && a.Name == runtimeiam.RoleName {
			return true
		}
	}
	return false
}
