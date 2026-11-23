package idle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The account-wide teardown (lagotto#340, #345, #346). lagotto's account-wide
// resources (the poller role, the Scheduler invoke role, lagotto-instance-role
// and its profile, the profiles setup made for authorized instance roles, and
// lagotto-cli-user when nothing is attached to it) are removed by exactly one
// kind of actor: the CLI, from its lazy last mile or from `lagotto teardown`.
// The pollers never touch them; each removes only its own region.
//
// The actor works under the lease on lagotto's global scope (the item
// tool#lagotto#global in ControlRegion's spore-host-lifecycle table), and may
// delete only while all of these hold, re-checked before each delete:
//
//   - no region has a poller function;
//   - no lagotto-teardown one-shot job is pending in any region (they run as
//     the Scheduler invoke role);
//   - no region's lagotto lifecycle item is ACTIVE or held (a live lease),
//     and none is IDLE where something of lagotto's is left;
//   - the global item's epoch has not moved since the decision: every
//     creator of something account-wide (setup, deploy, watch, launch)
//     marks the global scope active first (MarkActive), which takes the
//     actor's lease away, so its next heartbeat fails and it stops.
//
// A read that fails, or a footprint that is not complete, means "don't". Any
// failure while deleting leaves the global lease held: it goes stale in
// 20 min, and the next CLI run takes it over and carries on. On success it
// finishes the global lease (the item is deleted) and closes the control
// region's table if nothing is left in it.
//
// The global scope has its own grace (AccountWideConfig.Grace): the actor
// records the scope idle (Observe) while none of the conditions blocks, and
// claims only once it has been idle that long, so a `lagotto setup` that
// deploys no poller is not undone by the next command's last mile. An
// explicit `lagotto teardown` uses no grace.
//
// A regional item whose lease went stale (its poller died, or the CLI's
// teardown did) or that is IDLE, in a region with no poller and no job left,
// is dead: nothing will carry it on. In reap mode the actor finishes it and
// closes that region's table; anything else of lagotto's left there (a
// teardown that stopped partway) needs nothing account-wide, and is
// 'lagotto teardown --plan''s to remove.

// AccountWideConfig is one run of the account-wide teardown.
type AccountWideConfig struct {
	AccountID string
	// Owner is the lifecycle client's owner, to recognise this run's own
	// global lease after a claim whose answer was lost.
	Owner string
	// Reap deletes. False is report mode: it reads, plans, and writes
	// nothing.
	Reap bool
	// Grace is how long the global scope must have been idle before the
	// resources go: DefaultGrace from the last mile, 0 from an explicit
	// `lagotto teardown`.
	Grace time.Duration
	// Only, when set, limits the deletes to these ARNs (`teardown --plan`).
	Only map[string]bool
	// Wait bounds how long it waits for another actor's global lease; the
	// context's deadline bounds the whole run. Default DefaultGlobalWait.
	Wait    time.Duration
	Options inventory.Options
	Now     func() time.Time
	Logf    func(format string, args ...any)
	Sleep   func(ctx context.Context, d time.Duration) error

	// sequential reads the regions one at a time (the model check's
	// scheduler needs one step at a time per actor).
	sequential bool
}

// DefaultGlobalWait is how long the account-wide teardown waits for another
// actor's global lease.
const DefaultGlobalWait = 2 * time.Minute

func (c AccountWideConfig) withDefaults() AccountWideConfig {
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	if c.Sleep == nil {
		c.Sleep = sleep
	}
	if c.Wait <= 0 {
		c.Wait = DefaultGlobalWait
	}
	return c
}

func (c AccountWideConfig) label() string {
	if c.Reap {
		return "REAP"
	}
	return "REPORT"
}

// AccountWideDeps are what the account-wide teardown reads and deletes with.
type AccountWideDeps struct {
	// Lifecycle needs a ControlRegion; without one the run is skipped.
	Lifecycle *lifecycle.Client
	// Source reads the footprint, and before each delete every region's
	// poller and one-shot jobs. Report mode with Regions and Gather set
	// needs none.
	Source inventory.Source
	// Regions, when set, are the account's enabled regions (the last
	// mile's); otherwise Source lists them.
	Regions []string
	// Gather, when set, is the first footprint of every region (the last
	// mile's, read once per run). The footprint read under the global lease
	// is always a fresh one from Source.
	Gather func(ctx context.Context) (inventory.Facts, error)
	// Deleters: only Account is used.
	Deleters inventory.Deleters
}

// AccountWideOutcome is what a run decided.
type AccountWideOutcome string

// Outcomes.
const (
	// AccountWideSkipped: no global scope (outside the aws partition).
	AccountWideSkipped AccountWideOutcome = "skipped"
	// AccountWideNothing: nothing account-wide is left, and no global item.
	AccountWideNothing AccountWideOutcome = "nothing"
	// AccountWideBlocked: a condition holds the resources (Blockers).
	AccountWideBlocked AccountWideOutcome = "blocked"
	// AccountWideWaiting: the global scope's grace has not passed, or
	// another actor holds the global lease.
	AccountWideWaiting AccountWideOutcome = "waiting"
	// AccountWideWould: report mode: the resources would go now.
	AccountWideWould AccountWideOutcome = "would-remove"
	// AccountWideRemoved: reap mode removed them, and the global item.
	AccountWideRemoved AccountWideOutcome = "removed"
	// AccountWideHeld: reap mode stopped partway; the global lease is left
	// held for the next run.
	AccountWideHeld AccountWideOutcome = "held"
	// AccountWideLeaseLost: a creator marked the global scope active while
	// reap mode held its lease (the epoch moved); it stopped.
	AccountWideLeaseLost AccountWideOutcome = "lease-lost"
	// AccountWideFailed: a read failed before anything was decided.
	AccountWideFailed AccountWideOutcome = "failed"
)

// AccountWideResult is what one run did.
type AccountWideResult struct {
	Outcome  AccountWideOutcome
	Why      string
	Blockers []string
	// Plan and Result are the deletes planned and run (report mode: re-checked
	// only).
	Plan   *fp.Plan
	Result *fp.Result
	Err    error
}

// TearDownAccountWide is one run of the account-wide teardown.
func TearDownAccountWide(ctx context.Context, cfg AccountWideConfig, d AccountWideDeps) AccountWideResult {
	cfg = cfg.withDefaults()
	lc := d.Lifecycle
	log := func(r AccountWideResult) AccountWideResult {
		line := fmt.Sprintf("account-wide %s outcome=%s why=%q", cfg.label(), r.Outcome, r.Why)
		if len(r.Blockers) > 0 {
			line += fmt.Sprintf(" blockers=%q", strings.Join(r.Blockers, "; "))
		}
		if r.Err != nil {
			line += fmt.Sprintf(" error=%q", r.Err.Error())
		}
		cfg.Logf("%s", line)
		return r
	}
	control, err := lc.RegionFor(lifecycle.ScopeGlobal)
	if err != nil {
		return log(AccountWideResult{Outcome: AccountWideSkipped, Why: err.Error()})
	}
	g, gok, err := lc.Get(ctx, Tool, lifecycle.ScopeGlobal)
	if err != nil {
		return log(AccountWideResult{Outcome: AccountWideFailed, Why: "could not read the global item", Err: err})
	}
	if gok && g.Lease != nil && !lc.LeaseStale(g) {
		return log(AccountWideResult{Outcome: AccountWideWaiting, Why: "an account-wide teardown holds the global lease (" + g.Lease.Owner + ")"})
	}
	blocked := func(blockers []string) AccountWideResult {
		if cfg.Reap && gok && g.State == lifecycle.StateIdle {
			// In use again: back to ACTIVE, and the grace starts over once
			// nothing blocks.
			if _, _, err := lc.Observe(ctx, Tool, lifecycle.ScopeGlobal, true); err != nil {
				cfg.Logf("account-wide %s observe=failed error=%q", cfg.label(), err.Error())
			}
		}
		return log(AccountWideResult{Outcome: AccountWideBlocked, Why: "the account-wide resources are still needed", Blockers: blockers})
	}

	// The regions' lifecycle items first: while lagotto is in use one of
	// them says so, and no footprint is needed.
	regions := d.Regions
	if regions == nil {
		if regions, err = d.Source.EnabledRegions(ctx); err != nil {
			return log(AccountWideResult{Outcome: AccountWideFailed, Why: "could not list the account's regions", Err: err})
		}
	}
	if b := itemBlockers(regionChecks(ctx, cfg, d, regions, false)); len(b) > 0 {
		return blocked(b)
	}
	gather := d.Gather
	if gather == nil {
		gather = func(ctx context.Context) (inventory.Facts, error) { return gatherAll(ctx, cfg, d) }
	}
	facts, err := gather(ctx)
	if err != nil {
		return log(AccountWideResult{Outcome: AccountWideFailed, Why: "could not read the footprint", Err: err})
	}
	blockers, dead := accountWideBlockers(ctx, cfg, d, facts)
	planned := accountWideResources(facts, cfg)
	if len(planned) == 0 && !gok && len(dead) == 0 && len(blockers) == 0 {
		if cfg.Reap {
			// A run that finished the global item but not the close.
			if items, exists, err := lc.ListItems(ctx, control); err == nil && exists && len(items) == 0 {
				if _, err := lc.CloseTableIfEmpty(ctx, control); err != nil {
					cfg.Logf("account-wide %s close_table=failed region=%s error=%q", cfg.label(), control, err.Error())
				}
			}
		}
		return log(AccountWideResult{Outcome: AccountWideNothing, Why: "nothing account-wide is left"})
	}
	if len(blockers) > 0 {
		return blocked(blockers)
	}

	// A stale global lease is a run that stopped partway, past its grace: the
	// next run resumes it at once.
	resume := gok && g.Lease != nil
	if resume {
		cfg.Grace = 0
	}
	if !cfg.Reap {
		// Report mode writes nothing: it says what reap mode would do.
		if cfg.Grace > 0 && (!gok || g.State != lifecycle.StateIdle || g.IdleSince.IsZero() || g.IdleFor(cfg.Now()) < cfg.Grace) {
			why := "the global scope's grace has not started (reap mode would start it now)"
			if gok && g.State == lifecycle.StateIdle && !g.IdleSince.IsZero() {
				why = fmt.Sprintf("the global scope has been idle for %s of its %s grace", g.IdleFor(cfg.Now()).Round(time.Minute), cfg.Grace)
			}
			return log(AccountWideResult{Outcome: AccountWideWaiting, Why: why})
		}
		for _, r := range dead {
			cfg.Logf("account-wide %s dead_item=would-finish region=%s", cfg.label(), r)
		}
		plan, res, err := accountWidePlan(ctx, cfg, d, facts, planned, nil)
		return log(AccountWideResult{Outcome: AccountWideWould, Why: "nothing needs them", Plan: plan, Result: res, Err: err})
	}

	// Reap: the global scope idle, its lease, then everything again under it.
	if err := retryOp(ctx, Config{Sleep: cfg.Sleep}, func() error {
		_, _, err := lc.Observe(ctx, Tool, lifecycle.ScopeGlobal, false)
		if errors.Is(err, lifecycle.ErrTableClosing) {
			_, _ = lc.CloseTableIfEmpty(ctx, control) // a closer that died (libs#88)
		}
		return err
	}); err != nil {
		return log(AccountWideResult{Outcome: AccountWideFailed, Why: "could not record the global scope idle", Err: err})
	}
	gl, err := claimGlobal(ctx, cfg, lc)
	if err != nil {
		out := AccountWideFailed
		if errors.Is(err, lifecycle.ErrNotClaimable) {
			out = AccountWideWaiting
		}
		return log(AccountWideResult{Outcome: out, Why: "the global lease was not taken", Err: err})
	}
	cfg.Logf("account-wide %s claim=ok lease=%s scope=%s", cfg.label(), shortToken(gl.Token()), lifecycle.ScopeGlobal)
	held := func(why string, err error, plan *fp.Plan, res *fp.Result) AccountWideResult {
		if errors.Is(err, lifecycle.ErrLeaseLost) {
			return log(AccountWideResult{Outcome: AccountWideLeaseLost, Why: why + ": a creator marked lagotto active; stopped", Err: err, Plan: plan, Result: res})
		}
		cfg.Logf("account-wide %s lease=held reason=%q", cfg.label(), why+"; the global lease goes stale in 20 min, and the next run takes it over")
		return log(AccountWideResult{Outcome: AccountWideHeld, Why: why, Err: err, Plan: plan, Result: res})
	}
	facts, err = gatherAll(ctx, cfg, d)
	if err != nil {
		return held("could not read the footprint under the global lease", err, nil, nil)
	}
	if blockers, dead = accountWideBlockers(ctx, cfg, d, facts); len(blockers) > 0 {
		// Something appeared since: the lease goes back (ACTIVE), and the
		// grace starts over.
		if err := gl.Release(ctx); err != nil {
			return held("could not give the global lease back", err, nil, nil)
		}
		return log(AccountWideResult{Outcome: AccountWideBlocked, Why: "the account-wide resources are needed again", Blockers: blockers})
	}
	for _, r := range dead {
		if err := finishDeadRegion(ctx, cfg, lc, r); err != nil {
			return held("could not finish "+r+"'s dead lifecycle item", err, nil, nil)
		}
	}
	plan, res, err := accountWidePlan(ctx, cfg, d, facts, accountWideResources(facts, cfg), gl)
	if err != nil || res.Count(fp.OutcomeFailed)+res.Count(fp.OutcomeStopped)+res.Count(fp.OutcomeNotRun) > 0 {
		if err == nil {
			err = errors.New("a delete did not complete")
		}
		return held("a delete did not complete", err, plan, res)
	}
	if err := retryOp(ctx, Config{Sleep: cfg.Sleep}, func() error {
		if err := gl.Finish(ctx); errors.Is(err, lifecycle.ErrLeaseLost) {
			return &final{err}
		} else {
			return err
		}
	}); err != nil {
		return held("could not finish the global lease", err, plan, res)
	}
	if _, err := lc.CloseTableIfEmpty(ctx, control); err != nil {
		cfg.Logf("account-wide %s close_table=failed region=%s error=%q", cfg.label(), control, err.Error())
	}
	return log(AccountWideResult{Outcome: AccountWideRemoved, Why: "nothing needed them", Plan: plan, Result: res})
}

func gatherAll(ctx context.Context, cfg AccountWideConfig, d AccountWideDeps) (inventory.Facts, error) {
	return inventory.Gather(ctx, d.Source, cfg.AccountID, inventory.Scope{AllRegions: true}, cfg.Options)
}

// claimGlobal claims the global lease, waiting while another actor holds it
// or the claim is throttled, for at most cfg.Wait by cfg.Now's clock and
// never past ctx's deadline. Any other failure is not tried again (the claim
// may have been applied); after any failure a live lease in cfg.Owner's
// name is recognised as this run's own claim whose answer was lost, and the
// run stops rather than wait for it.
func claimGlobal(ctx context.Context, cfg AccountWideConfig, lc *lifecycle.Client) (*lifecycle.Lease, error) {
	start := cfg.Now()
	for polls := 0; ; polls++ {
		l, err := lc.Claim(ctx, Tool, lifecycle.ScopeGlobal, cfg.Grace)
		if err == nil {
			return l, nil
		}
		if cfg.Owner != "" && ownLease(ctx, lc, lifecycle.ScopeGlobal, cfg.Owner) {
			return nil, fmt.Errorf("%w (%s)", err, ownLeaseNote)
		}
		var nc *lifecycle.NotClaimableError
		held := errors.As(err, &nc) && strings.HasPrefix(nc.Reason, "lease held by")
		if !held && !isThrottle(err) {
			return nil, err // the grace, or a failure: not something to wait out
		}
		// Polls are counted as well as timed, so a clock that stalls cannot
		// make the wait endless.
		if max(cfg.Now().Sub(start), time.Duration(polls)*globalPoll) >= cfg.Wait {
			return nil, fmt.Errorf("the global lease was not free in %s: %w", cfg.Wait, err)
		}
		if d, ok := ctx.Deadline(); ok && time.Until(d) < globalPoll {
			return nil, fmt.Errorf("the global lease was not free within the run's time: %w", err)
		}
		if err := cfg.Sleep(ctx, globalPoll); err != nil {
			return nil, err
		}
	}
}

// globalPoll is how often claimGlobal tries again.
const globalPoll = 5 * time.Second

// regionCheck is one region's state as the account-wide teardown judges it.
type regionCheck struct {
	region string
	item   lifecycle.Item
	hasItm bool
	// itemErr: the item could not be read.
	itemErr error
	// live: read again just now (fresh), the poller and the one-shot jobs.
	live    bool
	poller  bool
	jobs    []string
	liveErr error
	// staleLease: the item's lease has gone stale.
	staleLease bool
}

// regionChecks reads every region's lagotto item, and with live each
// region's poller and one-shot jobs too, a few regions at a time.
func regionChecks(ctx context.Context, cfg AccountWideConfig, d AccountWideDeps, regions []string, live bool) []regionCheck {
	check := func(region string) regionCheck {
		c := regionCheck{region: region, live: live}
		c.itemErr = retryOp(ctx, Config{Sleep: cfg.Sleep}, func() error {
			var err error
			c.item, c.hasItm, err = d.Lifecycle.Get(ctx, Tool, region)
			return err
		})
		c.staleLease = c.item.Lease != nil && d.Lifecycle.LeaseStale(c.item)
		if live {
			rs := d.Source.Region(region)
			fn, _, err := rs.Poller(ctx)
			c.poller = fn != nil
			if err == nil {
				var ss []inventory.Schedule
				if ss, err = rs.Schedules(ctx); err == nil {
					for _, s := range ss {
						if s.Group == runtimeiam.TeardownScheduleGroup {
							c.jobs = append(c.jobs, s.Name)
						}
					}
				}
			}
			c.liveErr = err
		}
		return c
	}
	out := make([]regionCheck, len(regions))
	if cfg.sequential {
		for i, region := range regions {
			out[i] = check(region)
		}
		return out
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i, region := range regions {
		wg.Add(1)
		go func(i int, region string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = check(region)
		}(i, region)
	}
	wg.Wait()
	return out
}

// itemBlockers says what in checks blocks the account-wide teardown: an item
// that cannot be read, is held, is ACTIVE, or has a live lease; and, for a
// live check (the one before each delete, once the dead items are finished),
// any item at all, a poller, a pending one-shot job, or a read that failed.
func itemBlockers(checks []regionCheck) []string {
	var out []string
	for _, c := range checks {
		name := c.region
		switch {
		case c.itemErr != nil:
			out = append(out, name+": could not read its lifecycle item")
		case !c.hasItm:
		case c.live:
			out = append(out, fmt.Sprintf("%s: it has a lifecycle item (%s)", name, c.item.State))
		case len(c.item.Holds) > 0:
			out = append(out, fmt.Sprintf("%s: held by %v", name, c.item.Holds))
		case c.item.State == lifecycle.StateActive:
			out = append(out, name+": its lifecycle item is ACTIVE (lagotto is in use there; once it is not, 'lagotto teardown --region "+name+"' clears it)")
		case c.item.Lease != nil && !c.stale():
			out = append(out, name+": a teardown holds its lease ("+c.item.Lease.Owner+")")
		}
		if !c.live {
			continue
		}
		switch {
		case c.liveErr != nil:
			out = append(out, name+": could not read its poller or one-shot jobs")
		case c.poller:
			out = append(out, name+": a poller is deployed")
		}
		for _, j := range c.jobs {
			out = append(out, name+": one-shot job "+j+" is pending")
		}
	}
	sort.Strings(out)
	return out
}

// stale reports whether the check's item's lease has gone stale.
func (c regionCheck) stale() bool { return c.staleLease }

// accountWideBlockers says what in facts blocks the account-wide teardown,
// and which regions hold only a dead lifecycle item (its lease stale, or
// IDLE, with nothing of lagotto's left there).
func accountWideBlockers(ctx context.Context, cfg AccountWideConfig, d AccountWideDeps, facts inventory.Facts) (blockers, dead []string) {
	if !facts.AllRegions || facts.AccountIncomplete {
		blockers = append(blockers, "the footprint could not be read in full")
	}
	names := make([]string, len(facts.Regions))
	for i, r := range facts.Regions {
		names[i] = r.Region
	}
	checks := regionChecks(ctx, cfg, d, names, false)
	blockers = append(blockers, itemBlockers(checks)...)
	for i := range facts.Regions {
		r, c := &facts.Regions[i], checks[i]
		var b []string
		if r.ActivityUnknown {
			b = append(b, r.Region+": could not be read in full")
		} else if r.Active() {
			b = append(b, r.Region+": in use ("+strings.Join(r.ActiveReasons(), "; ")+")")
		}
		if r.Function != nil {
			b = append(b, r.Region+": a poller is deployed"+pollerGrace(r.Function, c, cfg.Now()))
		}
		for _, s := range r.Schedules {
			if s.Group == runtimeiam.TeardownScheduleGroup {
				b = append(b, r.Region+": one-shot job "+s.Name+" is pending")
			}
		}
		if c.hasItm && c.itemErr == nil && len(b) == 0 && len(itemBlockers([]regionCheck{c})) == 0 {
			// No poller is left to carry it on: dead. What else of lagotto's
			// is left there (a teardown that stopped partway) needs nothing
			// account-wide; 'lagotto teardown --plan' removes it.
			dead = append(dead, r.Region)
			if lagottoLeft(r) {
				cfg.Logf("account-wide %s region=%s dead_item=%s left=%q", cfg.label(), r.Region, c.item.State,
					"something of lagotto's is left there, which needs nothing account-wide; 'lagotto teardown --plan' removes it")
			}
		}
		blockers = append(blockers, b...)
	}
	sort.Strings(blockers)
	return blockers, dead
}

// pollerGrace says when a region's poller removes its region: its own idle
// grace (LAGOTTO_IDLE_GRACE on the function, else DefaultGrace), and how long
// the region has been idle, per its item.
func pollerGrace(fn *inventory.Item, c regionCheck, now time.Time) string {
	grace, _ := GraceFrom(func(string) string { return fn.IdleGrace })
	out := fmt.Sprintf(" (it removes its region once idle for %s", grace)
	if c.hasItm && c.item.State == lifecycle.StateIdle && !c.item.IdleSince.IsZero() {
		out += fmt.Sprintf("; idle for %s", c.item.IdleFor(now).Round(time.Minute))
	}
	return out + ")"
}

// lagottoLeft reports whether anything of lagotto's is left in r.
func lagottoLeft(r *inventory.Region) bool {
	return len(r.Tables) > 0 || r.Bucket != nil || r.Topic != nil || r.Function != nil || r.LogGroup != nil ||
		len(r.Groups) > 0 || len(r.Schedules) > 0 || r.Stack != nil
}

// accountWideResources are the account-wide resources of facts the teardown
// would delete, as the footprint classifies them, limited to cfg.Only: not
// what is in use (lagotto-cli-user while anything is attached to it) or not
// lagotto's (an administrator's authorized role), which stay and do not keep
// the global item.
func accountWideResources(facts inventory.Facts, cfg AccountWideConfig) []fp.Resource {
	live := inventory.Classify(facts, cfg.Options, cfg.Now())
	var out []fp.Resource
	for _, r := range live.Resources {
		if r.Region == inventory.Global && r.Deletable && r.Class != fp.ClassActive && (cfg.Only == nil || cfg.Only[r.ARN]) {
			out = append(out, r)
		}
	}
	return out
}

// accountWidePlan plans and runs the deletes: in reap mode under gl, with
// every regional item read again before each delete; in report mode (gl
// nil) only re-checked.
func accountWidePlan(ctx context.Context, cfg AccountWideConfig, d AccountWideDeps, facts inventory.Facts, planned []fp.Resource, gl *lifecycle.Lease) (*fp.Plan, *fp.Result, error) {
	live := inventory.Classify(facts, cfg.Options, cfg.Now())
	scoped := *live
	scoped.Resources = planned
	plan, kept, err := inventory.PlanTeardown(&scoped, live, cfg.Options)
	if err != nil {
		return nil, nil, err
	}
	for _, k := range kept {
		cfg.Logf("account-wide %s outcome=kept type=%s name=%q reason=%q", cfg.label(), k.Resource.Type, k.Resource.Name, k.Reason)
	}
	var deleter fp.Deleter = inventory.NewDeleter(live, d.Deleters)
	mode := fp.ModeReport
	if gl != nil {
		mode = fp.ModeReap
		names := make([]string, len(facts.Regions))
		for i, r := range facts.Regions {
			names[i] = r.Region
		}
		deleter = fencedAccountDeleter{Deleter: deleter, recheck: func(ctx context.Context) error {
			if b := itemBlockers(regionChecks(ctx, cfg, d, names, true)); len(b) > 0 {
				return fmt.Errorf("%w: %s", errAccountWideBlocked, strings.Join(b, "; "))
			}
			return nil
		}}
	}
	res, err := fp.Execute(ctx, plan, mode, deleter, gl)
	for _, a := range res.Actions {
		row := fmt.Sprintf("account-wide %s outcome=%s type=%s name=%q arn=%s", cfg.label(), a.Outcome, a.Type, a.Name, a.ARN)
		if a.Reason != "" {
			row += fmt.Sprintf(" reason=%q", a.Reason)
		}
		if a.Err != nil {
			row += fmt.Sprintf(" error=%q", a.Err.Error())
		}
		cfg.Logf("%s", row)
	}
	return plan, res, err
}

// errAccountWideBlocked is a re-check's refusal before a delete.
var errAccountWideBlocked = errors.New("the account-wide resources are needed again")

// fencedAccountDeleter re-reads every region's lifecycle item before each
// delete's re-check (footprint.Execute then heartbeats the global lease,
// which a creator's mark has taken away).
type fencedAccountDeleter struct {
	fp.Deleter
	recheck func(ctx context.Context) error
}

func (f fencedAccountDeleter) Describe(ctx context.Context, r fp.Resource) (fp.Resource, error) {
	if err := f.recheck(ctx); err != nil {
		return fp.Resource{}, err
	}
	return f.Deleter.Describe(ctx, r)
}

// finishDeadRegion finishes a region's dead lifecycle item (its lease stale,
// or IDLE, and nothing of lagotto's left there) and closes the region's
// table if no tool is left in it. A table that cannot be closed is only
// logged.
func finishDeadRegion(ctx context.Context, cfg AccountWideConfig, lc *lifecycle.Client, region string) error {
	if _, _, err := lc.Observe(ctx, Tool, region, false); err != nil {
		return err
	}
	l, err := lc.Claim(ctx, Tool, region, 0)
	if err != nil {
		return err
	}
	if err := l.Finish(ctx); err != nil {
		return err
	}
	if _, err := lc.CloseTableIfEmpty(ctx, region); err != nil {
		cfg.Logf("account-wide %s dead_item=finished region=%s close_table=failed error=%q", cfg.label(), region, err.Error())
		return nil
	}
	cfg.Logf("account-wide %s dead_item=finished region=%s", cfg.label(), region)
	return nil
}
