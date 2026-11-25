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
	"github.com/aws/smithy-go"
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
	// Sleep waits for d or until ctx is done. Default a timer.
	Sleep func(ctx context.Context, d time.Duration) error
	// Owner is the lease owner the run's lifecycle client writes. Default
	// PollerOwner(Region); the poller makes it unique to its container
	// (RunOwner), so a concurrent invocation's lease is never its own.
	Owner string
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
	if c.Sleep == nil {
		c.Sleep = sleep
	}
	if c.Owner == "" {
		c.Owner = PollerOwner(c.Region)
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
	// OutcomeTornDown: the poller's rate schedule is gone, so its region
	// was torn down (by the CLI, or by a run of this poller that
	// committed): the run recorded nothing, or finished the lifecycle item
	// its own observation made again (lagotto#350).
	OutcomeTornDown Outcome = "torn-down"
)

// Phase is when, and by whom, a planned resource is removed.
type Phase string

// Phases.
const (
	// PhaseDirect: the poller deletes it at once (its tables, bucket, topic
	// and per-launch schedules).
	PhaseDirect Phase = "direct"
	// PhaseSelf: the poller's own function and log group, which a one-shot
	// job deletes after the poller has finished (see OneShot).
	PhaseSelf Phase = "self"
	// PhaseFinal: its own rate schedule and lagotto's schedule group, which
	// the poller deletes last, once the jobs exist.
	PhaseFinal Phase = "final"
	// PhaseCLI: what the poller leaves for the CLI: everything account-wide,
	// what it has no grant to delete, and what its own jobs need.
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
	case inventory.TypeRole, inventory.TypeInstanceProfile, inventory.TypePolicy:
		return PhaseCLI, "account-wide: the CLI removes it once no poller is left (lagotto#340)"
	}
	return PhaseCLI, "the poller does not delete a " + r.Type
}

// Plan is a region's teardown as planned and checked. It is the region's
// own resources only: the account-wide ones are the CLI's
// (accountwide.go).
type Plan struct {
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
	// committed: the run created its one-shot jobs and did not roll them
	// back.
	committed bool
	// facts is the footprint the run read under its lease, if it did.
	facts *inventory.Facts
}

// Tick runs one region's idle check: the verdict, Observe, and, once the
// region has been idle for the grace period, the region's teardown. It never
// disables or slows the poller's schedule (pkg/pollcadence owns the cadence),
// in report mode it deletes nothing, and in either mode it touches nothing
// account-wide. Every step is logged.
//
// In reap mode a run that does not commit its region's removal (the region
// is in use, within its grace, or the run stopped before its jobs stood)
// deletes the one-shot jobs an earlier run left there: such a job keeps the
// time it was created with, and would remove the poller (its function)
// while its rate schedule, and so its region, is still there.
func Tick(ctx context.Context, cfg Config, d Deps) Result {
	cfg = cfg.withDefaults()
	res := tick(ctx, cfg, d)
	if cfg.Reap && !res.committed && res.Outcome != OutcomeTornDown {
		// (A torn-down region's jobs are its teardown's own.)
		var seen *inventory.Region
		// What this run read, unless it went on to create jobs itself.
		if res.facts != nil && (res.Plan == nil || len(res.Plan.OneShots) == 0) {
			seen = res.facts.RegionFacts(cfg.Region) // nil: listed
		}
		// Listed on every non-committing run (one ListSchedules) unless its
		// footprint was read in full: limiting it to a claimed, TEARDOWN or
		// incomplete item lets a leftover job fire (the model check's "a
		// launch while jobs are left" and "leftover jobs, a watch and the
		// CLI", lagotto#351's seventh review).
		cancelUncommittedJobs(ctx, cfg, d, seen)
	}
	return res
}

// cancelUncommittedJobs deletes the region's one-shot jobs a run that did
// not commit leaves behind (an earlier run's, at their old time). It never
// undoes a commit: it works under a lock on the region's lifecycle item
// (lifecycle LockIfUnchanged, so no run can claim, and so commit, while it
// deletes), only while the poller's rate schedule is still there (once it
// is gone, the region's teardown committed and finished: this run is a
// retried or duplicate invocation), and heartbeats the lock before each
// delete. Another run's live lease, a lock it cannot take, or a read that
// fails, leaves the jobs: the next run tries again. A run that read its
// region's footprint (seen) and found no job there makes no call; one that
// stopped before reading it (active, within its grace, refused the claim)
// lists the region's schedules, since an earlier run's jobs may be there.
func cancelUncommittedJobs(ctx context.Context, cfg Config, d Deps, seen *inventory.Region) {
	skip := func(why string) {
		cfg.Logf("idle-teardown %s region=%s phase=one-shot cancel=skipped reason=%q", cfg.label(), cfg.Region, why)
	}
	list := func() (names []string, scheduled bool, err error) {
		var ss []inventory.Schedule
		if seen != nil {
			ss, seen = seen.Schedules, nil
		} else if ss, err = d.Source.Region(cfg.Region).Schedules(ctx); err != nil {
			return nil, false, err
		}
		for _, s := range ss {
			switch {
			case s.Group == runtimeiam.TeardownScheduleGroup:
				names = append(names, s.Name)
			case s.Name == deploy.PollerScheduleName:
				scheduled = true
			}
		}
		return names, scheduled, nil
	}
	names, scheduled, err := list()
	switch {
	case err != nil:
		skip("could not list the region's schedules: " + err.Error())
		return
	case len(names) == 0:
		return
	case !scheduled:
		skip("the poller's rate schedule is gone: the region's teardown committed, and its jobs are its own")
		return
	}
	// The lock is taken as the item is read; an item that changed in
	// between (a creator's mark ended, an activity stamped) is read again,
	// a few times, so a benign change does not leave the jobs.
	var lock *lifecycle.Lease
	for attempt := 1; ; attempt++ {
		var (
			it lifecycle.Item
			ok bool
		)
		err = retryOp(ctx, cfg, func() error {
			var err error
			it, ok, err = d.Lifecycle.Get(ctx, Tool, cfg.Region)
			return err
		})
		switch {
		case err != nil:
			skip("could not read the region's lifecycle item: " + err.Error())
			return
		case !ok:
			skip("the region has no lifecycle item to lock")
			return
		case it.Lease != nil && !d.Lifecycle.LeaseStale(it):
			skip("another run holds the region's lease (" + it.Lease.Owner + "); its jobs are its own")
			return
		}
		lock, err = d.Lifecycle.LockIfUnchanged(ctx, Tool, cfg.Region, lifecycle.ExpectOf(it, ok))
		var nc *lifecycle.NotClaimableError
		if err == nil || !errors.As(err, &nc) || !strings.HasPrefix(nc.Reason, "changed") || attempt == readAttempts {
			break
		}
	}
	if err != nil {
		skip("the region's lifecycle item could not be locked: " + err.Error())
		return
	}
	defer func() {
		if err := retryOp(ctx, cfg, func() error {
			if err := lock.Unlock(ctx); errors.Is(err, lifecycle.ErrLeaseLost) {
				return &final{err}
			} else {
				return err
			}
		}); err != nil && !errors.Is(err, lifecycle.ErrLeaseLost) {
			cfg.Logf("idle-teardown %s region=%s phase=one-shot unlock=failed error=%q", cfg.label(), cfg.Region, err.Error())
		}
	}()
	// Under the lock, the region as it stands now.
	if names, scheduled, err = list(); err != nil || !scheduled {
		skip("the region's schedules changed, or could not be read, under the lock")
		return
	}
	t := &run{cfg: cfg, d: d}
	for _, name := range names {
		if err := lock.Step(ctx, "delete one-shot "+name); err != nil {
			skip("the lock was lost: " + err.Error())
			return
		}
		if err := t.rollback(ctx, []string{name}); err != nil {
			return
		}
	}
}

func tick(ctx context.Context, cfg Config, d Deps) Result {
	logf := cfg.Logf
	v := ReadVerdict(ctx, d.Source.Region(cfg.Region), cfg.Region, cfg.Options)
	res := Result{Verdict: v}
	for _, w := range v.Warnings {
		logf("idle-verdict region=%s warning=%q", cfg.Region, w)
	}

	// A run in a region that was torn down records nothing (lagotto#350):
	// its Observe would make the region's lifecycle item again, and its
	// table, with nothing left there to remove them. A read that fails
	// observes as before.
	if tornDown(ctx, cfg, d, "before observing") {
		res.Outcome = OutcomeTornDown
		logf("idle-verdict region=%s active=%t observe=skipped reason=%q", cfg.Region, v.Active, tornDownWhy)
		return res
	}
	var idleFor time.Duration
	err := retryOp(ctx, cfg, func() error {
		var err error
		_, idleFor, err = d.Lifecycle.Observe(ctx, Tool, cfg.Region, v.Active)
		if errors.Is(err, lifecycle.ErrTableClosing) {
			return &final{err}
		}
		return err
	})
	res.IdleFor = idleFor
	if err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		if errors.Is(err, lifecycle.ErrTableClosing) {
			// A live closer's sentinel: Observe takes a dead closer's over
			// itself (libs#88).
			logf("idle-verdict region=%s active=%t observe=skipped reason=%q", cfg.Region, v.Active, "the lifecycle table is being deleted")
			return res
		}
		logf("idle-verdict region=%s active=%t observe=failed error=%q", cfg.Region, v.Active, err.Error())
		return res
	}
	// Torn down while this run observed: a teardown deletes the rate
	// schedule before it finishes the region's item, so an observation
	// after that finish made the item again (lagotto#350).
	if tornDown(ctx, cfg, d, "after observing") {
		res.Outcome = OutcomeTornDown
		finishTornDown(ctx, cfg, d)
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

	lease, err := claim(ctx, cfg, d)
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

// claim takes the region's teardown lease. A throttled claim (which DynamoDB
// did not apply) is tried again. Any other failure is not: the claim may
// have been applied all the same, and a retry would then find this poller's
// own lease and be refused. So after any failure it reads the item, and a
// live lease in this run's owner name (Config.Owner, unique to the poller's
// container) is recognised as its own claim whose answer was lost, and
// given back (lifecycle AdoptLease): the run stops, and leaves nothing for
// a creator to wait for.
func claim(ctx context.Context, cfg Config, d Deps) (*lifecycle.Lease, error) {
	var lease *lifecycle.Lease
	err := retryOp(ctx, cfg, func() error {
		var err error
		lease, err = d.Lifecycle.Claim(ctx, Tool, cfg.Region, cfg.Grace)
		if err != nil && !isThrottle(err) {
			return &final{err}
		}
		return err
	})
	if err != nil && ownLease(ctx, d.Lifecycle, cfg.Region, cfg.Owner) {
		err = fmt.Errorf("%w (%s)", err, giveBackOwn(ctx, d.Lifecycle, cfg.Region, cfg.Sleep))
	}
	return lease, err
}

// ownLeaseNote says what a lease found in the claimant's own name is.
const ownLeaseNote = "the lease is this claimant's own, from a claim whose answer was lost"

// giveBackOwn gives back the lease a claim whose answer was lost left in the
// claimant's own name (lifecycle AdoptLease, then Release, tried again on
// failure), so no creator waits for a run that has stopped, and says what
// became of it.
func giveBackOwn(ctx context.Context, c *lifecycle.Client, scope string, sleep func(context.Context, time.Duration) error) string {
	err := retryOp(ctx, Config{Sleep: sleep}, func() error {
		l, err := c.AdoptLease(ctx, Tool, scope)
		if errors.Is(err, lifecycle.ErrLeaseLost) {
			return &final{err}
		}
		if err != nil {
			return err
		}
		if err := l.Release(ctx); errors.Is(err, lifecycle.ErrLeaseLost) {
			return &final{err}
		} else {
			return err
		}
	})
	switch {
	case err == nil:
		return ownLeaseNote + "; it was given back"
	case errors.Is(err, lifecycle.ErrLeaseLost):
		return ownLeaseNote + "; it was gone already"
	}
	return ownLeaseNote + "; it could not be given back (" + err.Error() + "), and goes stale in 20 min"
}

// ownLease reports whether scope's lagotto item holds a live lease in
// owner's name. A failed read is "no".
func ownLease(ctx context.Context, c *lifecycle.Client, scope, owner string) bool {
	it, ok, err := c.Get(ctx, Tool, scope)
	return err == nil && ok && it.Lease != nil && it.Lease.Owner == owner && !c.LeaseStale(it)
}

// isThrottle reports whether err is a throttle, which DynamoDB answers
// without applying the request.
func isThrottle(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	switch ae.ErrorCode() {
	case "ThrottlingException", "ProvisionedThroughputExceededException", "RequestLimitExceeded", "Throttling":
		return true
	}
	return false
}

// tornDownWhy is why a run in a torn-down region records nothing.
const tornDownWhy = "the poller's rate schedule is gone: its region was torn down (lagotto#350)"

// tornDown reports whether the poller's rate schedule is gone from its
// region: the region was torn down, by the CLI (`lagotto teardown --plan`,
// `deploy --teardown`) or by a run of this poller that committed, and only
// a run already in flight, or a retried or duplicate invocation, is left.
// Every such teardown deletes the schedule before it finishes the region's
// lifecycle item. A read that fails is logged and is "no".
func tornDown(ctx context.Context, cfg Config, d Deps, when string) bool {
	ss, err := d.Source.Region(cfg.Region).Schedules(ctx)
	if err != nil {
		cfg.Logf("idle-verdict region=%s schedule_check=failed when=%q error=%q", cfg.Region, when, err.Error())
		return false
	}
	for _, s := range ss {
		if s.Name == deploy.PollerScheduleName {
			return false
		}
	}
	return true
}

// finishTornDown ends a run whose region was torn down while it observed
// (lagotto#350). Its Observe, coming after the teardown had finished the
// region's lifecycle item, made it again (and the table, if the teardown
// had closed it), in a region with nothing left to observe or to remove
// it. The item is finished as judged (finishIfDead: refused while it is in
// use, so a creator's mark, a hold or another run's live lease keeps it,
// and only as it was read), in either mode: it is this run's own write,
// not something the region's teardown would report. Reap mode then closes
// the region's table if no tool is left in it; report mode deletes no
// table, and an empty one is the last mile's to report. A failure is tried
// again; one that persists leaves a dead item, which the account-wide
// teardown finishes once the region's poller is gone.
func finishTornDown(ctx context.Context, cfg Config, d Deps) {
	var (
		done bool
		why  string
	)
	err := retryOp(ctx, cfg, func() error {
		var err error
		done, why, err = finishIfDead(ctx, d.Lifecycle, cfg.Region, cfg.Sleep)
		return err
	})
	switch {
	case err != nil:
		cfg.Logf("idle-verdict region=%s torn_down=true finish=failed error=%q", cfg.Region, err.Error())
		return
	case !done:
		cfg.Logf("idle-verdict region=%s torn_down=true finish=skipped reason=%q", cfg.Region, why)
		return
	}
	cfg.Logf("idle-verdict region=%s torn_down=true finish=ok reason=%q", cfg.Region, tornDownWhy)
	if !cfg.Reap {
		return
	}
	if _, err := d.Lifecycle.CloseTableIfEmpty(ctx, cfg.Region); err != nil {
		cfg.Logf("idle-verdict region=%s torn_down=true close_table=failed error=%q", cfg.Region, err.Error())
	}
}

// final wraps an error retryOp must not retry.
type final struct{ err error }

func (f *final) Error() string { return f.err.Error() }
func (f *final) Unwrap() error { return f.err }

// retryOp runs f up to readAttempts times while it fails, unless it returns
// a *final, whose error it returns at once.
func retryOp(ctx context.Context, cfg Config, f func() error) error {
	var err error
	for a := 0; a < readAttempts; a++ {
		if a > 0 {
			if serr := cfg.Sleep(ctx, time.Second); serr != nil {
				return errors.Join(err, serr)
			}
		}
		if err = f(); err == nil {
			return nil
		}
		var fe *final
		if errors.As(err, &fe) {
			return fe.err
		}
	}
	return err
}

// readAttempts is how many times a read or write that fails is tried.
const readAttempts = 3

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

// run is one region's teardown in progress.
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

func (t *run) summary(plan *Plan, res *Result, lease string) {
	if res.Outcome == OutcomeLeaseLost && lease == "released" {
		lease = "lost"
	}
	t.cfg.Logf("idle-teardown %s region=%s would_delete=%d kept=%d failed=%d one_shots=%d lease=%s",
		t.cfg.label(), t.cfg.Region, plan.WouldDelete(), plan.KeptCount(), plan.Failed(), len(plan.OneShots), lease)
}

// teardown runs with the region's lease held: re-check, plan, execute, hand
// off. Only the region's own resources are planned.
func teardown(ctx context.Context, cfg Config, d Deps, lease *lifecycle.Lease, res Result) Result {
	// The post-claim re-check, and the footprint the plan is made from, read
	// now that the lease keeps creators out of this region.
	facts, err := gatherFacts(ctx, d.Source, cfg.AccountID, inventory.Scope{Regions: []string{cfg.Region}}, cfg.Options)
	if err != nil {
		res.Outcome, res.Err = OutcomeReleased, err
		_ = release(ctx, cfg, lease, "could not read the footprint: "+err.Error())
		return res
	}
	// Only its own region is read (no other region's state matters to it),
	// and the account-wide resources, to say they are left for the CLI.
	here := facts.RegionFacts(cfg.Region)
	if here != nil && !here.ActivityUnknown {
		// Only a footprint read in full says what jobs the region has
		// (cancelUncommittedJobs); otherwise they are listed.
		res.facts = &facts
	}
	switch {
	case here == nil:
		// Gather read exactly this region, so it is there; a footprint
		// without it is one this run cannot judge.
		res.Outcome = OutcomeReleased
		_ = release(ctx, cfg, lease, "the footprint has no "+cfg.Region)
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
	plan := &Plan{Phases: map[Phase]*fp.Plan{}}
	res.Plan = plan
	live := inventory.Classify(facts, cfg.Options, cfg.Now())
	scoped := *live
	scoped.Resources = nil
	var accountWide []inventory.Kept
	for _, r := range live.Resources {
		switch r.Region {
		case cfg.Region:
			scoped.Resources = append(scoped.Resources, r)
		case inventory.Global:
			_, why := PhaseOf(r)
			accountWide = append(accountWide, inventory.Kept{Resource: r, Reason: "left for the CLI: " + why})
		}
	}
	full, kept, err := inventory.PlanTeardown(&scoped, live, cfg.Options)
	if err != nil {
		res.Outcome, res.Err = OutcomeFailed, err
		_ = release(ctx, cfg, lease, "could not plan: "+err.Error())
		return res
	}
	plan.Kept = append(kept, accountWide...)
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

	// 1. Its own function and log group, checked first: always report
	// mode, only to apply the delete guard; what passes becomes a one-shot
	// job.
	plan.Self, err = execute(ctx, plan.Phases[PhaseSelf], fp.ModeReport, deleter, lease)
	t.logRow(string(PhaseSelf), plan.Phases[PhaseSelf], plan.Self)
	if plan.Self == nil {
		return t.unrun(ctx, &res, plan, err, "the self-removal check")
	}
	if t.stop(ctx, &res, plan, err, "the self-removal check") {
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
	if cfg.Reap && here.Function != nil && !sr.Function {
		// The function did not pass the delete guard (not lagotto's, or
		// permanent, or kept): no job would remove it, and a commit would
		// remove its rate schedule and leave it. Nothing is deleted: the
		// region's removal waits (the account-wide teardown names it).
		why := "its function " + here.Function.Name + " did not pass the delete guard"
		if r, ok := unremovable(live, here.Function.ARN); ok {
			why += " (" + r + ")"
		}
		for _, a := range plan.Self.Actions {
			if a.Type == inventory.TypeFunction && a.Reason != "" {
				why += " (" + string(a.Outcome) + ": " + a.Reason + ")"
			}
		}
		for _, k := range plan.Kept {
			if k.Resource.Type == inventory.TypeFunction && k.Resource.Region == cfg.Region {
				why += " (kept: " + k.Reason + ")"
			}
		}
		res.Outcome = OutcomeReleased
		_ = release(ctx, cfg, lease, why+"; not committing the region's removal")
		t.summary(plan, &res, "released")
		return res
	}
	// 2. What the poller deletes directly.
	plan.Direct, err = execute(ctx, plan.Phases[PhaseDirect], mode, deleter, lease)
	t.logRow(string(PhaseDirect), plan.Phases[PhaseDirect], plan.Direct)
	if plan.Direct == nil {
		return t.unrun(ctx, &res, plan, err, "the direct deletes")
	}
	if t.stop(ctx, &res, plan, err, "the direct deletes") {
		return res
	}
	now := cfg.Now()
	plan.OneShots = sr.Plan(now)
	for _, o := range plan.OneShots {
		cfg.Logf("idle-teardown %s region=%s phase=one-shot name=%s offset=+%s at=%s group=%s target=%s input=%s resource=%s",
			cfg.label(), cfg.Region, o.Name, o.Offset, o.At.UTC().Format(time.RFC3339), runtimeiam.TeardownScheduleGroup, o.Target, o.Input, o.Resource)
	}
	if !cfg.Reap {
		// 3. Its own rate schedule and lagotto's group, re-checked. Report
		// mode never finishes the lease or closes the table: the grace
		// period starts over, and the next report is a grace period away.
		plan.Final, err = execute(ctx, plan.Phases[PhaseFinal], mode, deleter, lease)
		t.logRow(string(PhaseFinal), plan.Phases[PhaseFinal], plan.Final)
		if t.stop(ctx, &res, plan, err, "the final deletes") {
			return res
		}
		if err := lease.Release(ctx); err != nil {
			res.Outcome, res.Err = OutcomeFailed, err
			if errors.Is(err, lifecycle.ErrLeaseLost) {
				res.Outcome = OutcomeLeaseLost
			}
			t.summary(plan, &res, "release-failed")
			return res
		}
		res.Outcome = OutcomeReported
		t.summary(plan, &res, "released")
		return res
	}

	// 3. The commit: first the jobs an earlier run of this region left that
	// this plan does not list go, then the jobs, the function's last (a job
	// already there is recreated at this run's time). Creating the
	// function's job commits the poller's removal; every job is due after
	// the poller's next hourly run, so a run that stops before its own
	// schedule is deleted leaves a poller that runs again and finishes.
	if err := t.cancelLeftJobs(ctx, plan.OneShots); err != nil {
		if t.stop(ctx, &res, plan, err, "deleting the jobs an earlier run left") {
			return res
		}
	}
	created, err := CreateOneShots(ctx, d.Scheduler, cfg.Region, cfg.AccountID, now, plan.OneShots, lease.Step)
	res.committed = err == nil
	if err != nil {
		// Every planned job goes, not only those created: one an earlier run
		// left that could not be recreated keeps its own, earlier time, and
		// would remove the poller before its next run (the model check).
		all := make([]string, len(plan.OneShots))
		for i, o := range plan.OneShots {
			all[i] = o.Name
		}
		if rerr := t.rollback(ctx, all); rerr != nil {
			return t.strand(&res, plan, errors.Join(err, rerr), "creating the one-shot jobs")
		}
		_ = t.stop(ctx, &res, plan, err, "creating the one-shot jobs")
		return res
	}
	// 4. Its own rate schedule and lagotto's group, last.
	plan.Final, err = execute(ctx, plan.Phases[PhaseFinal], mode, deleter, lease)
	t.logRow(string(PhaseFinal), plan.Phases[PhaseFinal], plan.Final)
	if err != nil {
		if count(plan.Final, fp.OutcomeDeleted) == 0 {
			// Nothing of its own schedule is gone (a creator took the lease
			// over, or a delete failed): its jobs go, and its poller stays.
			// Nothing account-wide depends on this region's commit, so the
			// rollback is always safe.
			if rerr := t.rollback(ctx, created); rerr == nil {
				res.committed = false
				_ = t.stop(ctx, &res, plan, err, "the final deletes")
				return res
			}
		}
		return t.strand(&res, plan, err, "the final deletes")
	}
	// 5. Its lease, and its table.
	if err := finishLease(ctx, cfg.Sleep, d.Lifecycle, cfg.Region, lease); err != nil {
		return t.strand(&res, plan, err, "finishing its lease")
	}
	if _, err := d.Lifecycle.CloseTableIfEmpty(ctx, cfg.Region); err != nil {
		cfg.Logf("idle-teardown %s region=%s close_table=failed error=%q", cfg.label(), cfg.Region, err.Error())
	}
	res.Outcome = OutcomeReaped
	t.summary(plan, &res, "finished")
	return res
}

// gatherFacts is inventory.Gather. A seam for tests.
var gatherFacts = inventory.Gather

// unrun ends a run whose phase footprint.Execute could not run at all (a nil
// Result: nothing of it was tried), in either mode: the lease goes back.
func (t *run) unrun(ctx context.Context, res *Result, plan *Plan, err error, what string) Result {
	res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, err)
	_ = release(ctx, t.cfg, t.lease, what+" could not run: "+fmt.Sprint(err))
	t.summary(plan, res, "released")
	return *res
}

// stop handles a phase's error by stopped's rules, and logs the summary
// when it stops.
func (t *run) stop(ctx context.Context, res *Result, plan *Plan, err error, what string) bool {
	if !stopped(ctx, t.cfg, t.lease, res, err, what) {
		return false
	}
	t.summary(plan, res, "released")
	return true
}

// cancelLeftJobs deletes the one-shot jobs an earlier run of this region left
// (it stopped while committing) that keep does not list.
func (t *run) cancelLeftJobs(ctx context.Context, keep []OneShot) error {
	here := t.facts.RegionFacts(t.cfg.Region)
	if here == nil {
		return nil
	}
	wanted := map[string]bool{}
	for _, j := range keep {
		wanted[j.Name] = true
	}
	var left []string
	for _, s := range here.Schedules {
		if s.Group == runtimeiam.TeardownScheduleGroup && strings.HasPrefix(s.Name, "lagotto-teardown-") && !wanted[s.Name] {
			left = append(left, s.Name)
		}
	}
	return t.rollback(ctx, left)
}

// rollback deletes one-shot jobs (a failed commit's, or an earlier run's), so
// the poller stays. Each delete is tried again on failure; a job already gone
// is not an error.
func (t *run) rollback(ctx context.Context, names []string) error {
	var errs []error
	for _, name := range names {
		err := retryOp(ctx, t.cfg, func() error {
			_, err := t.d.Scheduler.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
				Name: aws.String(name), GroupName: aws.String(runtimeiam.TeardownScheduleGroup)})
			if isNotFound(err) {
				return nil
			}
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("delete one-shot job %s: %w", name, err))
		}
	}
	if len(names) > 0 {
		t.cfg.Logf("idle-teardown %s region=%s phase=one-shot deleted=%d error=%v", t.cfg.label(), t.cfg.Region, len(names), errors.Join(errs...))
	}
	return errors.Join(errs...)
}

// strand ends a run that failed after the poller committed its removal and
// started deleting its own schedule (or could not finish its lease): nothing
// is rolled back, and the regional lease is left held, to go stale. While
// the poller's schedule is still there its next run finishes; otherwise the
// stale item is the CLI's to clear (accountwide.go) and the last mile
// reports it.
func (t *run) strand(res *Result, plan *Plan, err error, what string) Result {
	res.Outcome, res.Err = OutcomeFailed, errors.Join(res.Err, err)
	if errors.Is(err, lifecycle.ErrLeaseLost) {
		res.Outcome = OutcomeLeaseLost
	}
	t.cfg.Logf("idle-teardown %s region=%s lease=held during=%q error=%q reason=%q", t.cfg.label(), t.cfg.Region, what, err.Error(),
		"failed after the poller committed its own removal; the regional lease is left to go stale")
	t.summary(plan, res, "held")
	return *res
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
