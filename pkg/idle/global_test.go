package idle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// world is an account with lagotto deployed in several regions, each with
// its own spore-host-lifecycle table and poller, sharing one clock and one
// footprint. Its deleters and Scheduler change the footprint, as the real
// ones change the account, so a region deciding after another sees what the
// other did (lagotto#340).
type world struct {
	t      *testing.T
	src    *inventorytest.Source
	ev     *events
	mu     sync.Mutex
	now    time.Time
	acct   inventory.Account
	tables map[string]*lifecycletest.Fake
	logs   []string
	// failProfile fails the instance profile's delete; failRoleJobs the
	// poller role's one-shot jobs.
	failProfile, failRoleJobs bool
}

const west = "us-west-2"

// newWorld deploys lagotto in each of deployed; eu-west-1 is enabled too,
// with nothing of lagotto's in it.
func newWorld(t *testing.T, deployed ...string) *world {
	t.Helper()
	w := &world{t: t, ev: &events{}, now: t0, acct: inventorytest.Account(acct), tables: map[string]*lifecycletest.Fake{}}
	w.src = inventorytest.New(append([]string{"eu-west-1"}, deployed...)...)
	for _, r := range deployed {
		w.src.SetRegion(inventorytest.Deployed(r, acct))
	}
	w.src.SetAccount(w.acct)
	return w
}

func (w *world) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *world) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

// table is region's lifecycle table.
func (w *world) table(region string) *lifecycletest.Fake {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.tables[region]
	if !ok {
		f = lifecycletest.New()
		w.tables[region] = f
	}
	return f
}

// client is a lifecycle client as owner (a region's poller, or a creator).
func (w *world) client(owner string) *lifecycle.Client {
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return w.table(r), nil },
		ControlRegion: ControlRegion(region),
		Owner:         owner,
		Now:           w.clock,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

func (w *world) logf(r string) func(string, ...any) {
	return func(f string, a ...any) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.logs = append(w.logs, r+": "+fmt.Sprintf(f, a...))
	}
}

func (w *world) log() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.logs, "\n")
}

func (w *world) cfg(r string, reap bool) Config {
	return Config{Region: r, AccountID: acct, Grace: 24 * time.Hour, Reap: reap, Now: w.clock, Logf: w.logf(r),
		// A waiting region lets the other run.
		GlobalWait: time.Hour, Sleep: func(context.Context, time.Duration) error { time.Sleep(time.Millisecond); return nil }}
}

func (w *world) deps(r string) Deps {
	return Deps{Lifecycle: w.client("lagotto-poller/" + r), Source: w.src, Scheduler: worldScheduler{w, r},
		Deleters: inventory.Deleters{
			AccountID: acct,
			Region:    func(r string) inventory.RegionDeleter { return worldRegion{w, r} },
			Account:   worldAccount{w},
		}}
}

func (w *world) item(r string) (lifecycle.Item, bool) {
	it, ok, err := w.client("test").Get(context.Background(), Tool, r)
	if err != nil {
		w.t.Fatal(err)
	}
	return it, ok
}

// idleTicks runs every region's first idle tick, then moves the clock past
// the grace.
func (w *world) idleTicks(reap bool, regions ...string) {
	w.t.Helper()
	for _, r := range regions {
		if res := Tick(context.Background(), w.cfg(r, reap), w.deps(r)); res.Outcome != OutcomeWaiting {
			w.t.Fatalf("%s first tick: %+v\n%s", r, res, w.log())
		}
	}
	w.advance(24*time.Hour + time.Minute)
}

// planners are the regions whose run planned the account-wide teardown.
func planners(rs map[string]Result) []string {
	var out []string
	for r, res := range rs {
		if res.Plan != nil && res.Plan.Global != nil {
			out = append(out, r)
		}
		if res.Plan != nil && res.Plan.Last != (res.Plan.Global != nil) && res.Err == nil {
			out = append(out, r+"(Last without Global)")
		}
	}
	return out
}

type worldRegion struct {
	w      *world
	region string
}

func (d worldRegion) update(f func(r *inventory.Region)) { d.w.src.Update(d.region, f) }

func (d worldRegion) DeleteTable(_ context.Context, n string) (watcher.TableOutcome, error) {
	d.w.ev.add("delete %s table %s", d.region, n)
	d.update(func(r *inventory.Region) {
		var keep []inventory.Table
		for _, t := range r.Tables {
			if t.Name != n {
				keep = append(keep, t)
			}
		}
		r.Tables = keep
	})
	return watcher.TableDeleted, nil
}
func (d worldRegion) DeleteArtifactBucket(context.Context, string, string) (deploy.BucketTeardown, error) {
	d.w.ev.add("delete %s bucket", d.region)
	d.update(func(r *inventory.Region) { r.Bucket = nil })
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, nil
}
func (d worldRegion) DeleteAlertsTopic(context.Context, string, string) (bool, error) {
	d.w.ev.add("delete %s topic", d.region)
	d.update(func(r *inventory.Region) { r.Topic = nil })
	return true, nil
}
func (d worldRegion) DeletePollerFunction(context.Context) (bool, error) {
	return false, ErrSelfDelete
}
func (d worldRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	return false, ErrSelfDelete
}
func (d worldRegion) DeleteSchedule(_ context.Context, g, n string) (bool, error) {
	d.w.ev.add("delete %s schedule %s/%s", d.region, g, n)
	d.update(func(r *inventory.Region) {
		var keep []inventory.Schedule
		for _, s := range r.Schedules {
			if s.Group != g || s.Name != n {
				keep = append(keep, s)
			}
		}
		r.Schedules = keep
	})
	return true, nil
}
func (d worldRegion) DeleteScheduleGroup(_ context.Context, n string) (bool, error) {
	d.w.ev.add("delete %s group %s", d.region, n)
	d.update(func(r *inventory.Region) {
		var keep []inventory.Item
		for _, g := range r.Groups {
			if g.Name != n {
				keep = append(keep, g)
			}
		}
		r.Groups = keep
	})
	return true, nil
}

type worldAccount struct{ w *world }

func (d worldAccount) DeleteRole(_ context.Context, n string) (bool, error) {
	if n != runtimeiam.InstanceRoleName {
		return false, ErrSelfDelete
	}
	d.w.ev.add("delete global role %s", n)
	d.w.mu.Lock()
	var keep []inventory.Item
	for _, r := range d.w.acct.Roles {
		if r.Name != n {
			keep = append(keep, r)
		}
	}
	d.w.acct.Roles = keep
	a := d.w.acct
	d.w.mu.Unlock()
	d.w.src.SetAccount(a)
	return true, nil
}
func (d worldAccount) DeleteInstanceProfile(_ context.Context, n string) (bool, error) {
	if d.w.failProfile {
		return false, errors.New("AccessDenied: iam:DeleteInstanceProfile")
	}
	d.w.ev.add("delete global profile %s", n)
	d.w.mu.Lock()
	d.w.acct.InstanceProfiles = nil
	a := d.w.acct
	d.w.mu.Unlock()
	d.w.src.SetAccount(a)
	return true, nil
}
func (d worldAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	return cliiam.Deletion{}, ErrSelfDelete
}

// worldScheduler creates the one-shot jobs in its region's footprint.
type worldScheduler struct {
	w      *world
	region string
}

func (s worldScheduler) CreateScheduleGroup(_ context.Context, in *scheduler.CreateScheduleGroupInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleGroupOutput, error) {
	s.w.src.Update(s.region, func(r *inventory.Region) {
		for _, g := range r.Groups {
			if g.Name == aws.ToString(in.Name) {
				return
			}
		}
		r.Groups = append(r.Groups, inventory.Item{Name: aws.ToString(in.Name),
			ARN: inventory.ScheduleGroupARN(s.region, acct, aws.ToString(in.Name)), Tags: inventorytest.Tags(s.region)})
	})
	return &scheduler.CreateScheduleGroupOutput{}, nil
}

func (s worldScheduler) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	if s.w.failRoleJobs && strings.HasPrefix(aws.ToString(in.Name), "lagotto-teardown-role") {
		return nil, errors.New("AccessDenied: scheduler:CreateSchedule")
	}
	s.w.ev.add("create %s one-shot %s", s.region, aws.ToString(in.Name))
	s.w.src.Update(s.region, func(r *inventory.Region) {
		r.Schedules = append(r.Schedules, inventory.Schedule{Group: aws.ToString(in.GroupName), Name: aws.ToString(in.Name), State: "ENABLED"})
	})
	return &scheduler.CreateScheduleOutput{}, nil
}

func (w *world) count(prefix string) int {
	n := 0
	for _, e := range w.ev.list() {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

// TestGlobalTwoRegionsIdleTogether is lagotto#340: two regions cross the
// grace two minutes apart (us-west-2 at 18:34, us-east-1 at 18:36 in the
// report). Each used to defer the account-wide teardown to the other. Now
// exactly one plans it: in reap mode the last to finish, which deletes
// lagotto-instance-role and its profile once and hands the poller role to
// its jobs once; in report mode the first, whose release the other then
// sees as ACTIVE.
func TestGlobalTwoRegionsIdleTogether(t *testing.T) {
	for _, reap := range []bool{true, false} {
		t.Run(fmt.Sprintf("reap=%t", reap), func(t *testing.T) {
			w := newWorld(t, west, region)
			w.idleTicks(reap, west, region)
			res := map[string]Result{}
			res[west] = Tick(context.Background(), w.cfg(west, reap), w.deps(west))
			w.advance(2 * time.Minute)
			res[region] = Tick(context.Background(), w.cfg(region, reap), w.deps(region))
			want := region // reap: the last to finish
			if !reap {
				want = west // report: the first to report
			}
			if got := planners(res); len(got) != 1 || got[0] != want {
				t.Fatalf("planned the globals: %v, want [%s]\n%s", got, want, w.log())
			}
			for r, x := range res {
				if x.Err != nil || (reap && x.Outcome != OutcomeReaped) || (!reap && x.Outcome != OutcomeReported) {
					t.Errorf("%s: %+v", r, x)
				}
			}
			if other := res[west]; reap && !strings.Contains(other.Plan.LastWhy, "finishing after this region") {
				t.Errorf("us-west-2 why: %q", other.Plan.LastWhy)
			}
			if reap {
				if w.count("delete global profile") != 1 || w.count("delete global role lagotto-instance-role") != 1 ||
					w.count("create us-east-1 one-shot lagotto-teardown-role") != 3 || w.count("create us-west-2 one-shot lagotto-teardown-role") != 0 {
					t.Errorf("events:\n%s", strings.Join(w.ev.list(), "\n"))
				}
				for _, r := range []string{west, region} {
					if _, ok := w.item(r); ok {
						t.Errorf("%s: the regional lease was not finished", r)
					}
				}
				if _, ok, _ := w.client("test").Get(context.Background(), Tool, lifecycle.ScopeGlobal); ok {
					t.Error("the global lease item was left behind")
				}
			} else if n := len(w.ev.list()); n != 0 {
				t.Errorf("report mode changed something: %v", w.ev.list())
			}
		})
	}
}

// TestGlobalTwoRegionsConcurrent: the two regions' teardowns run at the same
// moment, many times over; whatever the interleaving, exactly one plans the
// account-wide teardown.
func TestGlobalTwoRegionsConcurrent(t *testing.T) {
	for _, reap := range []bool{true, false} {
		for i := 0; i < 25; i++ {
			w := newWorld(t, west, region)
			w.idleTicks(reap, west, region)
			var mu sync.Mutex
			res := map[string]Result{}
			var wg sync.WaitGroup
			for _, r := range []string{west, region} {
				wg.Add(1)
				go func(r string) {
					defer wg.Done()
					x := Tick(context.Background(), w.cfg(r, reap), w.deps(r))
					mu.Lock()
					res[r] = x
					mu.Unlock()
				}(r)
			}
			wg.Wait()
			if got := planners(res); len(got) != 1 {
				t.Fatalf("reap=%t run %d: planned the globals: %v\n%s", reap, i, got, w.log())
			}
			if reap && (w.count("delete global profile") != 1 || w.count("create ") == 0) {
				t.Fatalf("reap run %d events:\n%s", i, strings.Join(w.ev.list(), "\n"))
			}
		}
	}
}

// TestGlobalActiveRegionBlocks: a region still in use, in its grace, or
// just deployed keeps the account-wide resources, in both modes.
func TestGlobalActiveRegionBlocks(t *testing.T) {
	cases := map[string]struct {
		set func(w *world)
		why string
	}{
		"active watch": {func(w *world) { w.src.Update(west, func(r *inventory.Region) { r.ActiveWatches = 1 }) }, "us-west-2 (1 active watch(es))"},
		"in its grace": {func(w *world) {
			// us-west-2 went idle an hour before us-east-1's teardown.
			w.advance(-time.Hour)
			if _, _, err := w.client("p").Observe(context.Background(), Tool, west, false); err != nil {
				w.t.Fatal(err)
			}
			w.advance(time.Hour)
		}, "us-west-2 (idle for 1h0m0s of the 24h0m0s grace)"},
		"just deployed": {func(w *world) {
			if err := w.client("alice").MarkActive(context.Background(), Tool, west); err != nil {
				w.t.Fatal(err)
			}
		}, "us-west-2 (state ACTIVE)"},
		"held": {func(w *world) {
			if err := w.client("spawn").AddHold(context.Background(), Tool, west, "spawn"); err != nil {
				w.t.Fatal(err)
			}
		}, "us-west-2 (held by [spawn])"},
		"a poller from before #326": {func(w *world) {}, "us-west-2 (its poller has no lifecycle item"},
	}
	for name, c := range cases {
		for _, reap := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/reap=%t", name, reap), func(t *testing.T) {
				w := newWorld(t, west, region)
				w.idleTicks(reap, region)
				c.set(w)
				r := Tick(context.Background(), w.cfg(region, reap), w.deps(region))
				if r.Plan == nil || r.Plan.Last || r.Plan.Global != nil {
					t.Fatalf("planned the globals with %s: %+v\n%s", name, r, w.log())
				}
				if !strings.Contains(r.Plan.LastWhy, "still active: "+c.why) {
					t.Errorf("why %q, want %q", r.Plan.LastWhy, c.why)
				}
				if strings.Contains(strings.Join(w.ev.list(), "\n"), "global") || w.count("create us-east-1 one-shot lagotto-teardown-role") != 0 {
					t.Errorf("events:\n%s", strings.Join(w.ev.list(), "\n"))
				}
			})
		}
	}
}

// TestGlobalLeaseReadErrorBlocks: another region's lifecycle item that cannot
// be read counts as active (fail safe): nobody plans the globals.
func TestGlobalLeaseReadErrorBlocks(t *testing.T) {
	for _, reap := range []bool{true, false} {
		w := newWorld(t, west, region)
		w.idleTicks(reap, west, region)
		w.table(west).Err = func(op string, _ any) error {
			if op == "GetItem" {
				return errors.New("ThrottlingException: rate exceeded")
			}
			return nil
		}
		r := Tick(context.Background(), w.cfg(region, reap), w.deps(region))
		if r.Plan == nil || r.Plan.Last || r.Plan.Global != nil ||
			!strings.Contains(r.Plan.LastWhy, "us-west-2 (could not read its lifecycle item") {
			t.Fatalf("reap=%t: %+v\n%s", reap, r, w.log())
		}
		if reap && (r.Outcome != OutcomeReaped || w.count("delete global") != 0) {
			t.Errorf("reap: %+v %v", r, w.ev.list())
		}
	}
}

// TestGlobalLeaseUnavailable: the global lease cannot be taken (another
// region holds it for the whole wait, or its table fails). Nothing global is
// planned; in reap mode the regional lease is left held, since ending it
// could leave nobody to decide, and in report mode it is released.
func TestGlobalLeaseUnavailable(t *testing.T) {
	for _, reap := range []bool{true, false} {
		for _, how := range []string{"held", "broken"} {
			w := newWorld(t, west, region)
			w.idleTicks(reap, west, region)
			ctx := context.Background()
			switch how {
			case "held":
				other := w.client("lagotto-poller/eu-central-1")
				if _, _, err := other.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
					t.Fatal(err)
				}
				if _, err := other.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
					t.Fatal(err)
				}
			case "broken":
				// The control region's table (us-east-1's own) fails.
				w.table(ControlRegion(west)).Err = func(string, any) error { return errors.New("InternalServerError") }
			}
			cfg := w.cfg(west, reap)
			cfg.GlobalWait = 10 * time.Second
			r := Tick(ctx, cfg, w.deps(west))
			if r.Plan == nil || r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "could not take the global teardown lease") || r.Err == nil {
				t.Fatalf("reap=%t %s: %+v\n%s", reap, how, r, w.log())
			}
			it, ok := w.item(west)
			switch {
			case reap && (r.Outcome != OutcomeFailed || !ok || it.Lease == nil):
				t.Errorf("reap %s: %+v, item %+v %v", how, r, it, ok)
			case !reap && (r.Outcome != OutcomeReported || it.Lease != nil):
				t.Errorf("report %s: %+v, item %+v", how, r, it)
			}
		}
	}
}

// TestGlobalAfterAFinishedRegion: us-west-2 finished hours ago (its item gone,
// its function waiting for its job). us-east-1, the last, decides alone.
func TestGlobalAfterAFinishedRegion(t *testing.T) {
	w := newWorld(t, west, region)
	w.src.Update(west, func(r *inventory.Region) {
		*r = inventory.Region{Region: west, Function: r.Function, LogGroup: r.LogGroup, FunctionRole: r.FunctionRole,
			Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}}
	})
	w.idleTicks(true, region)
	r := Tick(context.Background(), w.cfg(region, true), w.deps(region))
	if r.Outcome != OutcomeReaped || !r.Plan.Last || r.Plan.Global == nil {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if strings.Contains(w.log(), "phase=global claim=ok") {
		t.Error("took the global lease with no other region left to decide")
	}
	if !strings.Contains(w.log(), `other_region=us-west-2 standing=done why="no lifecycle item; its poller is handed to its one-shot job"`) {
		t.Errorf("log:\n%s", w.log())
	}
}

func TestJudge(t *testing.T) {
	now := t0
	grace := 24 * time.Hour
	poller := &inventory.Item{Name: deploy.PollerFunctionName}
	live := &lifecycle.LeaseInfo{Owner: "lagotto-poller/us-west-2", Heartbeat: now}
	for name, c := range map[string]struct {
		rf    inventory.Region
		it    lifecycle.Item
		ok    bool
		err   error
		stale bool
		want  Standing
		why   string
	}{
		"read error":            {err: errors.New("boom"), want: StandingActive, why: "could not read its lifecycle item: boom"},
		"activity unknown":      {rf: inventory.Region{ActivityUnknown: true}, want: StandingActive, why: "activity could not be read"},
		"tearing down":          {it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: live}, ok: true, want: StandingPending},
		"stalled teardown":      {it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: live}, ok: true, stale: true, want: StandingActive, why: "stalled"},
		"idle past the grace":   {it: lifecycle.Item{State: lifecycle.StateIdle, IdleSince: now.Add(-25 * time.Hour)}, ok: true, want: StandingPending, why: "past the 24h0m0s grace"},
		"idle within the grace": {it: lifecycle.Item{State: lifecycle.StateIdle, IdleSince: now.Add(-time.Hour)}, ok: true, want: StandingActive, why: "of the 24h0m0s grace"},
		"released or active":    {it: lifecycle.Item{State: lifecycle.StateActive}, ok: true, want: StandingActive, why: "state ACTIVE"},
		"pre-#326 poller":       {rf: inventory.Region{Function: poller}, want: StandingActive, why: "no lifecycle item"},
		"finished, going":       {rf: inventory.Region{Function: poller, Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}}, want: StandingDone},
		"nothing of lagotto's":  {want: StandingDone, why: "no lifecycle item"},
	} {
		c.rf.Region = west
		o := judge(&c.rf, c.it, c.ok, c.err, c.stale, grace, now)
		if o.Standing != c.want || !strings.Contains(o.Why, c.why) || o.Region != west {
			t.Errorf("%s: %+v, want %s %q", name, o, c.want, c.why)
		}
	}
}

func TestDecideGlobal(t *testing.T) {
	all := &inventory.Facts{AllRegions: true}
	pending := []OtherRegion{{Region: west, Standing: StandingPending}, {Region: "eu-west-1", Standing: StandingDone}}
	for name, c := range map[string]struct {
		f      *inventory.Facts
		others []OtherRegion
		reap   bool
		last   bool
		why    string
	}{
		"one region read":  {f: &inventory.Facts{}, last: false, why: "did not read every enabled region"},
		"account unread":   {f: &inventory.Facts{AllRegions: true, AccountIncomplete: true}, why: "could not all be read"},
		"pending, reap":    {f: all, others: pending, reap: true, why: "finishing after this region"},
		"pending, report":  {f: all, others: pending, last: true, why: "report mode"},
		"active wins":      {f: all, others: append([]OtherRegion{{Region: "ap-south-1", Standing: StandingActive, Why: "x"}}, pending...), why: "still active: ap-south-1 (x)"},
		"all done, report": {f: all, others: pending[1:], last: true, why: "no other enabled region"},
	} {
		last, why := decideGlobal(c.f, c.others, c.reap)
		if last != c.last || !strings.Contains(why, c.why) {
			t.Errorf("%s: %v %q", name, last, why)
		}
	}
}

func TestControlRegion(t *testing.T) {
	for in, want := range map[string]string{"us-west-2": "us-east-1", "eu-west-1": "us-east-1", "": "us-east-1",
		"us-gov-east-1": "us-gov-west-1", "cn-northwest-1": "cn-north-1"} {
		if got := ControlRegion(in); got != want {
			t.Errorf("ControlRegion(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestGlobalStepFailures: what the last region does when its account-wide
// work fails. Its own region is torn down either way; the failure is in the
// result and the log, and nothing after the failed step is tried.
func TestGlobalStepFailures(t *testing.T) {
	ctx := context.Background()
	t.Run("the profile's delete fails", func(t *testing.T) {
		w := newWorld(t, region)
		w.failProfile = true
		w.idleTicks(true, region)
		r := Tick(ctx, w.cfg(region, true), w.deps(region))
		if r.Outcome != OutcomeReaped || r.Err == nil || !r.Plan.Last || r.Plan.Failed() == 0 {
			t.Fatalf("%+v\n%s", r, w.log())
		}
		if n := w.count("create us-east-1 one-shot lagotto-teardown-role"); n != 0 {
			t.Errorf("created %d role jobs after a failed delete", n)
		}
	})
	t.Run("the role jobs fail", func(t *testing.T) {
		w := newWorld(t, region)
		w.failRoleJobs = true
		w.idleTicks(true, region)
		r := Tick(ctx, w.cfg(region, true), w.deps(region))
		if r.Outcome != OutcomeReaped || r.Err == nil || !strings.Contains(w.log(), `phase=global error="create one-shot teardown job lagotto-teardown-role-inline`) {
			t.Fatalf("%+v\n%s", r, w.log())
		}
	})
	t.Run("the footprint cannot be read again", func(t *testing.T) {
		w := newWorld(t, region)
		w.idleTicks(false, region)
		// The global step's first read of the other region's item.
		w.table("eu-west-1").Hook = func(_ context.Context, op string) {
			if op == "GetItem" {
				w.src.Fail("global EnabledRegions", errors.New("UnauthorizedOperation"))
			}
		}
		r := Tick(ctx, w.cfg(region, false), w.deps(region))
		if r.Outcome != OutcomeReported || r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "could not read the footprint again") {
			t.Fatalf("%+v\n%s", r, w.log())
		}
	})
	t.Run("a creator takes the regional lease over", func(t *testing.T) {
		w := newWorld(t, west, region)
		w.idleTicks(true, west, region)
		creator, err := lifecycle.New(lifecycle.Options{
			DynamoDB: func(r string) (lifecycle.API, error) { return w.table(r), nil },
			Owner:    "alice",
			Now:      func() time.Time { return w.clock().Add(time.Hour) }, // west's lease looks stale to it
		})
		if err != nil {
			t.Fatal(err)
		}
		once := false
		w.table(ControlRegion(west)).Hook = func(ctx context.Context, op string) {
			if op == "UpdateItem" && !once { // the global claim
				once = true
				if err := creator.MarkActive(ctx, Tool, west); err != nil {
					t.Errorf("creator: %v", err)
				}
			}
		}
		r := Tick(ctx, w.cfg(west, true), w.deps(west))
		if r.Outcome != OutcomeLeaseLost || r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "could not end this region's lease") {
			t.Fatalf("%+v\n%s", r, w.log())
		}
		if _, ok, _ := w.client("test").Get(ctx, Tool, lifecycle.ScopeGlobal); ok {
			t.Error("the global lease was not given back")
		}
	})
	t.Run("the wait is cancelled", func(t *testing.T) {
		w := newWorld(t, west, region)
		w.idleTicks(false, west, region)
		other := w.client("lagotto-poller/eu-central-1")
		if _, _, err := other.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
			t.Fatal(err)
		}
		if _, err := other.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
			t.Fatal(err)
		}
		cfg := w.cfg(west, false)
		cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
		r := Tick(ctx, cfg, w.deps(west))
		if r.Plan.Last || !errors.Is(r.Err, context.Canceled) {
			t.Fatalf("%+v", r)
		}
	})
}

func TestSleep(t *testing.T) {
	if err := sleep(context.Background(), time.Millisecond); err != nil {
		t.Errorf("sleep: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled sleep: %v", err)
	}
}
