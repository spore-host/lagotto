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
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	// poller role's one-shot jobs; failLogGroupJob the log group's;
	// failFinal the delete of a region's rate schedule; failTables a
	// table's delete.
	failProfile, failRoleJobs, failLogGroupJob, failFunctionJob, failFinal, failTables bool
}

const west = "us-west-2"

// newWorld deploys lagotto in each of deployed; eu-west-1 is enabled too,
// with nothing of lagotto's in it.
func newWorld(t *testing.T, deployed ...string) *world {
	t.Helper()
	w := &world{t: t, ev: &events{}, now: t0, acct: inventorytest.Account(acct), tables: map[string]*lifecycletest.Fake{}}
	enabled := []string{"eu-west-1"}
	for _, r := range deployed {
		if r != "eu-west-1" {
			enabled = append(enabled, r)
		}
	}
	w.src = inventorytest.New(enabled...)
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
		ControlRegion: region, // ControlRegion(any commercial region)
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
		if res.Plan != nil && res.Plan.Last && res.Plan.Global != nil {
			out = append(out, r)
		}
		if res.Plan != nil && res.Plan.Last && res.Plan.Global == nil && res.Err == nil {
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
	if d.w.failTables {
		return "", errors.New("ResourceInUseException: table busy")
	}
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
	if d.w.failFinal && n == deploy.PollerScheduleName {
		return false, errors.New("ThrottlingException: rate exceeded")
	}
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
	if (s.w.failRoleJobs && strings.HasPrefix(aws.ToString(in.Name), "lagotto-teardown-role")) ||
		(s.w.failLogGroupJob && aws.ToString(in.Name) == "lagotto-teardown-log-group") ||
		(s.w.failFunctionJob && aws.ToString(in.Name) == FunctionJobName) {
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

// globalItem is the global-scope lifecycle item, if any.
func (w *world) globalItem() (lifecycle.Item, bool) {
	it, ok, err := w.client("test").Get(context.Background(), Tool, lifecycle.ScopeGlobal)
	if err != nil {
		w.t.Fatal(err)
	}
	return it, ok
}

// finished makes r a region whose teardown finished: no lifecycle item, its
// function waiting for its one-shot job, its rate schedule gone.
func (w *world) finished(r string) {
	w.src.Update(r, func(x *inventory.Region) {
		*x = inventory.Region{Region: r, Function: x.Function, LogGroup: x.LogGroup, FunctionRole: x.FunctionRole,
			Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}}
	})
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
			if _, ok := w.globalItem(); ok {
				t.Error("the global lease item was left behind")
			}
			if !reap {
				if n := len(w.ev.list()); n != 0 {
					t.Errorf("report mode changed something: %v", w.ev.list())
				}
				return
			}
			if !strings.Contains(res[west].Plan.LastWhy, "finishing after this region") {
				t.Errorf("us-west-2 why: %q", res[west].Plan.LastWhy)
			}
			if w.count("delete global profile") != 1 || w.count("delete global role lagotto-instance-role") != 1 ||
				w.count("create us-east-1 one-shot lagotto-teardown-role") != 3 || w.count("create us-west-2 one-shot lagotto-teardown-role") != 0 {
				t.Errorf("events:\n%s", strings.Join(w.ev.list(), "\n"))
			}
			for _, r := range []string{west, region} {
				if _, ok := w.item(r); ok {
					t.Errorf("%s: the regional lease was not finished", r)
				}
			}
			if !strings.Contains(w.log(), "global_lease=finished") {
				t.Errorf("log:\n%s", w.log())
			}
		})
	}
}

// TestGlobalTwoRegionsConcurrent: the two regions' teardowns run at the same
// moment, many times over. In reap mode, whatever the interleaving, exactly
// one plans the account-wide teardown; report mode, which takes no lock,
// never plans it twice.
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
			got := planners(res)
			if (reap && len(got) != 1) || len(got) > 1 {
				t.Fatalf("reap=%t run %d: planned the globals: %v\n%s", reap, i, got, w.log())
			}
			if reap && (w.count("delete global profile") != 1 || w.count("create ") == 0) {
				t.Fatalf("reap run %d events:\n%s", i, strings.Join(w.ev.list(), "\n"))
			}
		}
	}
}

// TestGlobalActiveRegionBlocks: a region still in use, in its grace, or
// just deployed keeps the account-wide resources, in both modes, and the
// decision is final (the global lease is finished: that region decides
// later).
func TestGlobalActiveRegionBlocks(t *testing.T) {
	observe := func(w *world, active bool) {
		if _, _, err := w.client(PollerOwner(west)).Observe(context.Background(), Tool, west, active); err != nil {
			w.t.Fatal(err)
		}
	}
	cases := map[string]struct {
		set func(w *world)
		why string
		// held: no region with a live poller owes them, so the global
		// lease stays held.
		held bool
	}{
		"active watch": {set: func(w *world) {
			w.src.Update(west, func(r *inventory.Region) { r.ActiveWatches = 1 })
			observe(w, true)
		}, why: "us-west-2 (1 active watch(es))"},
		"in its grace": {set: func(w *world) {
			// us-west-2 went idle an hour before us-east-1's teardown.
			w.advance(-time.Hour)
			observe(w, false)
			w.advance(time.Hour)
		}, why: "us-west-2 (idle for 1h0m0s of its 24h0m0s grace)"},
		"in its own longer grace": {set: func(w *world) {
			// Idle 25 h, but its poller waits 72 h (LAGOTTO_IDLE_GRACE there).
			w.src.Update(west, func(r *inventory.Region) { r.Function.IdleGrace = "72h" })
			w.advance(-25 * time.Hour)
			observe(w, false)
			w.advance(25 * time.Hour)
		}, why: "us-west-2 (idle for 25h0m0s of its 72h0m0s grace)"},
		"just deployed": {set: func(w *world) {
			if err := w.client("alice").MarkActive(context.Background(), Tool, west); err != nil {
				w.t.Fatal(err)
			}
		}, why: "us-west-2 (state ACTIVE)"},
		"held": {set: func(w *world) {
			if err := w.client("spawn").AddHold(context.Background(), Tool, west, "spawn"); err != nil {
				w.t.Fatal(err)
			}
		}, why: "us-west-2 (held by [spawn])"},
		"a poller from before #326": {set: func(w *world) {}, why: "us-west-2 (its poller has no lifecycle item", held: true},
		// The CLI tears us-west-2 down: it never runs the poller's global
		// step, so it owes nothing (review of #343).
		"the CLI tears it down": {set: func(w *world) {
			cli := w.client("arn:aws:iam::123456789012:user/alice")
			observe(w, false)
			if _, err := cli.Claim(context.Background(), Tool, west, 0); err != nil {
				w.t.Fatal(err)
			}
		}, why: "us-west-2 (a teardown by arn:aws:iam::123456789012:user/alice holds its lease", held: true},
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
				if it, ok := w.globalItem(); reap && ok != c.held {
					t.Errorf("global item %+v present=%v, want present=%v", it, ok, c.held)
				}
			})
		}
	}
}

// TestGlobalLeaseReadError: another region's lifecycle item that cannot be
// read counts as active (fail safe): nobody plans the globals, and in reap
// mode the global lease is left held, to be resumed, since that region may
// in fact be done. A read that fails once and then succeeds is retried.
func TestGlobalLeaseReadError(t *testing.T) {
	for _, reap := range []bool{true, false} {
		w := newWorld(t, west, region)
		w.idleTicks(reap, region)
		w.finished(west)
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
		it, held := w.globalItem()
		switch {
		case reap && (r.Outcome != OutcomeReaped || w.count("delete global") != 0 || !held || it.Lease == nil):
			t.Errorf("reap: %+v %v, global item %+v %v", r, w.ev.list(), it, held)
		case !reap && held:
			t.Error("report mode wrote the global item")
		}
	}
	// Once only: retried, and the globals go.
	w := newWorld(t, west, region)
	w.idleTicks(true, region)
	w.finished(west)
	failed := false
	w.table(west).Err = func(op string, _ any) error {
		if op == "GetItem" && !failed {
			failed = true
			return errors.New("ThrottlingException: rate exceeded")
		}
		return nil
	}
	if r := Tick(context.Background(), w.cfg(region, true), w.deps(region)); !failed || r.Plan == nil || r.Plan.Global == nil {
		t.Fatalf("a read that failed once: %+v\n%s", r, w.log())
	}
}

// TestGlobalLeaseUnavailable: in reap mode the global lease cannot be taken
// (another region holds it for the whole wait, its table keeps failing, or
// the wait is cancelled). The poller takes it before its first delete, so it
// gives its regional lease back having deleted and created nothing, and
// tries again a grace period later. A failure that clears is retried.
func TestGlobalLeaseUnavailable(t *testing.T) {
	ctx := context.Background()
	for _, how := range []string{"held", "broken", "cancelled"} {
		w := newWorld(t, west, region)
		w.idleTicks(true, west, region)
		cfg := w.cfg(west, true)
		cfg.GlobalWait = 10 * time.Second
		switch how {
		case "held", "cancelled":
			other := w.client("lagotto-poller/eu-central-1")
			if _, _, err := other.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
				t.Fatal(err)
			}
			if _, err := other.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
				t.Fatal(err)
			}
			if how == "cancelled" {
				cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
			}
		case "broken":
			w.table(region).Err = func(string, any) error { return errors.New("InternalServerError") }
		}
		r := Tick(ctx, cfg, w.deps(west))
		if r.Outcome != OutcomeFailed || r.Plan == nil || r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "could not take the global teardown lease") {
			t.Fatalf("%s: %+v\n%s", how, r, w.log())
		}
		if how == "cancelled" && !errors.Is(r.Err, context.Canceled) {
			t.Errorf("cancelled: %v", r.Err)
		}
		if it, ok := w.item(west); !ok || it.Lease != nil || it.State != lifecycle.StateActive {
			t.Errorf("%s: the regional lease was not given back: %+v", how, it)
		}
		if n := len(w.ev.list()); n != 0 {
			t.Errorf("%s: deleted or created %d thing(s) before taking the global lease: %v", how, n, w.ev.list())
		}
	}

	// A control table that fails twice, then answers: retried in the wait.
	w := newWorld(t, west, region)
	w.idleTicks(true, west, region)
	n := 0
	w.table(region).Err = func(op string, _ any) error {
		if op == "TransactWriteItems" && n < 2 {
			n++
			return errors.New("ThrottlingException: rate exceeded")
		}
		return nil
	}
	if r := Tick(ctx, w.cfg(west, true), w.deps(west)); r.Outcome != OutcomeReaped || r.Err != nil {
		t.Fatalf("a transient failure: %+v\n%s", r, w.log())
	}
}

// TestGlobalReportCreatesNothing: report mode takes no global lease and so
// creates no table in the control region, even where lagotto has none
// (deployed only in us-west-2 and eu-west-1), and leaves a held global
// lease alone.
func TestGlobalReportCreatesNothing(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, west, "eu-west-1")
	w.src.Update("eu-west-1", func(r *inventory.Region) { r.ActiveWatches = 1 })
	w.idleTicks(false, west)
	r := Tick(ctx, w.cfg(west, false), w.deps(west))
	if r.Outcome != OutcomeReported || r.Plan.Last {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	control := w.table(region)
	for _, op := range []string{"CreateTable", "PutItem", "UpdateItem", "TransactWriteItems", "DeleteItem"} {
		if n := control.Calls(op); n != 0 {
			t.Errorf("report mode called %s %d time(s) in the control region", op, n)
		}
	}

	// A global lease held by a reap-mode poller: report mode leaves it.
	w = newWorld(t, west, region)
	w.idleTicks(false, west)
	w.finished(region)
	other := w.client("lagotto-poller/eu-central-1")
	if _, _, err := other.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
		t.Fatal(err)
	}
	r = Tick(ctx, w.cfg(west, false), w.deps(west))
	if r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "holds or owes the global lease (lagotto-poller/eu-central-1)") {
		t.Fatalf("%+v", r.Plan)
	}
	if it, ok := w.globalItem(); !ok || it.Lease == nil {
		t.Error("report mode touched the held global lease")
	}
	// Unreadable: not the last.
	w.table(region).Err = func(string, any) error { return errors.New("InternalServerError") }
	w.advance(25 * time.Hour)
	if res := Tick(ctx, w.cfg(west, false), w.deps(west)); res.Plan != nil && res.Plan.Last {
		t.Fatalf("an unreadable global lease: %+v", res.Plan)
	}
}

// TestGlobalAfterAFinishedRegion: us-west-2 finished hours ago (its item gone,
// its function waiting for its job). us-east-1, the last, removes them.
func TestGlobalAfterAFinishedRegion(t *testing.T) {
	w := newWorld(t, west, region)
	w.finished(west)
	w.idleTicks(true, region)
	r := Tick(context.Background(), w.cfg(region, true), w.deps(region))
	if r.Outcome != OutcomeReaped || !r.Plan.Last || r.Plan.Global == nil {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if !strings.Contains(w.log(), `other_region=us-west-2 standing=done why="no lifecycle item; its poller is handed to its one-shot job"`) {
		t.Errorf("log:\n%s", w.log())
	}
}

// TestGlobalTakesOverAStaleLease: a global lease left held (a failed step)
// goes stale; the CLI's last mile then sees it as a teardown to resume, and
// the next region's teardown takes it over, decides, and finishes it.
func TestGlobalTakesOverAStaleLease(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, west, region)
	w.finished(west)
	w.failProfile = true
	w.idleTicks(true, region)
	r := Tick(ctx, w.cfg(region, true), w.deps(region))
	if r.Err == nil || !strings.Contains(w.log(), "global_lease=held") {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	w.advance(21 * time.Minute)
	lc := w.client("lagotto last mile")
	if it, ok := w.globalItem(); !ok || !lc.LeaseStale(it) {
		t.Fatalf("the held lease is not stale after 20 min: %+v %v", it, ok)
	}
	// A later region (redeployed in eu-west-1, now idle) takes it over.
	w.failProfile = false
	w.src.SetRegion(inventorytest.Deployed("eu-west-1", acct))
	w.idleTicks(true, "eu-west-1")
	if r := Tick(ctx, w.cfg("eu-west-1", true), w.deps("eu-west-1")); r.Outcome != OutcomeReaped || r.Plan.Global == nil {
		t.Fatalf("takeover: %+v\n%s", r, w.log())
	}
	if _, ok := w.globalItem(); ok {
		t.Error("the stale global lease was not taken over and finished")
	}
	if w.count("delete global profile") != 1 {
		t.Errorf("events: %v", w.ev.list())
	}
}

// TestGlobalFence: a region that stops being done after the decision (a
// deploy there) stops the account-wide changes before the next one. A stop
// before the deletes leaves no Global; one before the jobs keeps what ran.
func TestGlobalFence(t *testing.T) {
	for _, before := range []int{2, 3} { // the deletes; the role jobs
		ctx := context.Background()
		w := newWorld(t, west, region)
		w.finished(west)
		w.idleTicks(true, region)
		reads, inHook := 0, false
		w.table(west).Hook = func(ctx context.Context, op string) {
			if op != "GetItem" || inHook {
				return
			}
			if reads++; reads == before {
				inHook = true
				if err := w.client("alice").MarkActive(ctx, Tool, west); err != nil {
					t.Errorf("deploy: %v", err)
				}
				inHook = false
			}
		}
		r := Tick(ctx, w.cfg(region, true), w.deps(region))
		if r.Outcome != OutcomeReaped || r.Plan.Last || !strings.Contains(r.Plan.LastWhy, "us-west-2 is no longer done") {
			t.Fatalf("fence %d: %+v\n%s", before, r, w.log())
		}
		if n := w.count("create us-east-1 one-shot lagotto-teardown-role"); n != 0 {
			t.Errorf("fence %d: created %d role jobs", before, n)
		}
		if deleted := w.count("delete global profile"); (before == 2) != (deleted == 0) {
			t.Errorf("fence %d: profile deleted %d time(s)", before, deleted)
		}
		// us-west-2's deploy has no live poller yet: nobody owes them, so the
		// global lease stays held (its new poller takes it over later).
		if it, ok := w.globalItem(); !ok || it.Lease == nil {
			t.Errorf("fence %d: the global lease was not left held", before)
		}
		if (before == 2) != (r.Plan.Global == nil) {
			t.Errorf("fence %d: Global = %+v", before, r.Plan.Global)
		}
	}
}

func TestJudge(t *testing.T) {
	now := t0
	poller := func(grace string) *inventory.Item {
		return &inventory.Item{Name: deploy.PollerFunctionName, IdleGrace: grace}
	}
	rate := []inventory.Schedule{{Group: "lagotto", Name: deploy.PollerScheduleName}}
	job := []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}
	livePoller := inventory.Region{Function: poller(""), Schedules: rate}
	tables := inventory.Region{Tables: []inventory.Table{{Item: inventory.Item{Name: "lagotto-watches"}}}}
	lease := &lifecycle.LeaseInfo{Owner: PollerOwner(west), Heartbeat: now}
	cliLease := &lifecycle.LeaseInfo{Owner: "alice", Heartbeat: now}
	idle := func(d time.Duration) lifecycle.Item {
		return lifecycle.Item{State: lifecycle.StateIdle, IdleSince: now.Add(-d)}
	}
	for name, c := range map[string]struct {
		rf    inventory.Region
		it    lifecycle.Item
		ok    bool
		err   error
		stale bool
		want  Standing
		why   string
		owes  bool
	}{
		"read error":       {rf: livePoller, err: errors.New("boom"), want: StandingActive, why: "could not read its lifecycle item: boom"},
		"activity unknown": {rf: inventory.Region{ActivityUnknown: true, Function: poller(""), Schedules: rate}, want: StandingActive, why: "activity could not be read"},
		"its poller tearing it down": {rf: livePoller, it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: lease}, ok: true,
			want: StandingPending, why: "its poller", owes: true},
		"the CLI tearing it down": {rf: livePoller, it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: cliLease}, ok: true,
			want: StandingActive, why: "a teardown by alice holds its lease"},
		"a poller lease, no live poller": {rf: inventory.Region{Function: poller(""), Schedules: job}, it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: lease},
			ok: true, want: StandingActive, why: "only its own poller decides"},
		"active, nothing left":         {it: lifecycle.Item{State: lifecycle.StateActive}, ok: true, want: StandingDone, why: "state ACTIVE), and nothing of lagotto's left"},
		"active, leftovers, no poller": {rf: tables, it: lifecycle.Item{State: lifecycle.StateActive}, ok: true, want: StandingActive, why: "no poller to tear it down"},
		"stalled, poller live": {rf: livePoller, it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: lease}, ok: true, stale: true,
			want: StandingActive, why: "its poller resumes it", owes: true},
		"stalled, poller gone": {rf: inventory.Region{Function: poller(""), Schedules: job}, it: lifecycle.Item{State: lifecycle.StateTeardown, Lease: lease},
			ok: true, stale: true, want: StandingDone, why: "the last mile reports it"},
		"idle past its grace":         {rf: livePoller, it: idle(25 * time.Hour), ok: true, want: StandingPending, why: "past its 24h0m0s grace", owes: true},
		"idle within its grace":       {rf: livePoller, it: idle(time.Hour), ok: true, want: StandingActive, why: "of its 24h0m0s grace", owes: true},
		"idle within its own grace":   {rf: inventory.Region{Function: poller("72h"), Schedules: rate}, it: idle(25 * time.Hour), ok: true, want: StandingActive, why: "of its 72h0m0s grace", owes: true},
		"idle past, no poller":        {rf: tables, it: idle(25 * time.Hour), ok: true, want: StandingActive, why: "no poller to tear it down"},
		"idle, no poller, nothing":    {it: idle(25 * time.Hour), ok: true, want: StandingDone, why: "nothing of lagotto's left"},
		"idle, poller without a rate": {rf: inventory.Region{Function: poller("")}, it: idle(25 * time.Hour), ok: true, want: StandingActive, why: "no poller"},
		"released or active":          {rf: livePoller, it: lifecycle.Item{State: lifecycle.StateActive}, ok: true, want: StandingActive, why: "state ACTIVE", owes: true},
		"pre-#326 poller":             {rf: livePoller, want: StandingActive, why: "has no lifecycle item"},
		"finished, going":             {rf: inventory.Region{Function: poller(""), Schedules: append(job, rate...)}, want: StandingDone, why: "handed to its one-shot job"},
		"tables only":                 {rf: tables, want: StandingDone, why: "no lifecycle item"},
		"nothing of lagotto's":        {want: StandingDone, why: "no lifecycle item"},
		"a function, no job, no rate": {rf: inventory.Region{Function: poller("")}, want: StandingDone, why: "no lifecycle item"},
	} {
		c.rf.Region = west
		o := judge(&c.rf, c.it, c.ok, c.err, c.stale, now)
		if o.Standing != c.want || !strings.Contains(o.Why, c.why) || o.Region != west || o.Owes != c.owes {
			t.Errorf("%s: %+v, want %s %q owes=%v", name, o, c.want, c.why, c.owes)
		}
		if c.why == "no lifecycle item" && strings.Contains(o.Why, "one-shot") {
			t.Errorf("%s: names a job that does not exist: %q", name, o.Why)
		}
	}
}

func TestDecideGlobal(t *testing.T) {
	all := &inventory.Facts{AllRegions: true}
	pending := []OtherRegion{{Region: west, Standing: StandingPending, Owes: true}, {Region: "eu-west-1", Standing: StandingDone}}
	unreadable := OtherRegion{Region: "ap-south-1", Standing: StandingActive, Why: "x"}
	inUse := OtherRegion{Region: "ap-south-2", Standing: StandingActive, Why: "y", Owes: true}
	for name, c := range map[string]struct {
		f      *inventory.Facts
		others []OtherRegion
		reap   bool
		last   bool
		owed   bool
		why    string
	}{
		"one region read":     {f: &inventory.Facts{}, why: "did not read every enabled region"},
		"account unread":      {f: &inventory.Facts{AllRegions: true, AccountIncomplete: true}, why: "could not all be read"},
		"pending, reap":       {f: all, others: pending, reap: true, owed: true, why: "finishing after this region"},
		"pending, report":     {f: all, others: pending, last: true, why: "report mode"},
		"in use wins":         {f: all, others: append([]OtherRegion{inUse}, pending...), owed: true, why: "still active: ap-south-2 (y)"},
		"unreadable only":     {f: all, others: []OtherRegion{unreadable}, reap: true, why: "ap-south-1 (x)"},
		"unreadable, in use":  {f: all, others: []OtherRegion{unreadable, inUse}, reap: true, owed: true, why: "ap-south-1 (x), ap-south-2 (y)"},
		"unreadable, pending": {f: all, others: append([]OtherRegion{unreadable}, pending...), reap: true, owed: true, why: "ap-south-1 (x)"},
		"all done":            {f: all, others: pending[1:], last: true, why: "no other enabled region"},
	} {
		last, why, owed := decideGlobal(c.f, c.others, c.reap)
		if last != c.last || owed != c.owed || !strings.Contains(why, c.why) {
			t.Errorf("%s: %v %v %q", name, last, owed, why)
		}
	}
}

func TestControlRegion(t *testing.T) {
	for in, want := range map[string]string{"us-west-2": "us-east-1", "eu-west-1": "us-east-1", "": "us-east-1",
		"ap-southeast-4": "us-east-1", "il-central-1": "us-east-1", "mx-central-1": "us-east-1",
		"us-gov-east-1": "us-gov-west-1", "cn-northwest-1": "cn-north-1"} {
		if got, err := ControlRegion(in); got != want || err != nil {
			t.Errorf("ControlRegion(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	// Partitions it does not know fail closed (lagotto#340 review).
	for _, in := range []string{"us-iso-east-1", "us-isob-east-1", "us-isof-south-1", "eusc-de-east-1", "xx-mars-1"} {
		if got, err := ControlRegion(in); got != "" || err == nil || !strings.Contains(err.Error(), in) {
			t.Errorf("ControlRegion(%q) = %q, %v, want an error", in, got, err)
		}
	}
}

// TestGlobalUnknownPartition: in reap mode a region whose partition has no
// known control region stops before its first delete.
func TestGlobalUnknownPartition(t *testing.T) {
	const iso = "us-iso-east-1"
	w := newWorld(t, iso)
	w.idleTicks(true, iso)
	r := Tick(context.Background(), w.cfg(iso, true), w.deps(iso))
	if r.Outcome != OutcomeFailed || !strings.Contains(r.Plan.LastWhy, "no control region for us-iso-east-1") || len(w.ev.list()) != 0 {
		t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
	}
	if it, ok := w.item(iso); !ok || it.Lease != nil {
		t.Errorf("the regional lease was not given back: %+v", it)
	}
}

// TestGlobalStepFailures: the last region's account-wide work fails. Its own
// region is torn down either way; the failure is in the result, nothing
// after the failed step is tried, and the global lease is left held, to be
// resumed (TestGlobalTakesOverAStaleLease).
func TestGlobalStepFailures(t *testing.T) {
	ctx := context.Background()
	for name, set := range map[string]func(w *world){
		"the profile's delete fails": func(w *world) { w.failProfile = true },
		"the role jobs fail":         func(w *world) { w.failRoleJobs = true },
		"the footprint cannot be read again": func(w *world) {
			// Once the regional lease is finished, the next read is the
			// global step's.
			w.table(region).Hook = func(_ context.Context, op string) {
				if op == "DeleteItem" {
					w.src.Fail("global EnabledRegions", errors.New("UnauthorizedOperation"))
				}
			}
		},
	} {
		w := newWorld(t, west, region)
		w.finished(west)
		w.idleTicks(true, region)
		set(w)
		r := Tick(ctx, w.cfg(region, true), w.deps(region))
		if r.Outcome != OutcomeReaped || r.Err == nil || !strings.Contains(w.log(), "global_lease=held") {
			t.Fatalf("%s: %+v\n%s", name, r, w.log())
		}
		if it, ok := w.globalItem(); !ok || it.Lease == nil {
			t.Errorf("%s: the global lease was not left held: %+v", name, it)
		}
		if _, ok := w.item(region); ok {
			t.Errorf("%s: the regional lease was not finished", name)
		}
		if name == "the profile's delete fails" && w.count("create us-east-1 one-shot lagotto-teardown-role") != 0 {
			t.Errorf("%s: created role jobs after a failed delete", name)
		}
	}

	// Report mode: a footprint that cannot be read again is not the last.
	w := newWorld(t, region)
	w.idleTicks(false, region)
	reads := 0
	d := w.deps(region)
	d.Source = hookSource{Source: w.src, before: func() {
		if reads++; reads == 2 { // the post-claim footprint, then the global step's
			w.src.Fail("global EnabledRegions", errors.New("UnauthorizedOperation"))
		}
	}}
	if r := Tick(ctx, w.cfg(region, false), d); r.Outcome != OutcomeReported || r.Plan.Last ||
		!strings.Contains(r.Plan.LastWhy, "could not read the footprint again") {
		t.Fatalf("report: %+v", r)
	}

	// A creator takes the regional lease over before the jobs: the poller
	// stops, and gives the global lease back.
	w = newWorld(t, west, region)
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
	w.table(region).Hook = func(ctx context.Context, op string) {
		if op == "UpdateItem" && !once { // the global claim
			once = true
			if err := creator.MarkActive(ctx, Tool, west); err != nil {
				t.Errorf("creator: %v", err)
			}
		}
	}
	r := Tick(ctx, w.cfg(west, true), w.deps(west))
	if r.Outcome != OutcomeLeaseLost || w.count("create us-west-2 one-shot") != 0 {
		t.Fatalf("lease lost: %+v %v\n%s", r, w.ev.list(), w.log())
	}
	if _, ok := w.globalItem(); ok {
		t.Error("the global lease was not given back")
	}
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

// hookSource runs before, once set, ahead of each EnabledRegions: the
// start of every footprint read.
type hookSource struct {
	inventory.Source
	before func()
}

func (h hookSource) EnabledRegions(ctx context.Context) ([]string, error) {
	h.before()
	return h.Source.EnabledRegions(ctx)
}

// TestGlobalDecidingRegionHoldsTheMutex is the interleaving CI found: region
// A has finished its own lease under the global lease and is about to read
// the footprint and the others, when region B reads the regional items,
// finds A's gone, and would decide alone (then A would find B gone too, and
// both would plan). B must see A's global lease and wait for it; then A
// defers to B, and B, the last, plans the globals: exactly one.
func TestGlobalDecidingRegionHoldsTheMutex(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, west, region)
	w.idleTicks(true, west, region)
	var bRes Result
	bDone, bWaiting := make(chan struct{}), make(chan struct{})
	finished, started := false, false
	w.table(west).Hook = func(_ context.Context, op string) {
		if op == "DeleteItem" { // A (us-west-2) finishes its regional lease
			finished = true
		}
	}
	runB := func() {
		if !finished || started {
			return
		}
		started = true
		cfg := w.cfg(region, true)
		var once sync.Once
		cfg.Sleep = func(context.Context, time.Duration) error {
			once.Do(func() { close(bWaiting) })
			time.Sleep(time.Millisecond)
			return nil
		}
		go func() {
			defer close(bDone)
			bRes = Tick(ctx, cfg, w.deps(region))
		}()
		select { // B has decided alone, or is waiting for A's global lease
		case <-bDone:
		case <-bWaiting:
		}
	}
	d := w.deps(west)
	d.Source = hookSource{Source: w.src, before: runB}
	aRes := Tick(ctx, w.cfg(west, true), d)
	if !started {
		t.Fatal("the interleaving was not reached")
	}
	<-bDone
	if got := planners(map[string]Result{west: aRes, region: bRes}); len(got) != 1 || got[0] != region {
		t.Fatalf("planned the globals: %v, want [us-east-1]\n%s", got, w.log())
	}
	if w.count("delete global profile") != 1 {
		t.Errorf("events:\n%s", strings.Join(w.ev.list(), "\n"))
	}
}

// TestGlobalInvariant is the review's invariant (DEPLOYMENT.md, "The
// account-wide resources"): the global item goes only when the account-wide
// resources are gone or a region with a live poller owes them. Before a
// region commits its removal it owes them itself; after, every failure leaves
// both leases held, never the regional item back at ACTIVE.
func TestGlobalInvariant(t *testing.T) {
	ctx := context.Background()
	held := func(t *testing.T, w *world) {
		t.Helper()
		if it, ok := w.globalItem(); !ok || it.Lease == nil {
			t.Errorf("the global lease was not left held: %+v %v\n%s", it, ok, w.log())
		}
	}

	t.Run("this region's Finish fails after it committed", func(t *testing.T) {
		w := newWorld(t, west, region)
		w.finished(region)
		w.idleTicks(true, west)
		w.table(west).Err = func(op string, _ any) error {
			if op == "DeleteItem" {
				return errors.New("ThrottlingException: rate exceeded")
			}
			return nil
		}
		r := Tick(ctx, w.cfg(west, true), w.deps(west))
		if r.Outcome != OutcomeFailed || !strings.Contains(w.log(), "lease=finish-failed global_lease=held") {
			t.Fatalf("%+v\n%s", r, w.log())
		}
		held(t, w)
		if w.count("delete global") != 0 {
			t.Errorf("decided after its own lease failed: %v", w.ev.list())
		}
	})

	for name, set := range map[string]func(w *world){
		"its final deletes fail":           func(w *world) { w.failFinal = true },
		"a job after the function's fails": func(w *world) { w.failLogGroupJob = true },
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t, west, region, "eu-west-1")
			w.finished(region)
			w.idleTicks(true, west, "eu-west-1")
			set(w)
			r := Tick(ctx, w.cfg(west, true), w.deps(west))
			if r.Outcome != OutcomeFailed || w.count("create us-west-2 one-shot "+FunctionJobName) != 1 {
				t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
			}
			held(t, w)
			it, ok := w.item(west)
			if !ok || it.Lease == nil || it.State != lifecycle.StateTeardown {
				t.Fatalf("the regional lease was released after the commit: %+v %v", it, ok)
			}
			// Stale, with its poller handed to its job, us-west-2 is done:
			// eu-west-1 later takes the global lease over and removes them.
			w.failFinal, w.failLogGroupJob = false, false
			w.advance(21 * time.Minute)
			r = Tick(ctx, w.cfg("eu-west-1", true), w.deps("eu-west-1"))
			if r.Outcome != OutcomeReaped || !r.Plan.Last || r.Plan.Global == nil {
				t.Fatalf("eu-west-1: %+v\n%s", r, w.log())
			}
			if !strings.Contains(w.log(), "other_region=us-west-2 standing=done") {
				t.Errorf("log:\n%s", w.log())
			}
			if _, ok := w.globalItem(); ok {
				t.Error("the global item was not finished")
			}
		})
	}

	t.Run("the global item cannot be deleted", func(t *testing.T) {
		w := newWorld(t, west, region)
		w.finished(region)
		w.idleTicks(true, west)
		globalKey := lifecycle.ItemKey(Tool, lifecycle.ScopeGlobal)
		w.table(region).Err = func(op string, in any) error {
			if di, ok := in.(*dynamodb.DeleteItemInput); ok && op == "DeleteItem" {
				if pk, ok := di.Key["pk"].(*ddbtypes.AttributeValueMemberS); ok && pk.Value == globalKey {
					return errors.New("ThrottlingException: rate exceeded")
				}
			}
			return nil
		}
		r := Tick(ctx, w.cfg(west, true), w.deps(west))
		if r.Outcome != OutcomeReaped || !r.Plan.Last || !strings.Contains(w.log(), "phase=global lease=finish-failed") ||
			!strings.Contains(w.log(), "global_lease=held") {
			t.Fatalf("%+v\n%s", r, w.log())
		}
	})

	t.Run("its function's job fails, before the commit", func(t *testing.T) {
		w := newWorld(t, west, region)
		w.idleTicks(true, west, region)
		w.failFunctionJob = true
		r := Tick(ctx, w.cfg(west, true), w.deps(west))
		if r.Outcome != OutcomeFailed {
			t.Fatalf("%+v\n%s", r, w.log())
		}
		if it, ok := w.item(west); !ok || it.Lease != nil || it.State != lifecycle.StateActive {
			t.Errorf("the regional lease was not given back: %+v", it)
		}
		if _, ok := w.globalItem(); ok {
			t.Error("the global item was kept, but this region's poller still owes the decision")
		}
	})

	t.Run("a failure before the commit closes the table the lease created", func(t *testing.T) {
		w := newWorld(t, west) // nothing of lagotto's in the control region
		w.idleTicks(true, west)
		w.failTables = true
		r := Tick(ctx, w.cfg(west, true), w.deps(west))
		if r.Outcome != OutcomeFailed {
			t.Fatalf("%+v\n%s", r, w.log())
		}
		control := w.table(region)
		if control.Calls("CreateTable") != 1 || control.Calls("DeleteTable") != 1 {
			t.Errorf("control table: %d CreateTable, %d DeleteTable", control.Calls("CreateTable"), control.Calls("DeleteTable"))
		}
	})
}
