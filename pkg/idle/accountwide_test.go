package idle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// awWorld is the account-wide teardown against fakes: two regions, each
// with its own lifecycle table, and the global scope in us-east-1.
type awWorld struct {
	t      *testing.T
	src    *inventorytest.Source
	tables map[string]*lifecycletest.Fake
	ev     *events
	acct   *awAccount
	mu     sync.Mutex
	now    time.Time
	logs   []string
}

const cliOwner = "arn:aws:iam::123456789012:user/alice"

func newAWWorld(t *testing.T) *awWorld {
	t.Helper()
	w := &awWorld{t: t, src: inventorytest.New(region, "us-west-2"), ev: &events{}, now: t0,
		tables: map[string]*lifecycletest.Fake{region: lifecycletest.New(), "us-west-2": lifecycletest.New()}}
	w.src.SetAccount(inventorytest.Account(acct))
	w.acct = &awAccount{e: w.ev, fail: map[string]error{}}
	return w
}

func (w *awWorld) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *awWorld) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

func (w *awWorld) client(owner string) *lifecycle.Client {
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return w.tables[r], nil },
		ControlRegion: region, Owner: owner, Now: w.clock,
		Sleep: func(context.Context, time.Duration) error { w.advance(time.Second); return nil },
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

func (w *awWorld) cfg(reap bool, grace time.Duration) AccountWideConfig {
	return AccountWideConfig{AccountID: acct, Owner: cliOwner, Reap: reap, Grace: grace, Now: w.clock,
		Sleep: func(context.Context, time.Duration) error { w.advance(5 * time.Second); return nil },
		Logf: func(f string, a ...any) {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.logs = append(w.logs, fmt.Sprintf(f, a...))
		}}
}

func (w *awWorld) deps() AccountWideDeps {
	return AccountWideDeps{Lifecycle: w.client(cliOwner), Source: w.src, Deleters: inventory.Deleters{
		AccountID: acct,
		Region:    func(r string) inventory.RegionDeleter { return recRegion{w.ev, r} },
		Account:   w.acct,
	}}
}

func (w *awWorld) run(reap bool, grace time.Duration) AccountWideResult {
	return TearDownAccountWide(context.Background(), w.cfg(reap, grace), w.deps())
}

func (w *awWorld) log() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.logs, "\n")
}

func (w *awWorld) global() (lifecycle.Item, bool) {
	it, ok, err := w.client("reader").Get(context.Background(), Tool, lifecycle.ScopeGlobal)
	if err != nil {
		w.t.Fatal(err)
	}
	return it, ok
}

// writes counts every lifecycle write in every region.
func (w *awWorld) writes() int {
	n := 0
	for _, f := range w.tables {
		for _, op := range []string{"PutItem", "UpdateItem", "DeleteItem", "TransactWriteItems", "CreateTable", "DeleteTable"} {
			n += f.Calls(op)
		}
	}
	return n
}

// awAccount records the account-wide deletes, with a hook before each and
// failures by name; it is what the IAM is, so a delete removes it from src
// through gone.
type awAccount struct {
	e      *events
	fail   map[string]error
	before func(what string)
}

func (a *awAccount) do(what string) (bool, error) {
	if a.before != nil {
		a.before(what)
	}
	if err := a.fail[what]; err != nil {
		return false, err
	}
	a.e.add("delete global %s", what)
	return true, nil
}

func (a *awAccount) DeleteRole(_ context.Context, n string) (bool, error) { return a.do("role " + n) }
func (a *awAccount) DeleteInstanceProfile(_ context.Context, n string) (bool, error) {
	return a.do("profile " + n)
}
func (a *awAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	ok, err := a.do("policy")
	return cliiam.Deletion{Deleted: ok}, err
}

var allAccountWide = []string{
	"delete global profile " + runtimeiam.InstanceRoleName,
	"delete global role " + runtimeiam.InstanceRoleName,
	"delete global role " + runtimeiam.RoleName,
	"delete global role " + runtimeiam.SchedulerInvokeRoleName,
	"delete global policy",
}

func (w *awWorld) deleted(t *testing.T, want ...string) {
	t.Helper()
	got := strings.Join(w.ev.list(), "\n")
	for _, d := range want {
		if !strings.Contains(got, d) {
			t.Errorf("not deleted: %s\n%s", d, got)
		}
	}
}

// TestAccountWideReapRemovesEverything: no poller, no job, no item anywhere:
// one explicit teardown (no grace) removes every account-wide resource, the
// global item and the control table.
func TestAccountWideReapRemovesEverything(t *testing.T) {
	w := newAWWorld(t)
	r := w.run(true, 0)
	if r.Outcome != AccountWideRemoved || r.Err != nil {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	w.deleted(t, allAccountWide...)
	if len(w.ev.list()) != len(allAccountWide) {
		t.Errorf("deletes %v", w.ev.list())
	}
	if _, ok := w.global(); ok {
		t.Error("the global item is left")
	}
	if n := w.tables[region].Calls("DeleteTable"); n != 1 {
		t.Errorf("the control table was not closed (%d)", n)
	}
	// The order: the profile before its role, the invoke role last of the roles.
	ev := strings.Join(w.ev.list(), ",")
	if strings.Index(ev, "profile "+runtimeiam.InstanceRoleName) > strings.Index(ev, "role "+runtimeiam.InstanceRoleName) {
		t.Errorf("order %s", ev)
	}
	// Nothing left: the next run is "nothing", and writes nothing.
	w.src.SetAccount(inventory.Account{})
	before := w.writes()
	if r := w.run(true, 0); r.Outcome != AccountWideNothing || w.writes() != before {
		t.Errorf("after: %+v (%d writes)", r, w.writes()-before)
	}
}

// TestAccountWideReportWritesNothing: report mode reads and plans, and
// writes nothing at all: no lifecycle write in any region, no delete.
func TestAccountWideReportWritesNothing(t *testing.T) {
	w := newAWWorld(t)
	if r := w.run(false, DefaultGrace); r.Outcome != AccountWideWaiting || !strings.Contains(r.Why, "grace has not started") {
		t.Errorf("with a grace: %+v", r)
	}
	r := w.run(false, 0)
	if r.Outcome != AccountWideWould || r.Plan == nil || r.Result.Count(fp.OutcomeWouldDelete) != len(allAccountWide) {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if n := w.writes(); n != 0 || len(w.ev.list()) != 0 {
		t.Errorf("report mode wrote %d times, deleted %v", n, w.ev.list())
	}
	if !strings.Contains(w.log(), "account-wide REPORT outcome=would-delete type=AWS::IAM::Role name=\"lagotto-capacity-poller-role\"") {
		t.Errorf("log:\n%s", w.log())
	}
}

// TestAccountWideBlockers: each condition keeps everything, in either mode,
// and nothing is deleted.
func TestAccountWideBlockers(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		set  func(w *awWorld)
		want string
	}{
		"a poller": {func(w *awWorld) { w.src.SetRegion(inventorytest.Deployed("us-west-2", acct)) }, "us-west-2: a poller is deployed (it removes its region once idle for 24h0m0s)"},
		"an idle poller with its own grace": {func(w *awWorld) {
			r := inventorytest.Deployed("us-west-2", acct)
			r.Function.IdleGrace = "30m"
			w.src.SetRegion(r)
			w.advance(-10 * time.Minute)
			if _, _, err := w.client(PollerOwner("us-west-2")).Observe(context.Background(), Tool, "us-west-2", false); err != nil {
				t.Fatal(err)
			}
			w.advance(10 * time.Minute)
		}, "us-west-2: a poller is deployed (it removes its region once idle for 30m0s; idle for 10m0s)"},
		"a pending job": {func(w *awWorld) {
			w.src.SetRegion(inventory.Region{Region: "us-west-2", Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup,
				Name: FunctionJobName}}})
		}, "us-west-2: one-shot job lagotto-teardown-function is pending"},
		"an ACTIVE item (watch --local)": {func(w *awWorld) {
			if err := w.client("bob").MarkActive(ctx, Tool, "us-west-2"); err != nil {
				t.Fatal(err)
			}
		}, "us-west-2: its lifecycle item is ACTIVE"},
		"a live regional lease": {func(w *awWorld) {
			c := w.client(PollerOwner("us-west-2"))
			if _, _, err := c.Observe(ctx, Tool, "us-west-2", false); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Claim(ctx, Tool, "us-west-2", 0); err != nil {
				t.Fatal(err)
			}
		}, "us-west-2: a teardown holds its lease (lagotto-poller/us-west-2)"},
		"a hold": {func(w *awWorld) {
			if err := w.client("bob").AddHold(ctx, Tool, "us-west-2", "spawn"); err != nil {
				t.Fatal(err)
			}
		}, "us-west-2: held by"},
		"an item that cannot be read": {func(w *awWorld) {
			w.tables["us-west-2"].Err = func(op string, _ any) error {
				if op == "GetItem" {
					return errors.New("network down")
				}
				return nil
			}
		}, "us-west-2: could not read its lifecycle item"},
		"a region that cannot be read": {func(w *awWorld) { w.src.Fail("us-west-2 Poller", errors.New("AccessDenied")) }, "us-west-2: could not be read in full"},
		"IAM that cannot be read":      {func(w *awWorld) { w.src.Fail("global CLIPolicy", errors.New("AccessDenied")) }, "the footprint could not be read in full"},
		"an active region": {func(w *awWorld) {
			w.src.SetRegion(inventory.Region{Region: "us-west-2", Tables: inventorytest.Deployed("us-west-2", acct).Tables, ActiveWatches: 1})
		}, "us-west-2: in use"},
	}
	for name, c := range cases {
		for _, reap := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reap=%t", name, reap), func(t *testing.T) {
				w := newAWWorld(t)
				c.set(w)
				r := w.run(reap, 0)
				if r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, "\n"), c.want) {
					t.Errorf("%+v\n%s", r, w.log())
				}
				if len(w.ev.list()) != 0 {
					t.Errorf("deleted %v", w.ev.list())
				}
				if _, ok := w.global(); ok {
					t.Error("a blocked run made a global item")
				}
			})
		}
	}
}

// TestAccountWideGrace: from the last mile (a grace), the first idle run
// starts the global grace; a creator's mark starts it over; past it the
// resources go.
func TestAccountWideGrace(t *testing.T) {
	w := newAWWorld(t)
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideWaiting {
		t.Fatalf("first: %+v\n%s", r, w.log())
	}
	if g, ok := w.global(); !ok || g.State != lifecycle.StateIdle {
		t.Fatalf("the grace did not start: %+v", g)
	}
	w.advance(20 * time.Hour)
	if r := w.run(false, DefaultGrace); r.Outcome != AccountWideWaiting || !strings.Contains(r.Why, "idle for 20h0m0s of its 24h0m0s grace") {
		t.Errorf("report within the grace: %+v", r)
	}
	// A creator (setup) marks the global scope: the grace starts over.
	if err := MarkActive(context.Background(), w.client("bob"), lifecycle.ScopeGlobal, "set up"); err != nil {
		t.Fatal(err)
	}
	w.advance(5 * time.Hour)
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideWaiting || len(w.ev.list()) != 0 {
		t.Fatalf("after the mark: %+v", r)
	}
	w.advance(DefaultGrace + time.Minute)
	if r := w.run(false, DefaultGrace); r.Outcome != AccountWideWould {
		t.Errorf("report past the grace: %+v", r)
	}
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideRemoved {
		t.Fatalf("past the grace: %+v\n%s", r, w.log())
	}
	w.deleted(t, allAccountWide...)

	// In use: an ACTIVE regional item sets an IDLE global scope back to
	// ACTIVE in reap mode, so its grace starts over.
	w = newAWWorld(t)
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideWaiting {
		t.Fatal(r)
	}
	if err := w.client("bob").MarkActive(context.Background(), Tool, "us-west-2"); err != nil {
		t.Fatal(err)
	}
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideBlocked {
		t.Fatal(r)
	}
	if g, _ := w.global(); g.State != lifecycle.StateActive {
		t.Errorf("the global scope stayed %s", g.State)
	}
}

// TestAccountWideCreatorFences: a creator that marks the global scope while
// the teardown deletes takes its lease away; the next delete does not run.
func TestAccountWideCreatorFences(t *testing.T) {
	w := newAWWorld(t)
	bob := w.client("bob")
	n := 0
	w.acct.before = func(string) {
		if n++; n == 2 {
			// bob's MarkActive waits out the live lease; here it is the
			// stale-lease takeover after the wait, as a creator would.
			w.advance(21 * time.Minute)
			if err := bob.MarkActive(context.Background(), Tool, lifecycle.ScopeGlobal); err != nil {
				t.Error(err)
			}
		}
	}
	r := w.run(true, 0)
	if r.Outcome != AccountWideLeaseLost || !errors.Is(r.Err, lifecycle.ErrLeaseLost) {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if len(w.ev.list()) != 2 {
		t.Errorf("kept deleting after the mark: %v", w.ev.list())
	}
	if g, ok := w.global(); !ok || g.State != lifecycle.StateActive || g.Lease != nil {
		t.Errorf("global %+v", g)
	}
}

// TestAccountWideRecheckBeforeEachDelete: a poller deployed mid-run (a
// creator that never marked the global scope: an older lagotto) is seen
// before the next delete: the poller role stays while its function exists.
func TestAccountWideRecheckBeforeEachDelete(t *testing.T) {
	for _, appear := range []struct {
		name string
		set  func(w *awWorld)
	}{
		{"a poller", func(w *awWorld) { w.src.SetRegion(inventorytest.Deployed("us-west-2", acct)) }},
		{"a regional item", func(w *awWorld) {
			if _, _, err := w.client("p").Observe(context.Background(), Tool, "us-west-2", false); err != nil {
				t.Error(err)
			}
		}},
		{"an unreadable region", func(w *awWorld) { w.src.Fail("us-west-2 Schedules", errors.New("throttled")) }},
	} {
		t.Run(appear.name, func(t *testing.T) {
			w := newAWWorld(t)
			w.acct.before = func(string) {
				if len(w.ev.list()) == 1 {
					appear.set(w)
				}
			}
			r := w.run(true, 0)
			if r.Outcome != AccountWideHeld || !errors.Is(r.Err, errAccountWideBlocked) {
				t.Fatalf("%+v\n%s", r, w.log())
			}
			// The hook runs inside the second delete; the third's re-check
			// stops the run, before the poller role.
			if len(w.ev.list()) != 2 || strings.Contains(strings.Join(w.ev.list(), ","), runtimeiam.RoleName) {
				t.Errorf("deletes %v", w.ev.list())
			}
			if g, ok := w.global(); !ok || g.Lease == nil {
				t.Errorf("the global lease was not left held: %+v", g)
			}
		})
	}
}

// TestAccountWideFailureLeavesTheLeaseHeld: a delete that fails stops the
// run with the global lease held; a creator meanwhile waits; the next run,
// once the lease is stale, resumes (a grace is not waited again) and
// finishes.
func TestAccountWideFailureLeavesTheLeaseHeld(t *testing.T) {
	w := newAWWorld(t)
	w.acct.fail["role "+runtimeiam.RoleName] = errors.New("AccessDenied: iam:DeleteRole")
	r := w.run(true, 0)
	if r.Outcome != AccountWideHeld || r.Err == nil || !strings.Contains(w.log(), "lease=held") {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	g, ok := w.global()
	if !ok || g.Lease == nil || g.Lease.Owner != cliOwner {
		t.Fatalf("global %+v", g)
	}
	// Another run while it is live waits, and writes nothing.
	before := w.writes()
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideWaiting || w.writes() != before {
		t.Errorf("while held: %+v", r)
	}
	// Report mode says reap mode would resume it.
	w.advance(21 * time.Minute)
	if r := w.run(false, DefaultGrace); r.Outcome != AccountWideWould {
		t.Errorf("report of a stale lease: %+v", r)
	}
	delete(w.acct.fail, "role "+runtimeiam.RoleName)
	if r := w.run(true, DefaultGrace); r.Outcome != AccountWideRemoved {
		t.Fatalf("resume: %+v\n%s", r, w.log())
	}
	w.deleted(t, "delete global role "+runtimeiam.RoleName)
	if _, ok := w.global(); ok {
		t.Error("the global item is left")
	}
}

// TestAccountWideAttachedPolicyStays: lagotto-cli-user while attached is
// kept, and does not keep the global item.
func TestAccountWideAttachedPolicyStays(t *testing.T) {
	w := newAWWorld(t)
	a := inventorytest.Account(acct)
	a.PolicyAttachments = 2
	w.src.SetAccount(a)
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if strings.Contains(strings.Join(w.ev.list(), ","), "policy") {
		t.Errorf("deleted an attached policy: %v", w.ev.list())
	}
	// Only the attached policy is left: nothing to do, nothing written.
	a.Roles, a.InstanceProfiles = nil, nil
	w.src.SetAccount(a)
	before := w.writes()
	if r := w.run(true, 0); r.Outcome != AccountWideNothing || w.writes() != before {
		t.Errorf("%+v", r)
	}
}

// TestAccountWideDeadItems: a region's item whose lease went stale, or that
// is IDLE, with no poller or job left there, does not block; reap mode
// finishes it and closes that region's table; a failure to finish holds.
func TestAccountWideDeadItems(t *testing.T) {
	ctx := context.Background()
	strand := func(w *awWorld) {
		c := w.client(PollerOwner("us-west-2"))
		if _, _, err := c.Observe(ctx, Tool, "us-west-2", false); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Claim(ctx, Tool, "us-west-2", 0); err != nil {
			t.Fatal(err)
		}
		w.advance(21 * time.Minute)
	}
	w := newAWWorld(t)
	strand(w)
	if r := w.run(false, 0); r.Outcome != AccountWideWould || !strings.Contains(w.log(), "dead_item=would-finish region=us-west-2") || w.writes() != 3 {
		t.Errorf("report: %+v (%d writes)\n%s", r, w.writes(), w.log())
	}
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if n := w.tables["us-west-2"].Calls("DeleteTable"); n != 1 || !strings.Contains(w.log(), "dead_item=finished region=us-west-2") {
		t.Errorf("us-west-2's table not closed (%d)\n%s", n, w.log())
	}

	// An IDLE item with no poller: a poller run that was in flight while
	// `teardown --plan` finished its region observed it again (lagotto#350).
	w = newAWWorld(t)
	if _, _, err := w.client(PollerOwner("us-west-2")).Observe(ctx, Tool, "us-west-2", false); err != nil {
		t.Fatal(err)
	}
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved || w.tables["us-west-2"].Calls("DeleteTable") != 1 {
		t.Errorf("a stray IDLE item: %+v\n%s", r, w.log())
	}

	// Something else of lagotto's left there (its teardown stopped after
	// the function's job ran) does not block either: it needs nothing
	// account-wide.
	w = newAWWorld(t)
	strand(w)
	w.src.SetRegion(inventory.Region{Region: "us-west-2", Topic: inventorytest.Deployed("us-west-2", acct).Topic})
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved || !strings.Contains(w.log(), "region=us-west-2 dead_item=TEARDOWN left=") {
		t.Errorf("with something left: %+v\n%s", r, w.log())
	}

	w = newAWWorld(t)
	strand(w)
	w.tables["us-west-2"].Err = func(op string, _ any) error {
		if op == "DeleteItem" {
			return errors.New("throttled")
		}
		return nil
	}
	if r := w.run(true, 0); r.Outcome != AccountWideHeld || len(w.ev.list()) != 0 {
		t.Errorf("finish failing: %+v %v", r, w.ev.list())
	}
}

// TestAccountWideOnly: `teardown --plan` limits the deletes to its plan.
func TestAccountWideOnly(t *testing.T) {
	w := newAWWorld(t)
	cfg := w.cfg(true, 0)
	cfg.Only = map[string]bool{runtimeiam.SchedulerInvokeRoleARN(acct): true}
	if r := TearDownAccountWide(context.Background(), cfg, w.deps()); r.Outcome != AccountWideRemoved {
		t.Fatalf("%+v", r)
	}
	if got := w.ev.list(); len(got) != 1 || got[0] != "delete global role "+runtimeiam.SchedulerInvokeRoleName {
		t.Errorf("deletes %v", got)
	}
}

// TestAccountWideReadFailures: each read that fails before the decision is
// "don't", and writes nothing.
func TestAccountWideReadFailures(t *testing.T) {
	w := newAWWorld(t)
	w.tables[region].Err = func(string, any) error { return errors.New("network down") }
	if r := w.run(true, 0); r.Outcome != AccountWideFailed || !strings.Contains(r.Why, "global item") {
		t.Errorf("global item: %+v", r)
	}
	w = newAWWorld(t)
	w.src.Fail("global EnabledRegions", errors.New("AccessDenied"))
	if r := w.run(true, 0); r.Outcome != AccountWideFailed || w.writes() != 0 {
		t.Errorf("regions: %+v", r)
	}
	w = newAWWorld(t)
	d := w.deps()
	d.Gather = func(context.Context) (inventory.Facts, error) { return inventory.Facts{}, errors.New("budget") }
	if r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d); r.Outcome != AccountWideFailed || w.writes() != 0 {
		t.Errorf("footprint: %+v", r)
	}
	// The footprint failing under the lease: held.
	w = newAWWorld(t)
	reads := 0
	d = w.deps()
	d.Gather = func(ctx context.Context) (inventory.Facts, error) {
		reads++
		f, err := inventory.Gather(ctx, w.src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
		w.src.Fail("global EnabledRegions", errors.New("throttled"))
		return f, err
	}
	if r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d); r.Outcome != AccountWideHeld || reads != 1 {
		t.Errorf("under the lease: %+v (%d cached reads)", r, reads)
	}
	// Outside the aws partition there is no global scope.
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return lifecycletest.New(), nil }})
	d = w.deps()
	d.Lifecycle = c
	if r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d); r.Outcome != AccountWideSkipped {
		t.Errorf("no control region: %+v", r)
	}
}

// TestAccountWideNewBlockerUnderTheLease: something that appears between the
// first read and the one under the lease gives the lease back.
func TestAccountWideNewBlockerUnderTheLease(t *testing.T) {
	w := newAWWorld(t)
	d := w.deps()
	d.Gather = func(ctx context.Context) (inventory.Facts, error) {
		f, err := inventory.Gather(ctx, w.src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
		w.src.SetRegion(inventorytest.Deployed("us-west-2", acct))
		return f, err
	}
	r := TearDownAccountWide(context.Background(), w.cfg(true, 0), d)
	if r.Outcome != AccountWideBlocked || len(w.ev.list()) != 0 {
		t.Fatalf("%+v", r)
	}
	if g, _ := w.global(); g.Lease != nil || g.State != lifecycle.StateActive {
		t.Errorf("lease not given back: %+v", g)
	}
}

// TestClaimGlobal: the wait for another actor's lease is bounded by time and
// by the context's deadline; a throttle is waited out; a lease in the
// caller's own name (a claim whose answer was lost) is recognised.
func TestClaimGlobal(t *testing.T) {
	ctx := context.Background()
	w := newAWWorld(t)
	other := w.client("carol")
	if _, _, err := other.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
		t.Fatal(err)
	}
	cfg := w.cfg(true, 0)
	cfg.Wait = 12 * time.Second
	start := w.clock()
	if _, err := claimGlobal(ctx, cfg.withDefaults(), w.client(cliOwner)); err == nil || !strings.Contains(err.Error(), "not free in 12s") ||
		w.clock().Sub(start) != 15*time.Second {
		t.Errorf("bounded wait: %v after %s", err, w.clock().Sub(start))
	}
	dl, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := claimGlobal(dl, cfg.withDefaults(), w.client(cliOwner)); err == nil || !strings.Contains(err.Error(), "within the run's time") {
		t.Errorf("deadline: %v", err)
	}
	cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	if _, err := claimGlobal(ctx, cfg.withDefaults(), w.client(cliOwner)); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}

	// Its own lease: the claim was applied, the answer lost.
	w = newAWWorld(t)
	me := w.client(cliOwner)
	if _, _, err := me.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
		t.Fatal(err)
	}
	w.tables[region].Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			w.tables[region].Err = nil
			if _, err := w.client(cliOwner).Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); err != nil {
				t.Error(err)
			}
			return errors.New("connection reset")
		}
		return nil
	}
	if _, err := claimGlobal(ctx, w.cfg(true, 0).withDefaults(), me); err == nil || !strings.Contains(err.Error(), ownLeaseNote) {
		t.Errorf("own lease: %v", err)
	}

	// A throttle is tried again.
	w = newAWWorld(t)
	me = w.client(cliOwner)
	if _, _, err := me.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
		t.Fatal(err)
	}
	throttled := 0
	w.tables[region].Err = func(op string, _ any) error {
		if op == "UpdateItem" && throttled == 0 {
			throttled++
			return &smithy.GenericAPIError{Code: "ThrottlingException", Message: "slow down"}
		}
		return nil
	}
	if l, err := claimGlobal(ctx, w.cfg(true, 0).withDefaults(), me); err != nil || l == nil {
		t.Errorf("throttled: %v", err)
	}
}
