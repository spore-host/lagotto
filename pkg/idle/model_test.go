package idle

// A model check of the idle teardown's account-wide decision (lagotto#340).
//
// It drives the real Tick (and the real lifecycle protocol) against in-memory
// fakes, under a cooperative scheduler: every actor runs in its own goroutine
// and parks at each step point (every lifecycle table operation, every
// delete, every one-shot job), and the explorer lets exactly one actor take
// one step at a time. A depth-first search over those choices (stateless,
// with replay) enumerates every interleaving within a bound on preemptions
// (switching away from an actor that could go on) and on crashes (an actor
// stopping for good at a step point, before that step). Bounds are raised one
// at a time, so the first counterexample found is a minimal one.
//
// Actors: a region's poller run (Tick, reap or report mode); a deploy or a
// watch in a region (MarkActive, then creating what it creates); the CLI's
// `lagotto teardown --plan --mode reap` (its lifecycle protocol: claim every
// region's lease, delete, finish); and the clock (past the 20 min stale
// lease). Once no actor is left, a drain runs every live poller (advancing
// the clock past the grace, firing the one-shot jobs that are due) until
// nothing changes, then the CLI teardown once, as a user would when the last
// mile reports residue.
//
// Properties:
//
//   - SAFETY (checked at the moment of every account-wide delete, and of the
//     creation of a job that deletes the poller role): no region has an
//     ACTIVE lifecycle item or an active watch.
//   - A creator that took a region's lease over keeps its poller: no region
//     ends a run with an ACTIVE item and its function's delete job pending.
//   - LIVENESS, after the pollers' drain: lagotto's account-wide resources,
//     and the global lease item, are either gone, or some poller that can
//     still run is left to act on them. And if every region with something of
//     lagotto's has a live poller, they are gone, with every lifecycle item
//     and table. After the CLI teardown, nothing of lagotto's is left.
//   - Report mode writes nothing to the global scope and deletes nothing.
//
// Not modelled: the one-shot jobs racing a later deploy (lagotto#345 has
// deploy mark the global scope), and IAM permissions other than the poller
// role's own (a poller whose role is gone or stripped cannot run).

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	"github.com/spore-host/lagotto/pkg/schedgroup"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// ---- the cooperative scheduler ------------------------------------------------

type actorKey struct{}

func actorName(ctx context.Context) string {
	if a, _ := ctx.Value(actorKey{}).(*actor); a != nil {
		return a.name
	}
	return ""
}

// errCrash is the panic an actor crashes with: it unwinds the actor's run,
// and the step it was about to take never happens.
var errCrash = errors.New("model: crashed")

type actor struct {
	name   string
	run    func(ctx context.Context)
	resume chan bool   // true: crash at the step point
	parked chan string // the step point it stopped at, or "" once done
	at     string
	ran    bool
	done   bool
}

// park is a step point: the actor reports where it is and waits for its
// turn.
func park(ctx context.Context, label string) {
	a, _ := ctx.Value(actorKey{}).(*actor)
	if a == nil {
		return // the drain, and the checks' own reads
	}
	a.parked <- label
	if crash := <-a.resume; crash {
		panic(errCrash)
	}
}

// option is one choice at a step: let actor go on, or crash it.
type option struct {
	actor int
	crash bool
}

// execution is one run of a scenario under one schedule.
type execution struct {
	prefix     []int
	chosen     []int
	widths     []int
	trace      []string
	preempts   int
	crashes    int
	maxPreempt int
	maxCrash   int
}

func (e *execution) options(actors []*actor, cur int) []option {
	var out []option
	curEnabled := cur >= 0 && !actors[cur].done
	if curEnabled {
		out = append(out, option{actor: cur})
	}
	for i, a := range actors {
		if a.done || i == cur {
			continue
		}
		if curEnabled && e.preempts >= e.maxPreempt {
			continue
		}
		out = append(out, option{actor: i})
	}
	if e.crashes < e.maxCrash {
		for i, a := range actors {
			if !a.done && a.ran && (i == cur || !curEnabled || e.preempts < e.maxPreempt) {
				out = append(out, option{actor: i, crash: true})
			}
		}
	}
	return out
}

// run drives actors to the end under e's schedule: prefix first, then the
// first option at every later step.
func (e *execution) run(actors []*actor) {
	for _, a := range actors {
		a.resume, a.parked = make(chan bool), make(chan string)
		go func(a *actor) {
			ctx := context.WithValue(context.Background(), actorKey{}, a)
			crash := <-a.resume
			func() {
				defer func() {
					if r := recover(); r != nil && r != errCrash { //nolint:errorlint // the sentinel itself
						panic(r)
					}
				}()
				if !crash {
					a.run(ctx)
				}
			}()
			a.parked <- ""
		}(a)
	}
	cur := -1
	for {
		opts := e.options(actors, cur)
		if len(opts) == 0 {
			return
		}
		i := 0
		if n := len(e.chosen); n < len(e.prefix) {
			i = e.prefix[n]
		}
		e.chosen, e.widths = append(e.chosen, i), append(e.widths, len(opts))
		o := opts[i]
		curEnabled := cur >= 0 && !actors[cur].done
		if o.crash {
			e.crashes++
		}
		if curEnabled && o.actor != cur {
			e.preempts++
		}
		a := actors[o.actor]
		step := a.name
		if a.ran {
			step += "@" + a.at
		}
		if o.crash {
			step = "CRASH " + step
		}
		e.trace = append(e.trace, step)
		a.ran = true
		a.resume <- o.crash
		if a.at = <-a.parked; a.at == "" {
			a.done = true
		}
		cur = o.actor
	}
}

// explore runs every schedule of build within the bounds, raising them one
// at a time, and returns how many it ran and, for each kind of violation
// (SAFETY, LIVENESS, ...), the first one found, with its schedule: a minimal
// one, as the bounds rise one at a time.
func explore(t *testing.T, build func() (*mworld, []*actor), maxPreempt, maxCrash int, separate bool) (int, []string) {
	t.Helper()
	runs := 0
	var found []string
	kinds := map[string]bool{}
	for p := 0; p <= maxPreempt; p++ {
		for c := 0; c <= maxCrash; c++ {
			if separate && p > 0 && c > 0 {
				continue
			}
			stack := [][]int{{}}
			for len(stack) > 0 {
				prefix := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				m, actors := build()
				e := &execution{prefix: prefix, maxPreempt: p, maxCrash: c}
				e.run(actors)
				runs++
				if e.preempts == p && e.crashes == c { // each schedule is checked at its own bounds
					m.finish(e.trace)
					if v := m.violation(); v != "" {
						kind := v
						if i := strings.IndexAny(v, ":,"); i > 0 {
							kind = v[:i]
						}
						if !kinds[kind] {
							kinds[kind] = true
							found = append(found, fmt.Sprintf("%s\n  schedule (preemptions %d, crashes %d):\n    %s", v, p, c, strings.Join(e.trace, "\n    ")))
						}
					}
				}
				for d := len(prefix); d < len(e.chosen); d++ {
					for alt := e.widths[d] - 1; alt >= 1; alt-- {
						next := append(append([]int(nil), e.chosen[:d]...), alt)
						stack = append(stack, next)
					}
				}
			}
		}
	}
	return runs, found
}

// ---- the modelled account -----------------------------------------------------

// mworld is the account the model runs in.
type mworld struct {
	mu      sync.Mutex
	now     time.Time
	src     *inventorytest.Source
	acct    inventory.Account
	tables  map[string]*lifecycletest.Fake
	regions []string // with lagotto deployed at the start; the control region is enabled too
	reap    bool
	// stripped: the poller role's inline policy is gone (its job ran).
	stripped bool
	// due is when each one-shot job ("<region> <name>") fires.
	due map[string]time.Time
	// finished: the region's lifecycle item was deleted (its teardown
	// finished, or the CLI's).
	finished map[string]bool
	// excused: a creator's MarkActive came after the region's teardown had
	// finished (watch's preflight ran before it; lagotto#349).
	excused map[string]bool
	// late: a deploy found the account-wide teardown already committed and
	// stood aside (the jobs racing a later deploy are lagotto#345's).
	late bool
	// committed: a region created its function's job with the poller
	// role's jobs: the account-wide teardown is committed.
	committed bool
	// seq counts steps; read[actor][region] is the step at which actor last
	// read region's lifecycle item, mark[region] the step at which a
	// creator's MarkActive there returned, and jobsBy the actor that created
	// the poller role's jobs.
	seq    int
	read   map[string]map[string]int
	mark   map[string]int
	jobsBy string
	// actors are the scenario's, for the clock's check.
	actors []*actor
	// window counts the account-wide changes made after a creator's mark
	// the deciding region could not have read yet: lagotto#345's window.
	window int
	// account-wide writes to the global scope, and deletes, in report mode.
	globalWrites, accountDeletes int
	violations                   []string
	report                       string
	logs                         []string
}

const modelControl = "us-east-1"

// regionSpec is one region's start: whether its poller runs (its rate
// schedule ENABLED, as a watch leaves it, or DISABLED, as deploy creates
// it), and its lifecycle item.
type regionSpec struct {
	name    string
	enabled bool
	item    string // "none" (enabled, nothing of lagotto's), "absent", "active", "idle", "teardown", "stale"
}

func (s regionSpec) String() string {
	e := "enabled"
	if !s.enabled {
		e = "disabled"
	}
	return s.name + "/" + e + "/" + s.item
}

func newMWorld(reap bool, specs ...regionSpec) *mworld {
	m := &mworld{now: t0, tables: map[string]*lifecycletest.Fake{}, reap: reap, acct: inventorytest.Account(acct), due: map[string]time.Time{},
		finished: map[string]bool{}, excused: map[string]bool{}, read: map[string]map[string]int{}, mark: map[string]int{}}
	enabled := []string{modelControl}
	for _, s := range specs {
		m.regions = append(m.regions, s.name)
		if s.name != modelControl {
			enabled = append(enabled, s.name)
		}
	}
	sort.Strings(enabled)
	m.src = inventorytest.New(enabled...)
	m.src.SetAccount(m.acct)
	ctx := context.Background()
	for _, s := range specs {
		if s.item == "none" {
			continue
		}
		r := inventorytest.Deployed(s.name, acct)
		if !s.enabled {
			r.Schedules[0].State = "DISABLED"
		}
		m.src.SetRegion(r)
		poller := m.client(PollerOwner(s.name))
		at := func(d time.Duration, f func()) {
			m.now = t0.Add(d)
			f()
			m.now = t0
		}
		must := func(err error) {
			if err != nil {
				panic(err)
			}
		}
		switch s.item {
		case "active":
			must(m.client("deploy").MarkActive(ctx, Tool, s.name))
		case "idle":
			at(-25*time.Hour, func() { _, _, err := poller.Observe(ctx, Tool, s.name, false); must(err) })
		case "teardown", "stale":
			at(-25*time.Hour, func() { _, _, err := poller.Observe(ctx, Tool, s.name, false); must(err) })
			d := time.Duration(0)
			if s.item == "stale" {
				d = -21 * time.Minute
			}
			at(d, func() { _, err := poller.Claim(ctx, Tool, s.name, 0); must(err) })
		}
	}
	return m
}

func (m *mworld) clock() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *mworld) advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

var globalPK = lifecycle.ItemKey(Tool, lifecycle.ScopeGlobal)

func (m *mworld) table(region string) *lifecycletest.Fake {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.tables[region]
	if !ok {
		f = lifecycletest.New()
		f.Hook = func(ctx context.Context, op string) {
			park(ctx, region+" "+op)
			if a, _ := ctx.Value(actorKey{}).(*actor); a != nil {
				m.mu.Lock()
				m.seq++
				if op == "GetItem" {
					if m.read[a.name] == nil {
						m.read[a.name] = map[string]int{}
					}
					m.read[a.name][region] = m.seq
				}
				m.mu.Unlock()
			}
		}
		f.Err = func(op string, in any) error {
			if !m.reap && region == modelControl && touchesGlobal(op, in) {
				m.mu.Lock()
				m.globalWrites++
				m.mu.Unlock()
			}
			if di, ok := in.(*dynamodb.DeleteItemInput); ok && op == "DeleteItem" {
				if v, ok := di.Key["pk"].(*ddbtypes.AttributeValueMemberS); ok && v.Value == lifecycle.ItemKey(Tool, region) {
					m.mu.Lock()
					m.finished[region] = true
					m.mu.Unlock()
				}
			}
			return nil
		}
		m.tables[region] = f
	}
	return f
}

// touchesGlobal reports whether op writes the global item.
func touchesGlobal(op string, in any) bool {
	key := func(k map[string]ddbtypes.AttributeValue) bool {
		v, ok := k["pk"].(*ddbtypes.AttributeValueMemberS)
		return ok && v.Value == globalPK
	}
	switch x := in.(type) {
	case *dynamodb.UpdateItemInput:
		return key(x.Key)
	case *dynamodb.DeleteItemInput:
		return key(x.Key)
	case *dynamodb.PutItemInput:
		return key(x.Item)
	case *dynamodb.TransactWriteItemsInput:
		for _, it := range x.TransactItems {
			if it.Update != nil && key(it.Update.Key) {
				return true
			}
		}
	case *dynamodb.CreateTableInput:
		return true // only the global scope would create the control region's table here
	}
	return false
}

// client is a lifecycle client as owner; a blocked MarkActive waits in steps.
func (m *mworld) client(owner string) *lifecycle.Client {
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return m.table(r), nil },
		ControlRegion: modelControl,
		Owner:         owner,
		Now:           m.clock,
		Sleep:         func(ctx context.Context, _ time.Duration) error { park(ctx, "wait"); return nil },
		WaitTimeout:   30 * time.Second,
		PollInterval:  10 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return c
}

func (m *mworld) cfg(r string) Config {
	return Config{Region: r, AccountID: acct, Grace: 24 * time.Hour, Reap: m.reap, Now: m.clock,
		Logf: func(f string, a ...any) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.logs = append(m.logs, fmt.Sprintf(f, a...))
		},
		GlobalWait: 10 * time.Second,
		Sleep:      func(ctx context.Context, _ time.Duration) error { park(ctx, "sleep"); return nil }}
}

func (m *mworld) deps(r string) Deps {
	return Deps{Lifecycle: m.client(PollerOwner(r)), Source: m.src, Scheduler: mScheduler{m, r},
		Deleters: inventory.Deleters{
			AccountID: acct,
			Region:    func(r string) inventory.RegionDeleter { return mRegion{m, r} },
			Account:   mAccount{m},
		}}
}

func (m *mworld) violate(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.violations) == 0 {
		tail := m.logs
		if len(tail) > 12 {
			tail = tail[len(tail)-12:]
		}
		var keep []string
		for _, l := range tail {
			if !strings.Contains(l, " phase=direct ") && !strings.Contains(l, " phase=final ") {
				keep = append(keep, l)
			}
		}
		m.violations = append(m.violations, fmt.Sprintf(format, a...)+"\n  last log lines:\n    "+strings.Join(keep, "\n    "))
	}
}

func (m *mworld) violation() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.violations) > 0 {
		return m.violations[0]
	}
	return ""
}

// item reads region's lagotto item without a step point.
func (m *mworld) item(region string) (lifecycle.Item, bool) {
	it, ok, err := m.client("model").Get(context.Background(), Tool, region)
	if err != nil {
		panic(err)
	}
	return it, ok
}

// checkSafe is SAFETY, at an account-wide change (what) made by the actor
// named by: no region may have an ACTIVE lifecycle item or an active watch
// that the deciding region had already read. A creator's mark it could not
// have read yet (it came after the actor's last read of that region) is the
// window lagotto#345 closes, by having creators mark the global scope; it is
// counted, not failed.
func (m *mworld) checkSafe(by, what string) {
	if !m.reap {
		m.mu.Lock()
		m.accountDeletes++
		m.mu.Unlock()
	}
	for _, r := range m.allRegions() {
		it, ok := m.item(r)
		active := ok && it.State == lifecycle.StateActive
		if f := m.regionFacts(r); f.ActiveWatches > 0 {
			active = true
		}
		if !active {
			continue
		}
		m.mu.Lock()
		excused := m.excused[r]
		mark, seen := m.mark[r], m.read[by][r]
		if !excused && mark > seen {
			m.window++
		}
		m.mu.Unlock()
		own := by == "poller "+r // its own regional lease fences its own region
		if !excused && (mark <= seen || own) {
			m.violate("SAFETY: %s by %s after it read %s as marked active (step %d, read at %d)", what, by, r, mark, seen)
		}
	}
}

func (m *mworld) allRegions() []string {
	rs, _ := m.src.EnabledRegions(context.Background())
	return rs
}

func (m *mworld) regionFacts(r string) inventory.Region {
	var out inventory.Region
	m.src.Update(r, func(x *inventory.Region) { out = *x })
	return out
}

func (m *mworld) account() inventory.Account {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acct
}

func (m *mworld) setAccount(f func(a *inventory.Account)) {
	m.mu.Lock()
	f(&m.acct)
	a := m.acct
	m.mu.Unlock()
	m.src.SetAccount(a)
}

func (m *mworld) hasRole(name string) bool {
	for _, r := range m.account().Roles {
		if r.Name == name {
			return true
		}
	}
	return false
}

// pollerRuns reports whether region's poller runs on its schedule: its
// function (until its delete job fires), its rate schedule ENABLED, and the
// poller role intact. It is the model's own reading of the account,
// independent of the code under test.
func (m *mworld) pollerRuns(region string) bool {
	r := m.regionFacts(region)
	m.mu.Lock()
	stripped := m.stripped
	m.mu.Unlock()
	if r.Function == nil || stripped || !m.hasRole(runtimeiam.RoleName) {
		return false
	}
	for _, s := range r.Schedules {
		if s.Name == deploy.PollerScheduleName && s.State == "ENABLED" {
			return true
		}
	}
	return false
}

type mRegion struct {
	m      *mworld
	region string
}

func (d mRegion) del(ctx context.Context, what string, f func(r *inventory.Region)) {
	park(ctx, "delete "+d.region+" "+what)
	if !d.m.reap {
		d.m.mu.Lock()
		d.m.accountDeletes++
		d.m.mu.Unlock()
	}
	d.m.src.Update(d.region, f)
}

func (d mRegion) DeleteTable(ctx context.Context, n string) (watcher.TableOutcome, error) {
	d.del(ctx, "table "+n, func(r *inventory.Region) {
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
func (d mRegion) DeleteArtifactBucket(ctx context.Context, _, _ string) (deploy.BucketTeardown, error) {
	d.del(ctx, "bucket", func(r *inventory.Region) { r.Bucket = nil })
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, nil
}
func (d mRegion) DeleteAlertsTopic(ctx context.Context, _, _ string) (bool, error) {
	d.del(ctx, "topic", func(r *inventory.Region) { r.Topic = nil })
	return true, nil
}
func (d mRegion) DeletePollerFunction(context.Context) (bool, error) { return false, ErrSelfDelete }
func (d mRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	return false, ErrSelfDelete
}
func (d mRegion) DeleteSchedule(ctx context.Context, g, n string) (bool, error) {
	d.del(ctx, "schedule "+g+"/"+n, func(r *inventory.Region) {
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
func (d mRegion) DeleteScheduleGroup(ctx context.Context, n string) (bool, error) {
	d.del(ctx, "group "+n, func(r *inventory.Region) {
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

type mAccount struct{ m *mworld }

func (d mAccount) DeleteRole(ctx context.Context, n string) (bool, error) {
	if n != runtimeiam.InstanceRoleName {
		return false, ErrSelfDelete
	}
	park(ctx, "delete role "+n)
	d.m.checkSafe(actorName(ctx), "deleting role "+n)
	d.m.setAccount(func(a *inventory.Account) { a.Roles = removeItem(a.Roles, n) })
	return true, nil
}
func (d mAccount) DeleteInstanceProfile(ctx context.Context, n string) (bool, error) {
	park(ctx, "delete profile "+n)
	d.m.checkSafe(actorName(ctx), "deleting instance profile "+n)
	d.m.setAccount(func(a *inventory.Account) { a.InstanceProfiles = nil })
	return true, nil
}
func (d mAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	return cliiam.Deletion{}, ErrSelfDelete
}

func removeItem(items []inventory.Item, name string) []inventory.Item {
	var keep []inventory.Item
	for _, it := range items {
		if it.Name != name {
			keep = append(keep, it)
		}
	}
	return keep
}

// mScheduler creates the one-shot jobs as schedules in the region's
// footprint; the drain fires them.
type mScheduler struct {
	m      *mworld
	region string
}

func (s mScheduler) CreateScheduleGroup(ctx context.Context, in *scheduler.CreateScheduleGroupInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleGroupOutput, error) {
	park(ctx, "create group "+aws.ToString(in.Name))
	s.m.src.Update(s.region, func(r *inventory.Region) {
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

func (s mScheduler) CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	name := aws.ToString(in.Name)
	park(ctx, "create job "+name)
	if name == FunctionJobName && s.m.roleJobsIn(s.region) {
		s.m.mu.Lock()
		s.m.committed = true
		s.m.mu.Unlock()
	}
	if strings.HasPrefix(name, roleJobPrefix) {
		s.m.mu.Lock()
		s.m.jobsBy = actorName(ctx)
		s.m.mu.Unlock()
	}
	at, err := time.Parse("at(2006-01-02T15:04:05)", aws.ToString(in.ScheduleExpression))
	if err != nil {
		panic(err)
	}
	s.m.mu.Lock()
	if _, ok := s.m.due[s.region+" "+name]; !ok {
		s.m.due[s.region+" "+name] = at
	}
	s.m.mu.Unlock()
	s.m.src.Update(s.region, func(r *inventory.Region) {
		for _, x := range r.Schedules {
			if x.Group == runtimeiam.TeardownScheduleGroup && x.Name == name {
				return
			}
		}
		r.Schedules = append(r.Schedules, inventory.Schedule{Group: aws.ToString(in.GroupName), Name: name, State: "ENABLED"})
	})
	park(ctx, "created job "+name) // a pause after the job, before the next heartbeat
	return &scheduler.CreateScheduleOutput{}, nil
}

func (s mScheduler) DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	name := aws.ToString(in.Name)
	park(ctx, "delete job "+name)
	s.m.mu.Lock()
	delete(s.m.due, s.region+" "+name)
	s.m.mu.Unlock()
	s.m.src.Update(s.region, func(r *inventory.Region) {
		var keep []inventory.Schedule
		for _, x := range r.Schedules {
			if x.Group != runtimeiam.TeardownScheduleGroup || x.Name != name {
				keep = append(keep, x)
			}
		}
		r.Schedules = keep
	})
	return &scheduler.DeleteScheduleOutput{}, nil
}

// ---- actors -------------------------------------------------------------------

// tick is one run of region's poller.
func (m *mworld) tick(region string) *actor {
	return &actor{name: "poller " + region, run: func(ctx context.Context) {
		Tick(ctx, m.cfg(region), m.deps(region))
	}}
}

// deployActor is `lagotto deploy` in region (MarkActive, then the tables,
// the poller, its rate schedule DISABLED, and the account-wide IAM setup
// ensures) followed by a watch, which enables the schedule. A region deployed
// and never watched has no poller that ever runs (lagotto#348); the
// scenarios with a DISABLED poller from the start cover it.
func (m *mworld) deployActor(region string) *actor {
	return &actor{name: "deploy " + region, run: func(ctx context.Context) {
		if m.roleJobsExist() {
			m.mu.Lock()
			m.late = true
			m.mu.Unlock()
			return
		}
		if err := m.client("deploy").MarkActive(ctx, Tool, region); err != nil {
			return // refused: created nothing
		}
		m.marked(region)
		m.mu.Lock()
		m.late = m.late || m.committed
		m.mu.Unlock()
		park(ctx, "deploy creates the region")
		r := inventorytest.Deployed(region, acct)
		r.Schedules[0].State = "DISABLED"
		m.src.SetRegion(r)
		park(ctx, "deploy ensures the IAM")
		fresh := inventorytest.Account(acct)
		m.setAccount(func(a *inventory.Account) {
			a.Roles, a.InstanceProfiles = fresh.Roles, fresh.InstanceProfiles
			m.stripped = false
		})
		m.watchActor(region).run(ctx)
	}}
}

// watchActor is `lagotto watch` in region: its preflight (the poller's
// schedule exists: requireHostedPoller), MarkActive, a watch, and the rate
// schedule armed ENABLED.
func (m *mworld) watchActor(region string) *actor {
	return &actor{name: "watch " + region, run: func(ctx context.Context) {
		f := m.regionFacts(region)
		hasSchedule := false
		for _, s := range f.Schedules {
			hasSchedule = hasSchedule || s.Name == deploy.PollerScheduleName
		}
		if !hasSchedule {
			return // no hosted poller: refused, nothing created
		}
		if err := m.client("watch").MarkActive(ctx, Tool, region); err != nil {
			return
		}
		m.marked(region)
		m.mu.Lock()
		late := m.finished[region]
		m.excused[region] = late
		m.mu.Unlock()
		if late {
			return // lagotto#349: the teardown finished between the preflight and MarkActive
		}
		park(ctx, "watch stores the watch")
		m.src.Update(region, func(r *inventory.Region) {
			r.ActiveWatches = 1
			for i := range r.Schedules {
				if r.Schedules[i].Name == deploy.PollerScheduleName {
					r.Schedules[i].State = "ENABLED"
				}
			}
		})
	}}
}

// staleWatchActor is the clock moving past the 20 min stale lease (as
// clockActor does) and then a watch in region: one actor, so a single
// preemption reaches a creator taking a stale lease over.
func (m *mworld) staleWatchActor(region string) *actor {
	clock, watch := m.clockActor(), m.watchActor(region)
	return &actor{name: "the clock, then a watch " + region, run: func(ctx context.Context) {
		clock.run(ctx)
		watch.run(ctx)
	}}
}

// clockActor moves the clock past the 20 min stale lease, but never while
// an actor is inside a lease write or between a heartbeat and the delete or
// job it is for: the lease protocol assumes that time is "well under 20 min"
// (libs lifecycle).
func (m *mworld) clockActor() *actor {
	return &actor{name: "clock", run: func(ctx context.Context) {
		park(ctx, "+21m")
		for _, a := range m.actors {
			if a.done {
				continue
			}
			busy := strings.HasPrefix(a.at, "delete ") || strings.HasPrefix(a.at, "create ") // not "created "
			for _, w := range []string{" UpdateItem", " PutItem", " DeleteItem", " TransactWriteItems"} {
				busy = busy || strings.HasSuffix(a.at, w) // a lease write computes its time before it parks
			}
			if busy {
				return
			}
		}
		m.advance(21 * time.Minute)
	}}
}

// cliTeardown is `lagotto teardown --plan --mode reap` with a plan of
// everything lagotto has, by its lifecycle protocol (cmd/teardown_plan.go
// reapWithLeases): it claims the lease of every region with something of
// lagotto's (Observe idle, then Claim with no grace; a live lease or
// activity refuses the whole run), deletes there under it, deletes the
// account-wide resources when nothing is active anywhere, then finishes each
// cleared region's lease and closes its table. It never touches the global
// scope.
func (m *mworld) cliTeardown() *actor {
	return &actor{name: "cli teardown", run: func(ctx context.Context) { m.runCLI(ctx) }}
}

func (m *mworld) runCLI(ctx context.Context) (refused string) {
	c := m.client("lagotto teardown --plan")
	facts, err := inventory.Gather(ctx, m.src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
	if err != nil {
		return err.Error()
	}
	type claimed struct {
		region string
		lease  *lifecycle.Lease
	}
	var leases []claimed
	giveBack := func() {
		for _, l := range leases {
			_ = l.lease.Release(ctx)
		}
	}
	for i := range facts.Regions {
		r := &facts.Regions[i]
		if !lagottoLeft(r) {
			continue
		}
		if r.Active() {
			giveBack()
			return r.Region + " is active"
		}
		_, _, err := c.Observe(ctx, Tool, r.Region, false)
		if errors.Is(err, lifecycle.ErrTableClosing) { // as claimRegion does
			if _, err = c.CloseTableIfEmpty(ctx, r.Region); err == nil {
				_, _, err = c.Observe(ctx, Tool, r.Region, false)
			}
		}
		if err != nil {
			giveBack()
			return err.Error()
		}
		l, err := c.Claim(ctx, Tool, r.Region, 0)
		if err != nil {
			giveBack()
			return err.Error()
		}
		leases = append(leases, claimed{r.Region, l})
	}
	for _, l := range leases {
		if err := l.lease.Step(ctx, "delete everything"); err != nil {
			continue
		}
		park(ctx, "cli deletes "+l.region)
		m.src.SetRegion(inventory.Region{Region: l.region})
	}
	if len(leases) > 0 || !m.anyRegionLagotto() {
		park(ctx, "cli deletes the account-wide IAM")
		if a := m.account(); len(a.Roles) > 0 || len(a.InstanceProfiles) > 0 || a.Policy != nil {
			m.checkSafe(actorName(ctx), "the CLI deleting the account-wide IAM")
		}
		m.setAccount(func(a *inventory.Account) { *a = inventory.Account{}; m.stripped = false })
	}
	for _, l := range leases {
		if err := l.lease.Finish(ctx); err == nil {
			_, _ = c.CloseTableIfEmpty(ctx, l.region)
		}
	}
	return ""
}

// marked records a creator's MarkActive returning in region.
func (m *mworld) marked(region string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	if _, ok := m.mark[region]; !ok {
		m.mark[region] = m.seq
	}
}

func (m *mworld) roleJobsIn(region string) bool {
	for _, s := range m.regionFacts(region).Schedules {
		if s.Group == runtimeiam.TeardownScheduleGroup && strings.HasPrefix(s.Name, roleJobPrefix) {
			return true
		}
	}
	return false
}

func (m *mworld) roleJobsExist() bool {
	for _, r := range m.allRegions() {
		for _, s := range m.regionFacts(r).Schedules {
			if s.Group == runtimeiam.TeardownScheduleGroup && strings.HasPrefix(s.Name, roleJobPrefix) {
				return true
			}
		}
	}
	return false
}

func (m *mworld) anyRegionLagotto() bool {
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		if lagottoLeft(&f) {
			return true
		}
	}
	return false
}

// ---- the drain and the checks -------------------------------------------------

// fireJobs runs the one-shot jobs that are due, and reports the earliest
// still pending (zero if none).
func (m *mworld) fireJobs() time.Time {
	now := m.clock()
	var next time.Time
	for _, region := range m.allRegions() {
		r := m.regionFacts(region)
		var fired []string
		for _, s := range r.Schedules {
			if s.Group != runtimeiam.TeardownScheduleGroup {
				continue
			}
			m.mu.Lock()
			at, ok := m.due[region+" "+s.Name]
			m.mu.Unlock()
			if ok && at.After(now) {
				if next.IsZero() || at.Before(next) {
					next = at
				}
				continue
			}
			fired = append(fired, s.Name)
			switch s.Name {
			case FunctionJobName:
				m.src.Update(region, func(x *inventory.Region) { x.Function = nil })
			case "lagotto-teardown-log-group":
				m.src.Update(region, func(x *inventory.Region) { x.LogGroup = nil })
			case "lagotto-teardown-role-inline":
				m.mu.Lock()
				m.stripped = true
				m.mu.Unlock()
			case "lagotto-teardown-role":
				m.mu.Lock()
				by := m.jobsBy
				m.mu.Unlock()
				m.checkSafe(by, "the job deleting the poller role")
				m.setAccount(func(a *inventory.Account) { a.Roles = removeItem(a.Roles, runtimeiam.RoleName) })
			}
		}
		for _, name := range fired {
			m.mu.Lock()
			delete(m.due, region+" "+name)
			m.mu.Unlock()
			m.src.Update(region, func(x *inventory.Region) {
				var rest []inventory.Schedule
				for _, s := range x.Schedules {
					if s.Group != runtimeiam.TeardownScheduleGroup || s.Name != name {
						rest = append(rest, s)
					}
				}
				x.Schedules = rest
			})
		}
	}
	return next
}

func (m *mworld) snapshot() string {
	var b strings.Builder
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		it, ok := m.item(r)
		fmt.Fprintf(&b, "%s: left=%v live=%v item=%v/%s/%v; ", r, lagottoLeft(&f), m.pollerRuns(r), ok, it.State, it.Lease != nil)
	}
	g, ok := m.item(lifecycle.ScopeGlobal)
	a := m.account()
	fmt.Fprintf(&b, "global=%v/%v roles=%d profiles=%d stripped=%v", ok, g.Lease != nil, len(a.Roles), len(a.InstanceProfiles), m.stripped)
	return b.String()
}

// finish runs the checks of one terminal state: the creators' pollers, the
// drain, LIVENESS, the CLI teardown, report mode.
func (m *mworld) finish(trace []string) {
	_ = trace
	ctx := context.Background()
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		m.mu.Lock()
		excused := m.excused[r]
		m.mu.Unlock()
		if it, ok := m.item(r); ok && it.State == lifecycle.StateActive && functionJobPending(&f) && !excused {
			m.violate("a creator took %s over, but its poller's delete job is still pending", r)
		}
		m.mu.Lock()
		late := m.late
		m.mu.Unlock()
		m.mu.Lock()
		seenMark := m.mark[r] > 0 && m.mark[r] <= m.read[m.jobsBy][r]
		m.mu.Unlock()
		if it, ok := m.item(r); ok && it.State == lifecycle.StateActive && m.roleJobsExist() && !late && !excused && seenMark {
			m.violate("SAFETY: the poller role's jobs are pending while %s's lifecycle item is ACTIVE, which their creator had read", r)
		}
	}
	if !m.reap {
		if m.globalWrites > 0 || m.accountDeletes > 0 {
			m.violate("report mode wrote the global scope %d time(s) and deleted %d thing(s)", m.globalWrites, m.accountDeletes)
		}
		return
	}
	// The drain: watches end, the clock moves past the grace, the due jobs
	// fire, and every poller that can run runs, until nothing changes.
	m.src.Update(modelControl, func(*inventory.Region) {})
	for _, r := range m.allRegions() {
		m.src.Update(r, func(x *inventory.Region) { x.ActiveWatches = 0 })
	}
	// Time moves to the next job that is due or the pollers' next hourly
	// run, whichever is first; with nothing pending and nothing changing,
	// past the grace. It stops once a move past the grace changes nothing.
	prev, quiet := "", 0
	for step := 0; step < 200 && quiet < 2; step++ {
		next := m.fireJobs()
		wait := time.Hour
		if !next.IsZero() {
			if d := next.Sub(m.clock()) + time.Second; d < wait {
				wait = d
			}
		} else if s := m.snapshot(); s == prev {
			wait, quiet = 25*time.Hour, quiet+1
		} else {
			quiet = 0
		}
		prev = m.snapshot()
		m.advance(wait)
		m.fireJobs()
		for _, r := range m.allRegions() {
			if m.pollerRuns(r) {
				Tick(ctx, m.cfg(r), m.deps(r))
			}
		}
		if s := m.snapshot(); s != prev {
			quiet = 0
		}
	}
	prev = m.snapshot()
	m.report = prev
	// LIVENESS.
	a := m.account()
	accountLeft := len(a.InstanceProfiles) > 0 || m.hasRole(runtimeiam.InstanceRoleName) || m.hasRole(runtimeiam.RoleName)
	_, globalLeft := m.item(lifecycle.ScopeGlobal)
	someRuns, allBacked := false, true
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		if m.pollerRuns(r) {
			someRuns = true
		} else if lagottoLeft(&f) {
			allBacked = false
		}
	}
	controlItems, _, _ := m.client("model").ListItems(ctx, modelControl)
	controlLeft := len(controlItems) > 0 && !m.lagottoIn(modelControl)
	m.mu.Lock()
	excused := false
	for _, e := range m.excused {
		excused = excused || e
	}
	m.mu.Unlock()
	m.mu.Lock()
	excused = excused || m.late
	m.mu.Unlock()
	switch {
	case excused:
		// A watch stored in a region whose teardown had finished (lagotto#349).
	case (accountLeft || globalLeft) && !someRuns:
		m.violate("LIVENESS: account-wide left (resources %v, global item %v) and no poller left to act on them [%s]", accountLeft, globalLeft, prev)
	case allBacked && (accountLeft || globalLeft || controlLeft):
		m.violate("LIVENESS: every region has a live poller, but the account-wide resources, the global item or the control table are left [%s]", prev)
	}
	if m.violation() != "" {
		return
	}
	// The user's last step: the CLI teardown removes the rest.
	m.advance(25 * time.Hour)
	refused := m.runCLI(ctx)
	if m.anyRegionLagotto() || len(m.account().Roles) > 0 {
		m.violate("LIVENESS: the CLI teardown left lagotto's resources (refused: %q) [%s]", refused, m.snapshot())
	}
	if _, ok := m.item(lifecycle.ScopeGlobal); ok {
		m.violate("LIVENESS: the global item is left after everything else [%s]", m.snapshot())
	}
}

// lagottoIn reports whether region has anything of lagotto's, its lifecycle
// item included.
func (m *mworld) lagottoIn(region string) bool {
	f := m.regionFacts(region)
	_, ok := m.item(region)
	return ok || lagottoLeft(&f)
}

// ---- the scenarios ------------------------------------------------------------

type scenario struct {
	name       string
	reap       bool
	specs      []regionSpec
	actors     func(m *mworld) []*actor
	maxPreempt int
	maxCrash   int
	// separate: preemptions and crashes are explored apart, not in the same
	// schedule (the default run's bound on the sweep of starting states).
	separate bool
}

func (s scenario) build() (*mworld, []*actor) {
	m := newMWorld(s.reap, s.specs...)
	m.actors = s.actors(m)
	return m, m.actors
}

// modelScenarios are the scenarios, bounded so that the default run takes
// well under a minute; long (the modelcheck build tag) raises the bounds and
// adds a third region.
func modelScenarios(long bool) []scenario {
	const a, b, c = "us-west-2", "eu-west-1", "ap-south-1"
	idle := func(r string) regionSpec { return regionSpec{name: r, enabled: true, item: "idle"} }
	ticks := func(rs ...string) func(m *mworld) []*actor {
		return func(m *mworld) []*actor {
			var out []*actor
			for _, r := range rs {
				out = append(out, m.tick(r))
			}
			return out
		}
	}
	p2 := 1
	if long {
		p2 = 2
	}
	var out []scenario
	for _, reap := range []bool{true, false} {
		out = append(out, scenario{name: fmt.Sprintf("two idle regions, reap=%t", reap), reap: reap,
			specs: []regionSpec{idle(a), idle(b)}, actors: ticks(a, b), maxPreempt: p2, maxCrash: 1, separate: !long && !reap})
	}
	// Every start of the other region, against an idle region's run.
	for _, enabled := range []bool{true, false} {
		for _, it := range []string{"absent", "active", "idle", "teardown", "stale"} {
			o := regionSpec{name: b, enabled: enabled, item: it}
			acts := ticks(a)
			if enabled && it != "absent" {
				acts = ticks(a, b)
			}
			out = append(out, scenario{name: "the other region " + o.String(), reap: true,
				specs: []regionSpec{idle(a), o}, actors: acts, maxPreempt: 1, maxCrash: 1, separate: !long})
		}
	}
	// One region alone, crashing anywhere.
	out = append(out, scenario{name: "one region", reap: true, specs: []regionSpec{idle(a)}, actors: ticks(a), maxCrash: 1})
	// Creators and the clock against a teardown.
	withActor := func(name string, extra func(m *mworld) *actor, specs ...regionSpec) scenario {
		return scenario{name: name, reap: true, specs: specs, maxPreempt: 1, maxCrash: 0,
			actors: func(m *mworld) []*actor { return []*actor{m.tick(a), extra(m)} }}
	}
	out = append(out,
		withActor("a deploy and a watch elsewhere", func(m *mworld) *actor { return m.deployActor(b) }, idle(a),
			regionSpec{name: b, item: "none"}),
		withActor("a deploy and a watch into a finished region", func(m *mworld) *actor { return m.deployActor(b) }, idle(a),
			regionSpec{name: b, enabled: false, item: "absent"}),
		withActor("a watch here", func(m *mworld) *actor { return m.watchActor(a) }, idle(a)),
		withActor("the CLI teardown", func(m *mworld) *actor { return m.cliTeardown() }, idle(a), idle(b)),
		scenario{name: "a watch here, the lease gone stale", reap: true, specs: []regionSpec{idle(a)}, maxPreempt: p2,
			actors: func(m *mworld) []*actor { return []*actor{m.tick(a), m.staleWatchActor(a)} }},
		scenario{name: "the clock, two regions", reap: true, specs: []regionSpec{idle(a), idle(b)}, maxPreempt: p2,
			actors: func(m *mworld) []*actor { return []*actor{m.tick(a), m.tick(b), m.clockActor()} }},
	)
	if long {
		out = append(out, scenario{name: "three idle regions", reap: true, specs: []regionSpec{idle(a), idle(b), idle(c)},
			actors: ticks(a, b, c), maxPreempt: 2, maxCrash: 1})
	}
	return out
}

func runModel(t *testing.T, long bool) {
	start := time.Now()
	var mu sync.Mutex
	total := 0
	t.Run("scenarios", func(t *testing.T) {
		for _, sc := range modelScenarios(long) {
			sc := sc
			t.Run(sc.name, func(t *testing.T) {
				t.Parallel()
				n, found := explore(t, sc.build, sc.maxPreempt, sc.maxCrash, sc.separate)
				mu.Lock()
				total += n
				mu.Unlock()
				t.Logf("%d schedules (preemptions <= %d, crashes <= %d, separately: %v)", n, sc.maxPreempt, sc.maxCrash, sc.separate)
				for _, f := range found {
					t.Errorf("counterexample: %s", f)
				}
			})
		}
	})
	t.Logf("model check: %d schedules in %s", total, time.Since(start).Round(time.Millisecond))
}

// TestModelCheck is the bounded model check; `-tags modelcheck` runs the long
// one (TestModelCheckLong).
func TestModelCheck(t *testing.T) {
	if testing.Short() && !longModel {
		t.Log("short mode: the model check still runs, at its default bounds")
	}
	runModel(t, false)
}

var _ = schedgroup.Name
