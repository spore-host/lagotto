package idle

// A model check of lagotto's teardown with one account-wide actor
// (lagotto#340, #345, #346).
//
// It drives the real code (Tick, TearDownAccountWide, the lifecycle
// protocol) against in-memory fakes, under a cooperative scheduler: every
// actor runs in its own goroutine and parks at each step point (every
// lifecycle table operation, every delete, every one-shot job), and the
// explorer lets exactly one actor take one step at a time. A depth-first
// search over those choices (stateless, with replay) enumerates every
// interleaving within a bound on preemptions (switching away from an actor
// that could go on), on crashes (an actor stopping for good at a step point,
// before that step) and on errors (the AWS call at a step point failing).
// Bounds are raised one at a time, so the first counterexample found is a
// minimal one.
//
// Actors: a region's poller run (Tick, reap or report mode); the creators
// (deploy and a watch, a watch, setup), each marking its region and the
// global scope before it creates anything; one or two CLI account-wide
// teardowns (`lagotto teardown --iam-roles`, `--plan`, or a last mile:
// TearDownAccountWide); and the clock (past the 20 min stale lease). Once no
// actor is left, a drain ends the watches and runs every poller that can run
// (advancing the clock past the grace and firing the one-shot jobs that are
// due) until nothing changes; then one CLI run, and one more if it did not
// finish (a lease left by a crashed or failed run).
//
// Properties:
//
//   - SAFETY, at every account-wide delete: no region has a poller function
//     or a pending one-shot job; no region has a lifecycle item the deleting
//     actor read since it last changed; and the deleting actor holds the
//     live global lease, unmoved (no creator marked the global scope since
//     its claim, and no other actor took it over). Pollers never delete, or
//     create a job for, anything account-wide, nor anything of another
//     region.
//   - No poller's role is deleted while its function exists (at each delete,
//     and at the end), and no one-shot job fires once the invoke role it
//     runs as is gone.
//   - A poller's function job never fires while its rate schedule is still
//     there: a run that stopped after committing is carried on by the next
//     hourly run, before the job is due (FunctionOffset).
//   - LIVENESS: once every region is idle and drained (no function, no job,
//     no ACTIVE item or live lease), one CLI run, or one more after a crash
//     or failure, removes every account-wide resource, the global item, and
//     every lifecycle table.
//   - Report mode deletes nothing, and the CLI's report run writes nothing
//     at all.
//
// Not modelled: IAM permissions other than the poller role's own (a poller
// whose role is gone cannot run), and Lambda's log delivery recreating a log
// group (lagotto#326).

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
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	fp "github.com/spore-host/libs/footprint"
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
	name string
	// noCrash: a crash is never offered (a creator that dies mid-way is the
	// user's failed command, which they run again).
	noCrash bool
	// injected: one of its AWS calls was failed by the model.
	injected bool
	// crashed: the model crashed it.
	crashed bool
	run     func(ctx context.Context)
	resume  chan int    // goOn, crashNow or failNow at the step point
	parked  chan string // the step point it stopped at, or "" once done
	at      string
	ran     bool
	done    bool
	// passed: the actor has passed the execution's crashAfter step.
	passed bool
}

// What an actor does at a step point.
const (
	goOn = iota
	crashNow
	failNow // the AWS call at this step returns an error
)

// errInjected is the error a step point fails with: a throttle, which every
// AWS call can return.
var errInjected = errors.New("ThrottlingException: Rate exceeded (injected by the model)")

// park is a step point: the actor reports where it is and waits for its
// turn. It reports whether the step's AWS call is to fail.
func park(ctx context.Context, label string) bool {
	a, _ := ctx.Value(actorKey{}).(*actor)
	if a == nil {
		return false // the drain, and the checks' own reads
	}
	a.parked <- label
	switch <-a.resume {
	case crashNow:
		panic(errCrash)
	case failNow:
		a.injected = true
		return true
	}
	return false
}

// failable reports whether the step at label is an AWS call that can fail:
// a lifecycle table operation, a delete, or a Scheduler write.
func failable(label string) bool {
	for _, p := range []string{"delete ", "create job ", "create group ", "delete job "} {
		if strings.HasPrefix(label, p) {
			return true
		}
	}
	for _, op := range []string{" GetItem", " PutItem", " UpdateItem", " DeleteItem", " TransactWriteItems", " Scan",
		" DescribeTable", " CreateTable", " DeleteTable"} {
		if strings.HasSuffix(label, op) {
			return true
		}
	}
	return false
}

// waiting reports whether a step point is a wait (a blocked MarkActive's
// poll, a backoff), from which an actor yields.
func waiting(label string) bool { return label == "wait" || label == "sleep" }

// option is one choice at a step: let actor go on, crash it, or fail its
// AWS call.
type option struct {
	actor int
	mode  int
}

// execution is one run of a scenario under one schedule.
type execution struct {
	prefix     []int
	chosen     []int
	widths     []int
	trace      []string
	preempts   int
	crashes    int
	fails      int
	maxPreempt int
	maxCrash   int
	maxFail    int
	// crashAfter, when set, offers a crash only once an actor has stopped
	// at a step with this label.
	crashAfter string
}

func (e *execution) options(actors []*actor, cur int) []option {
	var out []option
	// An actor parked waiting (a creator's MarkActive under a live lease,
	// a backoff) yields: switching away from it is no preemption.
	curEnabled := cur >= 0 && !actors[cur].done && !waiting(actors[cur].at)
	if cur >= 0 && !actors[cur].done {
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
	_ = curEnabled
	reach := func(i int, a *actor) bool {
		return !a.done && a.ran && (i == cur || !curEnabled || e.preempts < e.maxPreempt)
	}
	if e.crashes < e.maxCrash {
		for i, a := range actors {
			if reach(i, a) && !a.noCrash && (e.crashAfter == "" || a.passed) {
				out = append(out, option{actor: i, mode: crashNow})
			}
		}
	}
	if e.fails < e.maxFail {
		for i, a := range actors {
			if reach(i, a) && failable(a.at) {
				out = append(out, option{actor: i, mode: failNow})
			}
		}
	}
	return out
}

// run drives actors to the end under e's schedule: prefix first, then the
// first option at every later step.
func (e *execution) run(actors []*actor) {
	for _, a := range actors {
		a.resume, a.parked = make(chan int), make(chan string)
		go func(a *actor) {
			ctx := context.WithValue(context.Background(), actorKey{}, a)
			crash := <-a.resume == crashNow
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
		curEnabled := cur >= 0 && !actors[cur].done && !waiting(actors[cur].at)
		switch o.mode {
		case crashNow:
			e.crashes++
			actors[o.actor].crashed = true
		case failNow:
			e.fails++
		}
		if curEnabled && o.actor != cur {
			e.preempts++
		}
		a := actors[o.actor]
		step := a.name
		if a.ran {
			step += "@" + a.at
		}
		switch o.mode {
		case crashNow:
			step = "CRASH " + step
		case failNow:
			step = "FAIL " + step
		}
		e.trace = append(e.trace, step)
		a.ran = true
		a.resume <- o.mode
		if a.at = <-a.parked; a.at == "" {
			a.done = true
		}
		if e.crashAfter != "" && a.at == e.crashAfter {
			a.passed = true
		}
		cur = o.actor
	}
}

// explore runs every schedule of build within the bounds, raising them one
// at a time, and returns how many it ran and each distinct violation (up to
// five) with the first schedule found for it: a minimal one, as the bounds
// rise one at a time.
func explore(t *testing.T, build func() (*mworld, []*actor), maxPreempt, maxCrash, maxFail, kinds int, soloCrash bool, crashAfter string) (int, []string) {
	t.Helper()
	runs := 0
	var found []string
	seen := map[string]bool{}
	for p := 0; p <= maxPreempt; p++ {
		for c := 0; c <= maxCrash; c++ {
			for f := 0; f <= maxFail; f++ {
				n := 0
				for _, k := range []int{p, c, f} {
					if k > 0 {
						n++
					}
				}
				if (kinds > 0 && n > kinds) || (soloCrash && c > 0 && n > 1) {
					continue
				}
				stack := [][]int{{}}
				for len(stack) > 0 {
					prefix := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					m, actors := build()
					e := &execution{prefix: prefix, maxPreempt: p, maxCrash: c, maxFail: f, crashAfter: crashAfter}
					e.run(actors)
					runs++
					if e.preempts == p && e.crashes == c && e.fails == f { // each schedule is checked at its own bounds
						m.finish()
						if v := m.violation(); v != "" {
							kind := v // each distinct counterexample, its state left out
							if i := strings.Index(v, " ["); i > 0 {
								kind = v[:i]
							}
							if !seen[kind] && len(found) < 5 {
								seen[kind] = true
								found = append(found, fmt.Sprintf("%s\n  schedule (preemptions %d, crashes %d, errors %d):\n    %s", v, p, c, f, strings.Join(e.trace, "\n    ")))
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
	}
	return runs, found
}

// ---- the modelled account -----------------------------------------------------

const (
	modelControl = "us-east-1"
	mA, mB       = "us-west-2", "eu-west-1"
)

// mworld is the account the model runs in.
type mworld struct {
	mu     sync.Mutex
	now    time.Time
	src    *inventorytest.Source
	acct   inventory.Account
	tables map[string]*lifecycletest.Fake
	reap   bool
	// due is when each one-shot job ("<region> <name>") fires.
	due map[string]time.Time
	// actors are the scenario's, for the clock's check.
	actors []*actor
	// failNext: the lifecycle op the running actor is at fails (one actor
	// runs at a time).
	failNext bool
	// seq counts steps; read[actor][region] is the step at which actor last
	// read region's lagotto item, changed[region] the step of its last write.
	seq     int
	read    map[string]map[string]int
	changed map[string]int
	// writesBy counts each actor's lifecycle writes; deletes every delete.
	writesBy map[string]int
	current  string // the actor whose lifecycle operation this is
	// outside names what runs outside the actors: the drain's pollers, the
	// final CLI runs.
	outside string
	// passedUnder are the global leases a creator got past its marks under
	// (by token): a delete under one came after a creator started.
	passedUnder map[string]bool
	// decidedAt[actor] is the step of an actor's first read of the global
	// item (its decision starts there); passes the steps at which creators
	// got past their marks. A delete after a pass that came after the
	// deleter's decision deletes what a creator just started on.
	decidedAt map[string]int
	passes    []int
	// pollerReport: the pollers run in report mode (the CLI in reap).
	pollerReport bool
	// waitAdvances: see scenario.waitAdvances.
	waitAdvances bool
	creatorWait  time.Duration // the creators' WaitTimeout, with waitAdvances
	// creating counts the creators past their marks and not yet done.
	creating int
	// runs counts poller invocations, for their owners.
	runs int
	// phantomBy[region] is the poller run that created the region's
	// lifecycle item while its rate schedule was gone, the region torn down
	// (lagotto#350), if no creator has written the item since.
	phantomBy map[string]*actor
	// currentActor is the actor whose lifecycle operation this is (nil
	// outside the actors).
	currentActor *actor
	// planOrphan[region]: a CLI plan teardown there deleted the poller's
	// rate schedule but not (yet) its function: a plan that failed or was
	// killed in between is the user's failed command, which they run again,
	// and that state its own.
	planOrphan map[string]bool
	// finishedBy are the poller runs that deleted their region's item;
	// activeVerdict those whose last verdict was active.
	finishedBy, activeVerdict map[*actor]bool
	// createdBy[region] is the actor whose write last created the region's
	// lifecycle item (nil outside the actors).
	createdBy map[string]*actor
	// exitedMarks are the marks of creators that have returned.
	exitedMarks []string
	// regionMarkedAt[region] is the step of a creator's first mark there.
	regionMarkedAt map[string]int
	// markLive[region]: a creator's mark is the item's state, unchanged
	// since but by CLI runs (which must never erase it).
	markLive map[string]bool
	// failedRuns are the CLI runs that crashed, or ended Held after one of
	// their calls failed: those may leave the global lease held by design.
	failedRuns map[string]bool
	deletes    int
	violations []string
	report     string
	logs       []string
}

// regionSpec is one region's start.
type regionSpec struct {
	name string
	// deployed: lagotto's tables, poller and schedule are there; enabled:
	// the rate schedule is ENABLED (a watch armed it) rather than DISABLED
	// (as deploy creates it: lagotto#348).
	deployed, enabled bool
	// item: "absent", "idle" (25 h), "active", "stale" (a teardown whose
	// holder died).
	item string
	// jobs are one-shot jobs an earlier run left, due 30 min in.
	jobs []string
	// noSchedule: the poller's rate schedule is gone (its run deleted it
	// after committing), its function still there.
	noSchedule bool
}

func (s regionSpec) String() string {
	return fmt.Sprintf("%s/deployed=%t/enabled=%t/%s/jobs=%d", s.name, s.deployed, s.enabled, s.item, len(s.jobs))
}

// globalSpec is the global item's start: "absent", "idle" (25 h) or
// "stale" (a CLI run that died holding the global lease).
type globalSpec string

func newMWorld(reap bool, global globalSpec, specs ...regionSpec) *mworld {
	m := &mworld{now: t0, tables: map[string]*lifecycletest.Fake{}, reap: reap, acct: inventorytest.Account(acct), due: map[string]time.Time{},
		read: map[string]map[string]int{}, changed: map[string]int{}, writesBy: map[string]int{}, passedUnder: map[string]bool{}, decidedAt: map[string]int{}, failedRuns: map[string]bool{}, regionMarkedAt: map[string]int{}, markLive: map[string]bool{}, phantomBy: map[string]*actor{}, planOrphan: map[string]bool{}, finishedBy: map[*actor]bool{}, activeVerdict: map[*actor]bool{}, createdBy: map[string]*actor{}}
	enabled := []string{modelControl}
	for _, s := range specs {
		enabled = append(enabled, s.name)
	}
	sort.Strings(enabled)
	m.src = inventorytest.New(enabled...)
	m.src.SetAccount(m.acct)
	ctx := context.Background()
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	at := func(d time.Duration, f func()) {
		m.now = t0.Add(d)
		f()
		m.now = t0
	}
	for _, s := range specs {
		r := inventory.Region{Region: s.name}
		if s.deployed {
			r = inventorytest.Deployed(s.name, acct)
			if !s.enabled {
				r.Schedules[0].State = "DISABLED"
			}
			if s.noSchedule {
				r.Schedules, r.Groups = nil, nil
			}
		}
		if len(s.jobs) > 0 {
			r.Groups = append(r.Groups, inventory.Item{Name: runtimeiam.TeardownScheduleGroup,
				ARN: inventory.ScheduleGroupARN(s.name, acct, runtimeiam.TeardownScheduleGroup), Tags: inventorytest.Tags(s.name)})
		}
		for _, j := range s.jobs {
			r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: j, State: "ENABLED"})
			m.due[s.name+" "+j] = t0.Add(30 * time.Minute)
		}
		m.src.SetRegion(r)
		poller := m.client(PollerOwner(s.name))
		switch s.item {
		case "active":
			must(m.client("watch --local").MarkActive(ctx, Tool, s.name))
		case "idle":
			at(-25*time.Hour, func() { _, _, err := poller.Observe(ctx, Tool, s.name, false); must(err) })
		case "stale", "teardown":
			at(-25*time.Hour, func() { _, _, err := poller.Observe(ctx, Tool, s.name, false); must(err) })
			d := -21 * time.Minute
			if s.item == "teardown" {
				d = -time.Minute // a run that is finishing (or stranded after its commit), its lease live
			}
			at(d, func() { _, err := poller.Claim(ctx, Tool, s.name, 0); must(err) })
		}
	}
	switch global {
	case "idle", "stale":
		c := m.client("an earlier run")
		at(-25*time.Hour, func() { _, _, err := c.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); must(err) })
		if global == "stale" {
			at(-21*time.Minute, func() { _, err := c.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0); must(err) })
		}
	}
	m.writesBy = map[string]int{}
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

// writtenKeys are the item keys a write names.
func writtenKeys(in any) []string {
	key := func(k map[string]ddbtypes.AttributeValue) string {
		if v, ok := k["pk"].(*ddbtypes.AttributeValueMemberS); ok {
			return v.Value
		}
		return ""
	}
	switch x := in.(type) {
	case *dynamodb.UpdateItemInput:
		return []string{key(x.Key)}
	case *dynamodb.DeleteItemInput:
		return []string{key(x.Key)}
	case *dynamodb.PutItemInput:
		return []string{key(x.Item)}
	case *dynamodb.TransactWriteItemsInput:
		var out []string
		for _, it := range x.TransactItems {
			if it.Update != nil {
				out = append(out, key(it.Update.Key))
			}
		}
		return out
	}
	return nil
}

func (m *mworld) table(region string) *lifecycletest.Fake {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.tables[region]
	if !ok {
		f = lifecycletest.New()
		f.Hook = func(ctx context.Context, op string) {
			if park(ctx, region+" "+op) {
				m.mu.Lock()
				m.failNext = true
				m.mu.Unlock()
			}
			name := m.who(ctx)
			a, _ := ctx.Value(actorKey{}).(*actor)
			m.mu.Lock()
			m.current, m.currentActor = name, a
			m.mu.Unlock()
			if op == "GetItem" {
				m.mu.Lock()
				m.seq++
				if m.read[name] == nil {
					m.read[name] = map[string]int{}
				}
				m.read[name][region] = m.seq
				m.mu.Unlock()
			}
		}
		f.Err = func(op string, in any) error {
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.failNext {
				m.failNext = false
				return errInjected
			}
			if gi, ok := in.(*dynamodb.GetItemInput); ok && op == "GetItem" {
				if v, ok := gi.Key["pk"].(*ddbtypes.AttributeValueMemberS); ok && v.Value == lifecycle.ItemKey(Tool, lifecycle.ScopeGlobal) {
					if _, seen := m.decidedAt[m.current]; !seen {
						m.seq++
						m.decidedAt[m.current] = m.seq
					}
				}
			}
			switch op {
			case "PutItem", "UpdateItem", "DeleteItem", "TransactWriteItems", "CreateTable", "DeleteTable":
				m.writesBy[m.current]++
				m.seq++
				for _, k := range writtenKeys(in) {
					if k != lifecycle.ItemKey(Tool, region) {
						continue
					}
					m.changed[region] = m.seq
					switch {
					case strings.HasPrefix(m.current, "poller ") && op != "DeleteItem" && f.Item(lifecycle.TableName, k) == nil && !m.hasRateSchedule(region):
						m.phantomBy[region] = m.currentActor
					case !strings.HasPrefix(m.current, "poller ") && !strings.HasPrefix(m.current, "cli"):
						delete(m.phantomBy, region) // a creator's
					}
					cliRun := strings.HasPrefix(m.current, "cli")
					if op == "DeleteItem" && cliRun && m.markLive[region] {
						m.violations = append(m.violations, fmt.Sprintf("SAFETY: %s finished %s's lifecycle item, which a creator had marked since it was judged", m.current, region))
					}
					if op == "DeleteItem" && cliRun && liveMarkRaw(f.Item(lifecycle.TableName, k), m.now) {
						// Writers never idle an item; a CLI never removes one
						// a creator's live mark (or a hold) is on.
						m.violations = append(m.violations, fmt.Sprintf("SAFETY: %s finished %s's lifecycle item with a live creator mark or a hold on it", m.current, region))
					}
					if !cliRun {
						m.markLive[region] = false
					}
					if op == "DeleteItem" && m.currentActor != nil && strings.HasPrefix(m.current, "poller ") {
						m.finishedBy[m.currentActor] = true
						if (!m.reap || m.pollerReport) && m.createdBy[region] != m.currentActor {
							// Report mode undoes only its own writes (#354
							// re-review G1): an older item keeps its history.
							m.violations = append(m.violations, fmt.Sprintf("%s, in report mode, finished %s's lifecycle item, which it did not create", m.current, region))
						}
					}
					if op != "DeleteItem" && f.Item(lifecycle.TableName, k) == nil {
						m.createdBy[region] = m.currentActor
					}
				}
			}
			return nil
		}
		m.tables[region] = f
	}
	return f
}

// who is the actor ctx runs, or what runs outside them.
func (m *mworld) who(ctx context.Context) string {
	if n := actorName(ctx); n != "" {
		return n
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.outside
}

// client is a lifecycle client as owner; a blocked MarkActive waits in steps.
func (m *mworld) client(owner string) *lifecycle.Client {
	o := lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return m.table(r), nil },
		ControlRegion: modelControl,
		Owner:         owner,
		Now:           m.clock,
		Sleep:         func(ctx context.Context, _ time.Duration) error { park(ctx, "wait"); return nil },
		WaitTimeout:   30 * time.Second,
		PollInterval:  10 * time.Second,
	}
	if m.waitAdvances {
		// A creator's wait takes time, as lagotto's does (CreatorWait, past
		// the stale bound), in two polls.
		o.WaitTimeout, o.PollInterval = CreatorWait, CreatorWait/2
		m.mu.Lock()
		m.creatorWait = o.WaitTimeout
		m.mu.Unlock()
		o.Sleep = func(ctx context.Context, d time.Duration) error {
			park(ctx, "wait")
			// Its time passes only while no other actor is running: the
			// model's steps take no time, so a poll that advanced the clock
			// past a teardown still running would age that teardown's
			// lease by a wait it never had.
			if a, _ := ctx.Value(actorKey{}).(*actor); a != nil && a.noCrash && m.othersDone(a) {
				m.advance(d)
			}
			return nil
		}
	}
	c, err := lifecycle.New(o)
	if err != nil {
		panic(err)
	}
	return c
}

func (m *mworld) logf(f string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, fmt.Sprintf(f, a...))
}

func (m *mworld) cfg(r string) Config {
	return Config{Region: r, AccountID: acct, Grace: 24 * time.Hour, Reap: m.reap && !m.pollerReport, Now: m.clock, Logf: m.logf,
		Sleep: func(ctx context.Context, _ time.Duration) error { park(ctx, "sleep"); return nil }}
}

func (m *mworld) deleters() inventory.Deleters {
	return inventory.Deleters{AccountID: acct, Region: func(r string) inventory.RegionDeleter { return mRegion{m, r} }, Account: mAccount{m}}
}

// run is one poller invocation's config and deps, with an owner of its own
// (as the poller's RunOwner makes one per container).
func (m *mworld) run(r string) (Config, Deps) {
	m.mu.Lock()
	m.runs++
	owner := fmt.Sprintf("%s/%d", PollerOwner(r), m.runs)
	m.mu.Unlock()
	cfg, d := m.cfg(r), m.deps(r)
	cfg.Owner, d.Lifecycle = owner, m.client(owner)
	return cfg, d
}

func (m *mworld) deps(r string) Deps {
	return Deps{Lifecycle: m.client(PollerOwner(r)), Source: m.src, Scheduler: mScheduler{m, r}, Deleters: m.deleters()}
}

// awRun is one CLI account-wide teardown as owner.
func (m *mworld) awRun(ctx context.Context, owner string, reap bool, grace time.Duration) AccountWideResult {
	return TearDownAccountWide(ctx, AccountWideConfig{AccountID: acct, Owner: owner, Reap: reap, Grace: grace, Now: m.clock, Logf: m.logf,
		sequential: true,
		Sleep:      func(ctx context.Context, _ time.Duration) error { park(ctx, "sleep"); return nil }},
		AccountWideDeps{Lifecycle: m.client(owner), Source: m.src, Deleters: m.deleters()})
}

func (m *mworld) violate(format string, a ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.violations) == 0 {
		tail := m.logs
		if len(tail) > 10 {
			tail = tail[len(tail)-10:]
		}
		m.violations = append(m.violations, fmt.Sprintf(format, a...)+"\n  last log lines:\n    "+strings.Join(tail, "\n    "))
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

// item reads scope's lagotto item without a step point.
func (m *mworld) item(scope string) (lifecycle.Item, bool) {
	it, ok, err := m.client("model").Get(context.Background(), Tool, scope)
	if err != nil {
		panic(err)
	}
	return it, ok
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

// hasRateSchedule reports whether region's poller's rate schedule is there.
func (m *mworld) hasRateSchedule(region string) bool {
	for _, s := range m.regionFacts(region).Schedules {
		if s.Name == deploy.PollerScheduleName {
			return true
		}
	}
	return false
}

// pendingJobs are region's one-shot jobs.
func (m *mworld) pendingJobs(region string) []string {
	var out []string
	for _, s := range m.regionFacts(region).Schedules {
		if s.Group == runtimeiam.TeardownScheduleGroup {
			out = append(out, s.Name)
		}
	}
	return out
}

// pollerRuns reports whether region's poller runs on its schedule: its
// function, its rate schedule ENABLED, and the poller role there. It is the
// model's own reading of the account, independent of the code under test.
func (m *mworld) pollerRuns(region string) bool {
	r := m.regionFacts(region)
	if r.Function == nil || !m.hasRole(runtimeiam.RoleName) {
		return false
	}
	for _, s := range r.Schedules {
		if s.Name == deploy.PollerScheduleName && s.State == "ENABLED" {
			return true
		}
	}
	return false
}

// checkSafe is SAFETY, at an account-wide delete (what) by the actor named
// by.
func (m *mworld) checkSafe(by, what string) {
	if strings.HasPrefix(by, "poller ") {
		m.violate("SAFETY: %s deleted %s, which is account-wide", by, what)
	}
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		if f.Function != nil {
			if what == "role "+runtimeiam.RoleName {
				m.violate("SAFETY: the poller role deleted while %s's function exists (by %s)", r, by)
			}
			m.violate("SAFETY: %s by %s while %s has a poller function", what, by, r)
		}
		if f.ActiveWatches > 0 {
			m.violate("SAFETY: %s by %s while a watch is active in %s", what, by, r)
		}
		if j := m.pendingJobs(r); len(j) > 0 {
			m.violate("SAFETY: %s by %s while %s has one-shot jobs %v pending", what, by, r, j)
		}
		if it, ok := m.item(r); ok {
			m.mu.Lock()
			seen := m.read[by][r] > m.changed[r]
			m.mu.Unlock()
			if seen {
				m.violate("SAFETY: %s by %s after it read %s's lifecycle item (%s)", what, by, r, it.State)
			}
		}
	}
	m.mu.Lock()
	decided, hasDecided := m.decidedAt[by]
	var late []int
	for _, p := range m.passes {
		if hasDecided && p > decided {
			late = append(late, p)
		}
	}
	m.mu.Unlock()
	if len(late) > 0 {
		m.violate("SAFETY: %s by %s after a creator started since its decision (decided at step %d, creator at %v)", what, by, decided, late)
	}
	m.mu.Lock()
	creating := m.creating
	m.mu.Unlock()
	if creating > 0 {
		m.violate("SAFETY: %s by %s while a creator is running (past its marks, not done)", what, by)
	}
	g, ok := m.item(lifecycle.ScopeGlobal)
	if !ok || g.Lease == nil || g.Lease.Owner != by || g.State == lifecycle.StateActive {
		m.violate("SAFETY: %s by %s without its global lease (item %v, state %s, lease %+v)", what, by, ok, g.State, g.Lease)
	} else {
		m.mu.Lock()
		after := m.passedUnder[g.Lease.Token]
		m.mu.Unlock()
		if after {
			m.violate("SAFETY: %s by %s after a creator started under its lease", what, by)
		}
	}
}

type mRegion struct {
	m      *mworld
	region string
}

func (d mRegion) del(ctx context.Context, what string, f func(r *inventory.Region)) error {
	if park(ctx, "delete "+d.region+" "+what) {
		return errInjected
	}
	if by := d.m.who(ctx); strings.HasPrefix(by, "poller ") && by != "poller "+d.region {
		d.m.violate("%s deleted %s in %s", by, what, d.region)
	}
	d.m.mu.Lock()
	d.m.deletes++
	d.m.mu.Unlock()
	d.m.src.Update(d.region, f)
	return nil
}

func (d mRegion) DeleteTable(ctx context.Context, n string) (watcher.TableOutcome, error) {
	return watcher.TableDeleted, d.del(ctx, "table "+n, func(r *inventory.Region) {
		var keep []inventory.Table
		for _, t := range r.Tables {
			if t.Name != n {
				keep = append(keep, t)
			}
		}
		r.Tables = keep
	})
}
func (d mRegion) DeleteArtifactBucket(ctx context.Context, _, _ string) (deploy.BucketTeardown, error) {
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, d.del(ctx, "bucket", func(r *inventory.Region) { r.Bucket = nil })
}
func (d mRegion) DeleteAlertsTopic(ctx context.Context, _, _ string) (bool, error) {
	return true, d.del(ctx, "topic", func(r *inventory.Region) { r.Topic = nil })
}
func (d mRegion) DeletePollerFunction(context.Context) (bool, error) { return false, ErrSelfDelete }
func (d mRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	return false, ErrSelfDelete
}
func (d mRegion) DeleteSchedule(ctx context.Context, g, n string) (bool, error) {
	return true, d.del(ctx, "schedule "+g+"/"+n, func(r *inventory.Region) {
		var keep []inventory.Schedule
		for _, s := range r.Schedules {
			if s.Group != g || s.Name != n {
				keep = append(keep, s)
			}
		}
		r.Schedules = keep
	})
}
func (d mRegion) DeleteScheduleGroup(ctx context.Context, n string) (bool, error) {
	return true, d.del(ctx, "group "+n, func(r *inventory.Region) {
		var keep []inventory.Item
		for _, g := range r.Groups {
			if g.Name != n {
				keep = append(keep, g)
			}
		}
		r.Groups = keep
	})
}

type mAccount struct{ m *mworld }

func (d mAccount) del(ctx context.Context, what string, f func(a *inventory.Account)) error {
	if park(ctx, "delete "+what) {
		return errInjected
	}
	d.m.checkSafe(d.m.who(ctx), what)
	d.m.mu.Lock()
	d.m.deletes++
	d.m.mu.Unlock()
	d.m.setAccount(f)
	return nil
}

func (d mAccount) DeleteRole(ctx context.Context, n string) (bool, error) {
	return true, d.del(ctx, "role "+n, func(a *inventory.Account) { a.Roles = removeItem(a.Roles, n) })
}
func (d mAccount) DeleteInstanceProfile(ctx context.Context, n string) (bool, error) {
	return true, d.del(ctx, "profile "+n, func(a *inventory.Account) {
		var keep []inventory.Profile
		for _, p := range a.InstanceProfiles {
			if p.Name != n {
				keep = append(keep, p)
			}
		}
		a.InstanceProfiles = keep
	})
}
func (d mAccount) DeletePolicy(ctx context.Context) (cliiam.Deletion, error) {
	return cliiam.Deletion{Deleted: true}, d.del(ctx, "policy", func(a *inventory.Account) { a.Policy = nil })
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
	if park(ctx, "create group "+aws.ToString(in.Name)) {
		return nil, errInjected
	}
	exists := false
	s.m.src.Update(s.region, func(r *inventory.Region) {
		for _, g := range r.Groups {
			if g.Name == aws.ToString(in.Name) {
				exists = true
				return
			}
		}
		r.Groups = append(r.Groups, inventory.Item{Name: aws.ToString(in.Name),
			ARN: inventory.ScheduleGroupARN(s.region, acct, aws.ToString(in.Name)), Tags: inventorytest.Tags(s.region)})
	})
	if exists {
		return nil, &schedulertypes.ConflictException{Message: aws.String("schedule group exists")}
	}
	return &scheduler.CreateScheduleGroupOutput{}, nil
}

func (s mScheduler) CreateSchedule(ctx context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	name := aws.ToString(in.Name)
	if park(ctx, "create job "+name) {
		return nil, errInjected
	}
	if name != FunctionJobName && name != "lagotto-teardown-log-group" {
		s.m.violate("%s created the one-shot job %s: only its own function and log group are a poller's", s.m.who(ctx), name)
	}
	at, err := time.Parse("at(2006-01-02T15:04:05)", aws.ToString(in.ScheduleExpression))
	if err != nil {
		panic(err)
	}
	// As the Scheduler does: a job of that name already there answers
	// ConflictException, and keeps its own time.
	s.m.mu.Lock()
	_, exists := s.m.due[s.region+" "+name]
	if !exists {
		s.m.due[s.region+" "+name] = at
	}
	s.m.mu.Unlock()
	if exists {
		return nil, &schedulertypes.ConflictException{Message: aws.String("schedule exists")}
	}
	s.m.src.Update(s.region, func(r *inventory.Region) {
		r.Schedules = append(r.Schedules, inventory.Schedule{Group: aws.ToString(in.GroupName), Name: name, State: "ENABLED"})
	})
	park(ctx, "created job "+name) // a pause after the job, before the next heartbeat
	return &scheduler.CreateScheduleOutput{}, nil
}

func (s mScheduler) DeleteSchedule(ctx context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	name := aws.ToString(in.Name)
	if park(ctx, "delete job "+name) {
		return nil, errInjected
	}
	s.m.mu.Lock()
	_, exists := s.m.due[s.region+" "+name]
	delete(s.m.due, s.region+" "+name)
	s.m.mu.Unlock()
	if !exists {
		return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no such schedule")}
	}
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
		cfg, d := m.run(region)
		res := Tick(ctx, cfg, d)
		// A run whose own verdict is active never deletes its region's
		// item, a record of that activity (#354 review G3).
		a, _ := ctx.Value(actorKey{}).(*actor)
		m.mu.Lock()
		finished := m.finishedBy[a]
		m.activeVerdict[a] = res.Verdict.Active
		m.mu.Unlock()
		if res.Verdict.Active && finished {
			m.violate("poller %s finished its lifecycle item though its own verdict was active (%s) [%s]", region, strings.Join(res.Verdict.Reasons, "; "), m.snapshot())
		}
	}}
}

// cli is one CLI account-wide teardown: `lagotto teardown` (grace 0) or a
// last mile (the global grace).
func (m *mworld) cli(owner string, grace time.Duration) *actor {
	return &actor{name: owner, run: func(ctx context.Context) {
		m.mu.Lock()
		m.failedRuns[owner] = true // until it ends without a failure (a crash is one)
		m.mu.Unlock()
		r := m.awRun(ctx, owner, m.reap, grace)
		// A re-check's refusal stops the run: no delete after it; and a run
		// that had deleted something is suspended, not released (lagotto#351
		// ninth review).
		if r.Result != nil {
			refusedAt, deleted := -1, 0
			for i, act := range r.Result.Actions {
				if refusedAt < 0 && errors.Is(act.Err, errAccountWideBlocked) {
					refusedAt = i
				}
				if act.Outcome == fp.OutcomeDeleted {
					deleted++
					if refusedAt >= 0 && i > refusedAt {
						m.violate("%s deleted %s after a re-check refused", owner, act.Name)
					}
				}
			}
			if refusedAt >= 0 && deleted > 0 && r.Outcome != AccountWideHeld && r.Outcome != AccountWideLeaseLost {
				m.violate("%s was refused after %d delete(s) but ended %s, not suspended", owner, deleted, r.Outcome)
			}
		}
		a, _ := ctx.Value(actorKey{}).(*actor)
		m.mu.Lock()
		m.failedRuns[owner] = r.Outcome == AccountWideHeld && a != nil && a.injected
		m.mu.Unlock()
		if r.Outcome == AccountWideHeld && a != nil && !a.injected {
			// Held without a failure is only a run a creator stopped after
			// it had deleted something: suspended, with no lease, so the
			// creator's own mark waits for nothing.
			g, _ := m.item(lifecycle.ScopeGlobal)
			if !strings.HasPrefix(r.Why, "a creator arrived mid-run") || g.Lease != nil {
				m.violate("%s ended leaving the global lease held without a failure: %s (lease %+v)", owner, accountWideWhyModel(r), g.Lease)
			}
		}
	}}
}

// mark is a creator's first writes (cmd markActive): its region, then the
// global scope; a refusal of either creates nothing.
func (m *mworld) mark(ctx context.Context, c *lifecycle.Client, region string, kind creatorKindModel) (done func(), ok bool) {
	done = func() {}
	hasSchedule := func() bool {
		for _, s := range m.regionFacts(region).Schedules {
			if s.Name == deploy.PollerScheduleName {
				return true
			}
		}
		return false
	}
	if kind == mHosted && !hasSchedule() {
		return done, false // the region was torn down: refused, nothing written (cmd's pre-check)
	}
	regionToken := ""
	if region != "" {
		tok, err := Mark(ctx, c, region, "create")
		if err != nil {
			return done, false
		}
		regionToken = tok
		m.mu.Lock()
		m.seq++
		if m.regionMarkedAt[region] == 0 {
			m.regionMarkedAt[region] = m.seq
		}
		m.markLive[region] = true
		m.mu.Unlock()
	}
	endRegion := func() {
		if regionToken != "" {
			if EndMark(ctx, c, region, regionToken) == nil {
				m.mu.Lock()
				m.exitedMarks = append(m.exitedMarks, regionToken)
				m.mu.Unlock()
			}
		}
	}
	waitStart := m.clock()
	token, err := Mark(ctx, c, lifecycle.ScopeGlobal, "create")
	if err != nil {
		// Refused by a global lease: a run that has exited never holds one
		// a creator waits for, unless it crashed. (Judged at the refusal,
		// before this creator's own region mark is ended.)
		var tip *TeardownInProgressError
		if errors.As(err, &tip) && m.waitAdvances {
			// Waiting past the stale bound, a killed teardown's lease never
			// refuses a creator: it is taken over. (A live one may, in the
			// model, whose polls are not time when another actor is busy.)
			// Only a wait that took its whole time in the model counts: the
			// model's polls are not time while another actor is mid-write.
			m.mu.Lock()
			waited := m.now.Sub(waitStart) >= m.creatorWait
			m.mu.Unlock()
			for _, a := range m.actors {
				if waited && a.name == tip.Wait.Owner && a.crashed {
					m.violate("a creator was refused by the global lease of %s, killed after its claim, which it should have outwaited", a.name)
				}
			}
		}
		if errors.As(err, &tip) {
			for _, a := range m.actors {
				if a.name == tip.Wait.Owner && a.done && !a.crashed {
					m.violate("a creator was refused by the global lease of %s, which had exited without crashing", a.name)
				}
			}
		}
		endRegion()
		return done, false
	}
	done = func() {
		endRegion()
		if EndMark(ctx, c, lifecycle.ScopeGlobal, token) == nil {
			m.mu.Lock()
			m.exitedMarks = append(m.exitedMarks, token)
			m.mu.Unlock()
		}
	}
	if g, ok := m.item(lifecycle.ScopeGlobal); ok && g.Lease != nil {
		m.mu.Lock()
		m.passedUnder[g.Lease.Token] = true
		m.mu.Unlock()
	}
	m.mu.Lock()
	m.seq++
	m.passes = append(m.passes, m.seq)
	m.creating++
	m.mu.Unlock()
	mdone := done
	ended := false
	done = func() {
		if ended {
			return
		}
		ended = true
		// Its work is over: ending the mark is its last step.
		m.mu.Lock()
		m.creating--
		m.mu.Unlock()
		mdone()
	}
	if region != "" && kind == mHosted {
		if !hasSchedule() {
			done()
			return func() {}, false // torn down since the pre-check: refused
		}
		// As cmd's markCreating does (lagotto#340): the region's pending
		// teardown jobs go before anything is created or changed there.
		if !m.cancelJobs(ctx, region) {
			done()
			return func() {}, false // refused: nothing created
		}
	}
	return done, true
}

// cancelJobs deletes region's pending teardown jobs, as a creator does; false
// when a delete failed.
func (m *mworld) cancelJobs(ctx context.Context, region string) bool {
	sch := mScheduler{m, region}
	for _, j := range m.pendingJobs(region) {
		if _, err := sch.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{Name: aws.String(j), GroupName: aws.String(runtimeiam.TeardownScheduleGroup)}); err != nil &&
			!isNotFound(err) {
			return false
		}
	}
	return true
}

// settleJobs is cmd's settleDeployJobs: with the poller's rate schedule
// there, the region's pending teardown jobs go; without it they stay.
func (m *mworld) settleJobs(ctx context.Context, region string) {
	for _, s := range m.regionFacts(region).Schedules {
		if s.Name == deploy.PollerScheduleName {
			_ = m.cancelJobs(ctx, region)
			return
		}
	}
}

// creatorKindModel is cmd's creatorKind, in the model.
type creatorKindModel int

const (
	mDeploy creatorKindModel = iota
	mHosted
	mLocal
)

// ensureIAM is what setup and deploy converge: lagotto's account-wide IAM.
func (m *mworld) ensureIAM() {
	fresh := inventorytest.Account(acct)
	m.setAccount(func(a *inventory.Account) {
		a.Roles, a.InstanceProfiles, a.Policy = fresh.Roles, fresh.InstanceProfiles, fresh.Policy
	})
}

// deployActor is `lagotto deploy` in region (both marks, the IAM, the
// tables, the poller, its rate schedule DISABLED) and then a watch, which
// arms the schedule.
func (m *mworld) deployActor(region string) *actor { return m.deployActorFailing(region, false) }

// deployActorFailing is deployActor; fails: it fails (an artifact fetch
// error, not a crash) after its marks and the IAM, before it replaces the
// poller, and settles the region's jobs on the way out, as cmd's deploy
// does on every path.
func (m *mworld) deployActorFailing(region string, fails bool) *actor {
	name := "deploy " + region
	if fails {
		name = "failing deploy " + region
	}
	return &actor{name: name, noCrash: true, run: func(ctx context.Context) {
		done, ok := m.mark(ctx, m.client(name), region, mDeploy)
		if !ok {
			return
		}
		defer done()
		defer m.settleJobs(ctx, region)
		park(ctx, "deploy ensures the IAM")
		m.ensureIAM()
		if fails {
			park(ctx, "deploy fails")
			return
		}
		park(ctx, "deploy creates the region")
		r := inventorytest.Deployed(region, acct)
		r.Schedules[0].State = "DISABLED"
		old := m.regionFacts(region)
		for _, s := range old.Schedules {
			if s.Group == runtimeiam.TeardownScheduleGroup {
				r.Schedules = append(r.Schedules, s)
			}
		}
		for _, g := range old.Groups {
			if g.Name == runtimeiam.TeardownScheduleGroup {
				r.Groups = append(r.Groups, g)
			}
		}
		m.src.SetRegion(r)
		m.watchActor(region).run(ctx)
	}}
}

// watchActor is `lagotto watch` in region: its preflight (the poller's
// schedule exists), both marks, a watch, and the rate schedule armed.
func (m *mworld) watchActor(region string) *actor {
	return &actor{name: "watch " + region, noCrash: true, run: func(ctx context.Context) {
		has := false
		for _, s := range m.regionFacts(region).Schedules {
			has = has || s.Name == deploy.PollerScheduleName
		}
		if !has {
			return
		}
		done, ok := m.mark(ctx, m.client("watch "+region), region, mHosted)
		if !ok {
			return
		}
		defer done()
		park(ctx, "watch stores the watch")
		// lagotto#349: never in a region whose poller's teardown has
		// committed (its rate schedule gone, or its function's job pending).
		if !m.hasRateSchedule(region) {
			m.violate("lagotto#349: a watch was stored in %s, whose poller's rate schedule is gone [%s]", region, m.snapshot())
		}
		for _, j := range m.pendingJobs(region) {
			if j == FunctionJobName {
				m.violate("lagotto#349: a watch was stored in %s, whose poller's function is due for deletion [%s]", region, m.snapshot())
			}
		}
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

// localWatchActor is `lagotto watch --local` in region: both marks, and a
// watch a local poll daemon serves (it needs lagotto's instance role, not a
// hosted poller).
func (m *mworld) localWatchActor(region string) *actor {
	return &actor{name: "watch --local " + region, noCrash: true, run: func(ctx context.Context) {
		done, ok := m.mark(ctx, m.client("watch --local "+region), region, mLocal)
		if !ok {
			return
		}
		defer done()
		park(ctx, "watch --local stores the watch")
		m.src.Update(region, func(r *inventory.Region) {
			if len(r.Tables) == 0 {
				r.Tables = inventorytest.Deployed(region, acct).Tables // watch --local creates them
			}
			r.ActiveWatches = 1
		})
	}}
}

// launchActor is `lagotto launch` in region: it needs the hosted poller,
// and arms a schedule that invokes its function.
func (m *mworld) launchActor(region string) *actor {
	return &actor{name: "launch " + region, noCrash: true, run: func(ctx context.Context) {
		done, ok := m.mark(ctx, m.client("launch "+region), region, mHosted)
		if !ok {
			return
		}
		defer done()
		park(ctx, "launch arms its schedule")
		for _, j := range m.pendingJobs(region) {
			if j == FunctionJobName {
				m.violate("a scheduled launch was armed in %s against a poller whose function is due for deletion", region)
			}
		}
		if f := m.regionFacts(region); f.Function == nil {
			m.violate("a scheduled launch was armed in %s with no poller function", region)
		}
	}}
}

// setupActor is `lagotto setup`: the global mark, then the IAM.
// cliRegionActor is `lagotto teardown` in region, once its tables are gone
// (cmd finishRegionItem): with no poller function there, it claims the
// region's item as judged (ClaimJudged) and finishes it, and gives the lease
// back if the finish fails.
func (m *mworld) cliRegionActor(region string) *actor {
	return &actor{name: "cli teardown " + region, run: func(ctx context.Context) {
		if m.regionFacts(region).Function != nil {
			return // the poller carries it on
		}
		c := m.client("cli teardown " + region)
		if _, ok, err := c.Get(ctx, Tool, region); err != nil || !ok {
			return
		}
		l, err := ClaimJudged(ctx, c, region, true)
		if err != nil {
			return
		}
		if err := l.Finish(ctx); err != nil {
			_ = l.Release(ctx)
			return
		}
		_, _ = c.CloseTableIfEmpty(ctx, region)
	}}
}

// cliPlanActor is `lagotto teardown --plan --mode reap` of region's
// footprint (cmd executePlan and reapWithLeases): it reads the region
// afresh, refuses while anything there is in use, claims the region's item
// as judged (claimRegion: idle.ClaimJudged), deletes lagotto's regional
// resources under that lease (footprint.Execute, a heartbeat before each
// delete), the poller's own function and log group as well, and then
// finishes the item and closes the region's table; with anything left, it
// gives the lease back. A poller run already in flight carries on.
func (m *mworld) cliPlanActor(region string) *actor {
	name := "cli plan " + region
	return &actor{name: name, run: func(ctx context.Context) {
		facts, err := inventory.Gather(ctx, m.src, acct, inventory.Scope{Regions: []string{region}}, inventory.Options{})
		if err != nil {
			return
		}
		live := inventory.Classify(facts, inventory.Options{}, m.clock())
		planned := *live
		planned.Resources = nil
		for _, r := range live.Resources {
			if r.Region != region {
				continue
			}
			if r.Class == fp.ClassActive {
				return // claimRegion: something of lagotto's there is in use
			}
			planned.Resources = append(planned.Resources, r)
		}
		plan, _, err := inventory.PlanTeardown(&planned, live, inventory.Options{})
		if err != nil {
			return
		}
		c := m.client(name)
		l, err := ClaimJudged(ctx, c, region, true)
		if err != nil {
			return
		}
		del := inventory.NewDeleter(live, inventory.Deleters{AccountID: acct, Region: func(r string) inventory.RegionDeleter { return mCLIRegion{mRegion{m, r}} },
			Account: mAccount{m}})
		res, err := fp.Execute(ctx, plan, fp.ModeReap, del, l)
		cleared := err == nil && res != nil
		if res != nil {
			for _, a := range res.Actions {
				cleared = cleared && (a.Outcome == fp.OutcomeDeleted || a.Outcome == fp.OutcomeGone)
			}
		}
		if !cleared {
			_ = l.Release(ctx)
			return
		}
		if err := l.Finish(ctx); err != nil {
			return
		}
		_, _ = c.CloseTableIfEmpty(ctx, region)
	}}
}

// deployTeardownActor is `lagotto deploy --teardown` in region (pkg/deploy
// Teardown): no lease; the poller's rate schedule, its function, its log
// group, the topic and the artifact bucket go, in that order; the tables and
// the region's lifecycle item stay ('lagotto teardown' removes them). A
// failure stops it (the user runs it again).
func (m *mworld) deployTeardownActor(region string) *actor {
	return &actor{name: "deploy --teardown " + region, noCrash: true, run: func(ctx context.Context) {
		d := mCLIRegion{mRegion{m, region}}
		if _, err := d.DeleteSchedule(ctx, schedgroup.Name, deploy.PollerScheduleName); err != nil {
			return
		}
		if _, err := d.DeletePollerFunction(ctx); err != nil {
			return
		}
		if _, err := d.DeletePollerLogGroup(ctx, region); err != nil {
			return
		}
		if _, err := d.DeleteAlertsTopic(ctx, region, acct); err != nil {
			return
		}
		_, _ = d.DeleteArtifactBucket(ctx, region, acct)
	}}
}

// mCLIRegion is the CLI's regional deleters (cmd regionDeleter): the
// poller's function and log group too, which the CLI deletes itself.
type mCLIRegion struct{ mRegion }

func (d mCLIRegion) DeletePollerFunction(ctx context.Context) (bool, error) {
	err := d.del(ctx, "function", func(r *inventory.Region) { r.Function = nil })
	if err == nil {
		d.m.mu.Lock()
		d.m.planOrphan[d.region] = false
		d.m.mu.Unlock()
	}
	return true, err
}

func (d mCLIRegion) DeleteSchedule(ctx context.Context, g, n string) (bool, error) {
	ok, err := d.mRegion.DeleteSchedule(ctx, g, n)
	if err == nil && n == deploy.PollerScheduleName && d.m.regionFacts(d.region).Function != nil {
		d.m.mu.Lock()
		d.m.planOrphan[d.region] = true
		d.m.mu.Unlock()
	}
	return ok, err
}

func (d mCLIRegion) DeletePollerLogGroup(ctx context.Context, _ string) (bool, error) {
	return true, d.del(ctx, "log group", func(r *inventory.Region) { r.LogGroup = nil })
}

func (m *mworld) setupActor() *actor {
	return &actor{name: "setup", noCrash: true, run: func(ctx context.Context) {
		// setup marks its home region too (lagotto#351 review 5), so a
		// teardown deleting meanwhile stops at its next re-check.
		done, ok := m.mark(ctx, m.client("setup"), m.allRegions()[0], mLocal)
		if !ok {
			return
		}
		park(ctx, "setup ensures the IAM")
		m.ensureIAM()
		park(ctx, "setup ends its mark")
		done()
	}}
}

// clockActor moves the clock past the 20 min stale lease, but never while
// an actor is inside a lease write or between a heartbeat and the delete or
// job it is for: the lease protocol assumes that time is "well under 20 min"
// (libs lifecycle).
func (m *mworld) clockActor() *actor {
	return &actor{name: "clock", run: func(ctx context.Context) {
		park(ctx, "+21m")
		m.advanceIfQuiet(nil, 21*time.Minute)
	}}
}

// othersDone reports whether every actor but self has finished (or
// crashed).
func (m *mworld) othersDone(self *actor) bool {
	for _, a := range m.actors {
		if a != self && !a.done {
			return false
		}
	}
	return true
}

// advanceIfQuiet moves the clock by d, unless an actor other than self is
// inside a lease write or between a heartbeat and the delete or job it is
// for: the lease protocol assumes that time is "well under 20 min" (libs
// lifecycle).
func (m *mworld) advanceIfQuiet(self *actor, d time.Duration) {
	for _, a := range m.actors {
		if a.done || a == self {
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
	m.advance(d)
}

// ---- the drain and the checks -------------------------------------------------

// fireJobs runs the one-shot jobs that are due, and reports the earliest
// still pending (zero if none).
func (m *mworld) fireJobs() time.Time {
	now := m.clock()
	var next time.Time
	for _, region := range m.allRegions() {
		var fired []string
		for _, name := range m.pendingJobs(region) {
			m.mu.Lock()
			at, ok := m.due[region+" "+name]
			m.mu.Unlock()
			if ok && at.After(now) {
				if next.IsZero() || at.Before(next) {
					next = at
				}
				continue
			}
			fired = append(fired, name)
			if name == FunctionJobName {
				for _, s := range m.regionFacts(region).Schedules {
					if s.Name == deploy.PollerScheduleName {
						m.violate("%s's function job fired while its poller's rate schedule is still there: the poller is gone before it finished its region [%s]",
							region, m.snapshot())
					}
				}
			}
			if !m.hasRole(runtimeiam.SchedulerInvokeRoleName) {
				m.violate("the one-shot job %s in %s fired after the invoke role it runs as was deleted", name, region)
			}
			switch name {
			case FunctionJobName:
				m.src.Update(region, func(x *inventory.Region) { x.Function = nil })
			case "lagotto-teardown-log-group":
				m.src.Update(region, func(x *inventory.Region) { x.LogGroup = nil })
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
		fmt.Fprintf(&b, "%s: left=%v fn=%v live=%v jobs=%v item=%v/%s/%v; ", r, lagottoLeft(&f), f.Function != nil, m.pollerRuns(r),
			m.pendingJobs(r), ok, it.State, it.Lease != nil)
	}
	g, ok := m.item(lifecycle.ScopeGlobal)
	a := m.account()
	fmt.Fprintf(&b, "global=%v/%s/%v roles=%d profiles=%d policy=%v", ok, g.State, g.Lease != nil, len(a.Roles), len(a.InstanceProfiles), a.Policy != nil)
	return b.String()
}

// roleWhileFunction is the check that no poller is left whose role is gone.
func (m *mworld) roleWhileFunction(when string) {
	if m.hasRole(runtimeiam.RoleName) {
		return
	}
	for _, r := range m.allRegions() {
		if f := m.regionFacts(r); f.Function != nil {
			m.violate("%s: %s's poller function is left, but the poller role is gone [%s]", when, r, m.snapshot())
		}
	}
}

// finish runs the checks of one terminal state: report mode, or the drain,
// LIVENESS and the final CLI runs.
func (m *mworld) finish() {
	ctx := context.Background()
	if !m.reap {
		m.mu.Lock()
		deletes, writes := m.deletes, 0
		for name, n := range m.writesBy {
			if strings.HasPrefix(name, "cli") {
				writes += n
			}
		}
		m.mu.Unlock()
		if deletes > 0 || writes > 0 {
			m.violate("report mode deleted %d thing(s), and the CLI's report run wrote %d time(s)", deletes, writes)
		}
		return
	}
	m.roleWhileFunction("after the actors")
	// No creator that has returned leaves its mark (lagotto#340: a watch
	// refused after its marks would keep its region ACTIVE).
	for _, scope := range append(m.allRegions(), lifecycle.ScopeGlobal) {
		it, ok := m.item(scope)
		if !ok {
			continue
		}
		m.mu.Lock()
		exited := append([]string(nil), m.exitedMarks...)
		m.mu.Unlock()
		for _, mk := range it.Marks {
			for _, e := range exited {
				if mk.Token == e {
					m.violate("a creator returned leaving its mark on %s", scope)
				}
			}
		}
	}
	// The drain: watches end, the clock moves past the grace, the due jobs
	// fire, and every poller that can run runs, until nothing changes.
	for _, r := range m.allRegions() {
		m.src.Update(r, func(x *inventory.Region) { x.ActiveWatches = 0 })
	}
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
				m.mu.Lock()
				m.outside = "poller " + r
				m.mu.Unlock()
				cfg, d := m.run(r)
				Tick(ctx, cfg, d)
			}
		}
		if s := m.snapshot(); s != prev {
			quiet = 0
		}
	}
	m.roleWhileFunction("after the drain")
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		hasSchedule, hasJob := false, false
		for _, s := range f.Schedules {
			hasSchedule = hasSchedule || s.Name == deploy.PollerScheduleName
			hasJob = hasJob || s.Name == FunctionJobName
		}
		m.mu.Lock()
		planOrphan := m.planOrphan[r]
		m.mu.Unlock()
		if f.Function != nil && !hasSchedule && !hasJob && !planOrphan {
			m.violate("an orphaned poller in %s: its function, with no rate schedule and no job to delete it [%s]", r, m.snapshot())
		}
		// (A run killed after it made the item, or one whose verdict, read
		// again, is still active, leaves it on purpose: the account-wide
		// teardown finishes it, LIVENESS.)
		m.mu.Lock()
		by, phantom := m.phantomBy[r]
		left := by != nil && (by.crashed || m.activeVerdict[by])
		m.mu.Unlock()
		if _, ok := m.item(r); ok && phantom && !left {
			m.violate("lagotto#350: a poller run created %s's lifecycle item after the region was torn down (its rate schedule gone), and it is left [%s]",
				r, m.snapshot())
		}
	}
	m.report = m.snapshot()
	if m.violation() != "" {
		return
	}
	// LIVENESS, once every region is idle and drained.
	for _, r := range m.allRegions() {
		f := m.regionFacts(r)
		it, ok := m.item(r)
		if f.Function != nil || len(m.pendingJobs(r)) > 0 || (ok && m.client("model").InUse(it, CreatorMarkBound)) {
			return // not drained: a never-watched poller (lagotto#348), or a watch --local region
		}
	}
	m.advance(time.Hour)
	m.mu.Lock()
	m.outside = "cli final"
	m.mu.Unlock()
	res := m.awRun(ctx, "cli final", true, 0)
	if res.Outcome != AccountWideRemoved && res.Outcome != AccountWideNothing {
		m.advance(21 * time.Minute) // a lease a crashed or failed run left goes stale
		res = m.awRun(ctx, "cli final", true, 0)
	}
	a := m.account()
	_, global := m.item(lifecycle.ScopeGlobal)
	// The control table is lagotto's to close; another region's empty one
	// is the suite's (the last mile reports it, libs#78), but none may hold
	// a lagotto item.
	var tables []string
	for _, r := range m.allRegions() {
		if items, exists, _ := m.client("model").ListItems(ctx, r); exists && (r == modelControl || len(items) > 0) {
			tables = append(tables, r)
		}
	}
	if len(a.Roles) > 0 || len(a.InstanceProfiles) > 0 || a.Policy != nil || global || len(tables) > 0 {
		m.violate("LIVENESS: idle and drained, but after the CLI (%s: %s) roles=%d profiles=%d policy=%v global item=%v tables=%v [%s]",
			res.Outcome, res.Why, len(a.Roles), len(a.InstanceProfiles), a.Policy != nil, global, tables, m.snapshot())
	}
}

// ---- the scenarios ------------------------------------------------------------

type scenario struct {
	name       string
	reap       bool
	global     globalSpec
	specs      []regionSpec
	actors     func(m *mworld) []*actor
	maxPreempt int
	maxCrash   int
	maxFail    int
	// kinds bounds how many kinds of perturbation (preemptions, crashes,
	// errors) one schedule combines; 0 is all three.
	kinds int
	// crashAfter: crashes only after this step (see execution.crashAfter).
	crashAfter string
	// pollerReport: the pollers run in report mode, the CLI in reap.
	pollerReport bool
	// waitAdvances: a creator's wait for a lease takes time (CreatorWait).
	waitAdvances bool
}

func (s scenario) build() (*mworld, []*actor) {
	m := newMWorld(s.reap, s.global, s.specs...)
	m.pollerReport = s.pollerReport
	m.waitAdvances = s.waitAdvances
	m.actors = s.actors(m)
	return m, m.actors
}

// modelScenarios are the scenarios, bounded so that the default run takes
// well under a minute; long (the modelcheck build tag) raises the bounds.
func modelScenarios(long bool) []scenario {
	idle := func(r string) regionSpec { return regionSpec{name: r, deployed: true, enabled: true, item: "idle"} }
	empty := func(r string) regionSpec { return regionSpec{name: r, item: "absent"} }
	acts := func(f ...func(m *mworld) *actor) func(m *mworld) []*actor {
		return func(m *mworld) []*actor {
			var out []*actor
			for _, g := range f {
				out = append(out, g(m))
			}
			return out
		}
	}
	tick := func(r string) func(m *mworld) *actor { return func(m *mworld) *actor { return m.tick(r) } }
	cli := func(name string, grace time.Duration) func(m *mworld) *actor {
		return func(m *mworld) *actor { return m.cli(name, grace) }
	}
	clock := func(m *mworld) *actor { return m.clockActor() }
	p, k := 1, 1
	if long {
		p, k = 2, 2
	}
	var out []scenario
	for _, reap := range []bool{true, false} {
		out = append(out, scenario{name: fmt.Sprintf("two idle regions, their pollers and a CLI teardown, reap=%t", reap), reap: reap,
			specs: []regionSpec{idle(mA), idle(mB)}, actors: acts(tick(mA), tick(mB), cli("cli", 0)),
			maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k})
	}
	out = append(out,
		// Two CLI runs (a teardown and a last mile past its grace) and the
		// clock: the global lease's takeover races.
		scenario{name: "two CLI runs and the clock", reap: true, global: "idle", specs: []regionSpec{empty(mA)},
			actors: acts(cli("cli 1", 0), cli("cli 2", DefaultGrace), clock), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// A run that died holding the global lease, resumed.
		scenario{name: "a stale global lease, two CLI runs", reap: true, global: "stale", specs: []regionSpec{empty(mA)},
			actors: acts(cli("cli 1", DefaultGrace), cli("cli 2", 0)), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// Creators against the CLI (lagotto#345).
		scenario{name: "a deploy and a watch against the CLI", reap: true, specs: []regionSpec{empty(mA)},
			actors: acts(cli("cli", 0), func(m *mworld) *actor { return m.deployActor(mA) }), maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "setup against the CLI, and the clock", reap: true, specs: []regionSpec{empty(mA)},
			actors: acts(cli("cli", 0), func(m *mworld) *actor { return m.setupActor() }, clock), maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "a watch against its poller and the CLI", reap: true, specs: []regionSpec{idle(mA)},
			actors: acts(tick(mA), func(m *mworld) *actor { return m.watchActor(mA) }, cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		// Jobs an earlier run left (it crashed while committing, an hour
		// ago: they are due in 30 min), and a dead regional item. The start
		// is one crash already; a second in the run that carries it on,
		// before that run has re-timed the function's job, lets the job fire
		// before a third run: two crashes in a row, outside the bound (the
		// poller is then gone with its region unfinished, which needs
		// nothing account-wide, and 'lagotto teardown --plan' removes).
		scenario{name: "leftover jobs and the CLI", reap: true,
			specs:  []regionSpec{{name: mA, deployed: true, enabled: true, item: "idle", jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(tick(mA), cli("cli", 0)), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k, crashAfter: "created job " + FunctionJobName},
		scenario{name: "leftover jobs, a watch and the CLI", reap: true,
			specs:  []regionSpec{{name: mA, deployed: true, enabled: true, item: "idle", jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(tick(mA), func(m *mworld) *actor { return m.watchActor(mA) }, cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		// A run that committed and is finishing (its lease live, its rate
		// schedule gone), and an overlapping run of the same poller.
		scenario{name: "an overlapping poller run", reap: true,
			specs: []regionSpec{{name: mA, deployed: true, enabled: true, noSchedule: true, item: "teardown",
				jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(tick(mA), cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		// A dead regional item that a watch --local marks again while the
		// CLI judges it (finishDeadRegion acts only as judged).
		scenario{name: "a dead regional item, a watch --local and the CLI", reap: true, specs: []regionSpec{{name: mA, item: "stale"}},
			actors: acts(cli("cli", 0), func(m *mworld) *actor { return m.localWatchActor(mA) }), maxPreempt: p, maxFail: 1, kinds: k},
		// A launch in the job window, and one with a poller whose run left
		// its jobs.
		scenario{name: "a launch in the job window", reap: true,
			specs: []regionSpec{{name: mA, deployed: true, enabled: true, noSchedule: true, item: "absent",
				jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(func(m *mworld) *actor { return m.launchActor(mA) }), maxFail: 1, kinds: k},
		scenario{name: "a launch while jobs are left", reap: true,
			specs:  []regionSpec{{name: mA, deployed: true, enabled: true, item: "idle", jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(func(m *mworld) *actor { return m.launchActor(mA) }, tick(mA)), maxPreempt: p, maxFail: 1, kinds: k},
		// A retried or duplicate invocation after the region committed and
		// finished.
		scenario{name: "a duplicate poller invocation after the commit", reap: true,
			specs: []regionSpec{{name: mA, deployed: true, enabled: true, noSchedule: true, item: "absent",
				jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(tick(mA), cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		// Two concurrent invocations of one region's poller (a duplicate
		// delivery): one commits while the other, not committing, would
		// cancel jobs.
		scenario{name: "two concurrent runs of one poller", reap: true, specs: []regionSpec{idle(mA)},
			actors: acts(tick(mA), tick(mA)), maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "two concurrent runs of one poller, jobs left", reap: true,
			specs:  []regionSpec{{name: mA, deployed: true, enabled: true, item: "idle", jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(tick(mA), tick(mA)), maxPreempt: p, maxFail: 1, kinds: k},
		// A watch refused after its marks (the poller's schedule went while
		// it waited): its marks end, and nothing is left in use.
		scenario{name: "a watch while its region is torn down, and the CLI", reap: true, specs: []regionSpec{idle(mA)},
			actors: acts(tick(mA), func(m *mworld) *actor { return m.watchActor(mA) }, cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		// A CLI run that fails, and a creator after it.
		scenario{name: "a failing CLI run and a setup", reap: true, specs: []regionSpec{empty(mA)},
			actors: acts(cli("cli", 0), func(m *mworld) *actor { return m.setupActor() }), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// A redeploy in the job window, the pollers in report mode: deploy
		// deletes the region's jobs first.
		// A deploy that fails (not a crash) in the job window: the jobs stay,
		// and finish the region's teardown.
		scenario{name: "a failing deploy in the job window", reap: true,
			specs: []regionSpec{{name: mA, deployed: true, enabled: true, noSchedule: true, item: "absent",
				jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(func(m *mworld) *actor { return m.deployActorFailing(mA, true) }, cli("cli", 0)), maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "a deploy in the job window, pollers in report mode", reap: true, pollerReport: true,
			specs: []regionSpec{{name: mA, deployed: true, enabled: true, noSchedule: true, item: "absent",
				jobs: []string{"lagotto-teardown-log-group", FunctionJobName}}},
			actors: acts(func(m *mworld) *actor { return m.deployActor(mA) }), maxFail: 1, kinds: k},
		// The function's job has run; the log group's is still to (15 min
		// later), as the invoke role.
		scenario{name: "a pending job and the CLI", reap: true, specs: []regionSpec{{name: mA, item: "absent", jobs: []string{"lagotto-teardown-log-group"}}},
			actors: acts(cli("cli", 0)), maxCrash: 1, maxFail: 1, kinds: k},
		scenario{name: "a dead regional item and the CLI", reap: true, specs: []regionSpec{{name: mA, item: "stale"}, empty(mB)},
			actors: acts(cli("cli", 0), clock), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// What keeps the account-wide resources: a never-watched poller
		// (lagotto#348), and a watch --local region.
		scenario{name: "a never-watched poller", reap: true, specs: []regionSpec{{name: mA, deployed: true, item: "absent"}},
			actors: acts(cli("cli", 0)), maxCrash: 1, maxFail: 1, kinds: k},
		scenario{name: "a watch --local region", reap: true, specs: []regionSpec{{name: mA, item: "active"}},
			actors: acts(cli("cli", 0)), maxCrash: 1, maxFail: 1, kinds: k},
		// `lagotto teardown` in a region (its item claimed as judged)
		// against a creator there, and the account-wide run.
		scenario{name: "a CLI region teardown, a watch --local and the CLI", reap: true, specs: []regionSpec{{name: mA, item: "stale"}},
			actors:     acts(func(m *mworld) *actor { return m.cliRegionActor(mA) }, func(m *mworld) *actor { return m.localWatchActor(mA) }, cli("cli", 0)),
			maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "a CLI region teardown and a deploy", reap: true, specs: []regionSpec{{name: mA, item: "idle"}},
			actors:     acts(func(m *mworld) *actor { return m.cliRegionActor(mA) }, func(m *mworld) *actor { return m.deployActor(mA) }),
			maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// A CLI killed after its global claim (Ctrl-C, with no handler to
		// give the lease back): a creator outwaits the lease's staleness and
		// takes it over.
		scenario{name: "a CLI killed after its claim, and a setup", reap: true, global: "idle", specs: []regionSpec{empty(mA)}, waitAdvances: true,
			actors: acts(cli("cli", 0), func(m *mworld) *actor { return m.setupActor() }), maxPreempt: p, maxCrash: 1, kinds: k,
			crashAfter: modelControl + " UpdateItem"},
		// `lagotto teardown --plan --mode reap` removing a region while its
		// poller's run is in flight (lagotto#350): the run must not leave a
		// lifecycle item behind in the region the CLI emptied. With the
		// pollers in reap mode and in report mode (production).
		scenario{name: "a CLI plan teardown and its region's poller run", reap: true, specs: []regionSpec{idle(mA)},
			actors: acts(func(m *mworld) *actor { return m.cliPlanActor(mA) }, tick(mA)), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		scenario{name: "a CLI plan teardown and its region's poller run, pollers in report mode", reap: true, pollerReport: true, specs: []regionSpec{idle(mA)},
			actors: acts(func(m *mworld) *actor { return m.cliPlanActor(mA) }, tick(mA)), maxPreempt: p, maxCrash: 1, maxFail: 1, kinds: k},
		// ... and a watch --local that stores a watch there once the plan is
		// done (it needs no hosted poller): the run's verdict is active, so
		// it leaves the region's item (#354 review G3).
		scenario{name: "a CLI plan teardown, its region's poller run and a watch --local", reap: true, specs: []regionSpec{idle(mA)},
			actors:     acts(func(m *mworld) *actor { return m.cliPlanActor(mA) }, func(m *mworld) *actor { return m.localWatchActor(mA) }, tick(mA)),
			maxPreempt: p, maxFail: 1, kinds: k},
		// `deploy --teardown` (which keeps the tables and the region's item,
		// with its idle history) while the region's poller runs, in report
		// mode: the run never finishes an item it did not make (#354
		// re-review G1).
		scenario{name: "deploy --teardown and its region's poller run, pollers in report mode", reap: true, pollerReport: true, specs: []regionSpec{idle(mA)},
			actors: acts(func(m *mworld) *actor { return m.deployTeardownActor(mA) }, tick(mA)), maxPreempt: p, maxFail: 1, kinds: k},
		scenario{name: "the CLI's report run", specs: []regionSpec{empty(mA)}, global: "idle",
			actors: acts(cli("cli", 0), cli("cli last mile", DefaultGrace)), maxPreempt: p, maxFail: 1, kinds: k},
	)
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
				n, found := explore(t, sc.build, sc.maxPreempt, sc.maxCrash, sc.maxFail, sc.kinds, false, sc.crashAfter)
				mu.Lock()
				total += n
				mu.Unlock()
				t.Logf("%d schedules (preemptions <= %d, crashes <= %d, errors <= %d, kinds combined <= %d)", n, sc.maxPreempt, sc.maxCrash, sc.maxFail, sc.kinds)
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
	if longModel {
		t.Skip("-tags modelcheck: TestModelCheckLong runs it")
	}
	runModel(t, false)
}

func accountWideWhyModel(r AccountWideResult) string {
	return fmt.Sprintf("%s (%v; %v)", r.Why, r.Blockers, r.Err)
}

// liveMarkRaw reports whether a stored lifecycle item (raw) holds a hold, or
// a creator mark younger than CreatorMarkBound at now.
func liveMarkRaw(raw map[string]ddbtypes.AttributeValue, now time.Time) bool {
	if h, ok := raw["holds"].(*ddbtypes.AttributeValueMemberSS); ok && len(h.Value) > 0 {
		return true
	}
	mk, ok := raw["marks"].(*ddbtypes.AttributeValueMemberSS)
	if !ok {
		return false
	}
	for _, e := range mk.Value {
		i := strings.LastIndex(e, "|")
		at, err := time.Parse("2006-01-02T15:04:05.000Z", e[i+1:])
		if i < 0 || err != nil || now.Sub(at) < CreatorMarkBound {
			return true
		}
	}
	return false
}
