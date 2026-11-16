package arm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/lagotto/pkg/pollcadence"
)

// The #323 race, end to end: `lagotto watch` (PreflightPollSchedule, persist,
// BumpPollerEpoch, ArmPollSchedule) against the hosted poller's end-of-cycle
// pollcadence.Reconcile, over one shared, stateful world (the schedule, the
// poller-control item, the watches table). Every call either program makes to
// that world is one atomic step, and a gate decides which program takes the
// next step, so a test can replay one interleaving exactly or enumerate all
// of them.

type progKey struct{}

// raceWorld is the shared state. Its methods are the two programs' AWS
// calls; each first waits for the gate to let its program proceed.
type raceWorld struct {
	gate     *gate
	sched    scheduler.GetScheduleOutput
	epoch    int64
	watches  int
	disabled bool // an update ever set DISABLED
}

func newRaceWorld(expr string, state schedulertypes.ScheduleState) *raceWorld {
	return &raceWorld{sched: scheduler.GetScheduleOutput{
		Name:               aws.String("lagotto-capacity-poller"),
		Description:        aws.String("Polls for EC2 capacity matching active lagotto watches"),
		ScheduleExpression: aws.String(expr),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target:             &schedulertypes.Target{Arn: aws.String("arn:aws:lambda:us-east-1:1:function:p"), RoleArn: aws.String("arn:aws:iam::1:role/r")},
		GroupName:          aws.String("default"),
		State:              state,
	}}
}

func (w *raceWorld) step(ctx context.Context, op string) {
	if w.gate != nil {
		w.gate.step(ctx.Value(progKey{}).(string), op)
	}
}

func (w *raceWorld) GetSchedule(ctx context.Context, _ *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	w.step(ctx, "GetSchedule")
	c := w.sched
	return &c, nil
}

func (w *raceWorld) UpdateSchedule(ctx context.Context, in *scheduler.UpdateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error) {
	w.step(ctx, "UpdateSchedule("+aws.ToString(in.ScheduleExpression)+" "+string(in.State)+")")
	if in.State == schedulertypes.ScheduleStateDisabled {
		w.disabled = true
	}
	w.sched = scheduler.GetScheduleOutput{
		Name: in.Name, Description: in.Description, ScheduleExpression: in.ScheduleExpression,
		FlexibleTimeWindow: in.FlexibleTimeWindow, Target: in.Target, GroupName: in.GroupName, State: in.State,
	}
	return &scheduler.UpdateScheduleOutput{}, nil
}

func (w *raceWorld) PollerEpoch(ctx context.Context) (int64, error) {
	w.step(ctx, "PollerEpoch")
	return w.epoch, nil
}

func (w *raceWorld) MarkPollerIdle(ctx context.Context, seen int64) (bool, error) {
	w.step(ctx, "MarkPollerIdle")
	return w.epoch == seen, nil
}

func (w *raceWorld) BumpPollerEpoch(ctx context.Context) error {
	w.step(ctx, "BumpPollerEpoch")
	w.epoch++
	return nil
}

func (w *raceWorld) putWatch(ctx context.Context) {
	w.step(ctx, "PutWatch")
	w.watches++
}

// countIdle is the poller's count of the work left (strongly consistent here;
// see pkg/pollcadence for the eventually consistent index it reads in AWS).
func (w *raceWorld) countIdle(ctx context.Context) (bool, error) {
	w.step(ctx, "CountWatches")
	return w.watches == 0, nil
}

// polled reports whether the watch is polled at the normal cadence: the
// schedule is ENABLED every 5 minutes.
func (w *raceWorld) polled() bool {
	return w.sched.State == schedulertypes.ScheduleStateEnabled && aws.ToString(w.sched.ScheduleExpression) == pollcadence.NormalExpression
}

// cliWatch is `lagotto watch`'s schedule protocol (cmd/watch.go).
func cliWatch(ctx context.Context, w *raceWorld) error {
	cur, err := PreflightPollSchedule(ctx, w, "create watch w-1")
	if err != nil {
		return err
	}
	w.putWatch(ctx)
	if msg := BumpPollerEpoch(ctx, w, cur); msg != "" {
		return fmt.Errorf("%s", msg)
	}
	_, err = ArmPollSchedule(ctx, w, cur)
	return err
}

// pollerEnd is the poller's end of cycle (lambda/capacity-poller handlePoll).
func pollerEnd(ctx context.Context, w *raceWorld) error {
	_, err := pollcadence.Reconcile(ctx, w, aws.ToString(w.sched.Name), w, w.countIdle)
	return err
}

// gate runs two programs one atomic step at a time. When both are waiting to
// take a step, choose picks which goes; otherwise the one left goes.
type gate struct {
	msgs    chan gateMsg
	proceed map[string]chan struct{}
	mu      sync.Mutex
	trace   []string
}

type gateMsg struct {
	prog, op string
	done     bool
}

func (g *gate) step(prog, op string) {
	g.msgs <- gateMsg{prog: prog, op: op}
	<-g.proceed[prog]
	g.mu.Lock()
	g.trace = append(g.trace, prog+":"+op)
	g.mu.Unlock()
}

// interleave runs poller and cli under a gate. choose(i) picks the program for
// the i-th contested step. It returns the trace and each program's error.
func interleave(w *raceWorld, choose func(i int) string) (trace string, perr, cerr error) {
	g := &gate{msgs: make(chan gateMsg), proceed: map[string]chan struct{}{"poller": make(chan struct{}), "cli": make(chan struct{})}}
	w.gate = g
	defer func() { w.gate = nil }()
	errs := map[string]*error{"poller": &perr, "cli": &cerr}
	run := map[string]func(context.Context, *raceWorld) error{"poller": pollerEnd, "cli": cliWatch}
	for name, f := range run {
		go func(name string, f func(context.Context, *raceWorld) error) {
			err := f(context.WithValue(context.Background(), progKey{}, name), w)
			*errs[name] = err
			g.msgs <- gateMsg{prog: name, done: true}
		}(name, f)
	}
	waiting := map[string]bool{}
	live := 2
	for i := 0; i < 2; i++ {
		m := <-g.msgs
		if m.done {
			live--
		} else {
			waiting[m.prog] = true
		}
	}
	contested := 0
	for live > 0 {
		var next string
		switch {
		case waiting["poller"] && waiting["cli"]:
			next = choose(contested)
			contested++
		case waiting["poller"]:
			next = "poller"
		default:
			next = "cli"
		}
		waiting[next] = false
		g.proceed[next] <- struct{}{}
		m := <-g.msgs
		if m.done {
			live--
		} else {
			waiting[m.prog] = true
		}
	}
	return strings.Join(g.trace, " "), perr, cerr
}

// TestLostWatchInterleaving is the interleaving #323 reports: the poller
// counts zero watches; the CLI then stores a watch and finds the schedule
// ENABLED; then the poller acts on its count. Before #323 the poller disabled
// the schedule and the CLI, having seen ENABLED, did nothing: the watch was
// never polled. Now the CLI's epoch bump makes the poller's conditional
// mode=idle fail, so it leaves the schedule alone.
func TestLostWatchInterleaving(t *testing.T) {
	w := newRaceWorld(pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled)
	// The poller reads the epoch and counts; then the CLI runs to completion.
	order := []string{"poller", "poller"}
	trace, perr, cerr := interleave(w, func(i int) string {
		if i < len(order) {
			return order[i]
		}
		return "cli"
	})
	if perr != nil || cerr != nil {
		t.Fatalf("poller %v, cli %v\n%s", perr, cerr, trace)
	}
	if w.disabled || !w.polled() {
		t.Fatalf("the new watch is not polled: schedule %s %s\ntrace: %s",
			w.sched.State, aws.ToString(w.sched.ScheduleExpression), trace)
	}
	// And the next poll cycle sees it.
	res, err := pollcadence.Reconcile(context.Background(), w, "p", w, func(context.Context) (bool, error) { return w.watches == 0, nil })
	if err != nil || res.Outcome != pollcadence.Active {
		t.Errorf("next cycle: %+v, %v; want Active", res, err)
	}
}

// TestNoInterleavingLosesAWatch enumerates every interleaving of the CLI's
// and the poller's steps, from both an active (5 minutes) and an idle (1 hour)
// schedule, and from a DISABLED one an older poller left. In every one the
// schedule ends ENABLED every 5 minutes: the new watch is polled, either
// because nothing disabled or slowed the schedule, or because the epoch
// re-check restored it.
func TestNoInterleavingLosesAWatch(t *testing.T) {
	starts := []struct {
		expr  string
		state schedulertypes.ScheduleState
	}{
		{pollcadence.NormalExpression, schedulertypes.ScheduleStateEnabled},
		{pollcadence.IdleExpression, schedulertypes.ScheduleStateEnabled},
		{pollcadence.NormalExpression, schedulertypes.ScheduleStateDisabled},
	}
	const bits = 14 // more contested steps than the two programs take together
	for _, st := range starts {
		seen := map[string]bool{}
		for mask := 0; mask < 1<<bits; mask++ {
			w := newRaceWorld(st.expr, st.state)
			trace, perr, cerr := interleave(w, func(i int) string {
				if mask&(1<<i) != 0 {
					return "cli"
				}
				return "poller"
			})
			if seen[trace] {
				continue
			}
			seen[trace] = true
			if perr != nil || cerr != nil {
				t.Fatalf("from %s %s: poller %v, cli %v\ntrace: %s", st.state, st.expr, perr, cerr, trace)
			}
			if w.disabled || !w.polled() {
				t.Fatalf("from %s %s the new watch is not polled every 5 minutes: schedule %s %s (disabled=%v)\ntrace: %s",
					st.state, st.expr, w.sched.State, aws.ToString(w.sched.ScheduleExpression), w.disabled, trace)
			}
			if aws.ToString(w.sched.Description) == "" {
				t.Fatalf("an update dropped the Description\ntrace: %s", trace)
			}
		}
		t.Logf("from %s %s: %d distinct interleavings, the watch polled every 5 minutes in all", st.state, st.expr, len(seen))
		if len(seen) < 20 {
			t.Errorf("from %s %s only %d distinct interleavings ran; the gate is not interleaving", st.state, st.expr, len(seen))
		}
	}
}
