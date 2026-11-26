package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The regional teardown's own paths (lagotto#340): the claim, the commit and
// its rollback, and what is left held.

// pastGrace runs the first idle tick and advances past the grace.
func (h *harness) pastGrace(t *testing.T, reap bool) {
	t.Helper()
	if r := Tick(context.Background(), h.cfg(reap), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("first idle tick: %+v", r)
	}
	h.advance(25 * time.Hour)
}

// TestClaimRecognisesItsOwnLease: a claim whose answer is lost (applied, but
// answered with an error that is not a throttle) is not tried again; the
// lease found in the poller's own name is named as its own. A throttle is
// tried again.
func TestClaimRecognisesItsOwnLease(t *testing.T) {
	h := newHarness(t)
	h.pastGrace(t, true)
	ctx := context.Background()
	h.table.Err = func(op string, _ any) error {
		if op != "UpdateItem" {
			return nil
		}
		h.table.Err = nil
		if _, err := h.client.Claim(ctx, Tool, region, 0); err != nil { // the write that was applied
			t.Error(err)
		}
		return errors.New("connection reset by peer")
	}
	r := Tick(ctx, h.cfg(true), h.deps())
	if r.Outcome != OutcomeFailed || !strings.Contains(r.Err.Error(), ownLeaseNote) || len(h.ev.list()) != 0 {
		t.Fatalf("%+v\n%s", r, h.log())
	}
	// The lost answer, the applied write, and the give-back (adopted); no
	// retry of the claim.
	if n := h.table.Calls("UpdateItem"); n != 3 {
		t.Errorf("%d UpdateItem", n)
	}
	if it := h.item(t); it.Lease != nil || !strings.Contains(r.Err.Error(), "it was given back") {
		t.Errorf("its own lease was left: %+v %v", it, r.Err)
	}
	// A creator after it is not kept waiting.
	creator, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil },
		Now: h.clock, WaitTimeout: time.Nanosecond})
	if err := creator.MarkActive(ctx, Tool, region); err != nil {
		t.Errorf("a creator after the run: %v", err)
	}

	// Another's lease is not called its own.
	h = newHarness(t)
	h.pastGrace(t, false)
	other, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil }, Owner: "alice", Now: h.clock})
	if _, err := other.Claim(ctx, Tool, region, 0); err != nil {
		t.Fatal(err)
	}
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeNotClaimable || strings.Contains(r.Err.Error(), ownLeaseNote) {
		t.Errorf("another's lease: %+v", r)
	}

	// A throttle (not applied) is tried again.
	h = newHarness(t)
	h.pastGrace(t, false)
	throttled := false
	h.table.Err = func(op string, _ any) error {
		if op == "UpdateItem" && !throttled {
			throttled = true
			return &smithy.GenericAPIError{Code: "ProvisionedThroughputExceededException", Message: "slow down"}
		}
		return nil
	}
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeReported {
		t.Errorf("throttled: %+v\n%s", r, h.log())
	}
	if isThrottle(errors.New("ThrottlingException")) {
		t.Error("a plain error is a throttle")
	}
}

// failRegion fails the named regional deletes.
type failRegion struct {
	recRegion
	fail map[string]error
}

func (f failRegion) DeleteSchedule(ctx context.Context, g, n string) (bool, error) {
	if err := f.fail["schedule "+n]; err != nil {
		return false, err
	}
	return f.recRegion.DeleteSchedule(ctx, g, n)
}

func (f failRegion) DeleteScheduleGroup(ctx context.Context, n string) (bool, error) {
	if err := f.fail["group "+n]; err != nil {
		return false, err
	}
	return f.recRegion.DeleteScheduleGroup(ctx, n)
}

func (h *harness) failingDeps(fail map[string]error) Deps {
	d := h.deps()
	d.Deleters.Region = func(r string) inventory.RegionDeleter { return failRegion{recRegion{h.ev, r}, fail} }
	return d
}

// TestReapFinalFailureRollsBack: its own rate schedule cannot be deleted, so
// nothing of the final phase is gone: the jobs are deleted again, the lease
// given back, and the poller stays to run again.
func TestReapFinalFailureRollsBack(t *testing.T) {
	h := newHarness(t)
	h.pastGrace(t, true)
	d := h.failingDeps(map[string]error{"schedule " + deploy.PollerScheduleName: errors.New("AccessDenied")})
	r := Tick(context.Background(), h.cfg(true), d)
	if r.Outcome != OutcomeFailed {
		t.Fatalf("%+v\n%s", r, h.log())
	}
	ev := strings.Join(h.ev.list(), "\n")
	for _, want := range []string{"delete one-shot lagotto-teardown/lagotto-teardown-log-group", "delete one-shot lagotto-teardown/" + FunctionJobName} {
		if !strings.Contains(ev, want) {
			t.Errorf("not rolled back (%s):\n%s", want, ev)
		}
	}
	if it := h.item(t); it.Lease != nil {
		t.Errorf("lease kept: %+v", it)
	}
}

// TestReapStrandsAfterTheCommit: once its own schedule is gone, a failure
// rolls nothing back and leaves the lease held, to go stale (lagotto#340:
// the account-wide teardown finishes a dead item).
func TestReapStrandsAfterTheCommit(t *testing.T) {
	h := newHarness(t)
	h.pastGrace(t, true)
	d := h.failingDeps(map[string]error{"group lagotto": errors.New("AccessDenied")})
	r := Tick(context.Background(), h.cfg(true), d)
	if r.Outcome != OutcomeFailed || !strings.Contains(h.log(), `lease=held during="the final deletes"`) {
		t.Fatalf("%+v\n%s", r, h.log())
	}
	if strings.Contains(strings.Join(h.ev.list(), "\n"), "delete one-shot") {
		t.Errorf("rolled back after the commit: %v", h.ev.list())
	}
	if it := h.item(t); it.Lease == nil || it.State != lifecycle.StateTeardown {
		t.Errorf("lease not left held: %+v", it)
	}

	// A commit whose rollback fails strands too.
	h = newHarness(t)
	h.pastGrace(t, true)
	h.sched.conflict[FunctionJobName] = true
	h.sched.delErr = errors.New("AccessDenied: scheduler:DeleteSchedule")
	r = Tick(context.Background(), h.cfg(true), h.deps())
	if r.Outcome != OutcomeFailed || !strings.Contains(h.log(), `lease=held during="creating the one-shot jobs"`) {
		t.Errorf("rollback failing: %+v\n%s", r, h.log())
	}
	// A commit that fails but rolls back gives the lease back.
	h = newHarness(t)
	h.pastGrace(t, true)
	h.sched.err = errors.New("AccessDenied: scheduler:CreateSchedule")
	if r := Tick(context.Background(), h.cfg(true), h.deps()); r.Outcome != OutcomeFailed || h.item(t).Lease != nil {
		t.Errorf("rolled back: %+v", r)
	}
}

// TestReapCancelsLeftJobs: an earlier run's job this plan does not list (it
// stopped while committing, and the function's job is planned no more) is
// deleted before the commit; a failure to delete it stops the run before
// anything of the commit.
func TestReapCancelsLeftJobs(t *testing.T) {
	left := func(h *harness) {
		h.src.Update(region, func(r *inventory.Region) {
			r.Schedules = append(r.Schedules, inventory.Schedule{Group: runtimeiam.TeardownScheduleGroup, Name: "lagotto-teardown-role"})
		})
	}
	h := newHarness(t)
	left(h)
	if r := h.tickPastGrace(t, true); r.Outcome != OutcomeReaped {
		t.Fatalf("%+v\n%s", r, h.log())
	}
	ev := h.ev.list()
	if len(ev) == 0 || !strings.Contains(strings.Join(ev, "\n"), "delete one-shot lagotto-teardown/lagotto-teardown-role") {
		t.Errorf("left job not deleted:\n%s", strings.Join(ev, "\n"))
	}

	h = newHarness(t)
	left(h)
	h.sched.delErr = errors.New("AccessDenied")
	r := h.tickPastGrace(t, true)
	if r.Outcome != OutcomeFailed || len(h.sched.inputs) != 0 || h.item(t).Lease != nil {
		t.Errorf("%+v, %d jobs\n%s", r, len(h.sched.inputs), h.log())
	}
}

// rollbackScheduler answers DeleteSchedule with the error set for a name.
type rollbackScheduler struct {
	OneShotAPI
	errs    map[string]error
	deleted []string
}

func (r *rollbackScheduler) DeleteSchedule(_ context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	name := aws.ToString(in.Name)
	if err := r.errs[name]; err != nil {
		return nil, err
	}
	r.deleted = append(r.deleted, name)
	return &scheduler.DeleteScheduleOutput{}, nil
}

// TestRollback: a job already gone is not an error; a delete that fails is,
// and the rest are still tried.
func TestRollback(t *testing.T) {
	s := &rollbackScheduler{errs: map[string]error{
		"gone":   &schedulertypes.ResourceNotFoundException{Message: aws.String("no schedule")},
		"denied": errors.New("AccessDenied: scheduler:DeleteSchedule"),
	}}
	tr := &run{cfg: Config{Region: region, Logf: func(string, ...any) {}, Sleep: func(context.Context, time.Duration) error { return nil }}.withDefaults(),
		d: Deps{Scheduler: s}}
	if err := tr.rollback(context.Background(), []string{"gone", "a"}); err != nil || len(s.deleted) != 1 {
		t.Errorf("not found: %v %v", err, s.deleted)
	}
	if err := tr.rollback(context.Background(), []string{"denied", "b"}); err == nil || !strings.Contains(err.Error(), "denied") || len(s.deleted) != 2 {
		t.Errorf("denied: %v %v", err, s.deleted)
	}
	if err := (&run{cfg: tr.cfg}).cancelLeftJobs(context.Background(), nil); err != nil {
		t.Errorf("no facts: %v", err)
	}
}

func TestRetryOp(t *testing.T) {
	cfg := Config{Sleep: func(context.Context, time.Duration) error { return nil }}
	n := 0
	if err := retryOp(context.Background(), cfg, func() error {
		if n++; n < 3 {
			return errors.New("throttled")
		}
		return nil
	}); err != nil || n != 3 {
		t.Errorf("transient: %v after %d tries", err, n)
	}
	n = 0
	stop := errors.New("not claimable")
	err := retryOp(context.Background(), cfg, func() error { n++; return &final{stop} })
	if !errors.Is(err, stop) || n != 1 || (&final{stop}).Error() != "not claimable" {
		t.Errorf("final: %v after %d tries", err, n)
	}
	cfg.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	if err := retryOp(context.Background(), cfg, func() error { return errors.New("throttled") }); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
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

// TestControlRegion: each supported partition's IAM home region, whatever
// region of it the caller is in: us-east-1, and, now that lagotto's ARNs are
// in the account's partition (lagotto#347), us-gov-west-1 in aws-us-gov and
// cn-north-1 in aws-cn.
func TestControlRegion(t *testing.T) {
	for in, want := range map[string]string{"us-west-2": "us-east-1", "eu-west-1": "us-east-1", "": "us-east-1",
		"ap-southeast-4": "us-east-1", "il-central-1": "us-east-1", "mx-central-1": "us-east-1",
		"us-gov-east-1": "us-gov-west-1", "us-gov-west-1": "us-gov-west-1", "cn-north-1": "cn-north-1", "cn-northwest-1": "cn-north-1"} {
		if got, err := ControlRegion(in); got != want || err != nil {
			t.Errorf("ControlRegion(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	// The partitions lagotto does not support fail closed: the ISO partitions
	// (eu-isoe-west-1 starts like a commercial region), the European Sovereign
	// Cloud, and a region in no known partition.
	for _, in := range []string{"us-iso-east-1", "us-isob-east-1", "us-isof-south-1", "eu-isoe-west-1", "eusc-de-east-1", "xx-mars-1"} {
		if got, err := ControlRegion(in); got != "" || err == nil || !strings.Contains(err.Error(), in) {
			t.Errorf("ControlRegion(%q) = %q, %v, want an error", in, got, err)
		}
	}
	if PollerOwner("eu-west-1") != "lagotto-poller/eu-west-1" {
		t.Error("PollerOwner")
	}
}

// TestTeardownInProgressGlobal: a creator refused by the account-wide
// teardown's lease says so.
func TestTeardownInProgressGlobal(t *testing.T) {
	e := &TeardownInProgressError{Region: lifecycle.ScopeGlobal, What: "deploy the poller",
		Wait: &lifecycle.WaitError{Owner: "alice", Heartbeat: t0, Waited: 10 * time.Minute, Blocker: "lease"}}
	if s := e.Error(); !strings.Contains(s, "torn down account-wide") || !strings.Contains(s, "footprint --all-regions") {
		t.Error(s)
	}
}
