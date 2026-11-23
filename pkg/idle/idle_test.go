package idle

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	acct   = "123456789012"
	region = "us-east-1"
)

var t0 = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// events is one ordered log of every delete and Scheduler call.
type events struct {
	mu  sync.Mutex
	all []string
}

func (e *events) add(f string, a ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.all = append(e.all, fmt.Sprintf(f, a...))
}

func (e *events) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.all...)
}

type recRegion struct {
	e      *events
	region string
}

func (d recRegion) DeleteTable(_ context.Context, n string) (watcher.TableOutcome, error) {
	d.e.add("delete %s table %s", d.region, n)
	return watcher.TableDeleted, nil
}
func (d recRegion) DeleteArtifactBucket(context.Context, string, string) (deploy.BucketTeardown, error) {
	d.e.add("delete %s bucket", d.region)
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, nil
}
func (d recRegion) DeleteAlertsTopic(context.Context, string, string) (bool, error) {
	d.e.add("delete %s topic", d.region)
	return true, nil
}
func (d recRegion) DeletePollerFunction(context.Context) (bool, error) {
	d.e.add("delete %s function", d.region)
	return true, nil
}
func (d recRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	d.e.add("delete %s loggroup", d.region)
	return true, nil
}
func (d recRegion) DeleteSchedule(_ context.Context, g, n string) (bool, error) {
	d.e.add("delete %s schedule %s/%s", d.region, g, n)
	return true, nil
}
func (d recRegion) DeleteScheduleGroup(_ context.Context, n string) (bool, error) {
	d.e.add("delete %s group %s", d.region, n)
	return true, nil
}

type recAccount struct{ e *events }

func (d recAccount) DeleteRole(_ context.Context, n string) (bool, error) {
	d.e.add("delete global role %s", n)
	return true, nil
}
func (d recAccount) DeleteInstanceProfile(_ context.Context, n string) (bool, error) {
	d.e.add("delete global profile %s", n)
	return true, nil
}
func (d recAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	d.e.add("delete global policy")
	return cliiam.Deletion{Deleted: true}, nil
}

// fakeScheduler records the one-shot jobs; conflict makes those names (and
// "group") answer ConflictException.
type fakeScheduler struct {
	e        *events
	conflict map[string]bool
	inputs   []*scheduler.CreateScheduleInput
	err      error
}

func (f *fakeScheduler) CreateScheduleGroup(_ context.Context, in *scheduler.CreateScheduleGroupInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleGroupOutput, error) {
	f.e.add("create group %s tags=%d", aws.ToString(in.Name), len(in.Tags))
	if f.conflict["group"] {
		return nil, &schedulertypes.ConflictException{Message: aws.String("exists")}
	}
	return &scheduler.CreateScheduleGroupOutput{}, nil
}

func (f *fakeScheduler) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	f.e.add("create one-shot %s/%s", aws.ToString(in.GroupName), aws.ToString(in.Name))
	f.inputs = append(f.inputs, in)
	if f.err != nil {
		return nil, f.err
	}
	if f.conflict[aws.ToString(in.Name)] {
		return nil, &schedulertypes.ConflictException{Message: aws.String("exists")}
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

// harness is one region's teardown against fakes.
type harness struct {
	src    *inventorytest.Source
	table  *lifecycletest.Fake
	client *lifecycle.Client
	ev     *events
	sched  *fakeScheduler
	logs   []string
	now    time.Time
	mu     sync.Mutex
}

func newHarness(t *testing.T, regions ...string) *harness {
	t.Helper()
	if len(regions) == 0 {
		regions = []string{region, "us-west-2"}
	}
	h := &harness{src: inventorytest.New(regions...), table: lifecycletest.New(), ev: &events{}, now: t0}
	h.src.SetRegion(inventorytest.Deployed(region, acct)).SetAccount(inventorytest.Account(acct))
	h.sched = &fakeScheduler{e: h.ev, conflict: map[string]bool{}}
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(string) (lifecycle.API, error) { return h.table, nil },
		ControlRegion: region,
		Owner:         "lagotto-poller/" + region,
		Now:           h.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.client = c
	return h
}

func (h *harness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = h.now.Add(d)
}

func (h *harness) cfg(reap bool) Config {
	return Config{Region: region, AccountID: acct, Grace: 24 * time.Hour, Reap: reap, Now: h.clock,
		Sleep: func(context.Context, time.Duration) error { return nil },
		Logf: func(f string, a ...any) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.logs = append(h.logs, fmt.Sprintf(f, a...))
		}}
}

func (h *harness) deps() Deps {
	return Deps{Lifecycle: h.client, Source: h.src, Scheduler: h.sched, Deleters: inventory.Deleters{
		AccountID: acct,
		Region:    func(r string) inventory.RegionDeleter { return recRegion{h.ev, r} },
		Account:   recAccount{h.ev},
	}}
}

func (h *harness) log() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.logs, "\n")
}

func (h *harness) item(t *testing.T) lifecycle.Item {
	t.Helper()
	it, ok, err := h.client.Get(context.Background(), Tool, region)
	if err != nil || !ok {
		t.Fatalf("lifecycle item: %v %v", ok, err)
	}
	return it
}

// tickPastGrace runs the first tick (idle starts), then one past the grace.
func (h *harness) tickPastGrace(t *testing.T, reap bool) Result {
	t.Helper()
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(reap), h.deps()); r.Outcome != OutcomeWaiting || r.Err != nil {
		t.Fatalf("first idle tick: %+v", r)
	}
	h.advance(24*time.Hour + time.Minute)
	return Tick(ctx, h.cfg(reap), h.deps())
}

// ---- the verdict ------------------------------------------------------------

func TestVerdict(t *testing.T) {
	ctx := context.Background()
	src := inventorytest.New(region).SetRegion(inventorytest.Deployed(region, acct))
	if v := ReadVerdict(ctx, src.Region(region), region, inventory.Options{}); v.Active || len(v.Reasons) != 0 {
		t.Errorf("a deployed region with nothing running: %+v", v)
	}
	for name, set := range map[string]func(r *inventory.Region){
		"active watch":     func(r *inventory.Region) { r.ActiveWatches = 1 },
		"pending launch":   func(r *inventory.Region) { r.ArmedLaunches = 1 },
		"held reservation": func(r *inventory.Region) { r.Holds = []inventory.Item{{Name: "cr-1"}} },
		"running job":      func(r *inventory.Region) { r.Jobs = []inventory.Item{{Name: "lagotto-w-1"}} },
	} {
		src := inventorytest.New(region).SetRegion(inventorytest.Deployed(region, acct))
		src.Update(region, set)
		if v := ReadVerdict(ctx, src.Region(region), region, inventory.Options{}); !v.Active || len(v.Reasons) != 1 {
			t.Errorf("%s: %+v", name, v)
		}
	}
}

// TestVerdictReadErrorIsActive: any read that fails makes the region active.
func TestVerdictReadErrorIsActive(t *testing.T) {
	for _, m := range []string{"Activity", "Holds", "Jobs"} {
		src := inventorytest.New(region).SetRegion(inventorytest.Deployed(region, acct))
		src.Fail(region+" "+m, errors.New("AccessDenied"))
		v := ReadVerdict(context.Background(), src.Region(region), region, inventory.Options{})
		if !v.Active || !strings.Contains(strings.Join(v.Reasons, ";"), "could not be read") || len(v.Warnings) != 1 {
			t.Errorf("%s failing: %+v", m, v)
		}
	}
}

func TestGraceFrom(t *testing.T) {
	env := func(v string) func(string) string { return func(string) string { return v } }
	for v, want := range map[string]time.Duration{"": DefaultGrace, "30m": 30 * time.Minute, "2h": 2 * time.Hour,
		"nonsense": DefaultGrace, "-1h": DefaultGrace, "0": DefaultGrace} {
		got, warn := GraceFrom(env(v))
		if got != want {
			t.Errorf("%s=%q: %s, want %s", GraceEnv, v, got, want)
		}
		if (warn != "") != (want == DefaultGrace && v != "") {
			t.Errorf("%s=%q: warning %q", GraceEnv, v, warn)
		}
	}
}

// ---- Observe, Claim, and the post-claim re-check ----------------------------

// TestActiveRegionNeverClaims: activity resets idleness every tick; nothing
// is claimed however much time passes.
func TestActiveRegionNeverClaims(t *testing.T) {
	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 2 })
	for i := 0; i < 3; i++ {
		if r := Tick(context.Background(), h.cfg(false), h.deps()); r.Outcome != OutcomeActive {
			t.Fatalf("tick %d: %+v", i, r)
		}
		h.advance(48 * time.Hour)
	}
	if it := h.item(t); it.State != lifecycle.StateActive || !it.IdleSince.IsZero() {
		t.Errorf("item %+v", it)
	}
	if !strings.Contains(h.log(), `idle-verdict region=us-east-1 active=true idle_for=0s grace=24h0m0s reasons="2 active watch(es)"`) {
		t.Errorf("log:\n%s", h.log())
	}
}

// TestClaimOnlyAfterGrace: idle for less than the grace is waiting; past it,
// the lease is claimed and the report runs.
func TestClaimOnlyAfterGrace(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("first tick %+v", r)
	}
	h.advance(23 * time.Hour)
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting || r.IdleFor != 23*time.Hour {
		t.Fatalf("within the grace: %+v", r)
	}
	h.advance(2 * time.Hour)
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeReported {
		t.Fatalf("past the grace: %+v\n%s", r, h.log())
	}
}

// TestPostClaimRecheckReleases: a watch appears between the verdict and the
// claim's re-read. The teardown re-reads after claiming, finds it, and gives
// the lease back without planning anything.
func TestPostClaimRecheckReleases(t *testing.T) {
	h := newHarness(t)
	h.table.Hook = func(_ context.Context, op string) {
		if op == "UpdateItem" { // Claim is the only UpdateItem before the re-read
			h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 1 })
		}
	}
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReleased || r.Plan != nil {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
	if it := h.item(t); it.State != lifecycle.StateActive || it.Lease != nil || !it.IdleSince.IsZero() {
		t.Errorf("after the release: %+v", it)
	}
	if !strings.Contains(h.log(), `lease=released reason="active after the claim: 1 active watch(es)"`) {
		t.Errorf("log:\n%s", h.log())
	}
	if strings.Contains(h.log(), "phase=") {
		t.Errorf("planned after finding activity:\n%s", h.log())
	}
}

// TestPostClaimUnreadableIsActive: a footprint read failing after the claim
// counts as activity: released, nothing planned.
func TestPostClaimUnreadableIsActive(t *testing.T) {
	h := newHarness(t)
	h.src.Fail(region+" Topic", errors.New("AccessDenied: sns:GetTopicAttributes"))
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReleased || !strings.Contains(h.log(), "could not read the topic") {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
}

// ---- report mode ------------------------------------------------------------

var rowRE = regexp.MustCompile(`^idle-teardown REPORT region=us-east-1 phase=(\w+(?:-\w+)?) `)

// TestReportDeletesNothingAndLogsThePlan is the report-mode contract: zero
// deletes, zero Scheduler calls, the lease taken and given back (never
// finished, the table never closed), and every step of the plan logged,
// the self-removal jobs with their offsets.
func TestReportDeletesNothingAndLogsThePlan(t *testing.T) {
	h := newHarness(t)
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReported || r.Err != nil {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
	if ev := h.ev.list(); len(ev) != 0 {
		t.Fatalf("report mode called %v", ev)
	}
	if n := h.table.Calls("DeleteTable") + h.table.Calls("DeleteItem"); n != 0 {
		t.Errorf("report mode finished the lease or closed the table (%d calls)", n)
	}
	if it := h.item(t); it.State != lifecycle.StateActive || it.Lease != nil {
		t.Errorf("lease not given back: %+v", it)
	}
	log := h.log()
	for _, want := range []string{
		// direct: tables, bucket, topic; and in the last region lagotto's
		// instance role and profile.
		"phase=direct outcome=would-delete type=AWS::DynamoDB::Table resource_region=us-east-1 name=\"lagotto-watches\"",
		"phase=direct outcome=would-delete type=AWS::DynamoDB::Table resource_region=us-east-1 name=\"lagotto-match-history\"",
		"phase=direct outcome=would-delete type=AWS::S3::Bucket",
		"phase=direct outcome=would-delete type=AWS::SNS::Topic",
		// global-direct (the last region's): lagotto's instance role and
		// profile.
		"phase=global-direct outcome=would-delete type=AWS::IAM::InstanceProfile resource_region=global name=\"lagotto-instance-role\"",
		"phase=global-direct outcome=would-delete type=AWS::IAM::Role resource_region=global name=\"lagotto-instance-role\"",
		// self: checked by the guard, removed by jobs.
		"phase=self outcome=would-delete type=AWS::Lambda::Function",
		"phase=self outcome=would-delete type=AWS::Logs::LogGroup",
		"phase=global-self outcome=would-delete type=AWS::IAM::Role resource_region=global name=\"lagotto-capacity-poller-role\"",
		// final: its own rate schedule, then lagotto's group.
		"phase=final outcome=would-delete type=AWS::Scheduler::Schedule resource_region=us-east-1 name=\"lagotto-capacity-poller\"",
		"phase=final outcome=would-delete type=AWS::Scheduler::ScheduleGroup resource_region=us-east-1 name=\"lagotto\"",
		// left for the CLI.
		"phase=global-kept outcome=kept type=AWS::IAM::Role resource_region=global name=\"lagotto-capacity-poller-scheduler-invoke\"",
		"phase=global-kept outcome=kept type=AWS::IAM::ManagedPolicy",
		// the other region, judged.
		`phase=global other_region=us-west-2 standing=done why="no lifecycle item"`,
		// the jobs, with their offsets.
		// (each after the poller's next hourly run, lagotto#340).
		"phase=one-shot name=lagotto-teardown-function offset=+1h32m0s at=2026-11-17T13:33:00Z group=lagotto-teardown target=arn:aws:scheduler:::aws-sdk:lambda:deleteFunction input={\"FunctionName\":\"lagotto-capacity-poller\"}",
		"phase=one-shot name=lagotto-teardown-role-inline offset=+1h33m0s",
		"phase=one-shot name=lagotto-teardown-role-managed offset=+1h33m0s",
		"phase=one-shot name=lagotto-teardown-role offset=+1h34m0s",
		"phase=one-shot name=lagotto-teardown-log-group offset=+1h47m0s at=2026-11-17T13:48:00Z group=lagotto-teardown target=arn:aws:scheduler:::aws-sdk:cloudwatchlogs:deleteLogGroup input={\"LogGroupName\":\"/aws/lambda/lagotto-capacity-poller\"}",
		// the summary.
		"idle-teardown REPORT region=us-east-1 would_delete=12 kept=2 failed=0 one_shots=5 last_region=true lease=released",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("log lacks %q", want)
		}
	}
	// Order: direct, self, one-shots, final, then the global step: the
	// other regions, what stays, the direct deletes, the poller role and
	// its jobs.
	var phases []string
	for _, l := range strings.Split(log, "\n") {
		if m := rowRE.FindStringSubmatch(l); m != nil && (len(phases) == 0 || phases[len(phases)-1] != m[1]) {
			phases = append(phases, m[1])
		}
	}
	if got := strings.Join(phases, ","); got != "direct,self,one-shot,final,global,global-kept,global-direct,global-self,one-shot" {
		t.Errorf("phases logged in order %s", got)
	}
	if t.Failed() {
		t.Logf("log:\n%s", log)
	}
}

// TestReportNotLastRegion: another region's poller is in use, so the
// account-wide IAM is not in the plan, and no role job is planned.
func TestReportNotLastRegion(t *testing.T) {
	h := newHarness(t)
	h.src.SetRegion(inventorytest.Deployed("us-west-2", acct))
	if err := h.client.MarkActive(context.Background(), Tool, "us-west-2"); err != nil {
		t.Fatal(err)
	}
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReported || r.Plan.Last || r.Plan.Global != nil {
		t.Fatalf("result %+v", r)
	}
	log := h.log()
	if strings.Contains(log, "resource_region=global") || strings.Contains(log, "lagotto-teardown-role") {
		t.Errorf("not the last region, but the plan names IAM:\n%s", log)
	}
	for _, want := range []string{"one_shots=2 last_region=false",
		`last_region_why="still active: us-west-2 (state ACTIVE)"`} {
		if !strings.Contains(log, want) {
			t.Errorf("log lacks %q:\n%s", want, log)
		}
	}
}

// TestReportTablesOnlyRegionDoesNotBlock: lagotto's tables in a region with
// no poller and no lifecycle item (left by a lagotto from before #325) need
// none of the account-wide resources: they no longer keep them (#340).
func TestReportTablesOnlyRegionDoesNotBlock(t *testing.T) {
	h := newHarness(t)
	h.src.SetRegion(inventory.Region{Region: "us-west-2", Tables: inventorytest.Deployed("us-west-2", acct).Tables})
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReported || !r.Plan.Last || r.Plan.Global == nil {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
}

// TestReportGuardStillApplies: a log group recreated untagged (lagotto#326's
// live evidence) is refused by the guard, and the function's job still
// plans the log group's removal after it.
func TestReportGuardStillApplies(t *testing.T) {
	h := newHarness(t)
	h.src.Update(region, func(r *inventory.Region) { r.LogGroup.Tags = nil })
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReported {
		t.Fatalf("result %+v", r)
	}
	log := h.log()
	if !strings.Contains(log, "phase=self outcome=kept type=AWS::Logs::LogGroup") ||
		!strings.Contains(log, "name=lagotto-teardown-log-group offset=+1h47m0s") {
		t.Errorf("log:\n%s", log)
	}
}

// ---- reap mode (lagotto#328; never enabled by this release's poller) --------

// TestReapHandsOffItsOwnRemoval: deletes the direct phase, never its own
// function, log group or role, creates the jobs, then deletes its own rate
// schedule and group last, and finishes the lease and closes the table.
func TestReapHandsOffItsOwnRemoval(t *testing.T) {
	h := newHarness(t)
	r := h.tickPastGrace(t, true)
	if r.Outcome != OutcomeReaped || r.Err != nil {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
	ev := h.ev.list()
	joined := strings.Join(ev, "\n")
	for _, never := range []string{"function", "loggroup", "role " + runtimeiam.RoleName, "role " + runtimeiam.SchedulerInvokeRoleName, "policy"} {
		if strings.Contains(joined, "delete "+region+" "+never) || strings.Contains(joined, "delete global "+never) {
			t.Errorf("deleted %s directly:\n%s", never, joined)
		}
	}
	idx := func(s string) int {
		for i, e := range ev {
			if e == s {
				return i
			}
		}
		t.Errorf("no event %q in\n%s", s, joined)
		return -1
	}
	lastJob := idx("create one-shot lagotto-teardown/lagotto-teardown-log-group")
	if idx("delete us-east-1 table lagotto-watches") > idx("create group lagotto-teardown tags=5") ||
		lastJob > idx("delete us-east-1 schedule lagotto/lagotto-capacity-poller") ||
		idx("delete us-east-1 schedule lagotto/lagotto-capacity-poller") > idx("delete us-east-1 group lagotto") {
		t.Errorf("order:\n%s", joined)
	}
	idx("delete global profile lagotto-instance-role")
	idx("delete global role lagotto-instance-role")
	if len(h.sched.inputs) != 5 {
		t.Fatalf("%d jobs", len(h.sched.inputs))
	}
	for _, in := range h.sched.inputs {
		if in.ActionAfterCompletion != schedulertypes.ActionAfterCompletionDelete || aws.ToString(in.GroupName) != runtimeiam.TeardownScheduleGroup ||
			aws.ToString(in.Target.RoleArn) != runtimeiam.SchedulerInvokeRoleARN(acct) || !strings.HasPrefix(aws.ToString(in.Target.Arn), "arn:aws:scheduler:::aws-sdk:") ||
			in.FlexibleTimeWindow.Mode != schedulertypes.FlexibleTimeWindowModeOff {
			t.Errorf("job %s: %+v", aws.ToString(in.Name), in)
		}
	}
	if _, ok, _ := h.client.Get(context.Background(), Tool, region); ok {
		t.Error("the lease was not finished")
	}
	if n := h.table.Calls("DeleteTable"); n != 1 {
		t.Errorf("the lifecycle table was not closed (%d DeleteTable)", n)
	}
}

// TestReapResumesOnConflict: a teardown that crashed after creating some
// jobs runs again; the jobs (and the group) that exist answer
// ConflictException, which is success.
func TestReapResumesOnConflict(t *testing.T) {
	h := newHarness(t)
	h.sched.conflict = map[string]bool{"group": true, "lagotto-teardown-function": true, "lagotto-teardown-role": true}
	if r := h.tickPastGrace(t, true); r.Outcome != OutcomeReaped || r.Err != nil {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
}

// TestReapStopsOnJobFailure: a job that cannot be created gives the lease
// back before the poller deletes its own schedule.
func TestReapStopsOnJobFailure(t *testing.T) {
	h := newHarness(t)
	h.sched.err = errors.New("AccessDenied: scheduler:CreateSchedule")
	r := h.tickPastGrace(t, true)
	if r.Outcome != OutcomeFailed {
		t.Fatalf("result %+v", r)
	}
	if strings.Contains(strings.Join(h.ev.list(), "\n"), "lagotto-capacity-poller") {
		t.Errorf("deleted its own schedule after a failed job: %v", h.ev.list())
	}
	if it := h.item(t); it.Lease != nil || it.State != lifecycle.StateActive {
		t.Errorf("lease not given back: %+v", it)
	}
}

// TestReapStopsOnLostLease: a creator takes the lease over (it went stale
// mid-teardown); the next heartbeat fails and nothing more is deleted.
func TestReapStopsOnLostLease(t *testing.T) {
	h := newHarness(t)
	creator, err := lifecycle.New(lifecycle.Options{
		DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil },
		Owner:    "alice",
		Now:      func() time.Time { return h.clock().Add(time.Hour) }, // the lease looks stale to it
	})
	if err != nil {
		t.Fatal(err)
	}
	deletes := 0
	h.table.Hook = func(ctx context.Context, op string) {
		if op != "UpdateItem" {
			return
		}
		if deletes++; deletes == 3 { // the claim, a heartbeat, then the creator
			if err := creator.MarkActive(ctx, Tool, region); err != nil {
				t.Errorf("creator: %v", err)
			}
		}
	}
	r := h.tickPastGrace(t, true)
	if r.Outcome != OutcomeLeaseLost || !errors.Is(r.Err, lifecycle.ErrLeaseLost) {
		t.Fatalf("result %+v\n%s", r, h.log())
	}
	if n := len(h.ev.list()); n > 1 || len(h.sched.inputs) != 0 {
		t.Errorf("kept deleting after the lease was lost: %v", h.ev.list())
	}
}

// ---- the self-removal plan ----------------------------------------------------

func TestSelfRemovalPlanOffsets(t *testing.T) {
	jobs := SelfRemoval{Region: region, AccountID: acct, Function: true, LogGroup: true, Role: true}.Plan(t0)
	// In the order they are created: the poller role's first, the function's
	// (the commit) last; and every one fires after the poller's next hourly
	// run, so a teardown that stops after any of them runs once more and
	// finishes (lagotto#340).
	want := []struct {
		name string
		off  time.Duration
	}{{"lagotto-teardown-role-inline", 93 * time.Minute}, {"lagotto-teardown-role-managed", 93 * time.Minute},
		{"lagotto-teardown-role", 94 * time.Minute}, {"lagotto-teardown-log-group", 107 * time.Minute},
		{"lagotto-teardown-function", 92 * time.Minute}}
	if len(jobs) != len(want) {
		t.Fatalf("%d jobs", len(jobs))
	}
	for i, w := range want {
		if jobs[i].Name != w.name || jobs[i].Offset != w.off || !jobs[i].At.Equal(t0.Add(w.off)) {
			t.Errorf("job %d = %+v, want %s at +%s", i, jobs[i], w.name, w.off)
		}
		if w.off <= time.Hour {
			t.Errorf("job %s fires before the poller's next hourly run", w.name)
		}
	}
	if e := jobs[4].Expression(); e != "at(2026-11-16T13:32:00)" {
		t.Errorf("expression %s", e)
	}
	// The function goes before its role, and the log group after the function.
	if FunctionOffset >= RolePolicyOffset || RolePolicyOffset >= RoleOffset || FunctionOffset >= LogGroupOffset {
		t.Error("offsets out of order")
	}
	// The log group goes whenever the function does; the function's job last.
	if j := (SelfRemoval{Region: region, AccountID: acct, Function: true}).Plan(t0); len(j) != 2 || j[0].Name != "lagotto-teardown-log-group" || j[1].Name != FunctionJobName {
		t.Errorf("function only: %+v", j)
	}
	if j := (SelfRemoval{Region: region, AccountID: acct}).Plan(t0); len(j) != 0 {
		t.Errorf("nothing to remove: %+v", j)
	}
	if !strings.Contains(jobs[2].Why, "every other region is done") {
		t.Errorf("the role job's description: %q", jobs[2].Why)
	}
}

func TestCreateOneShotsIdempotent(t *testing.T) {
	ev := &events{}
	f := &fakeScheduler{e: ev, conflict: map[string]bool{"group": true, "lagotto-teardown-function": true}}
	jobs := SelfRemoval{Region: region, AccountID: acct, Function: true}.Plan(t0)
	var steps []string
	created, err := CreateOneShots(context.Background(), f, region, acct, t0, jobs, func(_ context.Context, s string) error {
		steps = append(steps, s)
		return nil
	})
	if err != nil || len(created) != 2 || len(steps) != 2 {
		t.Fatalf("created %v steps %v err %v", created, steps, err)
	}
	// A job already there keeps the time it was created with, so it is
	// deleted and created again at this run's (review 4, finding 3).
	if !slices.Contains(ev.list(), "delete one-shot lagotto-teardown/lagotto-teardown-function") || len(f.inputs) != 3 {
		t.Errorf("not recreated: %v (%d creates)", ev.list(), len(f.inputs))
	}
	// A job that conflicts again is an error.
	f.conflict[FunctionJobName] = true
	g := &stickyConflict{fakeScheduler: f}
	if _, err := CreateOneShots(context.Background(), g, region, acct, t0, jobs, nil); err == nil {
		t.Error("a job that conflicts after its delete was taken as created")
	}
	// A failed heartbeat stops before the next job.
	f.inputs = nil
	_, err = CreateOneShots(context.Background(), f, region, acct, t0, jobs, func(context.Context, string) error { return lifecycle.ErrLeaseLost })
	if !errors.Is(err, lifecycle.ErrLeaseLost) || len(f.inputs) != 0 {
		t.Errorf("err %v, %d jobs created", err, len(f.inputs))
	}
}

// ---- MarkActive -------------------------------------------------------------

func TestMarkActive(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, Owner: "alice"})
	if err := MarkActive(ctx, c, region, "create watch w-1"); err != nil {
		t.Fatal(err)
	}
	if it, ok, _ := c.Get(ctx, Tool, region); !ok || it.Epoch != 1 || it.State != lifecycle.StateActive {
		t.Errorf("item %+v", it)
	}

	// A live teardown lease for the whole wait: refused, clearly.
	holder, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, Owner: "lagotto-poller/us-east-1"})
	if _, _, err := holder.Observe(ctx, Tool, region, false); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Claim(ctx, Tool, region, 0); err != nil {
		t.Fatal(err)
	}
	blocked, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil },
		Sleep: func(context.Context, time.Duration) error { return nil }})
	err := MarkActive(ctx, blocked, region, "create watch w-2")
	var tip *TeardownInProgressError
	if !errors.As(err, &tip) || !errors.Is(err, lifecycle.ErrWaitTimeout) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"refusing to create watch w-2", "being torn down in us-east-1", "lagotto-poller/us-east-1", "Nothing was created"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}

	// IAM that predates the table: a warning, not a refusal.
	denied := lifecycletest.New()
	denied.Err = func(string, any) error { return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"} }
	d, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return denied, nil }})
	if err := MarkActive(ctx, d, region, "x"); !errors.Is(err, ErrLeaseUnavailable) || !strings.Contains(LeaseWarning(err), "lagotto setup") {
		t.Errorf("AccessDenied: %v", err)
	}
	// Anything else refuses.
	broken := lifecycletest.New()
	broken.Err = func(string, any) error { return errors.New("network down") }
	b, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return broken, nil }})
	if err := MarkActive(ctx, b, region, "x"); err == nil || errors.Is(err, ErrLeaseUnavailable) || !strings.Contains(err.Error(), "Nothing was created") {
		t.Errorf("other failure: %v", err)
	}
}

// ---- the poller's deleters ----------------------------------------------------

// TestPollerDeletersRefuseItself: the AWS deleters the poller runs with refuse
// its own function, log group, role, the invoke role and the CLI policy,
// before any AWS call.
func TestPollerDeletersRefuseItself(t *testing.T) {
	d := PollerDeleters(aws.Config{Region: region}, acct, "", "", "")
	ctx := context.Background()
	rd := d.Region(region)
	if _, err := rd.DeletePollerFunction(ctx); !errors.Is(err, ErrSelfDelete) {
		t.Errorf("function: %v", err)
	}
	if _, err := rd.DeletePollerLogGroup(ctx, region); !errors.Is(err, ErrSelfDelete) {
		t.Errorf("log group: %v", err)
	}
	for _, role := range []string{runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName, "team-gpu"} {
		if _, err := d.Account.DeleteRole(ctx, role); !errors.Is(err, ErrSelfDelete) {
			t.Errorf("role %s: %v", role, err)
		}
	}
	if _, err := d.Account.DeleteInstanceProfile(ctx, "team-gpu"); !errors.Is(err, ErrSelfDelete) {
		t.Errorf("profile: %v", err)
	}
	if _, err := d.Account.DeletePolicy(ctx); !errors.Is(err, ErrSelfDelete) {
		t.Errorf("policy: %v", err)
	}
	if d.Region(region) != rd {
		t.Error("a region's deleter is not reused")
	}
}

func TestPhaseOf(t *testing.T) {
	r := func(typ, name, arn string) Phase {
		p, _ := PhaseOf(fpResource(typ, name, arn))
		return p
	}
	if r(inventory.TypeSchedule, "lagotto-teardown-function", inventory.ScheduleARN(region, acct, runtimeiam.TeardownScheduleGroup, "lagotto-teardown-function")) != PhaseCLI {
		t.Error("a pending teardown job is the poller's to delete")
	}
	if r(inventory.TypeSchedule, "lagotto-launch-sl-1", inventory.ScheduleARN(region, acct, "lagotto", "lagotto-launch-sl-1")) != PhaseDirect {
		t.Error("a launch schedule is not direct")
	}
	if r(inventory.TypeSchedule, deploy.PollerScheduleName, inventory.ScheduleARN(region, acct, "lagotto", deploy.PollerScheduleName)) != PhaseFinal {
		t.Error("the rate schedule is not final")
	}
	if r(inventory.TypeRole, runtimeiam.RoleName, "") != PhaseSelf || r(inventory.TypeFunction, "f", "") != PhaseSelf ||
		r(inventory.TypeLogGroup, "g", "") != PhaseSelf {
		t.Error("its own function, log group or role is not a job's")
	}
	if r(inventory.TypeStack, "lagotto", "") != PhaseCLI || r(inventory.TypeCapacityReservation, "cr", "") != PhaseCLI {
		t.Error("the poller deletes a stack or a reservation")
	}
}

func fpResource(typ, name, arn string) fp.Resource {
	return fp.Resource{Type: typ, Name: name, ARN: arn}
}

func (f *fakeScheduler) DeleteSchedule(_ context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	f.e.add("delete one-shot %s/%s", aws.ToString(in.GroupName), aws.ToString(in.Name))
	delete(f.conflict, aws.ToString(in.Name)) // gone: a create no longer conflicts
	return &scheduler.DeleteScheduleOutput{}, nil
}

// stickyConflict's jobs conflict even after a delete.
type stickyConflict struct{ *fakeScheduler }

func (s *stickyConflict) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	return &scheduler.DeleteScheduleOutput{}, nil
}
