package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// absentAWS answers every read the poller's deleters start with: nothing is
// there.
type absentAWS struct{ calls []string }

func (a *absentAWS) rec(op string) { a.calls = append(a.calls, op) }

func (a *absentAWS) DescribeTable(context.Context, *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	a.rec("DescribeTable")
	return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("no table")}
}
func (a *absentAWS) DeleteTable(context.Context, *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	a.rec("DeleteTable")
	return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("no table")}
}
func (a *absentAWS) GetItem(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	a.rec("GetItem")
	return &dynamodb.GetItemOutput{}, nil
}
func (a *absentAWS) GetBucketTagging(context.Context, *s3.GetBucketTaggingInput) (*s3.GetBucketTaggingOutput, error) {
	a.rec("GetBucketTagging")
	return nil, &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "no bucket"}
}
func (a *absentAWS) HeadBucket(context.Context, *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	a.rec("HeadBucket")
	return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "no bucket"}
}
func (a *absentAWS) GetTopicAttributes(context.Context, *sns.GetTopicAttributesInput) (*sns.GetTopicAttributesOutput, error) {
	a.rec("GetTopicAttributes")
	return nil, &snstypes.NotFoundException{Message: aws.String("no topic")}
}
func (a *absentAWS) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput) (*scheduler.DeleteScheduleOutput, error) {
	a.rec("DeleteSchedule")
	return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no schedule")}
}
func (a *absentAWS) ListSchedules(context.Context, *scheduler.ListSchedulesInput) (*scheduler.ListSchedulesOutput, error) {
	a.rec("ListSchedules")
	return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no group")}
}
func (a *absentAWS) DeleteScheduleGroup(context.Context, *scheduler.DeleteScheduleGroupInput) (*scheduler.DeleteScheduleGroupOutput, error) {
	a.rec("DeleteScheduleGroup")
	return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no group")}
}
func (a *absentAWS) ListRolePolicies(context.Context, *iam.ListRolePoliciesInput) (*iam.ListRolePoliciesOutput, error) {
	a.rec("ListRolePolicies")
	return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no role")}
}
func (a *absentAWS) GetInstanceProfile(context.Context, *iam.GetInstanceProfileInput) (*iam.GetInstanceProfileOutput, error) {
	a.rec("GetInstanceProfile")
	return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no profile")}
}

// TestPollerDeletersReachTheRealDeleters: what the poller may delete goes
// through lagotto's own deleters, with real SDK clients, and an absent
// resource is reported absent rather than an error.
func TestPollerDeletersReachTheRealDeleters(t *testing.T) {
	fake := &absentAWS{}
	cfg := aws.Config{Region: region, APIOptions: []func(*middleware.Stack) error{testutil.AnswerFrom(fake)}}
	d := PollerDeleters(cfg, acct, "", "", "")
	ctx := context.Background()
	rd := d.Region(region)
	for _, name := range []string{"lagotto-watches", "lagotto-scheduled-launches"} {
		if out, err := rd.DeleteTable(ctx, name); err != nil || out != watcher.TableAbsent {
			t.Errorf("table %s: %v, %v", name, out, err)
		}
	}
	if _, err := rd.DeleteTable(ctx, "someone-elses"); err == nil {
		t.Error("deleted a table that is not lagotto's")
	}
	if out, err := rd.DeleteArtifactBucket(ctx, region, acct); err != nil || out.Outcome != deploy.BucketAbsent {
		t.Errorf("bucket: %+v, %v", out, err)
	}
	if ok, err := rd.DeleteAlertsTopic(ctx, region, acct); ok || err != nil {
		t.Errorf("topic: %v, %v", ok, err)
	}
	if ok, err := rd.DeleteSchedule(ctx, "lagotto", "lagotto-launch-sl-1"); ok || err != nil {
		t.Errorf("schedule: %v, %v", ok, err)
	}
	if ok, err := rd.DeleteScheduleGroup(ctx, "lagotto"); ok || err != nil {
		t.Errorf("group: %v, %v", ok, err)
	}
	if ok, err := d.Account.DeleteRole(ctx, runtimeiam.InstanceRoleName); ok || err != nil {
		t.Errorf("instance role: %v, %v", ok, err)
	}
	if ok, err := d.Account.DeleteInstanceProfile(ctx, runtimeiam.InstanceRoleName); ok || err != nil {
		t.Errorf("instance profile: %v, %v", ok, err)
	}
	if strings.Contains(strings.Join(fake.calls, ","), "DeleteFunction") {
		t.Errorf("calls %v", fake.calls)
	}
}

// ---- the run's failure paths: each fails closed and is logged ---------------

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.Grace != DefaultGrace || c.Now == nil || c.Logf == nil || c.Reap || c.mode() != "report" || c.label() != "REPORT" {
		t.Errorf("defaults %+v", c)
	}
	c.Logf("a %s line on stderr", "default")
	if (Config{Reap: true}).label() != "REAP" {
		t.Error("reap label")
	}
}

// TestTickLogsUnreadableSignals: a signal that cannot be read is logged and
// makes the region active, so nothing is claimed.
func TestTickLogsUnreadableSignals(t *testing.T) {
	h := newHarness(t)
	h.src.Fail(region+" Jobs", errors.New("AccessDenied: sagemaker:Search"))
	h.advance(0)
	r := Tick(context.Background(), h.cfg(false), h.deps())
	if r.Outcome != OutcomeActive || !strings.Contains(h.log(), `idle-verdict region=us-east-1 warning="us-east-1: could not read the SageMaker training jobs: AccessDenied: sagemaker:Search"`) {
		t.Errorf("result %+v\n%s", r, h.log())
	}
}

// TestTickObserveFailures: the lease table failing, or being deleted, ends
// the run with nothing claimed.
func TestTickObserveFailures(t *testing.T) {
	h := newHarness(t)
	h.table.Err = func(string, any) error { return errors.New("throttled") }
	if r := Tick(context.Background(), h.cfg(false), h.deps()); r.Outcome != OutcomeFailed || !strings.Contains(h.log(), "observe=failed") {
		t.Errorf("table failing: %+v\n%s", r, h.log())
	}

	h = newHarness(t)
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	closing := map[string]ddbtypes.AttributeValue{
		"pk":    &ddbtypes.AttributeValueMemberS{Value: lifecycle.SentinelKey},
		"since": &ddbtypes.AttributeValueMemberS{Value: h.clock().UTC().Format("2006-01-02T15:04:05.000Z")},
		"token": &ddbtypes.AttributeValueMemberS{Value: "t"},
	}
	if err := h.table.PutRaw(lifecycle.TableName, closing); err != nil {
		t.Fatal(err)
	}
	// A write is needed for the sentinel to be seen: the region turns active.
	h.src.Update(region, func(r *inventory.Region) { r.ActiveWatches = 1 })
	if r := Tick(ctx, h.cfg(false), h.deps()); !errors.Is(r.Err, lifecycle.ErrTableClosing) || !strings.Contains(h.log(), "observe=skipped") {
		t.Errorf("table closing: %+v\n%s", r, h.log())
	}
}

// TestTickClaimRefused: another teardown holds a live lease, or the claim
// write fails; nothing is planned.
func TestTickClaimRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	h.advance(25 * time.Hour)
	other, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil },
		Owner: "lagotto teardown --plan", Now: h.clock})
	if _, err := other.Claim(ctx, Tool, region, 0); err != nil {
		t.Fatal(err)
	}
	r := Tick(ctx, h.cfg(false), h.deps())
	if r.Outcome != OutcomeNotClaimable || r.Plan != nil || !strings.Contains(h.log(), "claim=refused") ||
		!strings.Contains(h.log(), "lease held by lagotto teardown --plan") {
		t.Errorf("held: %+v\n%s", r, h.log())
	}

	h = newHarness(t)
	h.table.Err = func(op string, _ any) error {
		if op == "UpdateItem" {
			return errors.New("throttled")
		}
		return nil
	}
	if r := h.tickPastGrace(t, false); r.Outcome != OutcomeFailed || r.Plan != nil {
		t.Errorf("claim failing: %+v", r)
	}
}

// TestTeardownFootprintFailures: after the claim, a footprint that cannot be
// listed, a region that is not enabled, or one that cannot be planned gives
// the lease back with nothing deleted.
func TestTeardownFootprintFailures(t *testing.T) {
	cases := map[string]func(h *harness){
		"regions unreadable": func(h *harness) { h.src.Fail("global EnabledRegions", errors.New("AccessDenied")) },
		"duplicate resource": func(h *harness) {
			h.src.Update(region, func(r *inventory.Region) { r.Tables = append(r.Tables, r.Tables[0]) })
		},
	}
	for name, set := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeWaiting {
				t.Fatalf("%+v", r)
			}
			set(h)
			h.advance(25 * time.Hour)
			r := Tick(ctx, h.cfg(true), h.deps())
			if r.Err == nil || (r.Outcome != OutcomeReleased && r.Outcome != OutcomeFailed) || len(h.ev.list()) != 0 {
				t.Errorf("result %+v, deletes %v", r, h.ev.list())
			}
			if it := h.item(t); it.Lease != nil {
				t.Errorf("lease kept: %+v", it)
			}
		})
	}

	// The poller's own region is not among the enabled ones.
	h := newHarness(t, "us-west-2")
	r := h.tickPastGrace(t, false)
	if r.Outcome != OutcomeReleased || !strings.Contains(h.log(), "us-east-1 is not among the account's enabled regions") {
		t.Errorf("region not enabled: %+v\n%s", r, h.log())
	}
}

// failUpdatesAfter fails every UpdateItem after the first n (the claim):
// heartbeats and the release, with an error that is not a lost lease.
func failUpdatesAfter(h *harness, n int) {
	count := 0
	h.table.Err = func(op string, _ any) error {
		if op != "UpdateItem" {
			return nil
		}
		if count++; count > n {
			return errors.New("throttled")
		}
		return nil
	}
}

// TestReportCarriesOnPastAFailedHeartbeat: in report mode a heartbeat that
// fails (not a lost lease) stops that phase's execution but not the report,
// and the failed release is reported.
func TestReportCarriesOnPastAFailedHeartbeat(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	h.advance(25 * time.Hour)
	failUpdatesAfter(h, 1)
	r := Tick(ctx, h.cfg(false), h.deps())
	if r.Outcome != OutcomeFailed || r.Plan == nil || r.Plan.Failed() == 0 || len(h.ev.list()) != 0 {
		t.Fatalf("result %+v, deletes %v", r, h.ev.list())
	}
	for _, want := range []string{"phase=direct outcome=stopped", "phase=self outcome=stopped", "phase=final outcome=stopped", "lease=release-failed"} {
		if !strings.Contains(h.log(), want) {
			t.Errorf("log lacks %q:\n%s", want, h.log())
		}
	}
}

// TestReapStopsOnAFailedHeartbeat: in reap mode the same failure stops the
// teardown before anything else, and tries to give the lease back. The first
// write after the claim is the global lease's (taken before the first
// delete), so that is where it stops.
func TestReapStopsOnAFailedHeartbeat(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(true), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	h.advance(25 * time.Hour)
	failUpdatesAfter(h, 1)
	r := Tick(ctx, h.cfg(true), h.deps())
	if r.Outcome != OutcomeFailed || len(h.ev.list()) != 0 || len(h.sched.inputs) != 0 {
		t.Fatalf("result %+v, deletes %v, jobs %d", r, h.ev.list(), len(h.sched.inputs))
	}
	if !strings.Contains(h.log(), `lease=release-failed reason="taking the global teardown lease failed`) {
		t.Errorf("log:\n%s", h.log())
	}
}

// TestReportLeaseLostAtRelease: a creator takes the lease over just before
// the report gives it back; the run says so.
func TestReportLeaseLostAtRelease(t *testing.T) {
	h := newHarness(t)
	creator, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return h.table, nil },
		Now: func() time.Time { return h.clock().Add(time.Hour) }})
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(false), h.deps()); r.Outcome != OutcomeWaiting {
		t.Fatalf("%+v", r)
	}
	h.advance(25 * time.Hour)
	updates := 0
	h.table.Hook = func(ctx context.Context, op string) {
		// The claim, 9 heartbeats (the region's own rows; the account-wide
		// rows come after the release, with no lease), then the release.
		if op == "UpdateItem" {
			if updates++; updates == 11 {
				if err := creator.MarkActive(ctx, Tool, region); err != nil {
					t.Error(err)
				}
			}
		}
	}
	r := Tick(ctx, h.cfg(false), h.deps())
	if r.Outcome != OutcomeLeaseLost || !strings.Contains(h.log(), "lease=release-failed") {
		t.Errorf("result %+v (%d updates)\n%s", r, updates, h.log())
	}
}

// TestReapFinishAndCloseFailures: a lease that cannot be finished fails the
// run; a lifecycle table that cannot be closed is only logged.
func TestReapFinishAndCloseFailures(t *testing.T) {
	h := newHarness(t)
	h.table.Err = func(op string, _ any) error {
		if op == "DeleteItem" {
			return errors.New("throttled")
		}
		return nil
	}
	if r := h.tickPastGrace(t, true); r.Outcome != OutcomeFailed || !strings.Contains(h.log(), "lease=finish-failed") {
		t.Errorf("finish failing: %+v\n%s", r, h.log())
	}

	h = newHarness(t)
	h.table.Err = func(op string, _ any) error {
		if op == "PutItem" {
			return errors.New("throttled")
		}
		return nil
	}
	if r := h.tickPastGrace(t, true); r.Outcome != OutcomeReaped || !strings.Contains(h.log(), "close_table=failed") {
		t.Errorf("close failing: %+v\n%s", r, h.log())
	}
}

func TestPhaseOfEveryType(t *testing.T) {
	for _, c := range []struct {
		typ, name string
		want      Phase
	}{
		{inventory.TypeScheduleGroup, "lagotto", PhaseFinal},
		{inventory.TypeScheduleGroup, runtimeiam.TeardownScheduleGroup, PhaseCLI},
		{inventory.TypeScheduleGroup, "other", PhaseCLI},
		{inventory.TypeRole, runtimeiam.InstanceRoleName, PhaseDirect},
		{inventory.TypeRole, runtimeiam.SchedulerInvokeRoleName, PhaseCLI},
		{inventory.TypeRole, "team-gpu", PhaseCLI},
		{inventory.TypeInstanceProfile, runtimeiam.InstanceRoleName, PhaseDirect},
		{inventory.TypeInstanceProfile, "team-gpu", PhaseCLI},
		{inventory.TypePolicy, "lagotto-cli-user", PhaseCLI},
		{inventory.TypeTable, "lagotto-watches", PhaseDirect},
		{inventory.TypeBucket, "b", PhaseDirect},
		{inventory.TypeTopic, "t", PhaseDirect},
		{inventory.TypeTrainingJob, "j", PhaseCLI},
	} {
		got, why := PhaseOf(fpResource(c.typ, c.name, ""))
		if got != c.want || (got == PhaseCLI) != (why != "") {
			t.Errorf("%s %s: %s %q, want %s", c.typ, c.name, got, why, c.want)
		}
	}
}

func TestCreateOneShotsFailures(t *testing.T) {
	ctx := context.Background()
	if created, err := CreateOneShots(ctx, nil, region, acct, t0, nil, nil); created != nil || err != nil {
		t.Errorf("no jobs: %v %v", created, err)
	}
	f := &groupFails{}
	_, err := CreateOneShots(ctx, f, region, acct, t0, SelfRemoval{Region: region, AccountID: acct, Function: true}.Plan(t0), nil)
	if err == nil || !strings.Contains(err.Error(), "create schedule group lagotto-teardown") || f.schedules != 0 {
		t.Errorf("group failing: %v, %d jobs", err, f.schedules)
	}
}

type groupFails struct{ schedules int }

func (g *groupFails) CreateScheduleGroup(context.Context, *scheduler.CreateScheduleGroupInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleGroupOutput, error) {
	return nil, errors.New("AccessDenied: scheduler:CreateScheduleGroup")
}

func (g *groupFails) CreateSchedule(context.Context, *scheduler.CreateScheduleInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	g.schedules++
	return &scheduler.CreateScheduleOutput{}, nil
}

// TestNewClient: the lease client reaches each region's table with the
// config's credentials, and applies the options given.
func TestNewClient(t *testing.T) {
	fake := &absentAWS{}
	cfg := aws.Config{Region: region, APIOptions: []func(*middleware.Stack) error{testutil.AnswerFrom(fake)}}
	applied := false
	c, err := NewClient(cfg, "alice", func(o *lifecycle.Options) { applied = o.Owner == "alice" })
	if err != nil || !applied {
		t.Fatalf("NewClient: %v, options applied %v", err, applied)
	}
	if _, ok, err := c.Get(context.Background(), Tool, "eu-west-1"); ok || err != nil {
		t.Errorf("Get on an empty table: %v %v", ok, err)
	}
	if len(fake.calls) != 1 || fake.calls[0] != "GetItem" {
		t.Errorf("calls %v", fake.calls)
	}
	_ = lifecycletest.New
}
