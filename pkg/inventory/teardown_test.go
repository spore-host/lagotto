package inventory

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// fakeDeleters records every delete, in order, as "<region> <what> <name>".
type fakeDeleters struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeDeleters) rec(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

type fakeRegion struct {
	f      *fakeDeleters
	region string
}

func (r fakeRegion) DeleteTable(_ context.Context, name string) (watcher.TableOutcome, error) {
	r.f.rec(r.region + " table " + name)
	return watcher.TableDeleted, nil
}
func (r fakeRegion) DeleteArtifactBucket(_ context.Context, region, _ string) (deploy.BucketTeardown, error) {
	r.f.rec(r.region + " bucket " + region)
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, nil
}
func (r fakeRegion) DeleteAlertsTopic(context.Context, string, string) (bool, error) {
	r.f.rec(r.region + " topic")
	return true, nil
}
func (r fakeRegion) DeletePollerFunction(context.Context) (bool, error) {
	r.f.rec(r.region + " function")
	return true, nil
}
func (r fakeRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	r.f.rec(r.region + " loggroup")
	return true, nil
}
func (r fakeRegion) DeleteSchedule(_ context.Context, group, name string) (bool, error) {
	r.f.rec(r.region + " schedule " + group + "/" + name)
	return true, nil
}
func (r fakeRegion) DeleteScheduleGroup(_ context.Context, name string) (bool, error) {
	r.f.rec(r.region + " group " + name)
	return true, nil
}

type fakeAccount struct{ f *fakeDeleters }

func (a fakeAccount) DeleteRole(_ context.Context, name string) (bool, error) {
	a.f.rec("global role " + name)
	return true, nil
}
func (a fakeAccount) DeleteInstanceProfile(_ context.Context, name string) (bool, error) {
	a.f.rec("global profile " + name)
	return true, nil
}
func (a fakeAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	a.f.rec("global policy")
	return cliiam.Deletion{Deleted: true}, nil
}

func (f *fakeDeleters) deleters() Deleters {
	return Deleters{AccountID: acct, Region: func(region string) RegionDeleter { return fakeRegion{f, region} }, Account: fakeAccount{f}}
}

func testLease(t *testing.T) *lifecycle.Lease {
	t.Helper()
	ddb := lifecycletest.New()
	c, err := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return ddb, nil }, ControlRegion: "local"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := c.Observe(ctx, Tool, lifecycle.ScopeGlobal, false); err != nil {
		t.Fatal(err)
	}
	l, err := c.Claim(ctx, Tool, lifecycle.ScopeGlobal, 0)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// run plans and executes rep, re-checked against live, and returns what was
// deleted.
func run(t *testing.T, planned, live *fp.Report, mode fp.Mode) (*fp.Result, []Kept, []string, error) {
	t.Helper()
	plan, kept, err := PlanTeardown(planned, live, Options{})
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDeleters{}
	var lease *lifecycle.Lease
	if mode == fp.ModeReap {
		lease = testLease(t)
	}
	res, err := fp.Execute(context.Background(), plan, mode, NewDeleter(live, f.deleters()), lease)
	return res, kept, f.calls, err
}

func indexOf(calls []string, s string) int {
	for i, c := range calls {
		if c == s {
			return i
		}
	}
	return -1
}

// TestReportModeDeletesNothing: report mode re-checks every step and records
// what would go, but calls no deleter.
func TestReportModeDeletesNothing(t *testing.T) {
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{deployed("us-east-1"), tablesOnly("us-west-2")}, Account: account(0)}, Options{}, now)
	res, _, calls, err := run(t, rep, rep, fp.ModeReport)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Errorf("report mode deleted: %v", calls)
	}
	if res.Count(fp.OutcomeWouldDelete) == 0 || res.Count(fp.OutcomeDeleted) != 0 {
		t.Errorf("would-delete %d, deleted %d", res.Count(fp.OutcomeWouldDelete), res.Count(fp.OutcomeDeleted))
	}
}

// TestReapIdleAccount: reap deletes every tagged resource of an idle account,
// dependents first, and nothing foreign.
func TestReapIdleAccount(t *testing.T) {
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{deployed("us-east-1"), tablesOnly("us-west-2")}, Account: account(0)}, Options{}, now)
	_, kept, calls, err := run(t, rep, rep, fp.ModeReap)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(kept) != 0 {
		t.Errorf("kept %+v in an idle account", kept)
	}
	for _, want := range []string{
		"us-east-1 schedule lagotto/" + deploy.PollerScheduleName, "us-east-1 function", "us-east-1 loggroup", "us-east-1 topic",
		"us-east-1 group lagotto", "us-east-1 bucket us-east-1", "us-west-2 table lagotto-watches", "us-west-2 table lagotto-scheduled-launches",
		"global role " + runtimeiam.RoleName, "global role " + runtimeiam.SchedulerInvokeRoleName, "global role " + runtimeiam.InstanceRoleName,
		"global profile " + runtimeiam.InstanceRoleName, "global profile team-gpu", "global policy",
	} {
		if indexOf(calls, want) < 0 {
			t.Errorf("not deleted: %s (calls %v)", want, calls)
		}
	}
	if indexOf(calls, "global role team-gpu") >= 0 {
		t.Error("deleted the administrator's authorized role")
	}
	order := [][2]string{
		{"us-east-1 schedule lagotto/" + deploy.PollerScheduleName, "us-east-1 group lagotto"},
		{"us-east-1 schedule lagotto/" + deploy.PollerScheduleName, "us-east-1 function"},
		{"us-east-1 function", "global role " + runtimeiam.RoleName},
		{"us-east-1 function", "us-east-1 table lagotto-watches"},
		{"us-east-1 function", "us-east-1 loggroup"},
		{"global profile " + runtimeiam.InstanceRoleName, "global role " + runtimeiam.InstanceRoleName},
	}
	for _, o := range order {
		if a, b := indexOf(calls, o[0]), indexOf(calls, o[1]); a > b {
			t.Errorf("%q deleted after %q", o[0], o[1])
		}
	}
	if last := calls[len(calls)-1]; last != "us-east-1 loggroup" {
		t.Errorf("last delete %q, want the log group", last)
	}
}

// TestReapKeepsActive: in an account where one region is active, reap
// deletes the orphan tables of the idle region and nothing that is in use,
// nor anything an active resource depends on.
func TestReapKeepsActive(t *testing.T) {
	east := deployed("us-east-1")
	east.ActiveWatches = 1
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{east, tablesOnly("us-west-2")}, Account: account(0)}, Options{}, now)
	_, kept, calls, err := run(t, rep, rep, fp.ModeReap)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range calls {
		if strings.HasPrefix(c, "us-east-1 ") {
			t.Errorf("deleted %q in the active region", c)
		}
		if strings.HasPrefix(c, "global role ") {
			t.Errorf("deleted %q while a poller runs", c)
		}
	}
	for _, name := range []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"} {
		if indexOf(calls, "us-west-2 table "+name) < 0 {
			t.Errorf("orphan table %s in us-west-2 not deleted (calls %v)", name, calls)
		}
	}
	keptARNs := map[string]string{}
	for _, k := range kept {
		keptARNs[k.Resource.ARN] = k.Reason
	}
	if r := keptARNs[runtimeiam.SchedulerInvokeRoleARN(acct)]; !strings.Contains(r, "depends on it") {
		t.Errorf("invoke role (residue) kept for %q, want: an active schedule depends on it", r)
	}
	if r := keptARNs[TableARN("us-east-1", acct, "lagotto-watches")]; r != "active" {
		t.Errorf("active table kept for %q", r)
	}
}

// TestReapRecheck: the live footprint decides. A planned resource that is gone
// is "gone"; one that became active since the plan is kept, with what it
// depends on; one whose tags changed is refused.
func TestReapRecheck(t *testing.T) {
	planned := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{tablesOnly("us-west-2")}}, Options{}, now)

	liveRegion := tablesOnly("us-west-2")
	liveRegion.Tables = liveRegion.Tables[:2]                               // the scheduled table is gone
	liveRegion.Tables[1].Tags = map[string]string{"lagotto:managed": "cli"} // history lost its spore.host tags
	live := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{liveRegion}}, Options{}, now)

	res, _, calls, err := run(t, planned, live, fp.ModeReap)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]fp.Outcome{}
	for _, a := range res.Actions {
		got[a.Name] = a.Outcome
	}
	if got["lagotto-scheduled-launches"] != fp.OutcomeGone || got["lagotto-match-history"] != fp.OutcomeRefused || got["lagotto-watches"] != fp.OutcomeDeleted {
		t.Errorf("outcomes %v", got)
	}
	if len(calls) != 1 || calls[0] != "us-west-2 table lagotto-watches" {
		t.Errorf("calls %v", calls)
	}

	// The same plan, after a watch was created in the region.
	liveRegion = tablesOnly("us-west-2")
	liveRegion.ActiveWatches = 1
	live = Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{liveRegion}}, Options{}, now)
	_, kept, calls, err := run(t, planned, live, fp.ModeReap)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 || len(kept) != 3 {
		t.Errorf("deleted %v, kept %d of 3 tables that became active", calls, len(kept))
	}
}

// TestGuardRefusesServiceAndForeign: whatever the plan says, nothing tagged
// spore-host:role=service, nothing foreign and nothing legacy-name is
// deleted, and what they depend on is not tried.
func TestGuardRefusesServiceAndForeign(t *testing.T) {
	r := tablesOnly("us-west-2")
	r.Groups = []Item{{ARN: ScheduleGroupARN("us-west-2", acct, schedgroup.Name), Name: schedgroup.Name, Tags: with(lt("us-west-2"), tags.Role, tags.RoleService)}}
	r.Schedules = []Schedule{{Group: schedgroup.Name, Name: "lagotto-launch-sl-dead"}}
	r.Tables[0].Tags = nil
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}, Account: account(0)}, Options{}, now)
	// A hand-edited plan that claims the foreign role is lagotto's litter.
	for i := range rep.Resources {
		if rep.Resources[i].Name == "team-gpu" && rep.Resources[i].Type == TypeRole {
			rep.Resources[i].Class = fp.ClassLitter
		}
	}
	res, _, calls, err := run(t, rep, rep, fp.ModeReap)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range calls {
		if strings.Contains(c, "team-gpu") && strings.Contains(c, "role") || strings.Contains(c, "group lagotto") ||
			strings.Contains(c, "lagotto-launch-sl-dead") || c == "us-west-2 table lagotto-watches" {
			t.Errorf("deleted %q", c)
		}
	}
	out := map[string]fp.Outcome{}
	for _, a := range res.Actions {
		out[a.Type+" "+a.Name] = a.Outcome
	}
	// The schedule carries its group's role=service tag, so it is kept, and
	// its group, invoke role and table are skipped.
	if out[TypeSchedule+" lagotto-launch-sl-dead"] != fp.OutcomeKept || out[TypeScheduleGroup+" lagotto"] != fp.OutcomeSkipped ||
		out[TypeRole+" team-gpu"] != fp.OutcomeKept || out[TypeTable+" lagotto-watches"] != fp.OutcomeKept {
		t.Errorf("outcomes %v", out)
	}
}

// TestNoDeleterForWorkload: a hold or training job is never deleted by a
// teardown, even in a hand-edited plan that calls it litter.
func TestNoDeleterForWorkload(t *testing.T) {
	r := Region{Region: "us-east-1", Holds: []Item{{ARN: "arn:aws:ec2:us-east-1:" + acct + ":capacity-reservation/cr-1", Name: "cr-1", Tags: lt("us-east-1")}}}
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}}, Options{}, now)
	rep.Resources[0].Class = fp.ClassLitter
	live := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{{Region: "us-east-1"}}}, Options{}, now)
	live.Resources = append(live.Resources, rep.Resources[0]) // still there, and called litter
	res, _, calls, err := run(t, rep, live, fp.ModeReap)
	if len(calls) != 0 || res.Actions[0].Outcome != fp.OutcomeKept {
		t.Errorf("hold: outcome %s, calls %v, err %v", res.Actions[0].Outcome, calls, err)
	}
}

func TestPlanTeardownRefusesAnotherAccountOrTool(t *testing.T) {
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{tablesOnly("us-west-2")}}, Options{}, now)
	other := *rep
	other.Account = "210987654321"
	if _, _, err := PlanTeardown(rep, &other, Options{}); err == nil || !strings.Contains(err.Error(), "account") {
		t.Errorf("another account's plan: err %v", err)
	}
	spawn := *rep
	spawn.Tool = tags.ToolSpawn
	if _, _, err := PlanTeardown(&spawn, rep, Options{}); err == nil {
		t.Error("ran spawn's footprint")
	}
}

func TestDescribeInUse(t *testing.T) {
	r := tablesOnly("us-west-2")
	r.ActiveWatches = 1
	live := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}}, Options{}, now)
	d := NewDeleter(live, (&fakeDeleters{}).deleters())
	if _, err := d.Describe(context.Background(), live.Resources[0]); !errors.Is(err, ErrInUse) {
		t.Errorf("Describe of an active table: %v", err)
	}
	if _, err := d.Describe(context.Background(), fp.Resource{ARN: "arn:x", Type: TypeTable}); !errors.Is(err, fp.ErrNotFound) {
		t.Errorf("Describe of an unlisted table: %v", err)
	}
}

func TestScheduleOf(t *testing.T) {
	g, n, ok := scheduleOf(ScheduleARN("us-east-1", acct, "lagotto", "lagotto-launch-sl-1"))
	if !ok || g != "lagotto" || n != "lagotto-launch-sl-1" {
		t.Errorf("scheduleOf = %q %q %v", g, n, ok)
	}
	for _, bad := range []string{"arn:aws:scheduler:us-east-1:1:schedule-group/lagotto", "arn:aws:scheduler:us-east-1:1:schedule/a/b/c", "x"} {
		if _, _, ok := scheduleOf(bad); ok {
			t.Errorf("scheduleOf(%q) ok", bad)
		}
	}
}

// scriptedRegion answers every delete with what it is told: absent, or
// lagotto's tag missing.
type scriptedRegion struct {
	table  watcher.TableOutcome
	bucket deploy.BucketOutcome
	found  bool
	err    error
}

func (s scriptedRegion) DeleteTable(context.Context, string) (watcher.TableOutcome, error) {
	return s.table, s.err
}
func (s scriptedRegion) DeleteArtifactBucket(context.Context, string, string) (deploy.BucketTeardown, error) {
	return deploy.BucketTeardown{Outcome: s.bucket}, s.err
}
func (s scriptedRegion) DeleteAlertsTopic(context.Context, string, string) (bool, error) {
	return s.found, s.err
}
func (s scriptedRegion) DeletePollerFunction(context.Context) (bool, error) { return s.found, s.err }
func (s scriptedRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	return s.found, s.err
}
func (s scriptedRegion) DeleteSchedule(context.Context, string, string) (bool, error) {
	return s.found, s.err
}
func (s scriptedRegion) DeleteScheduleGroup(context.Context, string) (bool, error) {
	return s.found, s.err
}

type scriptedAccount struct {
	found  bool
	policy cliiam.Deletion
	err    error
}

func (s scriptedAccount) DeleteRole(context.Context, string) (bool, error) { return s.found, s.err }
func (s scriptedAccount) DeleteInstanceProfile(context.Context, string) (bool, error) {
	return s.found, s.err
}
func (s scriptedAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	return s.policy, s.err
}

// TestDeleterOutcomes: each type's deleter turns "it was not there" into
// footprint.ErrNotFound (counted as gone) and "it lacks lagotto's own tag"
// into an error (failed, so nothing it depends on is tried), and passes a
// real failure through.
func TestDeleterOutcomes(t *testing.T) {
	ctx := context.Background()
	east := deployed("us-east-1")
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{east}, Account: account(0)}, Options{}, now)
	res := map[string]fp.Resource{}
	for _, r := range rep.Resources {
		res[r.Type+" "+r.Name] = r
	}
	del := func(region RegionDeleter, account AccountDeleter) fp.ByType {
		return NewDeleter(rep, Deleters{AccountID: acct, Region: func(string) RegionDeleter { return region }, Account: account})
	}
	gone := scriptedRegion{table: watcher.TableAbsent, bucket: deploy.BucketAbsent}
	untagged := scriptedRegion{table: watcher.TableUnmanaged, bucket: deploy.BucketRetainedUntagged, found: true}
	broken := scriptedRegion{err: errors.New("AccessDenied")}

	regional := []string{TypeTable + " lagotto-watches", TypeBucket + " " + deploy.DefaultBucketName(acct, "us-east-1"),
		TypeTopic + " " + deploy.AlertsTopicName, TypeFunction + " " + deploy.PollerFunctionName, TypeLogGroup + " " + deploy.PollerLogGroupName,
		TypeSchedule + " " + deploy.PollerScheduleName, TypeScheduleGroup + " lagotto"}
	for _, k := range regional {
		r := res[k]
		if err := del(gone, nil).Delete(ctx, r); !errors.Is(err, fp.ErrNotFound) {
			t.Errorf("%s absent: %v, want ErrNotFound", k, err)
		}
		if err := del(broken, nil).Delete(ctx, r); err == nil || errors.Is(err, fp.ErrNotFound) {
			t.Errorf("%s failure: %v", k, err)
		}
	}
	for _, k := range []string{TypeTable + " lagotto-watches", TypeBucket + " " + deploy.DefaultBucketName(acct, "us-east-1")} {
		if err := del(untagged, nil).Delete(ctx, res[k]); err == nil || !strings.Contains(err.Error(), "lagotto:managed") {
			t.Errorf("%s without lagotto's tag: %v", k, err)
		}
	}
	// A bucket that is not this account's artifact bucket for its region.
	b := res[TypeBucket+" "+deploy.DefaultBucketName(acct, "us-east-1")]
	b.Region = "eu-west-1"
	if err := del(scriptedRegion{bucket: deploy.BucketDeleted}, nil).Delete(ctx, b); err == nil {
		t.Error("deleted a bucket named for another region")
	}
	s := res[TypeSchedule+" "+deploy.PollerScheduleName]
	s.ARN = "arn:aws:scheduler:us-east-1:" + acct + ":schedule/lagotto/something-else"
	if err := del(scriptedRegion{found: true}, nil).Delete(ctx, s); err == nil {
		t.Error("deleted a schedule whose ARN names another")
	}

	for _, k := range []string{TypeRole + " " + runtimeiam.RoleName, TypeInstanceProfile + " " + runtimeiam.InstanceRoleName} {
		if err := del(nil, scriptedAccount{}).Delete(ctx, res[k]); !errors.Is(err, fp.ErrNotFound) {
			t.Errorf("%s absent: %v", k, err)
		}
		if err := del(nil, scriptedAccount{err: errors.New("AccessDenied")}).Delete(ctx, res[k]); err == nil {
			t.Errorf("%s failure swallowed", k)
		}
	}
	// Another profile not deleted was there at the re-check: it lacks setup's tag.
	if err := del(nil, scriptedAccount{}).Delete(ctx, res[TypeInstanceProfile+" team-gpu"]); err == nil || errors.Is(err, fp.ErrNotFound) {
		t.Errorf("authorized profile left in place: %v", err)
	}
	pol := res[TypePolicy+" "+cliiam.PolicyName]
	for _, c := range []struct {
		d    cliiam.Deletion
		err  error
		want string
	}{{cliiam.Deletion{Absent: true}, nil, "notfound"}, {cliiam.Deletion{Unmanaged: true}, nil, "tag"}, {cliiam.Deletion{}, errors.New("denied"), "denied"}, {cliiam.Deletion{Deleted: true}, nil, ""}} {
		err := del(nil, scriptedAccount{policy: c.d, err: c.err}).Delete(ctx, pol)
		switch c.want {
		case "notfound":
			if !errors.Is(err, fp.ErrNotFound) {
				t.Errorf("absent policy: %v", err)
			}
		case "":
			if err != nil {
				t.Errorf("deleted policy: %v", err)
			}
		default:
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("policy %+v: %v", c.d, err)
			}
		}
	}
}

// TestRecheckMatchesTheDeleter: Recheck is the Deleter's re-check without its
// deleters: the same live resource, not-found and in-use answers.
func TestRecheckMatchesTheDeleter(t *testing.T) {
	live := fp.NewReport(Tool, "123456789012", time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	tagged := map[string]string{tags.ManagedBy: string(Tool)}
	live.Resources = []fp.Resource{
		{ARN: "arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke", Type: TypeRole, Region: Global,
			Name: runtimeiam.SchedulerInvokeRoleName, Ownership: fp.OwnershipTagged, Class: fp.ClassResidue, Tags: tagged},
		{ARN: "arn:aws:dynamodb:us-west-2:123456789012:table/lagotto-watches", Type: TypeTable, Region: "us-west-2",
			Name: "lagotto-watches", Ownership: fp.OwnershipTagged, Class: fp.ClassActive, Tags: tagged},
	}
	d := NewDeleter(live, Deleters{AccountID: "123456789012"})
	gone := fp.Resource{ARN: "arn:aws:scheduler:us-west-2:123456789012:schedule-group/lagotto-teardown", Type: TypeScheduleGroup}
	for _, r := range append(append([]fp.Resource(nil), live.Resources...), gone) {
		got, err := Recheck(live, r)
		want, werr := d.Describe(context.Background(), r)
		if got.ARN != want.ARN || got.Class != want.Class || (err == nil) != (werr == nil) ||
			errors.Is(err, fp.ErrNotFound) != errors.Is(werr, fp.ErrNotFound) || errors.Is(err, ErrInUse) != errors.Is(werr, ErrInUse) {
			t.Errorf("%s: Recheck = %+v, %v; the Deleter's Describe = %+v, %v", r.ARN, got, err, want, werr)
		}
	}
	if _, err := Recheck(live, gone); !errors.Is(err, fp.ErrNotFound) {
		t.Errorf("a resource live does not list: %v, want not found", err)
	}
	if _, err := Recheck(nil, gone); !errors.Is(err, fp.ErrNotFound) {
		t.Errorf("no live footprint: %v, want not found", err)
	}
}
