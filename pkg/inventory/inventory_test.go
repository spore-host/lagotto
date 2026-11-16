package inventory

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
)

const acct = "123456789012"

var now = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// lt are the tags lagotto puts on what it creates.
func lt(scope string) map[string]string {
	return map[string]string{tags.ManagedBy: string(Tool), tags.Scope: scope, tags.Created: "2026-11-01T00:00:00Z", tags.Schema: "1"}
}

func with(t map[string]string, k, v string) map[string]string {
	out := copyTags(t)
	out[k] = v
	return out
}

// deployed is a region with a full lagotto deployment and nothing active.
func deployed(region string) Region {
	t := lt(region)
	return Region{
		Region: region,
		Tables: []Table{
			{Item: Item{ARN: TableARN(region, acct, "lagotto-watches"), Name: "lagotto-watches", Tags: t}},
			{Item: Item{ARN: TableARN(region, acct, "lagotto-match-history"), Name: "lagotto-match-history", Tags: t}, Records: 12},
			{Item: Item{ARN: TableARN(region, acct, "lagotto-scheduled-launches"), Name: "lagotto-scheduled-launches", Tags: t}},
		},
		Bucket:       &Item{ARN: BucketARN(deploy.DefaultBucketName(acct, region)), Name: deploy.DefaultBucketName(acct, region), Tags: with(t, "lagotto:managed", "cli")},
		Topic:        &Item{ARN: deploy.AlertsTopicARN(region, acct), Name: deploy.AlertsTopicName, Tags: t},
		Function:     &Item{ARN: deploy.PollerFunctionARN(region, acct), Name: deploy.PollerFunctionName, Tags: t},
		FunctionRole: runtimeiam.RoleARN(acct),
		LogGroup:     &Item{ARN: deploy.PollerLogGroupARN(region, acct), Name: deploy.PollerLogGroupName, Tags: t},
		Groups:       []Item{{ARN: ScheduleGroupARN(region, acct, schedgroup.Name), Name: schedgroup.Name, Tags: t}},
		Schedules:    []Schedule{{Group: schedgroup.Name, Name: deploy.PollerScheduleName, State: "ENABLED", TargetARN: deploy.PollerFunctionARN(region, acct)}},
	}
}

// tablesOnly is a region with lagotto's tables and nothing else: what the
// #749 account had in six of its eight regions.
func tablesOnly(region string) Region {
	r := deployed(region)
	return Region{Region: region, Tables: r.Tables}
}

func account(attachments int32) Account {
	g := lt(managedtagsGlobal)
	return Account{
		Roles: []Item{
			{ARN: runtimeiam.RoleARN(acct), Name: runtimeiam.RoleName, Tags: g},
			{ARN: runtimeiam.SchedulerInvokeRoleARN(acct), Name: runtimeiam.SchedulerInvokeRoleName, Tags: g},
			{ARN: IAMARN(acct, "role", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: g},
		},
		InstanceProfiles: []Profile{{Item: Item{ARN: IAMARN(acct, "instance-profile", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: g},
			Roles: []string{runtimeiam.InstanceRoleName}}},
		AuthorizedRoles:    []Item{{ARN: IAMARN(acct, "role", "team-gpu"), Name: "team-gpu"}},
		AuthorizedProfiles: []Profile{{Item: Item{ARN: IAMARN(acct, "instance-profile", "team-gpu"), Name: "team-gpu", Tags: with(g, "lagotto:managed", "instance-profile")}, Roles: []string{"team-gpu"}}},
		Policy:             &Item{ARN: cliiam.PolicyARN(acct), Name: cliiam.PolicyName, Tags: with(g, "lagotto:managed", "cli")},
		PolicyAttachments:  attachments,
	}
}

const managedtagsGlobal = "global"

func find(t *testing.T, rep *fp.Report, typ, region, name string) fp.Resource {
	t.Helper()
	for _, r := range rep.Resources {
		if r.Type == typ && r.Region == region && r.Name == name {
			return r
		}
	}
	t.Fatalf("no %s %s in %s in the report", typ, name, region)
	return fp.Resource{}
}

func wantClass(t *testing.T, rep *fp.Report, typ, region, name string, want fp.Class) fp.Resource {
	t.Helper()
	r := find(t, rep, typ, region, name)
	if r.Class != want {
		t.Errorf("%s %s (%s): class %s, want %s", typ, name, region, r.Class, want)
	}
	return r
}

// TestClassifyActiveRegion: a region with an active watch is in use
// throughout, and so is what its schedules and poller depend on.
func TestClassifyActiveRegion(t *testing.T) {
	r := deployed("us-east-1")
	r.ActiveWatches = 2
	f := Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}, Account: account(1)}
	rep := Classify(f, Options{}, now)

	for _, name := range []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"} {
		wantClass(t, rep, TypeTable, "us-east-1", name, fp.ClassActive)
	}
	for _, c := range []struct{ typ, name string }{
		{TypeFunction, deploy.PollerFunctionName}, {TypeTopic, deploy.AlertsTopicName},
		{TypeLogGroup, deploy.PollerLogGroupName}, {TypeBucket, deploy.DefaultBucketName(acct, "us-east-1")},
		{TypeSchedule, deploy.PollerScheduleName}, {TypeScheduleGroup, schedgroup.Name},
	} {
		wantClass(t, rep, c.typ, "us-east-1", c.name, fp.ClassActive)
	}
	wantClass(t, rep, TypeRole, Global, runtimeiam.RoleName, fp.ClassActive)
	wantClass(t, rep, TypeRole, Global, runtimeiam.InstanceRoleName, fp.ClassActive)
	wantClass(t, rep, TypeRole, Global, runtimeiam.SchedulerInvokeRoleName, fp.ClassResidue)
}

// TestClassifyPendingLaunchSchedules: a per-launch schedule a record names is
// active (a pending launch), and so is its group; one no record names is
// litter (doctor's orphan, #260), and so is a retry of an unrecorded one.
func TestClassifyPendingLaunchSchedules(t *testing.T) {
	r := deployed("us-east-1")
	r.Function, r.LogGroup, r.Schedules = nil, nil, []Schedule{
		{Group: schedgroup.Name, Name: "lagotto-launch-sl-aaaa"},
		{Group: schedgroup.Name, Name: "lagotto-launch-sl-aaaa-r2"},
		{Group: schedgroup.Name, Name: "lagotto-launch-sl-bbbb"},
		{Group: schedgroup.Default, Name: "lagotto-launch-sl-cccc"},
	}
	r.RecordedSchedules = []string{"lagotto-launch-sl-aaaa"}
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}}, Options{}, now)

	wantClass(t, rep, TypeSchedule, "us-east-1", "lagotto-launch-sl-aaaa", fp.ClassActive)
	wantClass(t, rep, TypeSchedule, "us-east-1", "lagotto-launch-sl-aaaa-r2", fp.ClassActive)
	orphan := wantClass(t, rep, TypeSchedule, "us-east-1", "lagotto-launch-sl-bbbb", fp.ClassLitter)
	if !orphan.Deletable || orphan.Ownership != fp.OwnershipTagged {
		t.Errorf("orphan schedule in lagotto's group: deletable %v (%s), ownership %s; want a deletable tagged one", orphan.Deletable, orphan.Reason, orphan.Ownership)
	}
	legacy := wantClass(t, rep, TypeSchedule, "us-east-1", "lagotto-launch-sl-cccc", fp.ClassLitter)
	if legacy.Ownership != fp.OwnershipLegacyName || legacy.Deletable {
		t.Errorf("schedule in the default group: ownership %s, deletable %v; want legacy-name, never deleted", legacy.Ownership, legacy.Deletable)
	}
	wantClass(t, rep, TypeScheduleGroup, "us-east-1", schedgroup.Name, fp.ClassActive)
}

// TestClassifyIdleAccount: with every region read and nothing active,
// everything is removable: the invoke role and an orphaned log group are
// residue, the rest litter, and every tagged resource is deletable.
func TestClassifyIdleAccount(t *testing.T) {
	east := deployed("us-east-1")
	west := tablesOnly("us-west-2")
	west.LogGroup = &Item{ARN: deploy.PollerLogGroupARN("us-west-2", acct), Name: deploy.PollerLogGroupName, Tags: lt("us-west-2")}
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{east, west}, Account: account(0)}, Options{}, now)

	wantClass(t, rep, TypeLogGroup, "us-west-2", deploy.PollerLogGroupName, fp.ClassResidue)
	wantClass(t, rep, TypeLogGroup, "us-east-1", deploy.PollerLogGroupName, fp.ClassLitter)
	wantClass(t, rep, TypeRole, Global, runtimeiam.SchedulerInvokeRoleName, fp.ClassResidue)
	wantClass(t, rep, TypeRole, Global, runtimeiam.RoleName, fp.ClassLitter)
	for _, r := range rep.Resources {
		if r.Class == fp.ClassActive {
			t.Errorf("%s %s (%s) is active in an idle account", r.Type, r.Name, r.Region)
		}
		if r.Ownership == fp.OwnershipTagged && !r.Deletable {
			t.Errorf("%s %s (%s) is tagged but not deletable: %s", r.Type, r.Name, r.Region, r.Reason)
		}
	}
	if len(rep.Warnings) != 0 {
		t.Errorf("warnings %v", rep.Warnings)
	}
}

// TestClassifyCLIUserPolicy: lagotto-cli-user is litter only when nothing is
// attached to it (an owner decision, spore-host/spore-host#752).
func TestClassifyCLIUserPolicy(t *testing.T) {
	for _, c := range []struct {
		attachments int32
		want        fp.Class
	}{{0, fp.ClassLitter}, {1, fp.ClassActive}, {7, fp.ClassActive}} {
		rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{tablesOnly("us-east-1")}, Account: account(c.attachments)}, Options{}, now)
		wantClass(t, rep, TypePolicy, Global, cliiam.PolicyName, c.want)
	}
}

// TestClassifyOrphanTablesAllRegions is the real-account case (#749): tables
// in a region with no poller are litter even while another region is active,
// and the active region's are not.
func TestClassifyOrphanTablesAllRegions(t *testing.T) {
	east := deployed("us-east-1")
	east.ArmedLaunches = 1
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{east, tablesOnly("eu-west-1"), tablesOnly("ap-south-1")}, Account: account(2)}, Options{}, now)
	for _, region := range []string{"eu-west-1", "ap-south-1"} {
		for _, name := range []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"} {
			r := wantClass(t, rep, TypeTable, region, name, fp.ClassLitter)
			if !r.Deletable {
				t.Errorf("orphan table %s in %s not deletable: %s", name, region, r.Reason)
			}
		}
	}
	wantClass(t, rep, TypeTable, "us-east-1", "lagotto-watches", fp.ClassActive)
}

// TestClassifyOneRegionGlobalsActive: from one region, the account-wide
// resources cannot be classified, so they are active, with a warning.
func TestClassifyOneRegionGlobalsActive(t *testing.T) {
	rep := Classify(Facts{AccountID: acct, Regions: []Region{tablesOnly("us-east-1")}, Account: account(0)}, Options{}, now)
	for _, name := range runtimeiam.LagottoRoles {
		wantClass(t, rep, TypeRole, Global, name, fp.ClassActive)
	}
	wantClass(t, rep, TypeInstanceProfile, Global, runtimeiam.InstanceRoleName, fp.ClassActive)
	if len(rep.Warnings) != 1 || !strings.Contains(rep.Warnings[0], "--all-regions") {
		t.Errorf("warnings %v, want one naming --all-regions", rep.Warnings)
	}
}

// TestClassifyUnknownActivity: a region whose activity could not be read is
// treated as active.
func TestClassifyUnknownActivity(t *testing.T) {
	r := tablesOnly("us-east-1")
	r.ActivityUnknown = true
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}}, Options{}, now)
	wantClass(t, rep, TypeTable, "us-east-1", "lagotto-watches", fp.ClassActive)
}

// TestClassifyOwnershipAndGuard: an untagged table is legacy-name, an
// authorized role foreign, and neither is deletable; nor is anything tagged
// role=service (spore.host's own live service) or permanent.
func TestClassifyOwnershipAndGuard(t *testing.T) {
	r := tablesOnly("us-east-1")
	r.Tables[0].Tags = map[string]string{"lagotto:managed": "cli"}
	r.Tables[1].Tags = with(r.Tables[1].Tags, tags.Role, tags.RoleService)
	r.Tables[2].Tags = with(r.Tables[2].Tags, tags.Permanent, tags.PermanentTrue)
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}, Account: account(0)}, Options{}, now)

	legacy := find(t, rep, TypeTable, "us-east-1", "lagotto-watches")
	if legacy.Ownership != fp.OwnershipLegacyName || legacy.Deletable {
		t.Errorf("untagged table: ownership %s, deletable %v", legacy.Ownership, legacy.Deletable)
	}
	svc := find(t, rep, TypeTable, "us-east-1", "lagotto-match-history")
	if svc.Deletable || !strings.Contains(svc.Reason, "role") {
		t.Errorf("role=service table: deletable %v (%s)", svc.Deletable, svc.Reason)
	}
	perm := wantClass(t, rep, TypeTable, "us-east-1", "lagotto-scheduled-launches", fp.ClassPermanent)
	if perm.Deletable {
		t.Error("permanent table is deletable")
	}
	foreign := find(t, rep, TypeRole, Global, "team-gpu")
	if foreign.Ownership != fp.OwnershipForeign || foreign.Deletable {
		t.Errorf("authorized role: ownership %s, deletable %v", foreign.Ownership, foreign.Deletable)
	}
	if p := find(t, rep, TypeInstanceProfile, Global, "team-gpu"); !p.Deletable {
		t.Errorf("setup's profile for an authorized role is not deletable: %s", p.Reason)
	}
}

// TestPlanOrder: the plan deletes dependents first: profiles before their
// role, a schedule before its group, the invoke role and the poller, the
// poller before its role, topic and tables, and every log group last.
func TestPlanOrder(t *testing.T) {
	west := tablesOnly("us-west-2")
	west.LogGroup = &Item{ARN: deploy.PollerLogGroupARN("us-west-2", acct), Name: deploy.PollerLogGroupName, Tags: lt("us-west-2")}
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{deployed("us-east-1"), west}, Account: account(0)}, Options{}, now)
	plan, err := fp.NewPlan(Tool, Patterns(Options{}), rep.Resources)
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, s := range plan.Steps {
		pos[s.Resource.ARN] = i
	}
	before := func(a, b string) {
		t.Helper()
		pa, oka := pos[a]
		pb, okb := pos[b]
		if !oka || !okb {
			t.Fatalf("missing from the plan: %s (%v) or %s (%v)", a, oka, b, okb)
		}
		if pa >= pb {
			t.Errorf("%s (step %d) is not before %s (step %d)", a, pa, b, pb)
		}
	}
	fn := deploy.PollerFunctionARN("us-east-1", acct)
	sched := ScheduleARN("us-east-1", acct, schedgroup.Name, deploy.PollerScheduleName)
	before(IAMARN(acct, "instance-profile", runtimeiam.InstanceRoleName), IAMARN(acct, "role", runtimeiam.InstanceRoleName))
	before(IAMARN(acct, "instance-profile", "team-gpu"), IAMARN(acct, "role", "team-gpu"))
	before(sched, ScheduleGroupARN("us-east-1", acct, schedgroup.Name))
	before(sched, runtimeiam.SchedulerInvokeRoleARN(acct))
	before(sched, fn)
	before(fn, runtimeiam.RoleARN(acct))
	before(fn, deploy.AlertsTopicARN("us-east-1", acct))
	before(fn, TableARN("us-east-1", acct, "lagotto-scheduled-launches"))
	// Both log groups come after every other step.
	n := len(plan.Steps)
	for _, lg := range []string{deploy.PollerLogGroupARN("us-east-1", acct), deploy.PollerLogGroupARN("us-west-2", acct)} {
		if pos[lg] < n-2 {
			t.Errorf("log group %s is step %d of %d, want one of the last two", lg, pos[lg], n)
		}
	}
}

// TestReportJSONRoundTrip: the report is the shared schema and survives
// footprint.Decode unchanged.
func TestReportJSONRoundTrip(t *testing.T) {
	r := deployed("us-east-1")
	r.ActiveWatches = 1
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r, tablesOnly("us-west-2")}, Account: account(0),
		Warnings: []string{"eu-west-1: could not read the schedules: denied"}}, Options{}, now)
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fp.Decode(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Decode: %v\n%s", err, b)
	}
	b2, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, b2) {
		t.Errorf("round trip changed the report:\n%s\n%s", b, b2)
	}
	if got.Schema != fp.SchemaVersion || got.Tool != Tool || got.Account != acct || !got.GeneratedAt.Equal(now) || len(got.Resources) != len(rep.Resources) {
		t.Errorf("header %+v", got)
	}
	var raw map[string]any
	_ = json.Unmarshal(b, &raw)
	for _, k := range []string{"schema", "tool", "account", "generated_at", "resources", "warnings"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("JSON lacks %q", k)
		}
	}
}

func TestLaunchScheduleRecorded(t *testing.T) {
	rec := []string{"lagotto-launch-sl-0a1b", ""}
	for name, want := range map[string]bool{
		"lagotto-launch-sl-0a1b":     true,
		"lagotto-launch-sl-0a1b-r1":  true,
		"lagotto-launch-sl-0a1b-r12": true,
		"lagotto-launch-sl-0a1b-rx":  false,
		"lagotto-launch-sl-0a1b-r":   false,
		"lagotto-launch-sl-0a1b9":    false,
		"lagotto-launch-sl-ffff":     false,
	} {
		if got := LaunchScheduleRecorded(name, rec); got != want {
			t.Errorf("LaunchScheduleRecorded(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestClassifyRestOfRegion: a teardown job's schedule is active and keeps
// its group; the schedule of a poller that is gone, and an unknown lagotto-*
// schedule, are litter and depend on their target; running jobs are active;
// the legacy stack is reported, never deleted.
func TestClassifyRestOfRegion(t *testing.T) {
	reg := "us-east-1"
	r := Region{
		Region: reg,
		Groups: []Item{{ARN: ScheduleGroupARN(reg, acct, runtimeiam.TeardownScheduleGroup), Name: runtimeiam.TeardownScheduleGroup, Tags: lt(reg)}},
		Schedules: []Schedule{
			{Group: runtimeiam.TeardownScheduleGroup, Name: "lagotto-teardown-job"},
			{Group: schedgroup.Name, Name: deploy.PollerScheduleName},
			{Group: schedgroup.Name, Name: "lagotto-something", TargetARN: "arn:aws:lambda:us-east-1:" + acct + ":function:other"},
		},
		Jobs:  []Item{{ARN: "arn:job", Name: "lagotto-job", Tags: map[string]string{"lagotto:managed": "true"}}},
		Stack: &Item{ARN: "arn:stack", Name: "lagotto"},
	}
	rep := Classify(Facts{AccountID: acct, AllRegions: true, Regions: []Region{r}}, Options{}, now)
	wantClass(t, rep, TypeSchedule, reg, "lagotto-teardown-job", fp.ClassActive)
	wantClass(t, rep, TypeScheduleGroup, reg, runtimeiam.TeardownScheduleGroup, fp.ClassActive)
	wantClass(t, rep, TypeSchedule, reg, deploy.PollerScheduleName, fp.ClassLitter)
	other := wantClass(t, rep, TypeSchedule, reg, "lagotto-something", fp.ClassLitter)
	if !strings.Contains(strings.Join(other.DependsOn, " "), "function:other") {
		t.Errorf("schedule does not depend on its target: %v", other.DependsOn)
	}
	if j := wantClass(t, rep, TypeTrainingJob, reg, "lagotto-job", fp.ClassActive); j.Deletable {
		t.Error("a training job is deletable")
	}
	if st := find(t, rep, TypeStack, reg, "lagotto"); st.Ownership != fp.OwnershipLegacyName || st.Deletable {
		t.Errorf("legacy stack: %s, deletable %v", st.Ownership, st.Deletable)
	}
}
