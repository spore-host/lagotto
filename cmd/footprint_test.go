package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const fpAcct = "123456789012"

func fpTags(scope string) map[string]string {
	return map[string]string{tags.ManagedBy: "lagotto", tags.Scope: scope}
}

// fpTables are lagotto's three tables in region.
func fpTables(region string) []inventory.Table {
	var out []inventory.Table
	for _, n := range []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"} {
		out = append(out, inventory.Table{Item: inventory.Item{ARN: inventory.TableARN(region, fpAcct, n), Name: n, Tags: fpTags(region)}})
	}
	return out
}

// fpFacts is an account with an active poller in us-east-1 and orphan tables
// in eu-west-1, read from every region.
func fpFacts() inventory.Facts {
	return inventory.Facts{
		AccountID: fpAcct, AllRegions: true,
		Regions: []inventory.Region{
			{Region: "eu-west-1", Tables: fpTables("eu-west-1")},
			{Region: "us-east-1", Tables: fpTables("us-east-1"), ActiveWatches: 1,
				Function: &inventory.Item{ARN: deploy.PollerFunctionARN("us-east-1", fpAcct), Name: deploy.PollerFunctionName, Tags: fpTags("us-east-1")}},
		},
		Account: inventory.Account{
			Roles:  []inventory.Item{{ARN: runtimeiam.RoleARN(fpAcct), Name: runtimeiam.RoleName, Tags: fpTags("global")}},
			Policy: &inventory.Item{ARN: cliiam.PolicyARN(fpAcct), Name: cliiam.PolicyName, Tags: fpTags("global")},
		},
	}
}

// stubFootprint makes the footprint read facts, and records every scope read.
func stubFootprint(t *testing.T, facts func() inventory.Facts) *[]inventory.Scope {
	t.Helper()
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-west-2"}, nil
	})
	var scopes []inventory.Scope
	oldGather, oldAcct, oldNow, oldFormat, oldAll := gatherFootprint, setupCallerAccount, footprintNow, outputFormat, footprintAllRegions
	gatherFootprint = func(_ context.Context, _ aws.Config, _ string, s inventory.Scope) (inventory.Facts, error) {
		scopes = append(scopes, s)
		return facts(), nil
	}
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return fpAcct, nil }
	footprintNow = func() time.Time { return time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC) }
	t.Cleanup(func() {
		gatherFootprint, setupCallerAccount, footprintNow, outputFormat, footprintAllRegions = oldGather, oldAcct, oldNow, oldFormat, oldAll
	})
	return &scopes
}

func TestFootprintJSON(t *testing.T) {
	scopes := stubFootprint(t, fpFacts)
	outputFormat = "json"
	var out bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&out)
	if err := runFootprint(c, nil); err != nil {
		t.Fatal(err)
	}
	rep, err := fp.Decode(&out)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if rep.Account != fpAcct || len(rep.Resources) == 0 {
		t.Errorf("report %+v", rep)
	}
	if len(*scopes) != 1 || (*scopes)[0].AllRegions || len((*scopes)[0].Regions) != 1 || (*scopes)[0].Regions[0] != "us-west-2" {
		t.Errorf("read %+v, want the configured region only", *scopes)
	}
}

func TestFootprintTable(t *testing.T) {
	stubFootprint(t, func() inventory.Facts {
		f := fpFacts()
		f.Warnings = []string{"ap-south-1: could not read the schedules: denied"}
		return f
	})
	outputFormat, footprintAllRegions = "table", true
	var out bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&out)
	if err := runFootprint(c, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"REGION", "OWNERSHIP", "DynamoDB::Table", "litter", "active", "warning: ap-south-1", "in account " + fpAcct} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("table lacks %q:\n%s", want, out.String())
		}
	}
}

func TestFootprintFlags(t *testing.T) {
	stubFootprint(t, fpFacts)
	c := &cobra.Command{}
	c.Flags().String("region", "", "")
	_ = c.Flags().Set("region", "us-east-1")
	footprintAllRegions = true
	if err := runFootprint(c, nil); err == nil || !strings.Contains(err.Error(), "--all-regions") {
		t.Errorf("--all-regions with --region: %v", err)
	}
	footprintAllRegions, outputFormat = false, "wide"
	if err := runFootprint(&cobra.Command{}, nil); err == nil {
		t.Error("-o wide accepted")
	}
}

// recordingDeleters fakes the deleters a plan runs with.
type recordingDeleters struct {
	mu    sync.Mutex
	calls []string
	// failTable fails DeleteTable for this table.
	failTable string
}

func (r *recordingDeleters) rec(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

type recRegion struct {
	r      *recordingDeleters
	region string
}

func (d recRegion) DeleteTable(_ context.Context, n string) (watcher.TableOutcome, error) {
	if n == d.r.failTable {
		return "", errors.New("AccessDenied: dynamodb:DeleteTable")
	}
	d.r.rec(d.region + " table " + n)
	return watcher.TableDeleted, nil
}
func (d recRegion) DeleteArtifactBucket(context.Context, string, string) (deploy.BucketTeardown, error) {
	d.r.rec(d.region + " bucket")
	return deploy.BucketTeardown{Outcome: deploy.BucketDeleted}, nil
}
func (d recRegion) DeleteAlertsTopic(context.Context, string, string) (bool, error) {
	d.r.rec(d.region + " topic")
	return true, nil
}
func (d recRegion) DeletePollerFunction(context.Context) (bool, error) {
	d.r.rec(d.region + " function")
	return true, nil
}
func (d recRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	d.r.rec(d.region + " loggroup")
	return true, nil
}
func (d recRegion) DeleteSchedule(_ context.Context, g, n string) (bool, error) {
	d.r.rec(d.region + " schedule " + g + "/" + n)
	return true, nil
}
func (d recRegion) DeleteScheduleGroup(_ context.Context, n string) (bool, error) {
	d.r.rec(d.region + " group " + n)
	return true, nil
}

type recAccount struct{ r *recordingDeleters }

func (d recAccount) DeleteRole(_ context.Context, n string) (bool, error) {
	d.r.rec("global role " + n)
	return true, nil
}
func (d recAccount) DeleteInstanceProfile(_ context.Context, n string) (bool, error) {
	d.r.rec("global profile " + n)
	return true, nil
}
func (d recAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	d.r.rec("global policy")
	return cliiam.Deletion{Deleted: true}, nil
}

// stubPlanTeardown writes a plan of facts to a file and stubs the deleters.
func stubPlanTeardown(t *testing.T, facts func() inventory.Facts) (string, *recordingDeleters, *[]inventory.Scope) {
	t.Helper()
	scopes := stubFootprint(t, facts)
	installFakeLeases(t, lifecycletest.New())
	rep := inventory.Classify(facts(), inventory.Options{}, time.Now())
	b, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	rec := &recordingDeleters{}
	oldSource := accountWideSource
	accountWideSource = func(aws.Config, string) inventory.Source { return sourceOf(facts()) }
	t.Cleanup(func() { accountWideSource = oldSource })
	old := newTeardownDeleters
	oldPlan, oldMode, oldYes, oldNoLease := teardownPlan, teardownMode, teardownYes, teardownNoLease
	newTeardownDeleters = func(aws.Config, string) inventory.Deleters {
		return inventory.Deleters{AccountID: fpAcct, Region: func(r string) inventory.RegionDeleter { return recRegion{rec, r} }, Account: recAccount{rec}}
	}
	t.Cleanup(func() {
		newTeardownDeleters = old
		teardownPlan, teardownMode, teardownYes, teardownNoLease = oldPlan, oldMode, oldYes, oldNoLease
	})
	teardownPlan, teardownMode, teardownYes, teardownNoLease, outputFormat = path, "report", false, false, "table"
	return path, rec, scopes
}

func runTeardownPlanForTest(t *testing.T, stdin string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&out)
	c.SetIn(strings.NewReader(stdin))
	err := runTeardown(c, nil)
	return out.String(), err
}

// TestTeardownPlanReportDeletesNothing: report mode, the default, deletes
// nothing, re-checks the plan against a footprint of every region (it names
// account-wide resources), and says what reap would delete.
func TestTeardownPlanReportDeletesNothing(t *testing.T) {
	_, rec, scopes := stubPlanTeardown(t, fpFacts)
	out, err := runTeardownPlanForTest(t, "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(rec.calls) != 0 {
		t.Errorf("report mode deleted %v", rec.calls)
	}
	if len(*scopes) != 1 || !(*scopes)[0].AllRegions {
		t.Errorf("re-checked against %+v, want every region", *scopes)
	}
	for _, want := range []string{"would-delete  eu-west-1", "kept", "report: would delete 4"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// TestTeardownPlanReap: with --yes and the no-lease acknowledgement, reap
// deletes the orphan tables, and nothing of the active region or the role
// its poller runs as; the unattached policy is account-wide, and with a
// poller in us-east-1 the account-wide teardown keeps it (lagotto#340).
func TestTeardownPlanReap(t *testing.T) {
	_, rec, _ := stubPlanTeardown(t, fpFacts)
	teardownMode, teardownYes, teardownNoLease = "reap", true, true
	out, err := runTeardownPlanForTest(t, "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	want := map[string]bool{"eu-west-1 table lagotto-watches": true, "eu-west-1 table lagotto-match-history": true,
		"eu-west-1 table lagotto-scheduled-launches": true}
	for _, c := range rec.calls {
		if !want[c] {
			t.Errorf("deleted %q", c)
		}
		delete(want, c)
	}
	if len(want) != 0 {
		t.Errorf("not deleted: %v", want)
	}
	if !strings.Contains(out, "us-east-1: a poller is deployed") {
		t.Errorf("the kept policy does not say why:\n%s", out)
	}
}

// TestTeardownPlanReapGuards: reap needs --yes; refusals delete nothing.
func TestTeardownPlanReapGuards(t *testing.T) {
	_, rec, scopes := stubPlanTeardown(t, fpFacts)
	teardownMode = "reap"
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "--yes") {
		t.Errorf("reap without --yes: %v", err)
	}
	teardownMode = "delete"
	if _, err := runTeardownPlanForTest(t, ""); err == nil {
		t.Error("--mode delete accepted")
	}
	if len(rec.calls) != 0 || len(*scopes) != 0 {
		t.Errorf("a refusal read %v or deleted %v", *scopes, rec.calls)
	}
}

// TestTeardownPlanStdin reads the plan from stdin with --plan -.
func TestTeardownPlanStdin(t *testing.T) {
	path, rec, _ := stubPlanTeardown(t, fpFacts)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	teardownPlan, outputFormat = "-", "json"
	out, err := runTeardownPlanForTest(t, string(b))
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	var res teardownReport
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if res.Mode != fp.ModeReport || len(res.Steps) == 0 || len(res.Kept) == 0 || len(rec.calls) != 0 {
		t.Errorf("result %+v, calls %v", res, rec.calls)
	}
}

func TestTeardownFlagsOfTheOtherPath(t *testing.T) {
	newCmd := func(set ...string) *cobra.Command {
		c := &cobra.Command{}
		for _, n := range []string{"mode", "yes", noLeaseFlag, "force", "iam-roles"} {
			c.Flags().String(n, "", "")
		}
		for _, n := range set {
			_ = c.Flags().Set(n, "x")
		}
		return c
	}
	old := teardownPlan
	t.Cleanup(func() { teardownPlan = old })
	teardownPlan = ""
	if err := checkTeardownFlags(newCmd("mode")); err == nil {
		t.Error("--mode without --plan accepted")
	}
	if err := checkTeardownFlags(newCmd("force")); err != nil {
		t.Errorf("--force without --plan: %v", err)
	}
	teardownPlan = "plan.json"
	if err := checkTeardownFlags(newCmd("iam-roles")); err == nil {
		t.Error("--iam-roles with --plan accepted")
	}
	if err := checkTeardownFlags(newCmd("mode", "yes")); err != nil {
		t.Errorf("--mode --yes with --plan: %v", err)
	}
}

// TestTeardownPlanReapTakesTheRealLease (lagotto#326): with --yes and no
// acknowledgement, reap claims the teardown lease in the region it deletes in
// (eu-west-1), deletes under it, and, the region being cleared, finishes it
// and closes the lifecycle table. The active region is not leased.
func TestTeardownPlanReapTakesTheRealLease(t *testing.T) {
	_, rec, _ := stubPlanTeardown(t, fpFacts)
	table := lifecycletest.New()
	var steps []string
	table.Hook = func(_ context.Context, op string) {
		if op == "UpdateItem" {
			steps = append(steps, op)
		}
	}
	installFakeLeases(t, table)
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(rec.calls) != 3 {
		t.Errorf("deleted %v, want the three eu-west-1 tables (the policy waits for us-east-1's poller)", rec.calls)
	}
	// Claim, then a heartbeat before each of the three deletes.
	if len(steps) < 4 {
		t.Errorf("%d lease writes, want the claim and a heartbeat per delete", len(steps))
	}
	if n := table.Calls("DeleteTable"); n != 1 {
		t.Errorf("the lifecycle table was deleted %d times, want 1 (cleared region, lease finished)", n)
	}
	if table.Item(lifecycle.TableName, "tool#lagotto#us-east-1") != nil {
		t.Error("the active region was leased")
	}
}

// TestTeardownPlanReapRefusesAHeldLease: another teardown holds the lease in
// eu-west-1. Without the acknowledgement reap refuses before deleting
// anything; with it, it falls back to the stand-in, warns, and deletes.
func TestTeardownPlanReapRefusesAHeldLease(t *testing.T) {
	_, rec, _ := stubPlanTeardown(t, fpFacts)
	table := lifecycletest.New()
	installFakeLeases(t, table)
	other, err := newLifecycleClient(aws.Config{}, "lagotto-poller/eu-west-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := other.Observe(ctx, inventory.Tool, "eu-west-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Claim(ctx, inventory.Tool, "eu-west-1", 0); err != nil {
		t.Fatal(err)
	}
	teardownMode, teardownYes = "reap", true
	_, err = runTeardownPlanForTest(t, "")
	if err == nil || !strings.Contains(err.Error(), "could not take the teardown lease in eu-west-1") ||
		!strings.Contains(err.Error(), "lagotto-poller/eu-west-1") {
		t.Fatalf("err = %v, want the refusal naming the holder", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("a refused reap deleted %v", rec.calls)
	}

	teardownNoLease = true
	out, err := runTeardownPlanForTest(t, "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(rec.calls) != 3 {
		t.Errorf("with the acknowledgement: deleted %v, want the 3 tables (a teardown holds eu-west-1, so the policy stays)", rec.calls)
	}
}

// sourceOf answers the account-wide teardown's reads (idle.TearDownAccountWide)
// from f, as the footprint seam answers the plan's.
func sourceOf(f inventory.Facts) inventory.Source {
	names := make([]string, len(f.Regions))
	for i, r := range f.Regions {
		names[i] = r.Region
	}
	s := inventorytest.New(names...)
	for _, r := range f.Regions {
		s.SetRegion(r)
	}
	var authorized []string
	for _, r := range f.Account.AuthorizedRoles {
		authorized = append(authorized, r.Name)
	}
	return s.SetAccount(f.Account, authorized...)
}
