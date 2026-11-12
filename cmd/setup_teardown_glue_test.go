package cmd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/spf13/cobra"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// ---- pollerRegions: the teardown --iam-roles region scan, with fake EC2/Lambda.

type fakeRegions struct {
	names []string
	err   error
}

func (f fakeRegions) DescribeRegions(context.Context, *ec2.DescribeRegionsInput, ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := &ec2.DescribeRegionsOutput{}
	for _, n := range f.names {
		out.Regions = append(out.Regions, ec2types.Region{RegionName: aws.String(n)})
	}
	return out, nil
}

type fakeFunction struct{ err error }

func (f fakeFunction) GetFunction(_ context.Context, in *lambda.GetFunctionInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	if aws.ToString(in.FunctionName) != "lagotto-capacity-poller" {
		return nil, errors.New("asked for the wrong function " + aws.ToString(in.FunctionName))
	}
	return nil, f.err
}

func lambdaByRegion(state map[string]error) func(string) functionGetter {
	return func(region string) functionGetter { return fakeFunction{err: state[region]} }
}

var notFound = &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}

func TestPollerRegions(t *testing.T) {
	ctx := context.Background()
	regions := fakeRegions{names: []string{"us-west-2", "eu-west-1", "us-east-1"}}

	got, err := pollerRegions(ctx, regions, lambdaByRegion(map[string]error{"eu-west-1": notFound}))
	if err != nil || !reflect.DeepEqual(got, []string{"us-east-1", "us-west-2"}) {
		t.Errorf("pollers in two regions: got %v, %v; want [us-east-1 us-west-2] sorted", got, err)
	}

	got, err = pollerRegions(ctx, regions, lambdaByRegion(map[string]error{"us-west-2": notFound, "eu-west-1": notFound, "us-east-1": notFound}))
	if err != nil || len(got) != 0 {
		t.Errorf("no poller anywhere: got %v, %v", got, err)
	}

	got, err = pollerRegions(ctx, regions, lambdaByRegion(map[string]error{
		"us-west-2": notFound, "us-east-1": notFound, "eu-west-1": errors.New("AccessDeniedException"),
	}))
	if err == nil || !strings.Contains(err.Error(), "eu-west-1 (AccessDeniedException)") {
		t.Errorf("an unchecked region must be an error naming it: got %v, %v", got, err)
	}

	if _, err := pollerRegions(ctx, fakeRegions{err: errors.New("UnauthorizedOperation")}, lambdaByRegion(nil)); err == nil ||
		!strings.Contains(err.Error(), "UnauthorizedOperation") {
		t.Errorf("a DescribeRegions failure must fail closed: %v", err)
	}
	if _, err := pollerRegions(ctx, fakeRegions{}, lambdaByRegion(nil)); err == nil {
		t.Error("no regions returned was taken as no poller anywhere")
	}
}

// ---- applySetupRoles: setup's IAM half, with a fake IAM.

type setupIAM struct {
	roles    map[string]bool
	inline   map[string]string
	created  []string
	putErr   error
	readErr  error
	attached int
}

func (f *setupIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if f.roles[aws.ToString(in.RoleName)] {
		return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName}}, nil
	}
	return nil, &iamtypes.NoSuchEntityException{}
}

func (f *setupIAM) GetRolePolicy(_ context.Context, in *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	if f.readErr != nil {
		return nil, f.readErr
	}
	doc, ok := f.inline[aws.ToString(in.RoleName)+"/"+aws.ToString(in.PolicyName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(doc)}, nil
}

func (f *setupIAM) CreateRole(_ context.Context, in *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	f.roles[aws.ToString(in.RoleName)] = true
	f.created = append(f.created, aws.ToString(in.RoleName))
	return &iam.CreateRoleOutput{}, nil
}

func (f *setupIAM) AttachRolePolicy(context.Context, *iam.AttachRolePolicyInput, ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	f.attached++
	return &iam.AttachRolePolicyOutput{}, nil
}

func (f *setupIAM) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.inline[aws.ToString(in.RoleName)+"/"+aws.ToString(in.PolicyName)] = aws.ToString(in.PolicyDocument)
	return &iam.PutRolePolicyOutput{}, nil
}

// fakeProfiles records the instance profiles setup provisions and deletes.
type fakeProfiles struct {
	ensured, deleted []string
	err              error
}

func (f *fakeProfiles) EnsureInstanceProfile(_ context.Context, role string) error {
	f.ensured = append(f.ensured, role)
	return f.err
}

func (f *fakeProfiles) DeleteManagedProfile(_ context.Context, role string) (bool, error) {
	f.deleted = append(f.deleted, role)
	return true, nil
}

func newSetupIAM() *setupIAM {
	return &setupIAM{roles: map[string]bool{}, inline: map[string]string{}}
}

// TestApplySetupRoles_FreshAccount (#229): setup creates both roles, says so,
// and writes both policies.
func TestApplySetupRoles_FreshAccount(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	f := newSetupIAM()
	var out bytes.Buffer
	if err := applySetupRoles(context.Background(), f, &fakeProfiles{}, "us-west-2", "123456789012", &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Created IAM role " + runtimeiam.RoleName, "Created IAM role " + runtimeiam.SchedulerInvokeRoleName,
		"Applied runtime IAM policy", "Applied invoke policy",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if f.inline[runtimeiam.SchedulerInvokeRoleName+"/"+runtimeiam.SchedulerInvokePolicyName] == "" {
		t.Error("the invoke policy was not written")
	}
}

// TestApplySetupRoles_RerunCarriesRolesForwardAndCreatesNothing.
func TestApplySetupRoles_RerunCarriesRolesForwardAndCreatesNothing(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	f := newSetupIAM()
	f.roles[runtimeiam.RoleName], f.roles[runtimeiam.SchedulerInvokeRoleName] = true, true
	f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName] = irAuthorizedPolicy(t, "loop-node-role")
	f.roles["loop-node-role"] = true
	var out bytes.Buffer
	if err := applySetupRoles(context.Background(), f, &fakeProfiles{}, "us-east-1", "123456789012", &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Created IAM role") || len(f.created) != 0 {
		t.Errorf("a re-run created roles: %v\n%s", f.created, out.String())
	}
	if !strings.Contains(out.String(), "Authorized instance role(s) for hosted spawns: loop-node-role") ||
		!strings.Contains(f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName], ":role/loop-node-role") {
		t.Errorf("the authorized instance role was not carried forward:\n%s", out.String())
	}
}

func TestApplySetupRoles_Errors(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	f := newSetupIAM()
	f.readErr = errors.New("AccessDenied: iam:GetRolePolicy")
	f.roles[runtimeiam.RoleName] = true
	if err := applySetupRoles(context.Background(), f, &fakeProfiles{}, "us-west-2", "123456789012", &bytes.Buffer{}); err == nil ||
		!strings.Contains(err.Error(), "preserve authorized instance roles") {
		t.Errorf("an unreadable deployed policy must stop setup: %v", err)
	}

	f = newSetupIAM()
	f.putErr = errors.New("MalformedPolicyDocument")
	var out bytes.Buffer
	err := applySetupRoles(context.Background(), f, &fakeProfiles{}, "us-west-2", "123456789012", &out)
	if err == nil || !strings.Contains(err.Error(), "MalformedPolicyDocument") {
		t.Errorf("a PutRolePolicy failure must fail setup: %v", err)
	}
	if !strings.Contains(out.String(), "Created IAM role "+runtimeiam.RoleName) {
		t.Errorf("a role created before the failure is not reported:\n%s", out.String())
	}
}

// ---- runTeardown with --iam-roles, end to end through the seams.

type fakeTables struct {
	empty   bool
	deleted bool
	// nonEmpty, when set, is what NonEmptyTables reports (else derived from empty).
	nonEmpty []string
	// result, when set, is what DeleteTables returns.
	result *watcher.TableTeardown
	// scheduleNames / namesErr are what ScheduledLaunchScheduleNames returns.
	scheduleNames []string
	namesErr      error
	// log, when set, records the order of reads and deletions (#260).
	log *[]string
}

func (f *fakeTables) ScheduledLaunchScheduleNames(context.Context) ([]string, error) {
	if f.log != nil {
		*f.log = append(*f.log, "read schedule names")
	}
	return f.scheduleNames, f.namesErr
}

// fakeLaunchSchedules stands in for the scheduler teardown deletes from.
type fakeLaunchSchedules struct {
	recorded []string
	called   bool
	result   deploy.LaunchScheduleTeardown
	err      error
	log      *[]string
}

func (f *fakeLaunchSchedules) DeleteLaunchSchedules(_ context.Context, recorded []string) (deploy.LaunchScheduleTeardown, error) {
	f.called, f.recorded = true, recorded
	if f.log != nil {
		*f.log = append(*f.log, "delete schedules")
	}
	return f.result, f.err
}

func (f *fakeTables) NonEmptyTables(context.Context) ([]string, error) {
	if f.nonEmpty != nil {
		return f.nonEmpty, nil
	}
	if f.empty {
		return nil, nil
	}
	return []string{"lagotto-watches"}, nil
}

func (f *fakeTables) DeleteTables(context.Context) (watcher.TableTeardown, error) {
	f.deleted = true
	if f.log != nil {
		*f.log = append(*f.log, "delete tables")
	}
	if f.result != nil {
		return *f.result, nil
	}
	return watcher.TableTeardown{Deleted: []string{"lagotto-watches", "lagotto-match-history", "lagotto-scheduled-launches"}}, nil
}

func stubTeardown(t *testing.T, tables *fakeTables, roles runtimeiam.RoleDeleteAPI, iamRoles, force bool) {
	t.Helper()
	stubLaunchSchedules(t, &fakeLaunchSchedules{})
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-west-2"}, nil
	})
	oldStore, oldDeleter, oldRoles, oldForce := newTeardownStore, newRoleDeleter, teardownIAMRoles, teardownForce
	oldInstanceRoles := newInstanceRoleTeardown
	newTeardownStore = func(aws.Config) teardownTables { return tables }
	newRoleDeleter = func(aws.Config) runtimeiam.RoleDeleteAPI { return roles }
	newInstanceRoleTeardown = func(aws.Config) instanceRoleTeardown { return &fakeInstanceRoleTeardown{} }
	teardownIAMRoles, teardownForce = iamRoles, force
	t.Cleanup(func() {
		newTeardownStore, newRoleDeleter, teardownIAMRoles, teardownForce = oldStore, oldDeleter, oldRoles, oldForce
		newInstanceRoleTeardown = oldInstanceRoles
	})
	stubPolicyDeleter(t, &fakePolicyDeleter{})
}

func stubLaunchSchedules(t *testing.T, f *fakeLaunchSchedules) {
	t.Helper()
	old := newLaunchScheduleDeleter
	newLaunchScheduleDeleter = func(aws.Config) launchScheduleDeleter { return f }
	t.Cleanup(func() { newLaunchScheduleDeleter = old })
}

func runTeardownForTest(t *testing.T) (string, error) {
	t.Helper()
	var out bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&out)
	err := runTeardown(c, nil)
	return out.String(), err
}

func TestRunTeardown_IAMRolesRefusalTouchesNothing(t *testing.T) {
	tables := &fakeTables{empty: true}
	roles := &cmdRoleDeleter{exists: map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true}}
	stubTeardown(t, tables, roles, true, false)
	stubPollerRegions(t, []string{"us-west-2"}, nil)

	if _, err := runTeardownForTest(t); err == nil || !strings.Contains(err.Error(), "lagotto deploy --teardown --region us-west-2") {
		t.Fatalf("err = %v, want the refusal naming the region", err)
	}
	if tables.deleted || len(roles.exists) != 2 {
		t.Errorf("a refused teardown deleted something: tables=%v roles=%v", tables.deleted, roles.exists)
	}
}

func TestRunTeardown_IAMRolesDeletesTablesAndRoles(t *testing.T) {
	tables := &fakeTables{empty: true}
	roles := &cmdRoleDeleter{exists: map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true}}
	stubTeardown(t, tables, roles, true, false)
	stubPollerRegions(t, nil, nil)

	out, err := runTeardownForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	if !tables.deleted || len(roles.exists) != 0 {
		t.Errorf("tables deleted=%v, roles left=%v", tables.deleted, roles.exists)
	}
	for _, want := range []string{"Deleted table lagotto-watches", "Deleted IAM role " + runtimeiam.RoleName, "Teardown complete."} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// Without --iam-roles nothing about the roles runs, and non-empty tables still
// need --force.
func TestRunTeardown_WithoutIAMRoles(t *testing.T) {
	tables := &fakeTables{empty: false}
	roles := &cmdRoleDeleter{exists: map[string]bool{runtimeiam.RoleName: true}}
	stubTeardown(t, tables, roles, false, false)
	stubPollerRegions(t, nil, errors.New("must not be called"))

	if _, err := runTeardownForTest(t); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Errorf("non-empty tables without --force: %v", err)
	}
	stubTeardown(t, tables, roles, false, true)
	if _, err := runTeardownForTest(t); err != nil || !tables.deleted || !roles.exists[runtimeiam.RoleName] {
		t.Errorf("--force deleted tables=%v, roles=%v, err=%v; want the tables only", tables.deleted, roles.exists, err)
	}
}

// The teardown summary names the scheduled-launches table it deleted, lists
// absent tables rather than erroring, and reports an untagged table it left
// alone (#250).
func TestRunTeardown_ReportsScheduledTable(t *testing.T) {
	tables := &fakeTables{empty: true, result: &watcher.TableTeardown{
		Deleted: []string{"lagotto-watches", "lagotto-scheduled-launches"},
		Absent:  []string{"lagotto-match-history"},
	}}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	out, err := runTeardownForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Deleted table lagotto-scheduled-launches",
		"Already absent: lagotto-match-history",
		"Teardown complete.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}

	tables = &fakeTables{empty: true, result: &watcher.TableTeardown{
		Deleted:   []string{"lagotto-watches", "lagotto-match-history"},
		Unmanaged: []string{"lagotto-scheduled-launches"},
	}}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	out, err = runTeardownForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Left table lagotto-scheduled-launches in place") {
		t.Errorf("an untagged scheduled table is not reported:\n%s", out)
	}
}

// A pending scheduled launch blocks teardown without --force, and the refusal
// names the table holding it (#250).
func TestRunTeardown_NonEmptyScheduledTableNeedsForce(t *testing.T) {
	tables := &fakeTables{nonEmpty: []string{"lagotto-scheduled-launches"}}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	_, err := runTeardownForTest(t)
	if err == nil || !strings.Contains(err.Error(), "lagotto-scheduled-launches") || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("err = %v, want a refusal naming the scheduled table and --force", err)
	}
	if tables.deleted {
		t.Error("a refused teardown deleted tables")
	}
}

// The default seams build real clients without touching the network.
func TestTeardownSeamDefaults(t *testing.T) {
	if newRoleDeleter(aws.Config{Region: "us-east-1"}) == nil || newTeardownStore(aws.Config{Region: "us-east-1"}) == nil {
		t.Error("a default seam returned nil")
	}
}

// TestEnsureRuntimePolicy wires the account and region into applySetupRoles.
func TestEnsureRuntimePolicy(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	f := newSetupIAM()
	oldAcct, oldIAM, oldProfiles := setupCallerAccount, newSetupIAMClient, newInstanceProfileSetup
	t.Cleanup(func() { setupCallerAccount, newSetupIAMClient, newInstanceProfileSetup = oldAcct, oldIAM, oldProfiles })
	newInstanceProfileSetup = func(aws.Config) instanceProfileSetup { return &fakeProfiles{} }
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "123456789012", nil }
	newSetupIAMClient = func(aws.Config) runtimeiam.IAMAPI { return f }

	var out bytes.Buffer
	if err := ensureRuntimePolicy(context.Background(), aws.Config{Region: "us-west-2"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName], "123456789012") {
		t.Error("the runtime policy was not written for the caller's account")
	}
	if err := ensureRuntimePolicy(context.Background(), aws.Config{}, &out); err == nil || !strings.Contains(err.Error(), "no region") {
		t.Errorf("no region: %v", err)
	}
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "", errors.New("ExpiredToken") }
	if err := ensureRuntimePolicy(context.Background(), aws.Config{Region: "us-west-2"}, &out); err == nil || !strings.Contains(err.Error(), "ExpiredToken") {
		t.Errorf("an STS failure must fail setup: %v", err)
	}
}

type failingDeleter struct{ *cmdRoleDeleter }

func (failingDeleter) DeleteRole(context.Context, *iam.DeleteRoleInput, ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	return nil, errors.New("AccessDenied")
}

func TestDeleteCLIOwnedRoles_Error(t *testing.T) {
	f := failingDeleter{&cmdRoleDeleter{exists: map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true}}}
	if err := deleteCLIOwnedRoles(context.Background(), f, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("err = %v, want the AccessDenied", err)
	}
}

// TestFindPollerRegionsDefaultWiring: the default seam reaches EC2 through the
// given config; a stubbed HTTP client that refuses proves no real call leaves.
func TestFindPollerRegionsDefaultWiring(t *testing.T) {
	cfg := aws.Config{Region: "us-east-1", HTTPClient: refusingHTTP{}, Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1}
	if _, err := findPollerRegions(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "list the account's regions") {
		t.Errorf("err = %v, want the DescribeRegions failure", err)
	}
}

type refusingHTTP struct{}

func (refusingHTTP) Do(*http.Request) (*http.Response, error) {
	return nil, errors.New("refused by test")
}

// With --force, teardown deletes the per-launch schedules the records name
// (and those found by prefix) BEFORE the tables, since the table is the only
// record of them, and reports them alongside the tables (#260).
func TestRunTeardown_DeletesLaunchSchedulesBeforeTables(t *testing.T) {
	var order []string
	tables := &fakeTables{nonEmpty: []string{"lagotto-scheduled-launches"}, log: &order,
		scheduleNames: []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-fired"}}
	sched := &fakeLaunchSchedules{log: &order, result: deploy.LaunchScheduleTeardown{
		Deleted: []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-orph"},
		Absent:  []string{"lagotto-launch-sl-fired"},
	}}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, true)
	stubLaunchSchedules(t, sched)

	out, err := runTeardownForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"read schedule names", "delete schedules", "delete tables"}; !reflect.DeepEqual(order, want) {
		t.Errorf("order = %v, want %v", order, want)
	}
	if !reflect.DeepEqual(sched.recorded, tables.scheduleNames) {
		t.Errorf("deleter got %v, want the recorded names %v", sched.recorded, tables.scheduleNames)
	}
	for _, want := range []string{
		"Deleted schedule lagotto-launch-sl-aaaa",
		"Deleted schedule lagotto-launch-sl-orph",
		"Schedules already absent: lagotto-launch-sl-fired",
		"Deleted table lagotto-watches",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

// Without --force, a non-empty table refuses before any schedule is touched.
func TestRunTeardown_RefusalTouchesNoSchedule(t *testing.T) {
	tables := &fakeTables{nonEmpty: []string{"lagotto-scheduled-launches"}}
	sched := &fakeLaunchSchedules{}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	stubLaunchSchedules(t, sched)
	if _, err := runTeardownForTest(t); err == nil {
		t.Fatal("non-empty tables without --force did not refuse")
	}
	if sched.called || tables.deleted {
		t.Errorf("a refused teardown deleted schedules=%v tables=%v", sched.called, tables.deleted)
	}
}

// When the schedules cannot be read or deleted, no table is deleted, so the
// record of the schedules survives for a retry.
func TestRunTeardown_ScheduleFailureKeepsTables(t *testing.T) {
	tables := &fakeTables{empty: true, namesErr: errors.New("AccessDeniedException: dynamodb:Scan")}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	if _, err := runTeardownForTest(t); err == nil || !strings.Contains(err.Error(), "no table was deleted") {
		t.Errorf("err = %v, want the record-read failure", err)
	}
	if tables.deleted {
		t.Error("tables deleted after the schedule names could not be read")
	}

	tables = &fakeTables{empty: true}
	sched := &fakeLaunchSchedules{
		result: deploy.LaunchScheduleTeardown{Deleted: []string{"lagotto-launch-sl-aaaa"}},
		err:    errors.New("AccessDeniedException: scheduler:DeleteSchedule"),
	}
	stubTeardown(t, tables, &cmdRoleDeleter{}, false, false)
	stubLaunchSchedules(t, sched)
	out, err := runTeardownForTest(t)
	if err == nil || !strings.Contains(err.Error(), "scheduler:ListSchedules and scheduler:DeleteSchedule") {
		t.Errorf("err = %v, want it to name the scheduler permissions", err)
	}
	if tables.deleted {
		t.Error("tables deleted after a schedule deletion failed")
	}
	if !strings.Contains(out, "Deleted schedule lagotto-launch-sl-aaaa") {
		t.Errorf("a schedule deleted before the failure is not reported:\n%s", out)
	}
}

type fakeInstanceRoleTeardown struct {
	authorized []string
	deleted    []string
}

func (f *fakeInstanceRoleTeardown) AuthorizedRoles(context.Context) ([]string, error) {
	return f.authorized, nil
}

func (f *fakeInstanceRoleTeardown) DeleteInstanceRole(context.Context) (bool, error) {
	f.deleted = append(f.deleted, runtimeiam.InstanceRoleName)
	return true, nil
}

func (f *fakeInstanceRoleTeardown) DeleteManagedProfile(_ context.Context, name string) (bool, error) {
	f.deleted = append(f.deleted, "profile:"+name)
	return true, nil
}

// TestApplySetupRoles_ProvisionsInstanceProfiles: setup provides every
// instance profile the poller may launch with (it creates none itself),
// refuses an authorized role that does not exist, and removes the profile it
// created for a revoked role.
func TestApplySetupRoles_ProvisionsInstanceProfiles(t *testing.T) {
	resetSetupRoleFlags(t, []string{"gpu-role"}, nil)
	f := newSetupIAM()
	f.roles["gpu-role"] = true
	p := &fakeProfiles{}
	if err := applySetupRoles(context.Background(), f, p, "us-west-2", "123456789012", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.ensured, ",") != runtimeiam.InstanceRoleName+",gpu-role" {
		t.Errorf("ensured = %v", p.ensured)
	}

	resetSetupRoleFlags(t, []string{"missing-role"}, nil)
	if err := applySetupRoles(context.Background(), newSetupIAM(), &fakeProfiles{}, "us-west-2", "123456789012", &bytes.Buffer{}); err == nil ||
		!strings.Contains(err.Error(), "does not exist") {
		t.Errorf("a missing authorized role: %v", err)
	}

	resetSetupRoleFlags(t, nil, []string{"gpu-role"})
	f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName] = irAuthorizedPolicy(t, "gpu-role")
	p = &fakeProfiles{}
	if err := applySetupRoles(context.Background(), f, p, "us-west-2", "123456789012", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(p.deleted) != 1 || p.deleted[0] != "gpu-role" {
		t.Errorf("revoke deleted %v, want [gpu-role]", p.deleted)
	}
	if strings.Contains(f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName], ":role/gpu-role") {
		t.Error("a revoked role is still passable")
	}

	resetSetupRoleFlags(t, []string{"spored-instance-role"}, nil)
	if err := applySetupRoles(context.Background(), newSetupIAM(), &fakeProfiles{}, "us-west-2", "123456789012", &bytes.Buffer{}); err == nil {
		t.Error("setup authorized spawn's shared spored-instance-role")
	}
}

func TestApplySetupRoles_SageMakerRoles(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	sm := "arn:aws:iam::123456789012:role/service-role/sm-exec"
	old := setupSageMakerRoles
	setupSageMakerRoles = []string{sm}
	t.Cleanup(func() { setupSageMakerRoles = old })
	f := newSetupIAM()
	if err := applySetupRoles(context.Background(), f, &fakeProfiles{}, "us-west-2", "123456789012", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	doc := f.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName]
	if !strings.Contains(doc, sm) || !strings.Contains(doc, "sagemaker.amazonaws.com") {
		t.Errorf("the SageMaker role is not passable:\n%s", doc)
	}
	setupSageMakerRoles = []string{"arn:aws:iam::999999999999:role/x"}
	if err := applySetupRoles(context.Background(), newSetupIAM(), &fakeProfiles{}, "us-west-2", "123456789012", &bytes.Buffer{}); err == nil {
		t.Error("a role in another account was authorized")
	}
}

func TestDeleteInstanceRoles(t *testing.T) {
	f := &fakeInstanceRoleTeardown{authorized: []string{"gpu-role"}}
	var out bytes.Buffer
	if err := deleteInstanceRoles(context.Background(), f, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.deleted, ",") != "profile:gpu-role,"+runtimeiam.InstanceRoleName {
		t.Errorf("deleted = %v", f.deleted)
	}
}
