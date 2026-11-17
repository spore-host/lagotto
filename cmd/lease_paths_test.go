package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// emptyLeaseTable answers the lifecycle table's reads: no item.
type emptyLeaseTable struct{ gets int }

func (e *emptyLeaseTable) GetItem(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	e.gets++
	return &dynamodb.GetItemOutput{}, nil
}

// TestDefaultLifecycleClient: the commands' lease client reaches the table
// with the command's own credentials (real SDK clients, answered here).
func TestDefaultLifecycleClient(t *testing.T) {
	fake := &emptyLeaseTable{}
	c, err := newLifecycleClient(answering(fake), "alice")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Get(context.Background(), inventory.Tool, "us-east-1"); ok || err != nil || fake.gets != 1 {
		t.Errorf("Get = %v, %v after %d reads", ok, err, fake.gets)
	}
}

// TestMarkActiveOwnerAndClientFailure: a caller with no ARN is named
// lagotto-cli in the table, and a client that cannot be built refuses the
// command with nothing created.
func TestMarkActiveOwnerAndClientFailure(t *testing.T) {
	prev := newLifecycleClient
	t.Cleanup(func() { newLifecycleClient = prev })
	var owner string
	newLifecycleClient = func(_ aws.Config, o string) (*lifecycle.Client, error) {
		owner = o
		return nil, errors.New("no DynamoDB client")
	}
	err := markActive(context.Background(), aws.Config{}, "", "us-east-1", "create watch w-1", &bytes.Buffer{})
	if owner != "lagotto-cli" || err == nil || !strings.Contains(err.Error(), "refusing to create watch w-1") ||
		!strings.Contains(err.Error(), "Nothing was created") {
		t.Errorf("owner %q, err %v", owner, err)
	}
}

// TestCreatorWarnsWithoutLeaseAccess: the warning a caller with old IAM gets.
func TestCreatorWarnsWithoutLeaseAccess(t *testing.T) {
	table := lifecycletest.New()
	table.Err = func(string, any) error {
		return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"}
	}
	installFakeLeases(t, table)
	var out bytes.Buffer
	if err := markActive(context.Background(), aws.Config{}, "arn:aws:iam::1:user/a", "us-east-1", "x", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Continuing without the teardown lease") || !strings.Contains(out.String(), "lagotto setup") {
		t.Errorf("warning %q", out.String())
	}
}

// orphanFacts: two regions with orphan tables, nothing active, and the
// unattached CLI policy.
func orphanFacts() inventory.Facts {
	return inventory.Facts{
		AccountID: fpAcct, AllRegions: true,
		Regions: []inventory.Region{
			{Region: "eu-west-1", Tables: fpTables("eu-west-1")},
			{Region: "ap-southeast-2", Tables: fpTables("ap-southeast-2")},
		},
		Account: inventory.Account{Policy: &inventory.Item{ARN: cliiam.PolicyARN(fpAcct), Name: cliiam.PolicyName, Tags: fpTags("global")}},
	}
}

// reapPlan runs teardown --plan --mode reap --yes over facts with table as
// the lifecycle table.
func reapPlan(t *testing.T, facts func() inventory.Facts, table *lifecycletest.Fake) (*recordingDeleters, string, error) {
	t.Helper()
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, table)
	teardownMode, teardownYes = "reap", true
	out, err := runTeardownPlanForTest(t, "")
	return rec, out, err
}

func leaseItem(t *testing.T, table *lifecycletest.Fake, region string) (lifecycle.Item, bool) {
	t.Helper()
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }})
	it, ok, err := c.Get(context.Background(), inventory.Tool, region)
	if err != nil {
		t.Fatal(err)
	}
	return it, ok
}

// TestTeardownPlanReapTwoRegionsAndIAM: each region's tables under its own
// lease, the policy under both, and both leases finished.
func TestTeardownPlanReapTwoRegionsAndIAM(t *testing.T) {
	table := lifecycletest.New()
	rec, out, err := reapPlan(t, orphanFacts, table)
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if len(rec.calls) != 7 {
		t.Errorf("deleted %v, want 6 tables and the policy", rec.calls)
	}
	for _, r := range []string{"eu-west-1", "ap-southeast-2"} {
		if _, ok := leaseItem(t, table, r); ok {
			t.Errorf("%s: the lease was not finished", r)
		}
	}
}

// TestTeardownPlanReapSecondRegionRefused: the second region's lease is held
// by another teardown. Nothing is deleted, and the first region's lease is
// given back rather than left to go stale.
func TestTeardownPlanReapSecondRegionRefused(t *testing.T) {
	table := lifecycletest.New()
	holder, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, Owner: "bob"})
	ctx := context.Background()
	if _, _, err := holder.Observe(ctx, inventory.Tool, "eu-west-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Claim(ctx, inventory.Tool, "eu-west-1", 0); err != nil {
		t.Fatal(err)
	}
	rec, _, err := reapPlan(t, orphanFacts, table)
	if err == nil || !strings.Contains(err.Error(), "could not take the teardown lease in eu-west-1") || len(rec.calls) != 0 {
		t.Fatalf("err %v, deleted %v", err, rec.calls)
	}
	if it, ok := leaseItem(t, table, "ap-southeast-2"); !ok || it.Lease != nil {
		t.Errorf("ap-southeast-2's lease was not given back: %+v", it)
	}
}

// TestTeardownPlanReapActiveRegionRefused: a plan step in a region where
// something is in use cannot take the lease there, so the run is refused.
func TestTeardownPlanReapActiveRegionRefused(t *testing.T) {
	facts := func() inventory.Facts {
		f := fpFacts()
		f.Regions[1].Stack = &inventory.Item{ARN: "arn:aws:cloudformation:us-east-1:" + fpAcct + ":stack/lagotto/1", Name: "lagotto"}
		return f
	}
	rec, _, err := reapPlan(t, facts, lifecycletest.New())
	if err == nil || !strings.Contains(err.Error(), "us-east-1 is in use") || len(rec.calls) != 0 {
		t.Fatalf("err %v, deleted %v", err, rec.calls)
	}
}

// TestTeardownPlanReapAccountOnly: a plan that deletes only account-wide
// resources leases the caller's region, and refuses without one.
func TestTeardownPlanReapAccountOnly(t *testing.T) {
	facts := func() inventory.Facts {
		f := fpFacts()
		f.Regions = f.Regions[1:] // only the active us-east-1
		return f
	}
	table := lifecycletest.New()
	rec, out, err := reapPlan(t, facts, table)
	if err != nil || len(rec.calls) != 1 || rec.calls[0] != "global policy" {
		t.Fatalf("err %v, deleted %v\n%s", err, rec.calls, out)
	}
	if table.Item(lifecycle.TableName, "tool#lagotto#us-west-2") != nil {
		t.Error("the caller's region's lease was not finished")
	}
	if table.Calls("CreateTable") != 1 {
		t.Errorf("the lease was not taken in the caller's region (%d CreateTable)", table.Calls("CreateTable"))
	}

	_, rec, _ = stubPlanTeardown(t, facts)
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, nil
	})
	teardownMode, teardownYes = "reap", true
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "pass --region") || len(rec.calls) != 0 {
		t.Errorf("no region: err %v, deleted %v", err, rec.calls)
	}
}

// TestTeardownPlanReapLeaseFailures: the lease client cannot be built, or
// the table cannot be read: nothing is deleted.
func TestTeardownPlanReapLeaseFailures(t *testing.T) {
	_, rec, _ := stubPlanTeardown(t, orphanFacts)
	prev := newLifecycleClient
	t.Cleanup(func() { newLifecycleClient = prev })
	newLifecycleClient = func(aws.Config, string) (*lifecycle.Client, error) { return nil, errors.New("no client") }
	teardownMode, teardownYes = "reap", true
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "not deleting anything: no client") || len(rec.calls) != 0 {
		t.Errorf("no client: %v, %v", err, rec.calls)
	}

	table := lifecycletest.New()
	table.Err = func(op string, _ any) error {
		if op == "GetItem" {
			return errors.New("throttled")
		}
		return nil
	}
	rec, _, err := reapPlan(t, orphanFacts, table)
	if err == nil || !strings.Contains(err.Error(), "throttled") || len(rec.calls) != 0 {
		t.Errorf("table unreadable: %v, %v", err, rec.calls)
	}
}

// TestTeardownPlanReapAfterwards: a lease that cannot be finished fails the
// run; a lifecycle table that cannot be closed is a warning; a region with
// something left releases its lease, and a failed release fails the run.
func TestTeardownPlanReapAfterwards(t *testing.T) {
	failOn := func(match func(op string, in any) bool) *lifecycletest.Fake {
		table := lifecycletest.New()
		table.Err = func(op string, in any) error {
			if match(op, in) {
				return errors.New("throttled")
			}
			return nil
		}
		return table
	}
	_, out, err := reapPlan(t, orphanFacts, failOn(func(op string, _ any) bool { return op == "DeleteItem" }))
	if err == nil || !strings.Contains(err.Error(), "throttled") {
		t.Errorf("finish failing: %v\n%s", err, out)
	}

	var stderr bytes.Buffer
	_, rec, _ := stubPlanTeardown(t, orphanFacts)
	installFakeLeases(t, failOn(func(op string, _ any) bool { return op == "PutItem" }))
	cfg, _ := awscfg.Load(context.Background(), "")
	planned := inventory.Classify(orphanFacts(), inventory.Options{}, footprintNow())
	plan, _, err := inventory.PlanTeardown(planned, planned, inventory.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reapWithLeases(context.Background(), cfg, plan, planned, inventory.NewDeleter(planned, newTeardownDeleters(cfg, fpAcct)), &stderr); err != nil {
		t.Errorf("a table that cannot be closed failed the run: %v", err)
	}
	if !strings.Contains(stderr.String(), "could not remove the empty spore-host-lifecycle table") || len(rec.calls) != 7 {
		t.Errorf("stderr %q, deleted %v", stderr.String(), rec.calls)
	}

	// Something left in eu-west-1 (a table fails), and the release fails.
	_, rec, _ = stubPlanTeardown(t, orphanFacts)
	rec.failTable = "lagotto-watches"
	isRelease := func(op string, in any) bool {
		u, ok := in.(*dynamodb.UpdateItemInput)
		return op == "UpdateItem" && ok && strings.HasPrefix(aws.ToString(u.UpdateExpression), "SET #state = :active REMOVE #lease")
	}
	installFakeLeases(t, failOn(isRelease))
	teardownMode, teardownYes = "reap", true
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "throttled") {
		t.Errorf("release failing: %v", err)
	}
}

// TestFencedDeleterStopsOnAnyLostLease: an account-wide step is re-checked
// only while every region's lease holds; one lost lease refuses it before
// the re-check reads anything.
func TestFencedDeleterStopsOnAnyLostLease(t *testing.T) {
	table := lifecycletest.New()
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }})
	ctx := context.Background()
	var leases []*lifecycle.Lease
	for _, r := range []string{"eu-west-1", "ap-southeast-2"} {
		l, err := claimRegion(ctx, c, r, false)
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, l)
	}
	inner := &countingDescriber{}
	d := fencedDeleter{Deleter: inner, leases: leases}
	res := fp.Resource{ARN: cliiam.PolicyARN(fpAcct), Type: inventory.TypePolicy}
	if _, err := d.Describe(ctx, res); err != nil || inner.n != 1 {
		t.Fatalf("both leases held: %v (%d reads)", err, inner.n)
	}
	// A creator in ap-southeast-2: the activity removes that lease.
	if _, _, err := c.Observe(ctx, inventory.Tool, "ap-southeast-2", true); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Describe(ctx, res); !errors.Is(err, lifecycle.ErrLeaseLost) || inner.n != 1 {
		t.Errorf("a lost lease: %v (%d reads)", err, inner.n)
	}
}

type countingDescriber struct{ n int }

func (c *countingDescriber) Describe(_ context.Context, r fp.Resource) (fp.Resource, error) {
	c.n++
	return r, nil
}
func (c *countingDescriber) Delete(context.Context, fp.Resource) error { return nil }
