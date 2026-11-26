package idle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The global-lease protocol in aws-us-gov (lagotto#347): with lagotto's ARNs
// in the account's partition there is a control region there, so the
// account-wide teardown runs fenced, as in aws. aws-cn has none yet.

// TestNewClientControlRegionByPartition: a client made in any region of a
// partition keeps the global scope in that partition's control region; in
// aws-cn (not yet supported) and an ISO region there is none, and every use
// of it fails, closed.
func TestNewClientControlRegionByPartition(t *testing.T) {
	for region, want := range map[string]string{
		"us-west-2": "us-east-1", "us-gov-east-1": "us-gov-west-1", "us-gov-west-1": "us-gov-west-1",
	} {
		c, err := NewClient(aws.Config{Region: region}, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := c.RegionFor(lifecycle.ScopeGlobal); got != want || err != nil {
			t.Errorf("%s: global scope in %q, %v, want %s", region, got, err, want)
		}
	}
	for _, region := range []string{"cn-north-1", "cn-northwest-1", "us-iso-east-1"} {
		c, err := NewClient(aws.Config{Region: region}, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := c.RegionFor(lifecycle.ScopeGlobal); err == nil {
			t.Errorf("%s has a global scope, in %q", region, got)
		}
	}
}

// TestSelfRemovalInEachPartition: the one-shot jobs call the partition's
// universal targets, on the partition's ARNs, as its invoke role.
func TestSelfRemovalInEachPartition(t *testing.T) {
	for r, p := range map[string]string{"us-gov-west-1": awsarn.GovCloud, "cn-northwest-1": awsarn.China, "us-east-1": awsarn.AWS} {
		jobs := SelfRemoval{Region: r, AccountID: acct, Function: true, LogGroup: true}.Plan(t0)
		if len(jobs) != 2 {
			t.Fatalf("%s: %d jobs", r, len(jobs))
		}
		want := map[string][2]string{
			"lagotto-teardown-log-group": {"arn:" + p + ":scheduler:::aws-sdk:cloudwatchlogs:deleteLogGroup", deploy.PollerLogGroupARN(r, acct)},
			FunctionJobName:              {"arn:" + p + ":scheduler:::aws-sdk:lambda:deleteFunction", deploy.PollerFunctionARN(r, acct)},
		}
		for _, j := range jobs {
			if w := want[j.Name]; j.Target != w[0] || j.Resource != w[1] || !strings.HasPrefix(j.Resource, "arn:"+p+":") {
				t.Errorf("%s %s: target %s on %s, want %v", r, j.Name, j.Target, j.Resource, w)
			}
		}
		f := &fakeScheduler{e: &events{}}
		if _, err := CreateOneShots(context.Background(), f, r, acct, t0, jobs, nil); err != nil {
			t.Fatal(err)
		}
		for _, in := range f.inputs {
			if got := aws.ToString(in.Target.RoleArn); got != runtimeiam.SchedulerInvokeRoleARN(p, acct) {
				t.Errorf("%s: job %s runs as %s", r, aws.ToString(in.Name), got)
			}
			if !strings.HasPrefix(aws.ToString(in.Target.Arn), "arn:"+p+":scheduler:::aws-sdk:") {
				t.Errorf("%s: job target %s", r, aws.ToString(in.Target.Arn))
			}
		}
	}
}

// newPartitionWorld is awWorld in partition p's regions, whose account-wide
// resources carry that partition's ARNs, with the global scope in its
// control region.
func newPartitionWorld(t *testing.T, p string, regions ...string) (*awWorld, string) {
	t.Helper()
	control, err := ControlRegion(regions[0])
	if err != nil {
		t.Fatal(err)
	}
	w := &awWorld{t: t, src: inventorytest.New(regions...), ev: &events{}, now: t0, tables: map[string]*lifecycletest.Fake{}}
	for _, r := range regions {
		w.tables[r] = lifecycletest.New()
	}
	w.src.SetAccount(inventorytest.AccountIn(p, acct))
	w.acct = &awAccount{e: w.ev, fail: map[string]error{}}
	return w, control
}

func (w *awWorld) clientIn(control, owner string) *lifecycle.Client {
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return w.tables[r], nil },
		ControlRegion: control, Owner: owner, Now: w.clock,
		Sleep: func(context.Context, time.Duration) error { w.advance(time.Second); return nil },
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

// TestAccountWideInEachPartition: in GovCloud the account-wide teardown
// takes the global lease in the partition's control region, is blocked by a
// creator's global mark there, by a pending one-shot job and by a poller in
// any of the partition's regions, and otherwise removes every account-wide
// resource, finishes the global item and closes the control region's table.
func TestAccountWideInEachPartition(t *testing.T) {
	ctx := context.Background()
	for p, regions := range map[string][]string{
		awsarn.GovCloud: {"us-gov-east-1", "us-gov-west-1"},
	} {
		w, control := newPartitionWorld(t, p, regions...)
		if control != regions[1] {
			t.Fatalf("%s: control region %s", p, control)
		}
		run := func() AccountWideResult {
			d := w.deps()
			d.Lifecycle = w.clientIn(control, cliOwner)
			return TearDownAccountWide(ctx, w.cfg(true, 0), d)
		}
		other := regions[0]

		// A creator's global mark, in the control region's table.
		creator := w.clientIn(control, "alice")
		token, err := creator.Mark(ctx, Tool, lifecycle.ScopeGlobal)
		if err != nil {
			t.Fatal(err)
		}
		if w.tables[control].Calls("UpdateItem")+w.tables[control].Calls("TransactWriteItems") == 0 {
			t.Fatalf("%s: the global mark is not in %s's table", p, control)
		}
		if r := run(); r.Outcome != AccountWideBlocked || !strings.Contains(r.Why, "a creator is running") {
			t.Errorf("%s: with a creator's mark: %+v", p, r)
		}
		if _, err := creator.MarkDone(ctx, Tool, lifecycle.ScopeGlobal, token); err != nil {
			t.Fatal(err)
		}

		// A pending one-shot job, then a poller, in the other region.
		w.src.SetRegion(inventory.Region{Region: other, Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: FunctionJobName}}})
		if r := run(); r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, "\n"), other+": one-shot job "+FunctionJobName+" is pending") {
			t.Errorf("%s: with a pending job: %+v", p, r)
		}
		w.src.SetRegion(inventorytest.Deployed(other, acct))
		if r := run(); r.Outcome != AccountWideBlocked || !strings.Contains(strings.Join(r.Blockers, "\n"), other+": a poller is deployed") {
			t.Errorf("%s: with a poller: %+v", p, r)
		}
		if len(w.ev.list()) != 0 {
			t.Fatalf("%s: deleted while blocked: %v", p, w.ev.list())
		}

		// Nothing left in the regions: everything account-wide goes.
		w.src.SetRegion(inventory.Region{Region: other})
		if r := run(); r.Outcome != AccountWideRemoved || r.Err != nil {
			t.Fatalf("%s: %+v\n%s", p, r, w.log())
		}
		w.deleted(t, allAccountWide...)
		if it, ok, err := w.clientIn(control, "reader").Get(ctx, Tool, lifecycle.ScopeGlobal); ok || err != nil {
			t.Errorf("%s: the global item is left: %+v %v", p, it, err)
		}
		if n := w.tables[control].Calls("DeleteTable"); n != 1 {
			t.Errorf("%s: the control table was not closed (%d)", p, n)
		}
		if n := w.tables[other].Calls("DeleteTable"); n != 0 {
			t.Errorf("%s: the other region's table was closed", p)
		}
	}
}
