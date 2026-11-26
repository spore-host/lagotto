package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The sixth review of lagotto#351.

// TestReview6PlanOwnerIsTheRun (G3): `teardown --plan` writes a lease
// owner of its own run, so a lost-answer adoption never takes another run's.
func TestReview6PlanOwnerIsTheRun(t *testing.T) {
	var owners []string
	prev := newLifecycleClient
	_, _, _ = reapPlan(t, orphanFacts, lifecycletest.New())
	installFakeLeases(t, lifecycletest.New())
	inner := newLifecycleClient
	newLifecycleClient = func(cfg aws.Config, owner string) (*lifecycle.Client, error) {
		owners = append(owners, owner)
		return inner(cfg, owner)
	}
	t.Cleanup(func() { newLifecycleClient = prev })
	if _, err := runTeardownPlanForTest(t, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := runTeardownPlanForTest(t, ""); err != nil {
		t.Fatal(err)
	}
	var plan []string
	for _, o := range owners {
		if strings.HasPrefix(o, "lagotto teardown --plan") {
			plan = append(plan, o)
		}
	}
	if len(plan) != 2 || plan[0] == plan[1] || plan[0] == "lagotto teardown --plan" {
		t.Errorf("owners: %q", owners)
	}
}

// TestReview6PlanNoLeaseUnfencedChecks (G4): `--plan --mode reap
// --i-understand-no-lease` outside aws, aws-us-gov and aws-cn goes through the same
// unfenced path as `teardown --iam-roles`: nothing account-wide is deleted
// while a lagotto-teardown job is pending in any region.
func TestReview6PlanNoLeaseUnfencedChecks(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1",
			Schedules: []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: "lagotto-teardown-log-group"}}}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	installFakeLeases(t, lifecycletest.New(), func(o *lifecycle.Options) { o.ControlRegion = "" })
	teardownMode, teardownYes, teardownNoLease = "reap", true, true
	out, err := runTeardownPlanForTest(t, "")
	// Kept, with why, like any in-use blocker of the fenced path.
	if err != nil || !strings.Contains(out, "us-east-1: one-shot job lagotto-teardown-log-group is pending") || len(rec.calls) != 0 {
		t.Errorf("%v, deleted %v\n%s", err, rec.calls, out)
	}
}

// TestReview6FreshCtx (G2a): a give-back's fresh context outlives the
// command's cancelled one.
func TestReview6FreshCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f, fcancel := freshCtx(ctx)
	defer fcancel()
	if f.Err() != nil {
		t.Errorf("a fresh context was cancelled with its parent: %v", f.Err())
	}
}
