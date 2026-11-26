package cmd

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// The seventh review of lagotto#351.

// TestReview7UnfencedActivityUnknown (G3): the unfenced path refuses, as a
// read failure, a region it could not read in full.
func TestReview7UnfencedActivityUnknown(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-iso-east-1", ActivityUnknown: true}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
	if r.Outcome != idle.AccountWideBlocked || !r.ReadFailed || len(rec.calls) != 0 {
		t.Errorf("%+v %v", r, rec.calls)
	}
}

// TestReview7PlanUsesTheSeam (G5): `teardown --plan --mode reap` runs its
// account-wide steps through the teardownAccountWide seam, limited to them.
func TestReview7PlanUsesTheSeam(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-east-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	prev := teardownAccountWide
	t.Cleanup(func() { teardownAccountWide = prev })
	var only map[string]bool
	teardownAccountWide = func(_ context.Context, _ aws.Config, _ string, o map[string]bool, _ func(string, ...any)) idle.AccountWideResult {
		only = o
		return idle.AccountWideResult{Outcome: idle.AccountWideNothing}
	}
	teardownMode, teardownYes = "reap", true
	if out, err := runTeardownPlanForTest(t, ""); err != nil || len(only) != 1 || len(rec.calls) != 0 {
		t.Errorf("%v only=%v calls=%v\n%s", err, only, rec.calls, out)
	}
}
