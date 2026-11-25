package cmd

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// TestReview8UnfencedActiveRegion (G1): the unfenced path refuses while a
// region is in use (a watch --local region's active watches).
func TestReview8UnfencedActiveRegion(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-gov-west-1", ActiveWatches: 1}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
	if r.Outcome != idle.AccountWideBlocked || r.ReadFailed || len(rec.calls) != 0 {
		t.Errorf("%+v %v", r, rec.calls)
	}
}
