package inventory

import (
	"context"
	"fmt"
)

// GatherActivity reads only region's activity signals: its active watches and
// scheduled launches (from lagotto's tables), the capacity reservations hold
// watches hold, and the SageMaker training jobs running for a watch. It is
// the poller's per-run idle verdict (lagotto#326): the same readers and the
// same rule ([Region.Active]) as the footprint's classification, without the
// rest of the footprint's reads.
//
// Every read that fails sets ActivityUnknown, so a region whose activity
// cannot be read is active. The returned warnings say what failed.
func GatherActivity(ctx context.Context, rs RegionSource, region string, o Options) (Region, []string) {
	o = o.withDefaults()
	r := Region{Region: region}
	var warnings []string
	fail := func(what string, err error) bool {
		if err == nil {
			return false
		}
		warnings = append(warnings, fmt.Sprintf("%s: could not read %s: %v", region, what, err))
		r.ActivityUnknown = true
		return true
	}
	// Activity answers an absent table as no activity, so it needs no
	// DescribeTable first.
	act, err := rs.Activity(ctx, o.WatchesTable, o.ScheduledTable)
	if !fail("the active watches and scheduled launches", err) {
		r.ActiveWatches, r.ArmedLaunches, r.RecordedSchedules = act.ActiveWatches, act.ArmedLaunches, act.Recorded
	}
	r.Holds, err = rs.Holds(ctx)
	fail("the capacity reservations lagotto holds", err)
	r.Jobs, err = rs.Jobs(ctx)
	fail("the SageMaker training jobs", err)
	return r, warnings
}

// RegionFacts returns region's facts, or nil when f did not read it.
func (f *Facts) RegionFacts(region string) *Region {
	for i := range f.Regions {
		if f.Regions[i].Region == region {
			return &f.Regions[i]
		}
	}
	return nil
}
