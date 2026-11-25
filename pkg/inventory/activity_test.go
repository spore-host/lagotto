package inventory

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestGatherActivityIsTheFootprintsRule: the poller's verdict reads only the
// activity signals, and decides with the same Region.Active the footprint
// classifies by.
func TestGatherActivityIsTheFootprintsRule(t *testing.T) {
	src := realAccount()
	for _, region := range src.enabled {
		r, w := GatherActivity(context.Background(), src.Region(region), region, Options{})
		full := src.regions[region]
		if r.Active() != full.Active() || len(w) != 0 {
			t.Errorf("%s: verdict active=%v, footprint active=%v (%v)", region, r.Active(), full.Active(), w)
		}
	}
	src.errs = map[string]error{"us-west-2 Jobs": errors.New("AccessDenied")}
	r, w := GatherActivity(context.Background(), src.Region("us-west-2"), "us-west-2", Options{})
	if !r.Active() || !r.ActivityUnknown || len(w) != 1 || !strings.Contains(w[0], "SageMaker") {
		t.Errorf("an unreadable signal: %+v %v", r, w)
	}
	if got := strings.Join(r.ActiveReasons(), ";"); got != "activity could not be read" {
		t.Errorf("reasons %q", got)
	}
}

// TestRegionFacts: a region the footprint read, and one it did not.
func TestRegionFacts(t *testing.T) {
	f, err := Gather(context.Background(), realAccount(), acct, Scope{AllRegions: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if f.RegionFacts("us-west-2") == nil || f.RegionFacts("mars-north-1") != nil {
		t.Error("RegionFacts")
	}
}

// TestActiveReasons names every live signal, one phrase each, and nothing for
// an idle region.
func TestActiveReasons(t *testing.T) {
	r := Region{ActiveWatches: 2, ArmedLaunches: 1, Holds: []Item{{}}, Jobs: []Item{{}, {}}}
	want := "2 active watch(es); 1 pending or retrying scheduled launch(es); 1 capacity reservation(s) held; 2 SageMaker training job(s) running"
	if got := strings.Join(r.ActiveReasons(), "; "); got != want || !r.Active() {
		t.Errorf("reasons %q, want %q", got, want)
	}
	if idle := (Region{}); idle.Active() || len(idle.ActiveReasons()) != 0 {
		t.Errorf("an idle region: %v", idle.ActiveReasons())
	}
}
