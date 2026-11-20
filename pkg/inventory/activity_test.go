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

func TestLastRegion(t *testing.T) {
	f, err := Gather(context.Background(), realAccount(), acct, Scope{AllRegions: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if last, why := LastRegion(f, "us-west-2"); last || !strings.Contains(why, "ap-southeast-2, eu-west-1, us-east-1") {
		t.Errorf("four regions with tables: %v %q", last, why)
	}

	one := func() *fakeSource {
		s := realAccount()
		s.regions = map[string]Region{"us-west-2": deployed("us-west-2")}
		return s
	}
	f, _ = Gather(context.Background(), one(), acct, Scope{AllRegions: true}, Options{})
	if last, why := LastRegion(f, "us-west-2"); !last {
		t.Errorf("the only region: %q", why)
	}
	// Fail closed: an unreadable other region, an incomplete account, or
	// facts of fewer than every region.
	s := one()
	s.errs = map[string]error{"eu-west-1 Tables": errors.New("AccessDenied")}
	f, _ = Gather(context.Background(), s, acct, Scope{AllRegions: true}, Options{})
	if last, why := LastRegion(f, "us-west-2"); last || !strings.Contains(why, "eu-west-1") {
		t.Errorf("an unreadable region: %v %q", last, why)
	}
	s = one()
	s.errs = map[string]error{"global Role " + "lagotto-instance-role": errors.New("AccessDenied")}
	f, _ = Gather(context.Background(), s, acct, Scope{AllRegions: true}, Options{})
	if last, _ := LastRegion(f, "us-west-2"); last {
		t.Error("an unreadable account counted as the last region")
	}
	f, _ = Gather(context.Background(), one(), acct, Scope{Regions: []string{"us-west-2"}}, Options{})
	if last, _ := LastRegion(f, "us-west-2"); last {
		t.Error("a one-region footprint counted as the last region")
	}
	f, _ = Gather(context.Background(), one(), acct, Scope{AllRegions: true}, Options{})
	if last, _ := LastRegion(f, "mars-north-1"); last {
		t.Error("a region that is not enabled counted as the last")
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
