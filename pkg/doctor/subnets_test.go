package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// The subnet-coverage check (#256).

func incidentCoverage() *watcher.SubnetCoverage {
	return &watcher.SubnetCoverage{
		WatchID: "w-a660980c", Pattern: "p5.4xlarge", Action: "spawn", Checked: true,
		Regions: []watcher.RegionSubnetCoverage{{
			Region:        "us-west-2",
			OfferedAZs:    []string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"},
			LaunchableAZs: []string{"us-west-2a", "us-west-2b", "us-west-2c"},
			Unreachable:   []watcher.UnreachableAZ{{AZ: "us-west-2d", InstanceTypes: []string{"p5.4xlarge"}}},
		}},
	}
}

func subnetState(t *testing.T, covs ...*watcher.SubnetCoverage) State {
	st := healthyState(t)
	st.SubnetsChecked = true
	st.Subnets = covs
	return st
}

func TestEvaluate_SubnetCoverageWarnsPerWatch(t *testing.T) {
	ok := &watcher.SubnetCoverage{WatchID: "w-ok", Pattern: "g6.xlarge", Action: "hold", Checked: true,
		Regions: []watcher.RegionSubnetCoverage{{Region: "us-east-1", OfferedAZs: []string{"us-east-1a"}, LaunchableAZs: []string{"us-east-1a"}}}}
	r := Evaluate(subnetState(t, incidentCoverage(), ok))
	c := find(t, r, CheckSubnetCover)
	if c.Status != StatusWarn {
		t.Fatalf("subnet-coverage = %s (%s), want WARN", c.Status, c.Summary)
	}
	if !strings.Contains(c.Summary, "1 of 2") {
		t.Errorf("summary = %q", c.Summary)
	}
	if len(c.Detail) != 1 || !strings.Contains(c.Detail[0], "w-a660980c (spawn p5.4xlarge): us-west-2d offers p5.4xlarge") {
		t.Errorf("detail = %q", c.Detail)
	}
	if len(c.Fix) != 1 || c.Fix[0] != "aws ec2 create-default-subnet --region us-west-2 --availability-zone us-west-2d" {
		t.Errorf("fix = %q", c.Fix)
	}
	if r.Failed() {
		t.Error("a subnet-coverage warning must not fail doctor")
	}
}

func TestEvaluate_SubnetCoverageBlockedAndDeduped(t *testing.T) {
	blocked := incidentCoverage()
	blocked.WatchID = "w-blocked"
	blocked.Regions[0].OfferedAZs = []string{"us-west-2d"}
	blocked.Regions[0].LaunchableAZs = nil
	c := find(t, Evaluate(subnetState(t, incidentCoverage(), blocked)), CheckSubnetCover)
	if c.Status != StatusWarn || !strings.Contains(c.Summary, "1 of them with no AZ they can launch in") {
		t.Errorf("subnet-coverage = %s (%s)", c.Status, c.Summary)
	}
	if !strings.Contains(strings.Join(c.Detail, "\n"), "w-blocked (spawn p5.4xlarge): can never launch") {
		t.Errorf("detail = %q", c.Detail)
	}
	if len(c.Fix) != 1 {
		t.Errorf("fix = %q, want the one command once", c.Fix)
	}
}

func TestEvaluate_SubnetCoveragePassAndOmitted(t *testing.T) {
	ok := &watcher.SubnetCoverage{WatchID: "w-ok", Checked: true,
		Regions: []watcher.RegionSubnetCoverage{{Region: "us-east-1", OfferedAZs: []string{"us-east-1a"}, LaunchableAZs: []string{"us-east-1a"}}}}
	if c := find(t, Evaluate(subnetState(t, ok, nil)), CheckSubnetCover); c.Status != StatusPass {
		t.Errorf("subnet-coverage = %s (%s), want PASS", c.Status, c.Summary)
	}
	absent(t, Evaluate(subnetState(t)), CheckSubnetCover)
	absent(t, Evaluate(healthyState(t)), CheckSubnetCover)
}

func TestEvaluate_SubnetCoverageErrors(t *testing.T) {
	st := subnetState(t)
	st.SubnetsErr = errors.New("AccessDeniedException")
	if c := find(t, Evaluate(st), CheckSubnetCover); c.Status != StatusWarn || !strings.Contains(c.Summary, "could not list active watches") {
		t.Errorf("list error: %s (%s)", c.Status, c.Summary)
	}
	unknown := &watcher.SubnetCoverage{WatchID: "w-x", Checked: true,
		Regions: []watcher.RegionSubnetCoverage{{Region: "us-west-2", Error: "describe default subnets: UnauthorizedOperation"}}}
	c := find(t, Evaluate(subnetState(t, unknown)), CheckSubnetCover)
	if c.Status != StatusWarn || c.Summary != "default-subnet coverage could not be fully checked" {
		t.Errorf("lookup error: %s (%s)", c.Status, c.Summary)
	}
}

func TestGather_SubnetCoverage(t *testing.T) {
	called := false
	st := Gather(context.Background(), Options{
		Region: "us-west-2",
		SubnetCoverage: func(context.Context) ([]*watcher.SubnetCoverage, error) {
			called = true
			return []*watcher.SubnetCoverage{incidentCoverage()}, nil
		},
	})
	if !called || !st.SubnetsChecked || len(st.Subnets) != 1 {
		t.Errorf("Gather: called=%v checked=%v subnets=%d", called, st.SubnetsChecked, len(st.Subnets))
	}
	if st := Gather(context.Background(), Options{Region: "us-west-2"}); st.SubnetsChecked {
		t.Error("no SubnetCoverage option should leave the check off")
	}
}
