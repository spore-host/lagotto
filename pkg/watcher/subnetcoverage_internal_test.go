package watcher

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// coverageFixture builds a checker over per-region fake EC2 clients.
type coverageFixture struct {
	mu      sync.Mutex
	regions map[string]*fakeSubnetEC2
	enabled []string
	listErr error
}

func (f *coverageFixture) checker() *SubnetCoverageChecker {
	return NewSubnetCoverageCheckerWith(
		func(region string) SubnetCoverageEC2 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if c, ok := f.regions[region]; ok {
				return c
			}
			c := &fakeSubnetEC2{}
			f.regions[region] = c
			return c
		},
		func(context.Context) ([]string, error) { return f.enabled, f.listErr })
}

// incident is #256's account: us-west-2 offers p5.4xlarge in a-d, default
// subnets in a-c only.
func incident() *coverageFixture {
	return &coverageFixture{regions: map[string]*fakeSubnetEC2{
		"us-west-2": {
			defaultAZs: []string{"us-west-2a", "us-west-2b", "us-west-2c"},
			offerings:  map[string][]string{"p5.4xlarge": {"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"}},
		},
	}}
}

func spawnWatch(pattern string, regions ...string) *Watch {
	return &Watch{WatchID: "w-256", InstanceTypePattern: pattern, Regions: regions, Action: ActionSpawn, Service: ServiceEC2}
}

// TestSubnetCoverage_WarnsOnUnreachableAZ is the #256 incident: 2d offers the
// type but has no default subnet. Not a refusal (a-c can land), but a warning
// naming 2d and the fix.
func TestSubnetCoverage_WarnsOnUnreachableAZ(t *testing.T) {
	f := incident()
	cov := f.checker().Check(context.Background(), spawnWatch("p5.4xlarge", "us-west-2"))
	if !cov.Checked || cov.AllRegions {
		t.Fatalf("coverage = %+v", cov)
	}
	if err := cov.Refusal(); err != nil {
		t.Fatalf("refused: %v", err)
	}
	r := cov.Regions[0]
	if !reflect.DeepEqual(r.LaunchableAZs, []string{"us-west-2a", "us-west-2b", "us-west-2c"}) ||
		!reflect.DeepEqual(r.Unreachable, []UnreachableAZ{{AZ: "us-west-2d", InstanceTypes: []string{"p5.4xlarge"}}}) {
		t.Errorf("region = %+v", r)
	}
	w := cov.Warnings()
	if len(w) != 1 || !strings.Contains(w[0], "us-west-2d offers p5.4xlarge but has no default subnet; this watch can't land there") ||
		!strings.Contains(w[0], "aws ec2 create-default-subnet --region us-west-2 --availability-zone us-west-2d") {
		t.Errorf("warnings = %q", w)
	}
	if !cov.HasUnreachable() {
		t.Error("HasUnreachable = false")
	}
	// The exact-type pattern went to EC2 as a filter.
	if lf := f.regions["us-west-2"].lastFilter; len(lf) != 1 || aws.ToString(lf[0].Name) != "instance-type" || !reflect.DeepEqual(lf[0].Values, []string{"p5.4xlarge"}) {
		t.Errorf("offerings filter = %+v", lf)
	}
}

// TestSubnetCoverage_RefusesBlockedRegion: --azs us-west-2d leaves only the AZ
// with no subnet, so the watch could never land. Refused, with the command.
func TestSubnetCoverage_RefusesBlockedRegion(t *testing.T) {
	f := incident()
	w := spawnWatch("p5.4xlarge", "us-west-2")
	w.AvailabilityZones = []string{"us-west-2d"}
	cov := f.checker().Check(context.Background(), w)
	err := cov.Refusal()
	if err == nil {
		t.Fatal("not refused")
	}
	for _, want := range []string{"us-west-2", "offered in us-west-2d", "aws ec2 create-default-subnet --region us-west-2 --availability-zone us-west-2d"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "drop the region") {
		t.Errorf("single-region refusal should not suggest dropping the region: %q", err)
	}
}

// TestSubnetCoverage_MultiRegion: a named region that is blocked refuses the
// watch even if another serves it, and says the region can be dropped.
func TestSubnetCoverage_MultiRegion(t *testing.T) {
	f := incident()
	f.regions["eu-north-1"] = &fakeSubnetEC2{
		offerings: map[string][]string{"p5.4xlarge": {"eu-north-1a"}},
	} // no default subnets at all
	cov := f.checker().Check(context.Background(), spawnWatch("p5.4xlarge", "us-west-2", "eu-north-1"))
	err := cov.Refusal()
	if err == nil || !strings.Contains(err.Error(), "eu-north-1") || !strings.Contains(err.Error(), "drop the region") {
		t.Fatalf("refusal = %v", err)
	}
	if !strings.Contains(err.Error(), "create-default-vpc --region eu-north-1") {
		t.Errorf("a region with no default subnet at all should mention create-default-vpc: %v", err)
	}
	if strings.Contains(err.Error(), "us-west-2:") {
		t.Errorf("us-west-2 is not blocked: %v", err)
	}

	// Two blocked regions are both named.
	f.regions["us-west-2"].defaultAZs = nil
	cov = f.checker().Check(context.Background(), spawnWatch("p5.4xlarge", "us-west-2", "eu-north-1"))
	if err := cov.Refusal(); err == nil || !strings.Contains(err.Error(), "us-west-2, eu-north-1") {
		t.Errorf("refusal = %v", err)
	}
}

// TestSubnetCoverage_AllRegions: a watch with no --regions is refused only
// when no enabled region can serve it.
func TestSubnetCoverage_AllRegions(t *testing.T) {
	f := incident()
	f.regions["eu-north-1"] = &fakeSubnetEC2{offerings: map[string][]string{"p5.4xlarge": {"eu-north-1a"}}}
	f.regions["ap-south-1"] = &fakeSubnetEC2{} // not offered: no opinion
	f.enabled = []string{"ap-south-1", "eu-north-1", "us-west-2"}
	cov := f.checker().Check(context.Background(), spawnWatch("p5.4xlarge"))
	if !cov.AllRegions || len(cov.Regions) != 3 {
		t.Fatalf("coverage = %+v", cov)
	}
	if err := cov.Refusal(); err != nil {
		t.Errorf("refused although us-west-2 can serve it: %v", err)
	}
	if w := cov.Warnings(); len(w) != 2 {
		t.Errorf("warnings = %q, want eu-north-1a and us-west-2d", w)
	}

	f.regions["us-west-2"].defaultAZs = nil
	cov = f.checker().Check(context.Background(), spawnWatch("p5.4xlarge"))
	if err := cov.Refusal(); err == nil {
		t.Error("not refused although no region can serve it")
	}

	f.listErr = errors.New("UnauthorizedOperation")
	cov = f.checker().Check(context.Background(), spawnWatch("p5.4xlarge"))
	if cov.Refusal() != nil || len(cov.Warnings()) != 1 || !strings.Contains(cov.Warnings()[0], "list enabled regions") {
		t.Errorf("list error: refusal %v, warnings %q", cov.Refusal(), cov.Warnings())
	}
}

// TestSubnetCoverage_LookupErrorsWarnOnly: a lookup that fails never refuses.
func TestSubnetCoverage_LookupErrorsWarnOnly(t *testing.T) {
	f := incident()
	f.regions["us-west-2"].subnetErr = errors.New("UnauthorizedOperation")
	cov := f.checker().Check(context.Background(), spawnWatch("p5.4xlarge", "us-west-2"))
	if cov.Refusal() != nil {
		t.Errorf("refused on a lookup error: %v", cov.Refusal())
	}
	if w := cov.Warnings(); len(w) != 1 || !strings.Contains(w[0], "describe default subnets") {
		t.Errorf("warnings = %q", w)
	}

	f = incident()
	f.regions["us-west-2"].offerErr = errors.New("UnauthorizedOperation")
	cov = f.checker().Check(context.Background(), spawnWatch("p5.4xlarge", "us-west-2"))
	if cov.Refusal() != nil || len(cov.Warnings()) != 1 || !strings.Contains(cov.Warnings()[0], "describe instance type offerings") {
		t.Errorf("offerings error: refusal %v, warnings %q", cov.Refusal(), cov.Warnings())
	}

	// An uncompilable pattern is reported, not refused.
	cov = incident().checker().Check(context.Background(), spawnWatch("p5.(", "us-west-2"))
	if cov.Refusal() != nil || len(cov.Warnings()) != 1 {
		t.Errorf("bad pattern: refusal %v, warnings %q", cov.Refusal(), cov.Warnings())
	}
}

// TestSubnetCoverage_NotApplicable: notify and SageMaker watches are not
// checked; a hold and a pre-service-field ("") EC2 watch are.
func TestSubnetCoverage_NotApplicable(t *testing.T) {
	c := incident().checker()
	notify := spawnWatch("p5.4xlarge", "us-west-2")
	notify.Action = ActionNotify
	sm := spawnWatch("ml.p5.48xlarge", "us-west-2")
	sm.Service = ServiceSageMaker
	for _, w := range []*Watch{notify, sm} {
		cov := c.Check(context.Background(), w)
		if cov.Checked || cov.Refusal() != nil || cov.Warnings() != nil || cov.HasUnreachable() || len(cov.Regions) != 0 {
			t.Errorf("%s/%s: coverage = %+v, want unchecked", w.Service, w.Action, cov)
		}
	}
	hold := spawnWatch("p5.4xlarge", "us-west-2")
	hold.Action, hold.Service = ActionHold, ""
	if cov := c.Check(context.Background(), hold); !cov.Checked || !cov.HasUnreachable() {
		t.Errorf("hold: coverage = %+v, want checked with us-west-2d unreachable", cov)
	}
	var nilCov *SubnetCoverage
	if nilCov.Refusal() != nil || nilCov.Warnings() != nil || nilCov.HasUnreachable() || nilCov.FixCommands() != nil {
		t.Error("nil coverage must be inert")
	}
}

// TestSubnetCoverage_PatternsAndMemo: a wildcard is matched with the poller's
// regex; a regex-form pattern reads every offering (no EC2 filter); types are
// grouped per AZ; each region's subnets are read once per checker.
func TestSubnetCoverage_PatternsAndMemo(t *testing.T) {
	f := &coverageFixture{regions: map[string]*fakeSubnetEC2{
		"us-west-2": {
			defaultAZs: []string{"us-west-2a"},
			offerings: map[string][]string{
				"p5.4xlarge":   {"us-west-2a", "us-west-2d"},
				"p5.48xlarge":  {"us-west-2d"},
				"p5e.48xlarge": {"us-west-2d"},
			},
		},
	}}
	c := f.checker()
	cov := c.Check(context.Background(), spawnWatch("p5.*", "us-west-2"))
	want := []UnreachableAZ{{AZ: "us-west-2d", InstanceTypes: []string{"p5.48xlarge", "p5.4xlarge"}}}
	if got := cov.Regions[0].Unreachable; !reflect.DeepEqual(got, want) {
		t.Errorf("unreachable = %+v, want %+v (p5e excluded by the pattern)", got, want)
	}

	cov = c.Check(context.Background(), spawnWatch("^p5e?\\.48xlarge$", "us-west-2"))
	if lf := f.regions["us-west-2"].lastFilter; lf != nil {
		t.Errorf("regex pattern sent filter %+v, want none", lf)
	}
	if cov.Refusal() == nil {
		t.Error("only 2d offers the 48xlarges and it has no subnet: want a refusal")
	}
	if n := f.regions["us-west-2"].subnetCalls; n != 1 {
		t.Errorf("DescribeSubnets called %d times across two checks, want 1 (memoized)", n)
	}

	// A pattern nothing matches says nothing: that's the poller's #135 warning.
	cov = c.Check(context.Background(), spawnWatch("zz9.large", "us-west-2"))
	if cov.Refusal() != nil || cov.Warnings() != nil {
		t.Errorf("unmatched pattern: refusal %v, warnings %q", cov.Refusal(), cov.Warnings())
	}
}

func TestNewSubnetCoverageChecker_Wiring(t *testing.T) {
	c := NewSubnetCoverageChecker(aws.Config{Region: "us-west-2"})
	if c.client == nil || c.listRegions == nil || c.subnets == nil {
		t.Fatalf("checker = %+v, want every dependency set", c)
	}
	if c.client("eu-north-1") == nil {
		t.Error("client(region) returned nil")
	}
}
