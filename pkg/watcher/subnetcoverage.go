package watcher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// Default-subnet coverage (#256). The poller launches a spawn watch, and the
// launch a hold prints, into the default VPC with no explicit subnet, so it can
// only land in an AZ that has a default subnet. Since #150 it silently skips
// the others. These checks tell the user before the watch is stored (`lagotto
// watch`, and `lagotto update` #257) and on an active watch (`lagotto doctor`).
//
// Explicit subnets: lagotto's spawn config has no subnet key (SpawnConfigFile
// has no such field and ParseSpawnConfigYAML rejects unknown keys), and a hold
// reserves by AZ alone, so no watch can name a subnet and there is nothing to
// skip the check for. TestSpawnConfigHasNoSubnetKey pins that: a subnet key
// added to the config must also make Check skip.

// SubnetCoverageEC2 is the slice of EC2 a coverage check calls. *ec2.Client
// satisfies it.
type SubnetCoverageEC2 interface {
	DescribeSubnetsAPI
	DescribeInstanceTypeOfferings(ctx context.Context, in *ec2.DescribeInstanceTypeOfferingsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error)
}

// SubnetCoverageChecker checks watches' default-subnet coverage. It memoizes
// each region's default subnets for its own lifetime, so `doctor` checking many
// watches reads each region once. Make one per command run.
type SubnetCoverageChecker struct {
	client      func(region string) SubnetCoverageEC2
	listRegions func(ctx context.Context) ([]string, error)

	mu      sync.Mutex
	subnets map[string]subnetRead
}

type subnetRead struct {
	set map[string]bool
	err error
}

// NewSubnetCoverageChecker returns a checker over cfg's credentials. It needs
// ec2:DescribeInstanceTypeOfferings and ec2:DescribeSubnets, plus
// ec2:DescribeRegions for a watch with no --regions (all enabled regions).
func NewSubnetCoverageChecker(cfg aws.Config) *SubnetCoverageChecker {
	return NewSubnetCoverageCheckerWith(
		func(region string) SubnetCoverageEC2 {
			return ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.Region = region })
		},
		func(ctx context.Context) ([]string, error) {
			out, err := ec2.NewFromConfig(cfg).DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
			if err != nil {
				return nil, err
			}
			regions := make([]string, 0, len(out.Regions))
			for _, r := range out.Regions {
				if n := aws.ToString(r.RegionName); n != "" {
					regions = append(regions, n)
				}
			}
			sort.Strings(regions)
			return regions, nil
		})
}

// NewSubnetCoverageCheckerWith is NewSubnetCoverageChecker with the EC2 client
// and the enabled-region list supplied, for tests.
func NewSubnetCoverageCheckerWith(client func(region string) SubnetCoverageEC2, listRegions func(ctx context.Context) ([]string, error)) *SubnetCoverageChecker {
	return &SubnetCoverageChecker{client: client, listRegions: listRegions, subnets: map[string]subnetRead{}}
}

// UnreachableAZ is an AZ that offers some of a watch's instance types but has
// no default subnet, so the watch can never land there.
type UnreachableAZ struct {
	AZ            string   `json:"az"`
	InstanceTypes []string `json:"instance_types"`
}

// RegionSubnetCoverage is one region's result.
type RegionSubnetCoverage struct {
	Region string `json:"region"`
	// OfferedAZs are the AZs offering any matched type (narrowed by --azs).
	// Empty when the pattern matches nothing offered here, which this check
	// leaves to the poller's own unmatched-pattern warning.
	OfferedAZs []string `json:"offered_azs,omitempty"`
	// LaunchableAZs are the offered AZs that have a default subnet.
	LaunchableAZs []string `json:"launchable_azs,omitempty"`
	// Unreachable are the offered AZs with no default subnet, with the types
	// each offers.
	Unreachable []UnreachableAZ `json:"unreachable,omitempty"`
	// NoDefaultSubnets is true when the region has no default subnet at all
	// (often: no default VPC).
	NoDefaultSubnets bool `json:"no_default_subnets,omitempty"`
	// Error is set when the region could not be checked. Its result is then
	// unknown: it never refuses a watch.
	Error string `json:"error,omitempty"`
}

// Blocked reports whether the region offers the watch's types but no offering
// AZ has a default subnet, so nothing can be launched there.
func (r RegionSubnetCoverage) Blocked() bool {
	return r.Error == "" && len(r.OfferedAZs) > 0 && len(r.LaunchableAZs) == 0
}

// SubnetCoverage is a watch's default-subnet coverage.
type SubnetCoverage struct {
	WatchID string `json:"watch_id,omitempty"`
	Pattern string `json:"pattern"`
	Action  string `json:"action"`
	// Checked is false when the check does not apply: a notify watch, or a
	// SageMaker watch. Such a coverage has nothing else set.
	Checked bool `json:"checked"`
	// AllRegions is true when the watch named no regions, so every enabled
	// region was checked and only a watch no region can serve is refused.
	AllRegions bool                   `json:"all_regions,omitempty"`
	Regions    []RegionSubnetCoverage `json:"regions,omitempty"`
	// Error is set when the check could not run at all (listing regions failed).
	Error string `json:"error,omitempty"`
}

// Check reports w's default-subnet coverage: for each region, the AZs where
// the watch's instance types are offered (DescribeInstanceTypeOfferings,
// narrowed by --azs), and which of them have a default subnet. It only applies
// to an EC2 spawn or hold watch; for any other it returns Checked=false. It
// makes read-only calls only, and never fails: a lookup error is recorded on
// the result and makes that region's result unknown.
//
// This is the one path `lagotto watch`, `lagotto update` (#257) and `lagotto
// doctor` share; Refusal and Warnings turn the result into what they print.
func (c *SubnetCoverageChecker) Check(ctx context.Context, w *Watch) *SubnetCoverage {
	cov := &SubnetCoverage{WatchID: w.WatchID, Pattern: w.InstanceTypePattern, Action: string(w.Action)}
	if normalizeService(w.Service) != ServiceEC2 || (w.Action != ActionSpawn && w.Action != ActionHold) {
		return cov
	}
	cov.Checked = true
	regions := w.Regions
	if len(regions) == 0 {
		cov.AllRegions = true
		var err error
		if regions, err = c.listRegions(ctx); err != nil {
			cov.Error = fmt.Sprintf("list enabled regions: %v", err)
			return cov
		}
	}
	matcher, err := regexp.Compile(patternToRegex(w.InstanceTypePattern))
	if err != nil {
		cov.Error = fmt.Sprintf("compile pattern %q: %v", w.InstanceTypePattern, err)
		return cov
	}
	cov.Regions = make([]RegionSubnetCoverage, len(regions))
	var wg sync.WaitGroup
	for i, region := range regions {
		wg.Add(1)
		go func(i int, region string) {
			defer wg.Done()
			cov.Regions[i] = c.checkRegion(ctx, region, w, matcher)
		}(i, region)
	}
	wg.Wait()
	return cov
}

func (c *SubnetCoverageChecker) checkRegion(ctx context.Context, region string, w *Watch, matcher *regexp.Regexp) RegionSubnetCoverage {
	rc := RegionSubnetCoverage{Region: region}
	api := c.client(region)
	offered, err := offeredAZsByType(ctx, api, w.InstanceTypePattern, matcher)
	if err != nil {
		rc.Error = fmt.Sprintf("describe instance type offerings: %v", err)
		return rc
	}
	// Narrow to the watch's --azs, as the matcher does (orderAZs).
	offeredSet := map[string]bool{}
	for typ, azs := range offered {
		azs = orderAZs(azs, w.AvailabilityZones)
		offered[typ] = azs
		for _, az := range azs {
			offeredSet[az] = true
		}
	}
	if len(offeredSet) == 0 {
		return rc
	}
	rc.OfferedAZs = sortedKeys(offeredSet)
	subnets, err := c.defaultSubnets(ctx, region, api)
	if err != nil {
		rc.Error = fmt.Sprintf("describe default subnets: %v", err)
		return rc
	}
	rc.NoDefaultSubnets = len(subnets) == 0
	unreachable := map[string][]string{}
	for typ, azs := range offered {
		for _, az := range azs {
			if !subnets[az] {
				unreachable[az] = append(unreachable[az], typ)
			}
		}
	}
	for _, az := range rc.OfferedAZs {
		if types, ok := unreachable[az]; ok {
			sort.Strings(types)
			rc.Unreachable = append(rc.Unreachable, UnreachableAZ{AZ: az, InstanceTypes: types})
		} else {
			rc.LaunchableAZs = append(rc.LaunchableAZs, az)
		}
	}
	return rc
}

// defaultSubnets reads a region's default-subnet AZs once per checker.
func (c *SubnetCoverageChecker) defaultSubnets(ctx context.Context, region string, api DescribeSubnetsAPI) (map[string]bool, error) {
	c.mu.Lock()
	if r, ok := c.subnets[region]; ok {
		c.mu.Unlock()
		return r.set, r.err
	}
	c.mu.Unlock()
	set, err := describeDefaultSubnetAZs(ctx, api)
	c.mu.Lock()
	c.subnets[region] = subnetRead{set: set, err: err}
	c.mu.Unlock()
	return set, err
}

// offeredAZsByType returns, for every instance type matching the watch
// pattern, the AZs in the client's region that offer it. A pattern of plain
// names and * wildcards is passed to EC2 as an instance-type filter (EC2
// filters take * too); a regex-form pattern (patternToRegex passes those
// through) reads every offering and matches locally. Either way the same
// regex the poller uses decides what matches.
func offeredAZsByType(ctx context.Context, api SubnetCoverageEC2, pattern string, matcher *regexp.Regexp) (map[string][]string, error) {
	in := &ec2.DescribeInstanceTypeOfferingsInput{LocationType: ec2types.LocationTypeAvailabilityZone}
	if parts := splitInstanceTypePatterns(pattern); len(parts) > 0 && !strings.ContainsAny(pattern, "^$()[]{}+\\") {
		in.Filters = []ec2types.Filter{{Name: aws.String("instance-type"), Values: parts}}
	}
	out := map[string][]string{}
	p := ec2.NewDescribeInstanceTypeOfferingsPaginator(api, in)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.InstanceTypeOfferings {
			typ, az := string(o.InstanceType), aws.ToString(o.Location)
			if typ == "" || az == "" || !matcher.MatchString(typ) {
				continue
			}
			out[typ] = append(out[typ], az)
		}
	}
	for typ := range out {
		sort.Strings(out[typ])
	}
	return out, nil
}

// Refusal returns the error a watch create or update refuses with, or nil.
//
// A watch that names its regions is refused when any of them is Blocked: the
// user asked for that region, and the watch could never land there. A watch
// with no --regions (every enabled region) is refused only when every region
// that offers its types is Blocked; otherwise Warnings lists the blocked ones.
// A region that could not be checked never refuses.
func (c *SubnetCoverage) Refusal() error {
	if c == nil || !c.Checked {
		return nil
	}
	var blocked []RegionSubnetCoverage
	usable := false
	for _, r := range c.Regions {
		switch {
		case r.Blocked():
			blocked = append(blocked, r)
		case len(r.OfferedAZs) > 0 || r.Error != "":
			usable = true
		}
	}
	if len(blocked) == 0 || (c.AllRegions && usable) {
		return nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "refusing %s watch %q: ", c.Action, c.Pattern)
	if len(blocked) == 1 {
		fmt.Fprintf(&b, "it can never launch in %s.", blocked[0].Region)
	} else {
		names := make([]string, len(blocked))
		for i, r := range blocked {
			names[i] = r.Region
		}
		fmt.Fprintf(&b, "it can never launch in %s.", strings.Join(names, ", "))
	}
	b.WriteString(" lagotto launches into the default VPC with no explicit subnet, so it can only land in an AZ with a default subnet, and none of the AZs that offer the type has one:\n")
	for _, r := range blocked {
		fmt.Fprintf(&b, "  %s: offered in %s; default subnet in none of them\n", r.Region, strings.Join(r.OfferedAZs, ", "))
	}
	b.WriteString("Create a default subnet in an AZ that offers the type, then re-run:\n")
	for _, cmd := range c.FixCommands(blocked...) {
		fmt.Fprintf(&b, "  %s\n", cmd)
	}
	if !c.AllRegions && len(blocked) < len(c.Regions) {
		b.WriteString("or drop the region(s) above from --regions.")
	}
	return fmt.Errorf("%s", strings.TrimRight(b.String(), "\n"))
}

// Warnings returns one line per offered AZ with no default subnet in a region
// that is not refused outright, plus one per region that could not be checked.
func (c *SubnetCoverage) Warnings() []string {
	if c == nil || !c.Checked {
		return nil
	}
	var out []string
	if c.Error != "" {
		out = append(out, fmt.Sprintf("could not check default-subnet coverage: %s", c.Error))
	}
	for _, r := range c.Regions {
		if r.Error != "" {
			out = append(out, fmt.Sprintf("%s: could not check default-subnet coverage: %s", r.Region, r.Error))
			continue
		}
		for _, u := range r.Unreachable {
			out = append(out, fmt.Sprintf("%s offers %s but has no default subnet; this watch can't land there (aws ec2 create-default-subnet --region %s --availability-zone %s)",
				u.AZ, strings.Join(u.InstanceTypes, ", "), r.Region, u.AZ))
		}
	}
	return out
}

// HasUnreachable reports whether any checked region has an offered AZ with no
// default subnet.
func (c *SubnetCoverage) HasUnreachable() bool {
	if c == nil {
		return false
	}
	for _, r := range c.Regions {
		if len(r.Unreachable) > 0 {
			return true
		}
	}
	return false
}

// FixCommands returns the commands that give each unreachable AZ of the given
// regions (all checked regions when none are given) a default subnet. A region
// with no default subnet at all may have no default VPC, which
// create-default-subnet needs; that is said in a comment on its first line.
func (c *SubnetCoverage) FixCommands(regions ...RegionSubnetCoverage) []string {
	if c == nil {
		return nil
	}
	if len(regions) == 0 {
		regions = c.Regions
	}
	var out []string
	for _, r := range regions {
		for i, u := range r.Unreachable {
			cmd := fmt.Sprintf("aws ec2 create-default-subnet --region %s --availability-zone %s", r.Region, u.AZ)
			if i == 0 && r.NoDefaultSubnets {
				cmd += fmt.Sprintf("  # %s has no default subnet at all: if it has no default VPC, run `aws ec2 create-default-vpc --region %s` first", r.Region, r.Region)
			}
			out = append(out, cmd)
		}
	}
	return out
}
