package watcher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// DefaultSubnetCacheTTL is how long the poller trusts its per-region set of AZs
// with a default subnet (#256). Before #256 the set was cached for the life of
// a warm Lambda container, so a default subnet the user created after a cold
// start stayed invisible until the next one. 15 minutes bounds that to a few
// poll cycles while keeping DescribeSubnets off the per-launch path.
const DefaultSubnetCacheTTL = 15 * time.Minute

// DescribeSubnetsAPI is the slice of EC2 the default-subnet lookup calls.
// *ec2.Client satisfies it.
type DescribeSubnetsAPI interface {
	DescribeSubnets(ctx context.Context, in *ec2.DescribeSubnetsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
}

// describeDefaultSubnetAZs returns the AZ names in the client's region that
// have a default subnet (default-for-az=true): the AZs a default-VPC
// RunInstances with no explicit subnet, as the poller launches, can place in.
// Requires only ec2:DescribeSubnets.
func describeDefaultSubnetAZs(ctx context.Context, api DescribeSubnetsAPI) (map[string]bool, error) {
	set := map[string]bool{}
	p := ec2.NewDescribeSubnetsPaginator(api, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{{Name: aws.String("default-for-az"), Values: []string{"true"}}},
	})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, sn := range out.Subnets {
			if az := aws.ToString(sn.AvailabilityZone); az != "" {
				set[az] = true
			}
		}
	}
	return set, nil
}

// subnetAZCache memoizes describeDefaultSubnetAZs per region for ttl (#256).
// An API error is not cached, so the next launch retries the lookup.
type subnetAZCache struct {
	mu       sync.Mutex
	describe func(ctx context.Context, region string) (map[string]bool, error)
	now      func() time.Time
	ttl      time.Duration
	entries  map[string]subnetCacheEntry
}

type subnetCacheEntry struct {
	set     map[string]bool
	fetched time.Time
}

func newSubnetAZCache(describe func(ctx context.Context, region string) (map[string]bool, error), now func() time.Time, ttl time.Duration) *subnetAZCache {
	if now == nil {
		now = time.Now
	}
	return &subnetAZCache{describe: describe, now: now, ttl: ttl, entries: map[string]subnetCacheEntry{}}
}

// newDefaultSubnetAZLookup returns the poller's cached default-subnet lookup
// over cfg's credentials. Best-effort: an API error or empty result gives nil,
// and callers fail open (try all AZs) rather than block a launch (#150).
func newDefaultSubnetAZLookup(cfg aws.Config) *subnetAZCache {
	return newSubnetAZCache(func(ctx context.Context, region string) (map[string]bool, error) {
		return describeDefaultSubnetAZs(ctx, ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.Region = region }))
	}, time.Now, DefaultSubnetCacheTTL)
}

// lookup returns the region's AZs with a default subnet, re-reading it once
// the cached copy is older than the TTL. nil on error (fail open, not cached).
func (c *subnetAZCache) lookup(ctx context.Context, region string) map[string]bool {
	if region == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[region]; ok && c.now().Sub(e.fetched) < c.ttl {
		return e.set
	}
	set, err := c.describe(ctx, region)
	if err != nil {
		delete(c.entries, region)
		return nil
	}
	c.entries[region] = subnetCacheEntry{set: set, fetched: c.now()}
	return set
}

// forget drops the region's cached set, so the next lookup re-reads it. The
// spawner and holder call it after a failed attempt that skipped AZs for want
// of a default subnet: the user may be creating one in response (#256).
func (c *subnetAZCache) forget(region string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, region)
}

// ErrClassNoSubnet is the LastError class for a failed attempt that skipped
// candidate AZs because they have no default subnet (#256). The failure
// itself keeps its own class (capacity, unknown, ...) for the retry decision;
// this class only changes what `list`/`status` say, so the user sees that an
// AZ was excluded rather than a plain capacity miss.
const ErrClassNoSubnet = "no-subnet"

// NoSubnetError wraps a launch or hold failure whose sweep skipped candidate
// AZs with no default subnet (#256). It unwraps to the original error, so
// ClassifyFailure, IsQuotaExceeded and errors.As see exactly what they did
// before; only the heartbeat's description changes.
type NoSubnetError struct {
	// AZs are the candidate AZs left out because they have no default subnet.
	AZs []string
	// All is true when no candidate had a default subnet, so the failure is
	// down to the missing subnets whatever EC2 said.
	All bool
	Err error
}

func (e *NoSubnetError) Error() string {
	return fmt.Sprintf("no default subnet in %s (skipped): %v", strings.Join(e.AZs, ", "), e.Err)
}

func (e *NoSubnetError) Unwrap() error { return e.Err }

// withNoSubnet wraps err in a NoSubnetError when any AZ was dropped.
func withNoSubnet(err error, dropped []string, all bool) error {
	if err == nil || len(dropped) == 0 {
		return err
	}
	return &NoSubnetError{AZs: dropped, All: all, Err: err}
}

// splitLaunchableAZs partitions attempts into those with a default subnet in
// usable (kept, in order; "" let-EC2-choose always kept) and those without
// (dropped).
func splitLaunchableAZs(usable map[string]bool, attempts []string) (kept, dropped []string) {
	kept = make([]string, 0, len(attempts))
	for _, a := range attempts {
		if a == "" || usable[a] {
			kept = append(kept, a)
		} else {
			dropped = append(dropped, a)
		}
	}
	return kept, dropped
}

// iceHintRE finds the AZ list EC2 appends to an InsufficientInstanceCapacity
// message: "...You can currently get p5.4xlarge capacity by not specifying an
// Availability Zone in your request or choosing us-west-2a, us-west-2b,
// us-west-2d."
var iceHintRE = regexp.MustCompile(`choosing ([a-z]{2}(?:-[a-z0-9]+)+(?:(?:,\s*|\s+)(?:or\s+)?[a-z]{2}(?:-[a-z0-9]+)+)*)`)

// iceCapacityHint returns the AZs EC2's capacity message says have capacity,
// comma-joined, or "" when the message carries no such hint. It is the most
// useful part of the message and comes last, so the 256-byte LastError cut
// used to drop it (#256); describeAttemptError puts it first instead.
func iceCapacityHint(msg string) string {
	m := iceHintRE.FindStringSubmatch(msg)
	if m == nil {
		return ""
	}
	var azs []string
	for _, f := range strings.FieldsFunc(m[1], func(r rune) bool { return r == ',' || r == ' ' }) {
		f = strings.TrimSuffix(f, ".")
		if f == "" || f == "or" {
			continue
		}
		azs = append(azs, f)
	}
	return strings.Join(azs, ", ")
}

// sortedKeys returns a set's members in order.
func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
