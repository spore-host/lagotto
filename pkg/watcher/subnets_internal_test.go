package watcher

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// iceErr is an InsufficientInstanceCapacity error with EC2's real message
// shape, from the #256 incident.
type iceErr struct{ msg string }

func (e *iceErr) Error() string                 { return "InsufficientInstanceCapacity: " + e.msg }
func (e *iceErr) ErrorCode() string             { return "InsufficientInstanceCapacity" }
func (e *iceErr) ErrorMessage() string          { return e.msg }
func (e *iceErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

const iceMsg256 = "We currently do not have sufficient p5.4xlarge capacity in the Availability Zone you requested (us-west-2c). " +
	"Our system will be working on provisioning additional capacity. You can currently get p5.4xlarge capacity by not " +
	"specifying an Availability Zone in your request or choosing us-west-2a, us-west-2b, us-west-2d."

// TestSubnetAZCache_TTL is #256 item 4: the lookup is re-read once older than
// the TTL (it used to be cached for the container's life), errors are not
// cached, and forget drops an entry.
func TestSubnetAZCache_TTL(t *testing.T) {
	now := time.Date(2026, 11, 7, 1, 46, 0, 0, time.UTC)
	calls := 0
	sets := []map[string]bool{
		{"us-west-2a": true, "us-west-2b": true, "us-west-2c": true},
		{"us-west-2a": true, "us-west-2b": true, "us-west-2c": true, "us-west-2d": true},
	}
	var failNext bool
	c := newSubnetAZCache(func(_ context.Context, region string) (map[string]bool, error) {
		if failNext {
			failNext = false
			return nil, errors.New("throttled")
		}
		s := sets[min(calls, len(sets)-1)]
		calls++
		return s, nil
	}, func() time.Time { return now }, DefaultSubnetCacheTTL)
	ctx := context.Background()

	if got := c.lookup(ctx, ""); got != nil {
		t.Errorf("empty region = %v, want nil", got)
	}
	if got := c.lookup(ctx, "us-west-2"); got["us-west-2d"] || len(got) != 3 {
		t.Fatalf("first lookup = %v", got)
	}
	now = now.Add(DefaultSubnetCacheTTL - time.Second)
	if got := c.lookup(ctx, "us-west-2"); got["us-west-2d"] || calls != 1 {
		t.Fatalf("within TTL: lookup = %v after %d calls, want the cached set, 1 call", got, calls)
	}
	now = now.Add(2 * time.Second)
	if got := c.lookup(ctx, "us-west-2"); !got["us-west-2d"] || calls != 2 {
		t.Fatalf("past TTL: lookup = %v after %d calls, want the re-read set with us-west-2d", got, calls)
	}

	// forget makes the next lookup re-read even within the TTL.
	c.forget("us-west-2")
	c.lookup(ctx, "us-west-2")
	if calls != 3 {
		t.Errorf("after forget: %d calls, want 3", calls)
	}

	// An error is not cached: nil now, and the next lookup tries again.
	c.forget("us-west-2")
	failNext = true
	if got := c.lookup(ctx, "us-west-2"); got != nil {
		t.Errorf("failed lookup = %v, want nil (fail open)", got)
	}
	if got := c.lookup(ctx, "us-west-2"); got == nil || calls != 4 {
		t.Errorf("lookup after an error = %v after %d calls, want a fresh read", got, calls)
	}
}

func TestNewSubnetAZCache_DefaultClock(t *testing.T) {
	c := newSubnetAZCache(func(context.Context, string) (map[string]bool, error) { return nil, nil }, nil, time.Minute)
	if c.now == nil {
		t.Fatal("nil clock not defaulted")
	}
	if newDefaultSubnetAZLookup(aws.Config{}).ttl != DefaultSubnetCacheTTL {
		t.Error("the poller's lookup does not use DefaultSubnetCacheTTL")
	}
}

// fakeSubnetEC2 serves DescribeSubnets and DescribeInstanceTypeOfferings from
// fixed data, paging both (one item per page) to exercise the paginators.
type fakeSubnetEC2 struct {
	defaultAZs []string
	offerings  map[string][]string // type -> AZs
	subnetErr  error
	offerErr   error

	subnetCalls int
	offerCalls  int
	lastFilter  []ec2types.Filter
}

func (f *fakeSubnetEC2) DescribeSubnets(_ context.Context, in *ec2.DescribeSubnetsInput, _ ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	f.subnetCalls++
	if f.subnetErr != nil {
		return nil, f.subnetErr
	}
	if len(in.Filters) != 1 || aws.ToString(in.Filters[0].Name) != "default-for-az" {
		return nil, errors.New("want the default-for-az filter")
	}
	i := 0
	if in.NextToken != nil {
		i = len(aws.ToString(in.NextToken))
	}
	if i >= len(f.defaultAZs) {
		return &ec2.DescribeSubnetsOutput{}, nil
	}
	out := &ec2.DescribeSubnetsOutput{Subnets: []ec2types.Subnet{{AvailabilityZone: aws.String(f.defaultAZs[i])}, {}}}
	if i+1 < len(f.defaultAZs) {
		out.NextToken = aws.String(strings.Repeat("x", i+1))
	}
	return out, nil
}

func (f *fakeSubnetEC2) DescribeInstanceTypeOfferings(_ context.Context, in *ec2.DescribeInstanceTypeOfferingsInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	f.offerCalls++
	f.lastFilter = in.Filters
	if f.offerErr != nil {
		return nil, f.offerErr
	}
	// Sorted, so the pages are stable across calls (map order is not).
	var all []ec2types.InstanceTypeOffering
	for _, typ := range sortedKeys(setOf(f.offerings)) {
		for _, az := range f.offerings[typ] {
			all = append(all, ec2types.InstanceTypeOffering{InstanceType: ec2types.InstanceType(typ), Location: aws.String(az)})
		}
	}
	i := 0
	if in.NextToken != nil {
		i = len(aws.ToString(in.NextToken))
	}
	if i >= len(all) {
		return &ec2.DescribeInstanceTypeOfferingsOutput{}, nil
	}
	out := &ec2.DescribeInstanceTypeOfferingsOutput{InstanceTypeOfferings: all[i : i+1]}
	if i+1 < len(all) {
		out.NextToken = aws.String(strings.Repeat("x", i+1))
	}
	return out, nil
}

func TestDescribeDefaultSubnetAZs(t *testing.T) {
	f := &fakeSubnetEC2{defaultAZs: []string{"us-west-2a", "us-west-2b", "us-west-2c"}}
	got, err := describeDefaultSubnetAZs(context.Background(), f)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"us-west-2a", "us-west-2b", "us-west-2c"}; !reflect.DeepEqual(sortedKeys(got), want) {
		t.Errorf("default-subnet AZs = %v, want %v (all pages)", sortedKeys(got), want)
	}
	f.subnetErr = errors.New("AccessDenied")
	if _, err := describeDefaultSubnetAZs(context.Background(), f); err == nil {
		t.Error("want the API error")
	}
}

func TestIceCapacityHint(t *testing.T) {
	for _, tc := range []struct{ msg, want string }{
		{iceMsg256, "us-west-2a, us-west-2b, us-west-2d"},
		{"... or choosing us-east-1a, us-east-1b, or us-east-1f.", "us-east-1a, us-east-1b, us-east-1f"},
		{"... or choosing us-west-2-lax-1a.", "us-west-2-lax-1a"},
		{"We currently do not have sufficient capacity.", ""},
	} {
		if got := iceCapacityHint(tc.msg); got != tc.want {
			t.Errorf("iceCapacityHint(%q) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}

// TestDescribeAttemptError_NoSubnet is #256 item 2: an attempt that skipped AZs
// for want of a default subnet reads "no-subnet: <azs>; ..." and keeps EC2's
// capacity hint inside the 256 bytes.
func TestDescribeAttemptError_NoSubnet(t *testing.T) {
	ice := &iceErr{msg: iceMsg256}
	wrapped := withNoSubnet(ice, []string{"us-west-2d"}, false)

	got := describeAttemptError(FailureCapacity, wrapped)
	want := "no-subnet: us-west-2d; capacity: InsufficientInstanceCapacity: EC2 suggests us-west-2a, us-west-2b, us-west-2d; We currently"
	if !strings.HasPrefix(got, want) {
		t.Errorf("last_error = %q\nwant prefix %q", got, want)
	}
	if len(got) > MaxLastErrorBytes {
		t.Errorf("last_error is %d bytes, over %d", len(got), MaxLastErrorBytes)
	}
	if ErrorClass(got) != ErrClassNoSubnet {
		t.Errorf("class = %q, want %q", ErrorClass(got), ErrClassNoSubnet)
	}

	// Without dropped AZs: just the hint, ahead of the message.
	if got := describeAttemptError(FailureCapacity, ice); !strings.HasPrefix(got, "capacity: InsufficientInstanceCapacity: EC2 suggests us-west-2a, us-west-2b, us-west-2d; ") {
		t.Errorf("plain ICE = %q", got)
	}

	// A terminal error the subnet didn't cause keeps its own class...
	term := withNoSubnet(&capErr{"InvalidAMIID.NotFound"}, []string{"us-west-2d"}, false)
	if got := describeAttemptError(FailureTerminal, term); ErrorClass(got) != ErrClassTerminal {
		t.Errorf("terminal with dropped AZs = %q, want class terminal", got)
	}
	// ...unless every candidate lacked a subnet (the sweep failed open).
	all := withNoSubnet(&capErr{"InvalidParameterValue"}, []string{"us-west-2d"}, true)
	if got := describeAttemptError(FailureTerminal, all); ErrorClass(got) != ErrClassNoSubnet {
		t.Errorf("all-no-subnet terminal = %q, want class no-subnet", got)
	}
	// A quota refusal keeps its class.
	quota := withNoSubnet(&capErr{"VcpuLimitExceeded"}, []string{"us-west-2d"}, true)
	if got := describeAttemptError(FailureTerminal, quota); ErrorClass(got) != ErrClassQuota {
		t.Errorf("quota with dropped AZs = %q, want class quota", got)
	}
}

// TestNoSubnetError_Transparent: the wrapper changes nothing the retry
// decision reads.
func TestNoSubnetError_Transparent(t *testing.T) {
	ice := &iceErr{msg: iceMsg256}
	w := withNoSubnet(ice, []string{"us-west-2d"}, false)
	if ClassifyFailure(w) != FailureCapacity {
		t.Errorf("ClassifyFailure(wrapped ICE) = %v, want capacity", ClassifyFailure(w))
	}
	var api smithy.APIError
	if !errors.As(w, &api) || api.ErrorCode() != "InsufficientInstanceCapacity" {
		t.Error("the AWS error is not reachable through the wrapper")
	}
	if !strings.Contains(w.Error(), "us-west-2d") {
		t.Errorf("Error() = %q, should name the skipped AZ", w.Error())
	}
	if withNoSubnet(ice, nil, false) != error(ice) {
		t.Error("no dropped AZs must return the error unchanged")
	}
	if withNoSubnet(nil, []string{"us-west-2d"}, false) != nil {
		t.Error("nil error must stay nil")
	}
}

// TestLaunchAcrossAZs_NoSubnet is the #256 incident: us-west-2d has capacity
// but no default subnet, so the sweep tries a/b/c, all ICE. The error must
// name 2d, and the cached subnet set must be dropped so the next cycle sees a
// subnet the user creates.
func TestLaunchAcrossAZs_NoSubnet(t *testing.T) {
	var tried []string
	forgot := ""
	sp := &Spawner{
		provision: func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			tried = append(tried, cfg.AvailabilityZone)
			return nil, &iceErr{msg: iceMsg256}
		},
		launchableAZs: func(context.Context, string) map[string]bool {
			return map[string]bool{"us-west-2a": true, "us-west-2b": true, "us-west-2c": true}
		},
		forgetLaunchableAZs: func(region string) { forgot = region },
	}
	_, err := sp.launchAcrossAZs(context.Background(), spawnaws.LaunchConfig{Region: "us-west-2"},
		[]string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"}, "", true)
	if !reflect.DeepEqual(tried, []string{"us-west-2a", "us-west-2b", "us-west-2c"}) {
		t.Errorf("tried %v, want a/b/c", tried)
	}
	var ns *NoSubnetError
	if !errors.As(err, &ns) || !reflect.DeepEqual(ns.AZs, []string{"us-west-2d"}) || ns.All {
		t.Fatalf("err = %v, want a NoSubnetError naming us-west-2d (not All)", err)
	}
	if forgot != "us-west-2" {
		t.Errorf("forgot %q, want the region's cached set dropped", forgot)
	}
	if ClassifyFailure(err) != FailureCapacity {
		t.Errorf("classify = %v, want capacity (still retried)", ClassifyFailure(err))
	}
	if got := describeAttemptError(ClassifyFailure(err), err); !strings.HasPrefix(got, "no-subnet: us-west-2d; capacity: ") {
		t.Errorf("last_error = %q", got)
	}
}

// TestLaunchAcrossAZs_AllNoSubnetFailsOpen: when no candidate has a default
// subnet the sweep still tries them (fail open, #150), and the error says
// every one was unlaunchable.
func TestLaunchAcrossAZs_AllNoSubnetFailsOpen(t *testing.T) {
	var tried []string
	sp := &Spawner{
		provision: func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			tried = append(tried, cfg.AvailabilityZone)
			return nil, &capErr{"InvalidParameterValue"}
		},
		launchableAZs: func(context.Context, string) map[string]bool { return map[string]bool{"us-west-2a": true} },
	}
	_, err := sp.launchAcrossAZs(context.Background(), spawnaws.LaunchConfig{Region: "us-west-2"}, []string{"us-west-2d"}, "", true)
	if !reflect.DeepEqual(tried, []string{"us-west-2d"}) {
		t.Errorf("tried %v, want the one candidate (fail open)", tried)
	}
	var ns *NoSubnetError
	if !errors.As(err, &ns) || !ns.All {
		t.Fatalf("err = %v, want a NoSubnetError with All", err)
	}
	if got := describeAttemptError(ClassifyFailure(err), err); ErrorClass(got) != ErrClassNoSubnet {
		t.Errorf("last_error = %q, want class no-subnet", got)
	}
}

// TestLaunchAcrossAZs_SuccessNotWrapped: a launch that lands is not an error,
// whatever was skipped.
func TestLaunchAcrossAZs_SuccessNotWrapped(t *testing.T) {
	sp := &Spawner{
		provision: func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			return &spawnaws.LaunchResult{InstanceID: "i-1", AvailabilityZone: cfg.AvailabilityZone}, nil
		},
		launchableAZs:       func(context.Context, string) map[string]bool { return map[string]bool{"us-west-2a": true} },
		forgetLaunchableAZs: func(string) { t.Error("forget called on a success") },
	}
	res, err := sp.launchAcrossAZs(context.Background(), spawnaws.LaunchConfig{Region: "us-west-2"}, []string{"us-west-2d", "us-west-2a"}, "", true)
	if err != nil || res.AZ != "us-west-2a" {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
}

// TestHold_NoSubnetErrors is #256 for holds: no candidate with a default
// subnet gives a NoSubnetError (class no-subnet), and a failed reservation
// that skipped AZs names them.
func TestHold_NoSubnetErrors(t *testing.T) {
	forgot := 0
	h := &Holder{
		createClient:        func(string) holderCreateAPI { return &fakeCreateEC2{} },
		launchableAZs:       func(context.Context, string) map[string]bool { return map[string]bool{"us-west-2a": true} },
		forgetLaunchableAZs: func(string) { forgot++ },
	}
	m := &MatchResult{InstanceType: "p5.4xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2d", CandidateAZs: []string{"us-west-2d"}}
	err := h.Hold(context.Background(), &Watch{WatchID: "w-256"}, m)
	var ns *NoSubnetError
	if !errors.As(err, &ns) || !ns.All || !reflect.DeepEqual(ns.AZs, []string{"us-west-2d"}) {
		t.Fatalf("err = %v, want NoSubnetError{us-west-2d, All}", err)
	}
	if got := describeAttemptError(ClassifyFailure(err), err); ErrorClass(got) != ErrClassNoSubnet {
		t.Errorf("last_error = %q, want class no-subnet", got)
	}
	if forgot != 1 {
		t.Errorf("forget called %d times, want 1", forgot)
	}

	// A capacity refusal in the AZ that has a subnet, with 2d skipped.
	h.createClient = func(string) holderCreateAPI { return iceCreateEC2{} }
	m = &MatchResult{InstanceType: "p5.4xlarge", Region: "us-west-2", CandidateAZs: []string{"us-west-2d", "us-west-2a"}}
	err = h.Hold(context.Background(), &Watch{WatchID: "w-256"}, m)
	if !errors.As(err, &ns) || ns.All || !reflect.DeepEqual(ns.AZs, []string{"us-west-2d"}) {
		t.Fatalf("err = %v, want NoSubnetError{us-west-2d}", err)
	}
	if ClassifyFailure(err) != FailureCapacity {
		t.Errorf("classify = %v, want capacity", ClassifyFailure(err))
	}
	if forgot != 2 {
		t.Errorf("forget called %d times, want 2", forgot)
	}

	// Nothing skipped: the error is unwrapped, and no forget.
	m = &MatchResult{InstanceType: "p5.4xlarge", Region: "us-west-2", CandidateAZs: []string{"us-west-2a"}}
	err = h.Hold(context.Background(), &Watch{WatchID: "w-256"}, m)
	if err == nil || errors.As(err, &ns) || forgot != 2 {
		t.Errorf("err = %v (forgot %d), want a plain error and no forget", err, forgot)
	}
}

type iceCreateEC2 struct{}

func (iceCreateEC2) CreateCapacityReservation(context.Context, *ec2.CreateCapacityReservationInput, ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error) {
	return nil, &iceErr{msg: iceMsg256}
}

// TestSpawnConfigHasNoSubnetKey pins the finding #256 relies on: lagotto's
// spawn config cannot name a subnet (every launch goes to the AZ's default
// subnet), so the coverage check has no explicit-subnet case to skip. If a
// subnet key is ever added, SubnetCoverageChecker.Check must skip watches
// that set it, and this test must change with it.
func TestSpawnConfigHasNoSubnetKey(t *testing.T) {
	typ := reflect.TypeOf(SpawnConfigFile{})
	for i := 0; i < typ.NumField(); i++ {
		if f := typ.Field(i); strings.Contains(strings.ToLower(f.Name+f.Tag.Get("json")), "subnet") {
			t.Errorf("SpawnConfigFile.%s carries a subnet: make SubnetCoverageChecker.Check skip watches that set it (#256)", f.Name)
		}
	}
	if _, err := ParseSpawnConfigYAML([]byte("instance_type: p5.4xlarge\nttl: 1h\nsubnet_id: subnet-0abc\n")); err == nil {
		t.Error("ParseSpawnConfigYAML accepted subnet_id; the coverage check must then skip watches that set it (#256)")
	}
}

func setOf(m map[string][]string) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}
