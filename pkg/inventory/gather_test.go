package inventory

import (
	"context"
	"errors"
	"strings"
	"testing"

	fp "github.com/spore-host/libs/footprint"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// fakeSource answers Gather from a set of Regions and an Account.
type fakeSource struct {
	enabled    []string
	regions    map[string]Region
	account    Account
	authorized []string
	errs       map[string]error // "<region> <method>" or "global <method>"
	bucketsErr error
}

func (s *fakeSource) EnabledRegions(context.Context) ([]string, error) { return s.enabled, nil }

func (s *fakeSource) ArtifactBucketRegions(context.Context, string) ([]string, error) {
	var out []string
	for _, r := range s.enabled {
		if s.regions[r].Bucket != nil {
			out = append(out, r)
		}
	}
	return out, s.bucketsErr
}

func (s *fakeSource) Region(region string) RegionSource {
	r := s.regions[region]
	return &fakeRegionSource{r: r, err: func(m string) error { return s.errs[region+" "+m] }}
}

func (s *fakeSource) Account() AccountSource { return &fakeAccountSource{s} }

type fakeRegionSource struct {
	r   Region
	err func(string) error
}

func (f *fakeRegionSource) Tables(context.Context, Options) ([]Table, error) {
	return f.r.Tables, f.err("Tables")
}
func (f *fakeRegionSource) Activity(context.Context, string, string) (Activity, error) {
	return Activity{ActiveWatches: f.r.ActiveWatches, ArmedLaunches: f.r.ArmedLaunches, Recorded: f.r.RecordedSchedules}, f.err("Activity")
}
func (f *fakeRegionSource) Poller(context.Context) (*Item, string, error) {
	return f.r.Function, f.r.FunctionRole, f.err("Poller")
}
func (f *fakeRegionSource) Topic(context.Context) (*Item, error) { return f.r.Topic, f.err("Topic") }
func (f *fakeRegionSource) LogGroup(context.Context) (*Item, error) {
	return f.r.LogGroup, f.err("LogGroup")
}
func (f *fakeRegionSource) ScheduleGroups(context.Context) ([]Item, error) {
	return f.r.Groups, f.err("ScheduleGroups")
}
func (f *fakeRegionSource) Schedules(context.Context) ([]Schedule, error) {
	return f.r.Schedules, f.err("Schedules")
}
func (f *fakeRegionSource) Holds(context.Context) ([]Item, error) { return f.r.Holds, f.err("Holds") }
func (f *fakeRegionSource) Jobs(context.Context) ([]Item, error)  { return f.r.Jobs, f.err("Jobs") }
func (f *fakeRegionSource) LegacyStack(context.Context, string) (*Item, error) {
	return f.r.Stack, f.err("LegacyStack")
}
func (f *fakeRegionSource) BucketTags(context.Context, string, string) (map[string]string, error) {
	if f.r.Bucket == nil {
		return nil, errors.New("no bucket")
	}
	return f.r.Bucket.Tags, f.err("BucketTags")
}

type fakeAccountSource struct{ s *fakeSource }

func (a *fakeAccountSource) Role(_ context.Context, name string) (*Item, error) {
	for _, r := range append(append([]Item{}, a.s.account.Roles...), a.s.account.AuthorizedRoles...) {
		if r.Name == name {
			r := r
			return &r, a.s.errs["global Role "+name]
		}
	}
	return nil, a.s.errs["global Role "+name]
}
func (a *fakeAccountSource) InstanceProfilesForRole(context.Context, string) ([]Profile, error) {
	return a.s.account.InstanceProfiles, nil
}
func (a *fakeAccountSource) InstanceProfile(_ context.Context, name string) (*Profile, error) {
	for _, p := range a.s.account.AuthorizedProfiles {
		if p.Name == name {
			p := p
			return &p, nil
		}
	}
	return nil, nil
}
func (a *fakeAccountSource) AuthorizedRoles(context.Context) ([]string, error) {
	return a.s.authorized, nil
}
func (a *fakeAccountSource) CLIPolicy(context.Context) (*Item, int32, error) {
	return a.s.account.Policy, a.s.account.PolicyAttachments, nil
}

// realAccount is the shape of the #749 account: two regions with a poller
// (one of them active), and orphan tables in two regions with none.
func realAccount() *fakeSource {
	active := deployed("us-east-1")
	active.ActiveWatches = 3
	return &fakeSource{
		enabled: []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-2"},
		regions: map[string]Region{
			"us-east-1":      active,
			"us-west-2":      deployed("us-west-2"),
			"eu-west-1":      tablesOnly("eu-west-1"),
			"ap-southeast-2": tablesOnly("ap-southeast-2"),
		},
		account:    account(0),
		authorized: []string{runtimeiam.InstanceRoleName, "team-gpu"},
	}
}

// TestGatherAllRegionsOrphanTables: --all-regions reads every enabled region,
// and the tables of a region with no poller and nothing active are litter
// while the active region's are not; the artifact buckets are read by the
// reader doctor uses.
func TestGatherAllRegionsOrphanTables(t *testing.T) {
	src := realAccount()
	f, err := Gather(context.Background(), src, acct, Scope{AllRegions: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Regions) != 4 || f.Regions[0].Region != "ap-southeast-2" {
		t.Fatalf("regions %+v", f.Regions)
	}
	rep := Classify(f, Options{}, now)
	for _, region := range []string{"eu-west-1", "ap-southeast-2"} {
		for _, name := range (Options{}).Tables() {
			if r := wantClass(t, rep, TypeTable, region, name, fp.ClassLitter); !r.Deletable {
				t.Errorf("orphan %s in %s: %s", name, region, r.Reason)
			}
		}
	}
	wantClass(t, rep, TypeTable, "us-east-1", "lagotto-watches", fp.ClassActive)
	wantClass(t, rep, TypeTable, "us-west-2", "lagotto-watches", fp.ClassLitter)
	wantClass(t, rep, TypeBucket, "us-east-1", "lagotto-lambda-"+acct+"-us-east-1", fp.ClassActive)
	wantClass(t, rep, TypeBucket, "us-west-2", "lagotto-lambda-"+acct+"-us-west-2", fp.ClassLitter)
	wantClass(t, rep, TypeRole, Global, runtimeiam.RoleName, fp.ClassActive)
	wantClass(t, rep, TypePolicy, Global, cliiam.PolicyName, fp.ClassLitter)
	find(t, rep, TypeInstanceProfile, Global, "team-gpu")
	find(t, rep, TypeRole, Global, "team-gpu")
	if len(rep.Warnings) != 0 {
		t.Errorf("warnings %v", rep.Warnings)
	}
}

// TestGatherOneRegion reads only the region asked for, and its bucket only.
func TestGatherOneRegion(t *testing.T) {
	f, err := Gather(context.Background(), realAccount(), acct, Scope{Regions: []string{"us-west-2"}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Regions) != 1 || f.Regions[0].Bucket == nil || f.AllRegions {
		t.Fatalf("facts %+v", f)
	}
}

// TestGatherFailsClosed: a region where anything cannot be read is active
// throughout, and an unreadable account-wide resource makes them all active.
func TestGatherFailsClosed(t *testing.T) {
	src := realAccount()
	src.errs = map[string]error{"eu-west-1 Schedules": errors.New("AccessDenied"), "global Role team-gpu": errors.New("AccessDenied")}
	f, err := Gather(context.Background(), src, acct, Scope{AllRegions: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	rep := Classify(f, Options{}, now)
	wantClass(t, rep, TypeTable, "eu-west-1", "lagotto-watches", fp.ClassActive)
	wantClass(t, rep, TypeTable, "ap-southeast-2", "lagotto-watches", fp.ClassLitter)
	wantClass(t, rep, TypeRole, Global, runtimeiam.SchedulerInvokeRoleName, fp.ClassActive)
	joined := strings.Join(rep.Warnings, "\n")
	if !strings.Contains(joined, "eu-west-1: could not read the schedules") || !strings.Contains(joined, "global: could not read authorized role team-gpu") {
		t.Errorf("warnings:\n%s", joined)
	}
}

// TestGatherArtifactBucketsTagsOnlyOrphans: doctor's mode reads a bucket's
// tags only when it is an orphan; the footprint's reads every one.
func TestGatherArtifactBucketsTagsOnlyOrphans(t *testing.T) {
	src := realAccount()
	src.regions["eu-west-1"] = func() Region { r := src.regions["eu-west-1"]; r.Bucket = deployed("eu-west-1").Bucket; return r }()
	byRegion := map[string]*Region{}
	for _, name := range src.enabled {
		r := src.regions[name]
		byRegion[name] = &r
	}
	probe := &countingProbe{factsProbe: factsProbe{src, byRegion}}
	buckets, err := GatherArtifactBuckets(context.Background(), acct, src, probe, false)
	if err != nil || len(buckets) != 3 {
		t.Fatalf("buckets %+v, %v", buckets, err)
	}
	if probe.tagReads != 1 {
		t.Errorf("read the tags of %d buckets, want only the orphan's", probe.tagReads)
	}
	for _, b := range buckets {
		if b.Orphan() != (b.Region == "eu-west-1") || (b.Orphan() && !b.Managed) {
			t.Errorf("bucket %+v", b)
		}
	}
	probe.tagReads = 0
	if _, err := GatherArtifactBuckets(context.Background(), acct, src, probe, true); err != nil || probe.tagReads != 3 {
		t.Errorf("everyTag read %d, err %v", probe.tagReads, err)
	}
}

type countingProbe struct {
	factsProbe
	tagReads int
}

func (p *countingProbe) BucketTags(ctx context.Context, region, accountID string) (map[string]string, error) {
	p.tagReads++
	return p.factsProbe.BucketTags(ctx, region, accountID)
}
