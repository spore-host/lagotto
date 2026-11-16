package inventory

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// Source reads what lagotto has in an account. AWSSource is the real one;
// tests fake it.
type Source interface {
	ArtifactBucketLister
	// EnabledRegions lists every region enabled for the account.
	EnabledRegions(ctx context.Context) ([]string, error)
	// Region reads one region.
	Region(region string) RegionSource
	// Account reads the account-wide (IAM) resources.
	Account() AccountSource
}

// Activity is what a region's tables say is in use.
type Activity struct {
	ActiveWatches int
	ArmedLaunches int
	// Recorded are the schedule names the scheduled-launch records hold.
	Recorded []string
}

// RegionSource reads one region. Each method returns nil (or empty) and a nil
// error for what is not there; an error means it could not be read.
type RegionSource interface {
	// Tables returns those of o's tables that exist, with their record
	// counts.
	Tables(ctx context.Context, o Options) ([]Table, error)
	Activity(ctx context.Context, watchesTable, scheduledTable string) (Activity, error)
	// Poller returns the poller Lambda and its execution role ARN.
	Poller(ctx context.Context) (*Item, string, error)
	Topic(ctx context.Context) (*Item, error)
	LogGroup(ctx context.Context) (*Item, error)
	// ScheduleGroups returns those of deploy.ScheduleGroups that exist.
	ScheduleGroups(ctx context.Context) ([]Item, error)
	// Schedules returns every schedule in lagotto's groups and every
	// lagotto-* one in the default group.
	Schedules(ctx context.Context) ([]Schedule, error)
	Holds(ctx context.Context) ([]Item, error)
	Jobs(ctx context.Context) ([]Item, error)
	LegacyStack(ctx context.Context, name string) (*Item, error)
	// BucketTags returns the tags of the region's default artifact bucket.
	BucketTags(ctx context.Context, region, accountID string) (map[string]string, error)
}

// AccountSource reads the account-wide resources.
type AccountSource interface {
	// Role returns the role, or nil when there is none.
	Role(ctx context.Context, name string) (*Item, error)
	// InstanceProfilesForRole returns every profile holding the role.
	InstanceProfilesForRole(ctx context.Context, role string) ([]Profile, error)
	// InstanceProfile returns the profile, or nil when there is none.
	InstanceProfile(ctx context.Context, name string) (*Profile, error)
	// AuthorizedRoles are the instance roles the poller policy authorizes.
	AuthorizedRoles(ctx context.Context) ([]string, error)
	// CLIPolicy returns lagotto-cli-user and how many principals it is
	// attached to, or nil when there is none.
	CLIPolicy(ctx context.Context) (*Item, int32, error)
}

// Scope is which regions Gather reads.
type Scope struct {
	// AllRegions reads every region enabled for the account. Otherwise
	// Regions are read.
	AllRegions bool
	Regions    []string
}

// maxParallelRegions bounds how many regions Gather reads at once.
const maxParallelRegions = 8

// Gather reads lagotto's footprint in the account: the regions of scope, then
// the artifact buckets of those regions, then the account-wide resources.
// What cannot be read is a warning in Facts; a region with any such warning is
// classed active throughout (Region.ActivityUnknown), and so are the
// account-wide resources when one of them could not be read. Only failing to
// list the regions is an error.
func Gather(ctx context.Context, src Source, accountID string, scope Scope, o Options) (Facts, error) {
	o = o.withDefaults()
	f := Facts{AccountID: accountID, AllRegions: scope.AllRegions}
	regions := scope.Regions
	if scope.AllRegions {
		var err error
		if regions, err = src.EnabledRegions(ctx); err != nil {
			return f, fmt.Errorf("list the account's regions: %w", err)
		}
	}
	if len(regions) == 0 {
		return f, fmt.Errorf("no region to read")
	}
	regions = append([]string(nil), regions...)
	sort.Strings(regions)

	f.Regions = make([]Region, len(regions))
	warnings := make([][]string, len(regions))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallelRegions)
	for i, region := range regions {
		wg.Add(1)
		go func(i int, region string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			f.Regions[i], warnings[i] = gatherRegion(ctx, src.Region(region), region, o)
		}(i, region)
	}
	wg.Wait()
	for _, w := range warnings {
		f.Warnings = append(f.Warnings, w...)
	}

	// The artifact buckets, with doctor's own reader (#264): one account-wide
	// listing, kept to the regions read.
	byRegion := map[string]*Region{}
	for i := range f.Regions {
		byRegion[f.Regions[i].Region] = &f.Regions[i]
	}
	buckets, err := GatherArtifactBuckets(ctx, accountID, regionFilter{src, byRegion}, factsProbe{src, byRegion}, true)
	if err != nil {
		f.Warnings = append(f.Warnings, fmt.Sprintf("could not list the artifact buckets (s3:ListAllMyBuckets): %v", err))
	}
	for _, b := range buckets {
		r := byRegion[b.Region]
		if b.Err != nil {
			f.Warnings = append(f.Warnings, fmt.Sprintf("%s: could not read artifact bucket %s: %v", b.Region, b.Name, b.Err))
			r.ActivityUnknown = true
			continue
		}
		r.Bucket = &Item{ARN: BucketARN(b.Name), Name: b.Name, Tags: b.Tags}
	}

	f.Account, f.AccountIncomplete, f.Warnings = gatherAccount(ctx, src.Account(), f.Warnings)
	return f, nil
}

// gatherRegion reads one region. Any failure makes the region's activity
// unknown, so nothing in it is classed removable on a partial read.
func gatherRegion(ctx context.Context, rs RegionSource, region string, o Options) (Region, []string) {
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
	var err error
	r.Tables, err = rs.Tables(ctx, o)
	fail("lagotto's tables", err)
	present := map[string]bool{}
	for _, t := range r.Tables {
		present[t.Name] = true
	}
	if len(r.Tables) > 0 || err != nil {
		watches, scheduled := o.WatchesTable, o.ScheduledTable
		if !present[watches] && err == nil {
			watches = ""
		}
		if !present[scheduled] && err == nil {
			scheduled = ""
		}
		act, aerr := rs.Activity(ctx, watches, scheduled)
		if !fail("the active watches and scheduled launches", aerr) {
			r.ActiveWatches, r.ArmedLaunches, r.RecordedSchedules = act.ActiveWatches, act.ArmedLaunches, act.Recorded
		}
	}
	r.Function, r.FunctionRole, err = rs.Poller(ctx)
	fail("the poller "+deploy.PollerFunctionName, err)
	r.Topic, err = rs.Topic(ctx)
	fail("the topic "+deploy.AlertsTopicName, err)
	r.LogGroup, err = rs.LogGroup(ctx)
	fail("the log group "+deploy.PollerLogGroupName, err)
	r.Groups, err = rs.ScheduleGroups(ctx)
	fail("the schedule groups", err)
	r.Schedules, err = rs.Schedules(ctx)
	fail("the schedules", err)
	r.Holds, err = rs.Holds(ctx)
	fail("the capacity reservations lagotto holds", err)
	r.Jobs, err = rs.Jobs(ctx)
	fail("the SageMaker training jobs", err)
	r.Stack, err = rs.LegacyStack(ctx, o.StackName)
	fail("the legacy CloudFormation stack "+o.StackName, err)
	return r, warnings
}

// gatherAccount reads the account-wide resources. incomplete reports that one
// could not be read.
func gatherAccount(ctx context.Context, as AccountSource, warnings []string) (Account, bool, []string) {
	var a Account
	incomplete := false
	fail := func(what string, err error) bool {
		if err == nil {
			return false
		}
		warnings = append(warnings, fmt.Sprintf("global: could not read %s: %v", what, err))
		incomplete = true
		return true
	}
	for _, name := range runtimeiam.LagottoRoles {
		role, err := as.Role(ctx, name)
		if !fail("IAM role "+name, err) && role != nil {
			a.Roles = append(a.Roles, *role)
		}
	}
	profiles, err := as.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName)
	if !fail("the instance profiles of "+runtimeiam.InstanceRoleName, err) {
		a.InstanceProfiles = profiles
	}
	authorized, err := as.AuthorizedRoles(ctx)
	if !fail("the authorized instance roles (the poller policy)", err) {
		for _, name := range authorized {
			if name == runtimeiam.InstanceRoleName {
				continue
			}
			role, err := as.Role(ctx, name)
			if !fail("authorized role "+name, err) && role != nil {
				a.AuthorizedRoles = append(a.AuthorizedRoles, *role)
			}
			p, err := as.InstanceProfile(ctx, name)
			if !fail("instance profile "+name, err) && p != nil {
				a.AuthorizedProfiles = append(a.AuthorizedProfiles, *p)
			}
		}
	}
	a.Policy, a.PolicyAttachments, err = as.CLIPolicy(ctx)
	fail("IAM policy lagotto-cli-user", err)
	return a, incomplete, warnings
}

// regionFilter lists the artifact buckets of the regions read only.
type regionFilter struct {
	ArtifactBucketLister
	read map[string]*Region
}

func (l regionFilter) ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error) {
	all, err := l.ArtifactBucketLister.ArtifactBucketRegions(ctx, accountID)
	var out []string
	for _, r := range all {
		if l.read[r] != nil {
			out = append(out, r)
		}
	}
	return out, err
}

// factsProbe answers the bucket probe from what the region read found.
type factsProbe struct {
	src  Source
	read map[string]*Region
}

func (p factsProbe) PollerExists(_ context.Context, region string) (bool, error) {
	return p.read[region].Function != nil, nil
}

func (p factsProbe) BucketTags(ctx context.Context, region, accountID string) (map[string]string, error) {
	return p.src.Region(region).BucketTags(ctx, region, accountID)
}

// ArtifactBucket is one default-named lagotto-lambda-<account>-<region>
// bucket and what is known about its region.
type ArtifactBucket struct {
	Region string
	Name   string
	// Poller is whether the poller Lambda exists in Region.
	Poller bool
	// Managed is whether the bucket carries lagotto's lagotto:managed=cli
	// tag, which is what lets `deploy --teardown` delete it, and Tags are all
	// of its tags. Read for an orphan, or for every bucket when asked.
	Managed bool
	Tags    map[string]string
	// Err is a failed poller or tag lookup; the bucket is then unclassified.
	Err error
}

// Orphan reports whether the bucket is in a region with no poller: left by a
// `deploy --teardown` from before #264, or by a deploy that never finished.
func (b ArtifactBucket) Orphan() bool { return b.Err == nil && !b.Poller }

// ArtifactBucketLister lists the regions of the account's default-named
// artifact buckets; *deploy.Deployer satisfies it.
type ArtifactBucketLister interface {
	ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error)
}

// ArtifactBucketProbe reports, for one region, whether the poller exists and
// the tags of that region's bucket.
type ArtifactBucketProbe interface {
	PollerExists(ctx context.Context, region string) (bool, error)
	BucketTags(ctx context.Context, region, accountID string) (map[string]string, error)
}

// GatherArtifactBuckets reads every default-named artifact bucket of the
// account and its region's poller. A bucket's tags are read when it is an
// orphan, or for every bucket with everyTag. It is the one reader of the
// buckets: `lagotto doctor`'s orphan-bucket check (#264) and the footprint
// both use it.
func GatherArtifactBuckets(ctx context.Context, accountID string, lister ArtifactBucketLister, probe ArtifactBucketProbe, everyTag bool) ([]ArtifactBucket, error) {
	regions, err := lister.ArtifactBucketRegions(ctx, accountID)
	var buckets []ArtifactBucket
	for _, r := range regions {
		b := ArtifactBucket{Region: r, Name: deploy.DefaultBucketName(accountID, r)}
		b.Poller, b.Err = probe.PollerExists(ctx, r)
		if b.Err == nil && (!b.Poller || everyTag) {
			b.Tags, b.Err = probe.BucketTags(ctx, r, accountID)
			b.Managed = b.Tags[deploy.ManagedTagKey] == deploy.ManagedTagValue
		}
		buckets = append(buckets, b)
	}
	return buckets, err
}
