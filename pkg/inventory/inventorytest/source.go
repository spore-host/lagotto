// Package inventorytest is an in-memory inventory.Source for tests that drive
// lagotto's footprint, its idle verdict and its idle teardown (lagotto#326)
// without AWS.
package inventorytest

import (
	"context"
	"errors"
	"sync"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
	"github.com/spore-host/libs/tags"
)

// Source answers inventory.Gather from Regions and Account. It is safe for
// concurrent use, and a test may change what it answers between reads with
// Update.
type Source struct {
	mu      sync.Mutex
	enabled []string
	regions map[string]inventory.Region
	account inventory.Account
	// authorized are the instance roles the poller policy authorizes.
	authorized []string
	errs       map[string]error // "<region> <Method>" or "global <Method>"
	reads      map[string]int   // "<region> <Method>" -> calls
}

// New returns a Source with these regions enabled and holding nothing.
func New(enabled ...string) *Source {
	return &Source{enabled: enabled, regions: map[string]inventory.Region{}, errs: map[string]error{}, reads: map[string]int{}}
}

// SetRegion makes region hold r.
func (s *Source) SetRegion(r inventory.Region) *Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regions[r.Region] = r
	return s
}

// SetAccount sets the account-wide resources.
func (s *Source) SetAccount(a inventory.Account, authorized ...string) *Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.account, s.authorized = a, authorized
	return s
}

// Update changes region's resources in place.
func (s *Source) Update(region string, f func(r *inventory.Region)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.regions[region]
	r.Region = region
	f(&r)
	s.regions[region] = r
}

// Fail makes a read fail: key is "<region> <Method>" ("us-east-1 Topic") or
// "global <Method>" ("global CLIPolicy"). A nil err clears it.
func (s *Source) Fail(key string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.errs, key)
		return
	}
	s.errs[key] = err
}

// Reads reports how many times a read ran ("<region> <Method>").
func (s *Source) Reads(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[key]
}

func (s *Source) read(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads[key]++
	return s.errs[key]
}

func (s *Source) region(name string) inventory.Region {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.regions[name]
}

// EnabledRegions lists the enabled regions.
func (s *Source) EnabledRegions(context.Context) ([]string, error) {
	if err := s.read("global EnabledRegions"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.enabled...), nil
}

// ArtifactBucketRegions lists the regions holding an artifact bucket.
func (s *Source) ArtifactBucketRegions(context.Context, string) ([]string, error) {
	err := s.read("global ArtifactBucketRegions")
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.enabled {
		if s.regions[r].Bucket != nil {
			out = append(out, r)
		}
	}
	return out, err
}

// Region reads one region.
func (s *Source) Region(region string) inventory.RegionSource {
	return &regionSource{s: s, name: region}
}

// Account reads the account-wide resources.
func (s *Source) Account() inventory.AccountSource { return &accountSource{s} }

type regionSource struct {
	s    *Source
	name string
}

func (f *regionSource) r() inventory.Region { return f.s.region(f.name) }

func (f *regionSource) err(m string) error { return f.s.read(f.name + " " + m) }

func (f *regionSource) Tables(context.Context, inventory.Options) ([]inventory.Table, error) {
	err := f.err("Tables")
	return f.r().Tables, err
}

func (f *regionSource) Activity(context.Context, string, string) (inventory.Activity, error) {
	err := f.err("Activity")
	r := f.r()
	return inventory.Activity{ActiveWatches: r.ActiveWatches, ArmedLaunches: r.ArmedLaunches, Recorded: r.RecordedSchedules}, err
}

func (f *regionSource) Poller(context.Context) (*inventory.Item, string, error) {
	err := f.err("Poller")
	r := f.r()
	return r.Function, r.FunctionRole, err
}

func (f *regionSource) Topic(context.Context) (*inventory.Item, error) {
	err := f.err("Topic")
	return f.r().Topic, err
}

func (f *regionSource) LogGroup(context.Context) (*inventory.Item, error) {
	err := f.err("LogGroup")
	return f.r().LogGroup, err
}

func (f *regionSource) ScheduleGroups(context.Context) ([]inventory.Item, error) {
	err := f.err("ScheduleGroups")
	return f.r().Groups, err
}

func (f *regionSource) Schedules(context.Context) ([]inventory.Schedule, error) {
	err := f.err("Schedules")
	return f.r().Schedules, err
}

func (f *regionSource) Holds(context.Context) ([]inventory.Item, error) {
	err := f.err("Holds")
	return f.r().Holds, err
}

func (f *regionSource) Jobs(context.Context) ([]inventory.Item, error) {
	err := f.err("Jobs")
	return f.r().Jobs, err
}

func (f *regionSource) LegacyStack(context.Context, string) (*inventory.Item, error) {
	err := f.err("LegacyStack")
	return f.r().Stack, err
}

func (f *regionSource) BucketTags(context.Context, string, string) (map[string]string, error) {
	err := f.err("BucketTags")
	r := f.r()
	if r.Bucket == nil {
		return nil, errors.New("no bucket")
	}
	return r.Bucket.Tags, err
}

type accountSource struct{ s *Source }

func (a *accountSource) acct() (inventory.Account, []string) {
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	return a.s.account, a.s.authorized
}

func (a *accountSource) Role(_ context.Context, name string) (*inventory.Item, error) {
	err := a.s.read("global Role " + name)
	acct, _ := a.acct()
	for _, r := range append(append([]inventory.Item{}, acct.Roles...), acct.AuthorizedRoles...) {
		if r.Name == name {
			r := r
			return &r, err
		}
	}
	return nil, err
}

func (a *accountSource) InstanceProfilesForRole(context.Context, string) ([]inventory.Profile, error) {
	err := a.s.read("global InstanceProfilesForRole")
	acct, _ := a.acct()
	return acct.InstanceProfiles, err
}

func (a *accountSource) InstanceProfile(_ context.Context, name string) (*inventory.Profile, error) {
	err := a.s.read("global InstanceProfile " + name)
	acct, _ := a.acct()
	for _, p := range acct.AuthorizedProfiles {
		if p.Name == name {
			p := p
			return &p, err
		}
	}
	return nil, err
}

func (a *accountSource) AuthorizedRoles(context.Context) ([]string, error) {
	err := a.s.read("global AuthorizedRoles")
	_, authorized := a.acct()
	return authorized, err
}

func (a *accountSource) CLIPolicy(context.Context) (*inventory.Item, int32, error) {
	err := a.s.read("global CLIPolicy")
	acct, _ := a.acct()
	return acct.Policy, acct.PolicyAttachments, err
}

// Tags are the tags lagotto puts on a resource in scope (a region, or
// "global").
func Tags(scope string) map[string]string {
	return map[string]string{tags.ManagedBy: string(tags.ToolLagotto), tags.Scope: scope}
}

func with(t map[string]string, k, v string) map[string]string {
	out := map[string]string{k: v}
	for kk, vv := range t {
		out[kk] = vv
	}
	return out
}

// Deployed is a region where `lagotto deploy` ran and nothing is active: the
// three tables, the artifact bucket, the topic, the poller and its log group,
// lagotto's schedule group and the poller's schedule, all tagged.
func Deployed(region, acct string) inventory.Region {
	t := Tags(region)
	return inventory.Region{
		Region: region,
		Tables: []inventory.Table{
			{Item: inventory.Item{ARN: inventory.TableARN(region, acct, "lagotto-watches"), Name: "lagotto-watches", Tags: with(t, "lagotto:managed", "cli")}},
			{Item: inventory.Item{ARN: inventory.TableARN(region, acct, "lagotto-match-history"), Name: "lagotto-match-history", Tags: with(t, "lagotto:managed", "cli")}, Records: 12},
			{Item: inventory.Item{ARN: inventory.TableARN(region, acct, "lagotto-scheduled-launches"), Name: "lagotto-scheduled-launches", Tags: with(t, "lagotto:managed", "cli")}},
		},
		Bucket:       &inventory.Item{ARN: inventory.BucketARN(deploy.DefaultBucketName(acct, region)), Name: deploy.DefaultBucketName(acct, region), Tags: with(t, "lagotto:managed", "cli")},
		Topic:        &inventory.Item{ARN: deploy.AlertsTopicARN(region, acct), Name: deploy.AlertsTopicName, Tags: t},
		Function:     &inventory.Item{ARN: deploy.PollerFunctionARN(region, acct), Name: deploy.PollerFunctionName, Tags: t},
		FunctionRole: runtimeiam.RoleARN(acct),
		LogGroup:     &inventory.Item{ARN: deploy.PollerLogGroupARN(region, acct), Name: deploy.PollerLogGroupName, Tags: with(t, "lagotto:managed", "cli")},
		Groups:       []inventory.Item{{ARN: inventory.ScheduleGroupARN(region, acct, schedgroup.Name), Name: schedgroup.Name, Tags: t}},
		Schedules: []inventory.Schedule{{Group: schedgroup.Name, Name: deploy.PollerScheduleName, State: "ENABLED",
			TargetARN: deploy.PollerFunctionARN(region, acct)}},
	}
}

// Account is the account-wide resources `lagotto setup` and `deploy` create:
// the poller's two roles, lagotto's instance role and its profile, and the
// lagotto-cli-user policy with no attachment.
func Account(acct string) inventory.Account {
	g := Tags(inventory.Global)
	return inventory.Account{
		Roles: []inventory.Item{
			{ARN: runtimeiam.RoleARN(acct), Name: runtimeiam.RoleName, Tags: g},
			{ARN: runtimeiam.SchedulerInvokeRoleARN(acct), Name: runtimeiam.SchedulerInvokeRoleName, Tags: g},
			{ARN: inventory.IAMARN(acct, "role", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: g},
		},
		InstanceProfiles: []inventory.Profile{{
			Item:  inventory.Item{ARN: inventory.IAMARN(acct, "instance-profile", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: with(g, "lagotto:managed", "instance-profile")},
			Roles: []string{runtimeiam.InstanceRoleName},
		}},
		Policy: &inventory.Item{ARN: inventory.IAMARN(acct, "policy", "lagotto-cli-user"), Name: "lagotto-cli-user", Tags: with(g, "lagotto:managed", "cli")},
	}
}
