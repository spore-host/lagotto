// Package inventory is lagotto's footprint (lagotto#325, part of
// leave-no-trace, spore-host/spore-host#752): every AWS resource lagotto has
// in an account, attributed and classified in the shared schema of
// github.com/spore-host/libs/footprint, which `lagotto footprint -o json`
// emits and `lagotto teardown --plan` executes.
//
// It is in three parts. [Gather] reads what is there through a [Source] into
// [Facts], and is the only part that calls AWS. [Classify] turns Facts into a
// footprint.Report without calling anything, so every classification rule is
// a pure function a test can pin. [Deleter] deletes a planned resource with
// lagotto's existing deleters, after re-checking it against a fresh
// footprint.
//
// # What it lists
//
// In each region: the three DynamoDB tables (with their record counts, and
// whether any watch is active or any scheduled launch armed), the artifact
// bucket, the alerts topic, the poller Lambda and its log group, the schedule
// groups "lagotto" and "lagotto-teardown" and their schedules, schedules named
// lagotto-* in the "default" group (from before lagotto#324), the capacity
// reservations a hold watch holds and the SageMaker training jobs running for
// a watch, and the legacy CloudFormation stack. Account-wide: the poller's two
// IAM roles, lagotto's instance role and its instance profiles, the profile
// `setup` created for each authorized instance role (and that role, as
// foreign), and the lagotto-cli-user policy with its attachment count.
//
// # Ownership
//
// A resource tagged spore-host:managed-by=lagotto is "tagged"; one that only
// has lagotto's name (made before lagotto#324 tagged everything) is
// "legacy-name", reported and never deleted; anything else listed (an
// administrator's authorized instance role) is "foreign". A schedule cannot
// be tagged, so it carries its group's tags: tagged in lagotto's groups,
// legacy-name in the default one.
//
// # Class
//
// A region is active when a watch in it is active, a scheduled launch is
// armed, a hold watch holds a capacity reservation, a training job runs, or
// its activity could not be read. Then:
//
//   - active: the tables, poller, topic and artifact bucket of an active
//     region; a per-launch schedule a scheduled-launch record names; the
//     poller's schedule of an active poller; a teardown job's schedule; a
//     group holding an active schedule; held reservations and running jobs;
//     the poller role while an active poller runs as it; the instance role,
//     its profiles and the authorized profiles while any region is active;
//     the lagotto-cli-user policy while anything is attached to it (an owner
//     decision: it is litter only at zero attachments).
//   - residue: the Scheduler invoke role, which every region's schedules
//     share, and a poller log group whose function is gone (Lambda recreated
//     it, lagotto#326).
//   - permanent: only what is tagged spore-host:permanent=true. lagotto owns
//     nothing permanent of its own.
//   - user-data: nothing. The match history is deleted with the teardown (an
//     owner decision, spore-host/spore-host#752); `lagotto history export` keeps it
//     first.
//   - litter: everything else.
//
// The account-wide resources serve every region, so they can be classified
// only from a footprint of every region (--all-regions). From fewer regions
// they are classed active, with a warning saying why.
//
// # Order
//
// DependsOn says what must outlive a resource: an instance profile depends on
// its role, a schedule on its group, the invoke role, its target and (a
// per-launch one) the scheduled-launches table, and the poller on its role,
// topic, tables and log group. The log groups are listed last, so a plan
// deletes each one after every function: deleted earlier, Lambda would
// recreate it on the next invocation.
package inventory

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Tool is the managed-by value lagotto's resources carry.
const Tool = tags.ToolLagotto

// Global is the Region of an account-wide (IAM) resource.
const Global = "global"

// Resource types, as CloudFormation type names.
const (
	TypeTable               = "AWS::DynamoDB::Table"
	TypeBucket              = "AWS::S3::Bucket"
	TypeTopic               = "AWS::SNS::Topic"
	TypeFunction            = "AWS::Lambda::Function"
	TypeLogGroup            = "AWS::Logs::LogGroup"
	TypeScheduleGroup       = "AWS::Scheduler::ScheduleGroup"
	TypeSchedule            = "AWS::Scheduler::Schedule"
	TypeCapacityReservation = "AWS::EC2::CapacityReservation"
	TypeTrainingJob         = "AWS::SageMaker::TrainingJob"
	TypeStack               = "AWS::CloudFormation::Stack"
	TypeRole                = "AWS::IAM::Role"
	TypeInstanceProfile     = "AWS::IAM::InstanceProfile"
	TypePolicy              = "AWS::IAM::ManagedPolicy"
)

// Options names what lagotto calls its resources where a flag can rename
// them. The zero value means the defaults.
type Options struct {
	WatchesTable, HistoryTable, ScheduledTable string
	// StackName is the legacy CloudFormation stack (default "lagotto").
	StackName string
}

func (o Options) withDefaults() Options {
	if o.WatchesTable == "" {
		o.WatchesTable = deploy.DefaultWatchesTable
	}
	if o.HistoryTable == "" {
		o.HistoryTable = deploy.DefaultHistoryTable
	}
	if o.ScheduledTable == "" {
		o.ScheduledTable = watcher.DefaultScheduledTable
	}
	if o.StackName == "" {
		o.StackName = "lagotto"
	}
	return o
}

// Tables are the three table names, in the order watches, history, scheduled.
func (o Options) Tables() []string {
	o = o.withDefaults()
	return []string{o.WatchesTable, o.HistoryTable, o.ScheduledTable}
}

// Patterns are lagotto's resource names, what footprint.Deletable requires a
// deleted resource's name to match. The capacity reservations, training jobs
// and the legacy stack have none: teardown never deletes them (`lagotto
// cancel` releases a hold; a stack still owning adopted resources is deleted
// by hand, see `lagotto doctor`).
func Patterns(o Options) []fp.NamePattern {
	o = o.withDefaults()
	alt := func(names ...string) string {
		q := make([]string, len(names))
		for i, n := range names {
			q[i] = regexp.QuoteMeta(n)
		}
		return strings.Join(q, "|")
	}
	return []fp.NamePattern{
		fp.MustPattern(TypeTable, alt(o.Tables()...)),
		fp.MustPattern(TypeBucket, regexp.QuoteMeta(strings.TrimSuffix(deploy.DefaultBucketName("", ""), "--"))+`-[0-9]{12}-[a-z0-9-]+`),
		fp.MustPattern(TypeTopic, alt(deploy.AlertsTopicName)),
		fp.MustPattern(TypeFunction, alt(deploy.PollerFunctionName)),
		fp.MustPattern(TypeLogGroup, alt(deploy.PollerLogGroupName)),
		fp.MustPattern(TypeScheduleGroup, alt(deploy.ScheduleGroups...)),
		fp.MustPattern(TypeSchedule, regexp.QuoteMeta(deploy.SchedulePrefix)+`[0-9A-Za-z_.-]+`),
		fp.MustPattern(TypeRole, alt(runtimeiam.LagottoRoles...)),
		// An authorized role's profile is named after the administrator's
		// role, so any name: its deleter also requires the
		// lagotto:managed=instance-profile tag setup puts on it.
		fp.MustPattern(TypeInstanceProfile, `[A-Za-z0-9+=,.@_-]{1,128}`),
		fp.MustPattern(TypePolicy, alt(cliiam.PolicyName)),
	}
}

// Item is one resource as read: its ARN, name and tags.
type Item struct {
	ARN  string
	Name string
	Tags map[string]string
}

// Table is one of lagotto's tables that exists.
type Table struct {
	Item
	// Records is how many items it holds (a Select=COUNT scan), not counting
	// the poller-control item of the watches table.
	Records int64
}

// Schedule is one EventBridge Scheduler schedule.
type Schedule struct {
	Group     string
	Name      string
	State     string
	TargetARN string
}

// Region is what one region holds. A nil pointer is a resource that is not
// there.
type Region struct {
	Region string
	Tables []Table
	// ActiveWatches and ArmedLaunches are the region's activity: active
	// watches, and scheduled launches that can still fire.
	ActiveWatches int
	ArmedLaunches int
	// ActivityUnknown is set when the activity could not be read. The region
	// is then treated as active.
	ActivityUnknown bool
	// RecordedSchedules are the schedule names the scheduled-launch records
	// hold.
	RecordedSchedules []string
	Bucket            *Item
	Topic             *Item
	Function          *Item
	// FunctionRole is the poller's execution role ARN.
	FunctionRole string
	LogGroup     *Item
	// Groups are the schedule groups of deploy.ScheduleGroups that exist.
	Groups    []Item
	Schedules []Schedule
	// Holds are the active or pending capacity reservations lagotto holds;
	// Jobs the training jobs in progress for a watch.
	Holds []Item
	Jobs  []Item
	Stack *Item
}

// Active reports whether anything lagotto manages in the region is in use: an
// active watch, a scheduled launch that can still fire (pending or retrying),
// a capacity reservation a hold watch holds, a training job running for a
// watch, or activity that could not be read. It is the one definition of a
// region in use: the footprint classes by it, and the poller's idle verdict
// (lagotto#326, [GatherActivity]) is it.
func (r *Region) Active() bool {
	return r.ActivityUnknown || r.ActiveWatches > 0 || r.ArmedLaunches > 0 || len(r.Holds) > 0 || len(r.Jobs) > 0
}

// ActiveReasons says why [Region.Active] is true, one phrase per signal, or
// nothing when the region is idle.
func (r *Region) ActiveReasons() []string {
	var out []string
	if r.ActivityUnknown {
		out = append(out, "activity could not be read")
	}
	if r.ActiveWatches > 0 {
		out = append(out, fmt.Sprintf("%d active watch(es)", r.ActiveWatches))
	}
	if r.ArmedLaunches > 0 {
		out = append(out, fmt.Sprintf("%d pending or retrying scheduled launch(es)", r.ArmedLaunches))
	}
	if len(r.Holds) > 0 {
		out = append(out, fmt.Sprintf("%d capacity reservation(s) held", len(r.Holds)))
	}
	if len(r.Jobs) > 0 {
		out = append(out, fmt.Sprintf("%d SageMaker training job(s) running", len(r.Jobs)))
	}
	return out
}

// Profile is an instance profile and the roles in it.
type Profile struct {
	Item
	Roles []string
}

// Account is what lagotto holds account-wide.
type Account struct {
	// Roles are lagotto's own roles (runtimeiam.LagottoRoles) that exist.
	Roles []Item
	// InstanceProfiles are the profiles holding lagotto's instance role.
	InstanceProfiles []Profile
	// AuthorizedRoles are the administrator's roles the poller policy
	// authorizes as instance roles, and AuthorizedProfiles the profile setup
	// created for each.
	AuthorizedRoles    []Item
	AuthorizedProfiles []Profile
	// Policy is lagotto-cli-user, attached to PolicyAttachments principals.
	Policy            *Item
	PolicyAttachments int32
}

// Facts are what Gather read.
type Facts struct {
	AccountID string
	// AllRegions is set when Regions covers every region enabled for the
	// account; the account-wide resources are classified only then.
	AllRegions bool
	Regions    []Region
	Account    Account
	// AccountIncomplete is set when an account-wide resource could not be
	// read; they are then all classed active.
	AccountIncomplete bool
	// Warnings are what could not be read.
	Warnings []string
}

// ScheduleARN is a schedule's ARN.
func ScheduleARN(region, accountID, group, name string) string {
	return fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule/%s/%s", region, accountID, group, name)
}

// ScheduleGroupARN is a schedule group's ARN.
func ScheduleGroupARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule-group/%s", region, accountID, name)
}

// TableARN is a table's ARN.
func TableARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:dynamodb:%s:%s:table/%s", region, accountID, name)
}

// BucketARN is a bucket's ARN.
func BucketARN(name string) string { return "arn:aws:s3:::" + name }

// IAMARN is the ARN of an IAM role, instance profile or policy (kind "role",
// "instance-profile", "policy") at the root path.
func IAMARN(accountID, kind, name string) string {
	return fmt.Sprintf("arn:aws:iam::%s:%s/%s", accountID, kind, name)
}

// LaunchScheduleRecorded reports whether a per-launch schedule belongs to a
// scheduled-launch record: it is a recorded schedule name, or one of the
// poller's boundary retries of one (<name>-r<n>, #62). One that does not is an
// orphan nothing lists or cancels. `lagotto doctor`'s launch-schedule check
// (#260) and the footprint both decide it here.
func LaunchScheduleRecorded(schedule string, recorded []string) bool {
	for _, r := range recorded {
		if r != "" && (schedule == r || isRetryOf(schedule, r)) {
			return true
		}
	}
	return false
}

func isRetryOf(schedule, recorded string) bool {
	n := len(recorded) + len("-r")
	if len(schedule) <= n || schedule[:n] != recorded+"-r" {
		return false
	}
	for _, c := range schedule[n:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ownership attributes a resource lagotto named.
func ownership(t map[string]string, lagottoNamed bool) fp.Ownership {
	switch {
	case tags.IsManagedBy(t, Tool):
		return fp.OwnershipTagged
	case lagottoNamed:
		return fp.OwnershipLegacyName
	}
	return fp.OwnershipForeign
}

// class applies the one rule above every other: anything tagged permanent is
// permanent.
func class(t map[string]string, c fp.Class) fp.Class {
	if tags.IsPermanent(t) {
		return fp.ClassPermanent
	}
	return c
}

func activeOr(active bool, otherwise fp.Class) fp.Class {
	if active {
		return fp.ClassActive
	}
	return otherwise
}

// Classify builds the footprint report for f: every resource attributed,
// classified and judged by footprint.Deletable, in plan input order (see the
// package documentation), stamped now.
func Classify(f Facts, o Options, now time.Time) *fp.Report {
	o = o.withDefaults()
	rep := fp.NewReport(Tool, f.AccountID, now)
	rep.Warnings = append(rep.Warnings, f.Warnings...)
	acct := f.AccountID
	invokeRole := runtimeiam.SchedulerInvokeRoleARN(acct)

	var regional, logGroups []fp.Resource
	add := func(dst *[]fp.Resource, region, typ string, it Item, own fp.Ownership, c fp.Class, deps ...string) {
		*dst = append(*dst, fp.Resource{
			ARN: it.ARN, Type: typ, Region: region, Scope: it.Tags[tags.Scope], Name: it.Name,
			Ownership: own, Class: class(it.Tags, c), DependsOn: compact(deps), Tags: it.Tags,
		})
	}

	anyActive := false
	pollerRoleActive := false
	for i := range f.Regions {
		r := &f.Regions[i]
		reg := r.Region
		active := r.Active()
		anyActive = anyActive || active
		if r.Function != nil && active {
			pollerRoleActive = true
		}

		var tableARNs []string
		scheduledARN := TableARN(reg, acct, o.ScheduledTable)
		for _, t := range r.Tables {
			add(&regional, reg, TypeTable, t.Item, ownership(t.Tags, true), activeOr(active, fp.ClassLitter))
			tableARNs = append(tableARNs, t.ARN)
			if t.Name == o.ScheduledTable {
				scheduledARN = t.ARN
			}
		}
		if r.Bucket != nil {
			add(&regional, reg, TypeBucket, *r.Bucket, ownership(r.Bucket.Tags, true), activeOr(active && r.Function != nil, fp.ClassLitter))
		}
		if r.Topic != nil {
			add(&regional, reg, TypeTopic, *r.Topic, ownership(r.Topic.Tags, true), activeOr(active, fp.ClassLitter))
		}
		fnClass := activeOr(active, fp.ClassLitter)
		fnARN := deploy.PollerFunctionARN(reg, acct)
		if r.Function != nil {
			fnARN = r.Function.ARN
			role := r.FunctionRole
			if role == "" {
				role = runtimeiam.RoleARN(acct)
			}
			deps := append([]string{role}, tableARNs...)
			if r.Topic != nil {
				deps = append(deps, r.Topic.ARN)
			}
			if r.LogGroup != nil {
				deps = append(deps, r.LogGroup.ARN)
			}
			add(&regional, reg, TypeFunction, *r.Function, ownership(r.Function.Tags, true), fnClass, deps...)
		}

		// Schedules first, so each group's class can follow theirs.
		groupTags := map[string]map[string]string{}
		groupARN := map[string]string{}
		for _, g := range r.Groups {
			groupTags[g.Name], groupARN[g.Name] = g.Tags, g.ARN
		}
		groupActive := map[string]bool{}
		for _, s := range r.Schedules {
			var c fp.Class
			var deps []string
			switch {
			case s.Group == runtimeiam.TeardownScheduleGroup:
				c = fp.ClassActive // a teardown job still to run
			case s.Name == deploy.PollerScheduleName:
				c = fp.ClassLitter // firing into a function that is gone
				if r.Function != nil {
					c = fnClass
				}
				deps = append(deps, fnARN)
			case deploy.IsLaunchSchedule(s.Name):
				c = activeOr(LaunchScheduleRecorded(s.Name, r.RecordedSchedules), fp.ClassLitter)
				deps = append(deps, scheduledARN)
			default:
				c = fp.ClassLitter
			}
			if s.TargetARN != "" && s.TargetARN != fnARN {
				deps = append(deps, s.TargetARN)
			}
			deps = append(deps, groupARN[s.Group], invokeRole)
			t := groupTags[s.Group] // a schedule carries its group's tags
			own := fp.OwnershipLegacyName
			if s.Group != schedgroup.Default {
				own = ownership(t, true)
			}
			if s.Group == schedgroup.Default {
				t = nil
			}
			c = class(t, c)
			groupActive[s.Group] = groupActive[s.Group] || c == fp.ClassActive
			add(&regional, reg, TypeSchedule, Item{ARN: ScheduleARN(reg, acct, s.Group, s.Name), Name: s.Name, Tags: copyTags(t)}, own, c, deps...)
		}
		for _, g := range r.Groups {
			add(&regional, reg, TypeScheduleGroup, g, ownership(g.Tags, true), activeOr(groupActive[g.Name], fp.ClassLitter))
		}
		for _, h := range r.Holds {
			add(&regional, reg, TypeCapacityReservation, h, ownership(h.Tags, true), fp.ClassActive)
		}
		for _, j := range r.Jobs {
			add(&regional, reg, TypeTrainingJob, j, ownership(j.Tags, true), fp.ClassActive)
		}
		if r.Stack != nil {
			add(&regional, reg, TypeStack, *r.Stack, ownership(r.Stack.Tags, true), fp.ClassLitter)
		}
		if r.LogGroup != nil {
			c := fp.ClassResidue // its function is gone: Lambda recreated it
			if r.Function != nil {
				c = fnClass
			}
			add(&logGroups, reg, TypeLogGroup, *r.LogGroup, ownership(r.LogGroup.Tags, true), c)
		}
	}

	// Account-wide resources serve every region: without all of them, they
	// are in use as far as this footprint can tell.
	known := f.AllRegions && !f.AccountIncomplete
	if !f.AllRegions && (len(f.Account.Roles) > 0 || len(f.Account.InstanceProfiles) > 0 || len(f.Account.AuthorizedProfiles) > 0) {
		rep.Warnings = append(rep.Warnings, "the account-wide IAM resources are classed active: they serve every region, "+
			"and this footprint did not read every region (use --all-regions)")
	}
	everywhereActive := anyActive || !known
	var global []fp.Resource
	for _, p := range f.Account.InstanceProfiles {
		add(&global, Global, TypeInstanceProfile, p.Item, ownership(p.Tags, true), activeOr(everywhereActive, fp.ClassLitter), roleARNs(acct, p.Roles)...)
	}
	for _, p := range f.Account.AuthorizedProfiles {
		add(&global, Global, TypeInstanceProfile, p.Item, ownership(p.Tags, false), activeOr(everywhereActive, fp.ClassLitter), roleARNs(acct, p.Roles)...)
	}
	for _, role := range f.Account.Roles {
		var c fp.Class
		switch role.Name {
		case runtimeiam.SchedulerInvokeRoleName:
			c = activeOr(!known, fp.ClassResidue)
		case runtimeiam.RoleName:
			c = activeOr(pollerRoleActive || !known, fp.ClassLitter)
		default:
			c = activeOr(everywhereActive, fp.ClassLitter)
		}
		add(&global, Global, TypeRole, role, ownership(role.Tags, true), c)
	}
	for _, role := range f.Account.AuthorizedRoles {
		add(&global, Global, TypeRole, role, fp.OwnershipForeign, activeOr(everywhereActive, fp.ClassLitter))
	}
	if p := f.Account.Policy; p != nil {
		add(&global, Global, TypePolicy, *p, ownership(p.Tags, true), activeOr(f.Account.PolicyAttachments > 0, fp.ClassLitter))
	}

	rep.Resources = append(append(append(rep.Resources, regional...), global...), logGroups...)
	patterns := Patterns(o)
	for i := range rep.Resources {
		rep.Resources[i].Deletable, rep.Resources[i].Reason = fp.Deletable(rep.Resources[i], Tool, patterns)
	}
	return rep
}

func roleARNs(acct string, names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = IAMARN(acct, "role", n)
	}
	return out
}

// compact drops empty and repeated ARNs, keeping the first of each.
func compact(arns []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, a := range arns {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

func copyTags(t map[string]string) map[string]string {
	if t == nil {
		return nil
	}
	out := make(map[string]string, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}

// Summary counts a report's resources by class, for a one-line summary.
func Summary(rep *fp.Report) map[fp.Class]int {
	out := map[fp.Class]int{}
	for _, r := range rep.Resources {
		out[r.Class]++
	}
	return out
}
