package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/spf13/cobra"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var (
	teardownForce    bool
	teardownIAMRoles bool
)

var teardownCmd = &cobra.Command{
	Use:         "teardown",
	Annotations: level(levelAdvanced),
	Short:       "Delete lagotto's DynamoDB tables and per-launch schedules",
	Long: `Delete the DynamoDB tables lagotto uses in one region (--region, else your
configured region): lagotto-watches, lagotto-match-history and
lagotto-scheduled-launches. 'lagotto deploy' creates all three with the poller;
'lagotto watch --local' creates the first two in a region with no poller (for a
local 'lagotto poll --daemon'). 'watch', 'launch' and 'setup' create none in a
region with no poller (lagotto#325).

Nothing deletes these tables automatically yet. A deployed poller's idle
teardown (lagotto#326) plans their removal once nothing lagotto manages has been
active in its region for 24 h, but in this release it only reports
(lagotto#328 turns deletion on), and once no watch is active the poller only
slows its schedule to once an hour. A poller whose function has
AUTO_DELETE_TABLES=true set by hand deletes them once they are empty, provided
'lagotto setup' has been re-run with a lagotto that includes lagotto#325, which
grants the poller dynamodb:DeleteTable (before that the poller was denied and
only logged a warning); 'lagotto deploy' never sets it. Until then, run this to
remove them. The match history is deleted with them: keep it with 'lagotto
history export' first.

By default it refuses to delete tables that still hold records, so you don't lose
match history or a pending scheduled launch; pass --force to delete regardless.
The scheduled-launches table is deleted only if it carries lagotto's
lagotto:managed=cli tag; an untagged table of that name is left in place and
reported. A table that does not exist is skipped.

Before deleting the tables it deletes the per-launch EventBridge Scheduler
schedules 'lagotto launch' creates (lagotto-launch-sl-*): every one a
scheduled-launch record names, and every one with that prefix in lagotto's
schedule group "lagotto" and in the "default" group (where schedules created
before lagotto#324 are), including the poller's retry schedules and any whose
record is gone. Then it deletes the "lagotto" schedule group if nothing is left
in it (a deployed poller's schedule keeps it). The scheduled-launches table is the only record of those schedules, and a
cron schedule never removes itself, so deleting the table first would leave it
firing indefinitely. A schedule that no longer exists is reported, not an error.
With --force this cancels any pending scheduled launch. If the schedules cannot
be listed or deleted (scheduler:ListSchedules, scheduler:DeleteSchedule,
scheduler:DeleteScheduleGroup), teardown stops before deleting any table.

--iam-roles also deletes the two IAM roles 'lagotto setup' and 'lagotto deploy'
create (lagotto-capacity-poller-role and lagotto-capacity-poller-scheduler-invoke),
with their policies, and the instance role 'lagotto setup' creates for launches
(lagotto-instance-role, with its instance profile), plus the instance profile setup
created for each role authorized with --instance-role (the role itself is yours and
is left in place). The roles are account-global and shared by the poller in every
region, so this first checks every region enabled in the account and refuses, before
deleting anything, while a poller still exists in any of them (remove each with
'lagotto deploy --teardown --region <region>') or when a region cannot be checked.
--force does not override that check.

--iam-roles also deletes the customer-managed policy lagotto-cli-user that
'lagotto setup' writes for the people who run lagotto (lagotto#232), once nothing
is attached to it: while any user, group or role has it, it is kept and reported
(detach it yourself first if you mean to remove it). A policy of that name without
lagotto's lagotto:managed=cli tag is left in place and reported.

The account-wide resources are removed by one code path, the same one the lazy
last mile uses (lagotto#340, #346): under lagotto's global teardown lease
(tool#lagotto#global in us-east-1's spore-host-lifecycle table), and only while
no region has a poller, a pending one-shot teardown job, or a lagotto lifecycle
item that is in use or held by a live teardown, all re-checked before each
delete. A deploy, watch or setup that starts meanwhile stops it before its next
delete. A failure leaves the global lease held: it goes stale in 20 minutes and
the next run takes it over and carries on.

Once the tables are gone, and no poller is left in the region, teardown also
finishes lagotto's item in that region's spore-host-lifecycle table (left
ACTIVE by deploy and watch), which would otherwise keep the account-wide
resources.

--plan <file|-> takes the other path (lagotto#325): it executes a footprint,
the JSON 'lagotto footprint -o json' writes, through the shared spore.host
teardown executor, and none of the above applies (--force and --iam-roles are
refused with it). It first takes the footprint again, of the plan's regions (of
every region when the plan names an account-wide resource), and keeps whatever
is active in either, and everything an active resource depends on: a pending
launch's schedule group, the role an active poller runs as. The rest is deleted
in dependency order (a profile before its role, a schedule before its group,
the poller before its role, topic and tables, each log group after every
function), each one re-checked first: one that is gone is skipped, and one
whose tags or class changed since the plan is refused. Only a resource tagged
spore-host:managed-by=lagotto, with lagotto's name, is ever deleted; never one
tagged spore-host:role=service or spore-host:permanent=true, a legacy-name or
foreign one, a capacity reservation, a training job or the legacy stack. When
one is not removed, nothing it depends on is tried.

--mode report, the default, deletes nothing and says what reap would delete.
--mode reap deletes and needs --yes. It first takes the teardown lease
(lagotto#326) in every region it deletes in, so another user's deploy, watch or
launch there waits for it, or stops it before its next delete. A region where
something is in use, or another teardown holds the lease, refuses the whole
run before anything is deleted. The plan's account-wide steps run last, through
the account-wide teardown described above for --iam-roles: while any region
still has a poller, a pending one-shot job or a live lifecycle item they are
kept, and reported as such. Use -o json for the outcome of every step.`,
	RunE: runTeardown,
}

func init() {
	rootCmd.AddCommand(teardownCmd)
	teardownCmd.Flags().BoolVar(&teardownForce, "force", false, "Delete even if the tables still contain records")
	teardownCmd.Flags().BoolVar(&teardownIAMRoles, "iam-roles", false,
		"Also delete lagotto's account-global IAM roles (the poller's two and its instance role) and the lagotto-cli-user policy (once nothing is attached to it), "+
			"under the global teardown lease; refused while a poller, a pending teardown job or a live lifecycle item exists in any region")
}

func runTeardown(cmd *cobra.Command, args []string) error {
	if err := checkTeardownFlags(cmd); err != nil {
		return err
	}
	if teardownPlan != "" {
		return runTeardownPlan(cmd)
	}
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := newTeardownStore(cfg)
	out := cmd.OutOrStdout()

	// The role check runs before anything is deleted, so a refusal leaves the
	// account exactly as it was.
	var acct string
	if teardownIAMRoles {
		if err := checkNoPollerAnywhere(ctx, cfg); err != nil {
			return err
		}
		// The CLI-user policy's ARN is account-scoped (#232).
		if acct, err = setupCallerAccount(ctx, cfg); err != nil {
			return fmt.Errorf("not deleting anything: resolve the account ID (the %s policy's ARN needs it): %w", cliiam.PolicyName, err)
		}
	}

	if !teardownForce {
		nonEmpty, err := store.NonEmptyTables(ctx)
		if err != nil {
			return fmt.Errorf("check tables: %w", err)
		}
		if len(nonEmpty) > 0 {
			return fmt.Errorf("table(s) still contain records: %s — "+
				"re-run with --force to delete anyway, or wait for them to age out via TTL", strings.Join(nonEmpty, ", "))
		}
	}

	// Schedules before tables (#260): the scheduled-launches table is the only
	// record of which schedules exist, so it must outlive their deletion.
	if err := deleteLaunchSchedules(ctx, store, newLaunchScheduleDeleter(cfg), out); err != nil {
		return err
	}

	res, err := store.DeleteTables(ctx)
	for _, name := range res.Deleted {
		_, _ = fmt.Fprintf(out, "Deleted table %s\n", name)
	}
	if err != nil {
		return fmt.Errorf("delete tables: %w", err)
	}
	if len(res.Absent) > 0 {
		_, _ = fmt.Fprintf(out, "Already absent: %s\n", strings.Join(res.Absent, ", "))
	}
	for _, name := range res.Unmanaged {
		_, _ = fmt.Fprintf(out, "Left table %s in place: it lacks the lagotto:managed=cli tag, so lagotto did not create it. "+
			"Delete it yourself if it is yours.\n", name)
	}
	// The region's lagotto lifecycle item: with its tables gone and no poller
	// left, nothing will ever carry it on, and while it is ACTIVE it keeps the
	// account-wide resources (lagotto#340).
	finishRegionItem(ctx, cfg, out)
	if teardownIAMRoles {
		// The one account-wide teardown (lagotto#340, #346): under the
		// global lease, and only while no region has a poller, a pending
		// one-shot job or a live lifecycle item, each re-checked before each
		// delete.
		r := teardownAccountWide(ctx, cfg, acct, nil, func(f string, a ...any) {
			if verbose {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "debug: "+f+"\n", a...)
			}
		})
		writeAccountWide(out, r)
		if err := accountWideError(r); err != nil {
			return err
		}
	}
	_, _ = fmt.Fprintln(out, "Teardown complete.")
	return nil
}

// teardownTables is the slice of the store teardown uses; *watcher.Store
// satisfies it.
type teardownTables interface {
	NonEmptyTables(ctx context.Context) ([]string, error)
	DeleteTables(ctx context.Context) (watcher.TableTeardown, error)
	ScheduledLaunchScheduleNames(ctx context.Context) ([]string, error)
}

// launchScheduleDeleter deletes the per-launch schedules; *deploy.Deployer
// satisfies it.
type launchScheduleDeleter interface {
	DeleteLaunchSchedules(ctx context.Context, recorded []string) (deploy.LaunchScheduleTeardown, error)
}

// newLaunchScheduleDeleter builds the scheduler teardown uses. A seam for tests.
var newLaunchScheduleDeleter = func(cfg aws.Config) launchScheduleDeleter { return deploy.New(cfg) }

// deleteLaunchSchedules deletes every per-launch schedule, those the
// scheduled-launch records name and those found by prefix, and reports each.
// Any failure is returned before a table is touched, so the records survive
// for a retry.
func deleteLaunchSchedules(ctx context.Context, store teardownTables, sched launchScheduleDeleter, out io.Writer) error {
	recorded, err := store.ScheduledLaunchScheduleNames(ctx)
	if err != nil {
		return fmt.Errorf("read the scheduled launches' schedule names (no table was deleted): %w", err)
	}
	res, err := sched.DeleteLaunchSchedules(ctx, recorded)
	for _, name := range res.Deleted {
		_, _ = fmt.Fprintf(out, "Deleted schedule %s\n", name)
	}
	if err != nil {
		return fmt.Errorf("delete per-launch %s* schedules (no table was deleted; "+
			"teardown needs scheduler:ListSchedules, scheduler:DeleteSchedule and scheduler:DeleteScheduleGroup): %w", deploy.LaunchSchedulePrefix, err)
	}
	if len(res.Absent) > 0 {
		_, _ = fmt.Fprintf(out, "Schedules already absent: %s\n", strings.Join(res.Absent, ", "))
	}
	if res.GroupDeleted {
		_, _ = fmt.Fprintf(out, "Deleted schedule group %s (nothing was left in it)\n", deploy.ScheduleGroupName)
	}
	return nil
}

// newTeardownStore builds the store teardown deletes from. A seam for tests.
var newTeardownStore = func(cfg aws.Config) teardownTables {
	return watcher.NewStore(cfg, watchesTable, historyTable)
}

// regionLister and functionGetter are the EC2 and Lambda calls the poller
// search makes; *ec2.Client and *lambda.Client satisfy them.
type regionLister interface {
	DescribeRegions(ctx context.Context, in *ec2.DescribeRegionsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error)
}

type functionGetter interface {
	GetFunction(ctx context.Context, in *lambda.GetFunctionInput, optFns ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error)
}

// findPollerRegions returns the regions of this account in which the poller
// function exists. A seam so tests run without AWS.
var findPollerRegions = func(ctx context.Context, cfg aws.Config) ([]string, error) {
	return pollerRegions(ctx, ec2.NewFromConfig(cfg), func(region string) functionGetter {
		return lambda.NewFromConfig(cfg, func(o *lambda.Options) { o.Region = region })
	})
}

// pollerRegions asks every region enabled for the account whether the poller
// function exists there. Without AllRegions, DescribeRegions returns only the
// enabled regions: the only ones a poller can be deployed in. A region that
// cannot be answered is an error naming it, never "no poller".
func pollerRegions(ctx context.Context, regions regionLister, lambdaIn func(region string) functionGetter) ([]string, error) {
	out, err := regions.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
	if err != nil {
		return nil, fmt.Errorf("list the account's regions: %w", err)
	}
	if len(out.Regions) == 0 {
		return nil, fmt.Errorf("list the account's regions: none returned")
	}
	var found, unchecked []string
	for _, r := range out.Regions {
		region := aws.ToString(r.RegionName)
		_, gerr := lambdaIn(region).GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(deploy.PollerFunctionName)})
		var notFound *lambdatypes.ResourceNotFoundException
		switch {
		case gerr == nil:
			found = append(found, region)
		case errors.As(gerr, &notFound):
		default:
			unchecked = append(unchecked, fmt.Sprintf("%s (%v)", region, gerr))
		}
	}
	if len(unchecked) > 0 {
		return found, fmt.Errorf("could not check for a poller in: %s", strings.Join(unchecked, "; "))
	}
	sort.Strings(found)
	return found, nil
}

// checkNoPollerAnywhere refuses `teardown --iam-roles` while any region runs a
// poller, or when it cannot tell: the roles are shared by every region's
// poller, so deleting them under a live one leaves it unable to run or be
// invoked, with nothing reporting it.
func checkNoPollerAnywhere(ctx context.Context, cfg aws.Config) error {
	regions, err := findPollerRegions(ctx, cfg)
	if err != nil {
		return fmt.Errorf("not deleting the IAM roles: %w. They are shared by the poller in every region, "+
			"so --iam-roles needs to see that none is left (lambda:GetFunction and ec2:DescribeRegions)", err)
	}
	if len(regions) > 0 {
		fixes := make([]string, len(regions))
		for i, r := range regions {
			fixes[i] = "lagotto deploy --teardown --region " + r
		}
		return fmt.Errorf("not deleting the IAM roles: the poller %s still exists in %s and runs as them. "+
			"Remove it first:\n  %s", deploy.PollerFunctionName, strings.Join(regions, ", "), strings.Join(fixes, "\n  "))
	}
	return nil
}

// teardownPollerExists reports whether the poller runs in cfg's region. A
// seam for tests.
var teardownPollerExists = func(ctx context.Context, cfg aws.Config) (bool, error) {
	info, err := deploy.New(cfg).InspectPoller(ctx)
	return info.Exists, err
}

// finishRegionItem finishes lagotto's lifecycle item in cfg's region (and
// closes the region's table if no tool is left in it) once `lagotto
// teardown` has deleted the tables, unless a poller is still there or it
// cannot tell. Anything that fails is a warning: the item only keeps the
// account-wide resources, and running teardown again finishes it.
func finishRegionItem(ctx context.Context, cfg aws.Config, out io.Writer) {
	warn := func(err error) {
		_, _ = fmt.Fprintf(out, "warning: lagotto's %s item in %s was left in place: %v; it keeps the account-wide resources until 'lagotto teardown' runs here again\n",
			lifecycle.TableName, cfg.Region, err)
	}
	if exists, err := teardownPollerExists(ctx, cfg); err != nil || exists {
		if err == nil {
			return // the poller carries it on
		}
		warn(fmt.Errorf("could not tell whether the poller is still deployed: %w", err))
		return
	}
	c, err := newLifecycleClient(cfg, runOwner(teardownOwner))
	if err != nil {
		warn(err)
		return
	}
	if _, ok, err := c.Get(ctx, inventory.Tool, cfg.Region); err != nil || !ok {
		if err != nil {
			warn(err)
		}
		return
	}
	l, err := claimRegion(ctx, c, cfg.Region, false)
	if err != nil {
		warn(err)
		return
	}
	if err := l.Finish(ctx); err != nil {
		warn(err)
		return
	}
	if _, err := c.CloseTableIfEmpty(ctx, cfg.Region); err != nil {
		_, _ = fmt.Fprintf(out, "warning: could not remove the empty %s table in %s: %v\n", lifecycle.TableName, cfg.Region, err)
	}
	_, _ = fmt.Fprintf(out, "Finished lagotto's %s item in %s\n", lifecycle.TableName, cfg.Region)
}
