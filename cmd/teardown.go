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
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var (
	teardownForce    bool
	teardownIAMRoles bool
)

var teardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Delete lagotto's DynamoDB tables and per-launch schedules",
	Long: `Delete the DynamoDB tables lagotto uses: lagotto-watches,
lagotto-match-history and lagotto-scheduled-launches (the last is created by
'lagotto deploy' or the first 'lagotto launch').

lagotto already tears these down automatically once there are no active watches
or pending scheduled launches and the tables have drained (records age out via
DynamoDB TTL). Use this to remove them explicitly.

By default it refuses to delete tables that still hold records, so you don't lose
match history or a pending scheduled launch; pass --force to delete regardless.
The scheduled-launches table is deleted only if it carries lagotto's
lagotto:managed=cli tag; an untagged table of that name is left in place and
reported. A table that does not exist is skipped.

Before deleting the tables it deletes the per-launch EventBridge Scheduler
schedules 'lagotto launch' creates (lagotto-launch-sl-*): every one a
scheduled-launch record names, and every one in the default schedule group with
that prefix, including the poller's retry schedules and any whose record is
gone. The scheduled-launches table is the only record of those schedules, and a
cron schedule never removes itself, so deleting the table first would leave it
firing indefinitely. A schedule that no longer exists is reported, not an error.
With --force this cancels any pending scheduled launch. If the schedules cannot
be listed or deleted (scheduler:ListSchedules, scheduler:DeleteSchedule),
teardown stops before deleting any table.

--iam-roles also deletes the two IAM roles 'lagotto setup' and 'lagotto deploy'
create (lagotto-capacity-poller-role and lagotto-capacity-poller-scheduler-invoke),
with their policies. The roles are account-global and shared by the poller in every
region, so this first checks every region enabled in the account and refuses, before
deleting anything, while a poller still exists in any of them (remove each with
'lagotto deploy --teardown --region <region>') or when a region cannot be checked.
--force does not override that check.`,
	RunE: runTeardown,
}

func init() {
	rootCmd.AddCommand(teardownCmd)
	teardownCmd.Flags().BoolVar(&teardownForce, "force", false, "Delete even if the tables still contain records")
	teardownCmd.Flags().BoolVar(&teardownIAMRoles, "iam-roles", false,
		"Also delete lagotto's two account-global IAM roles; refused while a poller exists in any region")
}

func runTeardown(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := newTeardownStore(cfg)
	out := cmd.OutOrStdout()

	// The role check runs before anything is deleted, so a refusal leaves the
	// account exactly as it was.
	if teardownIAMRoles {
		if err := checkNoPollerAnywhere(ctx, cfg); err != nil {
			return err
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
	if teardownIAMRoles {
		if err := deleteCLIOwnedRoles(ctx, newRoleDeleter(cfg), out); err != nil {
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
			"teardown needs scheduler:ListSchedules and scheduler:DeleteSchedule): %w", deploy.LaunchSchedulePrefix, err)
	}
	if len(res.Absent) > 0 {
		_, _ = fmt.Fprintf(out, "Schedules already absent: %s\n", strings.Join(res.Absent, ", "))
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

// newRoleDeleter builds the IAM client the role deletion uses. A seam for tests.
var newRoleDeleter = func(cfg aws.Config) runtimeiam.RoleDeleteAPI { return iam.NewFromConfig(cfg) }

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

// deleteCLIOwnedRoles deletes both CLI-owned roles and reports each one.
func deleteCLIOwnedRoles(ctx context.Context, client runtimeiam.RoleDeleteAPI, out io.Writer) error {
	deleted, err := runtimeiam.DeleteRoles(ctx, client)
	for _, name := range deleted {
		_, _ = fmt.Fprintf(out, "Deleted IAM role %s\n", name)
	}
	if err != nil {
		return fmt.Errorf("delete IAM roles: %w", err)
	}
	if len(deleted) == 0 {
		_, _ = fmt.Fprintf(out, "No lagotto IAM roles to delete (%s already absent).\n", strings.Join(runtimeiam.CLIOwnedRoles, ", "))
	}
	return nil
}
