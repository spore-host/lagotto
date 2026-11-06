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
	Short: "Delete lagotto's DynamoDB tables",
	Long: `Delete the DynamoDB tables lagotto uses (lagotto-watches and
lagotto-match-history by default).

lagotto already tears these down automatically once there are no active watches
and the tables have drained (watches and match history age out via DynamoDB TTL).
Use this to remove them explicitly.

By default it refuses to delete tables that still hold records, so you don't lose
match history; pass --force to delete regardless.

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

	store := watcher.NewStore(cfg, watchesTable, historyTable)
	out := cmd.OutOrStdout()

	// The role check runs before anything is deleted, so a refusal leaves the
	// account exactly as it was.
	if teardownIAMRoles {
		if err := checkNoPollerAnywhere(ctx, cfg); err != nil {
			return err
		}
	}

	if !teardownForce {
		empty, err := store.TablesEmpty(ctx)
		if err != nil {
			return fmt.Errorf("check tables: %w", err)
		}
		if !empty {
			return fmt.Errorf("tables still contain records (watches and/or match history) — " +
				"re-run with --force to delete anyway, or wait for them to age out via TTL")
		}
	}

	deleted, err := store.DeleteTables(ctx)
	if err != nil {
		return fmt.Errorf("delete tables: %w", err)
	}
	if len(deleted) == 0 {
		_, _ = fmt.Fprintf(out, "No lagotto tables to delete (%s, %s already absent).\n", watchesTable, historyTable)
	}
	for _, name := range deleted {
		_, _ = fmt.Fprintf(out, "Deleted table %s\n", name)
	}
	if teardownIAMRoles {
		if err := deleteCLIOwnedRoles(ctx, newRoleDeleter(cfg), out); err != nil {
			return err
		}
	}
	_, _ = fmt.Fprintln(out, "Teardown complete.")
	return nil
}

// findPollerRegions returns the regions of this account in which the poller
// function exists. A seam so tests run without AWS.
var findPollerRegions = func(ctx context.Context, cfg aws.Config) ([]string, error) {
	// Without AllRegions, DescribeRegions returns the regions enabled for the
	// account: the only ones a poller can be deployed in.
	out, err := ec2.NewFromConfig(cfg).DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
	if err != nil {
		return nil, fmt.Errorf("list the account's regions: %w", err)
	}
	var found, unchecked []string
	for _, r := range out.Regions {
		region := aws.ToString(r.RegionName)
		_, gerr := lambda.NewFromConfig(cfg, func(o *lambda.Options) { o.Region = region }).
			GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(deploy.PollerFunctionName)})
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
