package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var (
	deployStackName string
	deployRegion    string
	deployVersion   string
	deployEnv       string
	deployBucket    string
	deployTeardown  bool
	deployMigrate   bool
	deployYes       bool
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy the hosted capacity poller into your own AWS account",
	Long: `Stand up lagotto's hosted capacity poller (DynamoDB, SNS, Lambda, EventBridge
Scheduler) in your OWN AWS account, so watches are serviced server-side — armed
once, then hands-off — instead of depending on a foreground 'poll --daemon' that
dies when your laptop sleeps.

It downloads the published capacity-poller Lambda artifact for the given
--version, uploads it to a bucket in your account, and then creates or converges
the three poller resources with direct AWS API calls (#154): the SNS alerts
topic, the poller Lambda and its EventBridge schedule. Re-runs are incremental —
a --version bump is a fast code-only update — and a deploy never changes the
schedule's on/off state, so it can't switch off a running poller.

The poller schedule is created DISABLED; the first 'lagotto watch' enables it
(and the poller self-disables when no active watches remain).

CloudFormation is no longer used. The template is still shipped as an optional
declarative path for IaC/enterprise consumers — see DEPLOYMENT.md. If you have an
older CloudFormation-deployed stack, this command adopts its resources
automatically (they all have fixed names) and tells you how to detach the stack
with --migrate-from-cloudformation.

Use --teardown to delete the poller, topic and schedule.`,
	RunE: runDeploy,
}

func init() {
	rootCmd.AddCommand(deployCmd)
	f := deployCmd.Flags()
	// Rescoped by #154: deploy no longer creates a stack, so this names only the
	// LEGACY stack to detect (and optionally migrate away from).
	f.StringVar(&deployStackName, "stack-name", "lagotto", "Legacy CloudFormation stack to detect/migrate from (the poller's own resources have fixed names)")
	f.StringVar(&deployRegion, "region", "", "AWS region (default: from your AWS config)")
	f.StringVar(&deployVersion, "version", Version, "lagotto release version to pull the poller Lambda from")
	f.StringVar(&deployEnv, "environment", "production", "Environment tag (production, staging, development)")
	f.StringVar(&deployBucket, "lambda-bucket", "", "S3 bucket for the Lambda artifact (default: lagotto-lambda-<account>-<region>, created if absent)")
	f.BoolVar(&deployTeardown, "teardown", false, "Delete the poller, SNS topic and schedule instead of deploying them")
	f.BoolVar(&deployMigrate, "migrate-from-cloudformation", false, "Detach the legacy --stack-name stack from the poller resources (retain-update, verify, delete) before deploying")
	f.BoolVarP(&deployYes, "yes", "y", false, "Skip the confirmation prompt")
}

func runDeploy(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	out := cmd.OutOrStdout()

	if deployTeardown && deployMigrate {
		return fmt.Errorf("--teardown and --migrate-from-cloudformation are contradictory: " +
			"migration exists to keep the poller alive while detaching the stack, and teardown deletes the poller. " +
			"If you want both gone, run the teardown and then 'aws cloudformation delete-stack'")
	}

	// --region wins; otherwise the shared config's region (then ambient).
	cfg, err := awscfg.Load(ctx, deployRegion)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	region := cfg.Region
	if region == "" {
		return fmt.Errorf("no AWS region set; pass --region or configure one")
	}

	acctID := ""
	if id, ierr := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); ierr == nil && id.Account != nil {
		acctID = *id.Account
	}

	d := deploy.New(cfg)

	confirm := func(prompt string) bool {
		if deployYes {
			return true
		}
		fmt.Fprintf(os.Stderr, "%s [y/N] ", prompt)
		return readYes(os.Stdin)
	}

	// Every poller resource name is derived from the account ID, so nothing below
	// this point works without it.
	if acctID == "" {
		return fmt.Errorf("could not resolve AWS account ID (the poller's roles and ARNs are derived from it); check your credentials")
	}

	bucket := deployBucket
	if bucket == "" {
		bucket = deploy.DefaultBucketName(acctID, region)
	}

	if deployTeardown {
		// Say exactly what survives. Each of these has burned somebody: the tables
		// hold the watches, the IAM roles are shared with per-launch schedules
		// (#146), and the artifact bucket was never stack-owned so no previous
		// teardown removed it either.
		if !confirm(fmt.Sprintf(`Delete the lagotto poller in %s (account %s)?
This deletes the schedule %s, the Lambda %s and the SNS topic %s.
RETAINED: your DynamoDB tables (watches/history/scheduled — 'lagotto teardown' owns those),
RETAINED: the IAM roles %s and %s (CLI-owned, shared with per-launch scheduled launches),
RETAINED: the artifact bucket %s,
RETAINED: any per-launch 'lagotto-launch-sl-*' schedules.
Continue?`,
			region, acctID,
			deploy.PollerScheduleName, deploy.PollerFunctionName, deploy.AlertsTopicName,
			runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName,
			bucket)) {
			fmt.Fprintln(out, "Aborted.")
			return nil
		}
		fmt.Fprintf(os.Stderr, "Deleting the poller in %s / %s...\n", acctID, region)
		deleted, terr := d.Teardown(ctx, region, acctID)
		for _, what := range deleted {
			fmt.Fprintf(out, "Deleted %s\n", what)
		}
		if terr != nil {
			return terr
		}
		if len(deleted) == 0 {
			fmt.Fprintln(out, "Nothing to delete — the poller, topic and schedule are already gone.")
		}
		fmt.Fprintf(out, "Artifact bucket %s is retained; delete it manually if you want.\n", bucket)
		printLegacyStackNote(ctx, out, d, deploy.LegacyStackTeardownWarning(deployStackName))
		return nil
	}

	if deployVersion == "" || deployVersion == "dev" {
		return fmt.Errorf("--version is required (a released version like 0.44.0); the dev build has no published poller artifact")
	}

	if !confirm(fmt.Sprintf("Deploy the lagotto poller (v%s) into account %s / %s?", deployVersion, acctID, region)) {
		fmt.Fprintln(out, "Aborted.")
		return nil
	}

	// Migration runs BEFORE the deploy, deliberately. The retain-update carries the
	// stack's previous parameters across — including LambdaCodeKey — so a migration
	// run after a --version bump would push the OLD zip back onto the function. This
	// way the stack is detached first and the deploy below converges to --version.
	if deployMigrate {
		fmt.Fprintf(os.Stderr, "Detaching CloudFormation stack %s from the poller resources...\n", deployStackName)
		if merr := d.MigrateFromStack(ctx, deployStackName); merr != nil {
			return merr
		}
		fmt.Fprintf(out, "CloudFormation stack %s detached and deleted; the poller, topic and schedule were retained.\n", deployStackName)
	}

	// Ensure the CLI-owned DynamoDB tables exist before deploying (#59). The poller
	// is wired to them by name and never creates them, so deploying against an
	// account that already ran `lagotto watch`/`launch` (tables present) — or a
	// fresh account (tables absent) — both work. EnsureTables is idempotent.
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	if created, terr := store.EnsureTables(ctx); terr != nil {
		return fmt.Errorf("ensure watches/history tables: %w", terr)
	} else if len(created) > 0 {
		fmt.Fprintf(os.Stderr, "Created DynamoDB table(s): %v\n", created)
	}
	if name, terr := store.EnsureScheduledTable(ctx); terr != nil {
		return fmt.Errorf("ensure scheduled-launches table: %w", terr)
	} else if name != "" {
		fmt.Fprintf(os.Stderr, "Created DynamoDB table: %s\n", name)
	}

	// Ensure the CLI-owned IAM roles exist before deploying (#145/#146), exactly as
	// the tables above: the Lambda's execution role and the role EventBridge
	// Scheduler assumes to invoke it. Idempotent.
	if rerr := runtimeiam.EnsureRoles(ctx, iam.NewFromConfig(cfg), region, acctID); rerr != nil {
		return fmt.Errorf("ensure poller IAM roles: %w", rerr)
	}

	fmt.Fprintf(os.Stderr, "Deploying the poller (v%s) into %s / %s...\n", deployVersion, acctID, region)
	res, err := d.Deploy(ctx, deploy.Options{
		StackName:      deployStackName,
		Region:         region,
		Version:        deployVersion,
		Environment:    deployEnv,
		Bucket:         deployBucket,
		AccountID:      acctID,
		WatchesTable:   watchesTable,
		HistoryTable:   historyTable,
		ScheduledTable: store.ScheduledTable(),
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "\nPoller deployed into %s / %s.\n", acctID, region)
	keys := make([]string, 0, len(res.Outputs))
	for k := range res.Outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(out, "  %s: %s\n", k, res.Outputs[k])
	}
	if len(res.Actions) > 0 {
		fmt.Fprintln(out)
		for _, a := range res.Actions {
			fmt.Fprintf(out, "  %s\n", a)
		}
	}
	fmt.Fprintln(out, "\nArm a watch with 'lagotto watch …' — the poller schedule activates on the first watch.")

	// Legacy-stack detection runs LAST, after the deploy has succeeded, so a
	// DescribeStacks failure can never be what the user sees instead of a real
	// deploy error. Skipped when we just migrated the stack away.
	if !deployMigrate {
		printLegacyStackNote(ctx, out, d, deploy.LegacyStackAdoptionWarning(deployStackName, acctID, region))
	}
	return nil
}

// printLegacyStackNote prints note when a legacy CloudFormation stack is still
// present. Advisory only: a detection failure is reported as a one-line aside, not
// as a command failure, because by the time it runs the real work has succeeded.
func printLegacyStackNote(ctx context.Context, out io.Writer, d *deploy.Deployer, note string) {
	exists, _, err := d.LegacyStackState(ctx, deployStackName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Note: could not check for a legacy CloudFormation stack %q: %v\n", deployStackName, err)
		return
	}
	if !exists {
		return
	}
	fmt.Fprintf(out, "\n%s\n", note)
}
