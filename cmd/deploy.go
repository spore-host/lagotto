package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/managedtags"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/libs/tags"
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
	Use:         "deploy",
	Annotations: level(levelAdvanced),
	Short:       "Deploy the hosted capacity poller into your own AWS account",
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

The poller schedule is created DISABLED; the first 'lagotto watch' enables it,
every 5 minutes. When no active watches or pending scheduled launches remain,
the poller slows it to once an hour (about 720 invocations a month) rather than
disabling it, and arming a watch sets it back to every 5 minutes (lagotto#323).

CloudFormation is no longer used. The template is still shipped as an optional
declarative path for IaC/enterprise consumers — see DEPLOYMENT.md. If you have an
older CloudFormation-deployed stack, this command adopts its resources
automatically (they all have fixed names) and tells you how to detach the stack
with --migrate-from-cloudformation.

Deploy also creates the poller's CloudWatch Logs log group
/aws/lambda/lagotto-capacity-poller before the function, with a 30-day retention
and the lagotto:managed=cli tag, so Lambda never creates an unowned one. A group
that already exists is tagged, and given the retention only if it has none.

Everything deploy creates is tagged spore-host:managed-by=lagotto (with
spore-host:scope, :created, :schema, :component and :owner, your aws:userid),
and resources an older lagotto created without those tags gain them. Schedules
cannot be tagged, so the poller schedule lives in the tagged EventBridge
Scheduler schedule group "lagotto", which deploy creates. A schedule an older
lagotto left in the "default" group is moved: created in "lagotto", then deleted
from "default". It stays in "default" (deploy says why) while a legacy
CloudFormation stack owns it or the IAM policies predate the group: run 'lagotto
setup' and then deploy again.

Use --teardown to delete the poller's schedule (in either group), Lambda, log
group and SNS topic in this region, then the schedule group once nothing is left
in it, and then the default artifact bucket lagotto-lambda-<account>-<region> with every object version in
it. The bucket is deleted only when this account owns it and it carries the
lagotto:managed=cli tag, which deploy writes when it creates the bucket and
back-fills on an existing default-named bucket. A --lambda-bucket you supplied
is never tagged or deleted.`,
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
	f.BoolVar(&deployTeardown, "teardown", false, "Delete the poller, its log group, SNS topic, schedule and lagotto-managed artifact bucket instead of deploying them")
	f.BoolVar(&deployMigrate, "migrate-from-cloudformation", false, "Detach the legacy --stack-name stack from every resource it owns (retain-only change set, verify, delete) before deploying")
	f.BoolVarP(&deployYes, "yes", "y", false, "Skip the confirmation prompt")
}

func runDeploy(cmd *cobra.Command, args []string) error {
	ctx, stop := commandContext(cmd)
	defer stop()
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

	acctID, owner, callerARN := "", "", ""
	if id, ierr := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); ierr == nil && id.Account != nil {
		acctID = *id.Account
		// spore-host:owner on what this deploy creates (#324).
		owner = aws.ToString(id.UserId)
		callerARN = aws.ToString(id.Arn)
	}
	stamp := managedtags.Stamp{Owner: owner}

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

	if deployTeardown {
		// Say exactly what survives. Each of these has burned somebody: the tables
		// hold the watches, the IAM roles are shared with per-launch schedules
		// (#146) and with the poller in every OTHER region of the account (#218 —
		// teardown is per-region, so it never touches them). The artifact bucket
		// is deleted only when lagotto manages it (#264).
		if !confirm(deployTeardownPrompt(region, acctID, deployBucket)) {
			_, _ = fmt.Fprintln(out, "Aborted.")
			return nil
		}
		fmt.Fprintf(os.Stderr, "Deleting the poller in %s / %s...\n", acctID, region)
		deleted, terr := d.Teardown(ctx, region, acctID)
		for _, what := range deleted {
			_, _ = fmt.Fprintf(out, "Deleted %s\n", what)
		}
		if terr != nil {
			return terr
		}
		if len(deleted) == 0 {
			_, _ = fmt.Fprintln(out, "Nothing to delete — the poller, log group, topic and schedule are already gone.")
		}
		// The artifact bucket goes last (#264), and only after the poller is gone:
		// DeleteArtifactBucket deletes the DEFAULT bucket alone, and only when this
		// account owns it and it carries lagotto's managed tag.
		bt, berr := teardownArtifactBucket(ctx, d, region, acctID, deployBucket)
		reportArtifactBucketTeardown(out, bt, deployBucket, region)
		_, _ = fmt.Fprintf(out, "DynamoDB tables %s are retained; 'lagotto teardown' deletes them.\n", strings.Join(cliTables(), ", "))
		if berr != nil {
			return berr
		}
		printLegacyStackNote(ctx, out, d, deploy.LegacyStackTeardownWarning(deployStackName))
		return nil
	}

	if deployVersion == "" || deployVersion == "dev" {
		return fmt.Errorf("--version is required (a released version like 0.44.0); the dev build has no published poller artifact")
	}

	if !confirm(fmt.Sprintf("Deploy the lagotto poller (v%s) into account %s / %s?", deployVersion, acctID, region)) {
		_, _ = fmt.Fprintln(out, "Aborted.")
		return nil
	}

	// Mark lagotto active in the region before anything is created or
	// changed (lagotto#326): a teardown holding the lease there is waited
	// out, and refuses the deploy if it does not finish.
	markDone, err := markCreating(ctx, cfg, callerARN, region, "deploy the poller", creatorDeploy, os.Stderr)
	if err != nil {
		return err
	}
	defer markDone()
	// The region's pending teardown jobs are settled on every path, under
	// the marks (lagotto#340): a deploy that fails before its schedule stands
	// leaves them to finish the region's teardown.
	settled := false
	defer func() {
		if !settled {
			sctx, cancel := freshCtx(ctx) // also after an interrupt
			defer cancel()
			if serr := settleDeployJobs(sctx, cfg, region, false, os.Stderr); serr != nil {
				fmt.Fprintf(os.Stderr, "warning: %v\n", serr)
			}
		}
	}()

	// Migration runs BEFORE the deploy, deliberately. The retain-update carries the
	// stack's previous parameters across — including LambdaCodeKey — so a migration
	// run after a --version bump would push the OLD zip back onto the function. This
	// way the stack is detached first and the deploy below converges to --version.
	if deployMigrate {
		fmt.Fprintf(os.Stderr, "Detaching CloudFormation stack %s from the poller resources...\n", deployStackName)
		report, merr := d.MigrateFromStack(ctx, deployStackName, iam.NewFromConfig(cfg))
		if merr != nil {
			return merr
		}
		_, _ = fmt.Fprintf(out, "CloudFormation stack %s detached and deleted. Retained, and verified unchanged:\n", deployStackName)
		for _, r := range report.Retained {
			_, _ = fmt.Fprintf(out, "  %-28s %-26s %s\n", r.LogicalID, r.Type, r.PhysicalID)
		}
	}

	// Ensure the CLI-owned DynamoDB tables exist before deploying (#59). The poller
	// is wired to them by name and never creates them, so deploying against an
	// account that already ran `lagotto watch`/`launch` (tables present) — or a
	// fresh account (tables absent) — both work. EnsureTables is idempotent.
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	store.SetTagStamp(stamp)
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
	// Tables an older lagotto created gain the spore.host tags (#324).
	if tagged, terr := store.EnsureTableTags(ctx); terr != nil {
		return fmt.Errorf("tag DynamoDB tables: %w", terr)
	} else if len(tagged) > 0 {
		fmt.Fprintf(os.Stderr, "Tagged DynamoDB table(s) %s=%s: %v\n", tags.ManagedBy, tags.ToolLagotto, tagged)
	}

	// Ensure the CLI-owned IAM roles exist before deploying (#145/#146), exactly as
	// the tables above: the Lambda's execution role and the role EventBridge
	// Scheduler assumes to invoke it. Idempotent.
	iamClient := iam.NewFromConfig(cfg)
	if rerr := runtimeiam.EnsureRoles(ctx, iamClient, region, acctID, stamp); rerr != nil {
		return fmt.Errorf("ensure poller IAM roles: %w", rerr)
	}

	// Whether the poller schedule may move into lagotto's schedule group yet
	// (#324): not while a legacy stack owns it, nor before the IAM policies
	// grant the group.
	hold := scheduleMoveHold(ctx, d, iamClient, acctID, deployStackName)
	if hold != "" {
		fmt.Fprintf(os.Stderr, "Keeping the poller schedule in the default schedule group: %s.\n", hold)
	}

	fmt.Fprintf(os.Stderr, "Deploying the poller (v%s) into %s / %s...\n", deployVersion, acctID, region)
	res, err := d.Deploy(ctx, deploy.Options{
		StackName:        deployStackName,
		Region:           region,
		Version:          deployVersion,
		Environment:      deployEnv,
		Bucket:           deployBucket,
		AccountID:        acctID,
		WatchesTable:     watchesTable,
		HistoryTable:     historyTable,
		ScheduledTable:   store.ScheduledTable(),
		Owner:            owner,
		HoldScheduleMove: hold,
	})
	if err != nil {
		return err
	}
	settled = true
	if err := settleDeployJobs(ctx, cfg, region, true, os.Stderr); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(out, "\nPoller deployed into %s / %s.\n", acctID, region)
	keys := make([]string, 0, len(res.Outputs))
	for k := range res.Outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(out, "  %s: %s\n", k, res.Outputs[k])
	}
	if len(res.Actions) > 0 {
		_, _ = fmt.Fprintln(out)
		for _, a := range res.Actions {
			_, _ = fmt.Fprintf(out, "  %s\n", a)
		}
	}
	_, _ = fmt.Fprintln(out, "\nArm a watch with 'lagotto watch …' — the poller schedule activates on the first watch.")

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
	_, _ = fmt.Fprintf(out, "\n%s\n", note)
}

// cliTables names the three DynamoDB tables `lagotto deploy` creates and
// `lagotto teardown` deletes, in the order watches, history, scheduled launches.
func cliTables() []string {
	return []string{watchesTable, historyTable, watcher.DefaultScheduledTable}
}

// artifactBucketDeleter is the part of *deploy.Deployer deploy --teardown uses
// for the artifact bucket.
type artifactBucketDeleter interface {
	DeleteArtifactBucket(ctx context.Context, region, accountID string) (deploy.BucketTeardown, error)
}

// teardownArtifactBucket deletes the default artifact bucket when lagotto
// manages it (#264). A --lambda-bucket spelled exactly like the default still
// counts as user-supplied, so then nothing is deleted.
func teardownArtifactBucket(ctx context.Context, d artifactBucketDeleter, region, acctID, userBucket string) (deploy.BucketTeardown, error) {
	if userBucket == deploy.DefaultBucketName(acctID, region) {
		return deploy.BucketTeardown{}, nil
	}
	return d.DeleteArtifactBucket(ctx, region, acctID)
}

// reportArtifactBucketTeardown says what happened to the artifact bucket (#264),
// and names a user-supplied --lambda-bucket as retained.
func reportArtifactBucketTeardown(out io.Writer, bt deploy.BucketTeardown, userBucket, region string) {
	switch bt.Outcome {
	case deploy.BucketDeleted:
		_, _ = fmt.Fprintf(out, "Deleted artifact bucket %s (%d object version(s) removed first)\n", bt.Bucket, bt.Versions)
	case deploy.BucketRetainedUntagged:
		_, _ = fmt.Fprintf(out, "Artifact bucket %s is retained: it has no %s=%s tag, so lagotto cannot tell it created it. "+
			"'lagotto deploy --region %s' tags a default-named bucket this account owns; then re-run the teardown, or delete it by hand "+
			"(every object version, then the bucket).\n", bt.Bucket, deploy.ManagedTagKey, deploy.ManagedTagValue, region)
	}
	if userBucket != "" {
		_, _ = fmt.Fprintf(out, "Artifact bucket %s is retained: you supplied it with --lambda-bucket, and lagotto never deletes those.\n", userBucket)
	}
}

// deployTeardownPrompt is the `deploy --teardown` confirmation. It says exactly
// what survives, naming each table so the user knows what 'lagotto teardown'
// will remove (#250).
//
// The per-launch schedules are retained, not deleted (#260): each one fires a
// scheduled launch recorded in the retained scheduled-launches table, so they
// live and die with that table. Deleting them here would leave those records
// "pending" with nothing to fire them, and the poller keeps the account's infra
// alive while a launch is pending. A redeployed poller fires them again.
// 'lagotto teardown' deletes the schedules and the table together.
//
// The artifact bucket (#264): the default one is deleted when lagotto manages
// it; a user-supplied --lambda-bucket (userBucket) is named as retained.
func deployTeardownPrompt(region, acctID, userBucket string) string {
	def := deploy.DefaultBucketName(acctID, region)
	bucketLine := fmt.Sprintf(",\nthen the artifact bucket %s and every object version in it, IF this account owns it and it is tagged %s=%s (an untagged one is retained).\n",
		def, deploy.ManagedTagKey, deploy.ManagedTagValue)
	retainedUser := ""
	if userBucket != "" {
		retainedUser = fmt.Sprintf("RETAINED: your --lambda-bucket %s (lagotto never deletes a bucket you supplied),\n", userBucket)
		if userBucket == def {
			bucketLine = ".\n"
		}
	}
	return fmt.Sprintf(`Delete the lagotto poller in %s (account %s)?
This deletes the schedule %s, the Lambda %s, its log group %s, the SNS topic %s and the schedule group %s if nothing is left in it%s`+`RETAINED: the DynamoDB tables %s ('lagotto teardown' deletes those),
RETAINED: the IAM roles %s and %s (CLI-owned, IAM-global: shared by every region's poller and by per-launch scheduled launches),
%sRETAINED: any per-launch 'lagotto-launch-sl-*' schedules: they belong to the scheduled launches in those tables ('lagotto teardown' deletes both).
Continue?`,
		region, acctID,
		deploy.PollerScheduleName, deploy.PollerFunctionName, deploy.PollerLogGroupName, deploy.AlertsTopicName, deploy.ScheduleGroupName, bucketLine,
		strings.Join(cliTables(), ", "),
		runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName,
		retainedUser)
}

// scheduleMoveHold says why `lagotto deploy` must keep the poller schedule in
// the default schedule group rather than move it into lagotto's tagged group
// (#324), or "" when it may move it. Every doubt holds it: a moved schedule
// the poller's role cannot read or update would freeze the poller's cadence,
// and one the lagotto-cli-user policy does not cover would make 'lagotto watch'
// refuse to arm watches.
//
//   - A legacy CloudFormation stack still exists: it owns the schedule in the
//     default group, and moving it would delete the stack's resource.
//   - The poller's runtime policy, or the lagotto-cli-user policy, predates the
//     group: re-running 'lagotto setup' rewrites both. An absent policy is no
//     reason to hold (setup writes the current one when it runs).
func scheduleMoveHold(ctx context.Context, d legacyStackChecker, client scheduleMoveIAM, accountID, stackName string) string {
	if stackName != "" {
		exists, _, err := d.LegacyStackState(ctx, stackName)
		switch {
		case err != nil:
			return fmt.Sprintf("could not check for the legacy CloudFormation stack %s (%v)", stackName, err)
		case exists:
			return fmt.Sprintf("the legacy CloudFormation stack %s owns it; detach the stack with 'lagotto deploy --migrate-from-cloudformation', then deploy again", stackName)
		}
	}
	setupFix := "run 'lagotto setup', then 'lagotto deploy' again"
	doc, err := runtimeiam.ReadRuntimePolicy(ctx, client)
	switch {
	case errors.Is(err, runtimeiam.ErrRoleNotFound), errors.Is(err, runtimeiam.ErrPolicyNotFound):
	case err != nil:
		return fmt.Sprintf("could not read the poller's runtime policy (%v)", err)
	case !grantsScheduleGroup(doc):
		return "the poller's runtime IAM policy does not grant schedule group " + deploy.ScheduleGroupName + " yet; " + setupFix
	}
	doc, found, err := cliiam.ReadPolicy(ctx, client, accountID)
	switch {
	case errors.Is(err, cliiam.ErrUnmanagedPolicy), err == nil && !found:
	case err != nil:
		return fmt.Sprintf("could not read the %s policy (%v)", cliiam.PolicyName, err)
	case !grantsScheduleGroup(doc):
		return "the " + cliiam.PolicyName + " IAM policy does not grant schedule group " + deploy.ScheduleGroupName + " yet; " + setupFix
	}
	return ""
}

// scheduleMoveIAM is the IAM scheduleMoveHold reads both policies with.
// *iam.Client satisfies it.
type scheduleMoveIAM interface {
	runtimeiam.IAMAPI
	cliiam.PolicyAPI
}

// legacyStackChecker is the part of *deploy.Deployer scheduleMoveHold uses.
type legacyStackChecker interface {
	LegacyStackState(ctx context.Context, stackName string) (bool, string, error)
}

// grantsScheduleGroup reports whether a policy document lets its holder update
// the poller schedule in lagotto's schedule group (#324).
func grantsScheduleGroup(doc string) bool {
	grants, err := runtimeiam.ParsePolicy(doc)
	if err != nil {
		return false
	}
	want := ":schedule/" + deploy.ScheduleGroupName + "/" + deploy.PollerScheduleName
	for _, g := range grants {
		if strings.EqualFold(g.Effect, "Allow") && g.Action == "scheduler:UpdateSchedule" && strings.HasSuffix(g.Resource, want) {
			return true
		}
	}
	return false
}
