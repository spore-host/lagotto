package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/libs/i18n"
)

var doctorStackName string

var doctorCmd = &cobra.Command{
	Use:         "doctor",
	Annotations: level(levelAdvanced),
	Short:       "Check the deployed hosted poller against what this lagotto expects",
	Long: `Report drift between the hosted capacity poller deployed in your AWS account and
what THIS lagotto binary expects. Read-only: it makes no change of any kind, and
there is no --fix.

The headline check is the poller's runtime IAM policy. 'lagotto setup' applies it
with PutRolePolicy, which REPLACES the policy wholesale with whatever grants are
compiled into the binary that ran it. So upgrading lagotto without re-running
'setup' leaves the poller on the OLD permissions — and running an OLDER lagotto's
'setup' reverts newer ones. Neither produces any signal until a watch fails, and
for the hosted poller that failure is only visible in the poller's CloudWatch
Logs. That has bitten four times (lagotto#149, #150, #151, #153).

doctor also reports a poller Lambda older than this CLI, missing tables/roles/
poller/topic/schedule, a schedule that is switched off while watches are active
(the silent-death case: nothing is polling them), and a leftover CloudFormation
stack.

It reports email delivery for --notify email: channels: an address on an active
watch whose subscription is still pending confirmation (so receives nothing yet) or
missing, a subscription no active watch needs, and any email subscription on the
alerts topic with no filter policy, which receives every user's email events.
'lagotto notify-gc' removes unused subscriptions, and with --resubscribe
subscribes missing ones.

For each active spawn or hold watch it checks default-subnet coverage
(lagotto#256): the poller launches into the default VPC with no explicit subnet,
so an AZ that offers the watch's type but has no default subnet is skipped, and
the watch can never land there. Each such AZ is a warning, with the
'aws ec2 create-default-subnet' command that fixes it.

It checks every active spawn watch and armed scheduled launch (lagotto#309)
against the rules the poller applies before it launches, and FAILS for each
one it will refuse when it fires: a spawn config armed with iam_policy,
iam_policy_file or iam_policy_document (refused since 0.70.0), an iam_role no
rule allows, or one the deployed poller is not authorized to use. Each comes
with the fix: authorize a role ('lagotto setup --instance-role'), name it as
iam_role, then 'lagotto update <id> --spawn-config <file>'. Scheduled launches
are listed with a Scan, so administrator credentials; without them the check
is a warning.

It lists every active --action notify watch as a warning (lagotto#283): notify
is deprecated and will be removed in a later release, because it reports that a
type is offered at or under the max price, not that there is capacity. Use
--action hold or --action spawn instead. The check is omitted when there are
none.

It checks that the shared EventBridge Scheduler invoke role's policy allows
invoking THIS region's poller (lagotto#218: the role is IAM-global, and a deploy
by lagotto older than v0.65.1 scoped it to the deploying region alone, so every
other region's schedule failed silently). The policy is evaluated locally.

It lists every lagotto-lambda-<account>-<region> artifact bucket in the account
(lagotto#264) and warns about each one in a region with no poller, an orphan a
'deploy --teardown' from before #264 left behind, with the command that deletes
it. That needs s3:ListAllMyBuckets; without it the check is a warning.

It checks YOUR credentials (lagotto#232): the IAM policy simulator
(iam:SimulatePrincipalPolicy) is asked whether the identity running doctor may
call every action, on every resource, of the lagotto-cli-user policy 'lagotto
setup' writes, and it lists what is denied with the command that attaches the
policy. An assumed role is simulated as its IAM role, not the STS session. When
the simulation cannot be made (no iam:SimulatePrincipalPolicy, the root user)
the result is reported as unknown. Every outcome is a warning at most: the
simulator is a model (it ignores session policies), so a denial is a lead.

Best effort, it also reports EventBridge Scheduler target errors in the last 24h
(CloudWatch AWS/Scheduler TargetErrorCount for the poller's schedule group). That
needs cloudwatch:GetMetricStatistics on your credentials; without it the check
is a warning, never a failure.

Exit code is non-zero if any check FAILS, so it can gate CI. Warnings do not
affect it. Use -o json for the machine-readable form, including the exact
missing/extra IAM grants.`,
	Args:         cobra.NoArgs,
	RunE:         runDoctor,
	SilenceUsage: true, // a failed check is a finding, not a usage error
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().StringVar(&doctorStackName, "stack-name", "lagotto",
		"Legacy CloudFormation stack to look for (same meaning as on 'lagotto deploy')")
}

func runDoctor(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// The account ID scopes almost every ARN in the expected policy, so without it
	// the comparison would report false drift on every resource-scoped grant.
	// Better to refuse than to produce a report that's confidently wrong.
	acct, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("resolve AWS account ID (every expected ARN is derived from it): %w", err)
	}
	if cfg.Region == "" {
		return fmt.Errorf("no AWS region set; pass --region or configure one (the poller resources checked are per-region)")
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	state := doctor.Gather(ctx, doctor.Options{
		Region:          cfg.Region,
		AccountID:       *acct.Account,
		CLIVersion:      Version,
		LegacyStackName: doctorStackName,
		Tables:          []string{watchesTable, historyTable, store.ScheduledTable()},
		IAM:             iam.NewFromConfig(cfg),
		DynamoDB:        dynamodb.NewFromConfig(cfg),
		Infra:           deploy.New(cfg),
		Metrics:         cloudwatch.NewFromConfig(cfg),
		EmailAudit: func(ctx context.Context) (watcher.EmailAudit, error) {
			topicArn := emailTopicByName(os.Getenv, cfg.Region, *acct.Account)
			watches, lErr := store.ListActiveWatches(ctx)
			if lErr != nil {
				return watcher.EmailAudit{TopicArn: topicArn}, lErr
			}
			return newEmailSubscriber(cfg, topicArn).Audit(ctx, watches)
		},
		SubnetCoverage: func(ctx context.Context) ([]*watcher.SubnetCoverage, error) {
			watches, lErr := store.ListActiveWatches(ctx)
			if lErr != nil {
				return nil, lErr
			}
			return activeSubnetCoverage(ctx, watcher.NewSubnetCoverageChecker(cfg), watches), nil
		},
		ActiveWatches: func(ctx context.Context) (int, error) {
			watches, lErr := store.ListActiveWatches(ctx)
			if lErr != nil {
				return 0, lErr
			}
			return len(watches), nil
		},
	})
	report := doctor.Evaluate(state)
	appendLaunchScheduleCheck(ctx, report, cfg.Region, deploy.New(cfg), store.ScheduledLaunchScheduleNames)
	appendNotifyWatchCheck(ctx, report, store.ListActiveWatches)
	appendLaunchConfigCheck(ctx, report, store.ListActiveWatches, store.ListScheduledLaunches,
		newRoleCheck(ctx, iam.NewFromConfig(cfg), *acct.Account))
	appendArtifactBucketCheck(ctx, report, *acct.Account, deploy.New(cfg), regionalArtifactProbe{cfg})
	appendCallerCheck(ctx, report, iam.NewFromConfig(cfg), aws.ToString(acct.Arn), *acct.Account, cfg.Region)

	if err := writeDoctorReport(cmd.OutOrStdout(), report, getOutputFormat()); err != nil {
		return err
	}
	return doctorExitError(report)
}

// appendNotifyWatchCheck adds doctor's deprecated notify-watch check (#283)
// to report, when there is an active notify watch. A failed read adds nothing:
// the poller schedule check reports it.
func appendNotifyWatchCheck(ctx context.Context, report *doctor.Report, list func(context.Context) ([]watcher.Watch, error)) {
	watches, err := list(ctx)
	if err != nil {
		return
	}
	if c, ok := doctor.EvalNotifyWatches(watches); ok {
		report.Checks = append(report.Checks, c)
	}
}

// appendLaunchConfigCheck adds doctor's stored-launch-config check (#309):
// every active spawn watch and armed scheduled launch in the region whose
// config the poller will refuse when it fires.
func appendLaunchConfigCheck(ctx context.Context, report *doctor.Report, listWatches func(context.Context) ([]watcher.Watch, error),
	listLaunches func(context.Context) ([]watcher.ScheduledLaunch, error), roles roleCheck) {
	watches, werr := listWatches(ctx)
	launches, lerr := listLaunches(ctx)
	checked := 0
	for i := range watches {
		if watchLaunchesConfig(&watches[i]) {
			checked++
		}
	}
	for i := range launches {
		if launches[i].Armed() {
			checked++
		}
	}
	var listErr error
	switch {
	case werr != nil && lerr != nil:
		listErr = fmt.Errorf("list watches: %v; list scheduled launches: %v", werr, lerr)
	case werr != nil:
		listErr = fmt.Errorf("list watches: %w", werr)
	case lerr != nil:
		listErr = fmt.Errorf("list scheduled launches: %w", lerr)
	}
	report.Checks = append(report.Checks, doctor.EvalLaunchConfigs(launchConfigFindings(watches, launches, roles), checked, listErr))
}

// activeSubnetCoverage checks each spawn/hold watch's default-subnet coverage
// (#256), skipping the watches the check does not apply to.
func activeSubnetCoverage(ctx context.Context, checker *watcher.SubnetCoverageChecker, watches []watcher.Watch) []*watcher.SubnetCoverage {
	var out []*watcher.SubnetCoverage
	for i := range watches {
		if cov := checker.Check(ctx, &watches[i]); cov.Checked {
			out = append(out, cov)
		}
	}
	return out
}

// doctorExitError turns a report into the command's exit status: an error (hence a
// non-zero exit) if and only if something FAILED. Warnings deliberately do not
// count — doctor is meant to be usable as a CI gate, and one that trips on every
// advisory finding is one people stop running.
func doctorExitError(report *doctor.Report) error {
	if !report.Failed() {
		return nil
	}
	_, _, fail := report.Counts()
	return fmt.Errorf("doctor: %d check(s) failed", fail)
}

// writeDoctorReport emits the report in the requested format. Split out so the
// rendering (and the json/table branch) is unit-testable without AWS.
func writeDoctorReport(out io.Writer, report *doctor.Report, format string) error {
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	// Markers go through libs/i18n so --no-emoji and --accessibility are honored
	// (i18n.Symbol falls back to "?" if no localizer was initialized, hence the
	// explicit plain fallback).
	symbol := doctor.PlainSymbols
	if i18n.Global != nil {
		symbol = i18n.Symbol
	}
	_, err := io.WriteString(out, report.Text(symbol))
	return err
}
