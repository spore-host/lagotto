package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

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
	Use:   "doctor",
	Short: "Check the deployed hosted poller against what this lagotto expects",
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
		return fmt.Errorf("no AWS region set; pass --region or configure one (the expected ARNs are region-scoped)")
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
		ActiveWatches: func(ctx context.Context) (int, error) {
			watches, lErr := store.ListActiveWatches(ctx)
			if lErr != nil {
				return 0, lErr
			}
			return len(watches), nil
		},
	})
	report := doctor.Evaluate(state)

	if err := writeDoctorReport(cmd.OutOrStdout(), report, getOutputFormat()); err != nil {
		return err
	}
	return doctorExitError(report)
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
