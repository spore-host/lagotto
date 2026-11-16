package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/inventory"
)

var footprintAllRegions bool

var footprintCmd = &cobra.Command{
	Use:         "footprint",
	Annotations: level(levelAdvanced),
	Short:       "List every AWS resource lagotto has in this account, classified for teardown",
	Long: `List every AWS resource lagotto has in your account, say who owns each and
whether it is in use, and say whether lagotto's teardown may delete it
(lagotto#325, part of leave-no-trace spore-host/spore-host#752). Read-only: it
deletes and changes nothing. -o json emits the shared spore.host footprint
schema (libs/footprint, schema 1), which 'lagotto teardown --plan' executes.

It reads one region (--region, else your configured one) or, with
--all-regions, every region enabled for the account. In each region it lists the
three DynamoDB tables (with their record counts and whether any watch is active
or any scheduled launch armed), the artifact bucket, the alerts topic, the poller
Lambda and its log group, the schedule groups "lagotto" and "lagotto-teardown"
and their schedules, schedules named lagotto-* in the "default" group (made
before lagotto#324), the capacity reservations hold watches hold, the SageMaker
training jobs running for a watch, and the legacy CloudFormation stack
"lagotto". Account-wide it lists the poller's two IAM roles, lagotto's instance
role and its instance profiles, the instance profile 'setup' created for each
authorized instance role (and that role, which is yours), and the
lagotto-cli-user policy with how many principals it is attached to.

OWNERSHIP is "tagged" for a resource tagged spore-host:managed-by=lagotto,
"legacy-name" for one that only has lagotto's name (made before lagotto#324
tagged everything; reported, never deleted), and "foreign" for one that is not
lagotto's (an authorized instance role). A schedule cannot be tagged and carries
its group's tags.

CLASS is:
  active     in use: the tables, poller, topic and bucket of a region where a
             watch is active, a scheduled launch is armed, a hold holds a
             reservation or a training job runs (or where that could not be
             read); a per-launch schedule a scheduled-launch record names; the
             poller schedule of an active poller; anything an active resource
             depends on; lagotto-cli-user while anything is attached to it.
  residue    lagotto's own machinery, removable once nothing is active: the
             Scheduler invoke role every region shares, and a poller log group
             whose function is gone (Lambda recreated it).
  permanent  tagged spore-host:permanent=true. lagotto owns nothing permanent.
  user-data  nothing: the match history is deleted with the teardown, by owner
             decision. Export it first with 'lagotto history'.
  litter     everything else, removable now.
The account-wide resources serve every region, so they are classified only with
--all-regions; from one region they are classed active, with a warning.

DELETABLE is the shared delete guard's verdict: only a tagged resource with
lagotto's name, not tagged spore-host:role=service or spore-host:permanent=true,
whose class is not user-data or permanent. REASON says why not.`,
	Args:         cobra.NoArgs,
	RunE:         runFootprint,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(footprintCmd)
	footprintCmd.Flags().BoolVar(&footprintAllRegions, "all-regions", false,
		"Read every region enabled for the account (needed to classify the account-wide IAM resources); not with --region")
}

func runFootprint(cmd *cobra.Command, _ []string) error {
	if footprintAllRegions && cmd.Flags().Changed("region") {
		return fmt.Errorf("--all-regions reads every region; it cannot be combined with --region")
	}
	format := getOutputFormat()
	if format != "table" && format != "json" {
		return fmt.Errorf("footprint: -o %s is not supported (table or json)", format)
	}
	ctx := context.Background()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	acct, err := setupCallerAccount(ctx, cfg)
	if err != nil {
		return fmt.Errorf("resolve the AWS account ID (every ARN in the footprint is derived from it): %w", err)
	}
	scope := inventory.Scope{AllRegions: footprintAllRegions}
	if !scope.AllRegions {
		if cfg.Region == "" {
			return fmt.Errorf("no AWS region set; pass --region, or --all-regions")
		}
		scope.Regions = []string{cfg.Region}
	}
	rep, err := takeFootprint(ctx, cfg, acct, scope)
	if err != nil {
		return err
	}
	return writeFootprint(cmd.OutOrStdout(), rep, format)
}

// gatherFootprint reads the footprint from AWS. A seam for tests.
var gatherFootprint = func(ctx context.Context, cfg aws.Config, accountID string, scope inventory.Scope) (inventory.Facts, error) {
	return inventory.Gather(ctx, inventory.NewAWSSource(cfg, accountID), accountID, scope, footprintOptions())
}

// footprintNow stamps the report. A seam for tests.
var footprintNow = time.Now

// footprintOptions are the resource names this invocation uses.
func footprintOptions() inventory.Options {
	return inventory.Options{WatchesTable: watchesTable, HistoryTable: historyTable}
}

// takeFootprint reads and classifies the footprint in scope.
func takeFootprint(ctx context.Context, cfg aws.Config, accountID string, scope inventory.Scope) (*fp.Report, error) {
	facts, err := gatherFootprint(ctx, cfg, accountID, scope)
	if err != nil {
		return nil, fmt.Errorf("footprint: %w", err)
	}
	return inventory.Classify(facts, footprintOptions(), footprintNow()), nil
}

// writeFootprint renders the report as the shared JSON schema or a table.
func writeFootprint(out io.Writer, rep *fp.Report, format string) error {
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "REGION\tTYPE\tNAME\tOWNERSHIP\tCLASS\tDELETABLE\tREASON")
	for _, r := range rep.Resources {
		del := "no"
		if r.Deletable {
			del = "yes"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.Region, strings.TrimPrefix(r.Type, "AWS::"), r.Name, r.Ownership, r.Class, del, r.Reason)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	counts := inventory.Summary(rep)
	classes := make([]string, 0, len(counts))
	for c := range counts {
		classes = append(classes, string(c))
	}
	sort.Strings(classes)
	parts := make([]string, len(classes))
	for i, c := range classes {
		parts[i] = fmt.Sprintf("%d %s", counts[fp.Class(c)], c)
	}
	summary := "nothing"
	if len(parts) > 0 {
		summary = strings.Join(parts, ", ")
	}
	_, _ = fmt.Fprintf(out, "\n%d resource(s) in account %s: %s.\n", len(rep.Resources), rep.Account, summary)
	for _, wn := range rep.Warnings {
		_, _ = fmt.Fprintf(out, "warning: %s\n", wn)
	}
	return nil
}
