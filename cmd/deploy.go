package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/deploy"
)

var (
	deployStackName string
	deployRegion    string
	deployVersion   string
	deployEnv       string
	deployBucket    string
	deployTeardown  bool
	deployYes       bool
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy the hosted capacity-poller stack into your own AWS account",
	Long: `Stand up lagotto's hosted capacity poller (DynamoDB, SNS, Lambda, EventBridge
Scheduler) in your OWN AWS account, so watches are serviced server-side — armed
once, then hands-off — instead of depending on a foreground 'poll --daemon' that
dies when your laptop sleeps.

It downloads the published capacity-poller Lambda artifact for the given
--version, uploads it to a bucket in your account, and deploys the embedded
CloudFormation stack. The poller schedule deploys DISABLED; the first 'lagotto
watch' enables it (and the poller self-disables when no active watches remain).

Use --teardown to delete the stack.`,
	RunE: runDeploy,
}

func init() {
	rootCmd.AddCommand(deployCmd)
	f := deployCmd.Flags()
	f.StringVar(&deployStackName, "stack-name", "lagotto", "CloudFormation stack name")
	f.StringVar(&deployRegion, "region", "", "AWS region (default: from your AWS config)")
	f.StringVar(&deployVersion, "version", Version, "lagotto release version to pull the poller Lambda from")
	f.StringVar(&deployEnv, "environment", "production", "Environment tag (production, staging, development)")
	f.StringVar(&deployBucket, "lambda-bucket", "", "S3 bucket for the Lambda artifact (default: lagotto-lambda-<account>-<region>, created if absent)")
	f.BoolVar(&deployTeardown, "teardown", false, "Delete the stack instead of deploying it")
	f.BoolVarP(&deployYes, "yes", "y", false, "Skip the confirmation prompt")
}

func runDeploy(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	out := cmd.OutOrStdout()

	var optFns []func(*config.LoadOptions) error
	if deployRegion != "" {
		optFns = append(optFns, config.WithRegion(deployRegion))
	}
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
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

	if deployTeardown {
		if !confirm(fmt.Sprintf("Delete CloudFormation stack %q in %s (account %s)? This removes the poller, tables, SNS topic.", deployStackName, region, acctID)) {
			fmt.Fprintln(out, "Aborted.")
			return nil
		}
		fmt.Fprintf(out, "Deleting stack %s...\n", deployStackName)
		if err := d.Teardown(ctx, deployStackName); err != nil {
			return err
		}
		fmt.Fprintf(out, "Stack %s deleted.\n", deployStackName)
		return nil
	}

	if deployVersion == "" || deployVersion == "dev" {
		return fmt.Errorf("--version is required (a released version like 0.44.0); the dev build has no published poller artifact")
	}

	if !confirm(fmt.Sprintf("Deploy lagotto stack %q (poller v%s) into account %s / %s?", deployStackName, deployVersion, acctID, region)) {
		fmt.Fprintln(out, "Aborted.")
		return nil
	}

	fmt.Fprintf(os.Stderr, "Deploying %s (poller v%s) into %s / %s...\n", deployStackName, deployVersion, acctID, region)
	outputs, err := d.Deploy(ctx, deploy.Options{
		StackName:   deployStackName,
		Region:      region,
		Version:     deployVersion,
		Environment: deployEnv,
		Bucket:      deployBucket,
		AccountID:   acctID,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "\nStack %s deployed.\n", deployStackName)
	keys := make([]string, 0, len(outputs))
	for k := range outputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(out, "  %s: %s\n", k, outputs[k])
	}
	fmt.Fprintln(out, "\nArm a watch with 'lagotto watch …' — the poller schedule activates on the first watch.")
	return nil
}
