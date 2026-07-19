package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create lagotto's DynamoDB tables and grant the hosted poller its runtime IAM policy",
	Long: `Provision lagotto's backend: the DynamoDB tables it uses to store watches and
match history (lagotto-watches and lagotto-match-history by default; override with
--watches-table / --history-table), and — if the hosted poller has been deployed
('lagotto deploy') — the runtime IAM policy that lets the poller spawn/hold/submit.

The table creation is idempotent (existing tables are left untouched) and normally
automatic: 'lagotto watch' creates the tables on first use. Run 'setup' explicitly
to provision the backend ahead of time, or — importantly — after 'lagotto deploy'
to grant the poller its permissions. 'deploy' creates only a minimal execution
role (so the runtime Lambda can never self-escalate); 'setup', run by you, attaches
the spawn/hold/SageMaker/scheduler policy. Until then the poller can only notify.
If the poller role doesn't exist yet, setup creates the tables and prints a
next-step note instead of failing.`,
	RunE: runSetup,
}

func init() {
	rootCmd.AddCommand(setupCmd)
}

func runSetup(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	out := cmd.OutOrStdout()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// 1. Tables (CLI-owned, #59).
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	created, err := store.EnsureTables(ctx)
	if err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}
	if len(created) == 0 {
		fmt.Fprintf(out, "Tables already exist (%s, %s).\n", watchesTable, historyTable)
	}
	for _, name := range created {
		fmt.Fprintf(out, "Created table %s\n", name)
	}

	// 2. Runtime IAM policy (#16): lagotto owns the hosted poller's permissions in
	// Go, the same way it owns its tables. The CFN/SAM stack (`lagotto deploy`)
	// creates a minimal execution role; setup attaches the permissions policy to
	// it. Skipped gracefully when the role doesn't exist yet (deploy hasn't run) —
	// the poller only NOTIFIES until the policy is applied, so this is a clear
	// next-step message, not a hard failure.
	if err := ensureRuntimePolicy(ctx, cfg, out); err != nil {
		return err
	}

	fmt.Fprintln(out, "Setup complete.")
	return nil
}

// ensureRuntimePolicy resolves the account ID and writes the poller runtime
// policy onto its execution role. A missing role (NoSuchEntity) is reported as a
// "run lagotto deploy first" note rather than an error, since setup is also used
// pre-deploy to provision only the tables.
func ensureRuntimePolicy(ctx context.Context, cfg aws.Config, out io.Writer) error {
	acct, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("resolve account ID: %w", err)
	}
	region := cfg.Region
	if region == "" {
		return fmt.Errorf("no region resolved; set --region, SPORE_REGION, or AWS_REGION")
	}

	err = runtimeiam.EnsureRuntimeRole(ctx, iam.NewFromConfig(cfg), region, aws.ToString(acct.Account))
	if err != nil {
		var notFound *iamtypes.NoSuchEntityException
		if errors.As(err, &notFound) {
			fmt.Fprintf(out, "Runtime IAM role %q not found — run `lagotto deploy` to create it, then re-run setup.\n"+
				"(Until then the hosted poller can only send notifications, not spawn/hold/submit.)\n", runtimeiam.RoleName)
			return nil
		}
		return fmt.Errorf("ensure runtime IAM policy: %w", err)
	}
	fmt.Fprintf(out, "Applied runtime IAM policy %q to role %q.\n", runtimeiam.PolicyName, runtimeiam.RoleName)
	return nil
}
