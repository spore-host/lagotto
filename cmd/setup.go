package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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

var (
	setupInstanceRoles []string
	setupRevokeRoles   []string
)

func init() {
	setupCmd.Flags().StringArrayVar(&setupInstanceRoles, "instance-role", nil,
		"Authorize the hosted poller to use this IAM instance role when a watch's spawn_config names it in iam_role (repeatable). "+
			"Without this the watch waits for capacity, matches, then dies at launch with AccessDenied. "+
			"NOTE: this lets the poller write an inline policy to and attach a managed policy to that role, not merely pass it — spawn adds its spored/SSM baseline to every role it launches with.")
	setupCmd.Flags().StringArrayVar(&setupRevokeRoles, "revoke-instance-role", nil,
		"Remove a previously authorized instance role (repeatable). Takes precedence over --instance-role for the same name.")
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

	iamClient := iam.NewFromConfig(cfg)

	// Authorized instance roles (#170) are carried forward from the DEPLOYED policy
	// before being re-applied. The policy is written with a wholesale
	// PutRolePolicy, so without this a plain `lagotto setup` would silently revoke
	// every authorization — the same footgun as an older binary's setup reverting
	// newer grants, which is exactly how #170's reporter ended up with watches that
	// matched capacity and then died.
	roles, err := resolveAuthorizedInstanceRoles(ctx, iamClient, out)
	if err != nil {
		return err
	}

	// EnsureRuntimeRole now creates the CLI-owned role if absent (#145), so setup
	// works standalone on a fresh account without a prior `lagotto deploy`.
	if err = runtimeiam.EnsureRuntimeRole(ctx, iamClient, region, aws.ToString(acct.Account), roles); err != nil {
		return fmt.Errorf("ensure runtime IAM role/policy: %w", err)
	}
	fmt.Fprintf(out, "Applied runtime IAM policy %q to role %q.\n", runtimeiam.PolicyName, runtimeiam.RoleName)
	if len(roles) > 0 {
		fmt.Fprintf(out, "Authorized instance role(s) for hosted spawns: %s\n", strings.Join(roles, ", "))
		fmt.Fprintln(out, "  The poller may now read these roles, write their inline policy, attach managed policies to")
		fmt.Fprintln(out, "  them, and pass them to EC2. spawn adds its spored/SSM baseline to every role it launches with.")
	}
	return nil
}

// resolveAuthorizedInstanceRoles computes the instance-role authorizations to
// write: whatever the deployed policy already has, plus --instance-role, minus
// --revoke-instance-role.
//
// Revoke wins over authorize for the same name, so a scripted invocation that
// passes both is unambiguous rather than order-dependent.
func resolveAuthorizedInstanceRoles(ctx context.Context, client runtimeiam.IAMAPI, out io.Writer) ([]string, error) {
	for _, name := range append(append([]string{}, setupInstanceRoles...), setupRevokeRoles...) {
		if err := runtimeiam.ValidateInstanceRoleName(name); err != nil {
			return nil, err
		}
	}

	existing, err := runtimeiam.ReadRuntimePolicy(ctx, client)
	switch {
	case err == nil:
		// fall through: carry existing authorizations forward
	case errors.Is(err, runtimeiam.ErrRoleNotFound), errors.Is(err, runtimeiam.ErrPolicyNotFound):
		existing = "" // nothing deployed yet; start from just the flags
	default:
		// Never guess here: an AccessDenied reported as "no authorizations" would
		// make setup silently narrow the policy it is about to write.
		return nil, fmt.Errorf("read deployed runtime policy (needed to preserve authorized instance roles): %w", err)
	}

	var current []string
	if existing != "" {
		if current, err = runtimeiam.AuthorizedInstanceRoles(existing); err != nil {
			return nil, err
		}
	}

	revoked := map[string]bool{}
	for _, n := range setupRevokeRoles {
		revoked[strings.TrimSpace(n)] = true
	}

	keep := map[string]bool{}
	for _, n := range append(current, setupInstanceRoles...) {
		n = strings.TrimSpace(n)
		if n != "" && !revoked[n] {
			keep[n] = true
		}
	}

	final := make([]string, 0, len(keep))
	for n := range keep {
		final = append(final, n)
	}
	sort.Strings(final)

	for _, n := range setupRevokeRoles {
		n = strings.TrimSpace(n)
		if !containsString(current, n) {
			fmt.Fprintf(out, "note: instance role %q was not authorized; nothing to revoke.\n", n)
		}
	}
	return final, nil
}

func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
