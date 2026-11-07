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
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "Create lagotto's DynamoDB tables, the hosted poller's IAM roles, and the CLI-user policy",
	Long: `Provision lagotto's backend: the DynamoDB tables it uses to store watches and
match history (lagotto-watches and lagotto-match-history by default; override with
--watches-table / --history-table), and both CLI-owned IAM roles: the poller's
execution role (lagotto-capacity-poller-role) with the runtime IAM policy that lets
the poller spawn/hold/submit, and the role EventBridge Scheduler assumes to invoke
the poller (lagotto-capacity-poller-scheduler-invoke). The roles are account-global,
so one run in any region serves every region. Roles that already exist are reused;
both policies are rewritten on every run. Setup prints each role it created.

The table creation is idempotent (existing tables are left untouched) and normally
automatic: 'lagotto watch' creates the tables on first use. Run 'setup' explicitly
to provision the backend ahead of time, or — importantly — after 'lagotto deploy'
to grant the poller its permissions. 'deploy' creates only a minimal execution
role (so the runtime Lambda can never self-escalate); 'setup', run by you, attaches
the spawn/hold/SageMaker/scheduler policy. Until then the poller can only notify.
'lagotto teardown --iam-roles' deletes both roles once no region runs a poller.

It also writes the customer-managed policy lagotto-cli-user (lagotto#232): the
least-privilege permissions the everyday commands need with YOUR credentials
(watch, update, ack, extend, cancel, list, status, history, launch, notify-gc,
doctor), resource-scoped to lagotto's tables, schedules, topic, roles and poller.
It is generated from the CLI's own AWS calls (docs/reference/cli-iam-actions.md).
Attach it to the group, users or roles that run lagotto. A re-run that changes
it writes a new default version (deleting the oldest when IAM's limit of five is
reached); an unchanged one is left alone. 'poll', 'setup', 'deploy' and
'teardown' are not covered: they need administrator permissions. s3:// spawn
configs are readable only from the buckets you name with --config-bucket; those
are carried forward on later runs. 'lagotto teardown --iam-roles' detaches and
deletes the policy.`,
	RunE: runSetup,
}

var (
	setupInstanceRoles       []string
	setupRevokeRoles         []string
	setupConfigBuckets       []string
	setupRevokeConfigBuckets []string
)

func init() {
	setupCmd.Flags().StringArrayVar(&setupInstanceRoles, "instance-role", nil,
		"Authorize the hosted poller to use this IAM instance role when a watch's spawn_config names it in iam_role (repeatable). "+
			"Without this the watch waits for capacity, matches, then dies at launch with AccessDenied. "+
			"NOTE: this lets the poller write an inline policy to and attach a managed policy to that role, not merely pass it — spawn adds its spored/SSM baseline to every role it launches with.")
	setupCmd.Flags().StringArrayVar(&setupRevokeRoles, "revoke-instance-role", nil,
		"Remove a previously authorized instance role (repeatable). Takes precedence over --instance-role for the same name.")
	setupCmd.Flags().StringArrayVar(&setupConfigBuckets, "config-bucket", nil,
		"Let the "+cliiam.PolicyName+" policy read s3:// spawn configs from this bucket (repeatable; s3:GetObject on its objects). "+
			"Kept on later runs until revoked.")
	setupCmd.Flags().StringArrayVar(&setupRevokeConfigBuckets, "revoke-config-bucket", nil,
		"Remove a bucket granted with --config-bucket (repeatable). Takes precedence over --config-bucket for the same name.")
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
		_, _ = fmt.Fprintf(out, "Tables already exist (%s, %s).\n", watchesTable, historyTable)
	}
	for _, name := range created {
		_, _ = fmt.Fprintf(out, "Created table %s\n", name)
	}

	// 2. The CLI-owned IAM roles and their policies (#16, #145, #229): lagotto
	// owns the hosted poller's permissions in Go, the same way it owns its
	// tables. Both roles are get-or-created, so setup works standalone on a fresh
	// account, before or without a `lagotto deploy`.
	if err := ensureRuntimePolicy(ctx, cfg, out); err != nil {
		return err
	}

	// 3. The CLI-user policy (#232): what the people who run lagotto need.
	acct, err := setupCallerAccount(ctx, cfg)
	if err != nil {
		return fmt.Errorf("resolve account ID: %w", err)
	}
	if err := applyCLIUserPolicy(ctx, newSetupPolicyClient(cfg), acct, out); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(out, "Setup complete.")
	return nil
}

// ensureRuntimePolicy resolves the account ID, get-or-creates both CLI-owned
// roles, and writes the runtime policy onto the poller's execution role and the
// invoke policy onto the Scheduler invoke role. It prints each role it created.
func ensureRuntimePolicy(ctx context.Context, cfg aws.Config, out io.Writer) error {
	acct, err := setupCallerAccount(ctx, cfg)
	if err != nil {
		return fmt.Errorf("resolve account ID: %w", err)
	}
	region := cfg.Region
	if region == "" {
		return fmt.Errorf("no region resolved; set --region, SPORE_REGION, or AWS_REGION")
	}

	return applySetupRoles(ctx, newSetupIAMClient(cfg), region, acct, out)
}

// setupCallerAccount and newSetupIAMClient are setup's AWS clients, seams so
// ensureRuntimePolicy is tested without AWS.
var setupCallerAccount = func(ctx context.Context, cfg aws.Config) (string, error) {
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.Account), nil
}

var newSetupIAMClient = func(cfg aws.Config) runtimeiam.IAMAPI { return iam.NewFromConfig(cfg) }

// newSetupPolicyClient builds the IAM client the CLI-user policy is written
// with. A seam for tests.
var newSetupPolicyClient = func(cfg aws.Config) cliiam.PolicyAPI { return iam.NewFromConfig(cfg) }

// applyCLIUserPolicy writes the lagotto-cli-user managed policy (#232),
// carrying the config buckets the deployed version grants forward, and tells
// the administrator how to attach it.
func applyCLIUserPolicy(ctx context.Context, client cliiam.PolicyAPI, accountID string, out io.Writer) error {
	buckets, err := resolveConfigBuckets(ctx, client, accountID, out)
	if err != nil {
		return err
	}
	doc, err := cliiam.PolicyDocument(accountID, buckets)
	if err != nil {
		return err
	}
	res, err := cliiam.EnsurePolicy(ctx, client, accountID, doc)
	if res.DeletedVersion != "" {
		_, _ = fmt.Fprintf(out, "Deleted the oldest version (%s) of IAM policy %s to stay within IAM's five versions.\n",
			res.DeletedVersion, cliiam.PolicyName)
	}
	if err != nil {
		return fmt.Errorf("write the CLI-user IAM policy: %w", err)
	}
	switch {
	case res.Created:
		_, _ = fmt.Fprintf(out, "Created IAM policy %s (%s).\n", cliiam.PolicyName, res.ARN)
	case res.Updated:
		_, _ = fmt.Fprintf(out, "Updated IAM policy %s with a new default version.\n", cliiam.PolicyName)
	default:
		_, _ = fmt.Fprintf(out, "IAM policy %s is up to date.\n", cliiam.PolicyName)
	}
	if len(buckets) > 0 {
		_, _ = fmt.Fprintf(out, "  s3:// spawn configs readable from: %s\n", strings.Join(buckets, ", "))
	}
	_, _ = fmt.Fprintf(out, "  Attach it to whoever runs lagotto's everyday commands, for example:\n"+
		"    aws iam attach-group-policy --group-name <group> --policy-arn %s\n", res.ARN)
	return nil
}

// resolveConfigBuckets computes the s3:// config buckets to grant: those the
// deployed policy grants, plus --config-bucket, minus --revoke-config-bucket.
// Revoke wins for the same name. A deployed policy that cannot be read is an
// error, never "no buckets": guessing would narrow the grant being written.
func resolveConfigBuckets(ctx context.Context, client cliiam.PolicyAPI, accountID string, out io.Writer) ([]string, error) {
	for _, name := range append(append([]string{}, setupConfigBuckets...), setupRevokeConfigBuckets...) {
		if err := cliiam.ValidateConfigBucketName(name); err != nil {
			return nil, err
		}
	}
	var current []string
	doc, found, err := cliiam.ReadPolicy(ctx, client, accountID)
	if err != nil {
		return nil, fmt.Errorf("read the deployed %s policy (needed to keep its config buckets): %w", cliiam.PolicyName, err)
	}
	if found {
		if current, err = cliiam.ConfigBuckets(doc); err != nil {
			return nil, fmt.Errorf("parse the deployed %s policy: %w", cliiam.PolicyName, err)
		}
	}
	revoked := map[string]bool{}
	for _, n := range setupRevokeConfigBuckets {
		n = strings.TrimSpace(n)
		revoked[n] = true
		if !containsString(current, n) {
			_, _ = fmt.Fprintf(out, "note: config bucket %q was not granted; nothing to revoke.\n", n)
		}
	}
	var keep []string
	for _, n := range append(current, setupConfigBuckets...) {
		if n = strings.TrimSpace(n); !revoked[n] {
			keep = append(keep, n)
		}
	}
	return cliiam.NormalizeConfigBuckets(keep), nil
}

// applySetupRoles is the IAM half of setup, separated so it is tested against a
// fake IAM: carry the authorized instance roles forward, ensure both roles and
// their policies, and report what was created and applied.
func applySetupRoles(ctx context.Context, iamClient runtimeiam.IAMAPI, region, accountID string, out io.Writer) error {
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

	// Both CLI-owned roles (#229), not only the poller's: the Scheduler invoke role
	// is what the poller schedule (and a CloudFormation stack's PollerSchedule)
	// assumes, so without it nothing ever invokes the poller. Before #229 only
	// `lagotto deploy` created it.
	created, err := runtimeiam.EnsureSetupRoles(ctx, iamClient, region, accountID, roles)
	for _, name := range created {
		_, _ = fmt.Fprintf(out, "Created IAM role %s\n", name)
	}
	if err != nil {
		return fmt.Errorf("ensure poller IAM roles/policies: %w", err)
	}
	_, _ = fmt.Fprintf(out, "Applied runtime IAM policy %q to role %q.\n", runtimeiam.PolicyName, runtimeiam.RoleName)
	_, _ = fmt.Fprintf(out, "Applied invoke policy %q to role %q.\n", runtimeiam.SchedulerInvokePolicyName, runtimeiam.SchedulerInvokeRoleName)
	if len(roles) > 0 {
		_, _ = fmt.Fprintf(out, "Authorized instance role(s) for hosted spawns: %s\n", strings.Join(roles, ", "))
		_, _ = fmt.Fprintln(out, "  The poller may now read these roles, write their inline policy, attach managed policies to")
		_, _ = fmt.Fprintln(out, "  them, and pass them to EC2. spawn adds its spored/SSM baseline to every role it launches with.")
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
			_, _ = fmt.Fprintf(out, "note: instance role %q was not authorized; nothing to revoke.\n", n)
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
