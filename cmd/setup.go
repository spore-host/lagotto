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
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

var setupCmd = &cobra.Command{
	Use:         "setup",
	Annotations: level(levelAdvanced),
	Short:       "Create lagotto's DynamoDB tables, the hosted poller's IAM roles, and the CLI-user policy",
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

The poller creates and changes no IAM role. Setup creates the one it launches
with by default, lagotto-instance-role (with its instance profile and spawn's
spored/SSM baseline, nothing else), and lets the poller pass only that role, the
roles you name with --instance-role (which must exist; setup creates their
instance profiles) and, for SageMaker watches, the execution roles you name with
--sagemaker-role. Every lagotto user can launch with an authorized role.

Every record a CLI user creates is keyed under the creator's principal id
(<id>/w-…), and lagotto-cli-user lets each caller read and write only the records
under its own id, enforced by IAM (dynamodb:LeadingKeys). Before writing that
policy, setup adds the owner index to a watches table that predates it and moves
every older record (watches, groups, match history, scheduled launches) under its
creator's id: each in one transaction, so nothing is lost or duplicated, and it is
safe to re-run. Records whose creator cannot be resolved are left in place and
listed; the poller still services them.

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
	setupSageMakerRoles      []string
	setupRevokeSageMaker     []string
	setupConfigBuckets       []string
	setupRevokeConfigBuckets []string
)

func init() {
	setupCmd.Flags().StringArrayVar(&setupInstanceRoles, "instance-role", nil,
		"Authorize the hosted poller to launch with this existing IAM role when a watch's spawn_config names it in iam_role (repeatable). "+
			"Setup adds spawn's spored/SSM baseline to the role and creates its instance profile; the poller may then only pass it to EC2. "+
			"Every lagotto user can launch with an authorized role, so authorize only roles whose permissions they may all have.")
	setupCmd.Flags().StringArrayVar(&setupRevokeRoles, "revoke-instance-role", nil,
		"Remove a previously authorized instance role (repeatable), and the instance profile setup created for it. Takes precedence over --instance-role for the same name.")
	setupCmd.Flags().StringArrayVar(&setupSageMakerRoles, "sagemaker-role", nil,
		"Authorize the hosted poller to pass this SageMaker execution role (full role ARN, repeatable) for --service sagemaker watches. "+
			"A job spec naming any other RoleArn fails at submit. Every lagotto user can submit jobs with an authorized role.")
	setupCmd.Flags().StringArrayVar(&setupRevokeSageMaker, "revoke-sagemaker-role", nil,
		"Remove a previously authorized SageMaker execution role ARN (repeatable). Takes precedence over --sagemaker-role.")
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

	// 3. Owner-scoped keys: the owner index and every legacy record moved under
	// its owner's prefix, BEFORE the owner-scoped CLI-user policy below makes
	// legacy keys unreachable to CLI users.
	if _, err := ensureOwnerKeys(ctx, newOwnerKeysDynamo(cfg), newPrincipalIDs(cfg), watcher.MigrateTables{
		Watches: watchesTable, History: historyTable, Scheduled: store.ScheduledTable(),
	}, out); err != nil {
		return err
	}

	// 4. The CLI-user policy (#232): what the people who run lagotto need.
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

	return applySetupRoles(ctx, newSetupIAMClient(cfg), newInstanceProfileSetup(cfg), region, acct, out)
}

// instanceProfileSetup is the part of setup that provisions instance
// profiles with the administrator's credentials: lagotto's own instance role,
// and a profile for each authorized role. A seam for tests.
type instanceProfileSetup interface {
	// EnsureInstanceProfile creates (or completes) the role's same-named
	// instance profile with spawn's spored/SSM baseline on the role, creating
	// the role too when it is absent.
	EnsureInstanceProfile(ctx context.Context, roleName string) error
	// DeleteManagedProfile deletes the role's instance profile if setup
	// created it (runtimeiam.DeleteManagedInstanceProfile).
	DeleteManagedProfile(ctx context.Context, roleName string) (bool, error)
}

var newInstanceProfileSetup = func(cfg aws.Config) instanceProfileSetup { return spawnProfileSetup{cfg: cfg} }

// spawnProfileSetup provisions through spawn's own CreateOrGetInstanceProfile,
// so the role carries exactly the baseline `spawn launch --iam-role` writes.
// It runs here, with an administrator's credentials, and never in the poller.
type spawnProfileSetup struct{ cfg aws.Config }

func (p spawnProfileSetup) EnsureInstanceProfile(ctx context.Context, roleName string) error {
	_, err := spawnaws.NewClientFromConfig(p.cfg).CreateOrGetInstanceProfile(ctx, spawnaws.IAMRoleConfig{
		RoleName: roleName,
		Tags:     runtimeiam.ManagedProfileTags(),
	})
	return err
}

func (p spawnProfileSetup) DeleteManagedProfile(ctx context.Context, roleName string) (bool, error) {
	return runtimeiam.DeleteManagedInstanceProfile(ctx, iam.NewFromConfig(p.cfg), roleName)
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
func applySetupRoles(ctx context.Context, iamClient runtimeiam.IAMAPI, profiles instanceProfileSetup, region, accountID string, out io.Writer) error {
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
	smRoles, err := resolveAuthorizedSageMakerRoles(ctx, iamClient, accountID, out)
	if err != nil {
		return err
	}
	// An authorized role must already exist: its permissions are the
	// administrator's decision, and lagotto cannot guess them. Checked before
	// anything is written.
	for _, name := range roles {
		ok, err := runtimeiam.RoleExists(ctx, iamClient, name)
		if err != nil {
			return fmt.Errorf("check instance role %s: %w", name, err)
		}
		if !ok {
			return fmt.Errorf("instance role %q does not exist. Create it with the permissions the workload needs "+
				"(trusting ec2.amazonaws.com), then re-run setup; or revoke it with --revoke-instance-role %s", name, name)
		}
	}

	// Both CLI-owned roles (#229), not only the poller's: the Scheduler invoke role
	// is what the poller schedule (and a CloudFormation stack's PollerSchedule)
	// assumes, so without it nothing ever invokes the poller. Before #229 only
	// `lagotto deploy` created it.
	created, err := runtimeiam.EnsureSetupRoles(ctx, iamClient, region, accountID,
		runtimeiam.Authorizations{InstanceRoles: roles, SageMakerRoles: smRoles})
	for _, name := range created {
		_, _ = fmt.Fprintf(out, "Created IAM role %s\n", name)
	}
	if err != nil {
		return fmt.Errorf("ensure poller IAM roles/policies: %w", err)
	}

	// The instance profiles the poller launches with. The poller creates none
	// itself, so setup provides every one it may use: lagotto's own role, and
	// a profile for each authorized role.
	if err := profiles.EnsureInstanceProfile(ctx, runtimeiam.InstanceRoleName); err != nil {
		return fmt.Errorf("ensure instance role %s: %w", runtimeiam.InstanceRoleName, err)
	}
	_, _ = fmt.Fprintf(out, "Ensured instance role and profile %s (spawn's spored/SSM baseline only).\n", runtimeiam.InstanceRoleName)
	for _, name := range roles {
		if err := profiles.EnsureInstanceProfile(ctx, name); err != nil {
			return fmt.Errorf("ensure instance profile for %s: %w", name, err)
		}
	}
	for _, name := range setupRevokeRoles {
		name = strings.TrimSpace(name)
		if containsString(roles, name) {
			continue
		}
		gone, err := profiles.DeleteManagedProfile(ctx, name)
		if err != nil {
			return fmt.Errorf("delete the instance profile setup created for %s: %w", name, err)
		}
		if gone {
			_, _ = fmt.Fprintf(out, "Deleted instance profile %s (created by setup).\n", name)
		}
	}
	_, _ = fmt.Fprintf(out, "Applied runtime IAM policy %q to role %q.\n", runtimeiam.PolicyName, runtimeiam.RoleName)
	_, _ = fmt.Fprintf(out, "Applied invoke policy %q to role %q.\n", runtimeiam.SchedulerInvokePolicyName, runtimeiam.SchedulerInvokeRoleName)
	if len(roles) > 0 {
		_, _ = fmt.Fprintf(out, "Authorized instance role(s) for hosted spawns: %s\n", strings.Join(roles, ", "))
		_, _ = fmt.Fprintln(out, "  The poller may pass these roles to EC2, and nothing else: any lagotto user can launch with them.")
	}
	if len(smRoles) > 0 {
		_, _ = fmt.Fprintf(out, "Authorized SageMaker execution role(s): %s\n", strings.Join(smRoles, ", "))
	}
	return nil
}

// resolveAuthorizedSageMakerRoles is resolveAuthorizedInstanceRoles for the
// SageMaker execution roles (--sagemaker-role / --revoke-sagemaker-role).
// The old blanket PassRole-to-SageMaker on "*" names no role and is not
// carried forward.
func resolveAuthorizedSageMakerRoles(ctx context.Context, client runtimeiam.IAMAPI, accountID string, out io.Writer) ([]string, error) {
	for _, a := range append(append([]string{}, setupSageMakerRoles...), setupRevokeSageMaker...) {
		if err := runtimeiam.ValidateSageMakerRoleARN(a, accountID); err != nil {
			return nil, err
		}
	}
	existing, err := runtimeiam.ReadRuntimePolicy(ctx, client)
	switch {
	case err == nil:
	case errors.Is(err, runtimeiam.ErrRoleNotFound), errors.Is(err, runtimeiam.ErrPolicyNotFound):
		existing = ""
	default:
		return nil, fmt.Errorf("read deployed runtime policy (needed to preserve authorized SageMaker roles): %w", err)
	}
	var current []string
	if existing != "" {
		if current, err = runtimeiam.AuthorizedSageMakerRoles(existing); err != nil {
			return nil, err
		}
	}
	revoked := map[string]bool{}
	for _, a := range setupRevokeSageMaker {
		a = strings.TrimSpace(a)
		revoked[a] = true
		if !containsString(current, a) {
			_, _ = fmt.Fprintf(out, "note: SageMaker role %q was not authorized; nothing to revoke.\n", a)
		}
	}
	keep := map[string]bool{}
	for _, a := range append(current, setupSageMakerRoles...) {
		if a = strings.TrimSpace(a); a != "" && !revoked[a] {
			keep[a] = true
		}
	}
	final := make([]string, 0, len(keep))
	for a := range keep {
		final = append(final, a)
	}
	sort.Strings(final)
	return final, nil
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
