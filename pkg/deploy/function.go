package deploy

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// Action strings returned by EnsurePollerFunction / EnsurePollerSchedule, so a
// caller can report what actually happened without re-deriving it.
const (
	ActionCreated       = "created"
	ActionCodeUpdated   = "code updated"
	ActionConfigUpdated = "config updated"
	ActionUnchanged     = "unchanged"
)

// PollerFunctionInput is everything EnsurePollerFunction needs that isn't a fixed
// constant.
//
// NOTE on the word "environment": EnvVars is the Lambda Environment.Variables map
// the poller reads at runtime (WATCHES_TABLE, SNS_TOPIC_ARN, …). That is a
// completely different thing from lagotto's `--environment` flag, which is the
// production/staging/development *tag* value and arrives here inside Tags.
type PollerFunctionInput struct {
	RoleARN string // execution role ARN (runtimeiam.RoleARN)
	Bucket  string // S3 bucket holding the poller zip
	Key     string // S3 key of the poller zip
	// CodeSHA256 is the base64-std SHA-256 of the uploaded zip, from
	// uploadArtifact. Optional: empty means "always push the code". Purely an
	// optimization — see uploadArtifact's comment.
	CodeSHA256 string
	EnvVars    map[string]string
	Tags       map[string]string
}

// PollerEnvVars builds the poller's Environment.Variables map. This is the
// env-var contract the hosted poller depends on; it mirrors the CFN template's
// Environment block, and every key here is read by lambda/capacity-poller:
//
//	WATCHES_TABLE, HISTORY_TABLE, SCHEDULED_TABLE  → the CLI-owned tables (#59)
//	SNS_TOPIC_ARN                                  → capacity alerts (empty = no notifier)
//	SCHEDULE_NAME                                  → the schedule the poller self-disables
//	POLLER_FUNCTION_ARN, SCHEDULER_INVOKE_ROLE_ARN → re-arming a #62 boundary retry
//
// A MISSING VARIABLE SILENTLY DEGRADES THE POLLER rather than failing it (the
// table names have defaults; an empty SNS_TOPIC_ARN just means no notifications;
// empty ARNs mean a boundary retry can't re-arm), so the set is built in one
// place and asserted in tests. AUTO_DELETE_TABLES is deliberately NOT set here:
// it's an opt-in a human adds by hand, exactly as with the template.
func PollerEnvVars(region, accountID, watchesTable, historyTable, scheduledTable, topicARN string) map[string]string {
	return map[string]string{
		"WATCHES_TABLE":             orDefault(watchesTable, DefaultWatchesTable),
		"HISTORY_TABLE":             orDefault(historyTable, DefaultHistoryTable),
		"SCHEDULED_TABLE":           orDefault(scheduledTable, DefaultScheduledTable),
		"SNS_TOPIC_ARN":             topicARN,
		"SCHEDULE_NAME":             PollerScheduleName,
		"POLLER_FUNCTION_ARN":       PollerFunctionARN(region, accountID),
		"SCHEDULER_INVOKE_ROLE_ARN": schedulerInvokeRoleARN(accountID),
	}
}

// PollerTags mirrors the template's function tags.
func PollerTags(env string) map[string]string {
	if env == "" {
		env = "production"
	}
	return map[string]string{
		"Environment": env,
		"Application": "lagotto",
		"Component":   "capacity-poller",
	}
}

// EnsurePollerFunction get-or-creates the lagotto-capacity-poller Lambda and
// converges it onto the shape the CFN/SAM template used to declare. Returns the
// function ARN and which of ActionCreated / ActionCodeUpdated /
// ActionConfigUpdated / ActionUnchanged happened.
func (d *Deployer) EnsurePollerFunction(ctx context.Context, in PollerFunctionInput) (string, string, error) {
	cur, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	})
	if err != nil {
		var notFound *lambdatypes.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			return "", "", fmt.Errorf("look up function %s: %w", PollerFunctionName, err)
		}
		arn, cErr := d.createPollerFunction(ctx, in)
		if cErr != nil {
			return arn, "", cErr
		}
		return arn, ActionCreated, nil
	}
	if cur.Configuration == nil {
		return "", "", fmt.Errorf("look up function %s: no configuration returned", PollerFunctionName)
	}
	return d.updatePollerFunction(ctx, in, cur.Configuration)
}

// RequirePollerDeployed confirms the hosted poller Lambda actually exists in the
// caller's account, so a command that only needs the poller's ARN (rather than to
// create it) can fail early with an actionable message.
//
// This is what `lagotto launch` checks instead of reading CloudFormation stack
// outputs (#154): every poller resource has a fixed name, so the ARNs are
// derivable — but derivable is not the same as present. A GetFunction tests the
// thing launch actually depends on, which the old stack-outputs check did not: a
// stack keeps reporting its outputs even after the function has been deleted out
// from under it.
//
// region and accountID are only used to make the "not deployed" message name the
// place the user should look; they're passed in because a Deployer doesn't retain
// the aws.Config it was built from.
//
// An error other than ResourceNotFoundException is wrapped as-is: an AccessDenied
// on GetFunction must NOT be reported as "the poller isn't deployed", or the user
// goes off re-running `lagotto deploy` to fix a permissions problem.
func (d *Deployer) RequirePollerDeployed(ctx context.Context, region, accountID string) error {
	_, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	})
	if err == nil {
		return nil
	}
	var notFound *lambdatypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return fmt.Errorf("the hosted poller isn't deployed in account %s / %s (no Lambda function %q) — run 'lagotto deploy' first",
			accountID, region, PollerFunctionName)
	}
	return fmt.Errorf("check whether function %s exists: %w", PollerFunctionName, err)
}

// createPollerFunction creates the function from scratch.
func (d *Deployer) createPollerFunction(ctx context.Context, in PollerFunctionInput) (string, error) {
	var arn string
	// The IAM-role-propagation race: `lagotto deploy` calls
	// runtimeiam.EnsureRoles moments before this, and a freshly created role is
	// commonly not yet visible to Lambda, which refuses the create with
	// InvalidParameterValueException "The role defined for the function cannot be
	// assumed by Lambda". It's transient — the fix is simply to wait. Matched
	// NARROWLY (typed exception AND that message) so a genuinely bad parameter
	// still fails fast instead of burning a minute of backoff.
	err := d.retryTransient(ctx, rolePropagationAttempts, isRoleNotYetAssumable, func() error {
		out, cErr := d.lambda.CreateFunction(ctx, &lambda.CreateFunctionInput{
			FunctionName:  aws.String(PollerFunctionName),
			Role:          aws.String(in.RoleARN),
			Runtime:       lambdatypes.Runtime(pollerRuntime),
			Handler:       aws.String(pollerHandler),
			Timeout:       aws.Int32(pollerTimeout),
			MemorySize:    aws.Int32(pollerMemorySize),
			PackageType:   lambdatypes.PackageTypeZip,
			Architectures: []lambdatypes.Architecture{lambdatypes.Architecture(pollerArch)},
			Code: &lambdatypes.FunctionCode{
				S3Bucket: aws.String(in.Bucket),
				S3Key:    aws.String(in.Key),
			},
			Environment: &lambdatypes.Environment{Variables: in.EnvVars},
			Tags:        in.Tags,
		})
		if cErr != nil {
			return cErr
		}
		arn = aws.ToString(out.FunctionArn)
		return nil
	})
	if err != nil {
		if isRoleNotYetAssumable(err) {
			return "", fmt.Errorf("create function %s: IAM role propagation — role %s is not yet assumable by Lambda after %d attempts; re-run `lagotto deploy` in a moment: %w",
				PollerFunctionName, in.RoleARN, rolePropagationAttempts, err)
		}
		return "", fmt.Errorf("create function %s: %w", PollerFunctionName, err)
	}
	// FunctionActiveV2Waiter is the SDK waiter we DO use — it matches on
	// State == Active, which every endpoint populates (unlike LastUpdateStatus;
	// see waitFunctionSettled). Wait so a schedule can safely target the function.
	w := lambda.NewFunctionActiveV2Waiter(d.lambda)
	if err := w.Wait(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(PollerFunctionName)}, activeMaxWait); err != nil {
		return arn, fmt.Errorf("waiting for function %s to become active: %w", PollerFunctionName, err)
	}
	return arn, nil
}

// updatePollerFunction converges an existing function: code, then configuration,
// then tags. Each step is skipped when it would be a no-op — that's what makes a
// re-run cheap and avoids pointless revision churn.
func (d *Deployer) updatePollerFunction(ctx context.Context, in PollerFunctionInput, cur *lambdatypes.FunctionConfiguration) (string, string, error) {
	arn := aws.ToString(cur.FunctionArn)

	// A function created seconds ago, or a concurrent deploy, leaves an update in
	// flight; any Update* call then fails with ResourceConflictException
	// "...An update is in progress for resource...". Settle first.
	if err := d.waitFunctionSettled(ctx, PollerFunctionName); err != nil {
		return arn, "", err
	}

	action := ActionUnchanged

	if in.CodeSHA256 == "" || in.CodeSHA256 != aws.ToString(cur.CodeSha256) {
		if err := d.retryTransient(ctx, conflictAttempts, isResourceConflict, func() error {
			_, err := d.lambda.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{
				FunctionName: aws.String(PollerFunctionName),
				S3Bucket:     aws.String(in.Bucket),
				S3Key:        aws.String(in.Key),
				// Passing Architectures on UpdateFunctionCode is the ONLY way to
				// correct a function that somehow ended up x86_64:
				// UpdateFunctionConfiguration cannot change the architecture.
				Architectures: []lambdatypes.Architecture{lambdatypes.Architecture(pollerArch)},
			})
			return err
		}); err != nil {
			return arn, "", fmt.Errorf("update code of function %s: %w", PollerFunctionName, err)
		}
		if err := d.waitFunctionSettled(ctx, PollerFunctionName); err != nil {
			return arn, "", err
		}
		action = ActionCodeUpdated
	}

	if pollerConfigDiffers(in, cur) {
		if err := d.retryTransient(ctx, conflictAttempts, isResourceConflict, func() error {
			_, err := d.lambda.UpdateFunctionConfiguration(ctx, &lambda.UpdateFunctionConfigurationInput{
				FunctionName: aws.String(PollerFunctionName),
				Role:         aws.String(in.RoleARN),
				Runtime:      lambdatypes.Runtime(pollerRuntime),
				Handler:      aws.String(pollerHandler),
				Timeout:      aws.Int32(pollerTimeout),
				MemorySize:   aws.Int32(pollerMemorySize),
				// Lambda REPLACES Environment.Variables wholesale rather than
				// merging, so the full desired map must be resupplied. This matches
				// what CloudFormation did (so it's no regression), but it does mean a
				// hand-added variable — AUTO_DELETE_TABLES being the realistic one —
				// is wiped by a deploy that has to touch the config.
				Environment: &lambdatypes.Environment{Variables: in.EnvVars},
			})
			return err
		}); err != nil {
			return arn, "", fmt.Errorf("update configuration of function %s: %w", PollerFunctionName, err)
		}
		if err := d.waitFunctionSettled(ctx, PollerFunctionName); err != nil {
			return arn, "", err
		}
		if action == ActionUnchanged {
			action = ActionConfigUpdated
		}
	}

	// Lambda tags are not part of UpdateFunctionConfiguration, so they need their
	// own (idempotent) upsert — this is what makes a changed --environment retag
	// an existing function.
	if len(in.Tags) > 0 && arn != "" {
		if _, err := d.lambda.TagResource(ctx, &lambda.TagResourceInput{
			Resource: aws.String(arn),
			Tags:     in.Tags,
		}); err != nil {
			return arn, "", fmt.Errorf("tag function %s: %w", PollerFunctionName, err)
		}
	}
	return arn, action, nil
}

// pollerConfigDiffers reports whether any converged configuration field of the
// live function differs from the desired shape.
func pollerConfigDiffers(in PollerFunctionInput, cur *lambdatypes.FunctionConfiguration) bool {
	if aws.ToString(cur.Role) != in.RoleARN ||
		aws.ToString(cur.Handler) != pollerHandler ||
		string(cur.Runtime) != pollerRuntime ||
		aws.ToInt32(cur.Timeout) != pollerTimeout ||
		aws.ToInt32(cur.MemorySize) != pollerMemorySize {
		return true
	}
	var live map[string]string
	if cur.Environment != nil {
		live = cur.Environment.Variables
	}
	if len(live) != len(in.EnvVars) {
		return true
	}
	for k, v := range in.EnvVars {
		if live[k] != v {
			return true
		}
	}
	return false
}

// isRoleNotYetAssumable matches ONLY the IAM-propagation flavour of
// InvalidParameterValueException. Both halves matter: the typed exception covers
// a broad family of genuinely permanent parameter problems, so the message check
// is what keeps a real mistake from being retried for a minute.
func isRoleNotYetAssumable(err error) bool {
	var ipv *lambdatypes.InvalidParameterValueException
	if !errors.As(err, &ipv) {
		return false
	}
	return strings.Contains(err.Error(), "cannot be assumed")
}

// isResourceConflict matches Lambda's "an update is in progress" conflict.
func isResourceConflict(err error) bool {
	var rc *lambdatypes.ResourceConflictException
	return errors.As(err, &rc)
}

// pollerFunctionExists reports whether the poller Lambda is there. Teardown uses
// it to report what it ACTUALLY deleted: DeleteFunction alone can't tell you,
// because deletePollerFunction (correctly) treats an absent function as success.
// An error other than ResourceNotFoundException is returned, so an AccessDenied
// fails the teardown instead of being reported as "nothing to delete".
func (d *Deployer) pollerFunctionExists(ctx context.Context) (bool, error) {
	_, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	})
	if err == nil {
		return true, nil
	}
	var notFound *lambdatypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, fmt.Errorf("check whether function %s exists: %w", PollerFunctionName, err)
}

// deletePollerFunction removes the poller Lambda, tolerating an absent function.
func (d *Deployer) deletePollerFunction(ctx context.Context) error {
	_, err := d.lambda.DeleteFunction(ctx, &lambda.DeleteFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	})
	if err == nil {
		return nil
	}
	var notFound *lambdatypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return nil
	}
	return fmt.Errorf("delete function %s: %w", PollerFunctionName, err)
}
