package deploy

// Migration away from CloudFormation (#154, #226).
//
// After the cutover `lagotto deploy` provisions the poller, the SNS topic and the
// schedule with direct SDK calls. All three have FIXED names, so on an account
// that already has a CloudFormation-deployed stack the SDK path ADOPTS the
// existing resources: nothing is duplicated and nothing is recreated. That part is
// automatic and needs no user action.
//
// What it leaves behind is a hazard: the stack still believes it owns those
// resources — and, depending on the template version that created it, more:
// legacy stacks also own the account-wide IAM roles every region's poller uses,
// and the oldest own the DynamoDB tables. `aws cloudformation delete-stack` would
// delete all of it. Detaching the stack is therefore OPT-IN (`lagotto deploy
// --migrate-from-cloudformation`) while the warning about it is automatic.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// migrateStableStatuses are the stack statuses a migration may start from. Any
// in-progress or failed status is refused: a change set against a stack that is
// mid-anything either fails or races, and a retain-update is the one operation
// that must not be attempted hopefully.
var migrateStableStatuses = map[cfntypes.StackStatus]bool{
	cfntypes.StackStatusCreateComplete:         true,
	cfntypes.StackStatusUpdateComplete:         true,
	cfntypes.StackStatusUpdateRollbackComplete: true,
}

// migrateCapabilities acknowledge everything a legacy template may contain: the
// SAM transform, and (in older templates) IAM roles, some with fixed names. A
// capability the template does not need is harmless.
var migrateCapabilities = []cfntypes.Capability{
	cfntypes.CapabilityCapabilityIam,
	cfntypes.CapabilityCapabilityNamedIam,
	cfntypes.CapabilityCapabilityAutoExpand,
}

// Polling bounds for the asynchronous CloudFormation operations (change-set
// creation, drift detection). Through d.sleep, so tests never wait.
const (
	migratePollInterval = 5 * time.Second
	migrateMaxPolls     = 180 // 15 minutes
)

// OwnedResource is one resource a legacy stack owns, per ListStackResources.
type OwnedResource struct {
	LogicalID  string
	PhysicalID string
	Type       string
}

// IAMRoleReader is the read-only IAM access the migration needs to verify the
// roles a legacy stack owns survive (an *iam.Client satisfies it).
type IAMRoleReader = iamRoleAPI

// MigrationReport is what a successful MigrateFromStack retained.
type MigrationReport struct {
	// Retained is every resource the stack owned. Each was verified to exist,
	// unchanged, after the stack was deleted.
	Retained []OwnedResource
}

// LegacyStackState reports whether a legacy CloudFormation stack still exists in a
// live state, along with its status.
//
// "Live" means "DescribeStacks returns it and it isn't on its way out". A stack
// mid-delete is reported as not live: there is nothing useful to tell the user
// about it and nothing for them to do.
//
// Callers run this AFTER a successful deploy/teardown, never before: it is
// advisory, and a DescribeStacks failure (no cloudformation:DescribeStacks
// permission, say) must never be what a user sees instead of their real error.
func (d *Deployer) LegacyStackState(ctx context.Context, stackName string) (bool, string, error) {
	exists, status, err := d.stackState(ctx, stackName)
	if err != nil {
		return false, "", err
	}
	if !exists {
		return false, "", nil
	}
	switch status {
	case cfntypes.StackStatusDeleteComplete, cfntypes.StackStatusDeleteInProgress:
		return false, string(status), nil
	}
	return true, string(status), nil
}

// LegacyStackAdoptionWarning is the message `lagotto deploy` prints when a legacy
// stack is still around. Pure, so its wording is testable and so it can be
// rendered without another AWS call.
//
// It has to say three things, because getting any of them wrong costs the user
// their poller: nothing was duplicated (adoption already happened), the stack
// still thinks it owns the resources — possibly including the account-wide IAM
// roles — and therefore `delete-stack` is dangerous.
func LegacyStackAdoptionWarning(stackName, accountID, region string) string {
	return fmt.Sprintf(`NOTE: a CloudFormation stack %q still exists in account %s/%s.
lagotto now manages the poller, SNS topic and schedule directly (#154) and has
ADOPTED the existing resources — nothing was duplicated or recreated.
The stack still believes it owns them, and a stack created by an older lagotto
may also own the account-wide IAM roles that EVERY region's poller uses (and,
older still, the DynamoDB tables). So
    aws cloudformation delete-stack --stack-name %s
would DELETE your live poller and whatever else the stack owns. Detach it with:
    lagotto deploy --migrate-from-cloudformation
which retains every resource the stack owns, refuses unless the change is
retain-policy-only, and verifies each resource survives the delete.`, stackName, accountID, region, stackName)
}

// LegacyStackTeardownWarning is the message `lagotto deploy --teardown` prints
// when a legacy stack is still around after the poller, topic and schedule have
// been deleted. It must not just say "delete-stack": a legacy stack may also own
// the account-wide IAM roles other regions' pollers still use.
func LegacyStackTeardownWarning(stackName string) string {
	return fmt.Sprintf(`NOTE: a CloudFormation stack %q still exists. The poller, topic and schedule
it believed it owned have just been deleted out from under it. Check what else it
owns before removing it:
    aws cloudformation list-stack-resources --stack-name %s
A stack created by an older lagotto may own the account-wide IAM roles
%s and %s, which
every region's poller (and every scheduled launch) uses; delete-stack would
delete them. If it owns nothing you still use:
    aws cloudformation delete-stack --stack-name %s`, stackName, stackName,
		runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName, stackName)
}

// MigrateFromStack detaches a legacy CloudFormation stack from EVERY resource it
// owns, then deletes the stack, leaving each of those resources in place and
// unchanged (#226).
//
// # Why not DeleteStack + RetainResources
//
// CloudFormation accepts `DeleteStack --retain-resources` ONLY for a stack already
// in DELETE_FAILED. The supported mechanism is to put `DeletionPolicy: Retain`
// (plus `UpdateReplacePolicy: Retain`) on the resources with a stack update, and
// only then delete.
//
// # Why the stack's own template, and a change set
//
// The first version (#154) retained a fixed three logical IDs and ran UpdateStack
// with lagotto's CURRENT template. On a legacy stack that also owned the shared
// IAM roles, that update REMOVED the roles from the template, so CloudFormation
// would have deleted them — breaking every region's poller — and any property
// that differed between the legacy and current templates was applied to the live
// poller unreviewed. Now:
//
//  1. DescribeStacks: require a stable status.
//  2. ListStackResources: what the stack really owns. Every type must be one the
//     migration can verify afterwards (verifiableTypes).
//  3. GetTemplate (Original): the stack's OWN template, YAML or JSON, SAM or not.
//     withRetainPolicies adds the two Retain policies to every resource and
//     proves it changed nothing else.
//  4. Fingerprint every owned resource's live state.
//  5. CreateChangeSet with every parameter at UsePreviousValue and the stack's
//     own tags, notification ARNs and service role. Refuse unless every change
//     is a Modify with Replacement False whose Static details touch only
//     DeletionPolicy/UpdateReplacePolicy (vetChangeSet).
//  6. Dynamic re-evaluations — see requireInSync.
//  7. GetTemplate (Processed) of the CHANGE SET: every owned logical ID,
//     including SAM-generated ones, must carry DeletionPolicy: Retain BEFORE
//     anything is executed.
//  8. ExecuteChangeSet, then confirm Retain on the stack's processed template
//     and that every owned resource is still unchanged.
//  9. DeleteStack, then verify every formerly owned resource still exists,
//     unchanged.
//
// Any refusal before step 8 deletes the change set and leaves the stack exactly
// as it was. A refusal after step 8 leaves a stack whose only change is the
// retain policies.
func (d *Deployer) MigrateFromStack(ctx context.Context, stackName string, roles IAMRoleReader) (*MigrationReport, error) {
	// --- 1. the stack -----------------------------------------------------------
	stack, err := d.describeLegacyStack(ctx, stackName)
	if err != nil {
		return nil, err
	}

	// --- 2. what it owns --------------------------------------------------------
	owned, err := d.listOwnedResources(ctx, stackName)
	if err != nil {
		return nil, err
	}
	ownedIDs := make([]string, 0, len(owned))
	for _, r := range owned {
		ownedIDs = append(ownedIDs, r.LogicalID)
	}

	// --- 3. the overlay on the stack's own template ----------------------------
	orig, err := d.cfn.GetTemplate(ctx, &cloudformation.GetTemplateInput{
		StackName:     aws.String(stackName),
		TemplateStage: cfntypes.TemplateStageOriginal,
	})
	if err != nil {
		return nil, fmt.Errorf("migrate: read the deployed template of stack %s: %w", stackName, err)
	}
	body, err := withRetainPolicies(aws.ToString(orig.TemplateBody))
	if err != nil {
		return nil, fmt.Errorf("migrate: stack %s: %w. Nothing was changed.\n%s", stackName, err, manualMigrationRecipe(stackName))
	}
	if len(body) > maxTemplateBody {
		return nil, fmt.Errorf("migrate: the retain overlay of stack %s is %d bytes, over CloudFormation's %d-byte inline limit. Nothing was changed.\n%s",
			stackName, len(body), maxTemplateBody, manualMigrationRecipe(stackName))
	}

	// --- 4. the live state to preserve -----------------------------------------
	before, err := d.snapshotOwned(ctx, roles, owned)
	if err != nil {
		return nil, fmt.Errorf("migrate: cannot read every resource stack %s owns, so their survival could not be verified; nothing was changed:\n%w", stackName, err)
	}

	// --- 5–7. the change set, vetted --------------------------------------------
	csName := "lagotto-retain-" + time.Now().UTC().Format("20060102-150405")
	noChanges, err := d.createRetainChangeSet(ctx, stack, csName, body)
	if err != nil {
		return nil, err
	}
	if noChanges {
		// Every resource already carried the policies. Step 8's check on the
		// stack's processed template is what confirms that; fall through to it.
		_ = d.discardChangeSet(ctx, stackName, csName)
	} else {
		if err := d.vetRetainChangeSet(ctx, stackName, csName, owned, ownedIDs); err != nil {
			if derr := d.discardChangeSet(ctx, stackName, csName); derr != nil {
				err = fmt.Errorf("%w\n(and the change set %s could not be deleted: %v)", err, csName, derr)
			}
			return nil, err
		}

		// --- 8. execute -----------------------------------------------------------
		if _, err := d.cfn.ExecuteChangeSet(ctx, &cloudformation.ExecuteChangeSetInput{
			StackName:     aws.String(stackName),
			ChangeSetName: aws.String(csName),
		}); err != nil {
			return nil, fmt.Errorf("migrate: execute the retain change set %s on stack %s: %w", csName, stackName, err)
		}
		w := cloudformation.NewStackUpdateCompleteWaiter(d.cfn)
		if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
			return nil, fmt.Errorf("migrate: waiting for the retain change set on stack %s: %w", stackName, err)
		}
	}

	// The safety gate that authorizes the delete: the stack's PROCESSED template
	// (what CloudFormation acts on; for SAM, the expanded resources) retains every
	// owned resource.
	processed, err := d.cfn.GetTemplate(ctx, &cloudformation.GetTemplateInput{
		StackName:     aws.String(stackName),
		TemplateStage: cfntypes.TemplateStageProcessed,
	})
	if err != nil {
		return nil, fmt.Errorf("migrate: read back the processed template of stack %s (refusing to delete without confirming the retain policies): %w", stackName, err)
	}
	if missing, err := resourcesMissingRetain(aws.ToString(processed.TemplateBody), ownedIDs); err != nil || len(missing) > 0 {
		if err != nil {
			missing = []string{fmt.Sprintf("(unparseable processed template: %v)", err)}
		}
		return nil, fmt.Errorf("migrate: ABORTED without deleting anything. The processed template of stack %s "+
			"does not carry 'DeletionPolicy: Retain' for %s, so deleting the stack would DELETE those live "+
			"resources. Nothing has been removed.\n%s", stackName, strings.Join(missing, ", "), manualMigrationRecipe(stackName))
	}
	if problems := d.changedSince(ctx, roles, owned, before); len(problems) > 0 {
		return nil, fmt.Errorf("migrate: ABORTED without deleting the stack: after the retain change set, these resources no longer match their state before it:\n  %s\n"+
			"The stack is retained-but-not-deleted. A schedule whose State changed may simply have been switched by a watch or the poller; re-run once it is settled",
			strings.Join(problems, "\n  "))
	}

	// --- 9. the detach delete, and the proof ---------------------------------
	if err := d.deleteStack(ctx, stackName); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	if problems := d.changedSince(ctx, roles, owned, before); len(problems) > 0 {
		return nil, fmt.Errorf("migrate: stack %s was deleted but these resources it owned did NOT survive unchanged:\n  %s\n"+
			"Re-run 'lagotto deploy' (and 'lagotto setup' for the IAM roles) to recreate what's missing — and note that a "+
			"RE-CREATED SCHEDULE COMES BACK DISABLED, so your watches will not be polled until you re-enable it by creating "+
			"a watch ('lagotto watch …'). Check 'lagotto doctor' before walking away",
			stackName, strings.Join(problems, "\n  "))
	}
	return &MigrationReport{Retained: owned}, nil
}

// describeLegacyStack returns the stack, refusing an absent or unstable one.
func (d *Deployer) describeLegacyStack(ctx context.Context, stackName string) (*cfntypes.Stack, error) {
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return nil, fmt.Errorf("migrate: no CloudFormation stack %q in this account/region — nothing to migrate from", stackName)
		}
		return nil, fmt.Errorf("migrate: describe stack %s: %w", stackName, err)
	}
	for i := range out.Stacks {
		if aws.ToString(out.Stacks[i].StackName) == stackName {
			st := out.Stacks[i]
			if !migrateStableStatuses[st.StackStatus] {
				return nil, fmt.Errorf("migrate: stack %s is %s; migration needs a stable stack (%s). "+
					"Let the in-flight operation finish, or resolve the failure, then re-run. "+
					"Your poller is unaffected either way — the SDK path has already adopted the resources",
					stackName, st.StackStatus, stableStatusList())
			}
			return &st, nil
		}
	}
	return nil, fmt.Errorf("migrate: no CloudFormation stack %q in this account/region — nothing to migrate from", stackName)
}

// listOwnedResources lists every resource the stack owns and refuses any the
// migration could not verify afterwards.
func (d *Deployer) listOwnedResources(ctx context.Context, stackName string) ([]OwnedResource, error) {
	var owned []OwnedResource
	var refuse []string
	var next *string
	for {
		out, err := d.cfn.ListStackResources(ctx, &cloudformation.ListStackResourcesInput{
			StackName: aws.String(stackName),
			NextToken: next,
		})
		if err != nil {
			return nil, fmt.Errorf("migrate: list the resources of stack %s: %w", stackName, err)
		}
		for _, s := range out.StackResourceSummaries {
			r := OwnedResource{
				LogicalID:  aws.ToString(s.LogicalResourceId),
				PhysicalID: aws.ToString(s.PhysicalResourceId),
				Type:       aws.ToString(s.ResourceType),
			}
			status := string(s.ResourceStatus)
			switch {
			case !verifiableTypes[r.Type]:
				refuse = append(refuse, fmt.Sprintf("%s (%s): lagotto cannot verify this resource type survives", r.LogicalID, r.Type))
			case r.PhysicalID == "":
				refuse = append(refuse, fmt.Sprintf("%s (%s): no physical resource", r.LogicalID, r.Type))
			case strings.Contains(status, "FAILED") || strings.Contains(status, "IN_PROGRESS"):
				refuse = append(refuse, fmt.Sprintf("%s (%s): status %s", r.LogicalID, r.Type, status))
			}
			owned = append(owned, r)
		}
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		next = out.NextToken
	}
	if len(refuse) > 0 {
		return nil, fmt.Errorf("migrate: refusing to detach stack %s; nothing was changed:\n  %s\n%s",
			stackName, strings.Join(refuse, "\n  "), manualMigrationRecipe(stackName))
	}
	if len(owned) == 0 {
		return nil, fmt.Errorf("migrate: stack %s owns no resources; there is nothing to retain. Delete it with: aws cloudformation delete-stack --stack-name %s", stackName, stackName)
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].LogicalID < owned[j].LogicalID })
	return owned, nil
}

// createRetainChangeSet creates the change set and waits for it. noChanges is
// true when CloudFormation reports the template changes nothing (the policies
// were already in place).
func (d *Deployer) createRetainChangeSet(ctx context.Context, stack *cfntypes.Stack, csName, body string) (noChanges bool, err error) {
	stackName := aws.ToString(stack.StackName)
	var params []cfntypes.Parameter
	for _, p := range stack.Parameters {
		if key := aws.ToString(p.ParameterKey); key != "" {
			params = append(params, cfntypes.Parameter{ParameterKey: aws.String(key), UsePreviousValue: aws.Bool(true)})
		}
	}
	if _, err := d.cfn.CreateChangeSet(ctx, &cloudformation.CreateChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(csName),
		ChangeSetType: cfntypes.ChangeSetTypeUpdate,
		Description:   aws.String("lagotto --migrate-from-cloudformation: add DeletionPolicy/UpdateReplacePolicy Retain only"),
		TemplateBody:  aws.String(body),
		// Every parameter keeps its value; in particular LambdaCodeBucket, whose
		// empty default would flip DeployLambda and DELETE the function.
		Parameters:   params,
		Capabilities: migrateCapabilities,
		// The stack's own tags, notification ARNs and service role, so the
		// change set cannot alter them.
		Tags:             stack.Tags,
		NotificationARNs: stack.NotificationARNs,
		RoleARN:          stack.RoleARN,
	}); err != nil {
		return false, fmt.Errorf("migrate: create the retain change set on stack %s: %w", stackName, err)
	}
	for i := 0; i < migrateMaxPolls; i++ {
		out, err := d.cfn.DescribeChangeSet(ctx, &cloudformation.DescribeChangeSetInput{
			StackName:     aws.String(stackName),
			ChangeSetName: aws.String(csName),
		})
		if err != nil {
			return false, fmt.Errorf("migrate: describe change set %s: %w", csName, err)
		}
		switch out.Status {
		case cfntypes.ChangeSetStatusCreateComplete:
			return false, nil
		case cfntypes.ChangeSetStatusFailed:
			reason := aws.ToString(out.StatusReason)
			if strings.Contains(reason, "didn't contain changes") || strings.Contains(reason, "No updates are to be performed") {
				return true, nil
			}
			_ = d.discardChangeSet(ctx, stackName, csName)
			return false, fmt.Errorf("migrate: CloudFormation could not create the retain change set on stack %s: %s. Nothing was changed", stackName, reason)
		}
		d.sleep(migratePollInterval)
	}
	_ = d.discardChangeSet(ctx, stackName, csName)
	return false, fmt.Errorf("migrate: change set %s on stack %s did not finish creating in time; nothing was changed", csName, stackName)
}

// discardChangeSet deletes an unexecuted change set (no clutter left behind).
func (d *Deployer) discardChangeSet(ctx context.Context, stackName, csName string) error {
	_, err := d.cfn.DeleteChangeSet(ctx, &cloudformation.DeleteChangeSetInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(csName),
	})
	return err
}

// vetRetainChangeSet is steps 5–7: the change set may only add the policies,
// any Dynamic re-evaluation must be provably a no-op, and the change set's
// processed template must retain every owned resource.
func (d *Deployer) vetRetainChangeSet(ctx context.Context, stackName, csName string, owned []OwnedResource, ownedIDs []string) error {
	var changes []cfntypes.Change
	var next *string
	for {
		out, err := d.cfn.DescribeChangeSet(ctx, &cloudformation.DescribeChangeSetInput{
			StackName:     aws.String(stackName),
			ChangeSetName: aws.String(csName),
			NextToken:     next,
		})
		if err != nil {
			return fmt.Errorf("migrate: describe change set %s: %w", csName, err)
		}
		changes = append(changes, out.Changes...)
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		next = out.NextToken
	}

	dynamic, problems := vetChangeSet(changes, owned)
	if len(problems) > 0 {
		return fmt.Errorf("migrate: REFUSED: the retain change set on stack %s would do more than add retain policies, so it was deleted unexecuted and nothing was changed:\n  %s\n%s",
			stackName, strings.Join(problems, "\n  "), manualMigrationRecipe(stackName))
	}
	if len(dynamic) > 0 {
		if err := d.requireInSync(ctx, stackName, dynamic); err != nil {
			return err
		}
	}

	tpl, err := d.cfn.GetTemplate(ctx, &cloudformation.GetTemplateInput{
		StackName:     aws.String(stackName),
		ChangeSetName: aws.String(csName),
		TemplateStage: cfntypes.TemplateStageProcessed,
	})
	if err != nil {
		return fmt.Errorf("migrate: read the processed template of change set %s (refusing to execute without confirming the retain policies): %w", csName, err)
	}
	missing, err := resourcesMissingRetain(aws.ToString(tpl.TemplateBody), ownedIDs)
	if err != nil {
		return fmt.Errorf("migrate: parse the processed template of change set %s: %w", csName, err)
	}
	if len(missing) > 0 {
		return fmt.Errorf("migrate: REFUSED: after the transform, the change set's processed template still does not retain %s "+
			"(a SAM-generated resource the policy did not reach?). It was deleted unexecuted and nothing was changed.\n%s",
			strings.Join(missing, ", "), manualMigrationRecipe(stackName))
	}
	return nil
}

// dynamicChange is a resource whose properties CloudFormation will re-evaluate
// at execution time.
type dynamicChange struct {
	LogicalID  string
	Type       string
	Properties []string
}

// vetChangeSet checks that a change set only adds retain policies. It returns
// the resources with Dynamic re-evaluations, for requireInSync, and every
// reason to refuse.
//
// Accepted: a Resource change that is a Modify, Replacement False, whose
// details are each either
//   - Static, on the DeletionPolicy or UpdateReplacePolicy attribute; or
//   - Dynamic, on Properties, never requiring recreation, caused by a reference
//     to another resource (a dependent's !Ref/!GetAtt of a resource the change
//     set modifies is re-resolved at execution).
//
// Everything else is refused: Add/Remove/Import, any Replacement other than
// False, a Static property/metadata/tag change, a Dynamic change from a
// parameter or a direct modification, a Modify with no details at all (nothing
// to prove it policy-only), a change to a resource the stack does not list, and
// a Dynamic re-evaluation of a Lambda function's Code (no live comparison can
// see code, and re-applying the stack's stored Code would push the stack's old
// zip onto the live poller).
func vetChangeSet(changes []cfntypes.Change, owned []OwnedResource) ([]dynamicChange, []string) {
	types := map[string]string{}
	for _, r := range owned {
		types[r.LogicalID] = r.Type
	}
	var dynamic []dynamicChange
	var problems []string
	for _, c := range changes {
		rc := c.ResourceChange
		if c.Type != cfntypes.ChangeTypeResource || rc == nil {
			problems = append(problems, fmt.Sprintf("a non-resource change (%s)", c.Type))
			continue
		}
		id := aws.ToString(rc.LogicalResourceId)
		if _, ok := types[id]; !ok {
			problems = append(problems, fmt.Sprintf("%s: %s of a resource the stack does not own", id, rc.Action))
			continue
		}
		if rc.Action != cfntypes.ChangeActionModify {
			problems = append(problems, fmt.Sprintf("%s: %s (only Modify is a policy change)", id, rc.Action))
			continue
		}
		if rc.Replacement != cfntypes.ReplacementFalse {
			problems = append(problems, fmt.Sprintf("%s: Replacement %q", id, rc.Replacement))
			continue
		}
		if len(rc.Details) == 0 {
			problems = append(problems, fmt.Sprintf("%s: a Modify with no details, which cannot be shown to be policy-only", id))
			continue
		}
		var dynProps []string
		for _, det := range rc.Details {
			t := det.Target
			if t == nil {
				problems = append(problems, fmt.Sprintf("%s: a change detail with no target", id))
				continue
			}
			name := aws.ToString(t.Name)
			switch {
			case det.Evaluation == cfntypes.EvaluationTypeStatic &&
				(t.Attribute == cfntypes.ResourceAttributeDeletionPolicy || t.Attribute == cfntypes.ResourceAttributeUpdateReplacePolicy):
				// the change we asked for
			case det.Evaluation == cfntypes.EvaluationTypeDynamic &&
				t.Attribute == cfntypes.ResourceAttributeProperties &&
				t.RequiresRecreation == cfntypes.RequiresRecreationNever &&
				(det.ChangeSource == cfntypes.ChangeSourceResourceReference || det.ChangeSource == cfntypes.ChangeSourceResourceAttribute):
				if types[id] == "AWS::Lambda::Function" && name == "Code" {
					problems = append(problems, fmt.Sprintf("%s: a Dynamic re-evaluation of Code (it would re-apply the stack's stored code location to the live poller)", id))
					continue
				}
				dynProps = append(dynProps, name)
			default:
				problems = append(problems, fmt.Sprintf("%s: %s change to %s %q (source %s, recreation %s)",
					id, det.Evaluation, t.Attribute, name, det.ChangeSource, t.RequiresRecreation))
			}
		}
		if len(dynProps) > 0 {
			dynamic = append(dynamic, dynamicChange{LogicalID: id, Type: types[id], Properties: dynProps})
		}
	}
	return dynamic, problems
}

// requireInSync handles Dynamic re-evaluations (step 6).
//
// When a change set modifies a resource — even only its DeletionPolicy — every
// dependent that !Ref/!GetAtt's it shows a Dynamic re-evaluation: CloudFormation
// re-resolves the reference at execution, and if it decides the dependent needs
// updating it applies the dependent's STORED properties (for Lambda and
// Scheduler, a full PUT). For a stack whose resources lagotto's SDK path has
// adopted and since managed, the stored properties may no longer be what is
// live: a stale environment, an old role, a schedule stored DISABLED while a
// watch has it ENABLED. Re-applying them would silently revert the live poller.
//
// No CloudFormation option suppresses the re-evaluation, and the reference
// cannot be removed without a property change. What CloudFormation DOES offer is
// a comparison of exactly the stored values with the live resource: drift
// detection (DetectStackDrift over just these logical IDs). A resource that is
// IN_SYNC has stored == live for every property the template sets, so whatever
// the re-evaluation applies is what is already there: a no-op. Anything else —
// MODIFIED, DELETED, NOT_CHECKED, or a detection that fails — is refused, with
// the differing properties. vetChangeSet has already refused the one property
// drift detection cannot see (a Lambda function's Code). And step 8 checks
// lagotto's own fingerprints of the live resources after execution, so a
// re-evaluation that changed anything still stops the delete.
func (d *Deployer) requireInSync(ctx context.Context, stackName string, dynamic []dynamicChange) error {
	ids := make([]string, 0, len(dynamic))
	for _, c := range dynamic {
		ids = append(ids, c.LogicalID)
	}
	det, err := d.cfn.DetectStackDrift(ctx, &cloudformation.DetectStackDriftInput{
		StackName:          aws.String(stackName),
		LogicalResourceIds: ids,
	})
	if err != nil {
		return fmt.Errorf("migrate: REFUSED: the change set re-evaluates %s at execution, and comparing their stored "+
			"properties with the live resources (drift detection) failed: %w. Nothing was changed", strings.Join(ids, ", "), err)
	}
	status := cfntypes.StackDriftDetectionStatusDetectionInProgress
	reason := ""
	for i := 0; i < migrateMaxPolls && status == cfntypes.StackDriftDetectionStatusDetectionInProgress; i++ {
		out, err := d.cfn.DescribeStackDriftDetectionStatus(ctx, &cloudformation.DescribeStackDriftDetectionStatusInput{
			StackDriftDetectionId: det.StackDriftDetectionId,
		})
		if err != nil {
			return fmt.Errorf("migrate: drift detection status: %w", err)
		}
		status, reason = out.DetectionStatus, aws.ToString(out.DetectionStatusReason)
		if status == cfntypes.StackDriftDetectionStatusDetectionInProgress {
			d.sleep(migratePollInterval)
		}
	}
	if status == cfntypes.StackDriftDetectionStatusDetectionInProgress {
		return fmt.Errorf("migrate: REFUSED: drift detection on stack %s did not finish in time; nothing was changed", stackName)
	}

	drifts := map[string]cfntypes.StackResourceDrift{}
	var next *string
	for {
		out, err := d.cfn.DescribeStackResourceDrifts(ctx, &cloudformation.DescribeStackResourceDriftsInput{
			StackName: aws.String(stackName),
			NextToken: next,
		})
		if err != nil {
			return fmt.Errorf("migrate: read drift results for stack %s: %w", stackName, err)
		}
		for _, dr := range out.StackResourceDrifts {
			id := aws.ToString(dr.LogicalResourceId)
			if prev, ok := drifts[id]; !ok || aws.ToTime(dr.Timestamp).After(aws.ToTime(prev.Timestamp)) {
				drifts[id] = dr
			}
		}
		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}
		next = out.NextToken
	}

	var problems []string
	for _, c := range dynamic {
		dr, ok := drifts[c.LogicalID]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s (%s): no drift result", c.LogicalID, c.Type))
			continue
		}
		if dr.StackResourceDriftStatus == cfntypes.StackResourceDriftStatusInSync {
			continue
		}
		line := fmt.Sprintf("%s (%s, re-evaluates %s): %s", c.LogicalID, c.Type, strings.Join(c.Properties, ", "), dr.StackResourceDriftStatus)
		for _, pd := range dr.PropertyDifferences {
			line += fmt.Sprintf("\n      %s: stored %s, live %s", aws.ToString(pd.PropertyPath), aws.ToString(pd.ExpectedValue), aws.ToString(pd.ActualValue))
		}
		problems = append(problems, line)
	}
	if len(problems) > 0 {
		msg := fmt.Sprintf("migrate: REFUSED: executing the retain change set on stack %s would make CloudFormation re-apply "+
			"its STORED properties to resources that no longer match them, reverting the live resources:\n  %s\n",
			stackName, strings.Join(problems, "\n  "))
		if reason != "" && status == cfntypes.StackDriftDetectionStatusDetectionFailed {
			msg += "(drift detection: " + reason + ")\n"
		}
		msg += "The change set was deleted unexecuted and nothing was changed. If the difference is a schedule's State, re-run when no watch is active (the stack stores it DISABLED).\n" +
			manualMigrationRecipe(stackName)
		return errors.New(msg)
	}
	return nil
}

// manualMigrationRecipe is the by-hand procedure, for every refusal.
func manualMigrationRecipe(stackName string) string {
	return fmt.Sprintf(`To detach it by hand (the procedure this command automates):
    aws cloudformation list-stack-resources --stack-name %[1]s
    aws cloudformation get-template --stack-name %[1]s --template-stage Original
    # add ONLY 'DeletionPolicy: Retain' and 'UpdateReplacePolicy: Retain' to EVERY resource
    aws cloudformation create-change-set --stack-name %[1]s --change-set-name retain \
        --template-body file://retained.yaml --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --parameters ParameterKey=<each>,UsePreviousValue=true ...
    aws cloudformation describe-change-set --stack-name %[1]s --change-set-name retain
    # every change must be a Modify, Replacement False, touching only the two policies;
    # compare any Dynamic re-evaluation's stored properties with the live resource
    aws cloudformation execute-change-set --stack-name %[1]s --change-set-name retain
    aws cloudformation get-template --stack-name %[1]s --template-stage Processed   # Retain on every resource
    aws cloudformation delete-stack --stack-name %[1]s`, stackName)
}

// stableStatusList renders migrateStableStatuses for an error message.
func stableStatusList() string {
	return "CREATE_COMPLETE, UPDATE_COMPLETE or UPDATE_ROLLBACK_COMPLETE"
}
