package deploy

// Migration away from CloudFormation (#154).
//
// After the cutover `lagotto deploy` provisions the poller, the SNS topic and the
// schedule with direct SDK calls. All three have FIXED names, so on an account
// that already has a CloudFormation-deployed stack the SDK path ADOPTS the
// existing resources: nothing is duplicated and nothing is recreated. That part is
// automatic and needs no user action.
//
// What it leaves behind is a bookkeeping hazard, not a functional one: the stack
// still believes it owns those three resources, so `aws cloudformation
// delete-stack` would delete a live poller. Detaching the stack is therefore
// OPT-IN (`lagotto deploy --migrate-from-cloudformation`) while the warning about
// it is automatic.

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

// retainLogicalIDs are the template's logical IDs for the three resources the SDK
// path has adopted, i.e. exactly the ones that must survive the detach delete.
// They are the logical IDs in deployment/cloudformation/lagotto-stack.yaml; a
// rename there without a change here is caught by TestRetainOverlay_*.
var retainLogicalIDs = []string{
	"LagottoCapacityAlertsTopic",
	"CapacityPollerFunction",
	"CapacityPollerSchedule",
}

// migrateStableStatuses are the stack statuses a migration may start from. Any
// in-progress or failed status is refused: an UpdateStack against a stack that is
// mid-anything either fails or races, and a retain-update is the one operation
// that must not be attempted hopefully.
var migrateStableStatuses = map[cfntypes.StackStatus]bool{
	cfntypes.StackStatusCreateComplete:         true,
	cfntypes.StackStatusUpdateComplete:         true,
	cfntypes.StackStatusUpdateRollbackComplete: true,
}

// Indentation the template uses: two spaces for a resource's logical ID, four for
// its properties. The overlay is inserted at the property level.
const (
	resourceIndent = "  "
	propertyIndent = "    "
)

var (
	// resourceHeaderRE matches `  SomeLogicalId:` — a resource (or parameter, or
	// output) key at the top nesting level of its section.
	resourceHeaderRE = regexp.MustCompile(`^` + resourceIndent + `([A-Za-z0-9]+):\s*$`)
	// resourceTypeRE matches `    Type: AWS::Something::Something`. Requiring the
	// AWS:: prefix is what keeps a Parameter's `Type: String` from matching.
	resourceTypeRE = regexp.MustCompile(`^` + propertyIndent + `Type:\s*(AWS::\S+)\s*$`)
	// retainRE matches an already-present retain policy line at property level.
	retainRE = regexp.MustCompile(`^` + propertyIndent + `(DeletionPolicy|UpdateReplacePolicy):\s*Retain\s*$`)
)

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
// still thinks it owns the resources, and therefore `delete-stack` is dangerous.
func LegacyStackAdoptionWarning(stackName, accountID, region string) string {
	return fmt.Sprintf(`NOTE: a CloudFormation stack %q still exists in account %s/%s.
lagotto now manages the poller, SNS topic and schedule directly (#154) and has
ADOPTED the existing resources — nothing was duplicated or recreated.
The stack still believes it owns them, so
    aws cloudformation delete-stack --stack-name %s
would DELETE your live poller. Detach it safely with:
    lagotto deploy --migrate-from-cloudformation`, stackName, accountID, region, stackName)
}

// LegacyStackTeardownWarning is the message `lagotto deploy --teardown` prints
// when a legacy stack is still around after the resources have been deleted. The
// stack is now an empty shell whose resources are gone, and a plain delete-stack
// is the right way to be rid of it — the opposite of the deploy-time advice, which
// is why it's a separate message.
func LegacyStackTeardownWarning(stackName string) string {
	return fmt.Sprintf(`NOTE: a CloudFormation stack %q still exists. Its three resources have just
been deleted out from under it, so it is now an empty shell. Remove it with:
    aws cloudformation delete-stack --stack-name %s`, stackName, stackName)
}

// MigrateFromStack detaches an existing CloudFormation stack from the three poller
// resources the SDK path has adopted, then deletes the stack — leaving the live
// poller, topic and schedule untouched.
//
// # Why not DeleteStack + RetainResources
//
// Because CloudFormation does not allow it. `DeleteStack --retain-resources` is
// accepted ONLY for a stack already in DELETE_FAILED; on a healthy
// CREATE_COMPLETE/UPDATE_COMPLETE stack it is rejected. The supported mechanism is
// to put `DeletionPolicy: Retain` (plus `UpdateReplacePolicy: Retain`) on the
// resources with an UpdateStack, and only then delete.
//
// # The sequence
//
//  1. DescribeStacks — require a stable status, refuse anything else with guidance.
//  2. UpdateStack with the retain overlay, carrying the stack's existing parameter
//     values across as UsePreviousValue. "No updates are to be performed" is NOT
//     treated as success-by-another-name; it falls through to step 3.
//  3. SAFETY GATE: GetTemplate(TemplateStage: Original) and require that the
//     DEPLOYED body carries DeletionPolicy: Retain for all three logical IDs. If
//     it doesn't, abort WITHOUT deleting and print the manual commands. This is
//     the step that makes 4 safe even if CloudFormation silently no-ops a
//     DeletionPolicy-only diff.
//  4. DeleteStack + wait.
//  5. Verify adoption survived: the function, schedule and topic must all still
//     resolve. If any vanished, fail loudly.
func (d *Deployer) MigrateFromStack(ctx context.Context, stackName string) error {
	// --- 1. the stack, and its current parameters -----------------------------
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{
		StackName: aws.String(stackName),
	})
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			return fmt.Errorf("migrate: no CloudFormation stack %q in this account/region — nothing to migrate from", stackName)
		}
		return fmt.Errorf("migrate: describe stack %s: %w", stackName, err)
	}
	var stack *cfntypes.Stack
	for i := range out.Stacks {
		if aws.ToString(out.Stacks[i].StackName) == stackName {
			stack = &out.Stacks[i]
			break
		}
	}
	if stack == nil {
		return fmt.Errorf("migrate: no CloudFormation stack %q in this account/region — nothing to migrate from", stackName)
	}
	if !migrateStableStatuses[stack.StackStatus] {
		return fmt.Errorf("migrate: stack %s is %s; migration needs a stable stack (%s). "+
			"Let the in-flight operation finish, or resolve the failure, then re-run. "+
			"Your poller is unaffected either way — the SDK path has already adopted the resources",
			stackName, stack.StackStatus, stableStatusList())
	}

	// The topic ARN comes from the stack's own output rather than being
	// reconstructed: this verifies the exact resource the stack owned.
	topicARN := ""
	for _, o := range stack.Outputs {
		if aws.ToString(o.OutputKey) == "CapacityAlertsTopicArn" {
			topicARN = aws.ToString(o.OutputValue)
		}
	}

	// --- 2. the retain update -------------------------------------------------
	body, err := withRetainPolicies(cfn.StackTemplate)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	params := previousParameters(stack.Parameters, templateParameterNames(cfn.StackTemplate))
	_, err = d.cfn.UpdateStack(ctx, &cloudformation.UpdateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(body),
		Parameters:   params,
		Capabilities: deployCapabilities,
	})
	switch {
	case err == nil:
		w := cloudformation.NewStackUpdateCompleteWaiter(d.cfn)
		if wErr := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); wErr != nil {
			return fmt.Errorf("migrate: waiting for the retain-update of stack %s: %w", stackName, wErr)
		}
	case strings.Contains(err.Error(), "No updates are to be performed"):
		// NOT a green light. CloudFormation says this both when the retain policies
		// are already in place (fine) and when it decided the diff was a no-op
		// (not fine). The GetTemplate gate below is what distinguishes the two, so
		// fall through to it rather than guessing here.
	default:
		return fmt.Errorf("migrate: retain-update of stack %s: %w", stackName, err)
	}

	// --- 3. the safety gate ---------------------------------------------------
	tpl, err := d.cfn.GetTemplate(ctx, &cloudformation.GetTemplateInput{
		StackName:     aws.String(stackName),
		TemplateStage: cfntypes.TemplateStageOriginal,
	})
	if err != nil {
		return fmt.Errorf("migrate: read back the template of stack %s (refusing to delete without confirming the retain policies): %w", stackName, err)
	}
	if missing := logicalIDsMissingRetain(aws.ToString(tpl.TemplateBody)); len(missing) > 0 {
		return fmt.Errorf("migrate: ABORTED without deleting anything. The deployed template of stack %s "+
			"does not carry 'DeletionPolicy: Retain' for %s, so deleting the stack would DELETE those live "+
			"resources. Nothing has been removed and your poller is untouched.\n"+
			"Do it by hand if you need to:\n"+
			"    aws cloudformation get-template --stack-name %s --template-stage Original > lagotto-stack.yaml\n"+
			"    # add 'DeletionPolicy: Retain' and 'UpdateReplacePolicy: Retain' to %s\n"+
			"    aws cloudformation update-stack --stack-name %s --template-body file://lagotto-stack.yaml \\\n"+
			"        --capabilities CAPABILITY_AUTO_EXPAND --parameters ParameterKey=LambdaCodeBucket,UsePreviousValue=true ...\n"+
			"    aws cloudformation get-template --stack-name %s --template-stage Original   # verify\n"+
			"    aws cloudformation delete-stack --stack-name %s",
			stackName, strings.Join(missing, ", "), stackName,
			strings.Join(missing, ", "), stackName, stackName, stackName)
	}

	// --- 4. the detach delete -------------------------------------------------
	if err := d.deleteStack(ctx, stackName); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// --- 5. verify adoption survived -----------------------------------------
	return d.verifyPollerSurvived(ctx, stackName, topicARN)
}

// verifyPollerSurvived re-reads the three resources after the stack delete. A
// retain policy that didn't take would show up here as a resource that vanished,
// and the user needs to hear that immediately and in full — including the
// non-obvious part, that a re-created schedule comes back DISABLED.
func (d *Deployer) verifyPollerSurvived(ctx context.Context, stackName, topicARN string) error {
	var gone []string
	if _, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: aws.String(PollerFunctionName),
	}); err != nil {
		gone = append(gone, fmt.Sprintf("Lambda %s (%v)", PollerFunctionName, err))
	}
	if _, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name: aws.String(PollerScheduleName),
	}); err != nil {
		gone = append(gone, fmt.Sprintf("schedule %s (%v)", PollerScheduleName, err))
	}
	if topicARN != "" {
		if _, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
			TopicArn: aws.String(topicARN),
		}); err != nil {
			gone = append(gone, fmt.Sprintf("SNS topic %s (%v)", topicARN, err))
		}
	}
	if len(gone) == 0 {
		return nil
	}
	return fmt.Errorf("migrate: stack %s was deleted but the retain policy did NOT protect %s. "+
		"Re-run 'lagotto deploy' to recreate what's missing — and note that a RE-CREATED SCHEDULE "+
		"COMES BACK DISABLED, so your watches will not be polled until you re-enable it by creating "+
		"a watch ('lagotto watch …'). Check 'lagotto status' before walking away",
		stackName, strings.Join(gone, ", "))
}

// stableStatusList renders migrateStableStatuses for an error message.
func stableStatusList() string {
	return "CREATE_COMPLETE, UPDATE_COMPLETE or UPDATE_ROLLBACK_COMPLETE"
}

// withRetainPolicies returns the template with `DeletionPolicy: Retain` and
// `UpdateReplacePolicy: Retain` added to each of the three retainLogicalIDs —
// exactly six inserted lines, nothing else touched.
//
// It is deliberately LINE-ORIENTED rather than a yaml.Node round-trip. The
// template is a SAM template full of CloudFormation short-form intrinsic tags
// (!Ref, !Sub, !GetAtt, !Not, !Equals) and a `Transform:` header; re-emitting it
// through a YAML marshaller reformats the whole file and risks mangling or
// re-quoting those tags. Inserting six lines cannot do any of that, and the
// invariant "input plus exactly six lines" is directly testable.
//
// The input is always the EMBEDDED template (which carries no retain policies —
// asserted by TestStackTemplateHasNoRetainPolicies, because shipping Retain in
// the template would make `delete-stack` leave orphans for exactly the IaC
// consumer the template is kept for). It still tolerates an already-retained
// resource by skipping it, so it can't produce a duplicate key.
func withRetainPolicies(template string) (string, error) {
	// A resource that is ALREADY retained must be skipped, and a `DeletionPolicy`
	// line sits AFTER the `Type:` line we insert at — so existing policies have to
	// be known before the insertion pass, not discovered during it.
	targets := retainedLogicalIDs(template)
	for _, id := range retainLogicalIDs {
		if _, ok := targets[id]; !ok {
			targets[id] = false
		}
	}

	lines := strings.Split(template, "\n")
	out := make([]string, 0, len(lines)+2*len(retainLogicalIDs))
	current := ""
	for _, line := range lines {
		out = append(out, line)
		if m := resourceHeaderRE.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if !resourceTypeRE.MatchString(line) {
			continue
		}
		if done, ok := targets[current]; !ok || done {
			continue
		}
		out = append(out, propertyIndent+"DeletionPolicy: Retain", propertyIndent+"UpdateReplacePolicy: Retain")
		targets[current] = true
	}

	var missing []string
	for _, id := range retainLogicalIDs {
		if !targets[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("retain overlay: the embedded template has no resource declaration for %s "+
			"(was a logical ID renamed in deployment/cloudformation/lagotto-stack.yaml?)",
			strings.Join(missing, ", "))
	}
	return strings.Join(out, "\n"), nil
}

// logicalIDsMissingRetain returns the retainLogicalIDs that the given template
// body does NOT mark `DeletionPolicy: Retain`. An empty result is the only thing
// that authorizes a DeleteStack.
//
// It reads the body conservatively: an unparseable or unexpectedly-formatted
// template (a JSON one, for instance) yields "all three missing", which aborts.
// Failing towards "don't delete" is the whole point.
func logicalIDsMissingRetain(body string) []string {
	retained := retainedLogicalIDs(body)
	var missing []string
	for _, id := range retainLogicalIDs {
		if !retained[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

// retainedLogicalIDs maps every logical ID in the body that carries
// `DeletionPolicy: Retain` to true. Shared by the safety gate and the overlay so
// the two can never disagree about what "already retained" means.
func retainedLogicalIDs(body string) map[string]bool {
	retained := map[string]bool{}
	current := ""
	for _, line := range strings.Split(body, "\n") {
		if m := resourceHeaderRE.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if m := retainRE.FindStringSubmatch(line); m != nil && m[1] == "DeletionPolicy" && current != "" {
			retained[current] = true
		}
	}
	return retained
}

// templateParameterNames returns the parameter names the given template declares.
// Used to filter what gets carried across as UsePreviousValue.
func templateParameterNames(template string) map[string]bool {
	names := map[string]bool{}
	section := ""
	for _, line := range strings.Split(template, "\n") {
		if len(line) > 0 && line[0] != ' ' && line[0] != '#' && strings.HasSuffix(strings.TrimSpace(line), ":") {
			section = strings.TrimSuffix(strings.TrimSpace(line), ":")
			continue
		}
		if section != "Parameters" {
			continue
		}
		if m := resourceHeaderRE.FindStringSubmatch(line); m != nil {
			names[m[1]] = true
		}
	}
	return names
}

// previousParameters turns the stack's current parameters into UsePreviousValue
// entries, keeping only those the template still declares.
//
// Carrying the values across is what makes the retain-update a genuine no-op
// beyond the policies — in particular LambdaCodeBucket, whose empty default would
// flip the template's DeployLambda condition to false and DELETE the function and
// schedule. Filtering by the template's declared names matters in the other
// direction: passing a parameter the template no longer declares makes
// CloudFormation reject the whole update.
func previousParameters(cur []cfntypes.Parameter, declared map[string]bool) []cfntypes.Parameter {
	var out []cfntypes.Parameter
	for _, p := range cur {
		key := aws.ToString(p.ParameterKey)
		if key == "" || !declared[key] {
			continue
		}
		out = append(out, cfntypes.Parameter{
			ParameterKey:     aws.String(key),
			UsePreviousValue: aws.Bool(true),
		})
	}
	return out
}
