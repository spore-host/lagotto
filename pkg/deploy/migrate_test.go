package deploy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

// --- the retain overlay -----------------------------------------------------

// TestRetainOverlay_AddsExactlySixLines is the shape invariant.
//
// The overlay is a line-oriented insertion precisely so that this can be asserted
// literally: the output is the input plus six lines, at the three expected
// resources, and nothing else in the file moved. A yaml.Node round-trip would
// reformat the whole template and put the CloudFormation short-form tags (!Ref,
// !Sub, !GetAtt, !Not, !Equals) at risk for no benefit, which is why it isn't one.
func TestRetainOverlay_AddsExactlySixLines(t *testing.T) {
	in := cfn.StackTemplate
	out, err := withRetainPolicies(in)
	if err != nil {
		t.Fatalf("withRetainPolicies: %v", err)
	}

	inLines := strings.Split(in, "\n")
	outLines := strings.Split(out, "\n")
	if len(outLines) != len(inLines)+6 {
		t.Fatalf("output has %d lines, want %d (input %d + 6)", len(outLines), len(inLines)+6, len(inLines))
	}

	// Every input line is still present, in order, with only the six insertions
	// between them.
	var extra []string
	i := 0
	for _, line := range outLines {
		if i < len(inLines) && line == inLines[i] {
			i++
			continue
		}
		extra = append(extra, line)
	}
	if i != len(inLines) {
		t.Errorf("only %d of %d input lines survived in order — the overlay reordered or rewrote the template", i, len(inLines))
	}
	if len(extra) != 6 {
		t.Fatalf("inserted %d lines %q, want exactly 6", len(extra), extra)
	}
	for _, line := range extra {
		if line != "    DeletionPolicy: Retain" && line != "    UpdateReplacePolicy: Retain" {
			t.Errorf("unexpected inserted line %q", line)
		}
	}

	// Each of the three resources got its pair, at the right place.
	for _, id := range retainLogicalIDs {
		block := resourceBlock(t, out, id)
		for _, want := range []string{"    DeletionPolicy: Retain", "    UpdateReplacePolicy: Retain"} {
			if !containsLine(block, want) {
				t.Errorf("resource %s has no %q line; block:\n%s", id, want, strings.Join(block, "\n"))
			}
		}
	}

	// The transform and the intrinsics survived — the reason not to round-trip.
	if !strings.Contains(out, "AWS::Serverless-2016-10-31") {
		t.Error("the SAM transform is missing from the overlaid template")
	}
	for _, intrinsic := range []string{"!Sub 'arn:aws:lambda:${AWS::Region}", "!Ref Environment", "!GetAtt CapacityPollerFunction.Arn", "!Not [!Equals"} {
		if !strings.Contains(out, intrinsic) {
			t.Errorf("intrinsic %q is missing from the overlaid template", intrinsic)
		}
	}
}

// TestStackTemplateHasNoRetainPolicies: Retain must NEVER be committed to
// lagotto-stack.yaml. The template is kept for IaC consumers, and for them a
// `delete-stack` that silently leaves three orphaned resources behind is worse
// than no template at all. Retain belongs only in the transient overlay the
// migration builds.
func TestStackTemplateHasNoRetainPolicies(t *testing.T) {
	if strings.Contains(cfn.StackTemplate, "DeletionPolicy") || strings.Contains(cfn.StackTemplate, "UpdateReplacePolicy") {
		t.Error("deployment/cloudformation/lagotto-stack.yaml declares a deletion policy. " +
			"It must not: a CloudFormation consumer's delete-stack would then leave the " +
			"poller, topic and schedule orphaned. Retain is applied only by the " +
			"--migrate-from-cloudformation overlay.")
	}
}

// TestRetainOverlay_FailsOnARenamedLogicalID: the overlay knows three logical IDs
// by name. If the template renames one, the overlay must fail loudly rather than
// produce a body that retains only two of the three — which the safety gate would
// then (correctly) refuse, but with a far more confusing message.
func TestRetainOverlay_FailsOnARenamedLogicalID(t *testing.T) {
	renamed := strings.Replace(cfn.StackTemplate, "  CapacityPollerFunction:", "  PollerFn:", 1)
	if renamed == cfn.StackTemplate {
		t.Fatal("test setup: the CapacityPollerFunction resource header was not found")
	}
	if _, err := withRetainPolicies(renamed); err == nil {
		t.Error("withRetainPolicies accepted a template missing CapacityPollerFunction")
	}
}

// TestRetainOverlay_IsIdempotent: applying the overlay to an already-retained body
// must not duplicate the keys (a duplicate mapping key is invalid YAML and CFN
// would reject the update).
func TestRetainOverlay_IsIdempotent(t *testing.T) {
	once, err := withRetainPolicies(cfn.StackTemplate)
	if err != nil {
		t.Fatalf("first overlay: %v", err)
	}
	twice, err := withRetainPolicies(once)
	if err != nil {
		t.Fatalf("second overlay: %v", err)
	}
	if twice != once {
		t.Error("applying the retain overlay twice changed the template; it must be idempotent")
	}
}

// TestLogicalIDsMissingRetain is the gate's own predicate.
func TestLogicalIDsMissingRetain(t *testing.T) {
	if missing := logicalIDsMissingRetain(cfn.StackTemplate); len(missing) != len(retainLogicalIDs) {
		t.Errorf("the bare template reports %v missing, want all %v", missing, retainLogicalIDs)
	}
	overlaid, err := withRetainPolicies(cfn.StackTemplate)
	if err != nil {
		t.Fatalf("withRetainPolicies: %v", err)
	}
	if missing := logicalIDsMissingRetain(overlaid); len(missing) != 0 {
		t.Errorf("the overlaid template reports %v missing, want none", missing)
	}
	// A JSON body, or anything else this line-oriented reader can't parse, must
	// come back as "all missing" — failing towards not deleting.
	if missing := logicalIDsMissingRetain(`{"Resources":{"CapacityPollerFunction":{"DeletionPolicy":"Retain"}}}`); len(missing) != len(retainLogicalIDs) {
		t.Errorf("an unparseable body reports %v missing, want all %v (the gate must fail closed)", missing, retainLogicalIDs)
	}
}

// --- parameters -------------------------------------------------------------

func TestTemplateParameterNames(t *testing.T) {
	names := templateParameterNames(cfn.StackTemplate)
	for _, want := range []string{"Environment", "LambdaCodeBucket", "LambdaCodeKey", "WatchesTableName", "HistoryTableName", "ScheduledTableName"} {
		if !names[want] {
			t.Errorf("parameter %s not found (got %v)", want, names)
		}
	}
	// Resources and outputs must not leak in as parameters.
	for _, notWant := range []string{"CapacityPollerFunction", "CapacityPollerFunctionArn", "Resources", "Outputs"} {
		if names[notWant] {
			t.Errorf("%s was picked up as a parameter", notWant)
		}
	}
}

// TestPreviousParameters: every declared parameter is carried across as
// UsePreviousValue, and one the template no longer declares is dropped.
//
// Carrying LambdaCodeBucket across is the load-bearing case: its template default
// is the empty string, which flips the DeployLambda condition off and would make
// the retain-update DELETE the function and schedule.
func TestPreviousParameters(t *testing.T) {
	declared := templateParameterNames(cfn.StackTemplate)
	cur := []cfntypes.Parameter{
		{ParameterKey: aws.String("LambdaCodeBucket"), ParameterValue: aws.String("some-bucket")},
		{ParameterKey: aws.String("Environment"), ParameterValue: aws.String("production")},
		{ParameterKey: aws.String("ARetiredParameter"), ParameterValue: aws.String("x")},
	}
	got := previousParameters(cur, declared)
	if len(got) != 2 {
		t.Fatalf("previousParameters returned %d entries, want 2 (the retired one dropped): %+v", len(got), got)
	}
	for _, p := range got {
		if !aws.ToBool(p.UsePreviousValue) {
			t.Errorf("parameter %s does not set UsePreviousValue", aws.ToString(p.ParameterKey))
		}
		if p.ParameterValue != nil {
			t.Errorf("parameter %s sends a value as well as UsePreviousValue, which CloudFormation rejects", aws.ToString(p.ParameterKey))
		}
	}
}

// --- MigrateFromStack -------------------------------------------------------

// migrateFakes wires a stack in the given status with the given stored template.
func migrateFakes(status cfntypes.StackStatus, template string) (*Deployer, *fakeCFN, *fakeLambda, *fakeScheduler, *fakeSNS) {
	c := &fakeCFN{
		stackName: "lagotto",
		status:    status,
		template:  template,
		params: []cfntypes.Parameter{
			{ParameterKey: aws.String("LambdaCodeBucket"), ParameterValue: aws.String("lagotto-lambda-123456789012-us-east-1")},
			{ParameterKey: aws.String("LambdaCodeKey"), ParameterValue: aws.String("lagotto/capacity-poller-v0.44.0.zip")},
			{ParameterKey: aws.String("Environment"), ParameterValue: aws.String("production")},
		},
		outputs: []cfntypes.Output{
			{OutputKey: aws.String("CapacityAlertsTopicArn"), OutputValue: aws.String(AlertsTopicARN("us-east-1", "123456789012"))},
		},
	}
	l := &fakeLambda{getOut: &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
		FunctionArn: strptr(PollerFunctionARN("us-east-1", "123456789012")),
		State:       lambdatypes.StateActive,
	}}}
	sc := &fakeScheduler{getOut: &scheduler.GetScheduleOutput{Name: strptr(PollerScheduleName)}}
	sn := &fakeSNS{arn: AlertsTopicARN("us-east-1", "123456789012")}
	return testMigrator(c, l, sn, sc), c, l, sc, sn
}

// TestMigrateFromStack_RefusesToDeleteWithoutTheRetainGate is the single most
// important test in the #154 cutover.
//
// The scenario is the one CloudFormation actually produces: the retain-update is
// answered with "No updates are to be performed", so nothing was applied and the
// deployed template still has no DeletionPolicy. Deleting the stack at that point
// would DELETE the user's live poller, topic and schedule. The GetTemplate gate
// exists for exactly this, and what it must do is abort — so this asserts that NO
// DeleteStack call was recorded at all.
func TestMigrateFromStack_RefusesToDeleteWithoutTheRetainGate(t *testing.T) {
	// The stored template is the BARE one: no retain policies.
	d, c, _, _, _ := migrateFakes(cfntypes.StackStatusCreateComplete, cfn.StackTemplate)
	c.updateErr = errors.New("ValidationError: No updates are to be performed.")

	err := d.MigrateFromStack(context.Background(), "lagotto")
	if err == nil {
		t.Fatal("MigrateFromStack succeeded even though the deployed template shows no DeletionPolicy: Retain")
	}
	if len(c.deleteCalls) != 0 {
		t.Fatalf("MigrateFromStack issued %d DeleteStack call(s) without confirming the retain policies — "+
			"that deletes a live poller", len(c.deleteCalls))
	}
	if len(c.getTemplateCalls) == 0 {
		t.Error("the safety gate never called GetTemplate")
	}
	// The message has to be usable: name what's unprotected, say nothing was
	// removed, and give the manual route.
	for _, want := range []string{"ABORTED", "DeletionPolicy: Retain", "CapacityPollerFunction", "Nothing has been removed", "update-stack"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// TestMigrateFromStack_GateIsUnconditional: a successful UpdateStack is NOT a
// substitute for reading the template back. CloudFormation can report
// UPDATE_COMPLETE for a diff it decided was a no-op, so the gate runs on the
// success path too — and it must read the Original stage, not the post-transform
// Processed one.
func TestMigrateFromStack_GateIsUnconditional(t *testing.T) {
	d, c, _, _, _ := migrateFakes(cfntypes.StackStatusCreateComplete, cfn.StackTemplate)

	if err := d.MigrateFromStack(context.Background(), "lagotto"); err != nil {
		t.Fatalf("MigrateFromStack: %v", err)
	}
	if len(c.updateCalls) != 1 {
		t.Fatalf("UpdateStack called %d times, want 1 (the update succeeded here)", len(c.updateCalls))
	}
	if len(c.getTemplateCalls) == 0 {
		t.Fatal("GetTemplate was not called on the success path; the gate is not unconditional")
	}
	if got := c.getTemplateCalls[0].TemplateStage; got != cfntypes.TemplateStageOriginal {
		t.Errorf("GetTemplate used stage %q, want Original (Processed is the post-transform body, which has no DeletionPolicy of ours to find)", got)
	}
	if len(c.deleteCalls) != 1 {
		t.Errorf("DeleteStack called %d times, want 1", len(c.deleteCalls))
	}
}

// TestMigrateFromStack_RefusesUnstableStatus: every non-stable status is refused
// before anything is touched.
func TestMigrateFromStack_RefusesUnstableStatus(t *testing.T) {
	for _, status := range []cfntypes.StackStatus{
		cfntypes.StackStatusRollbackComplete,
		cfntypes.StackStatusUpdateInProgress,
		cfntypes.StackStatusCreateFailed,
		cfntypes.StackStatusDeleteFailed,
		cfntypes.StackStatusReviewInProgress,
	} {
		t.Run(string(status), func(t *testing.T) {
			d, c, _, _, _ := migrateFakes(status, cfn.StackTemplate)
			err := d.MigrateFromStack(context.Background(), "lagotto")
			if err == nil {
				t.Fatalf("MigrateFromStack accepted a stack in %s", status)
			}
			if !strings.Contains(err.Error(), string(status)) {
				t.Errorf("the error does not name the status: %v", err)
			}
			if len(c.updateCalls) != 0 || len(c.deleteCalls) != 0 {
				t.Errorf("an unstable stack was still updated (%d) / deleted (%d)", len(c.updateCalls), len(c.deleteCalls))
			}
		})
	}
}

// TestMigrateFromStack_AbsentStackIsAClearError: there's nothing to migrate from,
// and the user should hear that rather than a raw ValidationError.
func TestMigrateFromStack_AbsentStackIsAClearError(t *testing.T) {
	d, c, _, _, _ := migrateFakes(cfntypes.StackStatusCreateComplete, cfn.StackTemplate)
	c.missing = true
	err := d.MigrateFromStack(context.Background(), "lagotto")
	if err == nil || !strings.Contains(err.Error(), "nothing to migrate from") {
		t.Errorf("error = %v, want a 'nothing to migrate from' message", err)
	}
}

// TestMigrateFromStack_HappyPathSendsTheRightUpdate asserts the retain-update
// itself: the retain body, the previous parameter values, and AUTO_EXPAND (the SAM
// transform's capability — and the ONLY one this template needs since #146).
func TestMigrateFromStack_HappyPathSendsTheRightUpdate(t *testing.T) {
	d, c, _, _, _ := migrateFakes(cfntypes.StackStatusUpdateComplete, cfn.StackTemplate)

	if err := d.MigrateFromStack(context.Background(), "lagotto"); err != nil {
		t.Fatalf("MigrateFromStack: %v", err)
	}
	if len(c.updateCalls) != 1 {
		t.Fatalf("UpdateStack called %d times, want 1", len(c.updateCalls))
	}
	up := c.updateCalls[0]
	if missing := logicalIDsMissingRetain(aws.ToString(up.TemplateBody)); len(missing) > 0 {
		t.Errorf("the update body does not retain %v", missing)
	}
	if len(up.Parameters) != 3 {
		t.Errorf("update carried %d parameters, want the stack's 3 previous values", len(up.Parameters))
	}
	sawAutoExpand := false
	for _, cap := range up.Capabilities {
		if cap == cfntypes.CapabilityCapabilityAutoExpand {
			sawAutoExpand = true
		}
		if cap == cfntypes.CapabilityCapabilityIam || cap == cfntypes.CapabilityCapabilityNamedIam {
			t.Errorf("update sends %s; the template declares no IAM resources since #146", cap)
		}
	}
	if !sawAutoExpand {
		t.Error("update does not send CAPABILITY_AUTO_EXPAND, which the SAM transform requires")
	}
	if len(c.deleteCalls) != 1 {
		t.Errorf("DeleteStack called %d times, want 1", len(c.deleteCalls))
	}
}

// TestMigrateFromStack_PostDeleteVerificationFailsLoudly: if a retain policy
// didn't take, the resource is gone and the user must be told — including the
// non-obvious consequence, that a re-created schedule comes back DISABLED and
// their watches simply stop being polled until they re-enable it.
func TestMigrateFromStack_PostDeleteVerificationFailsLoudly(t *testing.T) {
	d, c, l, _, _ := migrateFakes(cfntypes.StackStatusCreateComplete, cfn.StackTemplate)
	// The function vanished with the stack despite the retain policy.
	l.getOut = nil
	l.getErr = &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}

	err := d.MigrateFromStack(context.Background(), "lagotto")
	if err == nil {
		t.Fatal("MigrateFromStack returned nil after the poller Lambda disappeared")
	}
	if len(c.deleteCalls) != 1 {
		t.Errorf("DeleteStack called %d times, want 1 (the gate passed, so the delete is expected)", len(c.deleteCalls))
	}
	for _, want := range []string{PollerFunctionName, "DISABLED", "lagotto deploy", "lagotto watch"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// TestMigrateFromStack_GetTemplateFailureDoesNotDelete: no read-back, no delete.
func TestMigrateFromStack_GetTemplateFailureDoesNotDelete(t *testing.T) {
	d, c, _, _, _ := migrateFakes(cfntypes.StackStatusCreateComplete, cfn.StackTemplate)
	c.getTemplateErr = fmt.Errorf("AccessDenied: not authorized to perform cloudformation:GetTemplate")

	err := d.MigrateFromStack(context.Background(), "lagotto")
	if err == nil {
		t.Fatal("MigrateFromStack succeeded without being able to read the template back")
	}
	if len(c.deleteCalls) != 0 {
		t.Errorf("DeleteStack was called %d time(s) despite GetTemplate failing", len(c.deleteCalls))
	}
}

// --- the automatic warning --------------------------------------------------

// TestLegacyStackAdoptionWarning_SaysTheThreeThingsThatMatter. Getting any of the
// three wrong costs somebody their poller, so the wording is asserted rather than
// left to review.
func TestLegacyStackAdoptionWarning_SaysTheThreeThingsThatMatter(t *testing.T) {
	msg := LegacyStackAdoptionWarning("lagotto", "123456789012", "us-west-2")
	for _, want := range []string{
		`"lagotto"`,                     // which stack
		"123456789012/us-west-2",        // where
		"ADOPTED",                       // nothing was duplicated
		"nothing was duplicated",        //
		"still believes it owns",        // why delete-stack is dangerous
		"would DELETE your live",        //
		"delete-stack --stack-name",     // the dangerous command, named
		"--migrate-from-cloudformation", // the safe way out
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the adoption warning does not mention %q:\n%s", want, msg)
		}
	}
}

// TestLegacyStackTeardownWarning tells the opposite story — after a teardown the
// stack IS safe to delete, because its resources are already gone.
func TestLegacyStackTeardownWarning(t *testing.T) {
	msg := LegacyStackTeardownWarning("lagotto")
	if !strings.Contains(msg, "delete-stack --stack-name lagotto") {
		t.Errorf("the teardown warning does not give the delete command:\n%s", msg)
	}
	if strings.Contains(msg, "--migrate-from-cloudformation") {
		t.Errorf("the teardown warning points at migration, but there is nothing left to retain:\n%s", msg)
	}
}

// --- helpers ----------------------------------------------------------------

// resourceBlock returns the lines belonging to the named 2-space-indented key.
func resourceBlock(t *testing.T, template, logicalID string) []string {
	t.Helper()
	var block []string
	in := false
	for _, line := range strings.Split(template, "\n") {
		if m := resourceHeaderRE.FindStringSubmatch(line); m != nil {
			if in {
				break
			}
			in = m[1] == logicalID
			continue
		}
		if in {
			block = append(block, line)
		}
	}
	if !in {
		t.Fatalf("resource %s not found in the template", logicalID)
	}
	return block
}

func containsLine(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}
