package deploy

import (
	"strings"
	"testing"

	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

// TestDeployCapabilities is the #143/#145 regression. The template's IAM roles
// are now CLI-owned and referenced by ARN, so the stack defines NO IAM resources:
// it must NOT declare an AWS::IAM::Role (a named IAM resource would collide with
// an already-existing role under Early Validation, #145), and the deploy
// capabilities need only CAPABILITY_AUTO_EXPAND (the AWS::Serverless transform),
// not CAPABILITY_IAM/NAMED_IAM.
func TestDeployCapabilities(t *testing.T) {
	has := func(c cfntypes.Capability) bool {
		for _, x := range deployCapabilities {
			if x == c {
				return true
			}
		}
		return false
	}
	if strings.Contains(cfn.StackTemplate, "AWS::IAM::Role") {
		t.Error("template defines an AWS::IAM::Role — roles must be CLI-owned + referenced by ARN, not stack-created (#145)")
	}
	if !has(cfntypes.CapabilityCapabilityAutoExpand) {
		t.Error("deployCapabilities missing CAPABILITY_AUTO_EXPAND — the AWS::Serverless transform needs it")
	}
	// The roles are referenced by their constructed ARNs, not created.
	for _, want := range []string{
		"role/lagotto-capacity-poller-role",
		"role/lagotto-capacity-poller-scheduler-invoke",
	} {
		if !strings.Contains(cfn.StackTemplate, want) {
			t.Errorf("template no longer references %q by ARN", want)
		}
	}
}

func TestLambdaArtifactURL(t *testing.T) {
	want := "https://github.com/spore-host/lagotto/releases/download/v0.44.0/capacity-poller_lambda_linux_arm64.zip"
	// Both bare and v-prefixed versions must produce the same canonical URL.
	for _, v := range []string{"0.44.0", "v0.44.0"} {
		if got := LambdaArtifactURL(v); got != want {
			t.Errorf("LambdaArtifactURL(%q) = %q, want %q", v, got, want)
		}
	}
}

func TestDefaultBucketName(t *testing.T) {
	got := DefaultBucketName("123456789012", "us-west-2")
	want := "lagotto-lambda-123456789012-us-west-2"
	if got != want {
		t.Errorf("DefaultBucketName = %q, want %q", got, want)
	}
}

func TestLambdaObjectKey(t *testing.T) {
	want := "lagotto/capacity-poller-v0.44.0.zip"
	for _, v := range []string{"0.44.0", "v0.44.0"} {
		if got := LambdaObjectKey(v); got != want {
			t.Errorf("LambdaObjectKey(%q) = %q, want %q", v, got, want)
		}
	}
}

// TestFailedCreateStates documents which stack statuses trigger the #59
// delete-and-recreate path: a stack stranded by a failed CreateStack (most
// importantly ROLLBACK_COMPLETE) can't be updated and must be recreated, while a
// healthy stack must NOT be torn down on a redeploy.
func TestFailedCreateStates(t *testing.T) {
	mustRecreate := []cfntypes.StackStatus{
		cfntypes.StackStatusRollbackComplete, // the #59 symptom
		cfntypes.StackStatusRollbackFailed,
		cfntypes.StackStatusReviewInProgress,
		cfntypes.StackStatusCreateFailed,
		cfntypes.StackStatusDeleteFailed,
	}
	for _, s := range mustRecreate {
		if !failedCreateStates[s] {
			t.Errorf("status %s should trigger delete-and-recreate", s)
		}
	}
	mustKeep := []cfntypes.StackStatus{
		cfntypes.StackStatusCreateComplete,
		cfntypes.StackStatusUpdateComplete,
		cfntypes.StackStatusUpdateRollbackComplete, // a healthy, updatable stack
	}
	for _, s := range mustKeep {
		if failedCreateStates[s] {
			t.Errorf("status %s is a live stack — must NOT be deleted on redeploy", s)
		}
	}
}
