package deploy

import (
	"testing"

	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
)

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
