package deploy

import "testing"

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
