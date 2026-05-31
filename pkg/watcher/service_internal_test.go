package watcher

import "testing"

// white-box test for the unexported sageMakerType helper, including its
// idempotent guard for an already-ml-prefixed type.
func TestSageMakerType(t *testing.T) {
	cases := map[string]string{
		"g5.2xlarge":    "ml.g5.2xlarge",
		"ml.g5.2xlarge": "ml.g5.2xlarge", // already prefixed → unchanged
		"":              "ml.",
	}
	for in, want := range cases {
		if got := sageMakerType(in); got != want {
			t.Errorf("sageMakerType(%q) = %q, want %q", in, got, want)
		}
	}
}
