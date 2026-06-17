package cfn

import "strings"

import "testing"

// TestStackTemplateEmbedded guards that the go:embed actually wired the YAML in
// (a build with a missing/renamed file would embed an empty string) and that it's
// the lagotto SAM template the deployer expects.
func TestStackTemplateEmbedded(t *testing.T) {
	if len(StackTemplate) == 0 {
		t.Fatal("StackTemplate is empty — go:embed of lagotto-stack.yaml failed")
	}
	for _, want := range []string{
		"AWS::Serverless-2016-10-31",
		"LambdaCodeBucket",
		"LambdaCodeKey",
		"CapacityPollerFunction",
	} {
		if !strings.Contains(StackTemplate, want) {
			t.Errorf("embedded template missing %q", want)
		}
	}
}
