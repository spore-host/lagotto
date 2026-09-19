package cmd

import (
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestResolveIfExists covers the --if-exists resolution, including the two
// different empty-flag defaults (one-shot launch skips if the instance exists;
// a persisted watch launches) and the invalid-value error.
func TestResolveIfExists(t *testing.T) {
	tests := []struct {
		name    string
		flag    string
		oneShot bool
		want    string
		wantErr bool
	}{
		{"empty one-shot defaults to skip", "", true, watcher.IfExistsSkip, false},
		{"empty watch defaults to launch", "", false, watcher.IfExistsLaunch, false},
		{"explicit skip", "skip", false, watcher.IfExistsSkip, false},
		{"explicit launch", "launch", true, watcher.IfExistsLaunch, false},
		{"explicit replace", "replace", false, watcher.IfExistsReplace, false},
		{"case-insensitive + trimmed", "  SKIP  ", false, watcher.IfExistsSkip, false},
		{"invalid value errors", "bogus", false, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveIfExists(tt.flag, tt.oneShot)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveIfExists(%q,%v) = %q, want error", tt.flag, tt.oneShot, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveIfExists(%q,%v): unexpected error %v", tt.flag, tt.oneShot, err)
			}
			if got != tt.want {
				t.Errorf("resolveIfExists(%q,%v) = %q, want %q", tt.flag, tt.oneShot, got, tt.want)
			}
		})
	}
}

// TestLaunchPollerARNDerivation pins the ARNs `lagotto launch` now targets instead
// of reading them out of CloudFormation stack outputs (#154).
//
// runLaunch itself is not exercised here: it loads an AWS config, calls STS,
// DynamoDB and EventBridge Scheduler directly with no injection seam, so driving
// it offline would mean refactoring the command rather than testing this change.
// What IS worth guarding is the derivation — these are the exact strings the CFN
// template built with !Sub, and a scheduled launch silently targets nothing if
// either drifts.
func TestLaunchPollerARNDerivation(t *testing.T) {
	const (
		account = "123456789012"
		region  = "us-west-2"
	)

	if got, want := deploy.PollerFunctionARN(region, account),
		"arn:aws:lambda:us-west-2:123456789012:function:lagotto-capacity-poller"; got != want {
		t.Errorf("poller function ARN = %q, want %q", got, want)
	}
	if got, want := runtimeiam.SchedulerInvokeRoleARN(account),
		"arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke"; got != want {
		t.Errorf("scheduler invoke role ARN = %q, want %q", got, want)
	}

	// The function ARN is region-scoped, the role ARN is not — getting that
	// backwards is the realistic mistake, so assert it.
	if deploy.PollerFunctionARN("eu-west-1", account) == deploy.PollerFunctionARN(region, account) {
		t.Error("the poller function ARN must vary with the region")
	}
}

// TestLaunchStackNameFlagIsDeprecated: --stack-name stays VISIBLE (not
// MarkDeprecated, which would hide it from --help and from docs-gen/launch.md) but
// must advertise that it is ignored, so a script that still passes it can find out
// why it stopped mattering.
func TestLaunchStackNameFlagIsDeprecated(t *testing.T) {
	f := launchCmd.Flags().Lookup("stack-name")
	if f == nil {
		t.Fatal("--stack-name must still parse, so existing scripts don't hard-fail")
	}
	if f.Hidden || f.Deprecated != "" {
		t.Error("--stack-name must stay visible in --help and in the generated docs")
	}
	if !strings.Contains(f.Usage, "deprecated") || !strings.Contains(f.Usage, "ignored") {
		t.Errorf("--stack-name usage %q should say it is deprecated and ignored", f.Usage)
	}
}
