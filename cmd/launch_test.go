package cmd

import (
	"bytes"
	"io"
	"os"
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

// TestLaunchStackNameWarnsWhenPassed: passing the inert --stack-name must not be
// rejected (scripts keep working) but must print a note saying it's ignored,
// otherwise a user whose script still sets it has no way to learn why the flag
// stopped mattering.
//
// Driven via the real runLaunch, which returns on the missing --spawn-config long
// before it loads an AWS config or calls anything — so this touches no AWS.
func TestLaunchStackNameWarnsWhenPassed(t *testing.T) {
	prev := launchStackName
	prevConfig := launchSpawnConfig
	t.Cleanup(func() {
		launchStackName = prev
		launchSpawnConfig = prevConfig
		// launchCmd is a package-level singleton, so undo the Changed bit too —
		// otherwise every later test in this package sees --stack-name as "passed".
		if f := launchCmd.Flags().Lookup("stack-name"); f != nil {
			f.Changed = false
		}
	})
	launchSpawnConfig = "" // force the early return, before any AWS call

	if err := launchCmd.Flags().Set("stack-name", "my-old-stack"); err != nil {
		t.Fatalf("--stack-name must still be settable: %v", err)
	}

	stderr := captureStderr(t, func() {
		if err := runLaunch(launchCmd, nil); err == nil {
			t.Error("expected runLaunch to stop at the missing --spawn-config")
		}
	})

	if !strings.Contains(stderr, "--stack-name") || !strings.Contains(stderr, "ignored") {
		t.Errorf("expected a note that --stack-name is ignored, got %q", stderr)
	}
	if !strings.Contains(stderr, "my-old-stack") {
		t.Errorf("the note should echo the value that was passed, got %q", stderr)
	}
}

// captureStderr runs fn with os.Stderr redirected to a pipe and returns what was
// written. The deprecation note goes to os.Stderr directly (not cmd.ErrOrStderr),
// matching how the rest of the command reports side notes.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stderr
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	os.Stderr = orig
	_ = w.Close()
	out := <-done
	_ = r.Close()
	return out
}
