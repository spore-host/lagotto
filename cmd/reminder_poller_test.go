package cmd

import (
	"bytes"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The decision and its text are tested in pkg/arm (reminderpoller_test.go); the
// tests here run the CLI's wrapper end to end.

// TestMinReminderPollerVersionMatchesChangelog holds watcher.MinReminderPollerVersion
// to the CHANGELOG: while the reminders entry is unreleased the constant must be
// newer than every released version (else the CLI would call a reminder-less
// poller new enough), and once it is released the constant must name that
// release exactly.
func TestMinReminderPollerVersionMatchesChangelog(t *testing.T) {
	data, err := os.ReadFile("../CHANGELOG.md")
	if err != nil {
		t.Fatalf("read CHANGELOG: %v", err)
	}
	const marker = "**Until-landed reminders**"
	header := regexp.MustCompile(`(?m)^## \[([^\]]+)\]`)
	locs := header.FindAllStringSubmatchIndex(string(data), -1)
	section, newestRelease := "", ""
	for i, loc := range locs {
		name := string(data[loc[2]:loc[3]])
		end := len(data)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		if name != "Unreleased" && newestRelease == "" {
			newestRelease = name
		}
		if section == "" && strings.Contains(string(data[loc[0]:end]), marker) {
			section = name
		}
	}
	minVer := watcher.MinReminderPollerVersion
	switch section {
	case "":
		t.Fatalf("no CHANGELOG section mentions %s", marker)
	case "Unreleased":
		cmp, ok := doctor.CompareVersions(minVer, newestRelease)
		if !ok || cmp <= 0 {
			t.Errorf("MinReminderPollerVersion = %s, but %s is already released without reminders: set it to the release that will ship them", minVer, newestRelease)
		}
	default:
		if cmp, ok := doctor.CompareVersions(minVer, section); !ok || cmp != 0 {
			t.Errorf("MinReminderPollerVersion = %s, but reminders shipped in %s: set it to %s", minVer, section, section)
		}
	}
}

// The warning end to end through the loopback AWS: the commands read the
// poller Lambda's Version tag with the real SDK client and warn on stderr only
// when it is older than MinReminderPollerVersion.
func TestAckWarnsWhenPollerPredatesReminders(t *testing.T) {
	for _, tc := range []struct {
		name, version string
		warn          bool
	}{
		{"older poller", "0.67.0", true},
		{"untagged poller", pollerUntagged, true},
		{"current poller", watcher.MinReminderPollerVersion, false},
		{"no poller deployed", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeAWS{item: storedUntilLandedItem("active", ""), pollerVersion: tc.version}
			var err error
			stderr := captureStderr(t, func() { _, err = runAckWithFake(t, f) })
			if err != nil {
				t.Fatalf("ack: %v", err)
			}
			if n := f.called("lambda:GetFunction"); n != 1 {
				t.Errorf("lambda:GetFunction calls = %d, want 1", n)
			}
			if got := strings.Contains(stderr, "first release that sends until-landed reminders"); got != tc.warn {
				t.Errorf("warned = %v, want %v; stderr:\n%s", got, tc.warn, stderr)
			}
			if tc.warn && !strings.Contains(stderr, "lagotto ack w-1") {
				t.Errorf("warning does not name the watch: %s", stderr)
			}
		})
	}
}

// A TTL watch never asks about the poller: reminders do not apply to it.
func TestExtendTTLWatchSkipsPollerCheck(t *testing.T) {
	setFlagsForTest(t, extendCmd, map[string]string{"ttl": "7d"})
	f := &fakeAWS{item: storedWatchItem("active"), pollerVersion: "0.1.0"}
	if err := runExtendWithFake(t, f); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if n := f.called("lambda:GetFunction"); n != 0 {
		t.Errorf("lambda:GetFunction calls = %d for a TTL watch, want 0", n)
	}
}

func TestWatchUntilLandedWarnsWhenPollerPredatesReminders(t *testing.T) {
	setFlagsForTest(t, watchCmd, map[string]string{
		"until-landed": "true",
		"action":       "hold",
		"yes":          "true",
		"notify":       "sns:arn:aws:sns:us-east-1:123456789012:mine",
	})
	f := &fakeAWS{scheduleState: "ENABLED", pollerVersion: "0.66.0"}
	var err error
	stderr := captureStderr(t, func() { err = runWatchWithFake(t, f) })
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if !strings.Contains(stderr, "is v0.66.0, older than v"+watcher.MinReminderPollerVersion) {
		t.Errorf("no poller-version warning on stderr:\n%s", stderr)
	}
}

func TestStatusWarnsWhenPollerPredatesReminders(t *testing.T) {
	oldFormat := outputFormat
	outputFormat = "json"
	t.Cleanup(func() { outputFormat = oldFormat })
	f := &fakeAWS{item: storedUntilLandedItem("active", ""), pollerVersion: "0.67.0"}
	installFakeAWS(t, f)
	var out bytes.Buffer
	statusCmd.SetOut(&out)
	t.Cleanup(func() { statusCmd.SetOut(nil) })
	var err error
	stderr := captureStderr(t, func() { err = statusCmd.RunE(statusCmd, []string{"w-1"}) })
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !strings.Contains(stderr, "is v0.67.0, older than") {
		t.Errorf("no poller-version warning on stderr:\n%s", stderr)
	}
	if strings.Contains(out.String(), "Warning") || !strings.HasPrefix(strings.TrimSpace(out.String()), "{") {
		t.Errorf("stdout is not clean JSON:\n%s", out.String())
	}
}
