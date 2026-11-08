package arm

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// *deploy.Deployer is the inspector the CLI passes.
var _ PollerInspector = (*deploy.Deployer)(nil)

var reminderDeadline = time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

func untilLandedWatch() *watcher.Watch {
	return &watcher.Watch{WatchID: "w-ul", Status: watcher.StatusActive, UntilLanded: true, ExpiresAt: reminderDeadline}
}

func TestReminderPollerWarning(t *testing.T) {
	ul := untilLandedWatch()
	paused := *ul
	paused.Status = watcher.StatusExpired
	ttl := *ul
	ttl.UntilLanded = false

	for _, tc := range []struct {
		name  string
		w     *watcher.Watch
		info  deploy.PollerInfo
		err   error
		wants []string // nil = no warning
	}{
		{"older poller", ul, deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil,
			[]string{"in us-east-1 is v0.67.0, older than v" + watcher.MinReminderPollerVersion, "pauses at 2026-11-27T12:00:00Z", "'lagotto ack w-ul'", "lagotto deploy"}},
		{"v-prefixed older poller", ul, deploy.PollerInfo{Exists: true, Version: "v0.66.0"}, nil, []string{"is v0.66.0,"}},
		{"untagged poller", ul, deploy.PollerInfo{Exists: true}, nil, []string{"has no Version tag", "may predate v" + watcher.MinReminderPollerVersion}},
		{"the minimum itself", ul, deploy.PollerInfo{Exists: true, Version: watcher.MinReminderPollerVersion}, nil, nil},
		{"newer poller", ul, deploy.PollerInfo{Exists: true, Version: "9.0.0"}, nil, nil},
		{"dev-build poller: cannot tell", ul, deploy.PollerInfo{Exists: true, Version: "dev"}, nil, nil},
		{"no poller here (poll --daemon)", ul, deploy.PollerInfo{}, nil, nil},
		{"cannot inspect (AccessDenied)", ul, deploy.PollerInfo{}, errors.New("AccessDenied: lambda:GetFunction"), nil},
		{"paused watch", &paused, deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil, nil},
		{"TTL watch", &ttl, deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil, nil},
		{"nil watch", nil, deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ReminderPollerWarning(tc.info, tc.err, "us-east-1", tc.w, "")
			if len(tc.wants) == 0 {
				if got != "" {
					t.Fatalf("warning = %q, want none", got)
				}
				return
			}
			if !strings.HasPrefix(got, "Warning: ") || strings.Contains(got, "\n") {
				t.Errorf("warning is not one line starting %q: %q", "Warning: ", got)
			}
			for _, want := range tc.wants {
				if !strings.Contains(got, want) {
					t.Errorf("warning %q missing %q", got, want)
				}
			}
		})
	}
	if got := ReminderPollerWarning(deploy.PollerInfo{Exists: true, Version: "0.1.0"}, nil, "", ul, ""); !strings.Contains(got, "the hosted poller is v0.1.0") {
		t.Errorf("with no region the warning should name none: %q", got)
	}
}

// The CLI's text, verbatim: `lagotto watch --until-landed`, `ack` and `status`
// print exactly this (the empty hint), so a wording change shows up here.
func TestReminderPollerWarningCLIText(t *testing.T) {
	ul := untilLandedWatch()
	const older = "Warning: the hosted poller in us-east-1 is v0.67.0, older than v" + watcher.MinReminderPollerVersion +
		", the first release that sends until-landed reminders. It will not remind you before watch w-ul pauses at " +
		"2026-11-27T12:00:00Z; the watch still pauses safely then, and 'lagotto ack w-ul' resumes it. " +
		"Run 'lagotto deploy' to upgrade the poller."
	if got := ReminderPollerWarning(deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil, "us-east-1", ul, ""); got != older {
		t.Errorf("older-poller text:\n got %q\nwant %q", got, older)
	}
	const untagged = "Warning: the hosted poller in us-east-1 has no Version tag (deployed before v0.61.0, or not by 'lagotto deploy'), " +
		"so it may predate v" + watcher.MinReminderPollerVersion + ", the first release that sends until-landed reminders. " +
		"If so it will not remind you before watch w-ul pauses at 2026-11-27T12:00:00Z; the watch still pauses safely then, " +
		"and 'lagotto ack w-ul' resumes it. Run 'lagotto deploy' to upgrade the poller."
	if got := ReminderPollerWarning(deploy.PollerInfo{Exists: true}, nil, "us-east-1", ul, ""); got != untagged {
		t.Errorf("untagged-poller text:\n got %q\nwant %q", got, untagged)
	}
}

// A non-CLI caller names its own resume step instead of the CLI command.
func TestReminderPollerWarningResumeHint(t *testing.T) {
	got := ReminderPollerWarning(deploy.PollerInfo{Exists: true, Version: "0.67.0"}, nil, "us-east-1", untilLandedWatch(), "lagotto_ack (watch_id w-ul)")
	if !strings.Contains(got, "and lagotto_ack (watch_id w-ul) resumes it.") {
		t.Errorf("hint not used: %q", got)
	}
	if strings.Contains(got, "lagotto ack") {
		t.Errorf("CLI command leaked into a custom-hint warning: %q", got)
	}
	if h := CLIResumeHint("w-9"); h != "'lagotto ack w-9'" {
		t.Errorf("CLIResumeHint = %q", h)
	}
}

type fakeInspector struct {
	info  deploy.PollerInfo
	err   error
	calls int
}

func (f *fakeInspector) InspectPoller(context.Context) (deploy.PollerInfo, error) {
	f.calls++
	return f.info, f.err
}

func TestCheckReminderPoller(t *testing.T) {
	ul := untilLandedWatch()
	f := &fakeInspector{info: deploy.PollerInfo{Exists: true, Version: "0.67.0"}}
	got := CheckReminderPoller(context.Background(), f, "us-west-2", ul, "")
	if f.calls != 1 {
		t.Errorf("InspectPoller calls = %d, want 1", f.calls)
	}
	if want := ReminderPollerWarning(f.info, nil, "us-west-2", ul, ""); got != want || got == "" {
		t.Errorf("CheckReminderPoller = %q, want %q", got, want)
	}

	// An inspect failure is silence, never a guessed warning.
	f = &fakeInspector{err: errors.New("AccessDenied")}
	if got := CheckReminderPoller(context.Background(), f, "us-west-2", ul, ""); got != "" {
		t.Errorf("inspect error warned: %q", got)
	}

	// A watch reminders do not apply to never costs a GetFunction.
	ttl := *ul
	ttl.UntilLanded = false
	paused := *ul
	paused.Status = watcher.StatusExpired
	for _, w := range []*watcher.Watch{&ttl, &paused, nil} {
		f = &fakeInspector{info: deploy.PollerInfo{Exists: true, Version: "0.1.0"}}
		if got := CheckReminderPoller(context.Background(), f, "us-west-2", w, ""); got != "" || f.calls != 0 {
			t.Errorf("watch %+v: warning %q, InspectPoller calls %d; want none, 0", w, got, f.calls)
		}
	}
}
