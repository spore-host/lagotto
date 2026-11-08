package arm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Warning when the deployed poller cannot send until-landed reminders (#239
// phase 4, exported in #282). Reminders are sent by the hosted poller, which
// runs its own copy of lagotto and is only upgraded by `lagotto deploy`. A
// poller older than watcher.MinReminderPollerVersion still pauses an
// until-landed watch at its deadline (the fail-safe), but never reminds anyone
// first, so the pause comes as a surprise. Every program that creates, acks or
// shows an until-landed watch should say so: the CLI does at `watch
// --until-landed`, `ack` and `status`; spore-host-mcp at lagotto_watch,
// lagotto_ack and lagotto_status.
//
// The deployed version is read the way `lagotto doctor` reads it: the Version
// tag `lagotto deploy` stamps on the poller Lambda (deploy.VersionTag), compared
// with doctor.CompareVersions. It is best-effort and silent when it cannot say:
// no poller deployed in this region (the watch is for `poll --daemon`), a
// poller from a dev build, or no lambda:GetFunction permission. A warning that
// might be wrong is worse than none.

// PollerInspector reads the deployed poller Lambda. *deploy.Deployer satisfies
// it (deploy.New(cfg)); InspectPoller's contract is deploy's: an absent function
// is (PollerInfo{Exists: false}, nil), any other failure an error.
type PollerInspector interface {
	InspectPoller(ctx context.Context) (deploy.PollerInfo, error)
}

// CLIResumeHint is the CLI's resume hint for watchID: 'lagotto ack <id>'.
// ReminderPollerWarning uses it when given an empty hint.
func CLIResumeHint(watchID string) string {
	return fmt.Sprintf("'lagotto ack %s'", watchID)
}

// CheckReminderPoller returns the warning for w, or "" when there is nothing to
// warn about (or no way to know). It inspects the poller only when w is an
// active until-landed watch, so calling it for any watch costs no API call
// otherwise. region names the poller's region in the text ("" names none).
// resumeHint is how the caller's user resumes a paused watch, e.g.
// "lagotto_ack (watch_id w-123)"; "" means CLIResumeHint(w.WatchID).
func CheckReminderPoller(ctx context.Context, p PollerInspector, region string, w *watcher.Watch, resumeHint string) string {
	if !needsReminderPoller(w) {
		return ""
	}
	info, err := p.InspectPoller(ctx)
	return ReminderPollerWarning(info, err, region, w, resumeHint)
}

// ReminderPollerWarning returns the warning for w given the inspected poller
// (info and inspectErr as InspectPoller returned them), or "" when there is
// nothing to warn about (or no way to know): w is not an active until-landed
// watch, the poller could not be inspected, none is deployed, or its version
// is at least watcher.MinReminderPollerVersion or not comparable (a dev build).
// An untagged poller IS warned about. region and resumeHint are as for
// CheckReminderPoller. The text is one line, starting "Warning: ".
func ReminderPollerWarning(info deploy.PollerInfo, inspectErr error, region string, w *watcher.Watch, resumeHint string) string {
	if !needsReminderPoller(w) || inspectErr != nil || !info.Exists {
		return ""
	}
	if resumeHint == "" {
		resumeHint = CLIResumeHint(w.WatchID)
	}
	minVer := watcher.MinReminderPollerVersion
	where := ""
	if region != "" {
		where = " in " + region
	}
	tail := fmt.Sprintf("before watch %s pauses at %s; the watch still pauses safely then, and %s resumes it. "+
		"Run 'lagotto deploy' to upgrade the poller.", w.WatchID, w.ExpiresAt.UTC().Format(time.RFC3339), resumeHint)
	if info.Version == "" {
		// `lagotto deploy` has stamped the tag since v0.61.0, so an untagged
		// poller is older than that or was not deployed by lagotto. Either way
		// nothing says it sends reminders. This is the one case doctor also
		// reports as unknown; here the cost of a wrong warning is a redeploy,
		// and the cost of a missing one is a surprise pause.
		return fmt.Sprintf("Warning: the hosted poller%s has no Version tag (deployed before v0.61.0, or not by 'lagotto deploy'), "+
			"so it may predate v%s, the first release that sends until-landed reminders. If so it will not remind you %s",
			where, minVer, tail)
	}
	cmp, ok := doctor.CompareVersions(info.Version, minVer)
	if !ok || cmp >= 0 {
		return ""
	}
	return fmt.Sprintf("Warning: the hosted poller%s is v%s, older than v%s, the first release that sends until-landed reminders. "+
		"It will not remind you %s", where, strings.TrimPrefix(info.Version, "v"), minVer, tail)
}

func needsReminderPoller(w *watcher.Watch) bool {
	return w != nil && w.UntilLanded && w.Status == watcher.StatusActive
}
