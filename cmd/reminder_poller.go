package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Warning when the deployed poller cannot send until-landed reminders (#239
// phase 4). Reminders are sent by the hosted poller, which runs its own copy of
// lagotto and is only upgraded by `lagotto deploy`. A poller older than
// watcher.MinReminderPollerVersion still pauses an until-landed watch at its
// deadline (the fail-safe), but never reminds anyone first, so the pause comes
// as a surprise. `watch --until-landed`, `ack` and `status` say so.
//
// The deployed version is read the way `lagotto doctor` reads it: the Version
// tag `lagotto deploy` stamps on the poller Lambda (deploy.VersionTag), compared
// with doctor.CompareVersions. It is best-effort and silent when it cannot say:
// no poller deployed in this region (the watch is for `poll --daemon`, which is
// this CLI), a poller from a dev build, or no lambda:GetFunction permission. A
// warning that might be wrong is worse than none.

// warnIfPollerPredatesReminders prints the warning for w (an active
// until-landed watch) to errOut when the hosted poller in this region is too
// old.
func warnIfPollerPredatesReminders(ctx context.Context, cfg aws.Config, errOut io.Writer, w *watcher.Watch) {
	if !w.UntilLanded || w.Status != watcher.StatusActive {
		return
	}
	info, err := deploy.New(cfg).InspectPoller(ctx)
	if msg := reminderPollerWarning(info, err, cfg.Region, w); msg != "" {
		_, _ = fmt.Fprintln(errOut, msg)
	}
}

// reminderPollerWarning is the warning for w given the inspected poller, or ""
// when there is nothing to warn about (or no way to know).
func reminderPollerWarning(info deploy.PollerInfo, inspectErr error, region string, w *watcher.Watch) string {
	if !w.UntilLanded || w.Status != watcher.StatusActive || inspectErr != nil || !info.Exists {
		return ""
	}
	minVer := watcher.MinReminderPollerVersion
	var which string
	if info.Version == "" {
		// The Version tag has been stamped since v0.61.0, so an untagged poller
		// is older than that, and so older than any reminder-sending release.
		which = "has no Version tag, so it predates v0.61.0"
	} else {
		cmp, ok := doctor.CompareVersions(info.Version, minVer)
		if !ok || cmp >= 0 {
			return ""
		}
		which = "is v" + strings.TrimPrefix(info.Version, "v")
	}
	where := ""
	if region != "" {
		where = " in " + region
	}
	return fmt.Sprintf("Warning: the hosted poller%s %s, older than v%s, the first release that sends until-landed reminders. "+
		"It will not remind you before watch %s pauses at %s; the watch still pauses safely then, and 'lagotto ack %s' resumes it. "+
		"Run 'lagotto deploy' to upgrade the poller.",
		where, which, minVer, w.WatchID, w.ExpiresAt.UTC().Format(time.RFC3339), w.WatchID)
}
