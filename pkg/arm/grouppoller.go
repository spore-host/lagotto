package arm

import (
	"context"
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Warning when the deployed poller predates watch groups (#287). A poller
// older than watcher.MinGroupPollerVersion ignores every group field, so it
// polls each member as an independent watch: two members can both land, and
// nothing cancels the others. Every program that creates a group member should
// say so; the CLI does at `watch --group`.
//
// The version is read the way CheckReminderPoller reads it (the Version tag
// `lagotto deploy` stamps on the poller Lambda), and the check is as quiet
// when it cannot say: no poller in this region, a dev build, or no
// lambda:GetFunction permission. An untagged poller IS warned about.

// CheckGroupPoller returns the warning for w, or "" when there is nothing to
// warn about (or no way to know). It inspects the poller only when w is an
// active group member, so calling it for any watch costs no API call otherwise.
// region names the poller's region in the text ("" names none).
func CheckGroupPoller(ctx context.Context, p PollerInspector, region string, w *watcher.Watch) string {
	if !needsGroupPoller(w) {
		return ""
	}
	info, err := p.InspectPoller(ctx)
	return GroupPollerWarning(info, err, region, w)
}

// GroupPollerWarning returns the warning for w given the inspected poller, or
// "" (see CheckGroupPoller). The text is one line, starting "Warning: ".
func GroupPollerWarning(info deploy.PollerInfo, inspectErr error, region string, w *watcher.Watch) string {
	if !needsGroupPoller(w) || inspectErr != nil || !info.Exists {
		return ""
	}
	minVer := watcher.MinGroupPollerVersion
	where := ""
	if region != "" {
		where = " in " + region
	}
	tail := fmt.Sprintf("so it polls watch %s as an independent watch, not as a member of group %s: "+
		"more than one member can land, and nothing cancels the others. Run 'lagotto deploy' to upgrade the poller.",
		w.WatchID, w.Group)
	if info.Version == "" {
		return fmt.Sprintf("Warning: the hosted poller%s has no Version tag (deployed before v0.61.0, or not by 'lagotto deploy'), "+
			"so it predates v%s, the first release with watch groups, %s", where, minVer, tail)
	}
	cmp, ok := doctor.CompareVersions(info.Version, minVer)
	if !ok || cmp >= 0 {
		return ""
	}
	return fmt.Sprintf("Warning: the hosted poller%s is v%s, older than v%s, the first release with watch groups, %s",
		where, strings.TrimPrefix(info.Version, "v"), minVer, tail)
}

func needsGroupPoller(w *watcher.Watch) bool {
	return w != nil && w.Group != "" && w.Status == watcher.StatusActive
}
