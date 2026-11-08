package doctor

import (
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// CheckNotifyWatches names the deprecated notify-watch check (#283).
const CheckNotifyWatches = "notify-watches"

// EvalNotifyWatches lists the active --action notify watches among active
// (every active watch in the account and region): notify is deprecated, and a
// notify watch reports an offering, not capacity. Pure, like Evaluate. WARN at
// most, never FAIL: a notify watch still works in this release.
//
// ok is false when there is nothing to say: no active notify watch. A failed
// read of the active watches is also left unsaid here, because the poller
// schedule check already reports it from the same read.
func EvalNotifyWatches(active []watcher.Watch) (Check, bool) {
	var lines []string
	for i := range active {
		w := &active[i]
		if w.Action != watcher.ActionNotify {
			continue
		}
		regions := "all enabled regions"
		if len(w.Regions) > 0 {
			regions = strings.Join(w.Regions, ",")
		}
		lines = append(lines, fmt.Sprintf("%s  %s  %s  (owner %s)", w.WatchID, w.InstanceTypePattern, regions, w.UserID))
	}
	if len(lines) == 0 {
		return Check{}, false
	}
	c := Check{
		Name:    CheckNotifyWatches,
		Status:  StatusWarn,
		Summary: fmt.Sprintf("%d active --action notify watch(es): notify is deprecated, and it reports an offering, not capacity", len(lines)),
		Detail:  lines,
	}
	c.Detail = append(c.Detail, strings.Split(watcher.NotifyDeprecation, "\n")...)
	c.Detail = append(c.Detail, "An update cannot change a watch's action: create a new watch with --action hold or --action spawn, then 'lagotto cancel <watch-id>' the notify one.")
	return c, true
}
