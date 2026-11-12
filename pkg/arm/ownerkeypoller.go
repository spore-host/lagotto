package arm

import (
	"context"
	"fmt"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// CheckOwnerKeyPoller warns when a watch keyed under its owner
// (watcher.OwnerKey) is created while the deployed poller predates owner
// keys: such a poller resolves the watch's group outside its owner's scope (so
// a group member is cancelled as "group no longer exists") and names
// SageMaker jobs and launch-retry schedules with the full key, which those
// names cannot hold. Quiet when it cannot tell, as CheckGroupPoller is.
func CheckOwnerKeyPoller(ctx context.Context, p PollerInspector, region string, w *watcher.Watch) string {
	if w == nil || watcher.IsLegacyKey(w.WatchID) {
		return ""
	}
	info, err := p.InspectPoller(ctx)
	return OwnerKeyPollerWarning(info, err, region)
}

// OwnerKeyPollerWarning is CheckOwnerKeyPoller's decision for an inspected
// poller.
func OwnerKeyPollerWarning(info deploy.PollerInfo, inspectErr error, region string) string {
	if inspectErr != nil || !info.Exists {
		return ""
	}
	where := ""
	if region != "" {
		where = " in " + region
	}
	minVer := watcher.MinOwnerKeyPollerVersion
	if info.Version != "" {
		if cmp, ok := doctor.CompareVersions(info.Version, minVer); !ok || cmp >= 0 {
			return ""
		}
	}
	return fmt.Sprintf("Warning: the hosted poller%s predates v%s, the first release with owner-scoped watch keys; "+
		"group and SageMaker watches fail under it. Run 'lagotto deploy' to upgrade the poller, then 'lagotto setup'.", where, minVer)
}
