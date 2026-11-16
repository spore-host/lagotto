package watcher

import (
	"time"

	"github.com/spore-host/lagotto/pkg/managedtags"
)

// runtimeTagsNow is the clock of RuntimeTags; tests replace it.
var runtimeTagsNow = time.Now

// RuntimeTags are the spore.host tags (#324) on what the poller creates for
// watch w in region: an instance (through spawn), a capacity reservation, a
// SageMaker training job. They carry spore-host:managed-by=lagotto, the
// region as the scope, and the watch's owner (its creator's aws:userid), next
// to the lagotto:* tags those resources already had.
func RuntimeTags(w *Watch, region string) map[string]string {
	owner := ""
	if w != nil {
		owner = w.OwnerID
		if owner == "" {
			owner = KeyOwner(w.WatchID)
		}
	}
	return managedtags.Workload(region, owner, runtimeTagsNow())
}
