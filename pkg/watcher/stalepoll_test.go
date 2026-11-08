package watcher_test

import (
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// StalePollAfter is an exported contract (#282): spore-host-mcp flags staleness
// with it. Three missed 5-minute poller cycles.
func TestStalePollAfter(t *testing.T) {
	if watcher.StalePollAfter != 15*time.Minute {
		t.Errorf("StalePollAfter = %v, want 15m", watcher.StalePollAfter)
	}
}
