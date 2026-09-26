package cmd

import (
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestListStatusFilter records the #161 finding: `list --all` needed no new flag.
// It already means "no status filter", which includes expired — the only thing
// missing was a record to find, because DynamoDB had deleted it at expiry.
func TestListStatusFilter(t *testing.T) {
	if got := listStatusFilter(true); got != "" {
		t.Errorf("listStatusFilter(true) = %q, want \"\" (no filter — every status)", got)
	}
	if got := listStatusFilter(false); got != watcher.StatusActive {
		t.Errorf("listStatusFilter(false) = %q, want active", got)
	}
}
