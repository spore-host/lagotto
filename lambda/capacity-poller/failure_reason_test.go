package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/failure"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// #309: a scheduled launch refused at fire time keeps the refusal and the fix
// on its record; there is no channel to notify, so this is how its owner
// learns why.
func TestScheduledFailureReason(t *testing.T) {
	sl := &watcher.ScheduledLaunch{ScheduleID: "o/sl-0011aabb", LaunchConfigJSON: []byte(`{"iampolicydocument":"{}"}`)}
	refused := fmt.Errorf("%w: spawn config sets iam_policy_document: ...", failure.ErrConfigRefused)
	got := scheduledFailureReason(sl, refused)
	for _, want := range []string{"spawn config sets iam_policy_document", "Fix: ", "lagotto setup --instance-role <role>", "; then lagotto update sl-0011aabb --spawn-config <file>"} {
		if !strings.Contains(got, want) {
			t.Errorf("reason %q lacks %q", got, want)
		}
	}
	if got := scheduledFailureReason(sl, errors.New("reservation cr-1 is expired")); got != "reservation cr-1 is expired" {
		t.Errorf("non-refusal reason = %q", got)
	}
	if got := scheduledFailureReason(sl, nil); got == "" {
		t.Error("nil error gave an empty reason")
	}
}
