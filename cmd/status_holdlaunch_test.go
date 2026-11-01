package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestStatusTable_ShowsHoldLaunchCommand is #208: a live fired hold's status
// prints the same launch command as the email (the shared builder), including
// the name and --instance-type spawn needs.
func TestStatusTable_ShowsHoldLaunchCommand(t *testing.T) {
	w := firedHold()
	w.LastMatch.InstanceType = "p5.48xlarge"
	var out bytes.Buffer
	writeLastMatch(&out, w)
	want := watcher.HoldLaunchCommandFor(w, w.LastMatch)
	if !strings.Contains(out.String(), "Launch into it:  "+want) {
		t.Errorf("status table lacks the launch command %q:\n%s", want, out.String())
	}
	if !strings.Contains(want, "spawn launch w-hold ") || !strings.Contains(want, "--instance-type p5.48xlarge") {
		t.Errorf("launch command lacks the name or type: %q", want)
	}

	// Released or expired: nothing to launch into, so no command.
	w.Status = watcher.StatusCancelled
	out.Reset()
	writeLastMatch(&out, w)
	if strings.Contains(out.String(), "spawn launch") {
		t.Errorf("status prints a launch command for a cancelled hold:\n%s", out.String())
	}
}
