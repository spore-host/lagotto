package cmd

import (
	"strings"
	"testing"
)

// The deploy --teardown prompt sends users to 'lagotto teardown' for the
// tables, so it must name every table that command deletes, the scheduled
// table included (#250).
func TestDeployTeardownPromptNamesEveryTable(t *testing.T) {
	p := deployTeardownPrompt("us-west-2", "123456789012", "lagotto-lambda-123456789012-us-west-2")
	for _, want := range []string{watchesTable, historyTable, "lagotto-scheduled-launches", "'lagotto teardown' deletes those"} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt lacks %q:\n%s", want, p)
		}
	}
}

// deploy --teardown keeps the per-launch schedules with the scheduled-launches
// table and names the command that deletes both (#260).
func TestDeployTeardownPromptNamesTheScheduleDeleter(t *testing.T) {
	p := deployTeardownPrompt("us-west-2", "123456789012", "bucket")
	if !strings.Contains(p, "'lagotto-launch-sl-*' schedules") || !strings.Contains(p, "'lagotto teardown' deletes both") {
		t.Errorf("prompt does not say what deletes the per-launch schedules:\n%s", p)
	}
}
