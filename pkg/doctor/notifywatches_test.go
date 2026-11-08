package doctor

import (
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestEvalNotifyWatches(t *testing.T) {
	if _, ok := EvalNotifyWatches(nil); ok {
		t.Error("no active watches should say nothing")
	}
	others := []watcher.Watch{
		{WatchID: "w-s", Action: watcher.ActionSpawn},
		{WatchID: "w-h", Action: watcher.ActionHold},
	}
	if _, ok := EvalNotifyWatches(others); ok {
		t.Error("spawn and hold watches should say nothing")
	}

	active := append(others,
		watcher.Watch{WatchID: "w-n1", Action: watcher.ActionNotify, InstanceTypePattern: "p5.48xlarge",
			Regions: []string{"us-west-2", "us-east-1"}, UserID: "arn:aws:iam::123456789012:user/a"},
		watcher.Watch{WatchID: "w-n2", Action: watcher.ActionNotify, InstanceTypePattern: "g6.*"})
	c, ok := EvalNotifyWatches(active)
	if !ok || c.Status != StatusWarn || c.Name != CheckNotifyWatches || !strings.HasPrefix(c.Summary, "2 active --action notify") {
		t.Fatalf("check: %+v", c)
	}
	detail := strings.Join(c.Detail, "\n")
	for _, want := range []string{
		"w-n1  p5.48xlarge  us-west-2,us-east-1  (owner arn:aws:iam::123456789012:user/a)",
		"w-n2  g6.*  all enabled regions",
		watcher.NotifyDeprecation, "lagotto cancel <watch-id>",
	} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail lacks %q:\n%s", want, detail)
		}
	}
	if strings.Contains(detail, "w-s") || strings.Contains(detail, "w-h") {
		t.Errorf("detail lists a spawn or hold watch:\n%s", detail)
	}

	// Never a failure: the doctor exit code is unaffected.
	r := &Report{Checks: []Check{c}}
	if r.Failed() {
		t.Error("the notify-watch check failed the report")
	}
}
