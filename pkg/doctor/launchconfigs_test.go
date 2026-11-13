package doctor

import (
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestEvalLaunchConfigs(t *testing.T) {
	f := *watcher.RoleUnauthorizedFinding("o/sl-0011aabb", "r", []string{"iam:PassRole"})
	denied := errors.New("AccessDenied")
	for _, c := range []struct {
		name     string
		findings []watcher.LaunchConfigFinding
		err      error
		want     Status
		detail   string
	}{
		{"finding", []watcher.LaunchConfigFinding{f}, nil, StatusFail, "sl-0011aabb: iam_role"},
		{"finding and unreadable", []watcher.LaunchConfigFinding{f}, denied, StatusFail, "not fully checked"},
		{"unreadable", nil, denied, StatusWarn, ""},
		{"clean", nil, nil, StatusPass, ""},
	} {
		got := EvalLaunchConfigs(c.findings, 3, c.err)
		if got.Name != CheckLaunchConfigs || got.Status != c.want {
			t.Errorf("%s: %s %s, want %s", c.name, got.Name, got.Status, c.want)
		}
		if c.detail != "" && !strings.Contains(strings.Join(got.Detail, "\n"), c.detail) {
			t.Errorf("%s: detail %q lacks %q", c.name, got.Detail, c.detail)
		}
		if c.want == StatusFail && len(got.Fix) == 0 {
			t.Errorf("%s: no fix", c.name)
		}
	}
}
