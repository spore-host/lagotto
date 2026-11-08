package arm

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

type countingInspector struct {
	info  deploy.PollerInfo
	calls int
}

func (c *countingInspector) InspectPoller(context.Context) (deploy.PollerInfo, error) {
	c.calls++
	return c.info, nil
}

func TestGroupPollerWarning(t *testing.T) {
	member := &watcher.Watch{WatchID: "w-g", Status: watcher.StatusActive, Group: "gpu"}
	plain := &watcher.Watch{WatchID: "w-p", Status: watcher.StatusActive}
	ended := *member
	ended.Status = watcher.StatusCancelled
	for _, tc := range []struct {
		name  string
		w     *watcher.Watch
		info  deploy.PollerInfo
		err   error
		wants []string
	}{
		{"older poller", member, deploy.PollerInfo{Exists: true, Version: "0.68.0"}, nil,
			[]string{"in us-east-1 is v0.68.0, older than v" + watcher.MinGroupPollerVersion, "independent watch", "group gpu", "lagotto deploy"}},
		{"untagged poller", member, deploy.PollerInfo{Exists: true}, nil, []string{"has no Version tag"}},
		{"the minimum", member, deploy.PollerInfo{Exists: true, Version: watcher.MinGroupPollerVersion}, nil, nil},
		{"dev build", member, deploy.PollerInfo{Exists: true, Version: "dev"}, nil, nil},
		{"no poller", member, deploy.PollerInfo{}, nil, nil},
		{"cannot inspect", member, deploy.PollerInfo{}, errors.New("AccessDenied"), nil},
		{"not a member", plain, deploy.PollerInfo{Exists: true, Version: "0.60.0"}, nil, nil},
		{"ended member", &ended, deploy.PollerInfo{Exists: true, Version: "0.60.0"}, nil, nil},
		{"nil", nil, deploy.PollerInfo{Exists: true, Version: "0.60.0"}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GroupPollerWarning(tc.info, tc.err, "us-east-1", tc.w)
			if len(tc.wants) == 0 {
				if got != "" {
					t.Fatalf("warning = %q, want none", got)
				}
				return
			}
			if !strings.HasPrefix(got, "Warning: ") || strings.Contains(got, "\n") {
				t.Errorf("not one Warning: line: %q", got)
			}
			for _, w := range tc.wants {
				if !strings.Contains(got, w) {
					t.Errorf("warning %q lacks %q", got, w)
				}
			}
		})
	}
	// Only a group member costs an inspection.
	ci := &countingInspector{info: deploy.PollerInfo{Exists: true, Version: "0.60.0"}}
	if CheckGroupPoller(context.Background(), ci, "", plain) != "" || ci.calls != 0 {
		t.Errorf("non-member inspected the poller (%d calls)", ci.calls)
	}
	if CheckGroupPoller(context.Background(), ci, "", member) == "" || ci.calls != 1 {
		t.Errorf("member: calls=%d", ci.calls)
	}
}
