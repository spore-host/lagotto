package main

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

type fakeWork struct {
	active     int
	pending    bool
	activeErr  error
	pendingErr error
}

func (f fakeWork) ListActiveWatches(context.Context) ([]watcher.Watch, error) {
	return make([]watcher.Watch, f.active), f.activeErr
}

func (f fakeWork) HasPendingScheduledLaunches(context.Context) (bool, error) {
	return f.pending, f.pendingErr
}

var _ workCounter = (*watcher.Store)(nil)

// TestIdleCheck: idle only with no active watch AND no pending scheduled
// launch (the #49 refcount); an unknown count is an error, so the poller
// leaves its schedule as it is (#323).
func TestIdleCheck(t *testing.T) {
	boom := errors.New("throttled")
	for _, c := range []struct {
		name     string
		work     fakeWork
		idle     bool
		wantsErr bool
	}{
		{"nothing", fakeWork{}, true, false},
		{"an active watch", fakeWork{active: 1}, false, false},
		{"a pending scheduled launch", fakeWork{pending: true}, false, false},
		{"watches unknown", fakeWork{activeErr: boom}, false, true},
		{"launches unknown", fakeWork{pendingErr: boom}, false, true},
	} {
		idle, err := idleCheck(c.work)(context.Background())
		if idle != c.idle || (err != nil) != c.wantsErr {
			t.Errorf("%s: got (%v, %v), want idle=%v err=%v", c.name, idle, err, c.idle, c.wantsErr)
		}
	}
}
