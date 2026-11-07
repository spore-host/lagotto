package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// #239 phase 2: a watch waiting for its launch or hold to land.

func landingWatch(now time.Time) *watcher.Watch {
	since := now.Add(-3 * time.Minute)
	return &watcher.Watch{
		WatchID: "w-land", Status: watcher.StatusActive, Action: watcher.ActionSpawn,
		InstanceTypePattern: "m7g.medium", Regions: []string{"us-east-2"},
		ExpiresAt: now.Add(time.Hour), Attempts: 1,
		PendingInstanceID: "i-0abc", PendingRegion: "us-east-2", PendingSince: &since,
	}
}

func TestListTable_Landing(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	plain := landingWatch(now)
	plain.WatchID, plain.PendingInstanceID, plain.PendingSince = "w-hunt", "", nil
	var b bytes.Buffer
	writeWatchTable(&b, []watcher.Watch{*landingWatch(now), *plain}, now, false)
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("table:\n%s", b.String())
	}
	if f := strings.Fields(lines[1]); f[1] != "landing" || !strings.HasSuffix(lines[1], "landing i-0abc (pending 3m)") {
		t.Errorf("landing row = %q", lines[1])
	}
	if f := strings.Fields(lines[2]); f[1] != "active" || strings.Contains(lines[2], "landing") {
		t.Errorf("hunting row = %q", lines[2])
	}
}

func TestStatusCol(t *testing.T) {
	now := time.Now()
	w := landingWatch(now)
	if got := statusCol(w); got != "landing" {
		t.Errorf("statusCol = %q", got)
	}
	w.Status = watcher.StatusMatched // a resolved record never reads as landing
	if got := statusCol(w); got != "matched" || landingDesc(w, now) != "" {
		t.Errorf("statusCol = %q, desc = %q", got, landingDesc(w, now))
	}
	w.Status, w.PendingSince = watcher.StatusActive, nil
	if got := landingDesc(w, now); got != "landing i-0abc" {
		t.Errorf("desc without pending_since = %q", got)
	}
}

func TestStatusLanding(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	var b bytes.Buffer
	writeLanding(&b, landingWatch(now), now)
	got := b.String()
	for _, want := range []string{
		"Landing:  i-0abc (pending 3m) in us-east-2; given up at 2026-11-06T12:07:00Z if still pending",
		"launches nothing more until this resolves",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status landing block lacks %q:\n%s", want, got)
		}
	}

	hold := landingWatch(now)
	hold.Action, hold.PendingInstanceID, hold.PendingReservationID = watcher.ActionHold, "", "cr-0abc"
	b.Reset()
	writeLanding(&b, hold, now)
	if !strings.Contains(b.String(), "cr-0abc (pending 3m)") || !strings.Contains(b.String(), "reserves nothing more") {
		t.Errorf("hold landing block:\n%s", b.String())
	}

	b.Reset()
	plain := landingWatch(now)
	plain.PendingInstanceID = ""
	writeLanding(&b, plain, now)
	if b.Len() != 0 {
		t.Errorf("a hunting watch printed a landing block: %q", b.String())
	}
}

// Cancelling a landing hold releases the pending reservation, in its region.
func TestCancel_ReleasesPendingReservation(t *testing.T) {
	now := time.Now()
	w := landingWatch(now)
	w.Action, w.PendingInstanceID, w.PendingReservationID = watcher.ActionHold, "", "cr-0pend"
	w.PendingMatch = &watcher.MatchResult{AvailabilityZone: "us-east-2a"}
	rel := &fakeReleaser{outcome: watcher.ReleaseOutcome{Released: true}}
	st := &statusRecorder{}
	var out, errOut bytes.Buffer
	if err := cancelWatch(context.Background(), &out, &errOut, w, rel, st.fn, yes); err != nil {
		t.Fatal(err)
	}
	if rel.calls != 1 || rel.id != "cr-0pend" || rel.region != "us-east-2" {
		t.Errorf("release = %d %q %q", rel.calls, rel.id, rel.region)
	}
	if !strings.Contains(out.String(), "Released capacity reservation cr-0pend (us-east-2a)") {
		t.Errorf("out = %q", out.String())
	}
	if len(st.set) != 1 || st.set[0] != watcher.StatusCancelled {
		t.Errorf("status writes = %v", st.set)
	}
}

// Cancelling a landing spawn does not terminate the instance; it says so.
func TestCancel_LandingInstanceNote(t *testing.T) {
	w := landingWatch(time.Now())
	rel := &fakeReleaser{}
	st := &statusRecorder{}
	var out, errOut bytes.Buffer
	if err := cancelWatch(context.Background(), &out, &errOut, w, rel, st.fn, yes); err != nil {
		t.Fatal(err)
	}
	if rel.calls != 0 {
		t.Errorf("released %d reservations for a spawn watch", rel.calls)
	}
	if !strings.Contains(errOut.String(), "instance i-0abc (us-east-2)") || !strings.Contains(errOut.String(), "spawn terminate i-0abc") {
		t.Errorf("errOut = %q", errOut.String())
	}
}
