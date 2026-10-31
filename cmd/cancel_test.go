package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestReadYes locks the confirmation convention (spawn#40): only an explicit
// yes proceeds; everything else — including EOF from a non-interactive/piped
// stdin — reads as "no" so `cancel` without --yes can't proceed unattended.
func TestReadYes(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"y\n", true},
		{"Y\n", true},
		{"yes\n", true},
		{"YES\n", true},
		{"  y  \n", true},
		{"n\n", false},
		{"no\n", false},
		{"\n", false}, // bare enter → default no
		{"", false},   // EOF / closed pipe → no
		{"yep\n", false},
	}
	for _, c := range cases {
		if got := readYes(strings.NewReader(c.in)); got != c.want {
			t.Errorf("readYes(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

type fakeReleaser struct {
	outcome watcher.ReleaseOutcome
	err     error
	calls   int
	region  string
	id      string
}

func (f *fakeReleaser) Release(_ context.Context, region, id string) (watcher.ReleaseOutcome, error) {
	f.calls++
	f.region, f.id = region, id
	return f.outcome, f.err
}

type statusRecorder struct{ set []watcher.WatchStatus }

func (s *statusRecorder) fn(_ context.Context, _ string, st watcher.WatchStatus) error {
	s.set = append(s.set, st)
	return nil
}

func yes(string) bool { return true }

func firedHold() *watcher.Watch {
	return &watcher.Watch{WatchID: "w-hold", Status: watcher.StatusMatched, Action: watcher.ActionHold,
		InstanceTypePattern: "p5.48xlarge",
		LastMatch: &watcher.MatchResult{Region: "us-east-2", AvailabilityZone: "us-east-2b",
			ActionTaken: "held", ReservationID: "cr-0abc123", MatchedAt: time.Unix(1700000000, 0)}}
}

// TestCancel_ReleasesFiredHold is #203: a matched --action hold watch's
// reservation is released through Holder.Release, once, in the match's region,
// and the watch keeps its matched status.
func TestCancel_ReleasesFiredHold(t *testing.T) {
	rel := &fakeReleaser{outcome: watcher.ReleaseOutcome{Released: true}}
	st := &statusRecorder{}
	var out, errOut bytes.Buffer
	if err := cancelWatch(context.Background(), &out, &errOut, firedHold(), rel, st.fn, yes); err != nil {
		t.Fatalf("cancelWatch: %v", err)
	}
	if rel.calls != 1 || rel.region != "us-east-2" || rel.id != "cr-0abc123" {
		t.Errorf("Release calls=%d region=%q id=%q, want 1 us-east-2 cr-0abc123", rel.calls, rel.region, rel.id)
	}
	if len(st.set) != 0 {
		t.Errorf("status changed to %v; a fired hold stays matched", st.set)
	}
	if !strings.Contains(out.String(), "Released capacity reservation cr-0abc123 (us-east-2b)") {
		t.Errorf("output = %q", out.String())
	}
}

// TestCancel_FiredHoldAlreadyEnded: releasing an already-expired or released
// reservation is success (idempotent), and says so.
func TestCancel_FiredHoldAlreadyEnded(t *testing.T) {
	rel := &fakeReleaser{outcome: watcher.ReleaseOutcome{AlreadyEnded: true, State: "expired"}}
	var out, errOut bytes.Buffer
	if err := cancelWatch(context.Background(), &out, &errOut, firedHold(), rel, (&statusRecorder{}).fn, yes); err != nil {
		t.Fatalf("already-ended reservation is an error: %v", err)
	}
	if !strings.Contains(out.String(), "already ended (expired)") {
		t.Errorf("output = %q", out.String())
	}
}

// TestCancel_FiredHoldNotPermitted: without ec2:CancelCapacityReservation the
// release fails with a message naming the permission and the auto-expiry.
func TestCancel_FiredHoldNotPermitted(t *testing.T) {
	rel := &fakeReleaser{err: fmt.Errorf("%w cr-0abc123: UnauthorizedOperation", watcher.ErrReleaseNotPermitted)}
	var out, errOut bytes.Buffer
	err := cancelWatch(context.Background(), &out, &errOut, firedHold(), rel, (&statusRecorder{}).fn, yes)
	if err == nil {
		t.Fatal("a refused release reported success")
	}
	for _, want := range []string{"ec2:CancelCapacityReservation", "us-east-2", "expires on its own"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q: %v", want, err)
		}
	}
}

func TestCancel_FiredHoldDeclined(t *testing.T) {
	rel := &fakeReleaser{}
	var out, errOut bytes.Buffer
	no := func(string) bool { return false }
	if err := cancelWatch(context.Background(), &out, &errOut, firedHold(), rel, (&statusRecorder{}).fn, no); err != nil {
		t.Fatal(err)
	}
	if rel.calls != 0 {
		t.Errorf("released %d times after the prompt was declined", rel.calls)
	}
}

// TestCancel_MatchedWithoutReservationRefused: a matched notify/spawn watch has
// nothing to cancel, as before.
func TestCancel_MatchedWithoutReservationRefused(t *testing.T) {
	w := firedHold()
	w.LastMatch.ReservationID = ""
	rel := &fakeReleaser{}
	var out, errOut bytes.Buffer
	err := cancelWatch(context.Background(), &out, &errOut, w, rel, (&statusRecorder{}).fn, yes)
	if err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("err = %v, want not active", err)
	}
	if rel.calls != 0 {
		t.Errorf("Release called %d times", rel.calls)
	}
}

// TestCancel_ActiveStillCancels: the active path is unchanged.
func TestCancel_ActiveStillCancels(t *testing.T) {
	w := &watcher.Watch{WatchID: "w-a", Status: watcher.StatusActive}
	rel := &fakeReleaser{}
	st := &statusRecorder{}
	var out, errOut bytes.Buffer
	if err := cancelWatch(context.Background(), &out, &errOut, w, rel, st.fn, yes); err != nil {
		t.Fatal(err)
	}
	if len(st.set) != 1 || st.set[0] != watcher.StatusCancelled {
		t.Errorf("status set = %v, want [cancelled]", st.set)
	}
	if rel.calls != 0 {
		t.Errorf("Release called %d times with no reservation", rel.calls)
	}
}

// TestStatusTable_ShowsReservation is #203: the status table names a fired
// hold's reservation and its AZ, which used to appear only in -o json.
func TestStatusTable_ShowsReservation(t *testing.T) {
	var out bytes.Buffer
	writeLastMatch(&out, firedHold())
	if !strings.Contains(out.String(), "Reservation: cr-0abc123 (us-east-2b)") {
		t.Errorf("status table lacks the reservation:\n%s", out.String())
	}
}
