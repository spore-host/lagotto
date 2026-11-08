package watcher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// ── #255: a spawn lost before it ran is recorded, with its reason, and notified ──

// The #255 incident, replayed: RunInstances succeeds, the instance is pending,
// then EC2 itself terminates it (Server.InternalError, during an AZ operational
// issue) before it ever runs. The watch must not complete; the reason and the
// time it was seen must reach history while EC2 still describes the instance;
// the watch's channels must hear about it; and the hunt must go on.
func TestLanding255_LostInPendingIsRecordedNotifiedAndRearmed(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-255"))
	h.cycle(t) // launches i-1, pending
	if got := h.get(t, "w-255"); got.Status != StatusActive || !got.Landing() {
		t.Fatalf("after RunInstances: status=%q landing=%v, want active and landing (not spawned)", got.Status, got.Landing())
	}

	h.describe = func(id string) (landingCheck, error) {
		if id != "i-1" {
			return landingCheck{State: "pending"}, nil
		}
		return landingCheck{State: "terminated", AZ: landRegion + "a",
			ReasonCode: "Server.InternalError", ReasonMessage: "Server.InternalError: Internal error on launch",
			TransitionReason: "Service initiated (2026-11-06 23:49:12 GMT)"}, nil
	}
	before := time.Now().UTC().Add(-time.Second)
	h.cycle(t)

	got := h.get(t, "w-255")
	if got.Status != StatusActive {
		t.Fatalf("status = %q, want active (hunting again)", got.Status)
	}
	assertLanding(t, got, "i-2") // re-armed: the next launch is already landing

	hist := h.history(t, "w-255")
	if len(hist) != 1 {
		t.Fatalf("history = %+v, want one row", hist)
	}
	r := hist[0]
	if r.ActionTaken != ActionLandingFailed || r.InstanceID != "i-1" || r.InstanceState != "terminated" ||
		r.AvailabilityZone != landRegion+"a" {
		t.Errorf("row = %+v", r)
	}
	// The code is not repeated, and EC2's own transition timestamp is kept.
	if want := "Server.InternalError: Internal error on launch (transition: Service initiated (2026-11-06 23:49:12 GMT))"; r.LandingReason != want {
		t.Errorf("landing_reason = %q, want %q", r.LandingReason, want)
	}
	if r.LostAt.Before(before) || !r.LostAt.After(r.MatchedAt.Add(-time.Second)) {
		t.Errorf("lost_at = %v (matched_at %v), want the time the poller saw it", r.LostAt, r.MatchedAt)
	}

	if len(h.notified) != 0 {
		t.Errorf("match notified = %+v, want none (nothing landed)", h.notified)
	}
	if len(h.lost) != 1 || h.lostStop[0] || h.lost[0].InstanceID != "i-1" || h.lost[0].LandingReason != r.LandingReason {
		t.Errorf("lost = %+v stopped=%v, want one hunting-again notice for i-1 with the reason", h.lost, h.lostStop)
	}
}

// A launch that never leaves pending is given up on and that is notified too.
func TestLanding255_TimedOutIsNotified(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-to"))
	h.cycle(t)
	h.backdate(t, "w-to", PendingLandingTimeout+time.Minute)
	h.cycle(t)
	if len(h.lost) != 1 || h.lost[0].ActionTaken != ActionLandingTimedOut || h.lostStop[0] ||
		!strings.Contains(h.lost[0].LandingReason, "lagotto terminated it") {
		t.Errorf("lost = %+v stopped=%v", h.lost, h.lostStop)
	}
	if hist := h.history(t, "w-to"); len(hist) != 1 || hist[0].LostAt.IsZero() {
		t.Errorf("history = %+v, want a timed-out row with lost_at", hist)
	}
}

// A failing lost-launch notifier never blocks the hunt.
func TestLanding255_NotifyErrorStillRearms(t *testing.T) {
	h := newLandHarness(t)
	h.poller.notifyLost = func(context.Context, *Watch, *MatchResult, bool) error {
		return &capErr{"Throttling"}
	}
	h.put(t, landWatch(t, "w-nerr"))
	h.cycle(t)
	h.describe = func(id string) (landingCheck, error) {
		if id != "i-1" {
			return landingCheck{State: "pending"}, nil
		}
		return landingCheck{State: "terminated", ReasonCode: "Server.InternalError"}, nil
	}
	h.cycle(t)
	assertLanding(t, h.get(t, "w-nerr"), "i-2")
}

func TestLandingReason(t *testing.T) {
	for _, c := range []struct {
		chk  landingCheck
		want string
	}{
		{landingCheck{}, ""},
		{landingCheck{ReasonCode: "Server.InsufficientInstanceCapacity", ReasonMessage: "Insufficient capacity."},
			"Server.InsufficientInstanceCapacity: Insufficient capacity."},
		{landingCheck{ReasonCode: "Client.InstanceInitiatedShutdown", ReasonMessage: "Client.InstanceInitiatedShutdown: Instance initiated shutdown"},
			"Client.InstanceInitiatedShutdown: Instance initiated shutdown"},
		{landingCheck{TransitionReason: "Service initiated (2026-11-06 23:49:12 GMT)"}, "Service initiated (2026-11-06 23:49:12 GMT)"},
		{landingCheck{ReasonCode: "Server.InternalError", TransitionReason: " "}, "Server.InternalError"},
		// A transition reason the message already contains is not repeated.
		{landingCheck{ReasonCode: "X", ReasonMessage: "boom (2026)", TransitionReason: "boom (2026)"}, "X: boom (2026)"},
		{landingCheck{ReasonCode: "X", ReasonMessage: "line1\nline2"}, "X: line1 line2"},
	} {
		if got := landingReason(c.chk); got != c.want {
			t.Errorf("landingReason(%+v) = %q, want %q", c.chk, got, c.want)
		}
	}
}

func TestDescribeInstanceState_TransitionReason(t *testing.T) {
	out := &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{{
		InstanceId:            aws.String("i-1"),
		State:                 &ec2types.InstanceState{Name: ec2types.InstanceStateNameShuttingDown},
		StateTransitionReason: aws.String("Service initiated (2026-11-06 23:49:12 GMT)"),
	}}}}}
	c, err := describeInstanceState(context.Background(), &fakeDescribeInstances{out: out}, "i-1")
	if err != nil || c.TransitionReason != "Service initiated (2026-11-06 23:49:12 GMT)" || c.State != "shutting-down" {
		t.Errorf("check = %+v, %v", c, err)
	}
}

func lostMatch() *MatchResult {
	return &MatchResult{
		WatchID: "w-255", InstanceType: "g7e.48xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2a",
		MatchedAt: time.Date(2026, 11, 6, 23, 47, 29, 0, time.UTC), ActionTaken: ActionLandingFailed,
		InstanceID: "i-0abc", InstanceState: "terminated", LandingReason: "Server.InternalError: Internal error on launch",
		LostAt: time.Date(2026, 11, 6, 23, 52, 0, 0, time.UTC),
	}
}

func TestNotifyLandingLost_Content(t *testing.T) {
	w := &Watch{WatchID: "w-255", InstanceTypePattern: "g7e.*"}
	m := lostMatch()

	if got := landingLostSubject(m, false); got != "[lagotto] g7e.48xlarge launch in us-west-2 lost before it landed: still hunting" {
		t.Errorf("subject = %q", got)
	}
	if got := landingLostSubject(m, true); !strings.HasSuffix(got, "watch stopped") {
		t.Errorf("stopped subject = %q", got)
	}
	body := landingLostBody(w, m, false)
	for _, s := range []string{"instance i-0abc", "w-255", "us-west-2a", "2026-11-06T23:47:29Z", "2026-11-06T23:52:00Z",
		"terminated", "Server.InternalError: Internal error on launch", "still ACTIVE", "lagotto cancel w-255"} {
		if !strings.Contains(body, s) {
			t.Errorf("body missing %q:\n%s", s, body)
		}
	}
	if strings.Contains(strings.ToLower(body), "capacity found") {
		t.Errorf("a lost launch must not read as a match:\n%s", body)
	}
	if b := landingLostBody(w, m, true); !strings.Contains(b, "STOPPED") {
		t.Errorf("stopped body:\n%s", b)
	}
	if b := landingLostBody(w, &MatchResult{ReservationID: "cr-1", ReservationState: "failed"}, false); !strings.Contains(b, "capacity reservation cr-1") ||
		!strings.Contains(b, "(EC2 gave no reason)") || !strings.Contains(b, "failed") {
		t.Errorf("hold body:\n%s", b)
	}

	p := landingLostPayload(w, m, false)
	data, _ := json.Marshal(p)
	for _, s := range []string{`"event":"landing_failed"`, `"status":"active"`, `"instance_id":"i-0abc"`,
		`"state":"terminated"`, `"lost_at":"2026-11-06T23:52:00Z"`, `"launched_at":"2026-11-06T23:47:29Z"`,
		`"reason":"Server.InternalError: Internal error on launch"`} {
		if !strings.Contains(string(data), s) {
			t.Errorf("payload missing %s: %s", s, data)
		}
	}
	if p := landingLostPayload(w, m, true); p["status"] != string(StatusFailed) {
		t.Errorf("stopped payload status = %v", p["status"])
	}
}

func TestNotifyLandingLost_NoChannelsIsNoop(t *testing.T) {
	var n Notifier // no clients: any send would panic
	if err := n.NotifyLandingLost(context.Background(), &Watch{}, lostMatch(), false); err != nil {
		t.Errorf("err = %v", err)
	}
}

func TestLostNotifierResolution(t *testing.T) {
	p := &Poller{}
	if p.lostNotifier() != nil {
		t.Error("no notifier wired, want nil")
	}
	p.notifier = &Notifier{}
	if p.lostNotifier() == nil {
		t.Error("wired notifier not used")
	}
}
