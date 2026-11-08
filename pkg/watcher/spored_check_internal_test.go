package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// ── #255 item 1b: the advisory spored check-in follow-up (option B) ──

type sporedHarness struct {
	*landHarness
	unconfirmed []MatchResult
}

func newSporedHarness(t *testing.T) *sporedHarness {
	t.Helper()
	h := &sporedHarness{landHarness: newLandHarness(t)}
	h.poller.notifySporedUnconfirmed = func(_ context.Context, _ *Watch, m *MatchResult) error {
		h.unconfirmed = append(h.unconfirmed, *m)
		return nil
	}
	return h
}

// matched launches i-1 and lands it running with no spored tag yet.
func (h *sporedHarness) matched(t *testing.T, id string) {
	t.Helper()
	h.put(t, landWatch(t, id))
	h.cycle(t)
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "c"}, nil }
	h.cycle(t)
}

// closeWindow moves the pending check's deadline into the past.
func (h *sporedHarness) closeWindow(t *testing.T, id string) {
	t.Helper()
	if _, err := h.store.client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        &h.store.watchesTable,
		Key:              map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: id}},
		UpdateExpression: aws.String("SET spored_check_until = :t"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)},
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func assertNoCheck(t *testing.T, w *Watch) {
	t.Helper()
	if w.SporedCheckInstanceID != "" || w.SporedCheckRegion != "" || w.SporedCheckUntil != nil {
		t.Errorf("spored check still pending: %q %q %v", w.SporedCheckInstanceID, w.SporedCheckRegion, w.SporedCheckUntil)
	}
}

// Running with no tag: matched exactly as before, plus a pending check. Inside
// the window a cycle describes it once and writes nothing; once the window
// passes it records spored_unconfirmed, notifies, never terminates, and stops.
func TestSpored_UnconfirmedAfterWindow(t *testing.T) {
	h := newSporedHarness(t)
	h.matched(t, "w-sp")
	w := h.get(t, "w-sp")
	if w.Status != StatusMatched || w.SporedCheckInstanceID != "i-1" || w.SporedCheckRegion != landRegion || w.SporedCheckUntil == nil {
		t.Fatalf("after landing: status=%q check=%q/%q/%v, want matched with a pending check",
			w.Status, w.SporedCheckInstanceID, w.SporedCheckRegion, w.SporedCheckUntil)
	}
	if d := w.SporedCheckUntil.Sub(w.LastMatch.LandedAt); d != SporedCheckWindow {
		t.Errorf("window = %s, want %s from landed_at", d, SporedCheckWindow)
	}
	if len(h.notified) != 1 {
		t.Fatalf("match notified %d times, want once", len(h.notified))
	}

	before := len(h.described)
	s := h.cycle(t)
	if len(h.described) != before+1 || s.SporedChecks != 1 || s.SporedUnconfirmed != 0 {
		t.Errorf("inside the window: described %d more, summary %+v; want one describe, no resolution", len(h.described)-before, s)
	}
	if hist := h.history(t, "w-sp"); len(hist) != 1 || h.get(t, "w-sp").SporedCheckInstanceID != "i-1" {
		t.Errorf("inside the window something was written: %+v", hist)
	}

	h.closeWindow(t, "w-sp")
	s = h.cycle(t)
	if s.SporedUnconfirmed != 1 {
		t.Errorf("summary = %+v, want one spored_unconfirmed", s)
	}
	if len(h.terminated) != 0 {
		t.Fatalf("terminated %v: the check is advisory and must never terminate", h.terminated)
	}
	got := h.get(t, "w-sp")
	assertNoCheck(t, got)
	if got.Status != StatusMatched {
		t.Errorf("status = %q, want still matched", got.Status)
	}
	hist := h.history(t, "w-sp")
	var row *MatchResult
	for i := range hist {
		if hist[i].ActionTaken == ActionSporedUnconfirmed {
			row = &hist[i]
		}
	}
	if len(hist) != 2 || row == nil {
		t.Fatalf("history = %+v, want spawned + spored_unconfirmed", hist)
	}
	if row.InstanceID != "i-1" || row.InstanceState != "running" ||
		!strings.Contains(row.SporedDetail, "spored has not checked in") ||
		!strings.Contains(row.SporedDetail, "after it was seen running") ||
		!strings.Contains(row.SporedDetail, "iam_role") || !strings.Contains(row.SporedDetail, "ec2:CreateTags") ||
		!strings.Contains(row.SporedDetail, "did not terminate") {
		t.Errorf("row = %+v", row)
	}
	if len(h.unconfirmed) != 1 || h.unconfirmed[0].SporedDetail != row.SporedDetail {
		t.Errorf("unconfirmed notices = %+v, want one with the row's detail", h.unconfirmed)
	}

	// Bounded: no more describes once resolved.
	before = len(h.described)
	if s := h.cycle(t); len(h.described) != before || s.SporedChecks != 0 {
		t.Errorf("after resolution: %d more describes, summary %+v; want none", len(h.described)-before, s)
	}
}

// The tag appears: the check is cleared, spored_confirmed_at is recorded on the
// watch's last_match and the spawned row, and nothing else is written or sent.
func TestSpored_ConfirmedClearsQuietly(t *testing.T) {
	h := newSporedHarness(t)
	h.matched(t, "w-ok")
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", SporedSeen: true}, nil }
	h.cycle(t)
	got := h.get(t, "w-ok")
	assertNoCheck(t, got)
	if got.LastMatch == nil || got.LastMatch.SporedConfirmedAt == nil {
		t.Errorf("last_match.spored_confirmed_at not set: %+v", got.LastMatch)
	}
	hist := h.history(t, "w-ok")
	if len(hist) != 1 || hist[0].ActionTaken != "spawned" || hist[0].SporedConfirmedAt == nil {
		t.Errorf("history = %+v, want the spawned row with spored_confirmed_at", hist)
	}
	if len(h.unconfirmed) != 0 {
		t.Errorf("notified unconfirmed: %+v", h.unconfirmed)
	}
	h.closeWindow(t, "w-ok") // no check left: must not resurrect one
	h.cycle(t)
	if len(h.unconfirmed) != 0 || len(h.history(t, "w-ok")) != 1 {
		t.Error("a confirmed watch was reported later")
	}
}

// Already tagged when it is first seen running: nothing to follow up.
func TestSpored_SeenAtLanding(t *testing.T) {
	h := newSporedHarness(t)
	h.put(t, landWatch(t, "w-fast"))
	h.cycle(t)
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", SporedSeen: true}, nil }
	h.cycle(t)
	got := h.get(t, "w-fast")
	assertNoCheck(t, got)
	if got.Status != StatusMatched || got.LastMatch.SporedConfirmedAt == nil {
		t.Errorf("status=%q last_match=%+v", got.Status, got.LastMatch)
	}
}

// Gone (or stopped) after running, before spored checked in: a fail-closed
// bootstrap. Reported at once, not at the window, and still not terminated.
func TestSpored_GoneBeforeCheckIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		chk  landingCheck
		want string
	}{
		{"terminated", landingCheck{State: "terminated", ReasonCode: "Client.InstanceInitiatedShutdown"}, "is terminated and spored never checked in (Client.InstanceInitiatedShutdown)"},
		{"stopped", landingCheck{State: "stopped"}, "is stopped and spored never checked in"},
		{"not found", landingCheck{NotFound: true}, "is gone and spored never checked in"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSporedHarness(t)
			h.matched(t, "w-gone")
			h.describe = func(string) (landingCheck, error) { return tc.chk, nil }
			h.cycle(t)
			if len(h.unconfirmed) != 1 || !strings.Contains(h.unconfirmed[0].SporedDetail, tc.want) {
				t.Errorf("notices = %+v, want one containing %q", h.unconfirmed, tc.want)
			}
			if len(h.terminated) != 0 {
				t.Errorf("terminated %v", h.terminated)
			}
			assertNoCheck(t, h.get(t, "w-gone"))
		})
	}
}

// A describe error inside the window retries next cycle; past the window the
// check is dropped without claiming spored failed.
func TestSpored_DescribeError(t *testing.T) {
	h := newSporedHarness(t)
	h.matched(t, "w-err")
	h.describe = func(string) (landingCheck, error) { return landingCheck{}, errors.New("throttled") }
	h.cycle(t)
	if h.get(t, "w-err").SporedCheckInstanceID != "i-1" {
		t.Fatal("a describe error inside the window dropped the check")
	}
	h.closeWindow(t, "w-err")
	h.cycle(t)
	assertNoCheck(t, h.get(t, "w-err"))
	if len(h.unconfirmed) != 0 || len(h.history(t, "w-err")) != 1 {
		t.Errorf("an unreadable check was reported as unconfirmed: %+v", h.unconfirmed)
	}
}

// Two pollers holding the same stale copy: only one records and notifies.
func TestSpored_RaceResolvesOnce(t *testing.T) {
	h := newSporedHarness(t)
	h.matched(t, "w-race")
	h.closeWindow(t, "w-race")
	stale := h.get(t, "w-race")
	now := time.Now().UTC()
	h.poller.checkSpored(context.Background(), stale, now, &PollSummary{})
	h.poller.checkSpored(context.Background(), stale, now, &PollSummary{})
	if len(h.unconfirmed) != 1 {
		t.Errorf("notices = %d, want 1", len(h.unconfirmed))
	}
	if err := h.store.endSporedCheck(context.Background(), stale, "i-1", nil); !errors.Is(err, errLandingResolved) {
		t.Errorf("endSporedCheck on a resolved check = %v, want errLandingResolved", err)
	}
}

// Running already when RunInstances returned: the check is armed on that path too.
func TestSpored_ArmedOnImmediateRunning(t *testing.T) {
	h := newSporedHarness(t)
	h.poller.spawner.provision = func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		h.launches = append(h.launches, "i-1")
		return &spawnaws.LaunchResult{InstanceID: "i-1", AvailabilityZone: landRegion + "b", State: "running"}, nil
	}
	h.put(t, landWatch(t, "w-imm"))
	h.cycle(t)
	if got := h.get(t, "w-imm"); got.Status != StatusMatched || got.SporedCheckInstanceID != "i-1" {
		t.Errorf("status=%q check=%q, want matched and armed", got.Status, got.SporedCheckInstanceID)
	}
}

// No describer, no hold, no check: holds and legacy pollers are untouched.
func TestSpored_NotArmedWithoutDescriberOrForHold(t *testing.T) {
	h := newSporedHarness(t)
	h.poller.spawner.describeInstance = nil
	h.put(t, landWatch(t, "w-legacy"))
	h.cycle(t)
	assertNoCheck(t, h.get(t, "w-legacy"))
	h.cycle(t) // pollSporedChecks with no describer is a no-op

	hh, _ := newHoldHarness(t, ec2types.CapacityReservationStateActive)
	hh.put(t, holdWatch(t, "w-hold"))
	hh.cycle(t)
	assertNoCheck(t, hh.get(t, "w-hold"))
}

// A scoped poller only follows up its own watches.
func TestSpored_RespectsFilter(t *testing.T) {
	h := newSporedHarness(t)
	h.matched(t, "w-mine")
	h.closeWindow(t, "w-mine")
	h.poller.filter = &WatchFilter{WatchIDs: []string{"w-other"}}
	if s := h.cycle(t); s.SporedChecks != 0 || len(h.unconfirmed) != 0 {
		t.Errorf("a filtered-out watch was followed up: %+v", s)
	}
}

func TestHasSporedTag(t *testing.T) {
	tag := func(k, v string) ec2types.Tag { return ec2types.Tag{Key: aws.String(k), Value: aws.String(v)} }
	for _, tc := range []struct {
		tags []ec2types.Tag
		want bool
	}{
		{nil, false},
		{[]ec2types.Tag{tag("spawn:managed", "true")}, false},
		{[]ec2types.Tag{tag("spawn:last-heartbeat", "")}, false},
		{[]ec2types.Tag{tag("spawn:last-heartbeat", "2026-11-06T23:50:00Z")}, true},
		{[]ec2types.Tag{tag("spawn:managed", "true"), tag("spawn:spored-version", "0.161.0")}, true},
	} {
		if got := hasSporedTag(tc.tags); got != tc.want {
			t.Errorf("hasSporedTag(%v) = %v, want %v", tc.tags, got, tc.want)
		}
	}
}

func TestNotifySporedUnconfirmed_Content(t *testing.T) {
	w := &Watch{WatchID: "w-sp"}
	m := &MatchResult{InstanceID: "i-0abc", InstanceType: "g7e.48xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2a",
		InstanceState: "running", SporedDetail: "instance i-0abc is running but spored has not checked in",
		LandedAt: time.Date(2026, 11, 6, 23, 50, 0, 0, time.UTC), MatchedAt: time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)}
	if got := sporedUnconfirmedSubject(m); got != "[lagotto] g7e.48xlarge i-0abc in us-west-2: spored has not checked in" {
		t.Errorf("subject = %q", got)
	}
	body := sporedUnconfirmedBody(w, m)
	for _, s := range []string{"w-sp", "i-0abc", "us-west-2a", "2026-11-06T23:50:00Z", "running", m.SporedDetail, "stays matched"} {
		if !strings.Contains(body, s) {
			t.Errorf("body missing %q:\n%s", s, body)
		}
	}
	data, _ := json.Marshal(sporedUnconfirmedPayload(w, m))
	for _, s := range []string{`"event":"spored_unconfirmed"`, `"instance_id":"i-0abc"`, `"running_at":"2026-11-06T23:50:00Z"`, `"checked_at":"2026-11-07T00:00:00Z"`} {
		if !strings.Contains(string(data), s) {
			t.Errorf("payload missing %s: %s", s, data)
		}
	}
	if p := sporedUnconfirmedPayload(w, &MatchResult{}); p["running_at"] != nil {
		t.Errorf("payload without landed_at has running_at: %v", p)
	}
	if b := sporedUnconfirmedBody(w, &MatchResult{}); !strings.Contains(b, "Running At:        -") {
		t.Errorf("body without landed_at:\n%s", b)
	}
	if err := (&Notifier{}).NotifySporedUnconfirmed(context.Background(), &Watch{}, m); err != nil {
		t.Errorf("no channels: %v", err)
	}
	pub := &capturePublisher{}
	n := &Notifier{snsClient: pub, topicArn: "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts"}
	w.NotifyChannels = []NotifyChannel{{Type: "email", Target: "a@example.com"}}
	if err := n.NotifySporedUnconfirmed(context.Background(), w, m); err != nil || len(pub.msgs) != 1 {
		t.Errorf("email: err=%v msgs=%d", err, len(pub.msgs))
	}
}

func TestSporedNotifierResolution(t *testing.T) {
	p := &Poller{}
	if p.sporedNotifier() != nil {
		t.Error("no notifier wired, want nil")
	}
	p.notifier = &Notifier{}
	if p.sporedNotifier() == nil {
		t.Error("wired notifier not used")
	}
}
