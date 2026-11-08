package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// The group notifications through the real Notifier: email gets the human
// text on the alerts topic, an SNS channel the JSON event, and no channel
// sends nothing.
func TestNotifyGroup_Publishes(t *testing.T) {
	w := &Watch{WatchID: "w-a", Action: ActionSpawn, NotifyChannels: []NotifyChannel{
		{Type: "email", Target: "a@example.com"},
		{Type: "sns", Target: "arn:aws:sns:us-west-2:111122223333:mine"},
	}}
	g := &WatchGroup{Name: "gpu", State: GroupLanded, WinnerWatch: "w-a", WinnerInstanceID: "i-1"}
	m := &MatchResult{InstanceType: "g6e.xlarge", InstanceID: "i-1", Region: "us-east-2", ActionTaken: "spawned", MatchedAt: time.Now(), LandedAt: time.Now()}
	pub := &capturePublisher{}
	n := &Notifier{snsClient: pub, topicArn: "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts", httpClient: &http.Client{}}
	if err := n.NotifyGroupLanded(context.Background(), w, g, m, []string{"w-b"}); err != nil {
		t.Fatal(err)
	}
	if len(pub.msgs) != 2 || !strings.Contains(aws.ToString(pub.msgs[0].Message), "Cancelled siblings: w-b") {
		t.Fatalf("published %d: %+v", len(pub.msgs), pub.msgs)
	}
	var ev map[string]interface{}
	if err := json.Unmarshal([]byte(aws.ToString(pub.msgs[1].Message)), &ev); err != nil || ev["event"] != EventGroupLanded || ev["landed_at"] == nil {
		t.Errorf("sns event = %v (%v)", ev, err)
	}
	m.GroupDetail = "lost"
	if err := n.NotifyGroupExtra(context.Background(), w, g, m); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(aws.ToString(pub.msgs[3].Message)), &ev); err != nil || ev["event"] != EventGroupExtra || ev["winner_watch_id"] != "w-a" {
		t.Errorf("extra sns event = %v (%v)", ev, err)
	}
	quiet := &Watch{WatchID: "w-q"}
	if n.NotifyGroupLanded(context.Background(), quiet, g, m, nil) != nil || n.NotifyGroupExtra(context.Background(), quiet, g, m) != nil || len(pub.msgs) != 4 {
		t.Error("a watch with no channels was notified")
	}

	p := &Poller{}
	if p.groupLandedNotifier() != nil || p.groupExtraNotifier() != nil {
		t.Error("no notifier wired, want nil")
	}
	p.notifier = n
	if p.groupLandedNotifier() == nil || p.groupExtraNotifier() == nil {
		t.Error("wired notifier not used")
	}
}

// resourceFate's other outcomes: a terminate that fails names the manual
// command; a kept reservation says it bills; no holder, no release.
func TestGroup_ResourceFate(t *testing.T) {
	h := newLandHarness(t)
	w := &Watch{WatchID: "w-a"}
	h.termErr = errors.New("UnauthorizedOperation: denied")
	if got := h.poller.resourceFate(context.Background(), w, landRegion, "i-1", ""); !strings.Contains(got, "could not be terminated") ||
		!strings.Contains(got, "spawn terminate i-1") {
		t.Errorf("failed terminate = %q", got)
	}
	if got := h.poller.resourceFate(context.Background(), w, landRegion, "", "cr-1"); !strings.Contains(got, "could not be released") ||
		!strings.Contains(got, "lagotto cancel w-a") {
		t.Errorf("no holder = %q", got)
	}
	w.GroupOnExtra = GroupOnExtraKeep
	if got := h.poller.resourceFate(context.Background(), w, landRegion, "", "cr-1"); !strings.Contains(got, "was kept") || !strings.Contains(got, "bills") {
		t.Errorf("kept reservation = %q", got)
	}
	if err := (&Poller{}).giveUpResource(context.Background(), landRegion, "i-1", ""); err == nil {
		t.Error("a poller with no spawner terminated something")
	}
}

// A failed terminate on an extra landing still cancels the member, and its
// tombstone says what to do.
func TestGroup_ExtraLandingTerminateFails(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	c, d := memberWatch(t, "w-c", "gpu"), memberWatch(t, "w-d", "gpu")
	h.join(t, c)
	h.join(t, d)
	if _, _, err := h.store.CompleteGroup(context.Background(), d, &MatchResult{InstanceID: "i-d"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.beginLandingAs(t, c, "i-c", "")
	h.termErr = errors.New("boom")
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running"}, nil }
	h.cycle(t)
	if cw := h.get(t, "w-c"); cw.Status != StatusCancelled || !strings.Contains(cw.CancelledReason, "spawn terminate i-c") {
		t.Errorf("w-c = %s %q", cw.Status, cw.CancelledReason)
	}
}

// The group's derived text for every state.
func TestGroup_CancelReasonAndSummary(t *testing.T) {
	if (&WatchGroup{}).LandedSummary() != "" || (*WatchGroup)(nil).LandedSummary() != "" {
		t.Error("summary of an unlanded group")
	}
	if got := (&WatchGroup{WinnerWatch: "w-a"}).LandedSummary(); got != "w-a" {
		t.Errorf("bare winner = %q", got)
	}
	if got := (&WatchGroup{WinnerWatch: "w-a", WinnerReservationID: "cr-1", WinnerRegion: "us-east-2"}).LandedSummary(); got != "w-a (cr-1, us-east-2)" {
		t.Errorf("reservation winner = %q", got)
	}
	if got := (&WatchGroup{Name: "g", State: GroupCancelled}).CancelReason(); got != "group g cancelled" {
		t.Errorf("cancelled = %q", got)
	}
	if got := (&WatchGroup{Name: "g", State: GroupHunting}).CancelReason(); got != "group g ended" {
		t.Errorf("hunting = %q", got)
	}
	if (&Watch{}).GroupOnExtraPolicy() != GroupOnExtraTerminate {
		t.Error("default policy")
	}
	if _, err := ParseGroupOnExtra("nope"); err == nil {
		t.Error("bad policy accepted")
	}
}

// A store whose table is gone: every group call reports the error rather than
// reading as "no group" (which would cancel members).
func TestGroup_StoreErrors(t *testing.T) {
	h := newLandHarness(t)
	ctx := context.Background()
	bad := &Store{client: h.store.client, watchesTable: "no-such-table", historyTable: "no-such-history"}
	if _, err := bad.GetGroup(ctx, "g"); err == nil {
		t.Error("GetGroup on a missing table")
	}
	if _, err := bad.JoinGroup(ctx, "g", "o", "w", 0); err == nil {
		t.Error("JoinGroup on a missing table")
	}
	if _, err := bad.ReserveGroupSlot(ctx, "g", "w", time.Now()); err == nil || errors.Is(err, ErrGroupSlotHeld) {
		t.Errorf("ReserveGroupSlot on a missing table: %v", err)
	}
	if err := bad.ReleaseGroupSlot(ctx, "g", "w"); err == nil {
		t.Error("ReleaseGroupSlot on a missing table")
	}
	if _, _, err := bad.CompleteGroup(ctx, &Watch{WatchID: "w", Group: "g"}, &MatchResult{}, time.Now()); err == nil {
		t.Error("CompleteGroup on a missing table")
	}
	if _, _, err := bad.CloseGroup(ctx, "g", "r"); err == nil {
		t.Error("CloseGroup on a missing table")
	}
	if _, err := bad.CancelGroupMember(ctx, "w", "r"); err == nil {
		t.Error("CancelGroupMember on a missing table")
	}
	// A group that does not exist: the slot claim says so.
	if _, err := h.store.ReserveGroupSlot(ctx, "absent", "w", time.Now()); !errors.Is(err, ErrGroupMissing) {
		t.Errorf("absent group: %v", err)
	}
	if _, _, err := h.store.CompleteGroup(ctx, &Watch{WatchID: "w", Group: "absent"}, &MatchResult{}, time.Now()); !errors.Is(err, ErrGroupMissing) {
		t.Errorf("complete absent group: %v", err)
	}
	if ok, err := h.store.CancelGroupMember(ctx, "w-none", "r"); ok || err != nil {
		t.Errorf("cancel a missing member: %v %v", ok, err)
	}

	// The poller side: an unreadable group neither cancels nor launches.
	w := memberWatch(t, "w-x", "g")
	p := &Poller{store: bad}
	s := &PollSummary{}
	if p.groupEnded(ctx, w, s) || p.claimGroupSlot(ctx, w, s) || s.GroupCancelled != 0 {
		t.Errorf("unreadable group: summary %+v", s)
	}
	if won, ok := p.winGroup(ctx, w, &MatchResult{}, time.Now(), s); won || ok {
		t.Error("winGroup on an unwritable group")
	}
	p.releaseGroupSlot(ctx, w) // logs, does not panic
}

// When the group cannot be written at the moment a launch is already
// running, the launch is recorded as landing so the next cycle decides it.
func TestGroup_UnwritableGroupAtImmediateLanding(t *testing.T) {
	h := newLandHarness(t)
	w := memberWatch(t, "w-a", "gpu")
	h.put(t, w)
	m := &MatchResult{WatchID: "w-a", InstanceID: "i-9", Region: landRegion, ActionTaken: "spawned", MatchedAt: time.Now()}
	if !h.poller.forceLanding(context.Background(), w, m, time.Now(), &PollSummary{}) {
		t.Fatal("forceLanding refused")
	}
	if got := h.get(t, "w-a"); got.PendingInstanceID != "i-9" {
		t.Errorf("pending = %q", got.PendingInstanceID)
	}
	if h.poller.forceLanding(context.Background(), w, &MatchResult{ActionTaken: "held"}, time.Now(), &PollSummary{}) {
		t.Error("forced a landing with no resource")
	}
}
