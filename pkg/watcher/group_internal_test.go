package watcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// ── #287: watch groups ──
//
// These drive the real PollAll path (landing, slot claim, Spawner.Spawn,
// Holder.Hold) against a substrate-backed store, reusing the #239 landing
// harness: a fake capacity search, a fake RunInstances and a describe whose
// answer the test sets per cycle. Every cycle reloads the watches from the
// store, as the stateless hosted poller does.

type groupNotes struct {
	landed    []string   // "<winner>:<cancelled,…>"
	cancelled [][]string // the cancelled list of each group notification
	channels  [][]NotifyChannel
	extra     []MatchResult
}

func withGroupNotes(h *landHarness) *groupNotes {
	n := &groupNotes{}
	h.poller.notifyGroupLanded = func(_ context.Context, w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) error {
		n.landed = append(n.landed, w.WatchID+":"+strings.Join(cancelled, ","))
		n.cancelled = append(n.cancelled, cancelled)
		n.channels = append(n.channels, w.NotifyChannels)
		return nil
	}
	h.poller.notifyGroupExtra = func(_ context.Context, _ *Watch, _ *WatchGroup, m *MatchResult) error {
		n.extra = append(n.extra, *m)
		return nil
	}
	return n
}

// join stores w as a member of its group, the way `lagotto watch --group` does.
func (h *landHarness) join(t *testing.T, w *Watch) {
	t.Helper()
	if _, err := h.store.JoinGroup(context.Background(), w.GroupRef(), w.UserID, w.WatchID, w.TTLTimestamp); err != nil {
		t.Fatalf("JoinGroup(%s): %v", w.WatchID, err)
	}
	h.put(t, w)
}

func memberWatch(t *testing.T, id, group string) *Watch {
	w := landWatch(t, id)
	w.Group = group
	w.NotifyChannels = []NotifyChannel{{Type: "webhook", Target: "https://example.com/" + id}}
	return w
}

func (h *landHarness) group(t *testing.T, name string) *WatchGroup {
	t.Helper()
	g, err := h.store.GetGroup(context.Background(), name)
	if err != nil || g == nil {
		t.Fatalf("GetGroup(%s): %v %v", name, g, err)
	}
	return g
}

// beginLandingAs puts w straight into the landing state with instance id, as
// a launch that bypassed the slot would (an expired slot, or a pre-#287
// poller that ignores groups).
func (h *landHarness) beginLandingAs(t *testing.T, w *Watch, instanceID, reservationID string) {
	t.Helper()
	m := &MatchResult{WatchID: w.WatchID, UserID: w.UserID, Region: landRegion, InstanceType: "m7g.medium",
		MatchedAt: time.Now().UTC(), InstanceID: instanceID, ReservationID: reservationID}
	if err := h.store.beginLanding(context.Background(), w.WatchID, instanceID, reservationID, landRegion, m,
		time.Now().Add(landingMarkerTTL)); err != nil {
		t.Fatal(err)
	}
}

func statuses(t *testing.T, h *landHarness, ids ...string) map[string]WatchStatus {
	t.Helper()
	out := map[string]WatchStatus{}
	for _, id := range ids {
		out[id] = h.get(t, id).Status
	}
	return out
}

func countStatus(m map[string]WatchStatus, st WatchStatus) int {
	n := 0
	for _, s := range m {
		if s == st {
			n++
		}
	}
	return n
}

// The slot race: two members find capacity in the same sweep, and only one
// launches. The other waits, active, without calling RunInstances.
func TestGroup_SlotRace_OneLaunchPerSweep(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	h.join(t, memberWatch(t, "w-a", "gpu"))
	h.join(t, memberWatch(t, "w-b", "gpu"))

	s := h.cycle(t)
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v, want exactly one for the group", h.launches)
	}
	if s.GroupWaiting != 1 || s.Landing != 1 {
		t.Errorf("summary group_waiting=%d landing=%d, want 1/1", s.GroupWaiting, s.Landing)
	}
	g := h.group(t, "gpu")
	if g.State != GroupHunting || g.SlotWatch == "" {
		t.Fatalf("group = %+v, want hunting with the slot held", g)
	}
	holder := h.get(t, g.SlotWatch)
	if holder.PendingInstanceID != "i-1" {
		t.Errorf("slot holder %s pending %q, want i-1", g.SlotWatch, holder.PendingInstanceID)
	}

	// Still pending next cycle: still one launch, slot still held.
	h.cycle(t)
	if len(h.launches) != 1 {
		t.Errorf("launches = %v while the slot holder is landing", h.launches)
	}
}

// The conditional write behind the slot: of many claimants exactly one wins,
// and an expired slot can be taken over.
//
// The claims are made one after another, not from goroutines: Substrate does
// not serialize concurrent conditional UpdateItems on one item
// (scttfrdmn/substrate#1449), so a concurrent version is flaky against it
// while real DynamoDB serializes them. The race the poller faces is the same
// condition evaluated against whichever claim landed first, which this pins.
func TestGroup_ReserveSlotIsExclusive(t *testing.T) {
	h := newLandHarness(t)
	ctx := context.Background()
	const n = 8
	for i := 0; i < n; i++ {
		if _, err := h.store.JoinGroup(ctx, "race", "arn:alice", fmt.Sprintf("w-%d", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	won, held := 0, 0
	for i := n - 1; i >= 0; i-- {
		_, err := h.store.ReserveGroupSlot(ctx, "race", fmt.Sprintf("w-%d", i), now)
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrGroupSlotHeld):
			held++
		default:
			t.Errorf("claim %d: %v", i, err)
		}
	}
	if won != 1 || held != n-1 {
		t.Fatalf("won=%d held=%d, want 1/%d", won, held, n-1)
	}
	g := h.group(t, "race")
	// Re-claiming one's own slot succeeds; a sibling is refused until it expires.
	if _, err := h.store.ReserveGroupSlot(ctx, "race", g.SlotWatch, now); err != nil {
		t.Errorf("re-claim own slot: %v", err)
	}
	other := "w-0"
	if g.SlotWatch == other {
		other = "w-1"
	}
	if _, err := h.store.ReserveGroupSlot(ctx, "race", other, now.Add(groupSlotTTL-time.Minute)); !errors.Is(err, ErrGroupSlotHeld) {
		t.Errorf("claim before expiry: %v, want ErrGroupSlotHeld", err)
	}
	if _, err := h.store.ReserveGroupSlot(ctx, "race", other, now.Add(groupSlotTTL+time.Minute)); err != nil {
		t.Errorf("claim after expiry: %v", err)
	}
	if err := h.store.ReleaseGroupSlot(ctx, "race", "w-not-holder"); err != nil {
		t.Errorf("release by a non-holder: %v, want a no-op", err)
	}
}

// The winner lands; every sibling is cancelled with a tombstone and a
// group_cancelled history row, and the group is notified once, on every
// member's channels, instead of the winner's own match notification.
func TestGroup_WinnerCancelsSiblingsWithTombstones(t *testing.T) {
	h := newLandHarness(t)
	n := withGroupNotes(h)
	ids := []string{"w-a", "w-b", "w-c"}
	for _, id := range ids {
		h.join(t, memberWatch(t, id, "gpu"))
	}
	h.cycle(t) // one launches i-1
	winner := h.group(t, "gpu").SlotWatch

	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "c"}, nil }
	s := h.cycle(t)

	st := statuses(t, h, ids...)
	if st[winner] != StatusMatched || countStatus(st, StatusCancelled) != 2 {
		t.Fatalf("statuses = %v, want %s matched and the others cancelled", st, winner)
	}
	if len(h.launches) != 1 {
		t.Errorf("launches = %v, want 1", h.launches)
	}
	g := h.group(t, "gpu")
	if g.State != GroupLanded || g.WinnerWatch != winner || g.WinnerInstanceID != "i-1" || g.SlotWatch != "" {
		t.Errorf("group = %+v, want landed by %s (i-1) with the slot cleared", g, winner)
	}
	want := fmt.Sprintf("group gpu landed by %s (i-1, m7g.medium, %sc)", winner, landRegion)
	for _, id := range ids {
		if id == winner {
			continue
		}
		w := h.get(t, id)
		if w.CancelledReason != want {
			t.Errorf("%s tombstone = %q, want %q", id, w.CancelledReason, want)
		}
		if w.TTLTimestamp < RetentionTTL(time.Now().Add(-time.Hour)) {
			t.Errorf("%s ttl %d is not the retention horizon", id, w.TTLTimestamp)
		}
		hist := h.history(t, id)
		if len(hist) != 1 || hist[0].ActionTaken != ActionGroupCancelled || hist[0].GroupDetail != want {
			t.Errorf("%s history = %+v", id, hist)
		}
	}
	if s.GroupCancelled != 2 || s.Launched != 1 {
		t.Errorf("summary cancelled=%d launched=%d", s.GroupCancelled, s.Launched)
	}
	if len(n.landed) != 1 || len(n.cancelled[0]) != 2 || len(h.notified) != 0 {
		t.Errorf("group notifications = %v, match notifications = %d; want one group notification", n.landed, len(h.notified))
	}
	if len(n.channels[0]) != 3 {
		t.Errorf("group notified on %v, want every member's channel", n.channels[0])
	}

	// Nothing more happens: no member is active.
	h.cycle(t)
	if len(h.launches) != 1 || len(n.landed) != 1 {
		t.Errorf("after: launches=%v notifications=%v", h.launches, n.landed)
	}
}

// A member whose instance dies in pending (#255) is not a win: it frees the
// slot and the group stays armed, and the next landing wins.
func TestGroup_DieInPendingDoesNotWin(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	h.join(t, memberWatch(t, "w-a", "gpu"))
	h.join(t, memberWatch(t, "w-b", "gpu"))
	h.cycle(t)
	first := h.group(t, "gpu").SlotWatch

	h.describe = func(id string) (landingCheck, error) {
		if id == "i-1" {
			return landingCheck{State: "terminated", ReasonCode: "Server.InternalError", ReasonMessage: "internal error"}, nil
		}
		return landingCheck{State: "pending"}, nil
	}
	h.cycle(t)
	g := h.group(t, "gpu")
	if g.State != GroupHunting {
		t.Fatalf("group = %+v after a death in pending, want hunting", g)
	}
	if len(h.launches) != 2 {
		t.Fatalf("launches = %v, want a relaunch after the loss", h.launches)
	}
	second := g.SlotWatch
	if st := statuses(t, h, "w-a", "w-b"); countStatus(st, StatusActive) != 2 {
		t.Fatalf("statuses = %v, want both still hunting", st)
	}

	h.describe = func(id string) (landingCheck, error) {
		if id == "i-2" {
			return landingCheck{State: "running", AZ: landRegion + "c"}, nil
		}
		return landingCheck{State: "terminated"}, nil
	}
	h.cycle(t)
	g = h.group(t, "gpu")
	if g.WinnerInstanceID != "i-2" || g.WinnerWatch != second {
		t.Errorf("group = %+v, want won by %s with i-2 (the first launch, i-1 of %s, died)", g, second, first)
	}
}

// Two members land in the same sweep (both launched, as with an expired slot
// or a pre-#287 poller): exactly one wins, and the other's instance is
// terminated through spawn's terminate path.
func TestGroup_TwoLandInSameSweep_LoserTerminated(t *testing.T) {
	h := newLandHarness(t)
	n := withGroupNotes(h)
	a, b := memberWatch(t, "w-a", "gpu"), memberWatch(t, "w-b", "gpu")
	h.join(t, a)
	h.join(t, b)
	h.beginLandingAs(t, a, "i-a", "")
	h.beginLandingAs(t, b, "i-b", "")
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "c"}, nil }

	h.cycle(t)
	st := statuses(t, h, "w-a", "w-b")
	if countStatus(st, StatusMatched) != 1 || countStatus(st, StatusCancelled) != 1 {
		t.Fatalf("statuses = %v, want one matched and one cancelled", st)
	}
	g := h.group(t, "gpu")
	loser, loserInstance := "w-b", "i-b"
	if g.WinnerWatch == "w-b" {
		loser, loserInstance = "w-a", "i-a"
	}
	if len(h.terminated) != 1 || h.terminated[0] != landRegion+"/"+loserInstance {
		t.Fatalf("terminated = %v, want only the loser %s", h.terminated, loserInstance)
	}
	lw := h.get(t, loser)
	if !strings.Contains(lw.CancelledReason, "group gpu landed by "+g.WinnerWatch) ||
		!strings.Contains(lw.CancelledReason, "instance "+loserInstance+" was terminated") || lw.Landing() {
		t.Errorf("loser tombstone = %q landing=%v", lw.CancelledReason, lw.Landing())
	}
	if len(n.landed) != 1 {
		t.Errorf("group notifications = %v, want 1", n.landed)
	}
	if again := h.cycle(t); len(h.terminated) != 1 || again.GroupExtra+again.GroupCancelled != 0 {
		t.Errorf("a second sweep acted again: terminated=%v summary=%+v", h.terminated, again)
	}
}

// A launch that lands after its group was won is an extra landing: by default
// it is terminated, the member cancelled, and its channels told.
func TestGroup_ExtraLandingTerminated(t *testing.T) {
	h := newLandHarness(t)
	n := withGroupNotes(h)
	a, b := memberWatch(t, "w-a", "gpu"), memberWatch(t, "w-b", "gpu")
	h.join(t, a)
	h.join(t, b)
	h.beginLandingAs(t, a, "i-a", "")
	h.beginLandingAs(t, b, "i-b", "")
	// Only w-a's instance is running; w-b's is still pending, so the win goes to
	// w-a and the sibling cancel gives up w-b's pending instance.
	h.describe = func(id string) (landingCheck, error) {
		if id == "i-a" {
			return landingCheck{State: "running", AZ: landRegion + "c"}, nil
		}
		return landingCheck{State: "pending"}, nil
	}
	h.cycle(t)
	if g := h.group(t, "gpu"); g.WinnerWatch != "w-a" {
		t.Fatalf("group = %+v, want won by w-a", g)
	}
	if len(h.terminated) != 1 || h.terminated[0] != landRegion+"/i-b" {
		t.Errorf("terminated = %v, want w-b's pending i-b", h.terminated)
	}
	if hist := h.history(t, "w-b"); len(hist) != 1 || hist[0].ActionTaken != ActionGroupCancelled || hist[0].InstanceID != "i-b" {
		t.Errorf("w-b history = %+v", hist)
	}

	// The extra-landing path proper: a member landing with its group already won.
	c := memberWatch(t, "w-c", "gpu2")
	d := memberWatch(t, "w-d", "gpu2")
	h.join(t, c)
	h.join(t, d)
	if _, won, err := h.store.CompleteGroup(context.Background(), d, &MatchResult{InstanceID: "i-d", InstanceType: "m7g.medium"}, time.Now()); err != nil || !won {
		t.Fatalf("CompleteGroup: %v %v", won, err)
	}
	h.beginLandingAs(t, c, "i-c", "")
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "a"}, nil }
	s := h.cycle(t)
	cw := h.get(t, "w-c")
	if cw.Status != StatusCancelled || !strings.Contains(cw.CancelledReason, "landed too late and its instance i-c was terminated") {
		t.Errorf("w-c = %s %q", cw.Status, cw.CancelledReason)
	}
	if h.terminated[len(h.terminated)-1] != landRegion+"/i-c" || s.GroupExtra != 1 || len(n.extra) != 1 {
		t.Errorf("terminated=%v extra=%d notices=%d", h.terminated, s.GroupExtra, len(n.extra))
	}
	if hist := h.history(t, "w-c"); len(hist) != 1 || hist[0].ActionTaken != ActionGroupExtra || hist[0].LandedAt.IsZero() {
		t.Errorf("w-c history = %+v", hist)
	}
}

// --group-on-extra keep leaves the extra landing running.
func TestGroup_ExtraLandingKept(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	c, d := memberWatch(t, "w-c", "gpu"), memberWatch(t, "w-d", "gpu")
	c.GroupOnExtra = GroupOnExtraKeep
	h.join(t, c)
	h.join(t, d)
	if _, _, err := h.store.CompleteGroup(context.Background(), d, &MatchResult{InstanceID: "i-d"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.beginLandingAs(t, c, "i-c", "")
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running"}, nil }
	h.cycle(t)
	cw := h.get(t, "w-c")
	if len(h.terminated) != 0 || cw.Status != StatusCancelled || !strings.Contains(cw.CancelledReason, "was kept (--group-on-extra keep)") {
		t.Errorf("terminated=%v w-c=%s %q", h.terminated, cw.Status, cw.CancelledReason)
	}
}

// A hold member's pending reservation is released when a spawn sibling wins.
func TestGroup_HoldSiblingReleased(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStatePending)
	n := withGroupNotes(h)
	hold := holdWatch(t, "w-hold")
	hold.Group = "mix"
	spawn := memberWatch(t, "w-spawn", "mix")
	h.join(t, hold)
	h.join(t, spawn)
	h.beginLandingAs(t, hold, "", "cr-h")
	h.beginLandingAs(t, spawn, "i-s", "")
	f.states["cr-h"] = ec2types.CapacityReservationStatePending
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "b"}, nil }

	h.cycle(t)
	if g := h.group(t, "mix"); g.WinnerWatch != "w-spawn" {
		t.Fatalf("group = %+v, want won by w-spawn", g)
	}
	if len(f.cancelled) != 1 || f.cancelled[0] != "cr-h" {
		t.Errorf("released = %v, want the hold sibling's cr-h", f.cancelled)
	}
	hw := h.get(t, "w-hold")
	if hw.Status != StatusCancelled || hw.Landing() || !strings.Contains(hw.CancelledReason, "capacity reservation cr-h was released") {
		t.Errorf("hold sibling = %s landing=%v %q", hw.Status, hw.Landing(), hw.CancelledReason)
	}
	if len(n.landed) != 1 || n.landed[0] != "w-spawn:w-hold" {
		t.Errorf("group notifications = %v", n.landed)
	}
}

// A hold member that wins: its reservation is the group's, and the spawn
// sibling's pending instance is terminated.
func TestGroup_HoldMemberWins(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStateActive)
	withGroupNotes(h)
	hold := holdWatch(t, "w-hold")
	hold.Group = "mix"
	h.join(t, hold)
	h.join(t, memberWatch(t, "w-spawn", "mix"))
	h.beginLandingAs(t, h.get(t, "w-spawn"), "i-s", "")
	// The spawn member's instance is pending; the hold member reserves and its
	// reservation is active at create, so it wins at once.
	h.cycle(t)
	if g := h.group(t, "mix"); g.WinnerWatch != "w-hold" || g.WinnerReservationID == "" {
		t.Fatalf("group = %+v, want won by w-hold", g)
	}
	if len(f.created) != 1 || len(h.terminated) != 1 || h.terminated[0] != landRegion+"/i-s" {
		t.Errorf("created=%v terminated=%v", f.created, h.terminated)
	}
}

// Convergence: a sibling the winner's cancel missed is cancelled the next
// sweep, with the same tombstone, and never launches.
func TestGroup_MissedSiblingConverges(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	x, y := memberWatch(t, "w-x", "gpu"), memberWatch(t, "w-y", "gpu")
	h.join(t, x)
	h.join(t, y)
	if _, _, err := h.store.CompleteGroup(context.Background(), x, &MatchResult{InstanceID: "i-x", InstanceType: "m7g.medium", AvailabilityZone: "us-east-2a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	s := h.cycle(t)
	yw := h.get(t, "w-y")
	if yw.Status != StatusCancelled || yw.CancelledReason != "group gpu landed by w-x (i-x, m7g.medium, us-east-2a)" {
		t.Errorf("w-y = %s %q", yw.Status, yw.CancelledReason)
	}
	if len(h.launches) != 0 || s.GroupCancelled < 1 {
		t.Errorf("launches=%v cancelled=%d", h.launches, s.GroupCancelled)
	}
}

// A member whose group was cancelled is cancelled too; one whose group is
// gone is cancelled rather than launched unarbitrated.
func TestGroup_CancelledAndMissingGroup(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	h.join(t, memberWatch(t, "w-a", "gpu"))
	if _, ok, err := h.store.CloseGroup(context.Background(), "gpu", "cancelled by its owner"); err != nil || !ok {
		t.Fatalf("CloseGroup: %v %v", ok, err)
	}
	if _, ok, _ := h.store.CloseGroup(context.Background(), "gpu", "again"); ok {
		t.Error("closed an already-cancelled group")
	}
	orphan := memberWatch(t, "w-orphan", "nonexistent")
	h.put(t, orphan)
	h.cycle(t)
	if w := h.get(t, "w-a"); w.Status != StatusCancelled || w.CancelledReason != "group gpu cancelled: cancelled by its owner" {
		t.Errorf("w-a = %s %q", w.Status, w.CancelledReason)
	}
	if w := h.get(t, "w-orphan"); w.Status != StatusCancelled || !strings.Contains(w.CancelledReason, "no longer exists") {
		t.Errorf("w-orphan = %s %q", w.Status, w.CancelledReason)
	}
	if len(h.launches) != 0 {
		t.Errorf("launches = %v", h.launches)
	}
}

// Join: creates the group, refuses another owner's, and re-arms an ended one
// as a new generation; the group keeps the largest member TTL.
func TestGroup_Join(t *testing.T) {
	h := newLandHarness(t)
	ctx := context.Background()
	g, err := h.store.JoinGroup(ctx, "gpu", "arn:alice", "w-1", 100)
	if err != nil || g.State != GroupHunting || g.Owner != "arn:alice" || len(g.Members) != 1 || g.TTLTimestamp != 100 {
		t.Fatalf("create: %+v %v", g, err)
	}
	if g, err = h.store.JoinGroup(ctx, "gpu", "arn:alice", "w-2", 50); err != nil || len(g.Members) != 2 || g.TTLTimestamp != 100 {
		t.Fatalf("join: %+v %v", g, err)
	}
	if _, err := h.store.JoinGroup(ctx, "gpu", "arn:bob", "w-3", 100); !errors.Is(err, ErrGroupNotOwned) {
		t.Errorf("another owner: %v, want ErrGroupNotOwned", err)
	}
	if _, _, err := h.store.CloseGroup(ctx, "gpu", "done"); err != nil {
		t.Fatal(err)
	}
	g, err = h.store.JoinGroup(ctx, "gpu", "arn:alice", "w-4", 100)
	if err != nil || g.State != GroupHunting || g.Generation != 1 || len(g.Members) != 1 || g.Members[0] != "w-4" || g.EndReason != "" {
		t.Errorf("re-arm: %+v %v", g, err)
	}
}

// The record shapes an older poller and CLI see (#287): the group item is in
// neither GSI, so ListActiveWatches (every poller) and ListWatchesByUser
// (list, spawn's dashboard) never return it, and a member's record differs from
// an independent watch's only by the additive, omitempty group attributes.
func TestGroup_OldPollerCompatibleRecordShape(t *testing.T) {
	h := newLandHarness(t)
	ctx := context.Background()
	member := memberWatch(t, "w-m", "gpu")
	member.GroupOnExtra = GroupOnExtraKeep
	h.join(t, member)
	plain := landWatch(t, "w-p")
	h.put(t, plain)

	active, err := h.store.ListActiveWatches(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, w := range active {
		ids = append(ids, w.WatchID)
	}
	sort.Strings(ids)
	if strings.Join(ids, ",") != "w-m,w-p" {
		t.Errorf("active watches = %v, want the two watches and not the group item", ids)
	}
	mine, err := h.store.ListWatchesByUser(ctx, "arn:alice", "")
	if err != nil || len(mine) != 2 {
		t.Errorf("user's watches = %d %v, want 2", len(mine), err)
	}

	raw := func(key string) map[string]types.AttributeValue {
		out, err := h.store.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &h.store.watchesTable,
			Key: map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: key}}})
		if err != nil || out.Item == nil {
			t.Fatalf("GetItem(%s): %v", key, err)
		}
		return out.Item
	}
	gi := raw(GroupKey("gpu"))
	for _, attr := range []string{"status", "user_id"} {
		if _, ok := gi[attr]; ok {
			t.Errorf("group item has %q, which would put it in a GSI older pollers and list read", attr)
		}
	}
	mi, pi := raw("w-m"), raw("w-p")
	for k := range mi {
		if _, ok := pi[k]; !ok && k != "group_name" && k != "group_on_extra" && k != "notify_channels" {
			t.Errorf("member record has %q, which an independent watch does not", k)
		}
	}
	for _, k := range []string{"group_name", "group_on_extra", "cancelled_reason"} {
		if _, ok := pi[k]; ok {
			t.Errorf("independent watch record has %q", k)
		}
	}
	// A pre-#287 reader that knows none of the group fields still reads the
	// member as a complete watch.
	if got := h.get(t, "w-m"); got.Status != StatusActive || got.Action != ActionSpawn || got.InstanceTypePattern != "m7g.medium" {
		t.Errorf("member = %+v", got)
	}
}

func TestGroup_Validation(t *testing.T) {
	ok := &Watch{Group: "h100-hunt", Action: ActionSpawn}
	if err := ValidateGroupMember(ok); err != nil {
		t.Errorf("valid member: %v", err)
	}
	for name, w := range map[string]*Watch{
		"notify":       {Group: "g1", Action: ActionNotify},
		"sagemaker":    {Group: "g1", Action: ActionSpawn, Service: ServiceSageMaker},
		"fleet":        {Group: "g1", Action: ActionSpawn, DesiredCount: 2},
		"until-landed": {Group: "g1", Action: ActionHold, UntilLanded: true},
		"bad name":     {Group: "has space", Action: ActionSpawn},
		"w- prefix":    {Group: "w-1234", Action: ActionSpawn},
		"sl- prefix":   {Group: "sl-x", Action: ActionSpawn},
		"g- prefix":    {Group: "g-x", Action: ActionSpawn},
		"bad extra":    {Group: "g1", Action: ActionSpawn, GroupOnExtra: "maybe"},
		"hold extra":   {Group: "g1", Action: ActionSpawn, GroupOnExtra: "hold"},
		"extra alone":  {Action: ActionSpawn, GroupOnExtra: "keep"},
	} {
		if err := ValidateGroupMember(w); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if groupManaged(&Watch{Group: "g", Action: ActionNotify}) || !groupManaged(&Watch{Group: "g", Action: ActionHold}) {
		t.Error("groupManaged")
	}
}

func TestGroup_Exposure(t *testing.T) {
	members := []*Watch{
		{WatchID: "w-a", Status: StatusActive, Exposure: &Exposure{HourlyUSD: 2}},
		{WatchID: "w-b", Status: StatusActive, Exposure: &Exposure{HourlyUSD: 5}},
		{WatchID: "w-c", Status: StatusCancelled, Exposure: &Exposure{HourlyUSD: 50}},
	}
	ge := ComputeGroupExposure(members)
	if ge.MaxWatch != "w-b" || ge.Max.HourlyUSD != 5 || ge.Note != "" {
		t.Errorf("exposure = %+v", ge)
	}
	members[0].GroupOnExtra = GroupOnExtraKeep
	ge = ComputeGroupExposure(members)
	if ge.KeepHourlyUSD != 7 || !strings.Contains(ge.Note, "w-a") || !strings.Contains(ge.Note, "$7.00/hr") {
		t.Errorf("keep exposure = %+v", ge)
	}
}

func TestGroup_NotificationText(t *testing.T) {
	g := &WatchGroup{Name: "gpu", State: GroupLanded, WinnerWatch: "w-a", WinnerInstanceID: "i-1", WinnerType: "g6e.xlarge", WinnerAZ: "us-east-2b"}
	w := &Watch{WatchID: "w-a", InstanceTypePattern: "g6e.xlarge", Action: ActionSpawn}
	m := &MatchResult{InstanceType: "g6e.xlarge", InstanceID: "i-1", Region: "us-east-2", AvailabilityZone: "us-east-2b", ActionTaken: "spawned", MatchedAt: time.Now()}
	subj, body := groupLandedSubjectBody(w, g, m, []string{"w-b", "w-c"})
	if subj != "[lagotto] group gpu landed: g6e.xlarge i-1 in us-east-2b" || !strings.Contains(body, "Cancelled siblings: w-b, w-c") ||
		!strings.Contains(body, "Instance ID: i-1") {
		t.Errorf("subject %q body %q", subj, body)
	}
	p := groupLandedPayload(w, g, m, nil)
	if p["event"] != EventGroupLanded || p["group"] != "gpu" || fmt.Sprint(p["cancelled"]) != "[]" {
		t.Errorf("payload %v", p)
	}
	x := &Watch{WatchID: "w-b"}
	xm := &MatchResult{InstanceID: "i-2", InstanceType: "g7e.2xlarge", Region: "us-west-2", GroupDetail: "detail"}
	subj, body = groupExtraSubjectBody(x, g, xm)
	if !strings.Contains(subj, "extra landing i-2 for watch w-b") || !strings.Contains(body, "--group-on-extra terminate") {
		t.Errorf("extra subject %q body %q", subj, body)
	}
	if got := unionChannels([]*Watch{
		{NotifyChannels: []NotifyChannel{{Type: "email", Target: "A@x"}}},
		{NotifyChannels: []NotifyChannel{{Type: "email", Target: "a@x"}, {Type: "sns", Target: "arn"}}},
	}); len(got) != 2 {
		t.Errorf("union = %v", got)
	}
	_ = aws.String
}
