package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// The poller's half of watch groups (#287); the protocol is described in
// group.go.

// sweepGroup returns the group, read once per sweep (nil when it does not
// exist). The cache only spares reads: every decision that matters is a
// conditional write on the group item, so a stale entry cannot double-launch.
func (p *Poller) sweepGroup(ctx context.Context, name string) (*WatchGroup, error) {
	if g, ok := p.groups[name]; ok {
		return g, nil
	}
	g, err := p.store.GetGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	p.cacheGroup(name, g)
	return g, nil
}

func (p *Poller) cacheGroup(name string, g *WatchGroup) {
	if p.groups == nil {
		p.groups = map[string]*WatchGroup{}
	}
	p.groups[name] = g
}

// groupEnded is the per-sweep check for a hunting group member: when its group
// has landed, been cancelled or no longer exists, the member is cancelled with
// a tombstone (the convergence half of the protocol) and true is returned. An
// unreadable group is not a reason to cancel anything: the slot claim, a
// conditional write, still keeps the member from launching into an ended
// group.
func (p *Poller) groupEnded(ctx context.Context, w *Watch, summary *PollSummary) bool {
	g, err := p.sweepGroup(ctx, w.Group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not read group %s: %v\n", w.WatchID, w.Group, err)
		return false
	}
	if g == nil {
		p.cancelMember(ctx, w, fmt.Sprintf("group %s no longer exists", w.Group), nil, summary)
		return true
	}
	if g.State == GroupHunting || g.WinnerWatch == w.WatchID {
		return false
	}
	p.cancelMember(ctx, w, g.CancelReason(), g, summary)
	return true
}

// claimGroupSlot claims the group's slot before the member's launch or hold.
// false means launch nothing this cycle: a sibling holds the slot, or the group
// ended (the member is then cancelled), or the claim could not be made.
func (p *Poller) claimGroupSlot(ctx context.Context, w *Watch, summary *PollSummary) bool {
	g, err := p.store.ReserveGroupSlot(ctx, w.Group, w.WatchID, p.clock())
	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrGroupSlotHeld):
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: group %s: %v; launching nothing this cycle\n", w.WatchID, w.Group, err)
		}
		summary.Retrying++
		summary.GroupWaiting++
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
	case errors.Is(err, ErrGroupClosed):
		p.cacheGroup(w.Group, g)
		if g.WinnerWatch != w.WatchID {
			p.cancelMember(ctx, w, g.CancelReason(), g, summary)
		}
	case errors.Is(err, ErrGroupMissing):
		p.cancelMember(ctx, w, fmt.Sprintf("group %s no longer exists", w.Group), nil, summary)
	default:
		fmt.Fprintf(os.Stderr, "Warning: watch %s: %v; launching nothing this cycle\n", w.WatchID, err)
	}
	return false
}

// releaseGroupSlot frees the slot if w holds it (best-effort: it expires on
// its own after groupSlotTTL).
func (p *Poller) releaseGroupSlot(ctx context.Context, w *Watch) {
	if !groupManaged(w) {
		return
	}
	if err := p.store.ReleaseGroupSlot(ctx, w.Group, w.WatchID); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: %v (it expires on its own within %s)\n", w.WatchID, err, groupSlotTTL)
	}
}

// cancelMember cancels a member whose group ended, with its tombstone and a
// group_cancelled history row. g may be nil (the group is gone).
func (p *Poller) cancelMember(ctx context.Context, w *Watch, reason string, _ *WatchGroup, summary *PollSummary) bool {
	ok, err := p.store.CancelGroupMember(ctx, w.WatchID, reason)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not cancel it for its group: %v\n", w.WatchID, err)
		return false
	}
	if !ok {
		return false
	}
	now := p.clock()
	w.Status, w.CancelledReason, w.UpdatedAt = StatusCancelled, reason, now
	w.clearLanding()
	fmt.Fprintf(os.Stderr, "Watch %s: cancelled: %s\n", w.WatchID, reason)
	m := &MatchResult{
		WatchID: w.WatchID, UserID: w.UserID, Region: firstRegion(w.Regions), InstanceType: w.InstanceTypePattern,
		IsSpot: w.Spot, MatchedAt: now, ActionTaken: ActionGroupCancelled, GroupDetail: reason,
	}
	if err := p.store.RecordHistory(ctx, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record group cancellation for %s: %v\n", w.WatchID, err)
	}
	summary.GroupCancelled++
	return true
}

// winGroup decides a member's landing (m) for its group. It returns won true
// when this member won (or had already won: a retried landing). When a sibling
// won first it handles the extra landing (handleExtraLanding) and returns won
// false. ok false means the group could not be written: the caller must leave
// the member as it is and retry next cycle.
func (p *Poller) winGroup(ctx context.Context, w *Watch, m *MatchResult, now time.Time, summary *PollSummary) (won, ok bool) {
	g, won, err := p.store.CompleteGroup(ctx, w, m, now)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not record its landing for group %s: %v (will retry)\n", w.WatchID, w.Group, err)
		return false, false
	}
	p.cacheGroup(w.Group, g)
	if won || g.WinnerWatch == w.WatchID {
		fmt.Fprintf(os.Stderr, "Watch %s: won group %s with %s\n", w.WatchID, w.Group, m.InstanceID+m.ReservationID)
		return true, true
	}
	p.handleExtraLanding(ctx, w, m, g, now, summary)
	return false, true
}

// giveUpResource terminates a launched instance (spawn's terminate path, gated
// on spawn:managed=true) or releases a capacity reservation.
func (p *Poller) giveUpResource(ctx context.Context, region, instanceID, reservationID string) error {
	if instanceID != "" {
		if p.spawner == nil || p.spawner.terminateInstance == nil {
			return errors.New("this poller cannot terminate instances")
		}
		return p.spawner.terminateInstance(ctx, region, instanceID)
	}
	if p.holder == nil {
		return errors.New("this poller cannot release capacity reservations")
	}
	_, err := p.holder.Release(ctx, region, reservationID)
	return err
}

// resourceFate applies a member's --group-on-extra policy to a resource it
// launched or reserved that is not (or will not be) the group's, and says what
// happened, for the tombstone and the notification.
func (p *Poller) resourceFate(ctx context.Context, w *Watch, region, instanceID, reservationID string) string {
	id := instanceID + reservationID
	if w.GroupOnExtraPolicy() == GroupOnExtraKeep {
		if instanceID != "" {
			return fmt.Sprintf("instance %s was kept (--group-on-extra keep); its TTL still applies", id)
		}
		return fmt.Sprintf("capacity reservation %s was kept (--group-on-extra keep); it bills until it is released or expires", id)
	}
	err := p.giveUpResource(ctx, region, instanceID, reservationID)
	switch {
	case err == nil && instanceID != "":
		return fmt.Sprintf("instance %s was terminated", id)
	case err == nil:
		return fmt.Sprintf("capacity reservation %s was released", id)
	case instanceID != "":
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not terminate %s: %v\n", w.WatchID, id, err)
		if isAccessDenied(err) {
			fmt.Fprintf(os.Stderr, "  The poller's runtime role cannot terminate it; redeploy the poller (lagotto deploy).\n")
		}
		return fmt.Sprintf("instance %s could not be terminated (%s); terminate it with: spawn terminate %s", id, oneLine(err.Error()), id)
	default:
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not release %s: %v\n", w.WatchID, id, err)
		return fmt.Sprintf("capacity reservation %s could not be released (%s); release it with: lagotto cancel %s", id, oneLine(err.Error()), w.WatchID)
	}
}

// handleExtraLanding deals with a member whose launch or hold landed after its
// group was already won (or cancelled): by --group-on-extra it terminates the
// instance or releases the reservation (terminate, the default) or leaves it
// (keep), then cancels the member with its tombstone, writes a group_extra
// history row and notifies the member's channels.
func (p *Poller) handleExtraLanding(ctx context.Context, w *Watch, m *MatchResult, g *WatchGroup, now time.Time, summary *PollSummary) {
	fate := p.resourceFate(ctx, w, m.Region, m.InstanceID, m.ReservationID)
	reason := fmt.Sprintf("%s; this watch's launch landed too late and its %s", g.CancelReason(), fate)
	fmt.Fprintf(os.Stderr, "Watch %s: extra landing: %s\n", w.WatchID, reason)

	row := *m
	row.ActionTaken, row.GroupDetail = ActionGroupExtra, reason
	if row.LandedAt.IsZero() {
		row.LandedAt = now
	}
	if err := p.store.RecordHistory(ctx, &row); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record extra landing for %s: %v\n", w.WatchID, err)
	}
	if _, err := p.store.CancelGroupMember(ctx, w.WatchID, reason); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not cancel it after its extra landing: %v\n", w.WatchID, err)
	}
	w.clearLanding()
	w.Status, w.CancelledReason, w.UpdatedAt = StatusCancelled, reason, now
	summary.GroupExtra++
	if notify := p.groupExtraNotifier(); notify != nil {
		if err := notify(ctx, w, g, &row); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: extra-landing notification failed for %s: %v\n", w.WatchID, err)
		}
	}
}

// finishGroup runs after a member won its group and its match was recorded:
// every other active member is cancelled with a tombstone (its pending
// launch or hold given up per its --group-on-extra policy), and the group is
// notified once, on every member's channels.
func (p *Poller) finishGroup(ctx context.Context, winner *Watch, m *MatchResult, g *WatchGroup, now time.Time, summary *PollSummary) {
	if g == nil || len(g.Members) == 0 {
		// The win is recorded but the member list could not be read; re-read.
		if rg, err := p.store.GetGroup(ctx, winner.Group); err == nil && rg != nil {
			g = rg
		}
	}
	var cancelled []*Watch
	if g != nil {
		for _, id := range g.Members {
			if id == winner.WatchID {
				continue
			}
			sw, err := p.store.GetWatch(ctx, id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: group %s: could not read member %s: %v (the next sweep cancels it)\n", winner.Group, id, err)
				continue
			}
			if sw == nil || sw.Status != StatusActive {
				continue
			}
			reason := g.CancelReason()
			if sw.Landing() {
				reason += "; its " + p.resourceFate(ctx, sw, sw.PendingRegion, sw.PendingInstanceID, sw.PendingReservationID)
			}
			if p.cancelMember(ctx, sw, reason, g, summary) {
				cancelled = append(cancelled, sw)
			}
		}
	}
	notify := p.groupLandedNotifier()
	if notify == nil {
		return
	}
	// One notification for the group, on every member's channels.
	all := *winner
	all.NotifyChannels = unionChannels(append([]*Watch{winner}, cancelled...))
	ids := make([]string, len(cancelled))
	for i, c := range cancelled {
		ids[i] = c.WatchID
	}
	if g == nil {
		g = &WatchGroup{Name: winner.Group, State: GroupLanded, WinnerWatch: winner.WatchID}
	}
	if err := notify(ctx, &all, g, m, ids); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: group %s notification failed: %v\n", winner.Group, err)
	}
}

// unionChannels is every distinct channel of the watches, in order.
func unionChannels(ws []*Watch) []NotifyChannel {
	seen := map[string]bool{}
	var out []NotifyChannel
	for _, w := range ws {
		for _, ch := range w.NotifyChannels {
			k := ch.Type + "\x00" + strings.ToLower(ch.Target)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, ch)
		}
	}
	return out
}

// groupLandedNotifier resolves where the group-landed notification goes: the
// test seam if set, else the wired Notifier, else nowhere.
func (p *Poller) groupLandedNotifier() func(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) error {
	if p.notifyGroupLanded != nil {
		return p.notifyGroupLanded
	}
	if p.notifier != nil {
		return p.notifier.NotifyGroupLanded
	}
	return nil
}

// groupExtraNotifier resolves where an extra-landing notification goes.
func (p *Poller) groupExtraNotifier() func(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult) error {
	if p.notifyGroupExtra != nil {
		return p.notifyGroupExtra
	}
	if p.notifier != nil {
		return p.notifier.NotifyGroupExtra
	}
	return nil
}

// forceLanding records a launch or hold that is already running (active) as
// landing, so the next cycle's landing check decides it for its group. Used
// when the group could not be written at launch time: a match nobody
// arbitrated could be a second landing. false when it could not be recorded
// either (the caller then records the match).
func (p *Poller) forceLanding(ctx context.Context, w *Watch, m *MatchResult, now time.Time, summary *PollSummary) bool {
	instanceID, reservationID := m.InstanceID, ""
	if m.ActionTaken == "held" {
		instanceID, reservationID = "", m.ReservationID
	}
	id := instanceID + reservationID
	if id == "" {
		return false
	}
	markerExp := now.Add(landingMarkerTTL)
	if err := p.store.beginLanding(ctx, w.WatchID, instanceID, reservationID, m.Region, m, markerExp); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not record %s as landing either (%v); recording the match\n", w.WatchID, id, err)
		return false
	}
	w.PendingInstanceID, w.PendingReservationID, w.PendingRegion = instanceID, reservationID, m.Region
	since := now
	w.PendingSince = &since
	w.PendingMatch = m
	w.LeaseOwner, w.LeaseExpiresAt = landingMarker(id), markerExp
	w.noteAttempt(&attempt{}, now)
	summary.Landing++
	return true
}
