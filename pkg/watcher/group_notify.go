package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Watch-group notifications (#287). Both are fanned out like the other
// sibling events (fanOut): email, webhook (re-validated) and the user's SNS
// topic. Their payloads carry an "event" key, new to these payloads only.

// Webhook/SNS event names.
const (
	EventGroupLanded = "group_landed"
	EventGroupExtra  = "group_extra"
)

// NotifyGroupLanded is the group's one notification: member w won, the
// siblings in cancelled were cancelled. w's channels are every member's
// (the poller passes their union). It replaces the winner's own match
// notification, so a group that lands sends one message, not one per member.
func (n *Notifier) NotifyGroupLanded(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}
	payload, err := json.Marshal(groupLandedPayload(w, g, m, cancelled))
	if err != nil {
		return fmt.Errorf("marshal group payload: %w", err)
	}
	subject, body := groupLandedSubjectBody(w, g, m, cancelled)
	return n.fanOut(ctx, w, subject, body, payload)
}

// groupLandedSubjectBody renders the email. Pure, for tests.
func groupLandedSubjectBody(w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) (string, string) {
	_, match := matchEmailSubjectBody(w, m)
	subject := fmt.Sprintf("[lagotto] group %s landed: %s %s in %s", g.Name, m.InstanceType,
		m.InstanceID+m.ReservationID, firstNonEmpty(m.AvailabilityZone, m.Region))
	sib := "none (no other member was still hunting)"
	if len(cancelled) > 0 {
		sib = strings.Join(cancelled, ", ")
	}
	body := fmt.Sprintf(`Watch group %s is complete: watch %s landed first.

Cancelled siblings: %s

`, g.Name, w.WatchID, sib) + match
	return subject, body
}

func groupLandedPayload(w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) map[string]interface{} {
	payload := matchWebhookPayload(w, m)
	payload["event"] = EventGroupLanded
	payload["group"] = g.Name
	if !m.LandedAt.IsZero() {
		payload["landed_at"] = m.LandedAt.Format(time.RFC3339)
	}
	if cancelled == nil {
		cancelled = []string{}
	}
	payload["cancelled"] = cancelled
	return payload
}

// NotifyGroupExtra tells member w's channels that its launch or hold landed
// after its group was already won, what lagotto did with it (m.GroupDetail),
// and that the watch is cancelled.
func (n *Notifier) NotifyGroupExtra(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}
	payload, err := json.Marshal(groupExtraPayload(w, g, m))
	if err != nil {
		return fmt.Errorf("marshal extra-landing payload: %w", err)
	}
	subject, body := groupExtraSubjectBody(w, g, m)
	return n.fanOut(ctx, w, subject, body, payload)
}

// groupExtraSubjectBody renders the email. Pure, for tests.
func groupExtraSubjectBody(w *Watch, g *WatchGroup, m *MatchResult) (string, string) {
	subject := fmt.Sprintf("[lagotto] group %s: extra landing %s for watch %s", g.Name, m.InstanceID+m.ReservationID, w.WatchID)
	body := fmt.Sprintf(`Watch %s's %s landed after its group %s was already complete.

%s

Instance Type:     %s
Region:            %s
Availability Zone: %s
Launched At:       %s

Watch %s is cancelled (--group-on-extra %s).
`, w.WatchID, lostWhat(m), g.Name, m.GroupDetail,
		m.InstanceType, m.Region, dashIfEmpty(m.AvailabilityZone), m.MatchedAt.Format(time.RFC3339),
		w.WatchID, w.GroupOnExtraPolicy())
	return subject, body
}

func groupExtraPayload(w *Watch, g *WatchGroup, m *MatchResult) map[string]interface{} {
	payload := matchWebhookPayload(w, m)
	payload["event"] = EventGroupExtra
	payload["group"] = g.Name
	payload["winner_watch_id"] = g.WinnerWatch
	payload["on_extra"] = w.GroupOnExtraPolicy()
	payload["detail"] = m.GroupDetail
	payload["status"] = string(StatusCancelled)
	return payload
}
