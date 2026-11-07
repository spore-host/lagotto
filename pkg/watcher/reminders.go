package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// Reminders for until-landed watches (#239 phase 4).
//
// An until-landed watch pauses at its acknowledgement deadline (ExpiresAt)
// unless someone runs `lagotto ack`. Reminders are how its owner hears that
// the deadline is coming. They ride on the poller's 5-minute sweep: no new
// schedule, table, topic or permission. A reminder is an UpdateItem on the
// watch (already granted) and a publish/POST to the watch's own channels
// (already done for every other notification).
//
// The schedule of one acknowledgement window, all derived from the stored
// fields (never from this poller's defaults):
//
//	reminder k (k = 1..RemindPauseAfter)  at AckWindowStart() + k × RemindEvery
//	final warning                          at ExpiresAt − 24h
//	pause                                  at ExpiresAt
//
// With the defaults (--remind 7d, pause after 2) that is day 7, day 14, day 20
// and the pause at day 21. When RemindEvery is 24h or less, the last reminder is
// already within 24h of the pause, so it IS the final warning and no separate
// one is sent.
//
// At most once per (window, stage), across any number of concurrent pollers:
// the "reminders sent" marker (ReminderWindow, RemindersSent) is claimed with a
// conditional UpdateItem BEFORE anything is sent, and only the poller whose
// claim succeeds sends. A send that then fails is not retried: a lost reminder
// can only cause a pause, which is the fail-safe outcome, while a retry loop
// could spam. The marker is keyed by the window's deadline, so an ack (which
// moves ExpiresAt) resets it with no write of its own.
//
// A poller that was down (or not yet upgraded) when a stage fell due catches up
// with ONE message, the latest stage due, rather than every missed one.

// MinReminderPollerVersion is the first lagotto release whose poller sends
// until-landed reminders. The CLI compares the deployed poller's Version tag
// (the one `lagotto doctor` reads) against it, and warns at `watch
// --until-landed`, `ack` and `status` when the poller is older: such a poller
// pauses the watch at its deadline without ever reminding anyone.
//
// It must name the release this feature ships in; a test in the cmd package
// holds it to the CHANGELOG (newer than every released version while the entry
// is unreleased, equal to the release that carries it once it is).
const MinReminderPollerVersion = "0.68.0"

// finalWarningLead is how long before the pause the final warning is due.
const finalWarningLead = 24 * time.Hour

// ReminderStage is one reminder of an acknowledgement window.
type ReminderStage struct {
	// Stage numbers the window's reminders in the order they fall due, from 1.
	// It is what the marker records.
	Stage int
	// Of is how many reminders the window has (the last one is the final
	// warning).
	Of int
	// Final marks the last reminder before the pause.
	Final bool
	// Due is when the stage falls due.
	Due time.Time
}

// ReminderSchedule lists the reminders of the watch's current acknowledgement
// window in the order they fall due. It is empty for a watch that is not
// until-landed, and for one whose window is too short to hold any (no
// pause-after reminders and an interval of 24h or less).
func (w *Watch) ReminderSchedule() []ReminderStage {
	start := w.AckWindowStart()
	if start.IsZero() {
		return nil
	}
	every := time.Duration(w.RemindEverySeconds) * time.Second
	var out []ReminderStage
	for k := 1; k <= w.RemindPauseAfter; k++ {
		out = append(out, ReminderStage{Stage: k, Due: start.Add(time.Duration(k) * every)})
	}
	switch {
	case every > finalWarningLead:
		// The last reminder is a whole interval before the pause: warn again
		// 24h before it.
		out = append(out, ReminderStage{Stage: len(out) + 1, Due: w.ExpiresAt.Add(-finalWarningLead)})
	case len(out) == 0:
		return nil
	}
	for i := range out {
		out[i].Of = len(out)
	}
	out[len(out)-1].Final = true
	return out
}

// remindersSentInWindow is how many stages of the current window the marker
// says were sent: zero when the marker belongs to an earlier window.
func (w *Watch) remindersSentInWindow() int {
	if w.ReminderWindow != reminderWindowKey(w) {
		return 0
	}
	return w.RemindersSent
}

// reminderWindowKey identifies the current acknowledgement window: its
// deadline, in Unix seconds. Acks only ever move the deadline forward, so keys
// only grow.
func reminderWindowKey(w *Watch) int64 {
	return w.ExpiresAt.Unix()
}

// DueReminder is the reminder to send at now, if any: the latest stage of the
// current window that has fallen due and has not been sent. ok is false when
// nothing is due, everything due was sent, or the deadline has passed (the
// pause notification takes over then).
func (w *Watch) DueReminder(now time.Time) (ReminderStage, bool) {
	if !w.UntilLanded || w.Status != StatusActive || !now.Before(w.ExpiresAt) {
		return ReminderStage{}, false
	}
	var due ReminderStage
	found := false
	for _, r := range w.ReminderSchedule() {
		if r.Due.After(now) {
			break
		}
		due, found = r, true
	}
	if !found || due.Stage <= w.remindersSentInWindow() {
		return ReminderStage{}, false
	}
	return due, true
}

// ClaimReminder records that stage of window (a reminderWindowKey) is being
// sent, and reports whether this caller won the claim. It is one conditional
// UpdateItem: it lands only while the watch is active and the marker is for
// an earlier window, or for this window at a lower stage. So of any number of
// pollers that see the same stage due, exactly one claims it, and a poller
// holding a snapshot from before an ack can never move the marker back to the
// old window. It writes nothing else: not updated_at (which `lagotto update`
// uses as its version), not the TTL.
func (s *Store) ClaimReminder(ctx context.Context, watchID string, window int64, stage int) (bool, error) {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		ConditionExpression: aws.String("#st = :active AND (attribute_not_exists(reminder_window) OR reminder_window < :win" +
			" OR (reminder_window = :win AND (attribute_not_exists(reminders_sent) OR reminders_sent < :k)))"),
		UpdateExpression:         aws.String("SET reminder_window = :win, reminders_sent = :k"),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":active": &types.AttributeValueMemberS{Value: string(StatusActive)},
			":win":    &types.AttributeValueMemberN{Value: strconv.FormatInt(window, 10)},
			":k":      &types.AttributeValueMemberN{Value: strconv.Itoa(stage)},
		},
	})
	if err != nil {
		var cf *types.ConditionalCheckFailedException
		if errors.As(err, &cf) {
			return false, nil
		}
		return false, fmt.Errorf("claim reminder %d for watch %s: %w", stage, watchID, err)
	}
	return true, nil
}

// remindIfDue sends w's due reminder, if it has one, after claiming it. It is
// best-effort throughout: nothing here may stop the sweep.
func (p *Poller) remindIfDue(ctx context.Context, w *Watch, now time.Time, summary *PollSummary) {
	r, ok := w.DueReminder(now)
	if !ok || len(w.NotifyChannels) == 0 {
		return
	}
	notify := p.reminderNotifier()
	if notify == nil {
		// Leave the stage unclaimed for a poller that can deliver it.
		return
	}
	window := reminderWindowKey(w)
	won, err := p.store.ClaimReminder(ctx, w.WatchID, window, r.Stage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record reminder %d for watch %s: %v\n", r.Stage, w.WatchID, err)
		return
	}
	if !won {
		return // another poller sent it, or the watch changed under us
	}
	// Mirror the claim onto the in-memory copy, so a later whole-item write in
	// this sweep (RecordMatch) keeps it.
	w.ReminderWindow, w.RemindersSent = window, r.Stage
	summary.Reminded++
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s: reminder %d/%d sent (pauses %s)\n", w.WatchID, r.Stage, r.Of, w.ExpiresAt.Format(time.RFC3339))
	}
	// Cost exposure (#239 phase 5), priced once for every channel. Derived,
	// so nothing about it is written back.
	w.Exposure = p.pollerExposure(ctx, w)
	if err := notify(ctx, w, r, now); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to send reminder %d for watch %s: %v\n", r.Stage, w.WatchID, err)
	}
}

// reminderNotifier resolves where a reminder goes: the test seam if set, else
// the wired Notifier, else nowhere.
func (p *Poller) reminderNotifier() func(ctx context.Context, w *Watch, r ReminderStage, now time.Time) error {
	if p.notifyReminder != nil {
		return p.notifyReminder
	}
	if p.notifier != nil {
		return p.notifier.NotifyReminder
	}
	return nil
}

// NotifyReminder sends an until-landed reminder (event "reminder") to every
// channel on the watch: email, webhook and sns, with the same webhook
// re-validation as Notify. now is the sweep's time, for "pauses in".
func (n *Notifier) NotifyReminder(ctx context.Context, w *Watch, r ReminderStage, now time.Time) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}
	subject := reminderSubject(w, r)
	body := reminderBody(w, r, now)
	payload, err := json.Marshal(reminderPayload(w, r, now))
	if err != nil {
		return fmt.Errorf("marshal reminder payload: %w", err)
	}

	var lastErr error
	for _, ch := range w.NotifyChannels {
		var err error
		switch ch.Type {
		case "email":
			err = n.publishEmail(ctx, ch.Target, subject, body)
		case "webhook":
			if verr := ValidateWebhookURL(ch.Target); verr != nil {
				err = fmt.Errorf("blocked unsafe webhook URL: %w", verr)
			} else {
				err = n.postWebhookJSON(ch.Target, payload)
			}
		case "sns":
			_, err = n.snsClient.Publish(ctx, &sns.PublishInput{
				TopicArn: aws.String(ch.Target),
				Message:  aws.String(string(payload)),
			}, InARNRegion(ch.Target))
		default:
			continue
		}
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// reminderSubject renders the reminder subject line. Pure, for tests.
func reminderSubject(w *Watch, r ReminderStage) string {
	if r.Final {
		return fmt.Sprintf("[lagotto] FINAL reminder: watch %s pauses %s unless acknowledged", w.WatchID, w.ExpiresAt.Format(time.RFC3339))
	}
	return fmt.Sprintf("[lagotto] reminder: watch %s is still hunting %s; acknowledge by %s", w.WatchID, w.InstanceTypePattern, w.ExpiresAt.Format(time.RFC3339))
}

// reminderBody renders the reminder body: what is being hunted, how the hunt
// is going (the heartbeat), the deadline, and the one command that matters.
func reminderBody(w *Watch, r ReminderStage, now time.Time) string {
	var b strings.Builder
	left := w.ExpiresAt.Sub(now)
	if r.Final {
		fmt.Fprintf(&b, "FINAL REMINDER. Your until-landed watch %s pauses at %s (in %s)\nunless you acknowledge it:\n", w.WatchID, w.ExpiresAt.Format(time.RFC3339), roundSpan(left))
	} else {
		fmt.Fprintf(&b, "Your until-landed watch %s is still hunting and has not landed. It pauses\nat %s (in %s) unless you acknowledge it:\n", w.WatchID, w.ExpiresAt.Format(time.RFC3339), roundSpan(left))
	}
	fmt.Fprintf(&b, "\n  lagotto ack %s\n\n", w.WatchID)
	fmt.Fprintf(&b, "Each ack moves the deadline %s ahead. This is reminder %d of %d in this window.\n\n", FormatSpan(w.AckWindow()), r.Stage, r.Of)

	fmt.Fprintf(&b, "Pattern:   %s\n", w.InstanceTypePattern)
	fmt.Fprintf(&b, "Regions:   %v\n", w.Regions)
	fmt.Fprintf(&b, "Spot:      %v\n", w.Spot)
	fmt.Fprintf(&b, "Action:    %s\n", w.Action)
	if w.MaxPrice > 0 {
		fmt.Fprintf(&b, "Max price: $%.4f/hr (the --max-price cap you set, not a price quote)\n", w.MaxPrice)
	}
	for i, line := range reminderExposure(w).Lines() {
		label := "Exposure:  "
		if i > 0 {
			label = "           "
		}
		fmt.Fprintf(&b, "%s%s\n", label, line)
	}
	fmt.Fprintf(&b, "Created:   %s\n", w.CreatedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "Deadline:  %s\n", w.ExpiresAt.Format(time.RFC3339))

	b.WriteString("\nHow the hunt is going:\n")
	if w.LastPolledAt.IsZero() {
		b.WriteString("  Last poll:    never\n")
	} else {
		fmt.Fprintf(&b, "  Last poll:    %s (%s ago)\n", w.LastPolledAt.Format(time.RFC3339), roundSpan(now.Sub(w.LastPolledAt)))
	}
	attempts := fmt.Sprintf("%d", w.Attempts)
	if !w.LastAttemptAt.IsZero() {
		attempts += fmt.Sprintf(" (last %s, %s ago)", w.LastAttemptAt.Format(time.RFC3339), roundSpan(now.Sub(w.LastAttemptAt)))
	}
	fmt.Fprintf(&b, "  Attempts:     %s\n", attempts)
	if w.LastError != "" {
		fmt.Fprintf(&b, "  Last error:   %s\n", w.LastError)
	}
	if w.LastPollError != "" {
		fmt.Fprintf(&b, "  Search error: %s\n", w.LastPollError)
	}

	fmt.Fprintf(&b, `
If you do nothing, the watch pauses at its deadline: nothing more is launched,
and 'lagotto ack %s' resumes it. If you no longer need it:

  lagotto cancel %s
`, w.WatchID, w.WatchID)
	return b.String()
}

// reminderPayload is the machine-readable reminder event, for webhook and sns
// channels.
func reminderPayload(w *Watch, r ReminderStage, now time.Time) map[string]interface{} {
	p := map[string]interface{}{
		"watch_id":             w.WatchID,
		"event":                "reminder",
		"pattern":              w.InstanceTypePattern,
		"regions":              w.Regions,
		"action":               string(w.Action),
		"until_landed":         true,
		"reminder":             r.Stage,
		"reminders_in_window":  r.Of,
		"final":                r.Final,
		"deadline":             w.ExpiresAt.UTC().Format(time.RFC3339),
		"seconds_until_pause":  int64(w.ExpiresAt.Sub(now) / time.Second),
		"ack_command":          "lagotto ack " + w.WatchID,
		"ack_window_seconds":   int64(w.AckWindow() / time.Second),
		"attempts":             w.Attempts,
		"remind_every_seconds": w.RemindEverySeconds,
	}
	if !w.LastPolledAt.IsZero() {
		p["last_polled_at"] = w.LastPolledAt.UTC().Format(time.RFC3339)
	}
	if !w.LastAttemptAt.IsZero() {
		p["last_attempt_at"] = w.LastAttemptAt.UTC().Format(time.RFC3339)
	}
	if w.LastError != "" {
		p["last_error"] = w.LastError
	}
	if w.LastPollError != "" {
		p["last_poll_error"] = w.LastPollError
	}
	if w.MaxPrice > 0 {
		p["max_price"] = w.MaxPrice
	}
	p["exposure"] = reminderExposure(w)
	return p
}

// ReminderStaticPriceReason labels the static rates of a reminder rendered
// with no live price source (remindIfDue sets one; this is the fallback).
const ReminderStaticPriceReason = "this reminder was rendered without a live AWS Price List client"

// reminderExposure is the watch's cost exposure for a reminder: the one
// remindIfDue priced (live where the poller can), else the static list.
func reminderExposure(w *Watch) *Exposure {
	if w.Exposure == nil {
		w.Exposure = StaticExposure(w, ReminderStaticPriceReason)
	}
	return w.Exposure
}

// pollerExposure prices w's exposure for a reminder: through truffle's live
// client when the poller has one (its runtime policy already allows
// pricing:GetProducts; truffle falls back to the static list, labelled, when
// the live API fails), else from the static list.
func (p *Poller) pollerExposure(ctx context.Context, w *Watch) *Exposure {
	if c, ok := p.truffle.(*truffleaws.Client); ok && c == nil {
		return StaticExposure(w, PollerNoLivePriceReason)
	}
	if lp, ok := p.truffle.(OnDemandPricer); ok {
		pctx, cancel := context.WithTimeout(ctx, exposurePriceTimeout)
		defer cancel()
		return ComputeExposure(pctx, w, lp)
	}
	return StaticExposure(w, PollerNoLivePriceReason)
}

// PollerNoLivePriceReason labels a reminder's static rates when the poller has
// no live Price List client.
const PollerNoLivePriceReason = "this poller has no live AWS Price List client"

// exposurePriceTimeout bounds the live price lookups of one reminder; a slow
// or throttled Price List degrades to truffle's labelled static fallback or
// "unknown", never a stalled sweep.
const exposurePriceTimeout = 15 * time.Second

// roundSpan renders a duration to the minute ("13d23h", "4h5m", "0m"), for
// reminder text.
func roundSpan(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Minute)
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	h := d / time.Hour
	m := (d - h*time.Hour) / time.Minute
	switch {
	case days > 0:
		return fmt.Sprintf("%dd%dh", days, h)
	case h > 0:
		return fmt.Sprintf("%dh%dm", h, m)
	default:
		return fmt.Sprintf("%dm", m)
	}
}
