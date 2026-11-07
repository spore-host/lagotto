package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const day = 24 * time.Hour

// reminderWatch is an active until-landed spawn watch created at base with the
// given reminder interval and pause-after count.
func reminderWatch(id string, base time.Time, every time.Duration, pauseAfter int) *Watch {
	w := &Watch{
		WatchID:             id,
		UserID:              "arn:alice",
		Status:              StatusActive,
		Action:              ActionSpawn,
		InstanceTypePattern: "p5.48xlarge",
		Regions:             []string{"us-east-1"},
		NotifyChannels:      []NotifyChannel{{Type: "sns", Target: "arn:aws:sns:us-east-1:111122223333:mine"}},
		CreatedAt:           base,
		UpdatedAt:           base,
		UntilLanded:         true,
		RemindEverySeconds:  int64(every / time.Second),
		RemindPauseAfter:    pauseAfter,
	}
	w.ExpiresAt, _ = w.AckDeadline(base)
	w.TTLTimestamp = RetentionTTL(w.ExpiresAt)
	return w
}

func TestReminderSchedule(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	type stage struct {
		k     int
		at    time.Duration // from base
		final bool
	}
	for _, tc := range []struct {
		name       string
		every      time.Duration
		pauseAfter int
		want       []stage
	}{
		{"defaults: two reminders and a final warning 24h before the pause", 7 * day, 2,
			[]stage{{1, 7 * day, false}, {2, 14 * day, false}, {3, 20 * day, true}}},
		{"36h: the final warning is its own stage", 36 * time.Hour, 2,
			[]stage{{1, 36 * time.Hour, false}, {2, 72 * time.Hour, false}, {3, 84 * time.Hour, true}}},
		{"exactly 24h: the last reminder is the final warning", day, 2,
			[]stage{{1, day, false}, {2, 2 * day, true}}},
		{"12h: the last reminder is the final warning", 12 * time.Hour, 2,
			[]stage{{1, 12 * time.Hour, false}, {2, 24 * time.Hour, true}}},
		{"no pause-after reminders, long interval: only the final warning", 3 * day, 0,
			[]stage{{1, 2 * day, true}}},
		{"no pause-after reminders, short interval: nothing", time.Hour, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := reminderWatch("w-s", base, tc.every, tc.pauseAfter).ReminderSchedule()
			if len(got) != len(tc.want) {
				t.Fatalf("schedule = %+v, want %d stages", got, len(tc.want))
			}
			for i, w := range tc.want {
				g := got[i]
				if g.Stage != w.k || !g.Due.Equal(base.Add(w.at)) || g.Final != w.final || g.Of != len(tc.want) {
					t.Errorf("stage %d = %+v, want stage %d at +%s final=%v of %d", i, g, w.k, w.at, w.final, len(tc.want))
				}
			}
		})
	}

	// Not until-landed: no reminders, whatever else is set.
	ttl := reminderWatch("w-ttl", base, 7*day, 2)
	ttl.UntilLanded = false
	if got := ttl.ReminderSchedule(); got != nil {
		t.Errorf("TTL watch schedule = %+v, want none", got)
	}
}

// The schedule follows the stored window, not the creation time: after an ack
// the window starts again from the ack.
func TestReminderSchedule_FollowsAck(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := reminderWatch("w-ack", base, 7*day, 2)
	ackAt := base.Add(10 * day)
	w.ExpiresAt, _ = w.AckDeadline(ackAt)
	got := w.ReminderSchedule()
	if len(got) != 3 || !got[0].Due.Equal(ackAt.Add(7*day)) || !got[2].Due.Equal(ackAt.Add(20*day)) {
		t.Fatalf("schedule after ack = %+v, want it counted from the ack at %s", got, ackAt)
	}
}

func TestDueReminder(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := reminderWatch("w-due", base, 7*day, 2)

	for _, tc := range []struct {
		name  string
		at    time.Duration
		sent  int
		stale bool // the marker is for an earlier window
		want  int  // 0 = nothing due
	}{
		{"before the first reminder", 7*day - time.Minute, 0, false, 0},
		{"first reminder due", 7 * day, 0, false, 1},
		{"first reminder already sent", 8 * day, 1, false, 0},
		{"second due after the first", 14*day + time.Minute, 1, false, 2},
		{"catch-up sends only the latest due", 15 * day, 0, false, 2},
		{"final warning", 20*day + time.Minute, 2, false, 3},
		{"everything sent", 20*day + time.Hour, 3, false, 0},
		{"a marker from an earlier window counts as nothing sent", 7*day + time.Hour, 3, true, 1},
		{"at the deadline the pause takes over", 21 * day, 0, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := *w
			c.RemindersSent = tc.sent
			c.ReminderWindow = reminderWindowKey(&c)
			if tc.stale {
				c.ReminderWindow -= 1000
			}
			r, ok := c.DueReminder(base.Add(tc.at))
			if tc.want == 0 {
				if ok {
					t.Fatalf("DueReminder = %+v, want none", r)
				}
				return
			}
			if !ok || r.Stage != tc.want {
				t.Fatalf("DueReminder = %+v (ok=%v), want stage %d", r, ok, tc.want)
			}
		})
	}

	inactive := *w
	inactive.Status = StatusMatched
	if r, ok := inactive.DueReminder(base.Add(8 * day)); ok {
		t.Errorf("a matched watch is due %+v, want nothing", r)
	}
}

// TestClaimReminder_AtMostOnce exercises the conditional UpdateItem against
// the DynamoDB emulator: a stage is claimed once per window, never below the
// stage already claimed, never for an older window, and never on a watch that
// is no longer active.
func TestClaimReminder_AtMostOnce(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	w := reminderWatch("w-claim", base, 7*day, 2)
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	win := reminderWindowKey(w)

	steps := []struct {
		name   string
		window int64
		stage  int
		want   bool
	}{
		{"first claim of stage 1", win, 1, true},
		{"stage 1 again", win, 1, false},
		{"stage 2", win, 2, true},
		{"back to stage 1", win, 1, false},
		{"an older window, even at a higher stage", win - 3600, 3, false},
		{"a newer window (after an ack) starts again at stage 1", win + 3600, 1, true},
		{"the old window cannot be reclaimed after that", win, 3, false},
	}
	for _, s := range steps {
		got, err := store.ClaimReminder(ctx, w.WatchID, s.window, s.stage)
		if err != nil {
			t.Fatalf("%s: ClaimReminder: %v", s.name, err)
		}
		if got != s.want {
			t.Errorf("%s: claimed = %v, want %v", s.name, got, s.want)
		}
	}

	stored, err := store.GetWatch(ctx, w.WatchID)
	if err != nil || stored == nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if stored.ReminderWindow != win+3600 || stored.RemindersSent != 1 {
		t.Errorf("marker = (%d, %d), want (%d, 1)", stored.ReminderWindow, stored.RemindersSent, win+3600)
	}
	if !stored.UpdatedAt.Equal(w.UpdatedAt) {
		t.Errorf("updated_at moved %s -> %s: a claim must not bump the version `lagotto update` checks", w.UpdatedAt, stored.UpdatedAt)
	}

	if err := store.UpdateWatchStatus(ctx, w.WatchID, StatusCancelled); err != nil {
		t.Fatalf("UpdateWatchStatus: %v", err)
	}
	if got, err := store.ClaimReminder(ctx, w.WatchID, win+7200, 1); err != nil || got {
		t.Errorf("claim on a cancelled watch = %v, %v; want false, nil", got, err)
	}
}

// reminderPoller is a poller on store whose sweep clock is *clock and whose
// reminders are counted rather than sent.
func reminderPoller(store *Store, clock *time.Time, sent *[]ReminderStage, mu *sync.Mutex) *Poller {
	p := NewPoller(nil, store, false)
	p.truffle = &fakeSearcher{} // nothing offered: the watch keeps hunting
	p.now = func() time.Time { return *clock }
	p.notifyReminder = func(_ context.Context, w *Watch, r ReminderStage, _ time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		*sent = append(*sent, r)
		return nil
	}
	return p
}

// TestPollAll_RemindersAcrossAWindow steps the sweep clock through a default
// 21-day window: each stage is sent exactly once, on the first sweep after it
// falls due, however many sweeps follow; an ack resets the marker so the new
// window's reminders go out again; the deadline pauses the watch.
func TestPollAll_RemindersAcrossAWindow(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	w := reminderWatch("w-window", base, 7*day, 2)
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	var (
		mu    sync.Mutex
		sent  []ReminderStage
		clock time.Time
	)
	p := reminderPoller(store, &clock, &sent, &mu)
	sweep := func(at time.Duration) *PollSummary {
		t.Helper()
		clock = base.Add(at)
		s, err := p.PollAll(ctx)
		if err != nil {
			t.Fatalf("PollAll at +%s: %v", at, err)
		}
		return s
	}
	expectSent := func(when string, stages ...int) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		var got []int
		for _, r := range sent {
			got = append(got, r.Stage)
		}
		if len(got) != len(stages) {
			t.Fatalf("%s: sent stages %v, want %v", when, got, stages)
		}
		for i := range got {
			if got[i] != stages[i] {
				t.Fatalf("%s: sent stages %v, want %v", when, got, stages)
			}
		}
	}

	sweep(6 * day)
	expectSent("day 6")
	if s := sweep(7*day + 5*time.Minute); s.Reminded != 1 {
		t.Errorf("day 7 summary Reminded = %d, want 1", s.Reminded)
	}
	sweep(7*day + 10*time.Minute)
	sweep(10 * day)
	expectSent("day 10", 1)
	sweep(14*day + 5*time.Minute)
	sweep(19 * day)
	expectSent("day 19", 1, 2)
	sweep(20*day + 5*time.Minute)
	sweep(20*day + 23*time.Hour)
	expectSent("day 20", 1, 2, 3)
	mu.Lock()
	if last := sent[2]; !last.Final || last.Of != 3 {
		t.Errorf("third reminder = %+v, want the final one of 3", last)
	}
	mu.Unlock()

	// ack on day 20 + 23h: the deadline moves 21 days ahead of it, through
	// the same ExtendWatch `lagotto ack` uses. No marker write is needed.
	ackAt := base.Add(20*day + 23*time.Hour)
	newDeadline, _ := w.AckDeadline(ackAt)
	if err := store.ExtendWatch(ctx, w.WatchID, newDeadline, false); err != nil {
		t.Fatalf("ExtendWatch: %v", err)
	}
	sweep(21*day + time.Hour) // past the OLD deadline: not paused, not reminded
	expectSent("after the ack", 1, 2, 3)
	if got, _ := store.GetWatch(ctx, w.WatchID); got == nil || got.Status != StatusActive {
		t.Fatalf("after the ack the watch is %+v, want active", got)
	}
	sweep(20*day + 23*time.Hour + 7*day + time.Minute)
	expectSent("first reminder of the new window", 1, 2, 3, 1)

	// Past the new deadline the watch pauses, and no reminder goes with it.
	s := sweep(20*day + 23*time.Hour + 21*day + time.Minute)
	if s.Expired != 1 || s.Reminded != 0 {
		t.Errorf("at the new deadline summary = %+v, want 1 expired and no reminder", s)
	}
	expectSent("at the pause", 1, 2, 3, 1)
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got == nil || !got.Paused() {
		t.Fatalf("after the new deadline the watch is %+v, want paused", got)
	}
}

// TestRemindIfDue_SecondPollerLosesTheClaim is the double-poller case (the
// hosted Lambda plus a local daemon, #47): both loaded the watch before either
// sent anything, so both see stage 1 due in memory. Only the one whose
// conditional claim lands sends; the other's claim fails on the marker and it
// sends nothing. The pollers run one after the other because the emulator does
// not serialize concurrent conditional writes (real DynamoDB does, per item).
func TestRemindIfDue_SecondPollerLosesTheClaim(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	if err := store.PutWatch(ctx, reminderWatch("w-race", base, 7*day, 2)); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	loadedA, _ := store.GetWatch(ctx, "w-race")
	loadedB, _ := store.GetWatch(ctx, "w-race")

	var (
		mu   sync.Mutex
		sent []ReminderStage
	)
	clock := base.Add(7*day + time.Minute)
	a := reminderPoller(store, &clock, &sent, &mu)
	b := reminderPoller(store, &clock, &sent, &mu)
	sa, sb := &PollSummary{}, &PollSummary{}
	a.remindIfDue(ctx, loadedA, clock, sa)
	b.remindIfDue(ctx, loadedB, clock, sb)
	if len(sent) != 1 || sent[0].Stage != 1 || sa.Reminded != 1 || sb.Reminded != 0 {
		t.Fatalf("sent = %+v, Reminded = %d/%d; want stage 1 sent once, by the first poller", sent, sa.Reminded, sb.Reminded)
	}
	if loadedA.RemindersSent != 1 || loadedA.ReminderWindow != reminderWindowKey(loadedA) {
		t.Errorf("winner's in-memory marker = (%d, %d), want it mirrored so a later PutWatch keeps it", loadedA.ReminderWindow, loadedA.RemindersSent)
	}
	if loadedB.RemindersSent != 0 {
		t.Errorf("loser's in-memory marker = %d, want untouched", loadedB.RemindersSent)
	}
}

// A send that fails is neither fatal nor retried: the claim already landed,
// and a lost reminder can only cause the (safe) pause.
func TestPollAll_ReminderSendFailureIsNotRetried(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	if err := store.PutWatch(ctx, reminderWatch("w-fail", base, 7*day, 2)); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	clock := base.Add(7*day + time.Minute)
	p := NewPoller(nil, store, false)
	p.truffle = &fakeSearcher{}
	p.now = func() time.Time { return clock }
	calls := 0
	p.notifyReminder = func(context.Context, *Watch, ReminderStage, time.Time) error {
		calls++
		return errors.New("webhook down")
	}
	for i := 0; i < 3; i++ {
		if _, err := p.PollAll(ctx); err != nil {
			t.Fatalf("PollAll: %v", err)
		}
		clock = clock.Add(5 * time.Minute)
	}
	if calls != 1 {
		t.Errorf("reminder sends = %d, want 1 (no retry)", calls)
	}
}

// Nothing to deliver to, or nothing to deliver with: the stage is left
// unclaimed, so a poller that can deliver it still will.
func TestPollAll_ReminderUnclaimedWithoutDelivery(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	quiet := reminderWatch("w-quiet", base, 7*day, 2)
	quiet.NotifyChannels = nil
	if err := store.PutWatch(ctx, quiet); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	loud := reminderWatch("w-loud", base, 7*day, 2)
	if err := store.PutWatch(ctx, loud); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	clock := base.Add(7*day + time.Minute)
	p := NewPoller(nil, store, false) // no Notifier, no seam
	p.truffle = &fakeSearcher{}
	p.now = func() time.Time { return clock }
	if s, err := p.PollAll(ctx); err != nil || s.Reminded != 0 {
		t.Fatalf("PollAll = %+v, %v; want no reminder without a notifier", s, err)
	}
	for _, id := range []string{"w-quiet", "w-loud"} {
		got, _ := store.GetWatch(ctx, id)
		if got == nil || got.RemindersSent != 0 || got.ReminderWindow != 0 {
			t.Errorf("%s marker = %+v, want unclaimed", id, got)
		}
	}
}

func TestNotifyReminder_Channels(t *testing.T) {
	ts, posts := hookServer(t)
	pub := &capturePublisher{}
	alerts := "arn:aws:sns:us-east-1:111122223333:lagotto-capacity-alerts"
	own := "arn:aws:sns:us-east-1:111122223333:my-own-topic"
	n := NewNotifier(aws.Config{}, alerts)
	n.httpClient = routedHTTPClient(ts)
	n.snsClient = pub

	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := reminderWatch("w-chan", base, 7*day, 2)
	w.NotifyChannels = []NotifyChannel{
		{Type: "email", Target: "alice@example.com"},
		{Type: "webhook", Target: hookURL},
		{Type: "sns", Target: own},
	}
	w.MaxPrice = 98.32
	w.Attempts = 412
	w.LastAttemptAt = base.Add(19*day + 23*time.Hour)
	w.LastError = "capacity: InsufficientInstanceCapacity: no p5.48xlarge in us-east-1a"
	w.LastPolledAt = base.Add(20*day - 5*time.Minute)
	r := w.ReminderSchedule()[2]
	now := base.Add(20*day + time.Minute)

	if err := n.NotifyReminder(context.Background(), w, r, now); err != nil {
		t.Fatalf("NotifyReminder: %v", err)
	}
	if got := atomic.LoadInt32(posts); got != 1 {
		t.Errorf("webhook POSTs = %d, want 1", got)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("publishes = %d, want 2 (email + sns)", len(pub.msgs))
	}
	email, own2 := pub.msgs[0], pub.msgs[1]
	if aws.ToString(email.TopicArn) != alerts || !strings.Contains(aws.ToString(email.Subject), "FINAL") {
		t.Errorf("email publish = topic %q subject %q, want the alerts topic and a FINAL subject", aws.ToString(email.TopicArn), aws.ToString(email.Subject))
	}
	body := aws.ToString(email.Message)
	for _, want := range []string{
		"lagotto ack w-chan", "lagotto cancel w-chan", "FINAL REMINDER", "in 23h59m",
		"Attempts:     412", "InsufficientInstanceCapacity", "Last poll:", "reminder 3 of 3",
		"21d ahead", "$98.3200/hr (the --max-price cap you set, not a price quote)",
		w.ExpiresAt.Format(time.RFC3339),
	} {
		if !strings.Contains(body, want) {
			t.Errorf("email body missing %q:\n%s", want, body)
		}
	}

	if aws.ToString(own2.TopicArn) != own {
		t.Fatalf("sns published to %q, want %q", aws.ToString(own2.TopicArn), own)
	}
	var p map[string]interface{}
	if err := json.Unmarshal([]byte(aws.ToString(own2.Message)), &p); err != nil {
		t.Fatalf("sns payload is not JSON: %v", err)
	}
	for k, want := range map[string]interface{}{
		"event": "reminder", "watch_id": "w-chan", "final": true, "reminder": 3.0, "reminders_in_window": 3.0,
		"until_landed": true, "ack_command": "lagotto ack w-chan", "attempts": 412.0,
		"deadline": w.ExpiresAt.Format(time.RFC3339), "seconds_until_pause": float64((day - time.Minute) / time.Second),
		"last_error": w.LastError, "max_price": 98.32,
	} {
		if p[k] != want {
			t.Errorf("payload[%q] = %v, want %v", k, p[k], want)
		}
	}
}

func TestReminderText_NotFinalAndQuietHeartbeat(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := reminderWatch("w-first", base, 7*day, 2)
	r := w.ReminderSchedule()[0]
	now := base.Add(7*day + time.Minute)
	if s := reminderSubject(w, r); strings.Contains(s, "FINAL") || !strings.Contains(s, "still hunting p5.48xlarge") {
		t.Errorf("first subject = %q", s)
	}
	body := reminderBody(w, r, now)
	for _, want := range []string{"is still hunting", "in 13d23h", "Last poll:    never", "Attempts:     0", "reminder 1 of 3"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
	for _, unwanted := range []string{"FINAL", "Max price", "Last error", "Search error"} {
		if strings.Contains(body, unwanted) {
			t.Errorf("body has %q, which does not apply:\n%s", unwanted, body)
		}
	}
	p := reminderPayload(w, r, now)
	for _, k := range []string{"last_error", "last_poll_error", "last_polled_at", "last_attempt_at", "max_price"} {
		if _, ok := p[k]; ok {
			t.Errorf("payload has %q for a watch without one", k)
		}
	}
}

func TestRoundSpan(t *testing.T) {
	for d, want := range map[time.Duration]string{
		-time.Hour:                  "0m",
		30 * time.Second:            "1m",
		4*time.Hour + 5*time.Minute: "4h5m",
		13*day + 23*time.Hour + 59*time.Minute + 40*time.Second: "14d0h",
	} {
		if got := roundSpan(d); got != want {
			t.Errorf("roundSpan(%s) = %q, want %q", d, got, want)
		}
	}
}
