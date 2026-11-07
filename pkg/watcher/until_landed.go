package watcher

import (
	"fmt"
	"time"
)

// Until-landed watches (#239 phase 3).
//
// An --until-landed watch has no TTL: it keeps hunting until its launch or hold
// lands. It is still bounded ("everything dies eventually") by a rolling
// ACKNOWLEDGEMENT DEADLINE, a dead-man's switch stored in ExpiresAt:
//
//	ExpiresAt = last acknowledgement + (RemindPauseAfter+1) × RemindEvery
//
// The defaults are a reminder every 7 days and a pause after 2 unacknowledged
// reminders, so 21 days. `lagotto ack` (or `extend`) moves the deadline forward
// by a full window. When it passes, the poller expires the watch with
// ExpiredReason=unacknowledged ("paused"); `ack` or `extend` resumes it.
//
// Storing the deadline in ExpiresAt is what makes this fail safe across
// versions. A poller that predates this phase knows nothing about until-landed,
// sees an ordinary ExpiresAt, and expires the watch at the deadline; the
// record's DynamoDB TTL is RetentionTTL(ExpiresAt), finite like any other. A
// lost or late reminder (phase 4) can therefore only cause an unwanted pause,
// never a launch nobody is watching over.
//
// Phase 4 (reminders, reminders.go) builds on this: reminder k
// (1..RemindPauseAfter) of the current window is due at
// AckWindowStart()+k×RemindEvery, and the final warning at ExpiresAt-24h. All of
// those are derived from the stored fields, so a poller never applies its own
// defaults to someone else's watch.

const (
	// DefaultRemindEvery is the default --remind interval.
	DefaultRemindEvery = 7 * 24 * time.Hour
	// MaxRemindEvery caps --remind (owner decision 2): at the default pause-after
	// of 2, the longest an until-landed watch can go unacknowledged is 90 days.
	MaxRemindEvery = 30 * 24 * time.Hour
	// MinRemindEvery is the shortest --remind. The hosted poller sweeps every 5
	// minutes, so a shorter interval could not be honoured, and an interval of
	// minutes would pause the watch before anyone could act on a reminder.
	MinRemindEvery = time.Hour
	// DefaultRemindPauseAfter is how many unacknowledged reminders are sent
	// before the watch pauses (owner decision 2).
	DefaultRemindPauseAfter = 2
)

// ExpiredReasonUnacknowledged is the ExpiredReason of an until-landed watch whose
// acknowledgement deadline passed: it is paused, not given up on, and `lagotto
// ack` resumes it.
const ExpiredReasonUnacknowledged = "unacknowledged"

// untilLandedNotifyUnsupported is the refusal for --until-landed on a notify
// watch. notify fires on an offering match, which says nothing about whether a
// launch would succeed (#239: a p5.48xlarge notify watch fired twice within
// minutes while RunInstances returned InsufficientInstanceCapacity in every AZ),
// so an until-landed notify watch would "land" on its first poll. A real
// launchability signal for notify is phase 6.
const untilLandedNotifyUnsupported = "--until-landed is not supported for --action notify yet: notify fires when the type is " +
	"offered, which is not a sign a launch would succeed, so the watch would end on its first poll. " +
	"Use --action spawn (the launch itself is the capacity test) or --action hold, or keep notify with a --ttl"

// ValidateUntilLanded checks that a watch may be created as until-landed: not a
// notify watch (no launchability signal yet), not a goal-driven fleet (which has
// its own completion condition), and with at least one notify channel (owner
// decision 6), because a watch with no TTL must be able to tell its owner that
// it landed, paused, or (phase 4) needs acknowledging. remindEvery and pauseAfter are the
// values that will be stored.
func ValidateUntilLanded(action ActionMode, desiredCount int, channels []NotifyChannel, remindEvery time.Duration, pauseAfter int) error {
	if action == ActionNotify {
		return fmt.Errorf("%s", untilLandedNotifyUnsupported)
	}
	if desiredCount > 0 {
		return fmt.Errorf("--until-landed cannot be combined with --maintain: a fleet ends on its --until condition")
	}
	if err := ValidateRemindEvery(remindEvery); err != nil {
		return err
	}
	if pauseAfter < 0 {
		return fmt.Errorf("the reminder pause-after count must be >= 0")
	}
	if len(channels) == 0 {
		return fmt.Errorf("--until-landed --action %s needs a --notify channel (email:, webhook: or sns:): the watch has no TTL, "+
			"so a channel is how you hear that it landed, paused itself, or needs acknowledging", action)
	}
	return nil
}

// ValidateRemindEvery checks a --remind interval against MinRemindEvery and
// MaxRemindEvery.
func ValidateRemindEvery(d time.Duration) error {
	if d < MinRemindEvery {
		return fmt.Errorf("--remind must be at least %s (got %s)", FormatSpan(MinRemindEvery), FormatSpan(d))
	}
	if d > MaxRemindEvery {
		return fmt.Errorf("--remind is capped at 30d (got %s): an until-landed watch must be acknowledged at least that often", FormatSpan(d))
	}
	return nil
}

// AckWindow is how far one acknowledgement moves the deadline:
// (RemindPauseAfter+1) × RemindEvery. Zero for a watch that is not until-landed.
func (w *Watch) AckWindow() time.Duration {
	if !w.UntilLanded || w.RemindEverySeconds <= 0 {
		return 0
	}
	return time.Duration(w.RemindPauseAfter+1) * time.Duration(w.RemindEverySeconds) * time.Second
}

// AckDeadline is the acknowledgement deadline an acknowledgement at now sets.
// It returns false for a watch that is not until-landed (or carries no reminder
// interval), which has no deadline to move.
func (w *Watch) AckDeadline(now time.Time) (time.Time, bool) {
	win := w.AckWindow()
	if win <= 0 {
		return time.Time{}, false
	}
	return now.Add(win).UTC(), true
}

// AckWindowStart is when the current acknowledgement window began: the last
// acknowledgement, or the watch's creation. Reminders (phase 4) count from it.
// Zero for a watch that is not until-landed.
func (w *Watch) AckWindowStart() time.Time {
	win := w.AckWindow()
	if win <= 0 || w.ExpiresAt.IsZero() {
		return time.Time{}
	}
	return w.ExpiresAt.Add(-win)
}

// Paused reports whether the watch is an until-landed watch that stopped because
// nobody acknowledged it.
func (w *Watch) Paused() bool {
	return w.Status == StatusExpired && w.ExpiredReason == ExpiredReasonUnacknowledged
}

// expiryReason is the ExpiredReason the poller records when w passes ExpiresAt:
// unacknowledged for an until-landed watch, none for a TTL expiry.
func expiryReason(w *Watch) string {
	if w.UntilLanded {
		return ExpiredReasonUnacknowledged
	}
	return ""
}

// FormatSpan renders a reminder interval or acknowledgement window the way it is
// typed: "21d", "36h", or Go's form for anything finer ("90m0s").
func FormatSpan(d time.Duration) string {
	const day = 24 * time.Hour
	switch {
	case d > 0 && d%day == 0:
		return fmt.Sprintf("%dd", d/day)
	case d > 0 && d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	default:
		return d.String()
	}
}
