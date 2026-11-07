package cmd

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// untilLandedFlags is the parsed --until-landed / --remind pair (#239).
type untilLandedFlags struct {
	enabled     bool
	remindEvery time.Duration
	pauseAfter  int
}

// window is how far creation (or an ack) puts the acknowledgement deadline.
func (u untilLandedFlags) window() time.Duration {
	return time.Duration(u.pauseAfter+1) * u.remindEvery
}

// parseTTLFlag parses a --ttl value. A TTL must be positive (#239, owner
// decision 1): "0" and "-1h" used to be accepted and meant "expire on the next
// poll", which nobody asks for on purpose. "Never" is spelled --until-landed.
func parseTTLFlag(raw string) (time.Duration, error) {
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		ttl, err = parseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid TTL %q: %w", raw, err)
		}
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("invalid TTL %q: it must be greater than zero (to keep hunting until a launch lands, use --until-landed instead of --ttl)", raw)
	}
	return ttl, nil
}

// parseUntilLandedFlags validates the flag combination of `watch --until-landed`.
// It is exclusive with --ttl (it replaces it), and with --maintain/--until (a
// fleet has its own end condition). --remind means nothing without it.
func parseUntilLandedFlags(untilLanded bool, remind string, ttlSet, remindSet, maintainSet, untilSet bool) (untilLandedFlags, error) {
	if !untilLanded {
		if remindSet {
			return untilLandedFlags{}, fmt.Errorf("--remind only applies to --until-landed watches")
		}
		return untilLandedFlags{}, nil
	}
	switch {
	case ttlSet:
		return untilLandedFlags{}, fmt.Errorf("--until-landed and --ttl are mutually exclusive: an until-landed watch has no TTL, it pauses unless acknowledged (see --remind)")
	case maintainSet:
		return untilLandedFlags{}, fmt.Errorf("--until-landed and --maintain are mutually exclusive: a fleet ends on its --until condition")
	case untilSet:
		return untilLandedFlags{}, fmt.Errorf("--until-landed and --until are mutually exclusive")
	}
	d, err := time.ParseDuration(remind)
	if err != nil {
		d, err = parseDuration(remind)
		if err != nil {
			return untilLandedFlags{}, fmt.Errorf("invalid --remind %q: %w", remind, err)
		}
	}
	if err := watcher.ValidateRemindEvery(d); err != nil {
		return untilLandedFlags{}, err
	}
	return untilLandedFlags{enabled: true, remindEvery: d, pauseAfter: watcher.DefaultRemindPauseAfter}, nil
}

// stdinIsTerminal reports whether stdin is an interactive terminal.
func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// confirmUntilLanded asks before creating an until-landed spawn or hold watch
// (#239, owner decision 9): with no TTL it may launch or reserve billable
// capacity at any point in the next weeks. --yes skips the prompt; without a
// terminal to ask on, --yes is required. Every other watch passes straight
// through.
func confirmUntilLanded(in io.Reader, errOut io.Writer, isTTY, yes bool, action watcher.ActionMode, pattern string, ul untilLandedFlags) error {
	if !ul.enabled || yes || (action != watcher.ActionSpawn && action != watcher.ActionHold) {
		return nil
	}
	what := "launch a billable " + pattern + " instance"
	if action == watcher.ActionHold {
		what = "create a billable capacity reservation for " + pattern + " (billed for 30 minutes whether used or not)"
	}
	if !isTTY {
		return fmt.Errorf("--until-landed --action %s needs confirmation: it may %s at any time until it lands. "+
			"Pass --yes to confirm when not running in a terminal; no watch was created", action, what)
	}
	_, _ = fmt.Fprintf(errOut, "This watch has no TTL: it may %s at any time until it lands.\n", what)
	_, _ = fmt.Fprintf(errOut, "It pauses itself unless you run 'lagotto ack <id>' within %s (%d × the %s --remind interval).\n",
		watcher.FormatSpan(ul.window()), ul.pauseAfter+1, watcher.FormatSpan(ul.remindEvery))
	_, _ = fmt.Fprint(errOut, "Create it? [y/N] ")
	if !readYes(in) {
		return fmt.Errorf("not confirmed; no watch was created")
	}
	return nil
}

// warnUndeliverableUntilLanded warns when an until-landed watch's only channels
// are email addresses still awaiting confirmation: until one is confirmed, its
// reminders reach nobody and it will simply pause at its deadline (the safe
// outcome, but probably not the one wanted).
func warnUndeliverableUntilLanded(errOut io.Writer, w *watcher.Watch) {
	if !w.UntilLanded || len(w.NotifyChannels) == 0 {
		return
	}
	for _, ch := range w.NotifyChannels {
		if ch.Type != "email" || ch.Subscription == watcher.SubConfirmed {
			return
		}
	}
	_, _ = fmt.Fprintf(errOut, "Warning: none of this watch's email addresses has confirmed its subscription yet. Until one does, "+
		"nothing it sends reaches you, and it pauses at %s unless you run 'lagotto ack %s'.\n",
		w.ExpiresAt.Format(time.RFC3339), w.WatchID)
}

// untilLandedExpiresCol is the list EXPIRES column for an until-landed watch:
// "until landed (ack in 4d)" while active, "paused (unacked)" once its deadline
// passed. ok is false for any other watch.
func untilLandedExpiresCol(w *watcher.Watch, now time.Time) (string, bool) {
	if !w.UntilLanded {
		return "", false
	}
	if w.Paused() {
		return "paused (unacked)", true
	}
	if w.Status != watcher.StatusActive {
		return "until landed", true
	}
	left := w.ExpiresAt.Sub(now)
	if left <= 0 {
		return "until landed (ack due)", true
	}
	return fmt.Sprintf("until landed (ack in %s)", formatAge(left)), true
}

// writeExpires prints the `lagotto status` expiry line(s). An until-landed
// watch (#239) shows its acknowledgement deadline and how to move it, and a
// paused one says why it stopped and how to resume it.
func writeExpires(out io.Writer, w *watcher.Watch, now time.Time) {
	if !w.UntilLanded {
		_, _ = fmt.Fprintf(out, "Expires:  %s\n", w.ExpiresAt.Format(time.RFC3339))
		return
	}
	remind := watcher.FormatSpan(time.Duration(w.RemindEverySeconds) * time.Second)
	switch {
	case w.Paused():
		_, _ = fmt.Fprintf(out, "Expires:  until landed; PAUSED at %s, not acknowledged in time\n", w.ExpiresAt.Format(time.RFC3339))
		_, _ = fmt.Fprintf(out, "  Resume the hunt with: lagotto ack %s\n", w.WatchID)
	case w.Status == watcher.StatusActive:
		_, _ = fmt.Fprintf(out, "Expires:  until landed; pauses %s (in %s) unless acknowledged\n",
			w.ExpiresAt.Format(time.RFC3339), formatAge(w.ExpiresAt.Sub(now)))
		_, _ = fmt.Fprintf(out, "  Acknowledge with 'lagotto ack %s': each ack moves the deadline %s ahead (%d × the %s --remind interval).\n",
			w.WatchID, watcher.FormatSpan(w.AckWindow()), w.RemindPauseAfter+1, remind)
	default:
		_, _ = fmt.Fprintf(out, "Expires:  until landed (deadline was %s)\n", w.ExpiresAt.Format(time.RFC3339))
	}
}
