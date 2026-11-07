package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestParseTTLFlag(t *testing.T) {
	for _, ok := range []string{"24h", "7d", "1.5d", "90m"} {
		if d, err := parseTTLFlag(ok); err != nil || d <= 0 {
			t.Errorf("parseTTLFlag(%q) = %s, %v", ok, d, err)
		}
	}
	for _, bad := range []string{"0", "0s", "-1h", "0d"} {
		_, err := parseTTLFlag(bad)
		if err == nil || !strings.Contains(err.Error(), "greater than zero") || !strings.Contains(err.Error(), "--until-landed") {
			t.Errorf("parseTTLFlag(%q) = %v, want a must-be-positive refusal naming --until-landed", bad, err)
		}
	}
	if _, err := parseTTLFlag("never"); err == nil || !strings.Contains(err.Error(), "invalid TTL") {
		t.Errorf("parseTTLFlag(never) = %v", err)
	}
}

func TestParseUntilLandedFlags(t *testing.T) {
	ul, err := parseUntilLandedFlags(true, "7d", false, false, false, false)
	if err != nil || !ul.enabled || ul.remindEvery != 7*24*time.Hour || ul.window() != 21*24*time.Hour {
		t.Fatalf("defaults = %+v, %v", ul, err)
	}
	if ul, err := parseUntilLandedFlags(true, "36h", false, true, false, false); err != nil || ul.window() != 108*time.Hour {
		t.Errorf("--remind 36h = %+v, %v", ul, err)
	}
	if ul, err := parseUntilLandedFlags(false, "7d", true, false, true, true); err != nil || ul.enabled {
		t.Errorf("not until-landed = %+v, %v", ul, err)
	}
	for _, c := range []struct {
		name                                string
		until                               bool
		remind                              string
		ttlSet, remindSet, maintain, untilF bool
		want                                string
	}{
		{"ttl", true, "7d", true, false, false, false, "--ttl are mutually exclusive"},
		{"maintain", true, "7d", false, false, true, false, "--maintain are mutually exclusive"},
		{"until", true, "7d", false, false, false, true, "--until are mutually exclusive"},
		{"remind without", false, "3d", false, true, false, false, "only applies to --until-landed"},
		{"remind cap", true, "31d", false, true, false, false, "capped at 30d"},
		{"remind floor", true, "10m", false, true, false, false, "at least 1h"},
		{"remind junk", true, "soon", false, true, false, false, "invalid --remind"},
	} {
		_, err := parseUntilLandedFlags(c.until, c.remind, c.ttlSet, c.remindSet, c.maintain, c.untilF)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %q", c.name, err, c.want)
		}
	}
}

func TestConfirmUntilLanded(t *testing.T) {
	ul := untilLandedFlags{enabled: true, remindEvery: 7 * 24 * time.Hour, pauseAfter: 2}
	var errOut bytes.Buffer

	// Passes straight through: --yes, notify, or not until-landed at all.
	if err := confirmUntilLanded(strings.NewReader(""), &errOut, false, true, watcher.ActionSpawn, "p5.48xlarge", ul, nil); err != nil {
		t.Errorf("--yes: %v", err)
	}
	if err := confirmUntilLanded(strings.NewReader(""), &errOut, false, false, watcher.ActionSpawn, "p5.48xlarge", untilLandedFlags{}, nil); err != nil {
		t.Errorf("not until-landed: %v", err)
	}
	if errOut.Len() != 0 {
		t.Errorf("prompted when it should not: %q", errOut.String())
	}

	// No terminal and no --yes: refused, naming --yes.
	err := confirmUntilLanded(strings.NewReader("y\n"), &errOut, false, false, watcher.ActionHold, "p5.48xlarge", ul, nil)
	if err == nil || !strings.Contains(err.Error(), "--yes") || !strings.Contains(err.Error(), "capacity reservation") {
		t.Errorf("non-TTY = %v", err)
	}

	// A terminal: the prompt explains, and only an explicit yes proceeds.
	errOut.Reset()
	if err := confirmUntilLanded(strings.NewReader("y\n"), &errOut, true, false, watcher.ActionSpawn, "p5.48xlarge", ul, nil); err != nil {
		t.Errorf("TTY yes: %v", err)
	}
	for _, want := range []string{"no TTL", "billable p5.48xlarge instance", "lagotto ack", "21d", "7d", "[y/N]"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("prompt lacks %q:\n%s", want, errOut.String())
		}
	}
	err = confirmUntilLanded(strings.NewReader("\n"), &errOut, true, false, watcher.ActionSpawn, "p5.48xlarge", ul, nil)
	if err == nil || !strings.Contains(err.Error(), "not confirmed") {
		t.Errorf("TTY default answer = %v, want not confirmed", err)
	}
}

func TestWarnUndeliverableUntilLanded(t *testing.T) {
	w := &watcher.Watch{WatchID: "w-ul", UntilLanded: true, ExpiresAt: hbNow,
		NotifyChannels: []watcher.NotifyChannel{{Type: "email", Target: "a@example.com", Subscription: watcher.SubPending}}}
	var out bytes.Buffer
	warnUndeliverableUntilLanded(&out, w)
	if !strings.Contains(out.String(), "confirmed") || !strings.Contains(out.String(), "lagotto ack w-ul") {
		t.Errorf("pending-only email: %q", out.String())
	}
	for _, quiet := range []*watcher.Watch{
		{UntilLanded: true, NotifyChannels: []watcher.NotifyChannel{{Type: "email", Subscription: watcher.SubConfirmed}}},
		{UntilLanded: true, NotifyChannels: []watcher.NotifyChannel{{Type: "email", Subscription: watcher.SubPending}, {Type: "webhook"}}},
		{UntilLanded: false, NotifyChannels: []watcher.NotifyChannel{{Type: "email", Subscription: watcher.SubPending}}},
		{UntilLanded: true},
	} {
		out.Reset()
		warnUndeliverableUntilLanded(&out, quiet)
		if out.Len() != 0 {
			t.Errorf("warned for %+v: %q", quiet.NotifyChannels, out.String())
		}
	}
}

func ulWatch(status watcher.WatchStatus, expires time.Time) *watcher.Watch {
	return &watcher.Watch{WatchID: "w-ul", Status: status, UntilLanded: true, ExpiresAt: expires,
		RemindEverySeconds: int64(7 * 24 * time.Hour / time.Second), RemindPauseAfter: 2}
}

func TestExpiresCol(t *testing.T) {
	for _, c := range []struct {
		w    *watcher.Watch
		want string
	}{
		{ulWatch(watcher.StatusActive, hbNow.Add(4*24*time.Hour+time.Hour)), "until landed (ack in 4d)"},
		{ulWatch(watcher.StatusActive, hbNow.Add(5*time.Hour)), "until landed (ack in 5h)"},
		{ulWatch(watcher.StatusActive, hbNow.Add(-time.Minute)), "until landed (ack due)"},
		{func() *watcher.Watch {
			w := ulWatch(watcher.StatusExpired, hbNow)
			w.ExpiredReason = watcher.ExpiredReasonUnacknowledged
			return w
		}(), "paused (unacked)"},
		{ulWatch(watcher.StatusMatched, hbNow), "until landed"},
		{&watcher.Watch{Status: watcher.StatusActive, ExpiresAt: hbNow}, hbNow.Format(time.RFC3339)},
	} {
		if got := expiresCol(c.w, hbNow); got != c.want {
			t.Errorf("expiresCol = %q, want %q", got, c.want)
		}
		if n := len(expiresCol(c.w, hbNow)); n > 25 {
			t.Errorf("%q overflows the 25-wide EXPIRES column", expiresCol(c.w, hbNow))
		}
	}
	// The widest form still fits: two-digit days.
	if got := expiresCol(ulWatch(watcher.StatusActive, hbNow.Add(29*24*time.Hour)), hbNow); len(got) > 25 {
		t.Errorf("%q overflows", got)
	}
}

func TestWriteExpires(t *testing.T) {
	var out bytes.Buffer
	writeExpires(&out, &watcher.Watch{ExpiresAt: hbNow}, hbNow)
	if out.String() != "Expires:  "+hbNow.Format(time.RFC3339)+"\n" {
		t.Errorf("TTL watch = %q", out.String())
	}

	out.Reset()
	writeExpires(&out, ulWatch(watcher.StatusActive, hbNow.Add(3*24*time.Hour)), hbNow)
	for _, want := range []string{"until landed", "in 3d", "lagotto ack w-ul", "21d ahead", "3 × the 7d"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("active until-landed lacks %q:\n%s", want, out.String())
		}
	}

	out.Reset()
	paused := ulWatch(watcher.StatusExpired, hbNow)
	paused.ExpiredReason = watcher.ExpiredReasonUnacknowledged
	writeExpires(&out, paused, hbNow)
	if !strings.Contains(out.String(), "PAUSED") || !strings.Contains(out.String(), "lagotto ack w-ul") {
		t.Errorf("paused = %q", out.String())
	}

	out.Reset()
	writeExpires(&out, ulWatch(watcher.StatusMatched, hbNow), hbNow)
	if !strings.Contains(out.String(), "deadline was") {
		t.Errorf("matched = %q", out.String())
	}
}

func TestExtendAndAckExpiry(t *testing.T) {
	now := hbNow
	plain := &watcher.Watch{WatchID: "w-ttl"}
	if got, err := extendExpiry(plain, now, 48*time.Hour, true); err != nil || !got.Equal(now.Add(48*time.Hour)) {
		t.Errorf("extend TTL watch = %s, %v", got, err)
	}
	if _, err := ackExpiry(plain, now); err == nil || !strings.Contains(err.Error(), "not an --until-landed watch") {
		t.Errorf("ack TTL watch = %v", err)
	}

	ul := ulWatch(watcher.StatusActive, now)
	want := now.Add(21 * 24 * time.Hour)
	if got, err := ackExpiry(ul, now); err != nil || !got.Equal(want) {
		t.Errorf("ack = %s, %v", got, err)
	}
	if got, err := extendExpiry(ul, now, 24*time.Hour, false); err != nil || !got.Equal(want) {
		t.Errorf("extend until-landed (no --ttl) = %s, %v; want the ack deadline", got, err)
	}
	if _, err := extendExpiry(ul, now, 24*time.Hour, true); err == nil || !strings.Contains(err.Error(), "drop --ttl") {
		t.Errorf("extend until-landed --ttl = %v", err)
	}
}

func TestWriteExtended(t *testing.T) {
	var out bytes.Buffer
	writeExtended(&out, ulWatch(watcher.StatusActive, hbNow), hbNow)
	if !strings.Contains(out.String(), "Acknowledged watch w-ul") {
		t.Errorf("until-landed = %q", out.String())
	}
	out.Reset()
	writeExtended(&out, &watcher.Watch{WatchID: "w-ttl"}, hbNow)
	if !strings.Contains(out.String(), "w-ttl") || strings.Contains(out.String(), "Acknowledged") {
		t.Errorf("TTL = %q", out.String())
	}
}

// setFlagsForTest sets flags on c for one test and restores their values and
// Changed state afterwards, so the package-level flag variables do not leak.
func setFlagsForTest(t *testing.T, c *cobra.Command, kv map[string]string) {
	t.Helper()
	for name, val := range kv {
		f := c.Flags().Lookup(name)
		if f == nil {
			t.Fatalf("no flag --%s", name)
		}
		old, oldChanged := f.Value.String(), f.Changed
		if err := c.Flags().Set(name, val); err != nil {
			t.Fatalf("set --%s=%s: %v", name, val, err)
		}
		t.Cleanup(func() {
			if sv, ok := f.Value.(interface{ Replace([]string) error }); ok && name == "notify" {
				_ = sv.Replace(nil)
			} else {
				_ = f.Value.Set(old)
			}
			f.Changed = oldChanged
		})
	}
}

// TestWatchUntilLandedEndToEnd runs `lagotto watch --until-landed --action
// hold --yes --notify sns:…` against the loopback AWS: the stored item carries
// the until-landed fields, a 21-day deadline as expires_at, and a finite
// DynamoDB TTL past it.
func TestWatchUntilLandedEndToEnd(t *testing.T) {
	setFlagsForTest(t, watchCmd, map[string]string{
		"until-landed": "true",
		"action":       "hold",
		"yes":          "true",
		"notify":       "sns:arn:aws:sns:us-east-1:123456789012:mine",
	})
	f := &fakeAWS{scheduleState: "ENABLED"}
	before := time.Now().UTC()
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if len(f.puts) != 1 {
		t.Fatalf("PutItem count = %d", len(f.puts))
	}
	item := f.puts[0]
	if item["until_landed"]["BOOL"] != true {
		t.Errorf("until_landed = %v", item["until_landed"])
	}
	if item["remind_every_seconds"]["N"] != "604800" || item["remind_pause_after"]["N"] != "2" {
		t.Errorf("remind fields = %v / %v", item["remind_every_seconds"], item["remind_pause_after"])
	}
	exp, err := time.Parse(time.RFC3339, item["expires_at"]["S"].(string))
	if err != nil {
		t.Fatal(err)
	}
	want := before.Add(21 * 24 * time.Hour)
	if exp.Before(want.Add(-time.Minute)) || exp.After(want.Add(time.Minute)) {
		t.Errorf("expires_at = %s, want ~%s (3 × 7d)", exp, want)
	}
	// Fail safe for an old poller: the item's TTL is finite, the retention
	// horizon past the deadline.
	if got := item["ttl_timestamp"]["N"]; got != jsonNum(watcher.RetentionTTL(exp)) {
		t.Errorf("ttl_timestamp = %v, want RetentionTTL(deadline)", got)
	}
}

// pinStdinTerminal fixes what stdinIsTerminal reports for one test.
func pinStdinTerminal(t *testing.T, tty bool) {
	t.Helper()
	old := stdinIsTerminal
	stdinIsTerminal = func() bool { return tty }
	t.Cleanup(func() { stdinIsTerminal = old })
}

func jsonNum(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// TestWatchUntilLandedRefusals: the refusals happen before anything is stored.
func TestWatchUntilLandedRefusals(t *testing.T) {
	for _, c := range []struct {
		name  string
		flags map[string]string
		want  string
	}{
		{"notify", map[string]string{"until-landed": "true", "notify": "sns:arn:aws:sns:us-east-1:123456789012:t"}, "not supported for --action notify"},
		{"no channel", map[string]string{"until-landed": "true", "action": "hold", "yes": "true"}, "needs a --notify channel"},
		{"ttl", map[string]string{"until-landed": "true", "ttl": "7d"}, "mutually exclusive"},
		{"no yes, no TTY", map[string]string{"until-landed": "true", "action": "hold", "notify": "sns:arn:aws:sns:us-east-1:123456789012:t"}, "--yes"},
		{"ttl zero", map[string]string{"ttl": "0"}, "greater than zero"},
	} {
		t.Run(c.name, func(t *testing.T) {
			pinStdinTerminal(t, false)
			setFlagsForTest(t, watchCmd, c.flags)
			f := &fakeAWS{scheduleState: "ENABLED"}
			err := runWatchWithFake(t, f)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want %q", err, c.want)
			}
			if len(f.calls) != 0 {
				t.Errorf("refusal made AWS calls: %v", f.calls)
			}
		})
	}
}

// storedUntilLandedItem is a paused until-landed watch in DynamoDB JSON.
func storedUntilLandedItem(status, reason string) string {
	exp := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	item := `{"watch_id":{"S":"w-1"},"user_id":{"S":"` + fakeCallerARN + `"},"status":{"S":"` + status +
		`"},"instance_type_pattern":{"S":"g5.xlarge"},"expires_at":{"S":"` + exp +
		`"},"until_landed":{"BOOL":true},"remind_every_seconds":{"N":"604800"},"remind_pause_after":{"N":"2"}`
	if reason != "" {
		item += `,"expired_reason":{"S":"` + reason + `"}`
	}
	return item + `}`
}

func runAckWithFake(t *testing.T, f *fakeAWS) (string, error) {
	t.Helper()
	installFakeAWS(t, f)
	var out bytes.Buffer
	ackCmd.SetOut(&out)
	t.Cleanup(func() { ackCmd.SetOut(nil) })
	err := ackCmd.RunE(ackCmd, []string{"w-1"})
	return out.String(), err
}

// TestAckResumesPausedWatch: ack on a paused watch reactivates it, clears the
// reason, and moves expires_at and ttl_timestamp a full window ahead.
func TestAckResumesPausedWatch(t *testing.T) {
	f := &fakeAWS{item: storedUntilLandedItem("expired", watcher.ExpiredReasonUnacknowledged)}
	out, err := runAckWithFake(t, f)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(f.updates) != 1 {
		t.Fatalf("UpdateItems = %v", f.updates)
	}
	u := f.updates[0]
	for _, want := range []string{"expires_at = :exp", "ttl_timestamp = :ttl", "#st = :active", "REMOVE expired_reason"} {
		if !strings.Contains(u, want) {
			t.Errorf("UpdateItem lacks %q: %s", want, u)
		}
	}
	want := time.Now().UTC().Add(21 * 24 * time.Hour).Format("2006-01-02T15")
	if !strings.Contains(u, want) {
		t.Errorf("UpdateItem does not set a deadline 21d out (%s…): %s", want, u)
	}
	if !strings.Contains(out, "Reactivated") || !strings.Contains(out, "Acknowledged watch w-1") {
		t.Errorf("output = %q", out)
	}
}

// TestAckActiveWatch moves the deadline without touching the schedule.
func TestAckActiveWatch(t *testing.T) {
	f := &fakeAWS{getErr: "AccessDeniedException", item: storedUntilLandedItem("active", "")}
	if _, err := runAckWithFake(t, f); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if n := f.called("scheduler:GetSchedule"); n != 0 {
		t.Errorf("ack of an active watch read the schedule %d times", n)
	}
	if len(f.updates) != 1 || strings.Contains(f.updates[0], "REMOVE") {
		t.Errorf("UpdateItems = %v", f.updates)
	}
}

// TestAckRefusesTTLWatch: a TTL watch has nothing to acknowledge, and nothing
// is written.
func TestAckRefusesTTLWatch(t *testing.T) {
	f := &fakeAWS{item: storedWatchItem("active")}
	_, err := runAckWithFake(t, f)
	if err == nil || !strings.Contains(err.Error(), "lagotto extend") {
		t.Fatalf("ack of a TTL watch = %v", err)
	}
	if len(f.updates) != 0 {
		t.Errorf("refused ack wrote %v", f.updates)
	}
}

// TestAckRollbackKeepsPause: if the schedule cannot be switched on, the resumed
// watch is put back as PAUSED (with its reason), not as a plain TTL expiry.
func TestAckRollbackKeepsPause(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException",
		item: storedUntilLandedItem("expired", watcher.ExpiredReasonUnacknowledged)}
	if _, err := runAckWithFake(t, f); err == nil {
		t.Fatal("ack succeeded although UpdateSchedule was denied")
	}
	if len(f.updates) != 2 || !strings.Contains(f.updates[1], "expired_reason = :reason") ||
		!strings.Contains(f.updates[1], watcher.ExpiredReasonUnacknowledged) {
		t.Errorf("UpdateItems = %v, want reactivate then restore the pause", f.updates)
	}
}

// TestExtendUntilLandedRefusesTTL: `extend --ttl` on an until-landed watch is
// refused instead of silently ignoring --ttl.
func TestExtendUntilLandedRefusesTTL(t *testing.T) {
	setFlagsForTest(t, extendCmd, map[string]string{"ttl": "7d"})
	f := &fakeAWS{item: storedUntilLandedItem("active", "")}
	err := runExtendWithFake(t, f)
	if err == nil || !strings.Contains(err.Error(), "drop --ttl") {
		t.Fatalf("extend --ttl on until-landed = %v", err)
	}
	if len(f.updates) != 0 {
		t.Errorf("refused extend wrote %v", f.updates)
	}
}

func TestExtendRejectsZeroTTL(t *testing.T) {
	setFlagsForTest(t, extendCmd, map[string]string{"ttl": "0"})
	f := &fakeAWS{item: storedWatchItem("active")}
	if err := runExtendWithFake(t, f); err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("extend --ttl 0 = %v", err)
	}
}
