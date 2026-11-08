package watcher

import (
	"strings"
	"testing"
	"time"
)

// TestNotifyDeprecationText: the exported warning says what notify checks,
// what to use instead and that notify goes away (#283). The other front ends
// mirror this text, so its load-bearing phrases are pinned.
func TestNotifyDeprecationText(t *testing.T) {
	for _, want := range []string{
		"deprecated", "will be removed in a later release",
		"offered in the region at or under your max price", "not a capacity check",
		"--action hold", "--action spawn", "billed at the On-Demand rate", "launches the instance",
	} {
		if !strings.Contains(NotifyDeprecation, want) {
			t.Errorf("NotifyDeprecation lacks %q:\n%s", want, NotifyDeprecation)
		}
	}
	if got := NotifyDeprecationMessage(true); got != NotifyDeprecation {
		t.Errorf("explicit --action notify: %q", got)
	}
	got := NotifyDeprecationMessage(false)
	if !strings.HasPrefix(got, "no --action given, so this is a notify watch (deprecated); pass --action explicitly") ||
		!strings.HasSuffix(got, NotifyDeprecation) {
		t.Errorf("default action:\n%s", got)
	}
}

func TestActionDeprecations(t *testing.T) {
	for _, a := range []ActionMode{ActionSpawn, ActionHold} {
		if d := ActionDeprecations(a, true); d != nil {
			t.Errorf("%s: %v, want none", a, d)
		}
		if d := ActionDeprecations(a, false); d != nil {
			t.Errorf("%s (default): %v, want none", a, d)
		}
	}
	d := ActionDeprecations(ActionNotify, false)
	if len(d) != 1 || d[0].ID != DeprecationActionNotify || d[0].Message != NotifyDeprecationMessage(false) {
		t.Errorf("notify: %+v", d)
	}
}

func notifyMatch(action ActionMode) (*Watch, *MatchResult) {
	w := &Watch{WatchID: "w-n", Action: action, Service: ServiceEC2, InstanceTypePattern: "p5.48xlarge"}
	m := &MatchResult{WatchID: "w-n", InstanceType: "p5.48xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2a",
		Price: 98.32, MatchedAt: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC), ActionTaken: "notified"}
	return w, m
}

// TestNotifyWatchNotification_SaysOffered: a notify watch's own match
// notification says it was an offering, not capacity, and its payloads carry
// deprecated: true (#283).
func TestNotifyWatchNotification_SaysOffered(t *testing.T) {
	w, m := notifyMatch(ActionNotify)
	subject, body := matchEmailSubjectBody(w, m)
	if !strings.Contains(subject, "offered in us-west-2 (offered, not a capacity check)") || strings.Contains(subject, "capacity found") {
		t.Errorf("subject %q", subject)
	}
	if !strings.HasPrefix(body, "Offered (offered, not a capacity check) for your watch w-n") ||
		strings.Contains(body, "Capacity found") || !strings.Contains(body, NotifyDeprecation) {
		t.Errorf("body:\n%s", body)
	}
	for name, p := range map[string]map[string]interface{}{"webhook": matchWebhookPayload(w, m), "sns": matchSNSPayload(w, m)} {
		if p["deprecated"] != true || p["note"] != NotifyOfferedNote || p["deprecation"] != NotifyDeprecation {
			t.Errorf("%s payload: deprecated=%v note=%v", name, p["deprecated"], p["note"])
		}
	}
	// The default (empty) service is EC2.
	w.Service = ""
	if p := matchWebhookPayload(w, m); p["deprecated"] != true {
		t.Error("a notify watch with no stored service is not marked")
	}
}

// TestSpawnHoldNotification_NotDeprecated: spawn and hold matches are
// unchanged, including a spawn watch reported as "notified" by a poller with no
// launcher wired: it is the watch's action that is deprecated, not the outcome.
func TestSpawnHoldNotification_NotDeprecated(t *testing.T) {
	for _, a := range []ActionMode{ActionSpawn, ActionHold} {
		w, m := notifyMatch(a)
		subject, body := matchEmailSubjectBody(w, m)
		if !strings.Contains(subject, "capacity found") || !strings.HasPrefix(body, "Capacity found for your watch w-n") ||
			strings.Contains(body, "deprecated") {
			t.Errorf("%s email: %q\n%s", a, subject, body)
		}
		for name, p := range map[string]map[string]interface{}{"webhook": matchWebhookPayload(w, m), "sns": matchSNSPayload(w, m)} {
			for _, k := range []string{"deprecated", "note", "deprecation"} {
				if _, ok := p[k]; ok {
					t.Errorf("%s %s payload has %q", a, name, k)
				}
			}
		}
	}
}
