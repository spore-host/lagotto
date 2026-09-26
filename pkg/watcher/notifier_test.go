package watcher

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"context"
)

// TestSendWebhook_Payload tests the HTTP dispatch directly (bypasses URL validation
// since httptest servers are always http://).
func TestSendWebhook_Payload(t *testing.T) {
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	n := &Notifier{httpClient: ts.Client()}
	w := &Watch{WatchID: "w-hook1", InstanceTypePattern: "p5.*"}
	m := &MatchResult{
		WatchID:          "w-hook1",
		Region:           "us-west-2",
		AvailabilityZone: "us-west-2a",
		InstanceType:     "p5.48xlarge",
		Price:            32.77,
		IsSpot:           true,
		MatchedAt:        time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC),
		ActionTaken:      "notified",
	}

	// Call sendWebhook directly to test payload without URL validation
	if err := n.sendWebhook(context.Background(), ts.URL, w, m); err != nil {
		t.Fatalf("sendWebhook: %v", err)
	}
	if received == nil {
		t.Fatal("webhook was not called")
	}
	if got := received["instance_type"]; got != "p5.48xlarge" {
		t.Errorf("instance_type = %v, want p5.48xlarge", got)
	}
	if got := received["price"].(float64); got != 32.77 {
		t.Errorf("price = %v, want 32.77", got)
	}
}

// TestSafeHTTPClient_RefusesInternalDialIP verifies the dial-time SSRF guard:
// even if a request reaches the HTTP client, the custom DialContext re-validates
// the resolved IP and refuses to connect to internal addresses — the defense
// against DNS rebinding past ValidateWebhookURL (#40). An httptest server binds
// to 127.0.0.1 (loopback), which checkIP rejects.
func TestSafeHTTPClient_RefusesInternalDialIP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := newSafeHTTPClient(5 * time.Second)
	_, err := client.Get(ts.URL) // ts.URL is http://127.0.0.1:<port>
	if err == nil {
		t.Fatal("expected dial to a loopback address to be refused, got nil error")
	}
}

// TestNotifyWebhook_RejectsHTTP verifies that Notify blocks http:// webhook targets.
func TestNotifyWebhook_RejectsHTTP(t *testing.T) {
	n := &Notifier{httpClient: http.DefaultClient}
	w := &Watch{
		WatchID: "w-http-reject",
		NotifyChannels: []NotifyChannel{
			{Type: "webhook", Target: "http://evil.example.com/steal"},
		},
	}
	m := &MatchResult{MatchedAt: time.Now()}
	err := n.Notify(context.Background(), w, m)
	if err == nil {
		t.Fatal("expected Notify to reject http:// webhook URL")
	}
}

func TestNotifyWebhook_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer ts.Close()

	n := &Notifier{httpClient: ts.Client()}
	w := &Watch{
		WatchID:        "w-err",
		NotifyChannels: []NotifyChannel{{Type: "webhook", Target: ts.URL}},
	}
	m := &MatchResult{InstanceType: "t3.micro", MatchedAt: time.Now()}

	err := n.Notify(context.Background(), w, m)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestNotifyNoChannels(t *testing.T) {
	n := &Notifier{}
	w := &Watch{WatchID: "w-empty"}
	m := &MatchResult{}

	err := n.Notify(context.Background(), w, m)
	if err != nil {
		t.Errorf("expected nil error for no channels, got %v", err)
	}
}

// TODO: SNS tests require Substrate emulator or mock SNS client

// ── Quota-cap notification (#153) ─────────────────────────────────────────────

// TestSendQuotaCapWebhook_Payload checks the quota-cap webhook body. Called
// directly, as TestSendWebhook_Payload does, because httptest servers are
// http://loopback and ValidateWebhookURL rightly refuses those.
func TestSendQuotaCapWebhook_Payload(t *testing.T) {
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	n := &Notifier{httpClient: ts.Client()}
	w := &Watch{WatchID: "w-cap1", InstanceTypePattern: "g6e.xlarge", DesiredCount: 5}

	if err := n.sendQuotaCapWebhook(context.Background(), ts.URL, w, 2, "fleet capped at 2/5 by the EC2 G-family On-Demand vCPU quota"); err != nil {
		t.Fatalf("sendQuotaCapWebhook: %v", err)
	}
	if received == nil {
		t.Fatal("webhook was not called")
	}
	if got := received["event"]; got != "quota_capped" {
		t.Errorf("event = %v, want quota_capped", got)
	}
	if got := received["running"].(float64); got != 2 {
		t.Errorf("running = %v, want 2", got)
	}
	if got := received["desired"].(float64); got != 5 {
		t.Errorf("desired = %v, want 5", got)
	}
	if got := received["pattern"]; got != "g6e.xlarge" {
		t.Errorf("pattern = %v, want g6e.xlarge", got)
	}
	if got, _ := received["reason"].(string); got == "" {
		t.Error("reason is empty")
	}
}

// TestMatchWebhookHasNoEventKey: the "event" key is new to the quota-cap payload
// ONLY — the existing match payload is deliberately left unchanged so no webhook
// consumer has to adapt.
func TestMatchWebhookHasNoEventKey(t *testing.T) {
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	n := &Notifier{httpClient: ts.Client()}
	w := &Watch{WatchID: "w-match", InstanceTypePattern: "g6e.xlarge"}
	m := &MatchResult{InstanceType: "g6e.xlarge", MatchedAt: time.Now()}
	if err := n.sendWebhook(context.Background(), ts.URL, w, m); err != nil {
		t.Fatalf("sendWebhook: %v", err)
	}
	if _, ok := received["event"]; ok {
		t.Error(`match payload gained an "event" key — it must stay as-is`)
	}
}

func TestQuotaCapSubjectAndBody(t *testing.T) {
	w := &Watch{
		WatchID: "w-cap2", InstanceTypePattern: "g6e.xlarge,g6e.2xlarge",
		Regions: []string{"us-east-1"}, DesiredCount: 5,
	}
	if got, want := quotaCapSubject(w, 2), "[lagotto] fleet capped at 2/5 by EC2 quota"; got != want {
		t.Errorf("quotaCapSubject = %q, want %q", got, want)
	}
	body := quotaCapBody(w, 2, "fleet capped at 2/5 by the EC2 G-family On-Demand vCPU quota in us-east-1")
	for _, want := range []string{"w-cap2", "2 of 5", "g6e.xlarge,g6e.2xlarge", "stays ACTIVE", "G-family"} {
		if !strings.Contains(body, want) {
			t.Errorf("quotaCapBody missing %q:\n%s", want, body)
		}
	}
}

// TestNotifyQuotaCap_NoChannels: a watch with no --notify is silent and errorless
// (the persisted reason + `lagotto status` is its surface).
func TestNotifyQuotaCap_NoChannels(t *testing.T) {
	n := &Notifier{}
	if err := n.NotifyQuotaCap(context.Background(), &Watch{WatchID: "w-none"}, 1, "r"); err != nil {
		t.Errorf("want nil error with no channels, got %v", err)
	}
}

// TestNotifyQuotaCap_RejectsHTTP: the same defence-in-depth webhook re-check as
// Notify, for watches stored before URL validation shipped.
func TestNotifyQuotaCap_RejectsHTTP(t *testing.T) {
	n := &Notifier{httpClient: http.DefaultClient}
	w := &Watch{
		WatchID:        "w-cap-http",
		DesiredCount:   3,
		NotifyChannels: []NotifyChannel{{Type: "webhook", Target: "http://evil.example.com/steal"}},
	}
	if err := n.NotifyQuotaCap(context.Background(), w, 1, "r"); err == nil {
		t.Fatal("expected NotifyQuotaCap to reject an http:// webhook URL")
	}
}

// TestExpiredSubjectAndBody: pure renderers for the #161 expiry notification, so
// the copy is tested without SNS (snsClient is a concrete *sns.Client).
func TestExpiredSubjectAndBody(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	w := &Watch{
		WatchID: "w-cf8e1a08", InstanceTypePattern: "g7e.*",
		Regions:   []string{"us-east-1", "us-west-2"},
		Action:    ActionSpawn,
		Status:    StatusExpired,
		CreatedAt: created,
		UpdatedAt: created.Add(48 * time.Hour),
	}
	if got, want := expiredSubject(w), "[lagotto] watch w-cf8e1a08 expired without acquiring g7e.*"; got != want {
		t.Errorf("expiredSubject = %q, want %q", got, want)
	}
	body := expiredBody(w)
	for _, want := range []string{
		"w-cf8e1a08", "g7e.*", "us-west-2", "48h0m0s",
		"Nothing was launched", "status=expired", "lagotto extend w-cf8e1a08",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expiredBody missing %q:\n%s", want, body)
		}
	}
}

// TestExpiredPayload: the machine-readable event is explicitly an expiry, never a
// match — acquired=false and event="expired", so no consumer reads a give-up as an
// acquisition.
func TestExpiredPayload(t *testing.T) {
	created := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	w := &Watch{
		WatchID: "w-p", InstanceTypePattern: "p5.*", Regions: []string{"us-east-2"},
		Action: ActionSpawn, Status: StatusExpired,
		CreatedAt: created, UpdatedAt: created.Add(90 * time.Minute),
	}
	p := expiredPayload(w)
	if p["event"] != "expired" {
		t.Errorf("event = %v, want expired", p["event"])
	}
	if p["acquired"] != false {
		t.Errorf("acquired = %v, want false", p["acquired"])
	}
	if p["time_to_give_up_seconds"] != 5400.0 {
		t.Errorf("time_to_give_up_seconds = %v, want 5400", p["time_to_give_up_seconds"])
	}
	if _, ok := p["instance_type"]; ok {
		t.Error(`expiry payload has an "instance_type" key — it must not look like a match`)
	}
}

// TestNotifyExpired_NoChannels: a watch with no --notify is silent and errorless
// (the status=expired tombstone + `lagotto history` is its surface).
func TestNotifyExpired_NoChannels(t *testing.T) {
	n := &Notifier{}
	if err := n.NotifyExpired(context.Background(), &Watch{WatchID: "w-none"}); err != nil {
		t.Errorf("want nil error with no channels, got %v", err)
	}
}

// TestNotifyExpired_RejectsHTTP: same defence-in-depth webhook re-check as Notify,
// for watches stored before URL validation shipped.
func TestNotifyExpired_RejectsHTTP(t *testing.T) {
	n := &Notifier{httpClient: &http.Client{}}
	w := &Watch{
		WatchID:        "w-bad",
		NotifyChannels: []NotifyChannel{{Type: "webhook", Target: "http://169.254.169.254/latest/meta-data/"}},
	}
	err := n.NotifyExpired(context.Background(), w)
	if err == nil || !strings.Contains(err.Error(), "blocked unsafe webhook URL") {
		t.Errorf("NotifyExpired error = %v, want blocked unsafe webhook URL", err)
	}
}

// TestSendExpiredWebhook_Payload exercises the HTTP dispatch directly (bypassing
// URL validation, since httptest servers are always http://).
func TestSendExpiredWebhook_Payload(t *testing.T) {
	var received map[string]interface{}
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	n := &Notifier{httpClient: ts.Client()}
	w := &Watch{
		WatchID: "w-hook-exp", InstanceTypePattern: "g7e.*", Status: StatusExpired,
		CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now(),
	}
	if err := n.sendExpiredWebhook(context.Background(), ts.URL, w); err != nil {
		t.Fatalf("sendExpiredWebhook: %v", err)
	}
	if received["event"] != "expired" {
		t.Errorf("event = %v, want expired", received["event"])
	}
	if received["watch_id"] != "w-hook-exp" {
		t.Errorf("watch_id = %v, want w-hook-exp", received["watch_id"])
	}
}
