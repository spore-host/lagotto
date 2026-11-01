package watcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
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

// capturePublisher records every SNS publish.
type capturePublisher struct{ msgs []*sns.PublishInput }

func (c *capturePublisher) Publish(_ context.Context, in *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	c.msgs = append(c.msgs, in)
	return &sns.PublishOutput{}, nil
}

func heldMatch() (*Watch, *MatchResult) {
	w := &Watch{WatchID: "w-hold", InstanceTypePattern: "p5.48xlarge", Action: ActionHold}
	m := &MatchResult{Region: "us-east-2", AvailabilityZone: "us-east-2b", InstanceType: "p5.48xlarge",
		ActionTaken: "held", ReservationID: "cr-0abc123", MatchedAt: time.Unix(1700000000, 0)}
	return w, m
}

// TestMatchNotifications_CarryReservation is #203: every match notification
// body (the email via the default topic, a user's own SNS topic, and a
// webhook) names a fired hold's reservation id and its AZ. Email and SNS run
// through Notify itself, so this gates what is actually published, not a
// helper that a send path might stop calling.
func TestMatchNotifications_CarryReservation(t *testing.T) {
	w, m := heldMatch()
	w.NotifyChannels = []NotifyChannel{{Type: "email", Target: "a@example.com"}, {Type: "sns", Target: "arn:aws:sns:us-east-2:111122223333:mine"}}
	pub := &capturePublisher{}
	n := &Notifier{snsClient: pub, topicArn: "arn:aws:sns:us-east-2:111122223333:lagotto-capacity-alerts"}
	if err := n.Notify(context.Background(), w, m); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if len(pub.msgs) != len(w.NotifyChannels) {
		t.Fatalf("published %d messages, want %d", len(pub.msgs), len(w.NotifyChannels))
	}
	for i, in := range pub.msgs {
		body := aws.ToString(in.Message)
		for _, want := range []string{"cr-0abc123", "us-east-2b"} {
			if !strings.Contains(body, want) {
				t.Errorf("%s notification body lacks %q:\n%s", w.NotifyChannels[i].Type, want, body)
			}
		}
	}
	// The SNS channel is JSON: the id and AZ are fields, not just text.
	var snsMsg map[string]interface{}
	if err := json.Unmarshal([]byte(aws.ToString(pub.msgs[1].Message)), &snsMsg); err != nil {
		t.Fatalf("sns message is not JSON: %v", err)
	}
	if snsMsg["reservation_id"] != "cr-0abc123" || snsMsg["reservation_availability_zone"] != "us-east-2b" {
		t.Errorf("sns message reservation fields = %v / %v", snsMsg["reservation_id"], snsMsg["reservation_availability_zone"])
	}

	// Webhook: sendWebhook directly, as TestSendWebhook_Payload does
	// (httptest servers are http://, which Notify's URL validation rejects).
	var got map[string]interface{}
	posts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		posts++
		_ = json.NewDecoder(r.Body).Decode(&got)
		rw.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	wn := &Notifier{httpClient: ts.Client()}
	if err := wn.sendWebhook(context.Background(), ts.URL, w, m); err != nil {
		t.Fatalf("sendWebhook: %v", err)
	}
	if posts != 1 {
		t.Fatalf("webhook posted %d times, want 1", posts)
	}
	if got["reservation_id"] != "cr-0abc123" || got["reservation_availability_zone"] != "us-east-2b" {
		t.Errorf("webhook reservation fields = %v / %v", got["reservation_id"], got["reservation_availability_zone"])
	}
}

// TestMatchNotifications_NoReservationWithoutHold: a match that held nothing
// carries no reservation fields, so consumers can key on their presence.
func TestMatchNotifications_NoReservationWithoutHold(t *testing.T) {
	w, m := heldMatch()
	m.ReservationID, m.ActionTaken = "", "notified"
	for name, p := range map[string]map[string]interface{}{"webhook": matchWebhookPayload(w, m), "sns": matchSNSPayload(w, m)} {
		if _, ok := p["reservation_id"]; ok {
			t.Errorf("%s payload has reservation_id with no hold", name)
		}
	}
	if _, body := matchEmailSubjectBody(w, m); strings.Contains(body, "Capacity Reservation") {
		t.Errorf("email mentions a reservation with no hold:\n%s", body)
	}
}

// TestNotifyUnsupported_Content: the #195 retirement notice says the watch was
// stopped, why, and what works, and its payload can't be read as a match.
func TestNotifyUnsupported_Content(t *testing.T) {
	w := &Watch{WatchID: "w-sm", InstanceTypePattern: "ml.g5.2xlarge", Service: ServiceSageMaker, Action: ActionNotify}
	if got := unsupportedSubject(w); !strings.Contains(got, "w-sm") || !strings.Contains(got, "stopped") {
		t.Errorf("unsupportedSubject = %q", got)
	}
	body := unsupportedBody(w, sageMakerNotifyUnsupported)
	for _, want := range []string{"w-sm", "ml.g5.2xlarge", "sagemaker", "status=failed", "--action spawn", "Nothing was submitted"} {
		if !strings.Contains(body, want) {
			t.Errorf("unsupportedBody missing %q:\n%s", want, body)
		}
	}
	p := unsupportedPayload(w, sageMakerNotifyUnsupported)
	if p["event"] != "unsupported" || p["service"] != "sagemaker" || p["action"] != "notify" {
		t.Errorf("payload = %v", p)
	}
	if _, ok := p["instance_type"]; ok {
		t.Error(`retirement payload has an "instance_type" key; it must not look like a match`)
	}
}

// TestNotifyUnsupported_Channels: no channels is silent; an unsafe webhook is
// re-checked at send time, as for Notify.
func TestNotifyUnsupported_Channels(t *testing.T) {
	if err := (&Notifier{}).NotifyUnsupported(context.Background(), &Watch{WatchID: "w-none"}, "r"); err != nil {
		t.Errorf("want nil error with no channels, got %v", err)
	}
	n := &Notifier{httpClient: &http.Client{}}
	w := &Watch{WatchID: "w-bad", NotifyChannels: []NotifyChannel{{Type: "webhook", Target: "http://169.254.169.254/latest/meta-data/"}}}
	if err := n.NotifyUnsupported(context.Background(), w, "r"); err == nil || !strings.Contains(err.Error(), "blocked unsafe webhook URL") {
		t.Errorf("NotifyUnsupported error = %v, want blocked unsafe webhook URL", err)
	}
}
