package watcher

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/testutil"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// hookURL is a public TEST-NET-1 literal, so it passes ValidateWebhookURL;
// routedHTTPClient sends every request for it to the local test server.
const hookURL = "https://192.0.2.10/hook"

// routedHTTPClient is ts's TLS client with every dial sent to ts, whatever
// the URL's host. The notifier's real client refuses loopback (#40), and an
// httptest server is always loopback.
func routedHTTPClient(ts *httptest.Server) *http.Client {
	tr := ts.Client().Transport.(*http.Transport).Clone()
	addr := ts.Listener.Addr().String()
	// The test certificate names 127.0.0.1, not the URL's host.
	tr.TLSClientConfig = tr.TLSClientConfig.Clone()
	tr.TLSClientConfig.ServerName = "127.0.0.1"
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: tr}
}

func hookServer(t *testing.T) (*httptest.Server, *int32) {
	t.Helper()
	var posts int32
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			atomic.AddInt32(&posts, 1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	return ts, &posts
}

func notifyWatch(id string, channels ...NotifyChannel) *Watch {
	now := time.Now().UTC()
	return &Watch{
		WatchID:             id,
		UserID:              "arn:alice",
		Status:              StatusActive,
		Action:              ActionNotify,
		InstanceTypePattern: "m8g.8xlarge",
		Regions:             []string{"us-east-1"},
		NotifyChannels:      channels,
		CreatedAt:           now.Add(-time.Hour),
		UpdatedAt:           now.Add(-time.Hour),
		ExpiresAt:           now.Add(23 * time.Hour),
		TTLTimestamp:        now.Add(23 * time.Hour).Unix(),
	}
}

// TestPollAll_WebhookDeliveredWithoutAlertsTopic is the #241 behaviour gate.
// A local `lagotto poll` with no alerts topic builds NewNotifier(cfg, "")
// (cmd/poll_notify.go); a match on a webhook watch must reach the webhook.
// Before #241 the CLI built no notifier at all without LAGOTTO_SNS_TOPIC_ARN,
// and this watch was consumed (status=matched) with zero POSTs.
func TestPollAll_WebhookDeliveredWithoutAlertsTopic(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, "t241-watches")
	env.CreateHistoryTable(t, "t241-history")
	store := NewStore(env.AWSConfig, "t241-watches", "t241-history")
	ctx := context.Background()

	w := notifyWatch("w-hook-241", NotifyChannel{Type: "webhook", Target: hookURL})
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	ts, posts := hookServer(t)
	pub := &capturePublisher{}
	n := NewNotifier(env.AWSConfig, "") // what the CLI builds with no topic
	n.httpClient = routedHTTPClient(ts)
	n.snsClient = pub

	p := NewPoller(nil, store, false, PollerOpts{Notifier: n})
	p.truffle = &fakeSearcher{results: []truffleaws.InstanceTypeResult{
		{InstanceType: "m8g.8xlarge", Region: "us-east-1", AvailableAZs: []string{"us-east-1a"}, OnDemandPrice: 1.2, VCPUs: 32},
	}}

	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	if len(summary.Matches) != 1 {
		t.Fatalf("matches = %d, want 1", len(summary.Matches))
	}
	if got := atomic.LoadInt32(posts); got != 1 {
		t.Errorf("webhook POSTs = %d, want 1: a webhook needs no alerts topic (#241)", got)
	}
	if len(pub.msgs) != 0 {
		t.Errorf("SNS publishes = %d, want 0 for a webhook-only watch", len(pub.msgs))
	}
}

// TestNotify_NoTopic_EmailRefusedOthersDelivered: with no alerts topic, the
// email channel returns ErrNoAlertsTopic and makes no Publish (never one with
// an empty TopicArn), while the webhook and the user's own sns topic on the
// same watch are still delivered.
func TestNotify_NoTopic_EmailRefusedOthersDelivered(t *testing.T) {
	ts, posts := hookServer(t)
	pub := &capturePublisher{}
	n := NewNotifier(aws.Config{}, "")
	n.httpClient = routedHTTPClient(ts)
	n.snsClient = pub
	if n.HasAlertsTopic() {
		t.Fatal("HasAlertsTopic() = true with no topic")
	}

	own := "arn:aws:sns:us-east-1:111122223333:my-own-topic"
	w := notifyWatch("w-mixed",
		NotifyChannel{Type: "email", Target: "alice@example.com"},
		NotifyChannel{Type: "webhook", Target: hookURL},
		NotifyChannel{Type: "sns", Target: own},
	)
	m := &MatchResult{InstanceType: "m8g.8xlarge", Region: "us-east-1", MatchedAt: time.Now()}

	// Every notification kind, not just the match: each has its own email case.
	sends := map[string]func() error{
		"match":       func() error { return n.Notify(context.Background(), w, m) },
		"quota cap":   func() error { return n.NotifyQuotaCap(context.Background(), w, 1, "capped") },
		"expired":     func() error { return n.NotifyExpired(context.Background(), w) },
		"unsupported": func() error { return n.NotifyUnsupported(context.Background(), w, "retired") },
		"reminder": func() error {
			return n.NotifyReminder(context.Background(), w, ReminderStage{Stage: 1, Of: 3}, time.Now())
		},
	}
	for name, send := range sends {
		pub.msgs = nil
		before := atomic.LoadInt32(posts)
		if err := send(); !errors.Is(err, ErrNoAlertsTopic) {
			t.Errorf("%s: err = %v, want ErrNoAlertsTopic", name, err)
		}
		if got := atomic.LoadInt32(posts) - before; got != 1 {
			t.Errorf("%s: webhook POSTs = %d, want 1", name, got)
		}
		if len(pub.msgs) != 1 || aws.ToString(pub.msgs[0].TopicArn) != own {
			t.Errorf("%s: publishes = %+v, want exactly one, to the watch's own topic %s", name, pub.msgs, own)
		}
	}
}

// TestNotify_WithTopic_PublishesEmailAndSNS: with a topic, the email channel
// is published to it and the sns channel to its own target.
func TestNotify_WithTopic_PublishesEmailAndSNS(t *testing.T) {
	pub := &capturePublisher{}
	alerts := "arn:aws:sns:us-east-1:111122223333:lagotto-capacity-alerts"
	own := "arn:aws:sns:us-east-1:111122223333:my-own-topic"
	n := NewNotifier(aws.Config{}, alerts)
	n.snsClient = pub
	if !n.HasAlertsTopic() {
		t.Fatal("HasAlertsTopic() = false with a topic")
	}

	w := notifyWatch("w-topic",
		NotifyChannel{Type: "email", Target: "alice@example.com"},
		NotifyChannel{Type: "sns", Target: own},
	)
	m := &MatchResult{InstanceType: "m8g.8xlarge", Region: "us-east-1", MatchedAt: time.Now()}
	if err := n.Notify(context.Background(), w, m); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("publishes = %d, want 2", len(pub.msgs))
	}
	if got := aws.ToString(pub.msgs[0].TopicArn); got != alerts {
		t.Errorf("email published to %q, want %q", got, alerts)
	}
	if aws.ToString(pub.msgs[0].Subject) == "" {
		t.Error("email publish has no subject")
	}
	if got := aws.ToString(pub.msgs[1].TopicArn); got != own {
		t.Errorf("sns published to %q, want %q", got, own)
	}
}

func TestNeedsAlertsTopic(t *testing.T) {
	for _, tc := range []struct {
		chans []NotifyChannel
		want  bool
	}{
		{nil, false},
		{[]NotifyChannel{{Type: "webhook", Target: hookURL}}, false},
		{[]NotifyChannel{{Type: "sns", Target: "arn:aws:sns:us-east-1:1:t"}}, false},
		{[]NotifyChannel{{Type: "webhook", Target: hookURL}, {Type: "email", Target: "a@b.c"}}, true},
	} {
		if got := NeedsAlertsTopic(&Watch{NotifyChannels: tc.chans}); got != tc.want {
			t.Errorf("NeedsAlertsTopic(%+v) = %v, want %v", tc.chans, got, tc.want)
		}
	}
}
