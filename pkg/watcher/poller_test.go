package watcher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// pollerEnv builds a Poller backed by a substrate-mocked truffle client and a
// real DynamoDB-backed store (also substrate). Returns the store so tests can
// seed watches.
func pollerEnv(t *testing.T, verbose bool, opts ...watcher.PollerOpts) (*watcher.Poller, *watcher.Store, *testutil.TestEnv) {
	t.Helper()
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, testWatchesTable)
	env.CreateHistoryTable(t, testHistoryTable)
	store := watcher.NewStore(env.AWSConfig, testWatchesTable, testHistoryTable)
	truffle := truffleaws.NewClientFromConfig(env.AWSConfig)
	return watcher.NewPoller(truffle, store, verbose, opts...), store, env
}

func TestNewPoller(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := watcher.NewStore(env.AWSConfig, testWatchesTable, testHistoryTable)
	truffle := truffleaws.NewClientFromConfig(env.AWSConfig)

	// No opts.
	if p := watcher.NewPoller(truffle, store, false); p == nil {
		t.Fatal("NewPoller returned nil")
	}
	// With opts (notifier/spawner/holder may be nil — that's allowed).
	if p := watcher.NewPoller(truffle, store, true, watcher.PollerOpts{}); p == nil {
		t.Fatal("NewPoller(opts) returned nil")
	}
}

func TestPollAll_NoWatches(t *testing.T) {
	p, _, _ := pollerEnv(t, true)
	summary, err := p.PollAll(context.Background())
	if err != nil {
		t.Fatalf("PollAll error = %v", err)
	}
	if summary.Watched != 0 || len(summary.Matches) != 0 {
		t.Errorf("expected an empty summary with no active watches, got %+v", summary)
	}
}

func TestPollAll_WithActiveWatch(t *testing.T) {
	p, store, _ := pollerEnv(t, false)

	// Seed an active watch for a type substrate knows about.
	w := newTestWatch("w-poll01", "arn:aws:iam::123456789012:user/alice")
	w.InstanceTypePattern = "t3.micro"
	w.Spot = false
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	// PollAll groups by region/pattern and runs the full search path.
	summary, err := p.PollAll(context.Background())
	if err != nil {
		t.Fatalf("PollAll error = %v", err)
	}
	if summary.Watched != 1 {
		t.Errorf("expected 1 watched, got %d", summary.Watched)
	}
	// Substrate seeds t3.micro, so a match is expected; assert the path ran
	// and produced well-formed results (don't over-assert on emulator data).
	for _, m := range summary.Matches {
		if m.InstanceType == "" || m.Region == "" {
			t.Errorf("match has empty fields: %+v", m)
		}
	}
}

// TestPollAll_ExpiresPastTTL verifies the poller enforces the watch TTL itself
// (rather than waiting on lazy DynamoDB deletion): a still-"active" watch whose
// ExpiresAt has passed is transitioned to expired and not polled.
func TestPollAll_ExpiresPastTTL(t *testing.T) {
	p, store, _ := pollerEnv(t, false)
	ctx := context.Background()

	w := newTestWatch("w-expired", "arn:aws:iam::123456789012:user/zoe")
	w.InstanceTypePattern = "t3.micro"
	w.Spot = false
	w.ExpiresAt = time.Now().UTC().Add(-time.Hour) // already past TTL
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	summary, err := p.PollAll(ctx)
	if err != nil {
		t.Fatalf("PollAll error = %v", err)
	}
	if summary.Expired != 1 {
		t.Errorf("expected 1 expired, got %d (%+v)", summary.Expired, summary)
	}
	if summary.Watched != 0 {
		t.Errorf("expired watch should not be polled; Watched = %d", summary.Watched)
	}

	got, err := store.GetWatch(ctx, "w-expired")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Status != watcher.StatusExpired {
		t.Errorf("status = %q, want expired", got.Status)
	}
}

func TestPollWatch_Single(t *testing.T) {
	p, _, _ := pollerEnv(t, false)
	w := newTestWatch("w-pollwatch", "arn:aws:iam::123456789012:user/bob")
	w.InstanceTypePattern = "t3.micro"
	w.Spot = false

	matches, err := p.PollWatch(context.Background(), w)
	if err != nil {
		t.Fatalf("PollWatch error = %v", err)
	}
	for _, m := range matches {
		if m.InstanceType == "" {
			t.Errorf("match missing instance type: %+v", m)
		}
	}
}

func TestPollWatch_InvalidPattern(t *testing.T) {
	p, _, _ := pollerEnv(t, false)
	w := newTestWatch("w-badpattern", "arn:aws:iam::123456789012:user/carol")
	// A pattern that becomes an invalid regex after wildcard expansion.
	w.InstanceTypePattern = "[invalid("

	_, err := p.PollWatch(context.Background(), w)
	if err == nil {
		t.Error("expected error for invalid pattern, got nil")
	}
}

// --- action constructors & validation branches (no live AWS calls) ---

func TestNewHolder(t *testing.T) {
	env := testutil.SubstrateServer(t)
	if h := watcher.NewHolder(env.AWSConfig); h == nil {
		t.Fatal("NewHolder returned nil")
	}
}

func TestHolder_Hold_NoAZ(t *testing.T) {
	env := testutil.SubstrateServer(t)
	h := watcher.NewHolder(env.AWSConfig)

	w := newTestWatch("w-hold", "user")
	m := &watcher.MatchResult{InstanceType: "p5.48xlarge", Region: "us-east-1"} // no AZ
	err := h.Hold(context.Background(), w, m)
	if err == nil {
		t.Error("expected error when holding without an availability zone")
	}
}

func TestNewNotifier(t *testing.T) {
	env := testutil.SubstrateServer(t)
	if n := watcher.NewNotifier(env.AWSConfig, "arn:aws:sns:us-east-1:123456789012:topic"); n == nil {
		t.Fatal("NewNotifier returned nil")
	}
}

func TestNotifier_Notify_NoChannels(t *testing.T) {
	env := testutil.SubstrateServer(t)
	n := watcher.NewNotifier(env.AWSConfig, "")
	w := newTestWatch("w-nochan", "user") // no NotifyChannels
	m := &watcher.MatchResult{InstanceType: "g5.xlarge", Region: "us-east-1"}
	if err := n.Notify(context.Background(), w, m); err != nil {
		t.Errorf("Notify with no channels should be a no-op, got %v", err)
	}
}

func TestNotifier_Notify_SNSChannel(t *testing.T) {
	env := testutil.SubstrateServer(t)
	n := watcher.NewNotifier(env.AWSConfig, "")

	w := newTestWatch("w-sns", "user")
	w.NotifyChannels = []watcher.NotifyChannel{
		{Type: "sns", Target: "arn:aws:sns:us-east-1:123456789012:lagotto-matches"},
	}
	m := &watcher.MatchResult{
		WatchID: w.WatchID, InstanceType: "g5.xlarge", Region: "us-east-1",
		Price: 0.50, IsSpot: true, MatchedAt: time.Now().UTC(), ActionTaken: "notify",
	}
	// Exercises the sendSNS code path. Substrate may accept or reject the
	// Publish; either way the path runs. We only require it not to panic.
	_ = n.Notify(context.Background(), w, m)
}

func TestNotifier_Notify_UnknownChannelType(t *testing.T) {
	env := testutil.SubstrateServer(t)
	n := watcher.NewNotifier(env.AWSConfig, "")
	w := newTestWatch("w-unknown", "user")
	w.NotifyChannels = []watcher.NotifyChannel{{Type: "carrier-pigeon", Target: "x"}}
	m := &watcher.MatchResult{InstanceType: "g5.xlarge", Region: "us-east-1"}
	// Unknown channel types are skipped (no error).
	if err := n.Notify(context.Background(), w, m); err != nil {
		t.Errorf("unknown channel type should be skipped, got %v", err)
	}
}

func TestSpawner_Spawn_NoConfig(t *testing.T) {
	env := testutil.SubstrateServer(t)
	s, err := watcher.NewSpawner(contextWithEnv(env))
	if err != nil {
		t.Skipf("NewSpawner unavailable in this environment: %v", err)
	}
	w := newTestWatch("w-spawn", "user") // no LaunchConfigJSON
	m := &watcher.MatchResult{InstanceType: "g5.xlarge", Region: "us-east-1"}
	if err := s.Spawn(context.Background(), w, m); err == nil {
		t.Error("expected error spawning with no launch config")
	}
}

func TestSpawner_Spawn_BadJSON(t *testing.T) {
	env := testutil.SubstrateServer(t)
	s, err := watcher.NewSpawner(contextWithEnv(env))
	if err != nil {
		t.Skipf("NewSpawner unavailable: %v", err)
	}
	w := newTestWatch("w-spawn-bad", "user")
	w.LaunchConfigJSON = json.RawMessage(`{not valid json`)
	m := &watcher.MatchResult{InstanceType: "g5.xlarge", Region: "us-east-1"}
	if err := s.Spawn(context.Background(), w, m); err == nil {
		t.Error("expected error spawning with malformed launch config JSON")
	}
}

// --- store: match-history-by-user (uncovered store method) ---

func TestListMatchHistoryByUser(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	userID := "arn:aws:iam::123456789012:user/dave"

	w := newTestWatch("w-hist", userID)
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	m := &watcher.MatchResult{
		WatchID:      w.WatchID,
		UserID:       userID,
		InstanceType: "g5.xlarge",
		Region:       "us-east-1",
		MatchedAt:    time.Now().UTC(),
	}
	if err := store.RecordMatch(ctx, w, m); err != nil {
		t.Fatalf("RecordMatch: %v", err)
	}

	hist, err := store.ListMatchHistoryByUser(ctx, userID)
	if err != nil {
		t.Fatalf("ListMatchHistoryByUser: %v", err)
	}
	if len(hist) == 0 {
		t.Error("expected at least one match in user history")
	}
}

// contextWithEnv returns a context; NewSpawner loads its own AWS config from
// the default chain, so we cannot inject the substrate endpoint here. The
// spawn tests above are guarded with Skip when the client can't initialize.
func contextWithEnv(_ *testutil.TestEnv) context.Context {
	return context.Background()
}
