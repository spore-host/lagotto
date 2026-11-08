package watcher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// ── #254: EC2 throttling is retryable, backed off per region, and cools off
// the AZ it names ──
//
// These drive the real PollAll → pollGroup → Spawner.Spawn → launchAcrossAZs
// path against a substrate-backed store, with a fake capacity search and a
// fake provision. Every cycle uses a NEW Poller over the same store, as the
// hosted poller does (one Lambda invocation per cycle), so any state that
// carries over has to have been persisted on the watches.

// azThrottleMsg is EC2's message from the 2026-11-06 event, verbatim.
const azThrottleMsg = "Request limit exceeded. Your ec2:RunInstances request has been throttled due to an " +
	"operational issue in availability zone usw2-az1. Throttling will be removed once the issue is resolved."

func throttleErr(msg string) error {
	return &smithy.GenericAPIError{Code: "RequestLimitExceeded", Message: msg, Fault: smithy.FaultClient}
}

// thHarness is a store, a fake provision (outcome decides each attempt) and a
// stepped clock shared by the per-cycle pollers.
type thHarness struct {
	t        *testing.T
	store    *Store
	results  []truffleaws.InstanceTypeResult
	outcome  func(region, az string) error
	now      time.Time
	attempts []string // "watch@az", in order, for the current cycle
}

func newTHHarness(t *testing.T, outcome func(region, az string) error) *thHarness {
	t.Helper()
	return &thHarness{
		t:     t,
		store: fleetStore(t),
		results: []truffleaws.InstanceTypeResult{{
			InstanceType: "g7e.8xlarge", Region: "us-west-2",
			AvailableAZs:  []string{"us-west-2a", "us-west-2b"},
			OnDemandPrice: 1.0, VCPUs: 32,
		}},
		outcome: outcome,
		now:     time.Now().UTC(),
	}
}

func (h *thHarness) putWatch(id string) {
	h.t.Helper()
	now := h.now
	w := &Watch{
		WatchID: id, UserID: "arn:alice", Status: StatusActive, Action: ActionSpawn,
		InstanceTypePattern: "g7e.8xlarge", Regions: []string{"us-west-2"},
		LaunchConfigJSON: mustConfigJSON(h.t),
		CreatedAt:        now, UpdatedAt: now, ExpiresAt: now.Add(30 * 24 * time.Hour),
		TTLTimestamp: now.Add(30 * 24 * time.Hour).Unix(),
	}
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		h.t.Fatal(err)
	}
}

// cycle runs one sweep at the harness clock with a fresh poller. jitter is
// identity, so a backoff is exactly ThrottleBackoff(level).
func (h *thHarness) cycle() *PollSummary {
	h.t.Helper()
	h.attempts = nil
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		h.attempts = append(h.attempts, cfg.Tags[WatchIDTagKey]+"@"+cfg.AvailabilityZone)
		if err := h.outcome(cfg.Region, cfg.AvailabilityZone); err != nil {
			return nil, err
		}
		return &spawnaws.LaunchResult{InstanceID: "i-1", AvailabilityZone: cfg.AvailabilityZone}, nil
	})
	sp.azNameForID = func(_ context.Context, region, azID string) string {
		if region == "us-west-2" && azID == "usw2-az1" {
			return "us-west-2a"
		}
		return ""
	}
	now := h.now
	p := &Poller{store: h.store, spawner: sp, truffle: &fakeSearcher{results: h.results}, now: func() time.Time { return now }}
	p.book().jitter = func(d time.Duration) time.Duration { return d }
	summary, err := p.PollAll(context.Background())
	if err != nil {
		h.t.Fatalf("PollAll: %v", err)
	}
	return summary
}

func (h *thHarness) get(id string) *Watch {
	h.t.Helper()
	w, err := h.store.GetWatch(context.Background(), id)
	if err != nil || w == nil {
		h.t.Fatalf("GetWatch(%s): %v %v", id, w, err)
	}
	return w
}

func regionThrottle(w *Watch, region string) (RegionThrottle, bool) {
	for _, r := range w.Throttle {
		if r.Region == region {
			return r, true
		}
	}
	return RegionThrottle{}, false
}

// Before #254, RequestLimitExceeded classified FailureUnknown: every throttled
// cycle bumped the consecutive-failure streak, and at MaxConsecutiveFailures
// the watch ended as spawn_failed. Throttled well past that cap, the watch
// must stay active with its streak untouched.
func TestThrottle_NeverTerminal(t *testing.T) {
	h := newTHHarness(t, func(string, string) error { return throttleErr("Request limit exceeded.") })
	h.putWatch("w-thr")

	for i := 0; i < MaxConsecutiveFailures+5; i++ {
		s := h.cycle()
		if s.Failed != 0 {
			t.Fatalf("cycle %d: Failed = %d, want 0", i, s.Failed)
		}
		h.now = h.now.Add(ThrottleBackoffMax + time.Second) // past any backoff
	}
	w := h.get("w-thr")
	if w.Status != StatusActive {
		t.Fatalf("status = %q, want active: a throttle must never end a watch", w.Status)
	}
	if w.ConsecutiveFailures != 0 {
		t.Errorf("ConsecutiveFailures = %d, want 0: a throttle is not an unknown failure", w.ConsecutiveFailures)
	}
	if ErrorClass(w.LastError) != ErrClassThrottled || !strings.Contains(w.LastError, "RequestLimitExceeded") {
		t.Errorf("LastError = %q, want class throttled with the code", w.LastError)
	}
	if w.Attempts != MaxConsecutiveFailures+5 {
		t.Errorf("Attempts = %d, want %d (one per cycle)", w.Attempts, MaxConsecutiveFailures+5)
	}
	hist, err := h.store.ListMatchHistory(context.Background(), "w-thr")
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 0 {
		t.Errorf("history = %+v, want none (no spawn_failed row)", hist)
	}
}

// A throttle that names no AZ backs the region off for every watch in the
// account: in the sweep it happened in, the other watch does not call EC2;
// across sweeps (fresh pollers) the backoff doubles each time, and only one
// watch tries per sweep.
func TestThrottle_BackoffGrowsAndIsShared(t *testing.T) {
	h := newTHHarness(t, func(string, string) error { return throttleErr("Request limit exceeded.") })
	h.putWatch("w-a")
	h.putWatch("w-b")

	// Sweep 1: one watch tries, is throttled, and stops its AZ sweep; the
	// other is held back without a call.
	s := h.cycle()
	if len(h.attempts) != 1 {
		t.Fatalf("sweep 1 attempts = %v, want exactly 1 (no AZ sweep, no second watch)", h.attempts)
	}
	if s.Throttled != 2 || s.Retrying != 2 || s.Failed != 0 {
		t.Errorf("sweep 1 summary = %+v, want 2 throttled/retrying, 0 failed", s)
	}
	start := h.now
	for _, id := range []string{"w-a", "w-b"} {
		r, ok := regionThrottle(h.get(id), "us-west-2")
		if !ok || r.Level != 1 || r.Until.Sub(r.At) != ThrottleBackoffBase {
			t.Errorf("%s throttle = %+v (%v), want level 1, %s backoff, carried by both watches", id, r, ok, ThrottleBackoffBase)
		}
	}
	var skipped *Watch
	for _, id := range []string{"w-a", "w-b"} {
		if w := h.get(id); w.Attempts == 0 {
			skipped = w
		}
	}
	if skipped == nil || ErrorClass(skipped.LastPollError) != ErrClassThrottled || !strings.Contains(skipped.LastPollError, "backing off until") {
		t.Errorf("held-back watch = %+v, want last_poll_error \"throttled: ... backing off until ...\"", skipped)
	}

	// Inside the backoff: nobody calls EC2.
	h.now = start.Add(ThrottleBackoffBase / 2)
	h.cycle()
	if len(h.attempts) != 0 {
		t.Fatalf("attempts inside the backoff = %v, want none", h.attempts)
	}

	// Each time the backoff passes, one watch tries, is throttled again, and
	// the next backoff is twice as long.
	want := ThrottleBackoffBase
	for level := 2; level <= 5; level++ {
		r, _ := regionThrottle(h.get("w-a"), "us-west-2")
		h.now = r.Until.Add(time.Second)
		h.cycle()
		if len(h.attempts) != 1 {
			t.Fatalf("level %d: attempts = %v, want exactly 1", level, h.attempts)
		}
		want *= 2
		r, _ = regionThrottle(h.get("w-b"), "us-west-2")
		if r.Level != level || r.Until.Sub(r.At) != want {
			t.Fatalf("level %d: throttle = %+v (backoff %s), want backoff %s", level, r, r.Until.Sub(r.At), want)
		}
	}
	for _, id := range []string{"w-a", "w-b"} {
		if w := h.get(id); w.Status != StatusActive {
			t.Errorf("%s status = %q, want active", id, w.Status)
		}
	}
}

// A throttle that names an AZ (usw2-az1, which is us-west-2a in this account)
// cools that AZ off: the same sweep moves on to us-west-2b, later sweeps and
// other watches skip us-west-2a, and the region is not backed off. Once the
// cool-off expires, us-west-2a is tried again.
func TestThrottle_AZCooloffSkipsNamedAZThenExpires(t *testing.T) {
	h := newTHHarness(t, func(_, az string) error {
		if az == "us-west-2a" {
			return throttleErr(azThrottleMsg)
		}
		return &capErr{"InsufficientInstanceCapacity"}
	})
	h.putWatch("w-a")

	h.cycle()
	if got := strings.Join(h.attempts, ","); got != "w-a@us-west-2a,w-a@us-west-2b" {
		t.Fatalf("sweep 1 attempts = %s, want 2a then 2b (an AZ-scoped throttle moves on, it doesn't stop)", got)
	}
	w := h.get("w-a")
	if len(w.AZCooloffs) != 1 || w.AZCooloffs[0].AZ != "us-west-2a" || w.AZCooloffs[0].AZID != "usw2-az1" ||
		w.AZCooloffs[0].Until.Sub(w.AZCooloffs[0].At) != AZCooloffDuration {
		t.Fatalf("AZCooloffs = %+v, want us-west-2a (usw2-az1) for %s", w.AZCooloffs, AZCooloffDuration)
	}
	if len(w.Throttle) != 0 {
		t.Errorf("Throttle = %+v, want none: an AZ-scoped throttle does not back the region off", w.Throttle)
	}
	if ErrorClass(w.LastError) != ErrClassAZImpaired || !strings.Contains(w.LastError, "us-west-2a (usw2-az1)") {
		t.Errorf("LastError = %q, want it to start impaired: us-west-2a (usw2-az1)", w.LastError)
	}

	// A later sweep (a new invocation), with a second watch: neither tries 2a.
	h.putWatch("w-b")
	h.now = h.now.Add(5 * time.Minute)
	h.cycle()
	for _, a := range h.attempts {
		if strings.HasSuffix(a, "@us-west-2a") {
			t.Fatalf("attempts = %v, want us-west-2a skipped by every watch while cooling off", h.attempts)
		}
	}
	if len(h.attempts) != 2 {
		t.Errorf("attempts = %v, want each watch to try us-west-2b", h.attempts)
	}

	// After the cool-off, us-west-2a is tried again.
	h.now = h.now.Add(AZCooloffDuration)
	h.cycle()
	tried2a := 0
	for _, a := range h.attempts {
		if strings.HasSuffix(a, "@us-west-2a") {
			tried2a++
		}
	}
	if tried2a == 0 {
		t.Fatalf("attempts after the cool-off = %v, want us-west-2a tried again", h.attempts)
	}
}

// Every offered AZ cooling off: the watch makes no call and waits, active.
func TestThrottle_AllAZsCooledOffSkipsWithoutCall(t *testing.T) {
	h := newTHHarness(t, func(_, az string) error {
		return throttleErr(strings.Replace(azThrottleMsg, "usw2-az1", map[string]string{"us-west-2a": "usw2-az1", "us-west-2b": "usw2-az2"}[az], 1))
	})
	h.putWatch("w-a")
	h.cycle()
	if len(h.attempts) != 2 {
		t.Fatalf("sweep 1 attempts = %v, want both AZs (each throttle names its own AZ)", h.attempts)
	}
	h.now = h.now.Add(time.Minute)
	s := h.cycle()
	if len(h.attempts) != 0 {
		t.Fatalf("attempts = %v, want none: every offered AZ is cooling off", h.attempts)
	}
	if s.Throttled != 1 {
		t.Errorf("Throttled = %d, want 1", s.Throttled)
	}
	w := h.get("w-a")
	if w.Status != StatusActive || !strings.Contains(w.LastPollError, "every offered AZ is impaired") {
		t.Errorf("watch = status %q, last_poll_error %q", w.Status, w.LastPollError)
	}
}

func TestThrottleBackoffSchedule(t *testing.T) {
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute, 10 * time.Minute}
	for i, w := range want {
		if got := ThrottleBackoff(i + 1); got != w {
			t.Errorf("ThrottleBackoff(%d) = %s, want %s", i+1, got, w)
		}
	}
	for i := 0; i < 100; i++ {
		if d := equalJitter(time.Minute); d < 30*time.Second || d > time.Minute {
			t.Fatalf("equalJitter(1m) = %s, want within [30s, 1m]", d)
		}
	}
}

// The backoff level resets after a quiet spell.
func TestThrottleBook_LevelResets(t *testing.T) {
	b := NewThrottleBook()
	b.jitter = func(d time.Duration) time.Duration { return d }
	t0 := time.Date(2026, 11, 6, 23, 22, 0, 0, time.UTC)
	if r := b.NoteRegionThrottle("us-west-2", "RequestLimitExceeded", t0); r.Level != 1 {
		t.Fatalf("level = %d", r.Level)
	}
	if r := b.NoteRegionThrottle("us-west-2", "RequestLimitExceeded", t0.Add(time.Minute)); r.Level != 2 {
		t.Fatalf("level = %d, want 2", r.Level)
	}
	if r := b.NoteRegionThrottle("us-west-2", "RequestLimitExceeded", t0.Add(time.Minute+ThrottleBackoffResetAfter)); r.Level != 1 {
		t.Fatalf("level after a quiet spell = %d, want 1", r.Level)
	}
	if _, held := b.RegionBackoff("us-east-1", t0); held {
		t.Error("an untouched region must not be backed off")
	}
}

// The #255 API: a cool-off recorded through the poller is honoured by its
// spawner and persisted on the watch.
func TestPoller_CoolOffAZ(t *testing.T) {
	h := newTHHarness(t, func(string, string) error { return &capErr{"InsufficientInstanceCapacity"} })
	h.putWatch("w-a")
	w := h.get("w-a")
	p := &Poller{store: h.store}
	c := p.CoolOffAZ(context.Background(), w, "us-west-2", "us-west-2b", "usw2-az2", "early-death")
	if c.Until.Sub(c.At) != AZCooloffDuration {
		t.Errorf("cool-off = %+v", c)
	}
	if _, ok := p.AZCooledOff("us-west-2", "us-west-2b"); !ok {
		t.Error("AZCooledOff(us-west-2b) = false")
	}
	if _, ok := p.AZCooledOff("us-west-2", "usw2-az2"); !ok {
		t.Error("AZCooledOff by zone ID = false")
	}
	if got := h.get("w-a"); len(got.AZCooloffs) != 1 || got.AZCooloffs[0].Reason != "early-death" {
		t.Errorf("persisted AZCooloffs = %+v", got.AZCooloffs)
	}
	// The next sweep (a fresh poller) skips us-west-2b.
	h.now = time.Now().UTC()
	h.cycle()
	if got := strings.Join(h.attempts, ","); got != "w-a@us-west-2a" {
		t.Errorf("attempts = %s, want only us-west-2a", got)
	}
}

// A hold is not placed in a cooling-off AZ.
func TestHolder_SkipsCooledOffAZ(t *testing.T) {
	b := NewThrottleBook()
	b.CoolOffAZ("us-west-2", "us-west-2a", "usw2-az1", "RequestLimitExceeded", time.Now().UTC())
	h := &Holder{throttle: b}
	az, _, err := h.chooseHoldAZ(context.Background(), &MatchResult{Region: "us-west-2", CandidateAZs: []string{"us-west-2a", "us-west-2b"}})
	if err != nil || az != "us-west-2b" {
		t.Fatalf("chooseHoldAZ = %q, %v; want us-west-2b", az, err)
	}
	_, _, err = h.chooseHoldAZ(context.Background(), &MatchResult{Region: "us-west-2", CandidateAZs: []string{"us-west-2a"}})
	if !IsThrottled(err) {
		t.Fatalf("all AZs cooling off: err = %v, want a throttled skip", err)
	}
}

// A throttled SageMaker submit is retried; any other submit error still ends
// the watch.
func TestSageMakerSubmitFailureKind(t *testing.T) {
	if got := submitFailureKind(&smithy.GenericAPIError{Code: "ThrottlingException", Message: "Rate exceeded"}); got != FailureThrottled {
		t.Errorf("ThrottlingException = %v, want FailureThrottled", got)
	}
	if got := submitFailureKind(&smithy.GenericAPIError{Code: "ResourceLimitExceeded"}); got != FailureTerminal {
		t.Errorf("ResourceLimitExceeded = %v, want FailureTerminal", got)
	}
}
