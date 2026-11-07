package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/spore-host/lagotto/pkg/testutil"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// ── #239 phase 2: two-phase landing ──
//
// These drive the real PollAll → pollLanding / pollGroup → Spawner.Spawn path
// against a substrate-backed store, with a fake capacity search, a fake
// provision (RunInstances), and a fake describe whose answer the test sets per
// cycle. Each cycle reloads every watch from the store, as the stateless hosted
// poller does.

const landRegion = "us-east-2"

type landHarness struct {
	store      *Store
	poller     *Poller
	launches   []string // instance ids, in order
	attempted  []string // the AZ each launch attempted
	placeAZ    string   // the AZ the fake RunInstances "places" the instance in
	describe   func(id string) (landingCheck, error)
	described  []string
	terminated []string
	termErr    error
	notified   []MatchResult
}

func newLandHarness(t *testing.T) *landHarness {
	t.Helper()
	h := &landHarness{store: fleetStore(t), placeAZ: landRegion + "c"}
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "pending"}, nil }
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		id := fmt.Sprintf("i-%d", len(h.launches)+1)
		h.launches = append(h.launches, id)
		h.attempted = append(h.attempted, cfg.AvailabilityZone)
		return &spawnaws.LaunchResult{InstanceID: id, AvailabilityZone: h.placeAZ, State: "pending"}, nil
	})
	sp.describeInstance = func(_ context.Context, region, id string) (landingCheck, error) {
		if region != landRegion {
			t.Errorf("describe in %q, want %q", region, landRegion)
		}
		h.described = append(h.described, id)
		return h.describe(id)
	}
	sp.terminateInstance = func(_ context.Context, region, id string) error {
		if h.termErr != nil {
			return h.termErr
		}
		h.terminated = append(h.terminated, region+"/"+id)
		return nil
	}
	h.poller = &Poller{
		store: h.store, spawner: sp, leaseOwner: "hosted-lambda",
		truffle: &fakeSearcher{results: qiResults(qiCandidate{"m7g.medium", landRegion, 0.04, 1})},
	}
	h.poller.notifyMatch = func(_ context.Context, _ *Watch, m *MatchResult) error {
		h.notified = append(h.notified, *m)
		return nil
	}
	h.poller.notifyExpired = func(context.Context, *Watch) error { return nil }
	return h
}

func landWatch(t *testing.T, id string) *Watch {
	t.Helper()
	now := time.Now().UTC()
	return &Watch{
		WatchID: id, UserID: "arn:alice", Status: StatusActive, Action: ActionSpawn,
		InstanceTypePattern: "m7g.medium", Regions: []string{landRegion},
		LaunchConfigJSON: mustConfigJSON(t),
		CreatedAt:        now, UpdatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
		TTLTimestamp: RetentionTTL(now.Add(24 * time.Hour)),
	}
}

func (h *landHarness) put(t *testing.T, w *Watch) {
	t.Helper()
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
}

func (h *landHarness) cycle(t *testing.T) *PollSummary {
	t.Helper()
	s, err := h.poller.PollAll(context.Background())
	if err != nil {
		t.Fatalf("PollAll: %v", err)
	}
	return s
}

func (h *landHarness) get(t *testing.T, id string) *Watch {
	t.Helper()
	w, err := h.store.GetWatch(context.Background(), id)
	if err != nil || w == nil {
		t.Fatalf("GetWatch(%s): %v %v", id, w, err)
	}
	return w
}

func (h *landHarness) history(t *testing.T, id string) []MatchResult {
	t.Helper()
	ms, err := h.store.ListMatchHistory(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// backdate moves a landing watch's pending_since into the past, as if the
// launch had happened d ago.
func (h *landHarness) backdate(t *testing.T, id string, d time.Duration) {
	t.Helper()
	_, err := h.store.client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        &h.store.watchesTable,
		Key:              map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: id}},
		UpdateExpression: aws.String("SET pending_since = :t"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: time.Now().UTC().Add(-d).Format(time.RFC3339)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// After RunInstances the watch is landing, not matched: active, pending_*
// set, the lease marker held, nothing in history, nobody notified.
func assertLanding(t *testing.T, w *Watch, instanceID string) {
	t.Helper()
	if w.Status != StatusActive {
		t.Errorf("status = %q, want active while landing", w.Status)
	}
	if w.PendingInstanceID != instanceID || w.PendingRegion != landRegion || w.PendingSince == nil {
		t.Errorf("pending = %q/%q/%v, want %s in %s", w.PendingInstanceID, w.PendingRegion, w.PendingSince, instanceID, landRegion)
	}
	if w.LeaseOwner != LandingLeasePrefix+instanceID || !w.LeaseExpiresAt.After(time.Now()) {
		t.Errorf("lease = %q until %v, want the live landing marker", w.LeaseOwner, w.LeaseExpiresAt)
	}
	if w.PendingMatch == nil || w.PendingMatch.InstanceType != "m7g.medium" {
		t.Errorf("pending_match = %+v", w.PendingMatch)
	}
}

func TestLanding_RunningPathMatchesWithTrueAZ(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-run"))

	s := h.cycle(t)
	if s.Landing != 1 || s.Launched != 0 || len(h.launches) != 1 {
		t.Fatalf("cycle 1: landing=%d launched=%d launches=%v", s.Landing, s.Launched, h.launches)
	}
	w := h.get(t, "w-run")
	assertLanding(t, w, "i-1")
	if w.Attempts != 1 || w.ConsecutiveFailures != 0 {
		t.Errorf("attempts=%d streak=%d, want 1, 0", w.Attempts, w.ConsecutiveFailures)
	}
	if n := len(h.history(t, "w-run")); n != 0 {
		t.Errorf("history rows after launch = %d, want 0 (not landed yet)", n)
	}
	if len(h.notified) != 0 {
		t.Errorf("notified at launch: %v", h.notified)
	}

	h.describe = func(string) (landingCheck, error) {
		return landingCheck{State: "running", AZ: landRegion + "c"}, nil
	}
	s = h.cycle(t)
	if s.Launched != 1 || len(h.launches) != 1 {
		t.Fatalf("cycle 2: launched=%d launches=%v, want 1 and no second launch", s.Launched, h.launches)
	}
	w = h.get(t, "w-run")
	if w.Status != StatusMatched {
		t.Errorf("status = %q, want matched", w.Status)
	}
	if w.Landing() || w.PendingMatch != nil || w.LeaseOwner != "" {
		t.Errorf("landing state left behind: %q %q %+v lease=%q", w.PendingInstanceID, w.PendingReservationID, w.PendingMatch, w.LeaseOwner)
	}
	hist := h.history(t, "w-run")
	if len(hist) != 1 {
		t.Fatalf("history = %+v, want one row", hist)
	}
	m := hist[0]
	if m.ActionTaken != "spawned" || m.InstanceID != "i-1" || m.AvailabilityZone != landRegion+"c" ||
		m.InstanceState != "running" || m.LandedAt.IsZero() {
		t.Errorf("history row = %+v, want spawned i-1 in %sc, running, landed_at set", m, landRegion)
	}
	if !m.LandedAt.After(m.MatchedAt) && !m.LandedAt.Equal(m.MatchedAt) {
		t.Errorf("landed_at %v before matched_at %v", m.LandedAt, m.MatchedAt)
	}
	if len(h.notified) != 1 || h.notified[0].InstanceID != "i-1" {
		t.Errorf("notified = %+v, want once for i-1", h.notified)
	}
	if w.LastMatch == nil || w.LastMatch.AvailabilityZone != landRegion+"c" {
		t.Errorf("last_match = %+v", w.LastMatch)
	}
}

// The double-spend guard: while an instance is pending, cycles check it and
// never launch again.
func TestLanding_NoSecondLaunchWhilePending(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-guard"))
	for i := 0; i < 4; i++ {
		s := h.cycle(t)
		if s.Landing != 1 {
			t.Errorf("cycle %d: landing = %d, want 1", i+1, s.Landing)
		}
	}
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v, want exactly one while it is pending", h.launches)
	}
	if len(h.described) != 3 {
		t.Errorf("described = %v, want a check on each of the 3 later cycles", h.described)
	}
	w := h.get(t, "w-guard")
	assertLanding(t, w, "i-1")
	if w.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (checks are not attempts)", w.Attempts)
	}
}

func TestLanding_TerminatedResumesInSameCycle(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-ice"))
	h.cycle(t)

	h.describe = func(id string) (landingCheck, error) {
		if id != "i-1" {
			return landingCheck{State: "pending"}, nil
		}
		return landingCheck{State: "terminated", AZ: landRegion + "c",
			ReasonCode: "Server.InsufficientInstanceCapacity", ReasonMessage: "Insufficient capacity."}, nil
	}
	s := h.cycle(t)
	if len(h.launches) != 2 {
		t.Fatalf("launches = %v, want a relaunch in the same cycle", h.launches)
	}
	if s.Landing != 1 || s.Watched != 1 {
		t.Errorf("summary = %+v", s)
	}
	w := h.get(t, "w-ice")
	assertLanding(t, w, "i-2")
	if !strings.HasPrefix(w.LastError, "capacity: Server.InsufficientInstanceCapacity: instance i-1 terminated before it landed") {
		t.Errorf("last_error = %q", w.LastError)
	}
	if w.ConsecutiveFailures != 0 {
		t.Errorf("streak = %d, want 0 for a capacity landing failure", w.ConsecutiveFailures)
	}
	if w.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one per launch)", w.Attempts)
	}
	hist := h.history(t, "w-ice")
	if len(hist) != 1 || hist[0].ActionTaken != ActionLandingFailed || hist[0].InstanceID != "i-1" ||
		hist[0].InstanceState != "terminated" || !strings.Contains(hist[0].LandingReason, "Server.InsufficientInstanceCapacity") {
		t.Errorf("history = %+v, want one landing_failed row for i-1", hist)
	}
	if len(h.notified) != 0 {
		t.Errorf("notified = %+v, want none", h.notified)
	}
}

// A non-capacity landing failure (a fail-closed bootstrap's shutdown) counts
// toward the unclassified-failure stop.
func TestLanding_BootstrapShutdownCountsTowardStop(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-boot"))
	h.cycle(t)
	h.describe = func(string) (landingCheck, error) {
		return landingCheck{State: "shutting-down", ReasonCode: "Client.InstanceInitiatedShutdown",
			ReasonMessage: "Client.InstanceInitiatedShutdown: Instance initiated shutdown"}, nil
	}
	h.cycle(t)
	got := h.get(t, "w-boot")
	if got.ConsecutiveFailures != 1 || got.Status != StatusActive {
		t.Fatalf("streak=%d status=%q, want 1, active", got.ConsecutiveFailures, got.Status)
	}
	if !strings.HasPrefix(got.LastError, "unknown: Client.InstanceInitiatedShutdown") {
		t.Errorf("last_error = %q", got.LastError)
	}

	// A launch is not a success until it lands, so the relaunch did not reset
	// the streak: launches that keep dying this way reach the cap. Run them out.
	for i := 1; i < MaxConsecutiveFailures; i++ {
		h.cycle(t)
	}
	got = h.get(t, "w-boot")
	if got.Status != StatusFailed {
		t.Errorf("status = %q, want failed at the cap", got.Status)
	}
	if len(h.notified) != 1 || h.notified[0].ActionTaken != ActionLandingFailed {
		t.Errorf("notified = %+v, want the final failure once", h.notified)
	}
	if got.Landing() {
		t.Errorf("a failed watch is still landing %s", got.PendingResourceID())
	}
}

func TestLanding_StuckPendingTerminatesThenResumes(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-stuck"))
	h.cycle(t)
	h.backdate(t, "w-stuck", PendingLandingTimeout+time.Minute)

	h.cycle(t)
	if len(h.terminated) != 1 || h.terminated[0] != landRegion+"/i-1" {
		t.Fatalf("terminated = %v, want i-1 through spawn's terminate path", h.terminated)
	}
	if len(h.launches) != 2 {
		t.Fatalf("launches = %v, want a relaunch after the terminate", h.launches)
	}
	w := h.get(t, "w-stuck")
	assertLanding(t, w, "i-2")
	if !strings.HasPrefix(w.LastError, "unknown: instance i-1 did not land: still pending after 11m") ||
		!strings.Contains(w.LastError, "lagotto terminated it") {
		t.Errorf("last_error = %q", w.LastError)
	}
	if w.ConsecutiveFailures != 1 {
		t.Errorf("streak = %d, want 1", w.ConsecutiveFailures)
	}
	hist := h.history(t, "w-stuck")
	if len(hist) != 1 || hist[0].ActionTaken != ActionLandingTimedOut || hist[0].InstanceState != "pending" {
		t.Errorf("history = %+v, want one landing_timed_out row", hist)
	}
}

// A stuck instance that cannot be terminated (e.g. a runtime role without the
// #242 grant) keeps the watch landing: it is never left running beside a
// second launch.
func TestLanding_StuckTerminateFailsStaysLanding(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-stuck-deny"))
	h.cycle(t)
	h.backdate(t, "w-stuck-deny", PendingLandingTimeout+time.Minute)
	h.termErr = &capErr{"UnauthorizedOperation"}

	h.cycle(t)
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v, want no relaunch while i-1 may still land", h.launches)
	}
	w := h.get(t, "w-stuck-deny")
	if w.PendingInstanceID != "i-1" || w.LeaseOwner != LandingLeasePrefix+"i-1" {
		t.Errorf("pending=%q lease=%q, want still landing i-1", w.PendingInstanceID, w.LeaseOwner)
	}
	if !strings.Contains(w.LastError, "giving up on it failed") || !strings.Contains(w.LastError, "UnauthorizedOperation") {
		t.Errorf("last_error = %q", w.LastError)
	}
}

func TestLanding_DescribeErrorStaysLanding(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-derr"))
	h.cycle(t)
	h.describe = func(string) (landingCheck, error) { return landingCheck{}, errors.New("throttled") }
	h.backdate(t, "w-derr", PendingLandingTimeout+time.Minute)

	h.cycle(t)
	if len(h.launches) != 1 || len(h.terminated) != 0 {
		t.Fatalf("launches=%v terminated=%v: an unknown state must neither relaunch nor terminate", h.launches, h.terminated)
	}
	w := h.get(t, "w-derr")
	if w.PendingInstanceID != "i-1" || !strings.Contains(w.LastError, "landing check for i-1: throttled") {
		t.Errorf("pending=%q last_error=%q", w.PendingInstanceID, w.LastError)
	}
}

func TestLanding_NotFoundGraceThenGone(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-nf"))
	h.cycle(t)
	h.describe = func(id string) (landingCheck, error) {
		if id == "i-1" {
			return landingCheck{NotFound: true}, nil
		}
		return landingCheck{State: "pending"}, nil
	}
	h.cycle(t) // young: eventual consistency, still pending
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v, want none while not-found is within the grace", h.launches)
	}
	h.backdate(t, "w-nf", landingNotFoundGrace+time.Minute)
	h.cycle(t)
	if len(h.launches) != 2 {
		t.Fatalf("launches = %v, want a relaunch once not-found is past the grace", h.launches)
	}
	if hist := h.history(t, "w-nf"); len(hist) != 1 || hist[0].InstanceState != "not-found" {
		t.Errorf("history = %+v", hist)
	}
}

// The #47 lease: a live lease held by another poller (not the landing marker)
// means this poller does not check, let alone resolve, the landing.
func TestLanding_RespectsAnotherPollersLease(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-lease"))
	h.cycle(t)
	if err := h.store.ClaimLease(context.Background(), "w-lease", "cli:bob", time.Now().Add(time.Minute)); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("an old poller's ClaimLease over the marker = %v, want ErrLeaseHeld", err)
	}
	// Force a live foreign lease (as if bob's poller had claimed it first).
	w := h.get(t, "w-lease")
	w.LeaseOwner, w.LeaseExpiresAt = "cli:bob", time.Now().UTC().Add(time.Minute)
	h.put(t, w)
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running"}, nil }

	h.cycle(t)
	if len(h.described) != 0 {
		t.Errorf("described = %v, want no check under another poller's lease", h.described)
	}
	if got := h.get(t, "w-lease"); got.Status != StatusActive || got.PendingInstanceID != "i-1" {
		t.Errorf("status=%q pending=%q", got.Status, got.PendingInstanceID)
	}
}

// An old (pre-#239) poller ignores pending_*, so it would treat a landing watch
// as hunting: search, find capacity, and launch. pollGroup is exactly that
// path, unchanged since #47, so running it on a landing watch is running the
// old poller. The lease marker makes its ClaimLease fail, so it launches
// nothing.
func TestLanding_OldPollerCannotRelaunch(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-old"))
	h.cycle(t)

	old := &Poller{store: h.store, spawner: h.poller.spawner, truffle: h.poller.truffle, leaseOwner: "hosted-lambda"}
	w := h.get(t, "w-old")
	s := &PollSummary{}
	if err := old.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, w.Spot, []*Watch{w}, s); err != nil {
		t.Fatal(err)
	}
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v: an old poller launched a second instance beside the landing one", h.launches)
	}
	if s.Retrying != 1 {
		t.Errorf("old poller summary = %+v, want the lease-held skip", s)
	}
	assertLanding(t, h.get(t, "w-old"), "i-1")
}

// A watch whose deadline passes while landing: running still counts; pending is
// terminated and the watch expires.
func TestLanding_ExpiryWhileLanding(t *testing.T) {
	expire := func(t *testing.T, h *landHarness, id string) {
		t.Helper()
		w := h.get(t, id)
		w.ExpiresAt = time.Now().UTC().Add(-time.Minute)
		h.put(t, w)
	}

	t.Run("running wins", func(t *testing.T) {
		h := newLandHarness(t)
		h.put(t, landWatch(t, "w-exp-run"))
		h.cycle(t)
		expire(t, h, "w-exp-run")
		h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running", AZ: landRegion + "c"}, nil }
		h.cycle(t)
		if got := h.get(t, "w-exp-run"); got.Status != StatusMatched {
			t.Errorf("status = %q, want matched", got.Status)
		}
	})

	t.Run("pending is terminated and the watch expires", func(t *testing.T) {
		h := newLandHarness(t)
		h.put(t, landWatch(t, "w-exp-pend"))
		h.cycle(t)
		expire(t, h, "w-exp-pend")
		s := h.cycle(t)
		if len(h.terminated) != 1 || len(h.launches) != 1 {
			t.Fatalf("terminated=%v launches=%v", h.terminated, h.launches)
		}
		got := h.get(t, "w-exp-pend")
		if got.Status != StatusExpired || got.Landing() || s.Expired != 1 {
			t.Errorf("status=%q landing=%v expired=%d", got.Status, got.Landing(), s.Expired)
		}
	})

	t.Run("unknown state expires without terminating", func(t *testing.T) {
		h := newLandHarness(t)
		h.put(t, landWatch(t, "w-exp-unk"))
		h.cycle(t)
		expire(t, h, "w-exp-unk")
		h.describe = func(string) (landingCheck, error) { return landingCheck{}, errors.New("boom") }
		h.cycle(t)
		got := h.get(t, "w-exp-unk")
		if got.Status != StatusExpired || len(h.terminated) != 0 {
			t.Errorf("status=%q terminated=%v", got.Status, h.terminated)
		}
	})
}

// Old records: a watch stored before #239 has no pending_* attributes and is
// hunted exactly as before; a zero Watch marshals none of them.
func TestLanding_OldRecordCompatible(t *testing.T) {
	item, err := attributevalue.MarshalMap(landWatch(t, "w-plain"))
	if err != nil {
		t.Fatal(err)
	}
	for k := range item {
		if strings.HasPrefix(k, "pending_") {
			t.Errorf("a non-landing watch marshals %s", k)
		}
	}
	h := newLandHarness(t)
	// Written raw, as a pre-#239 CLI would: no pending attributes at all.
	if _, err := h.store.client.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: &h.store.watchesTable, Item: item}); err != nil {
		t.Fatal(err)
	}
	w := h.get(t, "w-plain")
	if w.Landing() {
		t.Fatalf("an old record reads as landing: %+v", w)
	}
	h.cycle(t)
	if len(h.launches) != 1 {
		t.Errorf("launches = %v, want the old record hunted as before", h.launches)
	}
}

// Without a describe seam (no landing check wired) the launch is matched at
// once, as before #239, and an instance RunInstances already reports running is
// matched at once too, with landed_at.
func TestLanding_ImmediateMatchCases(t *testing.T) {
	t.Run("no describer", func(t *testing.T) {
		h := newLandHarness(t)
		h.poller.spawner.describeInstance = nil
		h.put(t, landWatch(t, "w-legacy"))
		h.cycle(t)
		if got := h.get(t, "w-legacy"); got.Status != StatusMatched || got.Landing() {
			t.Errorf("status=%q landing=%v, want matched at once", got.Status, got.Landing())
		}
	})
	t.Run("running on return", func(t *testing.T) {
		h := newLandHarness(t)
		h.poller.spawner.provision = func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
			h.launches = append(h.launches, "i-1")
			return &spawnaws.LaunchResult{InstanceID: "i-1", AvailabilityZone: landRegion + "b", State: "running"}, nil
		}
		h.put(t, landWatch(t, "w-fast"))
		h.cycle(t)
		got := h.get(t, "w-fast")
		if got.Status != StatusMatched || got.LastMatch == nil || got.LastMatch.LandedAt.IsZero() ||
			got.LastMatch.AvailabilityZone != landRegion+"b" {
			t.Errorf("status=%q last_match=%+v", got.Status, got.LastMatch)
		}
	})
}

// The recorded AZ is the one EC2 placed the instance in, not the attempt: an
// unpinned attempt ("") used to record "".
func TestSpawn_RecordsPlacedAZNotAttempted(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidates []string
		placed     string
		want       string
	}{
		{"unpinned", nil, "us-east-2b", "us-east-2b"},
		{"pinned", []string{"us-east-2a"}, "us-east-2a", "us-east-2a"},
		{"no placement reported", []string{"us-east-2a"}, "", "us-east-2a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
				return &spawnaws.LaunchResult{InstanceID: "i-az", AvailabilityZone: tc.placed, State: "pending"}, nil
			})
			m := &MatchResult{Region: "us-east-2", InstanceType: "m7g.medium", CandidateAZs: tc.candidates}
			if err := sp.Spawn(context.Background(), &Watch{WatchID: "w-az", LaunchConfigJSON: mustConfigJSON(t)}, m); err != nil {
				t.Fatal(err)
			}
			if m.AvailabilityZone != tc.want || m.InstanceState != "pending" {
				t.Errorf("az=%q state=%q, want %q pending", m.AvailabilityZone, m.InstanceState, tc.want)
			}
		})
	}
}

// ── holds ──

type fakeHoldEC2 struct {
	createState ec2types.CapacityReservationState
	created     []string
	states      map[string]ec2types.CapacityReservationState
	cancelled   []string
	describeErr error
}

func (f *fakeHoldEC2) CreateCapacityReservation(_ context.Context, in *ec2.CreateCapacityReservationInput, _ ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error) {
	id := fmt.Sprintf("cr-%d", len(f.created)+1)
	f.created = append(f.created, id)
	return &ec2.CreateCapacityReservationOutput{CapacityReservation: &ec2types.CapacityReservation{
		CapacityReservationId: aws.String(id), State: f.createState, AvailabilityZone: in.AvailabilityZone,
	}}, nil
}

func (f *fakeHoldEC2) DescribeCapacityReservations(_ context.Context, in *ec2.DescribeCapacityReservationsInput, _ ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	id := in.CapacityReservationIds[0]
	st, ok := f.states[id]
	if !ok {
		return nil, &capErr{"InvalidCapacityReservationId.NotFound"}
	}
	return &ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{{
		CapacityReservationId: aws.String(id), State: st, AvailabilityZone: aws.String(landRegion + "a"),
	}}}, nil
}

func (f *fakeHoldEC2) CancelCapacityReservation(_ context.Context, in *ec2.CancelCapacityReservationInput, _ ...func(*ec2.Options)) (*ec2.CancelCapacityReservationOutput, error) {
	f.cancelled = append(f.cancelled, aws.ToString(in.CapacityReservationId))
	return &ec2.CancelCapacityReservationOutput{}, nil
}

func newHoldHarness(t *testing.T, createState ec2types.CapacityReservationState) (*landHarness, *fakeHoldEC2) {
	t.Helper()
	h := newLandHarness(t)
	f := &fakeHoldEC2{createState: createState, states: map[string]ec2types.CapacityReservationState{}}
	hold := &Holder{
		createClient:  func(string) holderCreateAPI { return f },
		releaseClient: func(string) holderReleaseAPI { return f },
	}
	hold.describeReservation = func(ctx context.Context, region, id string) (landingCheck, error) {
		h.described = append(h.described, id)
		return describeReservationState(ctx, hold.releaseClient(region), id)
	}
	h.poller.holder = hold
	return h, f
}

func holdWatch(t *testing.T, id string) *Watch {
	w := landWatch(t, id)
	w.Action = ActionHold
	w.LaunchConfigJSON = nil
	return w
}

func TestLanding_HoldActiveAtCreateMatchesNow(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStateActive)
	h.put(t, holdWatch(t, "w-hold-now"))
	h.cycle(t)
	got := h.get(t, "w-hold-now")
	if got.Status != StatusMatched || got.Landing() || len(f.created) != 1 || len(h.described) != 0 {
		t.Errorf("status=%q landing=%v created=%v described=%v", got.Status, got.Landing(), f.created, h.described)
	}
	if got.LastMatch == nil || got.LastMatch.ReservationState != "active" || got.LastMatch.LandedAt.IsZero() {
		t.Errorf("last_match = %+v", got.LastMatch)
	}
}

func TestLanding_HoldPendingThenActive(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStatePending)
	h.put(t, holdWatch(t, "w-hold-pend"))
	h.cycle(t)
	got := h.get(t, "w-hold-pend")
	if got.Status != StatusActive || got.PendingReservationID != "cr-1" || got.LeaseOwner != LandingLeasePrefix+"cr-1" {
		t.Fatalf("status=%q pending=%q lease=%q, want landing cr-1", got.Status, got.PendingReservationID, got.LeaseOwner)
	}

	f.states["cr-1"] = ec2types.CapacityReservationStatePending
	h.cycle(t)
	if len(f.created) != 1 {
		t.Fatalf("created = %v, want no second reservation while one is pending", f.created)
	}

	f.states["cr-1"] = ec2types.CapacityReservationStateActive
	h.cycle(t)
	got = h.get(t, "w-hold-pend")
	if got.Status != StatusMatched || got.Landing() {
		t.Errorf("status=%q landing=%v, want matched", got.Status, got.Landing())
	}
	hist := h.history(t, "w-hold-pend")
	if len(hist) != 1 || hist[0].ActionTaken != "held" || hist[0].ReservationID != "cr-1" || hist[0].ReservationState != "active" || hist[0].LandedAt.IsZero() {
		t.Errorf("history = %+v", hist)
	}
	if len(h.notified) != 1 {
		t.Errorf("notified = %d, want 1", len(h.notified))
	}
}

func TestLanding_HoldFailedResumes(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStatePending)
	h.put(t, holdWatch(t, "w-hold-fail"))
	h.cycle(t)
	f.states["cr-1"] = ec2types.CapacityReservationStateFailed
	h.cycle(t)
	if len(f.created) != 2 {
		t.Fatalf("created = %v, want a new reservation after the failed one", f.created)
	}
	got := h.get(t, "w-hold-fail")
	if got.PendingReservationID != "cr-2" || got.ConsecutiveFailures != 0 ||
		!strings.HasPrefix(got.LastError, "capacity: capacity reservation cr-1 failed before it landed") {
		t.Errorf("pending=%q streak=%d last_error=%q", got.PendingReservationID, got.ConsecutiveFailures, got.LastError)
	}
	if hist := h.history(t, "w-hold-fail"); len(hist) != 1 || hist[0].ReservationState != "failed" {
		t.Errorf("history = %+v", hist)
	}
}

func TestLanding_HoldStuckPendingIsCancelled(t *testing.T) {
	h, f := newHoldHarness(t, ec2types.CapacityReservationStatePending)
	h.put(t, holdWatch(t, "w-hold-stuck"))
	h.cycle(t)
	f.states["cr-1"] = ec2types.CapacityReservationStatePending
	h.backdate(t, "w-hold-stuck", PendingLandingTimeout+time.Minute)
	h.cycle(t)
	if len(f.cancelled) != 1 || f.cancelled[0] != "cr-1" {
		t.Fatalf("cancelled = %v, want cr-1", f.cancelled)
	}
	if got := h.get(t, "w-hold-stuck"); got.PendingReservationID != "cr-2" || !strings.Contains(got.LastError, "lagotto cancelled it") {
		t.Errorf("pending=%q last_error=%q", got.PendingReservationID, got.LastError)
	}
}

// ── the describes ──

type fakeDescribeInstances struct {
	out *ec2.DescribeInstancesOutput
	err error
}

func (f *fakeDescribeInstances) DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return f.out, f.err
}

func TestDescribeInstanceState(t *testing.T) {
	ctx := context.Background()
	full := &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{
		{InstanceId: aws.String("i-other")},
		{
			InstanceId:  aws.String("i-1"),
			State:       &ec2types.InstanceState{Name: ec2types.InstanceStateNameTerminated},
			Placement:   &ec2types.Placement{AvailabilityZone: aws.String("us-east-2c")},
			StateReason: &ec2types.StateReason{Code: aws.String("Server.InsufficientInstanceCapacity"), Message: aws.String("Insufficient capacity.")},
		},
	}}}}
	c, err := describeInstanceState(ctx, &fakeDescribeInstances{out: full}, "i-1")
	if err != nil || c.State != "terminated" || c.AZ != "us-east-2c" || c.ReasonCode != "Server.InsufficientInstanceCapacity" || c.NotFound {
		t.Errorf("check = %+v, %v", c, err)
	}
	if c, err := describeInstanceState(ctx, &fakeDescribeInstances{out: &ec2.DescribeInstancesOutput{}}, "i-1"); err != nil || !c.NotFound {
		t.Errorf("empty: %+v %v", c, err)
	}
	if c, err := describeInstanceState(ctx, &fakeDescribeInstances{err: &capErr{"InvalidInstanceID.NotFound"}}, "i-1"); err != nil || !c.NotFound {
		t.Errorf("not found: %+v %v", c, err)
	}
	if _, err := describeInstanceState(ctx, &fakeDescribeInstances{err: &capErr{"RequestLimitExceeded"}}, "i-1"); err == nil {
		t.Error("a describe error was swallowed")
	}
}

func TestDescribeReservationState(t *testing.T) {
	ctx := context.Background()
	f := &fakeHoldEC2{states: map[string]ec2types.CapacityReservationState{"cr-1": ec2types.CapacityReservationStateActive}}
	if c, err := describeReservationState(ctx, f, "cr-1"); err != nil || c.State != "active" || c.AZ != landRegion+"a" {
		t.Errorf("active: %+v %v", c, err)
	}
	if c, err := describeReservationState(ctx, f, "cr-gone"); err != nil || !c.NotFound {
		t.Errorf("not found: %+v %v", c, err)
	}
	f.describeErr = &capErr{"RequestLimitExceeded"}
	if _, err := describeReservationState(ctx, f, "cr-1"); err == nil {
		t.Error("a describe error was swallowed")
	}
}

func TestLandingPhasesAndKinds(t *testing.T) {
	for state, want := range map[string]landingPhase{
		"pending": phaseWaiting, "running": phaseLanded, "stopped": phaseStopped, "stopping": phaseStopped,
		"shutting-down": phaseGone, "terminated": phaseGone,
	} {
		if got := instancePhase(state); got != want {
			t.Errorf("instancePhase(%q) = %v, want %v", state, got, want)
		}
	}
	for state, want := range map[string]landingPhase{
		"pending": phaseWaiting, "assessing": phaseWaiting, "active": phaseLanded,
		"failed": phaseGone, "expired": phaseGone, "cancelled": phaseGone,
	} {
		if got := reservationPhase(state); got != want {
			t.Errorf("reservationPhase(%q) = %v, want %v", state, got, want)
		}
	}
	if k := landingFailureKind(false, ActionLandingFailed, landingCheck{ReasonCode: "Server.SpotInstanceTermination"}); k != FailureCapacity {
		t.Errorf("spot reclaim = %v, want capacity", k)
	}
	if k := landingFailureKind(true, ActionLandingFailed, landingCheck{State: "expired"}); k != FailureUnknown {
		t.Errorf("expired reservation = %v, want unknown", k)
	}
	if k := landingFailureKind(false, ActionLandingTimedOut, landingCheck{ReasonCode: "Server.InsufficientInstanceCapacity"}); k != FailureUnknown {
		t.Errorf("timed out = %v, want unknown", k)
	}
}

// PollWatch (the single-watch entry point) routes a landing watch to the
// landing check too.
func TestLanding_PollWatchRoutes(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-pw"))
	h.cycle(t)
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running"}, nil }
	ms, err := h.poller.PollWatch(context.Background(), h.get(t, "w-pw"))
	if err != nil || len(ms) != 1 || len(h.launches) != 1 {
		t.Errorf("matches=%v err=%v launches=%v", ms, err, h.launches)
	}
}

// Two pollers with no lease (poll --no-lease) load the same landing watch. The
// first resolves it; the second, acting on its stale copy, must not record a
// second resolution or launch again: every landing write is conditional on the
// watch still landing the same resource.
func TestLanding_StaleCopyCannotResolveTwice(t *testing.T) {
	h := newLandHarness(t)
	h.poller.leaseOwner = ""
	h.put(t, landWatch(t, "w-race"))
	h.cycle(t)
	stale := h.get(t, "w-race")

	h.describe = func(id string) (landingCheck, error) {
		if id == "i-1" {
			return landingCheck{State: "terminated", ReasonCode: "Server.InsufficientInstanceCapacity"}, nil
		}
		return landingCheck{State: "pending"}, nil
	}
	h.cycle(t) // resolves i-1, relaunches i-2
	if len(h.launches) != 2 {
		t.Fatalf("launches = %v", h.launches)
	}

	s := &PollSummary{}
	if h.poller.pollLanding(context.Background(), stale, s) {
		t.Error("the stale copy resumed hunting")
	}
	if n := len(h.history(t, "w-race")); n != 1 {
		t.Errorf("history rows = %d, want 1 (no second resolution)", n)
	}
	// A refresh from a stale copy must not resurrect i-1 either.
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "pending"}, nil }
	stale2 := *stale
	h.poller.pollLanding(context.Background(), &stale2, s)
	if got := h.get(t, "w-race"); got.PendingInstanceID != "i-2" || got.LeaseOwner != LandingLeasePrefix+"i-2" {
		t.Errorf("pending=%q lease=%q, want i-2 untouched", got.PendingInstanceID, got.LeaseOwner)
	}
}

// failingDynamo fails UpdateItem calls whose expression contains match.
type failingDynamo struct {
	dynamoAPI
	match string
}

func (f *failingDynamo) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if strings.Contains(aws.ToString(in.UpdateExpression), f.match) {
		return nil, errors.New("dynamodb unavailable")
	}
	return f.dynamoAPI.UpdateItem(ctx, in, o...)
}

// If the landing cannot be recorded, the match is recorded at once instead: a
// premature match beats a second launch next cycle.
func TestLanding_BeginWriteFailsFallsBackToMatch(t *testing.T) {
	h := newLandHarness(t)
	h.store.client = &failingDynamo{dynamoAPI: h.store.client, match: "pending_match = :pm"}
	h.put(t, landWatch(t, "w-fallback"))
	h.cycle(t)
	got := h.get(t, "w-fallback")
	if got.Status != StatusMatched || got.Landing() {
		t.Errorf("status=%q landing=%v, want matched", got.Status, got.Landing())
	}
	h.cycle(t)
	if len(h.launches) != 1 {
		t.Errorf("launches = %v", h.launches)
	}
}

// A failed clear leaves the watch landing (retried next cycle), and launches
// nothing.
func TestLanding_EndWriteFailsStaysLanding(t *testing.T) {
	h := newLandHarness(t)
	h.put(t, landWatch(t, "w-endfail"))
	h.cycle(t)
	h.store.client = &failingDynamo{dynamoAPI: h.store.client, match: "REMOVE pending_instance_id"}
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "terminated"}, nil }
	h.cycle(t)
	if len(h.launches) != 1 {
		t.Fatalf("launches = %v", h.launches)
	}
	if got := h.get(t, "w-endfail"); got.PendingInstanceID != "i-1" {
		t.Errorf("pending = %q", got.PendingInstanceID)
	}
}

// A poller with no describer or terminator wired cannot judge or give up on a
// landing; it stays landing rather than launching.
func TestLanding_NoSeamsStaysLanding(t *testing.T) {
	p := &Poller{}
	w := &Watch{PendingInstanceID: "i-1"}
	if _, err := p.checkLanding(context.Background(), w); err == nil {
		t.Error("checkLanding without a spawner succeeded")
	}
	if err := p.cancelPending(context.Background(), w); err == nil {
		t.Error("cancelPending without a spawner succeeded")
	}
	w = &Watch{PendingReservationID: "cr-1"}
	if _, err := p.checkLanding(context.Background(), w); err == nil {
		t.Error("checkLanding without a holder succeeded")
	}
	if err := p.cancelPending(context.Background(), w); err == nil {
		t.Error("cancelPending without a holder succeeded")
	}
	// A record without pending_match still yields a usable match.
	since := time.Now().UTC()
	m := landingMatch(&Watch{WatchID: "w", InstanceTypePattern: "m7g.medium", PendingInstanceID: "i-1", PendingRegion: "us-east-2", PendingSince: &since})
	if m.InstanceID != "i-1" || m.Region != "us-east-2" || m.InstanceType != "m7g.medium" || !m.MatchedAt.Equal(since) {
		t.Errorf("landingMatch = %+v", m)
	}
}

// The production describers are one EC2 call each; run them against the
// emulator so the wiring (region, client) is exercised.
func TestLanding_ProductionDescribers(t *testing.T) {
	env := testutil.SubstrateServer(t)
	c, err := newInstanceDescriber(env.AWSConfig)(context.Background(), "us-east-1", "i-0000000000000dead")
	if err == nil && !c.NotFound {
		t.Errorf("describe of a missing instance = %+v", c)
	}
	h := NewHolder(env.AWSConfig)
	c, err = h.describeReservation(context.Background(), "us-east-1", "cr-0000000000000dead")
	if err == nil && !c.NotFound {
		t.Errorf("describe of a missing reservation = %+v", c)
	}
}

// A stopped instance in the landing window is spawn's fail-closed bootstrap
// falling back to a shutdown (no InstanceInitiatedShutdownBehavior, so it
// stops): not a landing. It is terminated and the hunt resumes; if the
// terminate fails the hunt still resumes, since a stopped instance cannot
// become a second running one.
func TestLanding_StoppedIsNotALanding(t *testing.T) {
	stopped := func(id string) (landingCheck, error) {
		if id == "i-1" {
			return landingCheck{State: "stopped", ReasonCode: "Client.InstanceInitiatedShutdown",
				ReasonMessage: "Client.InstanceInitiatedShutdown: Instance initiated shutdown"}, nil
		}
		return landingCheck{State: "pending"}, nil
	}
	t.Run("terminated", func(t *testing.T) {
		h := newLandHarness(t)
		h.put(t, landWatch(t, "w-stop"))
		h.cycle(t)
		h.describe = stopped
		h.cycle(t)
		if len(h.terminated) != 1 || len(h.launches) != 2 {
			t.Fatalf("terminated=%v launches=%v", h.terminated, h.launches)
		}
		got := h.get(t, "w-stop")
		if got.Status != StatusActive || got.PendingInstanceID != "i-2" || got.ConsecutiveFailures != 1 ||
			!strings.HasPrefix(got.LastError, "unknown: Client.InstanceInitiatedShutdown: instance i-1 stopped before it landed") ||
			!strings.Contains(got.LastError, "lagotto terminated it") {
			t.Errorf("status=%q pending=%q streak=%d last_error=%q", got.Status, got.PendingInstanceID, got.ConsecutiveFailures, got.LastError)
		}
		if hist := h.history(t, "w-stop"); len(hist) != 1 || hist[0].InstanceState != "stopped" || hist[0].ActionTaken != ActionLandingFailed {
			t.Errorf("history = %+v", hist)
		}
	})
	t.Run("terminate fails", func(t *testing.T) {
		h := newLandHarness(t)
		h.put(t, landWatch(t, "w-stop-deny"))
		h.cycle(t)
		h.describe = stopped
		h.termErr = &capErr{"UnauthorizedOperation"}
		h.cycle(t)
		if len(h.launches) != 2 {
			t.Fatalf("launches = %v, want the hunt resumed", h.launches)
		}
		if got := h.get(t, "w-stop-deny"); !strings.Contains(got.LastError, "could not terminate it") ||
			!strings.Contains(got.LastError, "spawn terminate i-1") {
			t.Errorf("last_error = %q", got.LastError)
		}
	})
}
