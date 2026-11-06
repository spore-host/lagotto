package watcher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// ── #238: a quota refusal marks one candidate infeasible, not the whole watch ──
//
// These drive the real pollGroup → Spawner.Spawn → provision path against a
// substrate-backed store, with a fake capacity search and a fake provision that
// refuses chosen (type, region) candidates with VcpuLimitExceeded (a real quota
// code as far as ClassifyFailure / IsQuotaExceeded are concerned).

// qiCandidate is one offered (type, region) with its on-demand price.
type qiCandidate struct {
	typ, region string
	price       float64
	vcpus       int32
}

func qiResults(cands ...qiCandidate) []truffleaws.InstanceTypeResult {
	out := make([]truffleaws.InstanceTypeResult, 0, len(cands))
	for _, c := range cands {
		out = append(out, truffleaws.InstanceTypeResult{
			InstanceType: c.typ, Region: c.region, AvailableAZs: []string{c.region + "a"},
			OnDemandPrice: c.price, VCPUs: c.vcpus,
		})
	}
	return out
}

// qiHarness is a poller whose provision consults outcome(type, region): nil =
// launch succeeds, otherwise that error is returned. Every attempt is logged.
type qiHarness struct {
	store    *Store
	poller   *Poller
	attempts []string // "type@region", in order
	notified []string // reasons passed to the quota-cap notifier
	matches  int      // generic match notifications
}

func newQIHarness(t *testing.T, results []truffleaws.InstanceTypeResult, outcome func(typ, region string) error) *qiHarness {
	t.Helper()
	h := &qiHarness{store: fleetStore(t)}
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		h.attempts = append(h.attempts, cfg.InstanceType+"@"+cfg.Region)
		if err := outcome(cfg.InstanceType, cfg.Region); err != nil {
			return nil, err
		}
		return &spawnaws.LaunchResult{InstanceID: "i-" + cfg.Region}, nil
	})
	h.poller = &Poller{store: h.store, spawner: sp, truffle: &fakeSearcher{results: results}}
	h.poller.notifyQuotaCap = func(_ context.Context, _ *Watch, _ int, reason string) error {
		h.notified = append(h.notified, reason)
		return nil
	}
	return h
}

func qiWatch(t *testing.T, id string) *Watch {
	t.Helper()
	now := time.Now().UTC()
	return &Watch{
		WatchID: id, UserID: "arn:alice", Status: StatusActive, Action: ActionSpawn,
		InstanceTypePattern: "g7.*", Regions: []string{"eu-north-1", "us-west-2"},
		LaunchConfigJSON: mustConfigJSON(t),
		CreatedAt:        now, UpdatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
		TTLTimestamp: now.Add(24 * time.Hour).Unix(),
	}
}

// cycle runs one stateless poll cycle: reload the watch from the store (as the
// hosted poller does every invocation), then poll it.
func (h *qiHarness) cycle(t *testing.T, id string) (*Watch, *PollSummary) {
	t.Helper()
	ctx := context.Background()
	w, err := h.store.GetWatch(ctx, id)
	if err != nil || w == nil {
		t.Fatalf("GetWatch(%s): %v %v", id, w, err)
	}
	summary := &PollSummary{}
	if err := h.poller.pollGroup(ctx, w.Regions, w.InstanceTypePattern, w.Spot, []*Watch{w}, summary); err != nil {
		t.Fatalf("pollGroup: %v", err)
	}
	got, err := h.store.GetWatch(ctx, id)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	return got, summary
}

func refuse(pairs ...string) func(typ, region string) error {
	set := map[string]bool{}
	for _, p := range pairs {
		set[p] = true
	}
	return func(typ, region string) error {
		if set[typ+"@"+region] {
			return &capErr{quotaCode}
		}
		return nil
	}
}

// The w-c66f3217 shape: the cheapest candidate is in a region whose quota can't
// hold it; the next one fits. The watch must launch the second, in the same
// cycle, and remember the first as quota-infeasible.
func TestQuotaInfeasible_FallsThroughToNextCandidate(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	), refuse("g7.24xlarge@eu-north-1"))
	w := qiWatch(t, "w-fall")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	got, summary := h.cycle(t, w.WatchID)

	if want := []string{"g7.24xlarge@eu-north-1", "g7.24xlarge@us-west-2"}; strings.Join(h.attempts, ",") != strings.Join(want, ",") {
		t.Fatalf("attempts = %v, want %v (cheapest first, then fall through in the same cycle)", h.attempts, want)
	}
	if got.Status != StatusMatched {
		t.Fatalf("status = %q, want matched (a quota refusal in one region must not kill a multi-region watch)", got.Status)
	}
	if summary.Launched != 1 || summary.Failed != 0 {
		t.Errorf("Launched=%d Failed=%d, want 1/0", summary.Launched, summary.Failed)
	}
	if got.LastMatch == nil || got.LastMatch.Region != "us-west-2" || got.LastMatch.ActionTaken != "spawned" {
		t.Errorf("LastMatch = %+v, want spawned in us-west-2", got.LastMatch)
	}
	if got.MatchCount != 1 {
		t.Errorf("MatchCount = %d, want 1 (the quota refusal is not a match)", got.MatchCount)
	}
	if len(got.QuotaInfeasible) != 1 {
		t.Fatalf("QuotaInfeasible = %+v, want the eu-north-1 entry", got.QuotaInfeasible)
	}
	q := got.QuotaInfeasible[0]
	if q.InstanceType != "g7.24xlarge" || q.Region != "eu-north-1" || q.Code != quotaCode || q.VCPUs != 96 {
		t.Errorf("entry = %+v", q)
	}
	if len(h.notified) != 0 {
		t.Errorf("quota notifications = %d, want 0 (the watch did not fail)", len(h.notified))
	}

	// History records the refusal as its own quota_infeasible row, plus the launch.
	hist, err := h.store.ListMatchHistory(context.Background(), w.WatchID)
	if err != nil {
		t.Fatal(err)
	}
	actions := map[string]int{}
	for _, m := range hist {
		actions[m.ActionTaken]++
		if m.ActionTaken == ActionQuotaInfeasible && !strings.Contains(m.QuotaDetail, "needs 96 vCPU") {
			t.Errorf("quota row detail = %q, want the vCPU need", m.QuotaDetail)
		}
	}
	if actions[ActionQuotaInfeasible] != 1 || actions["spawned"] != 1 || len(hist) != 2 {
		t.Errorf("history actions = %v (%d rows), want 1 quota_infeasible + 1 spawned", actions, len(hist))
	}
}

// Every candidate refused: terminal, one notification, a message naming each.
func TestQuotaInfeasible_AllRefusedIsTerminal(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-east-2", 2.0, 96},
	), refuse("g7.24xlarge@eu-north-1", "g7.24xlarge@us-east-2"))
	w := qiWatch(t, "w-all")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	got, summary := h.cycle(t, w.WatchID)

	if got.Status != StatusFailed {
		t.Fatalf("status = %q, want failed (nothing left to try)", got.Status)
	}
	if summary.Failed != 1 || summary.Retrying != 0 {
		t.Errorf("Failed=%d Retrying=%d, want 1/0", summary.Failed, summary.Retrying)
	}
	if len(h.attempts) != 2 {
		t.Errorf("attempts = %v, want both candidates tried", h.attempts)
	}
	if len(h.notified) != 1 {
		t.Fatalf("quota notifications = %d, want exactly 1", len(h.notified))
	}
	for _, s := range []string{"All 2 candidates", "g7.24xlarge in eu-north-1", "g7.24xlarge in us-east-2", quotaCode} {
		if !strings.Contains(h.notified[0], s) {
			t.Errorf("notification reason missing %q:\n%s", s, h.notified[0])
		}
	}
	if got.QuotaCapReason != h.notified[0] {
		t.Errorf("persisted reason differs from the notified one:\n%q\n%q", got.QuotaCapReason, h.notified[0])
	}
	if got.LastMatch == nil || got.LastMatch.ActionTaken != ActionQuotaInfeasible {
		t.Errorf("LastMatch = %+v, want action quota_infeasible", got.LastMatch)
	}
	if len(got.QuotaInfeasible) != 2 {
		t.Errorf("QuotaInfeasible = %+v, want both", got.QuotaInfeasible)
	}
}

// A watch with one candidate keeps the pre-#238 behaviour: its first quota
// refusal is terminal (there is nothing else to try), now with the quota message.
func TestQuotaInfeasible_SingleCandidateStaysTerminal(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
	), refuse("g7.24xlarge@eu-north-1"))
	w := qiWatch(t, "w-one")
	w.Regions = []string{"eu-north-1"}
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	got, _ := h.cycle(t, w.WatchID)

	if got.Status != StatusFailed {
		t.Fatalf("status = %q, want failed", got.Status)
	}
	if len(h.attempts) != 1 || len(h.notified) != 1 {
		t.Fatalf("attempts=%v notifications=%d, want 1/1", h.attempts, len(h.notified))
	}
	if !strings.Contains(h.notified[0], "The only candidate") {
		t.Errorf("reason = %q", h.notified[0])
	}
}

// A refused candidate is skipped on the next cycle (no tight loop), and tried
// again once QuotaCapReprobeInterval has passed.
func TestQuotaInfeasible_SkippedThenReprobedAfterInterval(t *testing.T) {
	capacity := &capErr{"InsufficientInstanceCapacity"}
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	), func(typ, region string) error {
		if region == "eu-north-1" {
			return &capErr{quotaCode}
		}
		return capacity // us-west-2 is dry: the watch must keep waiting
	})
	w := qiWatch(t, "w-reprobe")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	// Cycle 1: eu-north-1 refused on quota → fall through → us-west-2 ICE.
	got, summary := h.cycle(t, w.WatchID)
	if got.Status != StatusActive || summary.Retrying != 1 {
		t.Fatalf("cycle 1: status=%q Retrying=%d, want active/1", got.Status, summary.Retrying)
	}
	if strings.Join(h.attempts, ",") != "g7.24xlarge@eu-north-1,g7.24xlarge@us-west-2" {
		t.Fatalf("cycle 1 attempts = %v", h.attempts)
	}

	// Cycle 2 (within the interval): the infeasible candidate is NOT retried.
	h.attempts = nil
	got, _ = h.cycle(t, w.WatchID)
	if strings.Join(h.attempts, ",") != "g7.24xlarge@us-west-2" {
		t.Fatalf("cycle 2 attempts = %v, want only us-west-2 (eu-north-1 is quota-infeasible)", h.attempts)
	}
	if got.Status != StatusActive {
		t.Fatalf("cycle 2 status = %q, want active", got.Status)
	}

	// Age the entry past the re-probe interval: eu-north-1 is tried again.
	got.QuotaInfeasible[0].At = time.Now().UTC().Add(-QuotaCapReprobeInterval - time.Minute)
	if err := h.store.PutWatch(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	h.attempts = nil
	got, _ = h.cycle(t, w.WatchID)
	if len(h.attempts) == 0 || h.attempts[0] != "g7.24xlarge@eu-north-1" {
		t.Fatalf("cycle 3 attempts = %v, want eu-north-1 re-probed first", h.attempts)
	}
	if got.Status != StatusActive {
		t.Fatalf("cycle 3 status = %q, want active (us-west-2 is capacity-dry, not quota-infeasible)", got.Status)
	}
	if len(got.QuotaInfeasible) != 1 || !got.QuotaInfeasible[0].Live(time.Now().UTC()) {
		t.Errorf("QuotaInfeasible = %+v, want one refreshed (live) entry", got.QuotaInfeasible)
	}
}

// A re-probe that succeeds (the quota was raised) launches there.
func TestQuotaInfeasible_ReprobeSucceedsAfterQuotaRaised(t *testing.T) {
	raised := false
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	), func(typ, region string) error {
		if region == "eu-north-1" && !raised {
			return &capErr{quotaCode}
		}
		if region == "us-west-2" {
			return &capErr{"InsufficientInstanceCapacity"}
		}
		return nil
	})
	w := qiWatch(t, "w-raised")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if got.Status != StatusActive {
		t.Fatalf("cycle 1 status = %q", got.Status)
	}
	raised = true
	got.QuotaInfeasible[0].At = time.Now().UTC().Add(-QuotaCapReprobeInterval - time.Minute)
	if err := h.store.PutWatch(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	got, _ = h.cycle(t, w.WatchID)
	if got.Status != StatusMatched || got.LastMatch == nil || got.LastMatch.Region != "eu-north-1" {
		t.Fatalf("status=%q last=%+v, want matched in eu-north-1 after the quota was raised", got.Status, got.LastMatch)
	}
}

// Fall-through is bounded per cycle; the rest continue next cycle.
func TestQuotaInfeasible_FallthroughBoundedPerCycle(t *testing.T) {
	regions := []string{"r-1", "r-2", "r-3", "r-4", "r-5", "r-6"}
	var cands []qiCandidate
	for i, r := range regions {
		cands = append(cands, qiCandidate{"g7.24xlarge", r, float64(i + 1), 96})
	}
	// The first five refuse on quota; r-6 launches.
	h := newQIHarness(t, qiResults(cands...), refuse(
		"g7.24xlarge@r-1", "g7.24xlarge@r-2", "g7.24xlarge@r-3", "g7.24xlarge@r-4", "g7.24xlarge@r-5"))
	w := qiWatch(t, "w-bounded")
	w.Regions = regions
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}

	got, summary := h.cycle(t, w.WatchID)
	if len(h.attempts) != maxQuotaFallthroughsPerCycle+1 {
		t.Fatalf("cycle 1 attempts = %v, want %d", h.attempts, maxQuotaFallthroughsPerCycle+1)
	}
	if got.Status != StatusActive || summary.Retrying != 1 {
		t.Fatalf("cycle 1: status=%q Retrying=%d, want active/1", got.Status, summary.Retrying)
	}

	h.attempts = nil
	got, _ = h.cycle(t, w.WatchID)
	if strings.Join(h.attempts, ",") != "g7.24xlarge@r-5,g7.24xlarge@r-6" {
		t.Fatalf("cycle 2 attempts = %v, want r-5 then r-6 (the 4 refused are skipped)", h.attempts)
	}
	if got.Status != StatusMatched || got.LastMatch.Region != "r-6" {
		t.Fatalf("cycle 2: status=%q last=%+v, want matched in r-6", got.Status, got.LastMatch)
	}
}

// A non-quota terminal error (bad AMI) still stops the watch at once, as before.
func TestQuotaInfeasible_NonQuotaTerminalUnchanged(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	), func(string, string) error { return &capErr{"InvalidAMIID.NotFound"} })
	w := qiWatch(t, "w-ami")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if got.Status != StatusFailed || len(h.attempts) != 1 {
		t.Fatalf("status=%q attempts=%v, want failed after 1 attempt", got.Status, h.attempts)
	}
	if len(got.QuotaInfeasible) != 0 || len(h.notified) != 0 {
		t.Errorf("a non-quota failure must not record quota state: %+v, notified=%d", got.QuotaInfeasible, len(h.notified))
	}
}

// Spot: a candidate above --max-price now has never been tried, so it can't be
// called infeasible — the watch stays active instead of failing.
func TestCandidateUniverse_SpotIgnoresMaxPrice(t *testing.T) {
	w := &Watch{Spot: true, MaxPrice: 1.5}
	results := qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 9, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 9, 96},
	)
	spot := []truffleaws.SpotPriceResult{
		{InstanceType: "g7.24xlarge", Region: "eu-north-1", AvailabilityZone: "eu-north-1a", SpotPrice: 1.0},
		{InstanceType: "g7.24xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2a", SpotPrice: 3.0},
	}
	if got := rankedCandidates(w, results, spot, true, time.Now()); len(got) != 1 {
		t.Fatalf("ranked = %d, want 1 under max-price", len(got))
	}
	u := candidateUniverse(w, results, spot, true)
	if len(u) != 2 {
		t.Fatalf("universe = %v, want both (spot price can drop)", u)
	}
	live := map[candidateKey]QuotaInfeasibility{{"g7.24xlarge", "eu-north-1"}: {}}
	if allQuotaInfeasible(u, live) {
		t.Error("allQuotaInfeasible = true with an untried, over-price spot candidate left")
	}
}

// Fleets keep the #153 path: a quota refusal caps the fleet and records no
// per-candidate infeasibility.
func TestQuotaInfeasible_FleetUnaffected(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-fleet-238")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	p, _ := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		return nil, &capErr{quotaCode}
	})
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }
	p.pollFleetWatch(ctx, w, &PollSummary{})
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.Status != StatusActive || len(got.QuotaInfeasible) != 0 {
		t.Errorf("fleet: status=%q QuotaInfeasible=%+v, want active and none", got.Status, got.QuotaInfeasible)
	}
}

// Compatibility: a record written before #238 (no quota_infeasible attribute)
// reads back with no entries, and the field is omitted from JSON when empty.
func TestQuotaInfeasible_OmitEmpty(t *testing.T) {
	raw, err := json.Marshal(&Watch{WatchID: "w-old"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "quota_infeasible") {
		t.Errorf("empty QuotaInfeasible serialized: %s", raw)
	}
	raw, _ = json.Marshal(&MatchResult{WatchID: "w-old"})
	if strings.Contains(string(raw), "quota_detail") {
		t.Errorf("empty QuotaDetail serialized: %s", raw)
	}
}

func TestQuotaInfeasibleReason_ListsEachWithRemediation(t *testing.T) {
	r := QuotaInfeasibleReason([]QuotaInfeasibility{
		{InstanceType: "g7.24xlarge", Region: "us-east-2", Code: quotaCode, VCPUs: 96, QuotaKnown: true, QuotaLimit: 64, QuotaUsed: 0, Family: "G"},
		{InstanceType: "g7.24xlarge", Region: "eu-north-1", Code: quotaCode, VCPUs: 96},
	})
	for _, s := range []string{
		"All 2 candidates",
		"g7.24xlarge in eu-north-1: refused by EC2 quota (VcpuLimitExceeded); needs 96 vCPU",
		"g7.24xlarge in us-east-2: refused by EC2 quota (VcpuLimitExceeded); needs 96 vCPU, G-family On-Demand quota 64 vCPU, 0 in use, 64 available",
		"--region us-east-2", "--region eu-north-1",
	} {
		if !strings.Contains(r, s) {
			t.Errorf("reason missing %q:\n%s", s, r)
		}
	}
}

func TestQuotaCapNotification_SingleShotWording(t *testing.T) {
	w := &Watch{WatchID: "w-x", InstanceTypePattern: "g7.*", QuotaInfeasible: []QuotaInfeasibility{{InstanceType: "g7.24xlarge", Region: "eu-north-1"}}}
	if s := quotaCapSubject(w, 0); !strings.Contains(s, "every candidate refused") || strings.Contains(s, "fleet") {
		t.Errorf("subject = %q", s)
	}
	if b := quotaCapBody(w, 0, "REASON"); !strings.Contains(b, "REASON") || strings.Contains(b, "stays ACTIVE") {
		t.Errorf("body = %q", b)
	}
	p := quotaCapPayload(w, 0, "REASON")
	if p["event"] != "quota_infeasible" || p["status"] != "failed" {
		t.Errorf("payload = %v", p)
	}
	// Fleet wording unchanged.
	f := &Watch{WatchID: "w-f", DesiredCount: 5}
	if s := quotaCapSubject(f, 2); s != "[lagotto] fleet capped at 2/5 by EC2 quota" {
		t.Errorf("fleet subject changed: %q", s)
	}
	if quotaCapPayload(f, 2, "r")["event"] != "quota_capped" {
		t.Error("fleet payload event changed")
	}
}
