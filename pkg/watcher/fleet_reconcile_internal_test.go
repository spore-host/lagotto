package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

const (
	fleetWatchesTable = "lagotto-fleet-test-watches"
	fleetHistoryTable = "lagotto-fleet-test-history"
)

// fleetStore builds a substrate-backed store for the reconcile paths that read/
// write watch status. truffle is left nil — the condition-complete and
// at-capacity paths return before any search (launch/gap-fill is exercised by
// the substrate poller integration test).
func fleetStore(t *testing.T) *Store {
	t.Helper()
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, fleetWatchesTable)
	env.CreateHistoryTable(t, fleetHistoryTable)
	return NewStore(env.AWSConfig, fleetWatchesTable, fleetHistoryTable)
}

// TestPollFleetWatch_ConditionMet_Completes: when --until is satisfied, the fleet
// watch retires as StatusCompleted and launches nothing.
func TestPollFleetWatch_ConditionMet_Completes(t *testing.T) {
	store := fleetStore(t)
	w := &Watch{
		WatchID: "w-done", Status: StatusActive, DesiredCount: 4,
		Regions: []string{"us-east-1"}, InstanceTypePattern: "m8g.8xlarge",
		CompletionCondition: "s3-empty: s3://b/manifest/ minus s3://b/prepared/",
	}
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	launches := 0
	sp := &Spawner{
		s3: &fakeS3{counts: map[string]int32{"b/manifest/": 10, "b/prepared/": 10}}, // done
		provision: func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
			launches++
			return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
		},
	}
	p := &Poller{store: store, spawner: sp}

	p.pollFleetWatch(context.Background(), w, &PollSummary{})

	got, err := store.GetWatch(context.Background(), "w-done")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", got.Status)
	}
	if launches != 0 {
		t.Errorf("launched %d workers, want 0 (condition already met)", launches)
	}
}

// TestPollFleetWatch_AtCapacity_NoLaunch: condition unmet but running == desired
// → no launch, watch stays active.
func TestPollFleetWatch_AtCapacity_NoLaunch(t *testing.T) {
	store := fleetStore(t)
	w := &Watch{
		WatchID: "w-full", Status: StatusActive, DesiredCount: 2,
		Regions: []string{"us-east-1"}, InstanceTypePattern: "m8g.8xlarge",
		// No completion condition → always "not done", so it proceeds to the count.
	}
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	launches := 0
	sp := &Spawner{
		listInstances: func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
			// Two running workers already carry the fleet tag → fleet is full.
			return []spawnaws.InstanceInfo{
				{InstanceID: "i-1", State: "running", Tags: map[string]string{FleetTagKey: "w-full"}},
				{InstanceID: "i-2", State: "running", Tags: map[string]string{FleetTagKey: "w-full"}},
			}, nil
		},
		provision: func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
			launches++
			return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
		},
	}
	p := &Poller{store: store, spawner: sp}

	p.pollFleetWatch(context.Background(), w, &PollSummary{})

	got, err := store.GetWatch(context.Background(), "w-full")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Status != StatusActive {
		t.Errorf("status = %q, want active (fleet full, not done)", got.Status)
	}
	if launches != 0 {
		t.Errorf("launched %d, want 0 (already at DesiredCount)", launches)
	}
}

// TestFillFleetGap_LaunchesGap: fills the whole gap when capacity holds, and each
// worker gets a distinct recorded match.
func TestFillFleetGap_LaunchesGap(t *testing.T) {
	store := fleetStore(t)
	w := &Watch{WatchID: "w-fill", Status: StatusActive, DesiredCount: 3, Regions: []string{"us-east-1"}}
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	n := 0
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		n++
		return &spawnaws.LaunchResult{InstanceID: "i-" + string(rune('0'+n))}, nil
	})
	// Spawn needs a launch config on the watch.
	w.LaunchConfigJSON = mustConfigJSON(t)
	p := &Poller{store: store, spawner: sp}
	summary := &PollSummary{}

	best := &MatchResult{Region: "us-east-1", InstanceType: "m8g.8xlarge", CandidateAZs: []string{"us-east-1a"}}
	got := p.fillFleetGap(context.Background(), w, best, 3, summary)
	if got.Launched != 3 {
		t.Errorf("filled %d, want 3", got.Launched)
	}
	if got.QuotaErr != nil {
		t.Errorf("QuotaErr = %v, want nil (no quota involved)", got.QuotaErr)
	}
	if summary.Launched != 3 {
		t.Errorf("summary.Launched = %d, want 3", summary.Launched)
	}
}

// TestFillFleetGap_StopsOnCapacityFailure: a launch failure mid-fill stops the
// cycle; the watch stays active (retry next poll), partial fill returned.
func TestFillFleetGap_StopsOnCapacityFailure(t *testing.T) {
	store := fleetStore(t)
	w := &Watch{WatchID: "w-partial", Status: StatusActive, DesiredCount: 4, Regions: []string{"us-east-1"}, LaunchConfigJSON: mustConfigJSON(t)}
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	n := 0
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		n++
		if n >= 3 { // first two succeed, capacity runs out on the third
			return nil, &capErr{"InsufficientInstanceCapacity"}
		}
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})
	p := &Poller{store: store, spawner: sp}
	summary := &PollSummary{}

	best := &MatchResult{Region: "us-east-1", InstanceType: "m8g.8xlarge", CandidateAZs: []string{"us-east-1a"}}
	got := p.fillFleetGap(context.Background(), w, best, 4, summary)
	if got.Launched != 2 {
		t.Errorf("filled %d, want 2 (capacity failed on the 3rd)", got.Launched)
	}
	if got.QuotaErr != nil {
		t.Errorf("QuotaErr = %v, want nil (a capacity failure is not a quota cap)", got.QuotaErr)
	}
}

// fakeSearcher is an in-memory capacitySearcher: returns canned on-demand
// results (no spot pricing needed for these tests).
type fakeSearcher struct {
	results []truffleaws.InstanceTypeResult
}

func (f *fakeSearcher) SearchInstanceTypes(context.Context, []string, *regexp.Regexp, truffleaws.FilterOptions) ([]truffleaws.InstanceTypeResult, error) {
	return f.results, nil
}
func (f *fakeSearcher) GetSpotPricing(context.Context, []truffleaws.InstanceTypeResult, truffleaws.SpotOptions) ([]truffleaws.SpotPriceResult, error) {
	return nil, nil
}

func TestSearchBestMatch(t *testing.T) {
	w := &Watch{WatchID: "w-s", Regions: []string{"us-east-1"}, InstanceTypePattern: "m8g.8xlarge"}

	// Nothing offered → nil match.
	p := &Poller{truffle: &fakeSearcher{}}
	if m, err := p.searchBestMatch(context.Background(), w); err != nil || m != nil {
		t.Errorf("no capacity → got m=%v err=%v, want nil/nil", m, err)
	}

	// One offered type → matched.
	p = &Poller{truffle: &fakeSearcher{results: []truffleaws.InstanceTypeResult{
		{InstanceType: "m8g.8xlarge", Region: "us-east-1", AvailableAZs: []string{"us-east-1a"}, OnDemandPrice: 1.23},
	}}}
	m, err := p.searchBestMatch(context.Background(), w)
	if err != nil {
		t.Fatalf("searchBestMatch: %v", err)
	}
	if m == nil || m.InstanceType != "m8g.8xlarge" || m.Region != "us-east-1" {
		t.Errorf("got %+v, want an m8g.8xlarge/us-east-1 match", m)
	}
}

// TestPollFleetWatch_LaunchesFromZero drives the full reconcile launch path:
// condition unmet, zero running, capacity available → launches the full
// DesiredCount and the watch stays active.
func TestPollFleetWatch_LaunchesFromZero(t *testing.T) {
	store := fleetStore(t)
	w := &Watch{
		WatchID: "w-zero", Status: StatusActive, DesiredCount: 3,
		Regions: []string{"us-east-1"}, InstanceTypePattern: "m8g.8xlarge",
		LaunchConfigJSON: mustConfigJSON(t),
		// no --until → never "done", proceeds to count+fill
	}
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	launches := 0
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		launches++
		return &spawnaws.LaunchResult{InstanceID: "i-new"}, nil
	})
	// listInstances returns nothing → zero running, gap == DesiredCount.
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) { return nil, nil }
	p := &Poller{
		store:   store,
		spawner: sp,
		truffle: &fakeSearcher{results: []truffleaws.InstanceTypeResult{
			{InstanceType: "m8g.8xlarge", Region: "us-east-1", AvailableAZs: []string{"us-east-1a"}, OnDemandPrice: 1.0},
		}},
	}
	summary := &PollSummary{}

	p.pollFleetWatch(context.Background(), w, summary)

	if launches != 3 {
		t.Errorf("launched %d, want 3 (fill from zero to DesiredCount)", launches)
	}
	got, _ := store.GetWatch(context.Background(), "w-zero")
	if got.Status != StatusActive {
		t.Errorf("status = %q, want active (fleet not done, keeps maintaining)", got.Status)
	}
}

func mustConfigJSON(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(SpawnConfigFile{Name: "worker", InstanceType: "m8g.8xlarge", Region: "us-east-1", TTL: "4h"})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return raw
}

// ── Quota-cap reconcile behavior (#153) ───────────────────────────────────────
//
// capErr above is a generic smithy.APIError, so "VcpuLimitExceeded" through it is
// a real quota error as far as ClassifyFailure/IsQuotaExceeded are concerned
// (quota codes stay FailureTerminal; these tests assert the fleet path reports the
// ceiling WITHOUT failing the watch).

const quotaCode = "VcpuLimitExceeded"

// quotaFleetWatch builds a 5-worker fleet watch with a launchable config.
func quotaFleetWatch(t *testing.T, id string) *Watch {
	t.Helper()
	return &Watch{
		WatchID: id, Status: StatusActive, DesiredCount: 5,
		Regions: []string{"us-east-1"}, InstanceTypePattern: "g6e.xlarge",
		LaunchConfigJSON: mustConfigJSON(t),
	}
}

// quotaPoller wires a poller with a substrate store, a canned capacity search,
// and a spawner whose provision is `fn`. running controls what countRunningFleet
// reports.
func quotaPoller(t *testing.T, store *Store, watchID string, running int, fn func() (*spawnaws.LaunchResult, error)) (*Poller, *int) {
	t.Helper()
	calls := 0
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		calls++
		return fn()
	})
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
		var out []spawnaws.InstanceInfo
		for i := 0; i < running; i++ {
			out = append(out, spawnaws.InstanceInfo{
				InstanceID: fmt.Sprintf("i-%d", i), State: "running",
				Tags: map[string]string{FleetTagKey: watchID},
			})
		}
		return out, nil
	}
	p := &Poller{
		store:   store,
		spawner: sp,
		truffle: &fakeSearcher{results: []truffleaws.InstanceTypeResult{
			{InstanceType: "g6e.xlarge", Region: "us-east-1", AvailableAZs: []string{"us-east-1a"}, OnDemandPrice: 2.0, VCPUs: 4},
		}},
	}
	return p, &calls
}

// TestQuotaError_ReportsOnceAndStaysActive: two workers launch, the third is
// refused by the vCPU quota → the ceiling is recorded and notified, the watch
// stays ACTIVE (its two healthy workers are not abandoned), and the cycle is
// counted as quota-capped rather than retrying/failed.
func TestQuotaError_ReportsOnceAndStaysActive(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-quota")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	n := 0
	p, calls := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		n++
		if n >= 3 {
			return nil, &capErr{quotaCode}
		}
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})
	notifications := 0
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { notifications++; return nil }

	summary := &PollSummary{}
	p.pollFleetWatch(ctx, w, summary)

	if summary.Launched != 2 {
		t.Errorf("summary.Launched = %d, want 2", summary.Launched)
	}
	if *calls != 3 {
		t.Errorf("provision calls = %d, want 3 (two launches + the refused one)", *calls)
	}
	if summary.QuotaCapped != 1 {
		t.Errorf("summary.QuotaCapped = %d, want 1", summary.QuotaCapped)
	}
	if summary.Failed != 0 || summary.Retrying != 0 {
		t.Errorf("Failed=%d Retrying=%d, want 0/0 (a quota cap is neither)", summary.Failed, summary.Retrying)
	}
	if notifications != 1 {
		t.Errorf("notifications = %d, want 1", notifications)
	}

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Status != StatusActive {
		t.Errorf("status = %q, want active (quota codes are terminal, but failing a fleet would abandon its running workers)", got.Status)
	}
	if got.QuotaCappedCount != 2 {
		t.Errorf("QuotaCappedCount = %d, want 2", got.QuotaCappedCount)
	}
	if got.QuotaCappedAt.IsZero() {
		t.Error("QuotaCappedAt not persisted (nothing anchors the backoff)")
	}
	if !strings.Contains(got.QuotaCapReason, "capped at 2/5") {
		t.Errorf("QuotaCapReason = %q, want it to say capped at 2/5", got.QuotaCapReason)
	}
	// And the in-memory watch is kept in step, so the rest of the cycle sees it.
	if w.QuotaCappedCount != 2 {
		t.Errorf("in-memory QuotaCappedCount = %d, want 2", w.QuotaCappedCount)
	}
}

// TestFillFleetGap_ReportsQuotaError: the gap-fill reports a quota refusal via
// fillOutcome.QuotaErr, distinct from the "warn and stop" it does for every other
// failure kind.
func TestFillFleetGap_ReportsQuotaError(t *testing.T) {
	store := fleetStore(t)
	w := quotaFleetWatch(t, "w-qerr")
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{quotaCode}
	})
	p := &Poller{store: store, spawner: sp}
	out := p.fillFleetGap(context.Background(), w, &MatchResult{Region: "us-east-1", InstanceType: "g6e.xlarge", CandidateAZs: []string{"us-east-1a"}}, 3, &PollSummary{})
	if out.Launched != 0 {
		t.Errorf("Launched = %d, want 0", out.Launched)
	}
	if out.QuotaErr == nil {
		t.Fatal("QuotaErr = nil, want the quota error")
	}
	if !IsQuotaExceeded(out.QuotaErr) {
		t.Errorf("QuotaErr %v is not recognized as a quota error", out.QuotaErr)
	}
}

// TestQuotaCapped_SuppressesRetryWithinInterval: a fleet at its known ceiling
// costs ZERO launch attempts (and zero AWS calls) until the re-probe interval
// elapses — then exactly one attempt is made.
func TestQuotaCapped_SuppressesRetryWithinInterval(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-suppress")
	w.QuotaCappedCount = 2
	w.QuotaCappedAt = time.Now().UTC()
	w.QuotaCapReason = "fleet capped at 2/5"
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p, calls := quotaPoller(t, store, w.WatchID, 2, func() (*spawnaws.LaunchResult, error) {
		return nil, &capErr{quotaCode}
	})

	summary := &PollSummary{}
	p.pollFleetWatch(ctx, w, summary)
	if *calls != 0 {
		t.Errorf("provision calls = %d within the re-probe interval, want 0", *calls)
	}
	if summary.QuotaCapped != 1 {
		t.Errorf("summary.QuotaCapped = %d, want 1 (the suppressed cycle is still reported)", summary.QuotaCapped)
	}

	// Past the interval → exactly one re-probe (which is refused again).
	w.QuotaCappedAt = time.Now().UTC().Add(-(QuotaCapReprobeInterval + time.Minute))
	p.pollFleetWatch(ctx, w, &PollSummary{})
	if *calls != 1 {
		t.Errorf("provision calls after the interval = %d, want exactly 1", *calls)
	}
}

// TestQuotaCapped_TopsUpBelowTheCeiling: the backoff applies ONLY to attempts we
// know will fail. A worker lost BELOW the known ceiling is replaced on the very
// next cycle, not up to QuotaCapReprobeInterval later.
func TestQuotaCapped_TopsUpBelowTheCeiling(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-below")
	w.QuotaCappedCount = 2
	w.QuotaCappedAt = time.Now().UTC() // well inside the re-probe interval
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	// Only ONE worker running against a ceiling of two → there is known-good room.
	p, calls := quotaPoller(t, store, w.WatchID, 1, func() (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-replacement"}, nil
	})

	p.pollFleetWatch(ctx, w, &PollSummary{})
	if *calls == 0 {
		t.Fatal("no launch attempted below the known ceiling — the backoff must not block replacing a dead worker")
	}
}

// TestSuccessClearsQuotaCap: a launch that succeeds while capped means the
// ceiling moved (a granted increase, or other usage freed up), so the cap state is
// dropped — which is what makes a quota increase self-correcting with no user
// action, and lets a later cap notify again.
func TestSuccessClearsQuotaCap(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-clear")
	w.QuotaCappedCount = 2
	w.QuotaCappedAt = time.Now().UTC().Add(-(QuotaCapReprobeInterval + time.Minute))
	w.QuotaCapReason = "fleet capped at 2/5"
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p, calls := quotaPoller(t, store, w.WatchID, 2, func() (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-new"}, nil
	})

	p.pollFleetWatch(ctx, w, &PollSummary{})
	if *calls != 3 {
		t.Errorf("provision calls = %d, want 3 (gap 5-2)", *calls)
	}
	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.QuotaCappedCount != 0 || got.QuotaCapReason != "" || !got.QuotaCappedAt.IsZero() {
		t.Errorf("cap state not cleared: count=%d at=%v reason=%q", got.QuotaCappedCount, got.QuotaCappedAt, got.QuotaCapReason)
	}
	if w.QuotaCappedCount != 0 {
		t.Errorf("in-memory QuotaCappedCount = %d, want 0", w.QuotaCappedCount)
	}
}

// TestQuotaCap_NotifiesOnceAtTheSameLevel: the persisted level is what makes
// "report once" possible for a stateless poller — two caps at the same level
// notify once; a cap at a NEW level notifies again ("capped at 3/5, was 2/5").
func TestQuotaCap_NotifiesOnceAtTheSameLevel(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-notify")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p := &Poller{store: store}
	notified := 0
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { notified++; return nil }

	m := &MatchResult{Region: "us-east-1", InstanceType: "g6e.xlarge"}
	p.recordQuotaCap(ctx, w, 2, m, &capErr{quotaCode}, &PollSummary{})
	p.recordQuotaCap(ctx, w, 2, m, &capErr{quotaCode}, &PollSummary{})
	if notified != 1 {
		t.Errorf("notifications after two caps at 2 = %d, want 1", notified)
	}
	// The anchor is still refreshed on the un-notified cycle.
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.QuotaCappedCount != 2 {
		t.Errorf("QuotaCappedCount = %d, want 2", got.QuotaCappedCount)
	}

	p.recordQuotaCap(ctx, w, 3, m, &capErr{quotaCode}, &PollSummary{})
	if notified != 2 {
		t.Errorf("notifications after a cap at a NEW level = %d, want 2", notified)
	}
	got, _ = store.GetWatch(ctx, w.WatchID)
	if got.QuotaCappedCount != 3 {
		t.Errorf("QuotaCappedCount = %d, want 3 (overwritten, not added)", got.QuotaCappedCount)
	}
}

// TestQuotaCap_NilQuotasLookup: with no Service Quotas lookup wired (a poller
// whose runtime policy predates the servicequotas grant), the cap is STILL
// recorded and notified — just without the exact numbers. This is the property
// that keeps #153 from becoming another #148/#150 "silently no-ops if you forget
// to re-run setup".
func TestQuotaCap_NilQuotasLookup(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-nolookup")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p := &Poller{store: store} // p.quotas == nil
	notified := 0
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { notified++; return nil }

	summary := &PollSummary{}
	p.recordQuotaCap(ctx, w, 2, &MatchResult{Region: "us-east-1", InstanceType: "g6e.xlarge"}, &capErr{quotaCode}, summary)

	if notified != 1 {
		t.Errorf("notifications = %d, want 1 even with no quota lookup", notified)
	}
	if summary.QuotaCapped != 1 {
		t.Errorf("summary.QuotaCapped = %d, want 1", summary.QuotaCapped)
	}
	got, _ := store.GetWatch(ctx, w.WatchID)
	if !strings.Contains(got.QuotaCapReason, "capped at 2/5") {
		t.Errorf("reason = %q, want the level even without numbers", got.QuotaCapReason)
	}
	if strings.Contains(got.QuotaCapReason, "quota is") {
		t.Errorf("reason invented numbers with no lookup: %q", got.QuotaCapReason)
	}
}

// TestCappedFleetStillCompletesOnUntil: the suppression guard sits AFTER the
// completion-condition check, so a capped fleet still retires on --until instead
// of being stuck active until its TTL.
func TestCappedFleetStillCompletesOnUntil(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-capped-done")
	w.CompletionCondition = "s3-empty: s3://b/manifest/ minus s3://b/prepared/"
	w.QuotaCappedCount = 2
	w.QuotaCappedAt = time.Now().UTC()
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	launches := 0
	sp := &Spawner{
		s3: &fakeS3{counts: map[string]int32{"b/manifest/": 10, "b/prepared/": 10}}, // done
		provision: func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
			launches++
			return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
		},
	}
	p := &Poller{store: store, spawner: sp}

	p.pollFleetWatch(ctx, w, &PollSummary{})

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Status != StatusCompleted {
		t.Errorf("status = %q, want completed (a capped fleet must still retire on --until)", got.Status)
	}
	if launches != 0 {
		t.Errorf("launched %d, want 0", launches)
	}
}
