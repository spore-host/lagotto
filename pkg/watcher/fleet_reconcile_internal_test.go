package watcher

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/spore-host/lagotto/pkg/testutil"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
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
	if got != 3 {
		t.Errorf("filled %d, want 3", got)
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
	if got != 2 {
		t.Errorf("filled %d, want 2 (capacity failed on the 3rd)", got)
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
