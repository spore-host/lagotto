package watcher

import (
	"context"
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
