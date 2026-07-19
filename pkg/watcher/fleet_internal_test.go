package watcher

import (
	"context"
	"encoding/json"
	"testing"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// TestCountRunningFleet counts only running/pending instances carrying this
// watch's FleetTagKey, de-duped, across the watch's regions.
func TestCountRunningFleet(t *testing.T) {
	w := &Watch{WatchID: "w-abc", DesiredCount: 4, Regions: []string{"us-east-1", "us-west-2"}}

	byRegion := map[string][]spawnaws.InstanceInfo{
		"us-east-1": {
			{InstanceID: "i-1", State: "running", Tags: map[string]string{FleetTagKey: "w-abc"}},
			{InstanceID: "i-2", State: "pending", Tags: map[string]string{FleetTagKey: "w-abc"}},
			{InstanceID: "i-term", State: "terminated", Tags: map[string]string{FleetTagKey: "w-abc"}}, // not counted
			{InstanceID: "i-other", State: "running", Tags: map[string]string{FleetTagKey: "w-zzz"}},   // other watch
			{InstanceID: "i-untagged", State: "running"},                                               // no fleet tag
		},
		"us-west-2": {
			{InstanceID: "i-3", State: "running", Tags: map[string]string{FleetTagKey: "w-abc"}},
		},
	}

	sp := &Spawner{listInstances: func(_ context.Context, region, _ string) ([]spawnaws.InstanceInfo, error) {
		return byRegion[region], nil
	}}

	n, err := sp.countRunningFleet(context.Background(), w)
	if err != nil {
		t.Fatalf("countRunningFleet: %v", err)
	}
	if n != 3 { // i-1, i-2 (east) + i-3 (west); terminated/other-watch/untagged excluded
		t.Errorf("countRunningFleet = %d, want 3", n)
	}
}

// TestSpawn_StampsFleetTag verifies a fleet watch (DesiredCount>0) tags its
// launched worker with FleetTagKey=WatchID, and a single-shot watch does not.
func TestSpawn_StampsFleetTag(t *testing.T) {
	cfgJSON, _ := json.Marshal(SpawnConfigFile{Name: "worker", InstanceType: "m8g.8xlarge", Region: "us-east-1", TTL: "4h"})

	run := func(desired int) spawnaws.LaunchConfig {
		var got spawnaws.LaunchConfig
		sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			got = cfg
			return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
		})
		w := &Watch{WatchID: "w-abc", DesiredCount: desired, LaunchConfigJSON: cfgJSON}
		m := &MatchResult{Region: "us-east-1", InstanceType: "m8g.8xlarge", CandidateAZs: []string{"us-east-1a"}}
		if err := sp.Spawn(context.Background(), w, m); err != nil {
			t.Fatalf("Spawn: %v", err)
		}
		return got
	}

	fleet := run(4)
	if fleet.Tags[FleetTagKey] != "w-abc" {
		t.Errorf("fleet launch tag = %q, want w-abc", fleet.Tags[FleetTagKey])
	}
	single := run(0)
	if _, ok := single.Tags[FleetTagKey]; ok {
		t.Errorf("single-shot launch should not carry the fleet tag, got %v", single.Tags)
	}
}
