package watcher

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/smithy-go"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// capErr is a minimal smithy.APIError that ClassifyFailure treats as a capacity
// (retryable) failure.
type capErr struct{ code string }

func (e *capErr) Error() string                 { return e.code }
func (e *capErr) ErrorCode() string             { return e.code }
func (e *capErr) ErrorMessage() string          { return e.code }
func (e *capErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// newSpawnerWithProvision builds a Spawner whose launch is the supplied fake,
// so we can drive the AZ retry loop without a real AWS client.
func newSpawnerWithProvision(fn func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)) *Spawner {
	return &Spawner{provision: fn}
}

func minimalWatch(t *testing.T) *Watch {
	t.Helper()
	// A minimal stored config; with fsx_create on, spawn#213 means the launcher
	// only creates the FSx AFTER a successful launch — so a capacity-failed AZ
	// attempt creates zero filesystems. This test asserts the lagotto-side
	// contract (the AZ sweep) that relies on that guarantee (lagotto#45).
	cfg := SpawnConfigFile{
		InstanceType: "g5.12xlarge",
		Region:       "us-east-1",
		FSxCreate:    true,
		FSxLifecycle: "ephemeral",
		FSxS3Bucket:  "b",
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return &Watch{WatchID: "w-1", LaunchConfigJSON: raw}
}

// TestSpawn_CapacitySweepsAllAZs verifies lagotto#45: on a capacity failure the
// spawner falls through to the next candidate AZ, trying every offered AZ within
// one poll before giving up (#34). Each attempt goes through launcher.Provision,
// which on spawn ≥ 0.61.0 (#213) creates the ephemeral FSx only after a
// successful launch — so these failed attempts create zero filesystems.
func TestSpawn_CapacitySweepsAllAZs(t *testing.T) {
	var triedAZs []string
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		triedAZs = append(triedAZs, cfg.AvailabilityZone)
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})

	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a", "us-east-1b", "us-east-1c"}}
	err := sp.Spawn(context.Background(), minimalWatch(t), m)
	if err == nil {
		t.Fatal("expected error after exhausting all AZs on capacity failure")
	}
	want := []string{"us-east-1a", "us-east-1b", "us-east-1c"}
	if len(triedAZs) != len(want) {
		t.Fatalf("tried AZs = %v, want all %v (capacity failure must sweep every candidate AZ)", triedAZs, want)
	}
	for i, az := range want {
		if triedAZs[i] != az {
			t.Errorf("attempt %d AZ = %q, want %q", i, triedAZs[i], az)
		}
	}
	if m.ActionTaken != "spawn_failed" {
		t.Errorf("ActionTaken = %q, want spawn_failed", m.ActionTaken)
	}
}

// TestSpawn_SucceedsOnSecondAZ confirms the sweep stops at the first AZ that
// launches: AZ #3 is never attempted once #2 succeeds (so exactly one FSx is
// created — by the successful launch — never one per AZ).
func TestSpawn_SucceedsOnSecondAZ(t *testing.T) {
	var triedAZs []string
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		triedAZs = append(triedAZs, cfg.AvailabilityZone)
		if cfg.AvailabilityZone == "us-east-1b" {
			return &spawnaws.LaunchResult{InstanceID: "i-success"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})

	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a", "us-east-1b", "us-east-1c"}}
	if err := sp.Spawn(context.Background(), minimalWatch(t), m); err != nil {
		t.Fatalf("expected success on 2nd AZ, got %v", err)
	}
	if want := []string{"us-east-1a", "us-east-1b"}; len(triedAZs) != len(want) {
		t.Fatalf("tried AZs = %v, want %v (must stop at the first success, not attempt 1c)", triedAZs, want)
	}
	if m.InstanceID != "i-success" {
		t.Errorf("InstanceID = %q, want i-success", m.InstanceID)
	}
	if m.AvailabilityZone != "us-east-1b" || m.ActionTaken != "spawned" {
		t.Errorf("AZ = %q ActionTaken = %q, want us-east-1b/spawned", m.AvailabilityZone, m.ActionTaken)
	}
}

// TestSpawn_TerminalStopsImmediately confirms a terminal error (bad config) stops
// the sweep at the first AZ — retrying other AZs can't help, and we don't want to
// thrash creating launch attempts (or, pre-#213, filesystems) across AZs.
func TestSpawn_TerminalStopsImmediately(t *testing.T) {
	var attempts int
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		// AuthFailure is classified terminal by ClassifyFailure.
		return nil, &capErr{"AuthFailure"}
	})

	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a", "us-east-1b", "us-east-1c"}}
	if err := sp.Spawn(context.Background(), minimalWatch(t), m); err == nil {
		t.Fatal("expected terminal error to fail the spawn")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (terminal error must stop the AZ sweep immediately)", attempts)
	}
}

// TestSpawn_GuaranteesTTL is the #38 chokepoint guard: even when the stored
// config carries NO ttl (e.g. written by an older CLI before watch-create
// validation), the launch config handed to launcher.Provision must still have a
// TTL — never empty — so lagotto can't create an unbounded billable instance.
func TestSpawn_GuaranteesTTL(t *testing.T) {
	var gotTTL string
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		gotTTL = cfg.TTL
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})

	// Stored config with an explicitly-empty TTL (no ValidateAndDefaultTTL ran).
	cfg := SpawnConfigFile{InstanceType: "g5.12xlarge", Region: "us-east-1", TTL: ""}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	w := &Watch{WatchID: "w-nottl", LaunchConfigJSON: raw}
	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}

	if err := sp.Spawn(context.Background(), w, m); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if gotTTL == "" {
		t.Error("launch config reached provision with an EMPTY TTL — #38 guard failed (instance could run unbounded)")
	}
	if gotTTL != DefaultInstanceTTL {
		t.Errorf("TTL = %q, want default %q", gotTTL, DefaultInstanceTTL)
	}
}
