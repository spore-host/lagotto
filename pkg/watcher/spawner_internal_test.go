package watcher

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestSpawn_PostLaunchStopsImmediately is the lagotto half of spawn#220: when
// Provision returns ErrPostLaunch (RunInstances SUCCEEDED, then a downstream step
// failed and Provision tore the instance down), the AZ sweep must STOP — not march
// to the next AZ launching another instance+FSx per attempt. The launch worked;
// capacity exists; the problem is downstream, so retrying other AZs only churns.
func TestSpawn_PostLaunchStopsImmediately(t *testing.T) {
	var attempts int
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		// Mirror spawn's wrapping: ErrPostLaunch inside an fmt.Errorf chain.
		return nil, fmt.Errorf("provision: %w: FSx setup failed, terminated instance i-x", launcher.ErrPostLaunch)
	})

	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a", "us-east-1b", "us-east-1c"}}
	if err := sp.Spawn(context.Background(), minimalWatch(t), m); err == nil {
		t.Fatal("expected post-launch error to fail the spawn")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (a post-launch failure must NOT retry other AZs — spawn#220)", attempts)
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

// scheduledLaunch builds a one-AZ ScheduledLaunch with the given Name + IfExists.
func scheduledLaunch(t *testing.T, name, ifExists string) *ScheduledLaunch {
	t.Helper()
	raw, err := json.Marshal(SpawnConfigFile{Name: name, InstanceType: "g5.12xlarge", Region: "us-east-1", TTL: "24h"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &ScheduledLaunch{ScheduleID: "sl-1", Region: "us-east-1", AvailabilityZone: "us-east-1a", InstanceName: name, IfExists: ifExists, LaunchConfigJSON: raw}
}

// TestLaunchScheduled_SkipWhenExists: a one-shot's default skip policy must NOT
// launch when an instance with the same Name already exists — it returns the
// existing id (a --at into a Capacity Block can't double-book).
func TestLaunchScheduled_SkipWhenExists(t *testing.T) {
	launched := false
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		launched = true
		return &spawnaws.LaunchResult{InstanceID: "i-new"}, nil
	})
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
		return []spawnaws.InstanceInfo{{InstanceID: "i-existing", Name: "block-job"}}, nil
	}

	id, err := sp.LaunchScheduled(context.Background(), scheduledLaunch(t, "block-job", IfExistsSkip))
	if err != nil {
		t.Fatalf("LaunchScheduled: %v", err)
	}
	if launched {
		t.Error("skip policy launched a new instance despite an existing one")
	}
	if id != "i-existing" {
		t.Errorf("id = %q, want the existing i-existing", id)
	}
}

// TestLaunchScheduled_ReplaceTerminatesThenLaunches: replace terminates the
// existing instance, then launches a fresh one.
func TestLaunchScheduled_ReplaceTerminatesThenLaunches(t *testing.T) {
	var terminated string
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-new"}, nil
	})
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
		return []spawnaws.InstanceInfo{{InstanceID: "i-old", Name: "nightly"}}, nil
	}
	sp.terminateInstance = func(_ context.Context, _, id string) error {
		terminated = id
		return nil
	}

	id, err := sp.LaunchScheduled(context.Background(), scheduledLaunch(t, "nightly", IfExistsReplace))
	if err != nil {
		t.Fatalf("LaunchScheduled: %v", err)
	}
	if terminated != "i-old" {
		t.Errorf("terminated = %q, want i-old", terminated)
	}
	if id != "i-new" {
		t.Errorf("id = %q, want i-new", id)
	}
}

// TestLaunchScheduled_LaunchPolicySkipsLookup: the launch policy (cron default)
// launches unconditionally and never even queries for an existing instance.
func TestLaunchScheduled_LaunchPolicySkipsLookup(t *testing.T) {
	listed := false
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-fresh"}, nil
	})
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
		listed = true
		return nil, nil
	}

	id, err := sp.LaunchScheduled(context.Background(), scheduledLaunch(t, "cron-box", IfExistsLaunch))
	if err != nil {
		t.Fatalf("LaunchScheduled: %v", err)
	}
	if listed {
		t.Error("launch policy should not query existing instances")
	}
	if id != "i-fresh" {
		t.Errorf("id = %q, want i-fresh", id)
	}
}

// TestLaunchScheduled_SkipLaunchesWhenAbsent: skip policy still launches when no
// matching instance exists.
func TestLaunchScheduled_SkipLaunchesWhenAbsent(t *testing.T) {
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-first"}, nil
	})
	sp.listInstances = func(context.Context, string, string) ([]spawnaws.InstanceInfo, error) {
		return []spawnaws.InstanceInfo{{InstanceID: "i-other", Name: "different-name"}}, nil
	}

	id, err := sp.LaunchScheduled(context.Background(), scheduledLaunch(t, "block-job", IfExistsSkip))
	if err != nil {
		t.Fatalf("LaunchScheduled: %v", err)
	}
	if id != "i-first" {
		t.Errorf("id = %q, want i-first (no overlap → launch)", id)
	}
}
