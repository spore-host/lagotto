package watcher

import (
	"context"
	"testing"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// snipeTarget is a minimal valid single-target request.
func snipeTarget() SnipeTarget {
	return SnipeTarget{
		InstanceType: "g7e.2xlarge",
		Region:       "us-east-1",
		AZs:          []string{"us-east-1a", "us-east-1b"},
	}
}

// TestSnipe_AcquiresAfterCapacityRetries verifies the block-and-wait loop: the
// first two rounds hit capacity (all AZs), the third succeeds, and Snipe returns
// a MatchResult with the launched id/region/AZ. Sleep is faked so no real wait.
func TestSnipe_AcquiresAfterCapacityRetries(t *testing.T) {
	var rounds int
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		// Count a round on the first AZ attempt (us-east-1a) of each sweep.
		if cfg.AvailabilityZone == "us-east-1a" {
			rounds++
		}
		if rounds >= 3 {
			return &spawnaws.LaunchResult{InstanceID: "i-sniped"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	var slept int
	sp.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	m, err := sp.Snipe(context.Background(), snipeTarget(), SnipeOptions{RetryInterval: time.Second})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if m.InstanceID != "i-sniped" {
		t.Errorf("InstanceID = %q, want i-sniped", m.InstanceID)
	}
	if m.Region != "us-east-1" || m.AvailabilityZone != "us-east-1a" {
		t.Errorf("Region/AZ = %q/%q, want us-east-1/us-east-1a", m.Region, m.AvailabilityZone)
	}
	if m.ActionTaken != "sniped" {
		t.Errorf("ActionTaken = %q, want sniped", m.ActionTaken)
	}
	if rounds != 3 {
		t.Errorf("rounds = %d, want 3 (two capacity-failed, third succeeds)", rounds)
	}
	if slept != 2 {
		t.Errorf("slept = %d, want 2 (one backoff after each failed round)", slept)
	}
}

// TestSnipe_TerminalStopsImmediately verifies a terminal failure returns at once
// without backing off — retrying can't help.
func TestSnipe_TerminalStopsImmediately(t *testing.T) {
	var attempts int
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		return nil, &capErr{"AuthFailure"} // terminal per ClassifyFailure
	})
	var slept int
	sp.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	// One AZ so a single sweep = a single provision call.
	tgt := snipeTarget()
	tgt.AZs = []string{"us-east-1a"}
	_, err := sp.Snipe(context.Background(), tgt, SnipeOptions{})
	if err == nil {
		t.Fatal("expected terminal failure to return an error")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (terminal must not retry)", attempts)
	}
	if slept != 0 {
		t.Errorf("slept = %d, want 0 (no backoff on terminal failure)", slept)
	}
}

// TestSnipe_DeadlineReached verifies that persistent capacity failure past the
// deadline returns a timeout wrapping the last error, and stops looping.
func TestSnipe_DeadlineReached(t *testing.T) {
	// Freeze/advance a fake clock: start at t0; each sleep advances it so the
	// deadline is crossed deterministically without real time.
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cur := base
	orig := timeNow
	timeNow = func() time.Time { return cur }
	defer func() { timeNow = orig }()

	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"} // never any capacity
	})
	sp.sleep = func(_ context.Context, d time.Duration) error { cur = cur.Add(d); return nil }

	tgt := snipeTarget()
	tgt.AZs = []string{"us-east-1a"}
	opts := SnipeOptions{Deadline: base.Add(90 * time.Second), RetryInterval: time.Minute}

	_, err := sp.Snipe(context.Background(), tgt, opts)
	if err == nil {
		t.Fatal("expected a deadline error after persistent capacity failure")
	}
	// Must not hang / loop forever; the fake clock crossing the deadline ends it.
}

// TestSnipe_ContextCancelDuringBackoff verifies a cancelled context during the
// inter-round wait aborts the loop with the ctx error.
func TestSnipe_ContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	sp.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel() // simulate cancellation landing during the backoff wait
		return ctx.Err()
	}

	tgt := snipeTarget()
	tgt.AZs = []string{"us-east-1a"}
	_, err := sp.Snipe(ctx, tgt, SnipeOptions{RetryInterval: time.Second})
	if err == nil {
		t.Fatal("expected context cancellation to abort Snipe")
	}
}

// TestSnipe_ValidatesTarget verifies required-field validation.
func TestSnipe_ValidatesTarget(t *testing.T) {
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		t.Fatal("provision must not be called when the target is invalid")
		return nil, nil
	})
	if _, err := sp.Snipe(context.Background(), SnipeTarget{Region: "us-east-1"}, SnipeOptions{}); err == nil {
		t.Error("want error for missing InstanceType")
	}
	if _, err := sp.Snipe(context.Background(), SnipeTarget{InstanceType: "g7e.2xlarge"}, SnipeOptions{}); err == nil {
		t.Error("want error for missing Region")
	}
}

// TestSnipe_GuaranteesTTL is the #38 chokepoint applied to Snipe: a stored config
// with an empty TTL must still reach provision with the default TTL.
func TestSnipe_GuaranteesTTL(t *testing.T) {
	var gotTTL string
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		gotTTL = cfg.TTL
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})

	tgt := snipeTarget()
	tgt.AZs = []string{"us-east-1a"}
	tgt.LaunchConfig = SpawnConfigFile{TTL: ""} // explicitly empty
	if _, err := sp.Snipe(context.Background(), tgt, SnipeOptions{}); err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if gotTTL != DefaultInstanceTTL {
		t.Errorf("TTL = %q, want default %q (#38 guard)", gotTTL, DefaultInstanceTTL)
	}
}

// TestSnipe_FallbackRegionWithinRound verifies the multi-region extension (#76):
// when the primary region has no capacity, Snipe tries the fallback region within
// the SAME round (before any backoff) and returns the fallback's result.
func TestSnipe_FallbackRegionWithinRound(t *testing.T) {
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if cfg.Region == "us-west-2" {
			return &spawnaws.LaunchResult{InstanceID: "i-west"}, nil // capacity only in the fallback
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	var slept int
	sp.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	primary := snipeTarget() // us-east-1
	primary.AZs = []string{"us-east-1a"}
	fallback := SnipeTarget{InstanceType: "g7e.2xlarge", Region: "us-west-2", AZs: []string{"us-west-2a"}}

	m, err := sp.Snipe(context.Background(), primary, SnipeOptions{Fallbacks: []SnipeTarget{fallback}})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if m.InstanceID != "i-west" || m.Region != "us-west-2" {
		t.Errorf("got id=%q region=%q, want i-west/us-west-2 (fallback region acquired)", m.InstanceID, m.Region)
	}
	if slept != 0 {
		t.Errorf("slept = %d, want 0 (fallback succeeded in the first round, no backoff)", slept)
	}
}

// TestSnipe_FallbackTerminalStops verifies a terminal failure on a fallback target
// stops immediately — a bad AMI/IAM in a region isn't a capacity issue to retry.
func TestSnipe_FallbackTerminalStops(t *testing.T) {
	var attempts int
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		if cfg.Region == "us-west-2" {
			return nil, &capErr{"AuthFailure"} // terminal in the fallback region
		}
		return nil, &capErr{"InsufficientInstanceCapacity"} // capacity-fail primary
	})
	sp.sleep = func(context.Context, time.Duration) error {
		t.Fatal("must not back off before the terminal fallback")
		return nil
	}

	primary := snipeTarget()
	primary.AZs = []string{"us-east-1a"}
	fallback := SnipeTarget{InstanceType: "g7e.2xlarge", Region: "us-west-2", AZs: []string{"us-west-2a"}}
	_, err := sp.Snipe(context.Background(), primary, SnipeOptions{Fallbacks: []SnipeTarget{fallback}})
	if err == nil {
		t.Fatal("expected the fallback's terminal failure to stop Snipe")
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (primary capacity-fail, then terminal fallback)", attempts)
	}
}

// TestSnipe_FallbackValidated verifies fallback targets are validated too.
func TestSnipe_FallbackValidated(t *testing.T) {
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	primary := snipeTarget()
	primary.AZs = []string{"us-east-1a"}
	bad := SnipeTarget{Region: "us-west-2"} // missing InstanceType
	if _, err := sp.Snipe(context.Background(), primary, SnipeOptions{Fallbacks: []SnipeTarget{bad}}); err == nil {
		t.Error("want error for a fallback with missing InstanceType")
	}
}

// TestBackoffFor verifies the capped exponential schedule.
func TestBackoffFor(t *testing.T) {
	base, max := time.Second, 8*time.Second
	want := []time.Duration{1, 2, 4, 8, 8, 8}
	for round, w := range want {
		if got := backoffFor(round, base, max); got != w*time.Second {
			t.Errorf("backoffFor(%d) = %v, want %v", round, got, w*time.Second)
		}
	}
}
