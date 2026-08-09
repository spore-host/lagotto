package snipe

import (
	"context"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/failure"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// capErr is a minimal smithy.APIError that failure.ClassifyFailure treats as a
// capacity (retryable) failure, mirroring pkg/watcher's test helper.
type capErr struct{ code string }

func (e *capErr) Error() string                 { return e.code }
func (e *capErr) ErrorCode() string             { return e.code }
func (e *capErr) ErrorMessage() string          { return e.code }
func (e *capErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// newAcquirerWithProvide builds an *acquirer whose launch is the supplied fake,
// so the retry loop is testable without a real AWS client.
func newAcquirerWithProvide(fn func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)) *acquirer {
	return &acquirer{provide: fn}
}

// newAcquirerWithRegionSpy builds an *acquirer that records every region
// clientFor is asked to resolve (#111 regression guard) and hands provide a
// distinct, non-nil *spawnaws.Client per region, so a test can assert both
// WHICH region was resolved and that the RIGHT client for that region reached
// provide (not a client built for a different target).
func newAcquirerWithRegionSpy(t *testing.T, fn func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)) (*acquirer, *[]string) {
	t.Helper()
	var requested []string
	clients := map[string]*spawnaws.Client{}
	return &acquirer{
		provide: fn,
		clientFor: func(_ context.Context, region string) (*spawnaws.Client, error) {
			requested = append(requested, region)
			c, ok := clients[region]
			if !ok {
				c = &spawnaws.Client{} // distinct pointer identity per region
				clients[region] = c
			}
			return c, nil
		},
	}, &requested
}

// target is a minimal valid single-target request.
func target() Target {
	return Target{
		InstanceType: "g7e.2xlarge",
		Region:       "us-east-1",
		Placements:   []Placement{{AZ: "us-east-1a"}, {AZ: "us-east-1b"}},
	}
}

// TestSnipe_AcquiresAfterCapacityRetries verifies the block-and-wait loop: the
// first two rounds hit capacity (all placements), the third succeeds, and Snipe
// returns a Result with the launched id/region/AZ. Sleep is faked so no real wait.
func TestSnipe_AcquiresAfterCapacityRetries(t *testing.T) {
	var rounds int
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if cfg.AvailabilityZone == "us-east-1a" {
			rounds++
		}
		if rounds >= 3 {
			return &spawnaws.LaunchResult{InstanceID: "i-sniped"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	var slept int
	a.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	r, err := snipeWith(context.Background(), a, target(), Options{RetryInterval: time.Second})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if r.InstanceID != "i-sniped" {
		t.Errorf("InstanceID = %q, want i-sniped", r.InstanceID)
	}
	if r.Region != "us-east-1" || r.AvailabilityZone != "us-east-1a" {
		t.Errorf("Region/AZ = %q/%q, want us-east-1/us-east-1a", r.Region, r.AvailabilityZone)
	}
	if rounds != 3 {
		t.Errorf("rounds = %d, want 3 (two capacity-failed, third succeeds)", rounds)
	}
	if slept != 2 {
		t.Errorf("slept = %d, want 2 (one backoff after each failed round)", slept)
	}
}

// TestSnipe_SubnetPerPlacement verifies a Placement's Subnet reaches the launch
// config for the AZ that acquires, and is reported back on Result (#106: some
// AZs have no default subnet, so a caller needs to say so per AZ).
func TestSnipe_SubnetPerPlacement(t *testing.T) {
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if cfg.AvailabilityZone == "us-west-2d" {
			if cfg.SubnetID != "subnet-only-in-2d" {
				t.Errorf("SubnetID = %q, want subnet-only-in-2d", cfg.SubnetID)
			}
			return &spawnaws.LaunchResult{InstanceID: "i-2d"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})

	tgt := Target{
		InstanceType: "g7e.2xlarge",
		Region:       "us-west-2",
		Placements: []Placement{
			{AZ: "us-west-2a"},
			{AZ: "us-west-2d", Subnet: "subnet-only-in-2d"},
		},
	}
	r, err := snipeWith(context.Background(), a, tgt, Options{})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if r.AvailabilityZone != "us-west-2d" || r.Subnet != "subnet-only-in-2d" {
		t.Errorf("AZ/Subnet = %q/%q, want us-west-2d/subnet-only-in-2d", r.AvailabilityZone, r.Subnet)
	}
}

// TestSnipe_TerminalStopsImmediately verifies a terminal failure returns at once
// without backing off — retrying can't help.
func TestSnipe_TerminalStopsImmediately(t *testing.T) {
	var attempts int
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		return nil, &capErr{"AuthFailure"} // terminal per failure.ClassifyFailure
	})
	var slept int
	a.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	_, err := snipeWith(context.Background(), a, tgt, Options{})
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

// TestSnipe_UnknownFailureCappedThenGivesUp verifies #106: a persistent
// unrecognized failure retries up to MaxConsecutiveUnknown times, then Snipe
// gives up rather than retrying it as a capacity wait for the whole deadline.
func TestSnipe_UnknownFailureCappedThenGivesUp(t *testing.T) {
	var attempts int
	a := newAcquirerWithProvide(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		return nil, &capErr{"SomeNewUnrecognizedCode"} // FailureUnknown
	})
	var slept int
	a.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	_, err := snipeWith(context.Background(), a, tgt, Options{MaxConsecutiveUnknown: 3, RetryInterval: time.Second})
	if err == nil {
		t.Fatal("expected Snipe to give up after MaxConsecutiveUnknown")
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (MaxConsecutiveUnknown)", attempts)
	}
	if slept != 2 {
		t.Errorf("slept = %d, want 2 (backoff between the 3 attempts, none after the last)", slept)
	}
}

// TestSnipe_CapacityResetsUnknownCounter verifies a recognized capacity failure
// in between unknown failures resets the consecutive-unknown counter, so an
// occasional unknown blip amid genuine capacity waits doesn't trip the cap.
func TestSnipe_CapacityResetsUnknownCounter(t *testing.T) {
	var attempts int
	a := newAcquirerWithProvide(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		switch {
		case attempts <= 2:
			return nil, &capErr{"SomeNewUnrecognizedCode"} // FailureUnknown x2
		case attempts == 3:
			return nil, &capErr{"InsufficientInstanceCapacity"} // resets the counter
		case attempts <= 5:
			return nil, &capErr{"SomeNewUnrecognizedCode"} // FailureUnknown x2 again
		default:
			return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
		}
	})
	a.sleep = func(context.Context, time.Duration) error { return nil }

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	// Cap is 3: two consecutive unknowns (1,2) don't trip it alone, the capacity
	// failure at attempt 3 resets the counter, so the next two unknowns (4,5)
	// don't trip it either, and attempt 6 succeeds. Without the reset, unknowns
	// 1,2,4,5 would total 4 and trip a cap of 3.
	r, err := snipeWith(context.Background(), a, tgt, Options{MaxConsecutiveUnknown: 3, RetryInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("Snipe: %v (want success after the reset)", err)
	}
	if r.InstanceID != "i-ok" {
		t.Errorf("InstanceID = %q, want i-ok", r.InstanceID)
	}
}

// TestSnipe_ProgressReported verifies Options.Progress is called with
// increasing rounds and the classified failure kind, so a caller can drive a
// live status line (#106).
func TestSnipe_ProgressReported(t *testing.T) {
	var calls int
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if calls >= 4 { // enough capacity-fail progress calls have landed
			return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	a.sleep = func(context.Context, time.Duration) error { return nil }

	var statuses []Status
	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	_, err := snipeWith(context.Background(), a, tgt, Options{
		RetryInterval: time.Millisecond,
		Progress:      func(s Status) { calls++; statuses = append(statuses, s) },
	})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if len(statuses) == 0 {
		t.Fatal("Progress was never called")
	}
	sawFailure := false
	for _, s := range statuses {
		if s.LastErr != nil {
			sawFailure = true
			if failure.Label(s.Kind) == "" {
				t.Errorf("Status.Kind has no label for a reported failure")
			}
		}
	}
	if !sawFailure {
		t.Error("Progress was never called with a classified failure")
	}
}

// TestSnipe_DeadlineReached verifies that persistent capacity failure past the
// deadline returns a timeout wrapping the last error, and stops looping.
func TestSnipe_DeadlineReached(t *testing.T) {
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	cur := base
	orig := timeNow
	timeNow = func() time.Time { return cur }
	defer func() { timeNow = orig }()

	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	a.sleep = func(_ context.Context, d time.Duration) error { cur = cur.Add(d); return nil }

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	opts := Options{Deadline: base.Add(90 * time.Second), RetryInterval: time.Minute}

	if _, err := snipeWith(context.Background(), a, tgt, opts); err == nil {
		t.Fatal("expected a deadline error after persistent capacity failure")
	}
}

// TestSnipe_ContextCancelDuringBackoff verifies a cancelled context during the
// inter-round wait aborts the loop with the ctx error.
func TestSnipe_ContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, _ spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	a.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	if _, err := snipeWith(ctx, a, tgt, Options{RetryInterval: time.Second}); err == nil {
		t.Fatal("expected context cancellation to abort Snipe")
	}
}

// TestSnipe_ValidatesTarget verifies required-field validation.
func TestSnipe_ValidatesTarget(t *testing.T) {
	a := newAcquirerWithProvide(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		t.Fatal("provide must not be called when the target is invalid")
		return nil, nil
	})
	if _, err := snipeWith(context.Background(), a, Target{Region: "us-east-1"}, Options{}); err == nil {
		t.Error("want error for missing InstanceType")
	}
	if _, err := snipeWith(context.Background(), a, Target{InstanceType: "g7e.2xlarge"}, Options{}); err == nil {
		t.Error("want error for missing Region")
	}
}

// TestSnipe_GuaranteesTTL is the "everything dies" chokepoint applied to Snipe:
// a config with an empty TTL must still reach provide with the default TTL.
func TestSnipe_GuaranteesTTL(t *testing.T) {
	var gotTTL string
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		gotTTL = cfg.TTL
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})

	tgt := target()
	tgt.Placements = []Placement{{AZ: "us-east-1a"}}
	tgt.LaunchConfig = spawnaws.LaunchConfig{TTL: ""} // explicitly empty
	if _, err := snipeWith(context.Background(), a, tgt, Options{}); err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if gotTTL != DefaultInstanceTTL {
		t.Errorf("TTL = %q, want default %q", gotTTL, DefaultInstanceTTL)
	}
}

// TestSnipe_FallbackRegionWithinRound verifies the multi-region extension (#76):
// when the primary region has no capacity, Snipe tries the fallback region within
// the SAME round (before any backoff) and returns the fallback's result.
func TestSnipe_FallbackRegionWithinRound(t *testing.T) {
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if cfg.Region == "us-west-2" {
			return &spawnaws.LaunchResult{InstanceID: "i-west"}, nil
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	var slept int
	a.sleep = func(context.Context, time.Duration) error { slept++; return nil }

	primary := target()
	primary.Placements = []Placement{{AZ: "us-east-1a"}}
	fallback := Target{InstanceType: "g7e.2xlarge", Region: "us-west-2", Placements: []Placement{{AZ: "us-west-2a"}}}

	r, err := snipeWith(context.Background(), a, primary, Options{Fallbacks: []Target{fallback}})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if r.InstanceID != "i-west" || r.Region != "us-west-2" {
		t.Errorf("got id=%q region=%q, want i-west/us-west-2 (fallback region acquired)", r.InstanceID, r.Region)
	}
	if slept != 0 {
		t.Errorf("slept = %d, want 0 (fallback succeeded in the first round, no backoff)", slept)
	}
}

// TestSnipe_FallbackTerminalStops verifies a terminal failure on a fallback
// target stops immediately — a bad AMI/IAM in a region isn't a capacity issue.
func TestSnipe_FallbackTerminalStops(t *testing.T) {
	var attempts int
	a := newAcquirerWithProvide(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		attempts++
		if cfg.Region == "us-west-2" {
			return nil, &capErr{"AuthFailure"}
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	a.sleep = func(context.Context, time.Duration) error {
		t.Fatal("must not back off before the terminal fallback")
		return nil
	}

	primary := target()
	primary.Placements = []Placement{{AZ: "us-east-1a"}}
	fallback := Target{InstanceType: "g7e.2xlarge", Region: "us-west-2", Placements: []Placement{{AZ: "us-west-2a"}}}
	_, err := snipeWith(context.Background(), a, primary, Options{Fallbacks: []Target{fallback}})
	if err == nil {
		t.Fatal("expected the fallback's terminal failure to stop Snipe")
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (primary capacity-fail, then terminal fallback)", attempts)
	}
}

// TestSnipe_FallbackValidated verifies fallback targets are validated too.
func TestSnipe_FallbackValidated(t *testing.T) {
	a := newAcquirerWithProvide(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	primary := target()
	primary.Placements = []Placement{{AZ: "us-east-1a"}}
	bad := Target{Region: "us-west-2"} // missing InstanceType
	if _, err := snipeWith(context.Background(), a, primary, Options{Fallbacks: []Target{bad}}); err == nil {
		t.Error("want error for a fallback with missing InstanceType")
	}
}

// TestSnipe_ClientResolvedForTargetRegion is the #111 regression guard: Snipe
// must resolve its AWS client PINNED TO the target's region, not the ambient
// default-credential-chain region — spawn#276 is exactly the bug this avoids
// (AMI/AZ/identity resolution silently happening in the wrong region).
func TestSnipe_ClientResolvedForTargetRegion(t *testing.T) {
	a, requested := newAcquirerWithRegionSpy(t, func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})
	tgt := Target{InstanceType: "g7e.2xlarge", Region: "eu-west-1", Placements: []Placement{{AZ: "eu-west-1a"}}}
	if _, err := snipeWith(context.Background(), a, tgt, Options{}); err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if len(*requested) != 1 || (*requested)[0] != "eu-west-1" {
		t.Errorf("clientFor requested regions = %v, want [eu-west-1]", *requested)
	}
}

// TestSnipe_FallbackUsesItsOwnRegionClient is the multi-region half of #111:
// a Fallback target in a DIFFERENT region from the primary must get a client
// resolved for ITS OWN region, not the primary's (or a single shared ambient
// client) — the exact scenario Options.Fallbacks (#76) exists for.
func TestSnipe_FallbackUsesItsOwnRegionClient(t *testing.T) {
	a, requested := newAcquirerWithRegionSpy(t, func(_ context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if cfg.Region == "us-west-2" {
			return &spawnaws.LaunchResult{InstanceID: "i-west"}, nil // capacity only in the fallback region
		}
		return nil, &capErr{"InsufficientInstanceCapacity"}
	})
	primary := target() // us-east-1
	primary.Placements = []Placement{{AZ: "us-east-1a"}}
	fallback := Target{InstanceType: "g7e.2xlarge", Region: "us-west-2", Placements: []Placement{{AZ: "us-west-2a"}}}

	r, err := snipeWith(context.Background(), a, primary, Options{Fallbacks: []Target{fallback}})
	if err != nil {
		t.Fatalf("Snipe: %v", err)
	}
	if r.InstanceID != "i-west" || r.Region != "us-west-2" {
		t.Fatalf("got id=%q region=%q, want i-west/us-west-2", r.InstanceID, r.Region)
	}
	if len(*requested) != 2 || (*requested)[0] != "us-east-1" || (*requested)[1] != "us-west-2" {
		t.Errorf("clientFor requested regions = %v, want [us-east-1 us-west-2] (each target resolved against ITS OWN region)", *requested)
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
