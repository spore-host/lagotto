package watcher

import (
	"context"
	"fmt"
	"strings"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

// Snipe defaults for the block-and-wait retry loop (#73).
const (
	// DefaultSnipeRetryInterval is the initial wait between capacity-failed rounds.
	DefaultSnipeRetryInterval = 30 * time.Second
	// DefaultSnipeMaxInterval caps the exponential backoff between rounds.
	DefaultSnipeMaxInterval = 5 * time.Minute
)

// timeNow is indirected so Snipe's deadline handling is testable without wall
// clock. Tests override it; production uses time.Now.
var timeNow = time.Now

func now() time.Time                  { return timeNow() }
func nowBefore(t time.Time) bool      { return timeNow().Before(t) }
func until(t time.Time) time.Duration { return t.Sub(timeNow()) }

// SnipeTarget is a single-target capacity acquire request for [Spawner.Snipe].
// It is the stateless, DynamoDB-free counterpart to a persisted Watch: no store,
// no poll cycle — just "acquire this type here, blocking, and hand me the result."
type SnipeTarget struct {
	// InstanceType is the exact EC2 type to acquire, e.g. "g7e.2xlarge". Required.
	// (Unlike Watch.InstanceTypePattern, this is not a wildcard — Snipe is a
	// single-target acquire, not a multi-candidate search.)
	InstanceType string
	// Region is the region to launch in, e.g. "us-east-1". Required.
	Region string
	// AZs optionally pins/orders the candidate AZs tried within each round (AZ
	// breadth within a region is free). Empty = a single AZ-unpinned attempt per
	// round (EC2 chooses the AZ).
	AZs []string
	// Spot requests a Spot instance instead of On-Demand.
	Spot bool
	// LaunchConfig is the stored spawn config (same shape a watch carries). TTL is
	// guaranteed (defaulted if empty) so a sniped instance can never run unbounded,
	// and iam_policy shorthands are provisioned into an instance profile — exactly
	// as the watch-match path does.
	LaunchConfig SpawnConfigFile
}

// SnipeOptions tunes the retry loop. The zero value is valid: it relies on the
// context for the stopping bound and uses the default backoff.
type SnipeOptions struct {
	// Deadline is a hard stop; on reaching it Snipe returns the last capacity
	// error wrapped as a timeout. Zero means "bounded only by ctx" — callers
	// SHOULD set either a Deadline or a ctx deadline so the loop can't run forever.
	Deadline time.Time
	// RetryInterval is the initial backoff between capacity-failed rounds; it
	// doubles each round up to MaxInterval. Zero = DefaultSnipeRetryInterval.
	RetryInterval time.Duration
	// MaxInterval caps the backoff. Zero = DefaultSnipeMaxInterval.
	MaxInterval time.Duration
}

// Snipe blocks until it acquires the target instance, the deadline passes, or a
// terminal failure occurs. It is the library primitive for "block-and-wait,
// single-target acquire" (#73): a thin, stateless wrapper over spawn's
// launcher.Provision that owns the capacity classify + retry loop, so an
// embedding consumer needs neither the persisted-Watch/DynamoDB machinery nor a
// reimplementation of the ClassifyFailure retry policy.
//
// Each round is one full AZ sweep (launchAcrossAZs): on InsufficientInstance
// Capacity across all candidate AZs it backs off and retries; on a terminal
// failure (bad AMI/IAM, quota, post-launch teardown) it returns immediately —
// retrying can't help. On success it returns a *MatchResult carrying the launched
// InstanceID, Region, and AZ. The result mirrors the watch-match path; to get the
// public IP or drive lifecycle, use spawn's client with the returned Region +
// InstanceID (spawn's LaunchResult already exposes PublicIP on launch, and the
// (region, instanceID) methods cover state/terminate).
func (s *Spawner) Snipe(ctx context.Context, target SnipeTarget, opts SnipeOptions) (*MatchResult, error) {
	if strings.TrimSpace(target.InstanceType) == "" {
		return nil, fmt.Errorf("snipe: target InstanceType is required")
	}
	if strings.TrimSpace(target.Region) == "" {
		return nil, fmt.Errorf("snipe: target Region is required")
	}

	interval := opts.RetryInterval
	if interval <= 0 {
		interval = DefaultSnipeRetryInterval
	}
	maxInterval := opts.MaxInterval
	if maxInterval <= 0 {
		maxInterval = DefaultSnipeMaxInterval
	}
	sleep := s.sleep
	if sleep == nil {
		sleep = sleepCtx
	}

	// Build the launch config once: guarantee a TTL (#38 — no unbounded instance
	// can escape) and pin type/region/spot. AZ is set per-attempt in the sweep.
	cfg := target.LaunchConfig.ToLaunchConfig()
	if strings.TrimSpace(cfg.TTL) == "" {
		cfg.TTL = DefaultInstanceTTL
	}
	cfg.Region = target.Region
	cfg.InstanceType = target.InstanceType
	cfg.Spot = target.Spot

	// Provision the IAM instance profile from iam_policy shorthands, mirroring the
	// watch-match path. When none are given, Provision sets up the default spored
	// profile itself, so we leave IamInstanceProfile empty. Skipped when there's no
	// spawn client (unit tests inject a fake provision and no client).
	if len(target.LaunchConfig.IAMPolicies) > 0 && s.client != nil {
		profile, err := s.client.CreateOrGetInstanceProfile(ctx, spawnaws.IAMRoleConfig{
			Policies: target.LaunchConfig.IAMPolicies,
		})
		if err != nil {
			return nil, fmt.Errorf("snipe: set up IAM instance profile: %w", err)
		}
		cfg.IamInstanceProfile = profile
	}

	attempts := target.AZs

	var lastErr error
	for round := 0; ; round++ {
		// Stop before an attempt if the deadline has already passed.
		if !opts.Deadline.IsZero() && !nowBefore(opts.Deadline) {
			return nil, fmt.Errorf("snipe: deadline reached without acquiring %s in %s: %w",
				target.InstanceType, target.Region, lastErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		instanceID, az, err := s.launchAcrossAZs(ctx, cfg, attempts)
		if err == nil {
			return &MatchResult{
				Region:           target.Region,
				AvailabilityZone: az,
				CandidateAZs:     attempts,
				InstanceType:     target.InstanceType,
				IsSpot:           target.Spot,
				MatchedAt:        now(),
				InstanceID:       instanceID,
				ActionTaken:      "sniped",
			}, nil
		}

		lastErr = err
		// A terminal failure will never resolve by waiting — stop now.
		if ClassifyFailure(err) == FailureTerminal {
			return nil, fmt.Errorf("snipe: terminal failure acquiring %s in %s: %w",
				target.InstanceType, target.Region, err)
		}

		// Capacity failure: back off (bounded by the deadline), then retry.
		wait := backoffFor(round, interval, maxInterval)
		if !opts.Deadline.IsZero() {
			if remaining := until(opts.Deadline); remaining <= 0 {
				return nil, fmt.Errorf("snipe: deadline reached without acquiring %s in %s: %w",
					target.InstanceType, target.Region, lastErr)
			} else if wait > remaining {
				wait = remaining // don't sleep past the deadline
			}
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, err // ctx cancelled/expired during the wait
		}
	}
}

// Snipe is the package-level convenience: it constructs a Spawner from ambient
// AWS credentials and blocks until it acquires the target (see [Spawner.Snipe]).
// Prefer the method when you already hold a Spawner or want to inject test fakes.
func Snipe(ctx context.Context, target SnipeTarget, opts SnipeOptions) (*MatchResult, error) {
	s, err := NewSpawner(ctx)
	if err != nil {
		return nil, fmt.Errorf("snipe: create spawner: %w", err)
	}
	return s.Snipe(ctx, target, opts)
}

// backoffFor returns the capped exponential backoff for a given round: base,
// 2*base, 4*base, … clamped to max.
func backoffFor(round int, base, max time.Duration) time.Duration {
	d := base
	for i := 0; i < round; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

// sleepCtx sleeps for d or returns early with the context's error if it is
// cancelled/expires first. It's the default Spawner.sleep.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
