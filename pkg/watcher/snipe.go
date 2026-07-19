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
	// Fallbacks is an optional ordered list of ADDITIONAL targets to try, in
	// order, within each round after the primary target's AZ sweep capacity-fails
	// — before backing off. This is the multi-region extension (#76): capacity is
	// bursty and region-uneven, and the AZ with capacity is often in a different
	// region than the one you picked. Each fallback is a full SnipeTarget because
	// cross-region is NOT free like AZ-breadth: it needs a region-specific AMI id,
	// in-region launch artifacts/SG/subnet, etc. — all of which the caller expresses
	// per-target. Off by default (empty); opt-in only. A terminal failure on any
	// target still stops immediately.
	Fallbacks []SnipeTarget
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

	// Build the per-target launch config(s) once. The primary target plus any
	// opt-in Fallbacks (#76) are each resolved to a (target, cfg) pair; each round
	// tries them in order before backing off.
	targets := append([]SnipeTarget{target}, opts.Fallbacks...)
	built := make([]builtTarget, 0, len(targets))
	for _, t := range targets {
		if strings.TrimSpace(t.InstanceType) == "" {
			return nil, fmt.Errorf("snipe: target InstanceType is required")
		}
		if strings.TrimSpace(t.Region) == "" {
			return nil, fmt.Errorf("snipe: target Region is required")
		}
		cfg, err := s.buildSnipeConfig(ctx, t)
		if err != nil {
			return nil, err
		}
		built = append(built, builtTarget{target: t, cfg: cfg})
	}

	var lastErr error
	for round := 0; ; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Try the primary then each fallback in order before backing off.
		for _, bt := range built {
			// Stop before an attempt if the deadline has already passed.
			if !opts.Deadline.IsZero() && !nowBefore(opts.Deadline) {
				return nil, fmt.Errorf("snipe: deadline reached without acquiring %s: %w",
					bt.target.InstanceType, lastErr)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			instanceID, az, err := s.launchAcrossAZs(ctx, bt.cfg, bt.target.AZs)
			if err == nil {
				return &MatchResult{
					Region:           bt.target.Region,
					AvailabilityZone: az,
					CandidateAZs:     bt.target.AZs,
					InstanceType:     bt.target.InstanceType,
					IsSpot:           bt.target.Spot,
					MatchedAt:        now(),
					InstanceID:       instanceID,
					ActionTaken:      "sniped",
				}, nil
			}

			lastErr = err
			// A terminal failure on this target will never resolve by waiting.
			// Stop now — a bad AMI/IAM/quota isn't a capacity problem another
			// region can paper over.
			if ClassifyFailure(err) == FailureTerminal {
				return nil, fmt.Errorf("snipe: terminal failure acquiring %s in %s: %w",
					bt.target.InstanceType, bt.target.Region, err)
			}
			// Capacity failure on this target → fall through to the next target.
		}

		// Every target capacity-failed this round: back off, then retry the sweep.
		wait := backoffFor(round, interval, maxInterval)
		if !opts.Deadline.IsZero() {
			if remaining := until(opts.Deadline); remaining <= 0 {
				return nil, fmt.Errorf("snipe: deadline reached without acquiring %s: %w",
					target.InstanceType, lastErr)
			} else if wait > remaining {
				wait = remaining // don't sleep past the deadline
			}
		}
		if err := sleep(ctx, wait); err != nil {
			return nil, err // ctx cancelled/expired during the wait
		}
	}
}

// builtTarget pairs a SnipeTarget with its resolved launch config.
type builtTarget struct {
	target SnipeTarget
	cfg    spawnaws.LaunchConfig
}

// buildSnipeConfig resolves a SnipeTarget into a launch config: guarantee a TTL
// (#38 — no unbounded instance can escape), pin type/region/spot, and provision
// the IAM instance profile from iam_policy shorthands (mirroring the watch-match
// path). AZ is set per-attempt in the sweep.
func (s *Spawner) buildSnipeConfig(ctx context.Context, target SnipeTarget) (spawnaws.LaunchConfig, error) {
	cfg := target.LaunchConfig.ToLaunchConfig()
	if strings.TrimSpace(cfg.TTL) == "" {
		cfg.TTL = DefaultInstanceTTL
	}
	cfg.Region = target.Region
	cfg.InstanceType = target.InstanceType
	cfg.Spot = target.Spot

	// When no iam_policy shorthands are given, Provision sets up the default spored
	// profile itself, so we leave IamInstanceProfile empty. Skipped when there's no
	// spawn client (unit tests inject a fake provision and no client).
	if len(target.LaunchConfig.IAMPolicies) > 0 && s.client != nil {
		profile, err := s.client.CreateOrGetInstanceProfile(ctx, spawnaws.IAMRoleConfig{
			Policies: target.LaunchConfig.IAMPolicies,
		})
		if err != nil {
			return spawnaws.LaunchConfig{}, fmt.Errorf("snipe: set up IAM instance profile: %w", err)
		}
		cfg.IamInstanceProfile = profile
	}
	return cfg, nil
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
