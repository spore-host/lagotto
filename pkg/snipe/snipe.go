// Package snipe is a dependency-light leaf for "block-and-wait, single-target
// capacity acquire" (#73, extracted from pkg/watcher in #106).
//
// It imports only the standard library, smithy-go, pkg/failure (itself a leaf),
// and spawn's pkg/aws + pkg/launcher — NOT pkg/watcher's stateful poller tree
// (DynamoDB, S3, SageMaker, SNS). A consumer that wants "acquire this instance
// type here, blocking, and hand me the result" — calque's original ask — no
// longer needs the persisted-Watch/poll-cycle machinery or a copy of the
// classify+retry loop to get it.
//
// watcher.Snipe / watcher.Spawner.Snipe still exist, unchanged, for existing
// callers of that API; this package is the recommended entry point for new
// callers (see watcher.Spawner.Snipe's doc for why they don't delegate here).
package snipe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spore-host/lagotto/pkg/failure"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// Defaults for the block-and-wait retry loop (#73).
const (
	// DefaultRetryInterval is the initial wait between capacity-failed rounds.
	DefaultRetryInterval = 30 * time.Second
	// DefaultMaxInterval caps the exponential backoff between rounds.
	DefaultMaxInterval = 5 * time.Minute
	// DefaultMaxConsecutiveUnknown caps how many consecutive FailureUnknown
	// results Snipe will retry before giving up (#106). FailureUnknown covers
	// both a genuinely transient blip AND an unrecognized-but-persistent fault;
	// retrying a handful of times absorbs the former without letting the latter
	// masquerade as a capacity wait for the whole deadline.
	DefaultMaxConsecutiveUnknown = 3
	// DefaultInstanceTTL is applied when Target.LaunchConfig carries no TTL, so a
	// sniped instance can never run unbounded (mirrors watcher.DefaultInstanceTTL).
	DefaultInstanceTTL = "24h"
)

// timeNow is indirected so deadline handling is testable without wall clock.
var timeNow = time.Now

func now() time.Time                  { return timeNow() }
func nowBefore(t time.Time) bool      { return timeNow().Before(t) }
func until(t time.Time) time.Duration { return t.Sub(timeNow()) }

// Placement pins one candidate AZ, optionally with its subnet. Subnet matters
// because some AZs have no default subnet — passing AZ alone yields
// EC2's InvalidInput there (observed: us-west-2d for g7e) — so a caller that
// knows its subnet layout must be able to say so per AZ, not just per region.
type Placement struct {
	// AZ is the candidate availability zone, e.g. "us-east-1a". Required.
	AZ string
	// Subnet is the VPC subnet ID to use in this AZ. Optional; empty lets EC2 use
	// the default subnet for AZ (which fails if AZ has none).
	Subnet string
}

// Target is a single-target capacity acquire request for [Snipe]. It is the
// stateless, DynamoDB-free counterpart to a persisted watcher.Watch: no store,
// no poll cycle — just "acquire this type here, blocking, and hand me the
// result."
type Target struct {
	// InstanceType is the exact EC2 type to acquire, e.g. "g7e.2xlarge". Required.
	// This is a single-target acquire, not a multi-candidate search.
	InstanceType string
	// Region is the region to launch in, e.g. "us-east-1". Required.
	Region string
	// Placements optionally pins/orders the candidate AZs (and their subnets)
	// tried within each round — AZ breadth within a region is free. Empty = a
	// single AZ-unpinned attempt per round (EC2 chooses the AZ and its default
	// subnet).
	Placements []Placement
	// Spot requests a Spot instance instead of On-Demand.
	Spot bool
	// LaunchConfig is the launch config to use, with InstanceType/Region/Spot/
	// AvailabilityZone/SubnetID overridden per attempt by Snipe. TTL is
	// guaranteed (defaulted to DefaultInstanceTTL if empty) so a sniped instance
	// can never run unbounded.
	LaunchConfig spawnaws.LaunchConfig
}

// Status reports live retry progress to Options.Progress (#106) — the missing
// piece for a caller that wants to show a "waiting for capacity..." line
// without polling anything itself.
type Status struct {
	// Round is the 0-indexed sweep number (one full pass over Target + any
	// Fallbacks, trying every Placement of each).
	Round int
	// Target is the InstanceType currently being attempted.
	Target string
	// Region is the Region currently being attempted.
	Region string
	// LastErr is the most recent attempt's error, classified. Nil on the very
	// first attempt of the very first round.
	LastErr error
	// Kind classifies LastErr (zero value failure.FailureNone on the first
	// attempt).
	Kind failure.FailureKind
	// Wait is how long Snipe will sleep before the next round, or zero if this
	// Status precedes an attempt rather than a backoff.
	Wait time.Duration
}

// Options tunes the retry loop. The zero value is valid: it relies on the
// context for the stopping bound, uses the default backoff, and caps
// consecutive unknown failures at DefaultMaxConsecutiveUnknown.
type Options struct {
	// Deadline is a hard stop; on reaching it Snipe returns the last error
	// wrapped as a timeout. Zero means "bounded only by ctx" — callers SHOULD
	// set either a Deadline or a ctx deadline so the loop can't run forever.
	Deadline time.Time
	// RetryInterval is the initial backoff between capacity-failed rounds; it
	// doubles each round up to MaxInterval. Zero = DefaultRetryInterval.
	RetryInterval time.Duration
	// MaxInterval caps the backoff. Zero = DefaultMaxInterval.
	MaxInterval time.Duration
	// MaxConsecutiveUnknown bounds how many consecutive FailureUnknown results
	// Snipe retries before giving up (#106) — without a cap, a persistent but
	// unrecognized fault (e.g. a misconfiguration whose error code isn't in
	// pkg/failure's taxonomy) retries silently for the entire Deadline instead of
	// failing fast with a clear signal. A recognized FailureCapacity result
	// always resets the counter and is never capped — genuine capacity waits may
	// legitimately run for days. Zero = DefaultMaxConsecutiveUnknown; negative
	// disables the cap entirely.
	MaxConsecutiveUnknown int
	// Progress, if set, is called before each attempt and before each backoff
	// sleep (#106), so a caller can drive a live "waiting for capacity..." status
	// line without polling anything itself. Called synchronously on Snipe's
	// goroutine; keep it fast and non-blocking.
	Progress func(Status)
	// Fallbacks is an optional ordered list of ADDITIONAL targets to try, in
	// order, within each round after the primary target's placement sweep fails
	// — before backing off. This is the multi-region extension (#76): capacity is
	// bursty and region-uneven, and the AZ with capacity is often in a different
	// region than the one picked. Each fallback is a full Target because
	// cross-region is NOT free like AZ-breadth: it needs a region-specific AMI
	// id, in-region launch artifacts/SG/subnet, etc. Off by default; opt-in only.
	// A terminal failure on any target still stops immediately.
	Fallbacks []Target
}

// Result is what Snipe returns on success — the launched instance's identity
// and where it landed. Deliberately narrower than watcher.MatchResult: no
// WatchID/UserID/Service/DynamoDB tags, since snipe has no persisted watch.
type Result struct {
	Region           string
	AvailabilityZone string
	Subnet           string
	InstanceType     string
	IsSpot           bool
	MatchedAt        time.Time
	InstanceID       string
}

// launcher abstracts spawn's launch primitive plus the client it needs, kept
// minimal so a caller in tests can fake it without a real AWS client. This is
// the whole reason snipe is a separate leaf from watcher.Spawner: Spawner also
// carries listInstances/terminateInstance/describeReservation/s3 for the
// poller's overlap/reservation/completion-condition features, none of which
// Snipe needs.
type acquirer struct {
	// clientFor resolves the *spawnaws.Client to launch a given region with,
	// cached per region for the retry loop's whole run (#111). Region-pinned,
	// not ambient-default: spawnaws.NewClient's default-credential-chain region
	// can resolve AMIs/AZs/identity in the WRONG region if it differs from the
	// caller's ambient AWS_REGION/profile (spawn#276) — every Target already
	// carries the region it wants, so there is no reason to ask the ambient
	// chain instead. Each Fallback (#76) may be in a DIFFERENT region, so this
	// is a function of region, not a single client.
	clientFor func(ctx context.Context, region string) (*spawnaws.Client, error)
	provide   func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)
	sleep     func(ctx context.Context, d time.Duration) error
}

func newAcquirer() *acquirer {
	return &acquirer{clientFor: cachedClientFor(), provide: launcher.Provision, sleep: sleepCtx}
}

// cachedClientFor returns a clientFor function that builds one
// region-pinned *spawnaws.Client per distinct region on first use and reuses
// it for the rest of the run — a multi-region Snipe (Options.Fallbacks) can
// revisit the same region many times across retry rounds.
func cachedClientFor() func(ctx context.Context, region string) (*spawnaws.Client, error) {
	clients := make(map[string]*spawnaws.Client)
	return func(ctx context.Context, region string) (*spawnaws.Client, error) {
		if c, ok := clients[region]; ok {
			return c, nil
		}
		c, err := spawnaws.NewClientWithRegion(ctx, region)
		if err != nil {
			return nil, err
		}
		clients[region] = c
		return c, nil
	}
}

// Snipe blocks until it acquires the target instance, the deadline passes, or a
// terminal failure occurs. It is the library primitive for "block-and-wait,
// single-target acquire" (#73): a thin, stateless wrapper over spawn's
// launcher.Provision that owns the capacity classify + retry loop, so an
// embedding consumer needs neither the persisted-Watch/DynamoDB machinery nor a
// reimplementation of the classify+retry policy.
//
// Each round is one full placement sweep: on FailureCapacity across all
// candidate placements it backs off and retries (uncapped — a watch may
// legitimately wait out scarce capacity for days); on FailureTerminal it
// returns immediately, since retrying can't help; on FailureUnknown it retries
// but counts toward Options.MaxConsecutiveUnknown (#106), so a persistent
// unrecognized fault eventually stops instead of masquerading as a capacity
// wait. On success it returns a *Result carrying the launched InstanceID,
// Region, AZ, and Subnet.
func Snipe(ctx context.Context, target Target, opts Options) (*Result, error) {
	return snipeWith(ctx, newAcquirer(), target, opts)
}

func snipeWith(ctx context.Context, l *acquirer, target Target, opts Options) (*Result, error) {
	interval := opts.RetryInterval
	if interval <= 0 {
		interval = DefaultRetryInterval
	}
	maxInterval := opts.MaxInterval
	if maxInterval <= 0 {
		maxInterval = DefaultMaxInterval
	}
	maxUnknown := opts.MaxConsecutiveUnknown
	if maxUnknown == 0 {
		maxUnknown = DefaultMaxConsecutiveUnknown
	}
	sleep := l.sleep
	if sleep == nil {
		sleep = sleepCtx
	}

	targets := append([]Target{target}, opts.Fallbacks...)
	built := make([]builtTarget, 0, len(targets))
	for _, t := range targets {
		if strings.TrimSpace(t.InstanceType) == "" {
			return nil, fmt.Errorf("snipe: target InstanceType is required")
		}
		if strings.TrimSpace(t.Region) == "" {
			return nil, fmt.Errorf("snipe: target Region is required")
		}
		built = append(built, builtTarget{target: t, cfg: buildConfig(t)})
	}

	var lastErr error
	consecutiveUnknown := 0
	for round := 0; ; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, bt := range built {
			if !opts.Deadline.IsZero() && !nowBefore(opts.Deadline) {
				return nil, fmt.Errorf("snipe: deadline reached without acquiring %s: %w",
					bt.target.InstanceType, lastErr)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			report(opts.Progress, Status{Round: round, Target: bt.target.InstanceType, Region: bt.target.Region})

			instanceID, az, subnet, err := launchAcrossPlacements(ctx, l, bt.cfg, bt.target.Placements)
			if err == nil {
				return &Result{
					Region:           bt.target.Region,
					AvailabilityZone: az,
					Subnet:           subnet,
					InstanceType:     bt.target.InstanceType,
					IsSpot:           bt.target.Spot,
					MatchedAt:        now(),
					InstanceID:       instanceID,
				}, nil
			}

			lastErr = err
			kind := failure.ClassifyFailure(err)
			report(opts.Progress, Status{Round: round, Target: bt.target.InstanceType, Region: bt.target.Region, LastErr: err, Kind: kind})

			if kind == failure.FailureTerminal {
				return nil, fmt.Errorf("snipe: terminal failure acquiring %s in %s: %w",
					bt.target.InstanceType, bt.target.Region, err)
			}
			if kind == failure.FailureUnknown {
				consecutiveUnknown++
				if maxUnknown > 0 && consecutiveUnknown >= maxUnknown {
					return nil, fmt.Errorf(
						"snipe: %d consecutive unrecognized failures acquiring %s in %s (giving up rather than "+
							"retrying a possible misconfiguration as a capacity wait): %w",
						consecutiveUnknown, bt.target.InstanceType, bt.target.Region, err)
				}
			} else {
				consecutiveUnknown = 0 // a recognized capacity failure resets the cap
			}
			// Capacity (or under-cap unknown) failure on this target → next target.
		}

		wait := backoffFor(round, interval, maxInterval)
		if !opts.Deadline.IsZero() {
			if remaining := until(opts.Deadline); remaining <= 0 {
				return nil, fmt.Errorf("snipe: deadline reached without acquiring %s: %w",
					target.InstanceType, lastErr)
			} else if wait > remaining {
				wait = remaining
			}
		}
		report(opts.Progress, Status{Round: round, Target: target.InstanceType, Region: target.Region, LastErr: lastErr, Wait: wait})
		if err := sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
}

func report(progress func(Status), s Status) {
	if progress != nil {
		progress(s)
	}
}

// builtTarget pairs a Target with its resolved launch config.
type builtTarget struct {
	target Target
	cfg    spawnaws.LaunchConfig
}

// buildConfig resolves a Target into a launch config: guarantee a TTL (no
// unbounded instance can escape), and pin type/region/spot. Placement
// (AZ/Subnet) is set per-attempt in the sweep.
func buildConfig(target Target) spawnaws.LaunchConfig {
	cfg := target.LaunchConfig
	if strings.TrimSpace(cfg.TTL) == "" {
		cfg.TTL = DefaultInstanceTTL
	}
	cfg.Region = target.Region
	cfg.InstanceType = target.InstanceType
	cfg.Spot = target.Spot
	return cfg
}

// launchAcrossPlacements tries each candidate placement in order (or a single
// EC2-chosen attempt if none given), returning on the first success. It stops
// immediately on a terminal failure — retrying a different AZ can't fix a bad
// AMI/IAM/quota — and otherwise returns the last error for the caller's
// classify+cap logic.
func launchAcrossPlacements(ctx context.Context, l *acquirer, cfg spawnaws.LaunchConfig, placements []Placement) (instanceID, az, subnet string, err error) {
	attempts := placements
	if len(attempts) == 0 {
		attempts = []Placement{{}} // let EC2 choose the AZ and default subnet
	}
	provide := l.provide
	if provide == nil {
		provide = launcher.Provision
	}
	// A test-constructed acquirer (see newAcquirerWithProvide) has no need for a
	// real client — its fake provide ignores the argument — so nil clientFor is
	// valid there and simply passes nil through.
	var client *spawnaws.Client
	if l.clientFor != nil {
		var err error
		client, err = l.clientFor(ctx, cfg.Region)
		if err != nil {
			return "", "", "", fmt.Errorf("snipe: create spawn client for %s: %w", cfg.Region, err)
		}
	}
	var lastErr error
	for _, p := range attempts {
		cfg.AvailabilityZone = p.AZ
		cfg.SubnetID = p.Subnet
		result, perr := provide(ctx, client, cfg, launcher.Options{
			// Keyless: a library caller typically has no SSH key. SSM-only launch.
		})
		if perr == nil {
			return result.InstanceID, p.AZ, p.Subnet, nil
		}
		lastErr = perr
		if failure.ClassifyFailure(perr) == failure.FailureTerminal {
			break
		}
	}
	return "", "", "", fmt.Errorf("launch instance (tried %d placement(s): %v): %w", len(attempts), attempts, lastErr)
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
// cancelled/expires first.
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
