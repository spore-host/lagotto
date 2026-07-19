package watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// Spawner launches instances via spawn's library when a watch matches.
type Spawner struct {
	client *spawnaws.Client
	// provision is the launch function, indirected so tests can drive the AZ
	// retry loop without a real AWS client. Defaults to launcher.Provision.
	provision func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)
	// listInstances / terminateInstance back the #49 IfExists overlap check, also
	// indirected so the skip/launch/replace policy is testable without real AWS.
	// Default to the client's methods.
	listInstances     func(ctx context.Context, region, stateFilter string) ([]spawnaws.InstanceInfo, error)
	terminateInstance func(ctx context.Context, region, instanceID string) error
	// describeReservation backs the #62 Capacity-Block start-time gate (verify the
	// reservation is launchable before firing). Indirected for testing.
	describeReservation func(ctx context.Context, region, reservationID string) (*spawnaws.CapacityReservation, error)
	// sleep backs Snipe's inter-round backoff (#73), indirected so tests drive the
	// retry loop without real waits. Nil = the default context-aware sleep.
	sleep func(ctx context.Context, d time.Duration) error
	// s3 backs s3-empty completion conditions (#70). Nil in tests that inject a
	// condition directly; set from the client's config in NewSpawner.
	s3 S3Lister
}

// S3Client returns the S3 lister for s3-empty completion conditions (#70), or
// nil if the spawner has no AWS client (test spawners).
func (s *Spawner) S3Client() S3Lister { return s.s3 }

// NewSpawner creates a Spawner that uses spawn's EC2 launch library.
func NewSpawner(ctx context.Context) (*Spawner, error) {
	client, err := spawnaws.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create spawn client: %w", err)
	}
	return &Spawner{
		client:              client,
		provision:           launcher.Provision,
		listInstances:       client.ListInstances,
		terminateInstance:   client.Terminate,
		describeReservation: client.DescribeCapacityReservation,
		s3:                  s3.NewFromConfig(client.Config()),
	}, nil
}

// reservationGate is the outcome of checking a Capacity Block reservation's
// state before a start-time launch (#62).
type reservationGate int

const (
	// reservationLaunchable — scheduled/active; go ahead and launch now.
	reservationLaunchable reservationGate = iota
	// reservationNotYet — payment-pending or scheduled-but-not-open; retry at the
	// boundary (transient, will become launchable).
	reservationNotYet
	// reservationDead — expired / payment-failed / cancelled; never launchable,
	// fail loudly (don't burn retries on a block that can't be used).
	reservationDead
)

// checkReservation classifies whether a Capacity Block reservation can accept a
// launch right now (#62). A describe error is treated as reservationNotYet (a
// transient API blip near the boundary shouldn't be terminal — the retry budget
// bounds it).
func (s *Spawner) checkReservation(ctx context.Context, region, reservationID string) (reservationGate, string) {
	cr, err := s.describeReservation(ctx, region, reservationID)
	if err != nil {
		return reservationNotYet, fmt.Sprintf("describe reservation %s: %v", reservationID, err)
	}
	switch cr.State {
	case "active":
		return reservationLaunchable, cr.State
	case "scheduled":
		// "scheduled" means payment cleared but the window may not be open yet;
		// the launch attempt itself is the real test (RunInstances tells us). Treat
		// as launchable — a not-yet-open block fails RunInstances and we retry.
		return reservationLaunchable, cr.State
	case "payment-pending":
		return reservationNotYet, cr.State
	case "expired", "payment-failed", "cancelled":
		return reservationDead, cr.State
	default:
		// Unknown/transitional state — retry, bounded by the deadline.
		return reservationNotYet, cr.State
	}
}

// Spawn deserializes the stored spawn config and launches a fully-functional
// spore in the region/AZ where capacity was found, then records the instance ID
// on the MatchResult.
//
// It goes through spawn's headless launcher (launcher.Provision) rather than the
// low-level client.Launch, so the instance gets the spored bootstrap: AMI is
// auto-detected when unset, the IAM profile is provisioned, and the spored agent
// is installed so the workload Command runs and OnComplete/PreStop/IdleTimeout
// are enforced in-instance (lagotto#19). The poller runs in a Lambda with no SSH
// key on disk, so the launch is SSM-only (keyless); `spawn connect` can inject a
// key over SSM later if interactive access is needed.
func (s *Spawner) Spawn(ctx context.Context, w *Watch, m *MatchResult) error {
	if len(w.LaunchConfigJSON) == 0 {
		return fmt.Errorf("watch %s has no launch config", w.WatchID)
	}

	var file SpawnConfigFile
	if err := json.Unmarshal(w.LaunchConfigJSON, &file); err != nil {
		return fmt.Errorf("unmarshal spawn config: %w", err)
	}

	cfg := file.ToLaunchConfig()

	// Guarantee a TTL even if the stored config somehow lacks one (#38): a config
	// written by an older CLI (before watch-create validation) could carry an
	// empty TTL, and an instance with no death clock runs unbounded. This is the
	// single chokepoint every launch passes through, so enforcing here makes "no
	// TTL reaches launcher.Provision" a hard invariant regardless of config origin.
	if strings.TrimSpace(cfg.TTL) == "" {
		cfg.TTL = DefaultInstanceTTL
	}

	// Override region and pin the matched instance type (the watch pattern may
	// have resolved to a specific type) and spot-ness. AZ is set per-attempt below.
	cfg.Region = m.Region
	cfg.InstanceType = m.InstanceType
	cfg.Spot = m.IsSpot

	// Goal-driven fleet watch: stamp the fleet tag so the supervisor can count
	// running members and top up toward DesiredCount (#70). Harmless for
	// single-shot watches, but only set it when this is actually a fleet so we
	// don't tag every launch.
	if w.DesiredCount > 0 {
		if cfg.Tags == nil {
			cfg.Tags = map[string]string{}
		}
		cfg.Tags[FleetTagKey] = w.WatchID
	}

	// Build a custom IAM instance profile from the config's iam_policy shorthands
	// (e.g. "s3:ReadWrite") before launching, mirroring `spawn launch
	// --iam-policy`. When none are given, Provision sets up the default spored
	// profile itself, so we leave IamInstanceProfile empty.
	if len(file.IAMPolicies) > 0 {
		profile, err := s.client.CreateOrGetInstanceProfile(ctx, spawnaws.IAMRoleConfig{
			Policies: file.IAMPolicies,
		})
		if err != nil {
			m.ActionTaken = "spawn_failed"
			return fmt.Errorf("set up IAM instance profile: %w", err)
		}
		cfg.IamInstanceProfile = profile
	}

	// Try each candidate AZ in preference order, falling through to the next on a
	// capacity failure. AZ breadth within a region is free (same-region data
	// locality), so exhausting all offered AZs in one attempt maximizes the chance
	// of catching scarce capacity before backing off to the next poll (#34). A
	// terminal failure (bad AMI/IAM/quota) stops immediately — retrying other AZs
	// won't help. Falls back to a single AZ-unpinned attempt when no candidates.
	attempts := m.CandidateAZs
	if len(attempts) == 0 {
		attempts = []string{m.AvailabilityZone} // may be "" → let EC2 choose the AZ
	}

	instanceID, az, err := s.launchAcrossAZs(ctx, cfg, attempts)
	if err != nil {
		m.ActionTaken = "spawn_failed"
		return err
	}
	m.InstanceID = instanceID
	m.AvailabilityZone = az
	m.ActionTaken = "spawned"
	return nil
}

// FleetTagKey is the EC2 tag stamped on every worker launched for a goal-driven
// fleet watch, valued with the WatchID. The supervisor counts running instances
// carrying this tag to decide how many to (re)launch toward DesiredCount (#70).
const FleetTagKey = "lagotto:watch"

// countRunningFleet returns how many of a fleet watch's workers are currently
// running (pending or running state), counted by the FleetTagKey=WatchID tag
// across the watch's regions. This is the authoritative live count — it reflects
// real reclaim/termination, so the supervisor tops up the true gap.
func (s *Spawner) countRunningFleet(ctx context.Context, w *Watch) (int, error) {
	regions := w.Regions
	if len(regions) == 0 {
		return 0, fmt.Errorf("watch %s has no regions", w.WatchID)
	}
	seen := map[string]struct{}{}
	for _, region := range regions {
		// "" state filter → all states; we keep pending+running (a just-launched
		// worker is pending and still counts toward the goal).
		insts, err := s.listInstances(ctx, region, "")
		if err != nil {
			return 0, fmt.Errorf("list instances in %s: %w", region, err)
		}
		for _, in := range insts {
			if in.Tags[FleetTagKey] != w.WatchID {
				continue
			}
			if in.State != "running" && in.State != "pending" {
				continue
			}
			seen[in.InstanceID] = struct{}{}
		}
	}
	return len(seen), nil
}

// launchAcrossAZs provisions an instance from a resolved LaunchConfig, trying
// each candidate AZ in preference order and falling through to the next on a
// capacity failure (AZ breadth within a region is free — #34). A terminal failure
// (bad AMI/IAM/quota) stops immediately; retrying other AZs can't help. Returns
// the launched instance id and the AZ it landed in. Shared by the watch-match
// path (Spawn) and the scheduled-launch path (LaunchScheduled, #49).
func (s *Spawner) launchAcrossAZs(ctx context.Context, cfg spawnaws.LaunchConfig, attempts []string) (instanceID, az string, err error) {
	if len(attempts) == 0 {
		attempts = []string{""} // let EC2 choose the AZ
	}
	provision := s.provision
	if provision == nil {
		provision = launcher.Provision
	}
	var lastErr error
	for _, a := range attempts {
		cfg.AvailabilityZone = a
		result, perr := provision(ctx, s.client, cfg, launcher.Options{
			// Keyless: the poller Lambda has no SSH key. SSM-only launch.
		})
		if perr == nil {
			return result.InstanceID, a, nil
		}
		lastErr = perr
		if ClassifyFailure(perr) == FailureTerminal {
			break
		}
	}
	return "", "", fmt.Errorf("launch instance (tried %d AZ(s): %v): %w", len(attempts), attempts, lastErr)
}

// LaunchScheduled fires a one-shot/cron scheduled launch (#49): it deserializes
// the stored SpawnConfigFile (same shape as a watch's), guarantees a TTL (#38),
// builds any IAM profile from iam_policy shorthands, and launches. A Capacity
// Block is AZ-pinned by its reservation, so the scheduled launch's AvailabilityZone
// (if set) is the single candidate; otherwise EC2 chooses. Returns the instance id.
func (s *Spawner) LaunchScheduled(ctx context.Context, sl *ScheduledLaunch) (string, error) {
	if len(sl.LaunchConfigJSON) == 0 {
		return "", fmt.Errorf("scheduled launch %s has no launch config", sl.ScheduleID)
	}
	var file SpawnConfigFile
	if err := json.Unmarshal(sl.LaunchConfigJSON, &file); err != nil {
		return "", fmt.Errorf("unmarshal spawn config: %w", err)
	}
	cfg := file.ToLaunchConfig()
	if strings.TrimSpace(cfg.TTL) == "" {
		cfg.TTL = DefaultInstanceTTL // #38 hard floor
	}
	if sl.Region != "" {
		cfg.Region = sl.Region
	}
	if len(file.IAMPolicies) > 0 {
		profile, err := s.client.CreateOrGetInstanceProfile(ctx, spawnaws.IAMRoleConfig{Policies: file.IAMPolicies})
		if err != nil {
			return "", fmt.Errorf("set up IAM instance profile: %w", err)
		}
		cfg.IamInstanceProfile = profile
	}

	// Overlap policy (#49): if a live instance with this launch's Name tag already
	// exists, the IfExists policy decides what to do — skip (don't double-launch,
	// the one-shot default for a Capacity Block), launch anyway (cron default,
	// each fire is a fresh box), or replace (terminate the existing, then launch).
	// IfExistsLaunch and a launch with no Name short-circuit the lookup entirely.
	if sl.InstanceName != "" && sl.IfExists != IfExistsLaunch {
		existing, err := s.findRunningByName(ctx, sl.Region, sl.InstanceName)
		if err != nil {
			return "", fmt.Errorf("check for existing instance %q: %w", sl.InstanceName, err)
		}
		if existing != "" {
			switch sl.IfExists {
			case IfExistsReplace:
				if err := s.terminateInstance(ctx, sl.Region, existing); err != nil {
					return "", fmt.Errorf("replace: terminate existing instance %s: %w", existing, err)
				}
			default: // IfExistsSkip (also the zero value / unknown → safe default)
				// Don't launch: the existing instance is the fulfillment. Report it
				// so the schedule records an instance id rather than a phantom launch.
				return existing, nil
			}
		}
	}

	var attempts []string
	if sl.AvailabilityZone != "" {
		attempts = []string{sl.AvailabilityZone}
	}
	instanceID, _, err := s.launchAcrossAZs(ctx, cfg, attempts)
	return instanceID, err
}

// ScheduledOutcome is the result of a single firing of RunScheduled (#62).
type ScheduledOutcome int

const (
	// OutcomeLaunched — an instance is running (or the IfExists policy resolved to
	// an existing one). Done.
	OutcomeLaunched ScheduledOutcome = iota
	// OutcomeRetry — a retryable boundary condition (reservation not yet active, or
	// transient capacity at RunInstances). The caller should re-arm a tight-interval
	// retry if still within the deadline.
	OutcomeRetry
	// OutcomeFailed — a terminal failure (dead reservation, bad config, or retry
	// budget exhausted). Do not retry.
	OutcomeFailed
)

// RunScheduled fires a scheduled launch, applying the Capacity-Block start-time
// semantics (#62) when the launch carries a ReservationID:
//   - Gate on the reservation state: a dead reservation (expired/payment-failed)
//     fails immediately; payment-pending retries; scheduled/active proceeds.
//   - On a launch failure, classify it: a capacity failure at the boundary is
//     retryable (the window may not be fully open yet); a terminal launch error
//     (bad AMI/IAM/quota) or post-launch failure is not.
//
// It returns the outcome, the instance id (on launch), and a human message. The
// caller (the Lambda handler) owns re-arming the EventBridge retry for OutcomeRetry.
// A launch with no ReservationID behaves exactly like LaunchScheduled wrapped as
// launched/failed (no boundary retry) — preserving the plain #49 path.
func (s *Spawner) RunScheduled(ctx context.Context, sl *ScheduledLaunch) (ScheduledOutcome, string, error) {
	if sl.ReservationID != "" {
		gate, state := s.checkReservation(ctx, sl.Region, sl.ReservationID)
		switch gate {
		case reservationDead:
			return OutcomeFailed, "", fmt.Errorf("reservation %s is %s — not launchable, giving up", sl.ReservationID, state)
		case reservationNotYet:
			return OutcomeRetry, fmt.Sprintf("reservation %s is %s; will retry at the window boundary", sl.ReservationID, state), nil
		}
		// reservationLaunchable → fall through to launch.
	}

	instanceID, err := s.LaunchScheduled(ctx, sl)
	if err != nil {
		// A capacity failure right at the window open is retryable (#62): the
		// reservation is active but RunInstances can transiently report no capacity
		// for a short period. An unclassified blip near the boundary is likewise
		// worth a retry. A terminal error (bad config) or a post-launch failure is
		// not — retrying can't help.
		if fk := ClassifyFailure(err); sl.ReservationID != "" && (fk == FailureCapacity || fk == FailureUnknown) {
			return OutcomeRetry, fmt.Sprintf("launch hit transient capacity at the boundary: %v", err), nil
		}
		return OutcomeFailed, "", err
	}
	return OutcomeLaunched, instanceID, nil
}

// findRunningByName returns the id of a non-terminated instance carrying the given
// Name tag in the region, or "" if none. spawn doesn't tag instances with their
// reservation id, so the Name tag is the dedup key for the IfExists overlap policy.
func (s *Spawner) findRunningByName(ctx context.Context, region, name string) (string, error) {
	// Empty stateFilter = spawn's default of pending/running/stopping/stopped —
	// i.e. every non-terminated state — so a prior launch (even one still pending
	// or stopped) still counts as an overlap.
	instances, err := s.listInstances(ctx, region, "")
	if err != nil {
		return "", err
	}
	for _, in := range instances {
		if in.Name == name {
			return in.InstanceID, nil
		}
	}
	return "", nil
}
