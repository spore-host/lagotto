package watcher

import (
	"context"
	"encoding/json"
	"fmt"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// Spawner launches instances via spawn's library when a watch matches.
type Spawner struct {
	client *spawnaws.Client
	// provision is the launch function, indirected so tests can drive the AZ
	// retry loop without a real AWS client. Defaults to launcher.Provision.
	provision func(ctx context.Context, client *spawnaws.Client, cfg spawnaws.LaunchConfig, opts launcher.Options) (*spawnaws.LaunchResult, error)
}

// NewSpawner creates a Spawner that uses spawn's EC2 launch library.
func NewSpawner(ctx context.Context) (*Spawner, error) {
	client, err := spawnaws.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create spawn client: %w", err)
	}
	return &Spawner{client: client, provision: launcher.Provision}, nil
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

	// Override region and pin the matched instance type (the watch pattern may
	// have resolved to a specific type) and spot-ness. AZ is set per-attempt below.
	cfg.Region = m.Region
	cfg.InstanceType = m.InstanceType
	cfg.Spot = m.IsSpot

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

	provision := s.provision
	if provision == nil {
		provision = launcher.Provision
	}

	var lastErr error
	for _, az := range attempts {
		cfg.AvailabilityZone = az
		result, err := provision(ctx, s.client, cfg, launcher.Options{
			// Keyless: the poller Lambda has no SSH key. SSM-only launch.
		})
		if err == nil {
			m.InstanceID = result.InstanceID
			m.AvailabilityZone = az
			m.ActionTaken = "spawned"
			return nil
		}
		lastErr = err
		// Only fall through to the next AZ on a capacity failure; a terminal error
		// (bad config) will fail identically in every AZ.
		if ClassifyFailure(err) == FailureTerminal {
			break
		}
	}

	m.ActionTaken = "spawn_failed"
	return fmt.Errorf("launch instance (tried %d AZ(s): %v): %w", len(attempts), attempts, lastErr)
}
