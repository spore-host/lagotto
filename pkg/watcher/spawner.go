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
}

// NewSpawner creates a Spawner that uses spawn's EC2 launch library.
func NewSpawner(ctx context.Context) (*Spawner, error) {
	client, err := spawnaws.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create spawn client: %w", err)
	}
	return &Spawner{client: client}, nil
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

	// Override region and AZ with where capacity was actually found, and pin the
	// matched instance type (the watch pattern may have resolved to a specific
	// type) and spot-ness.
	cfg.Region = m.Region
	if m.AvailabilityZone != "" {
		cfg.AvailabilityZone = m.AvailabilityZone
	}
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

	result, err := launcher.Provision(ctx, s.client, cfg, launcher.Options{
		// Keyless: the poller Lambda has no SSH key. SSM-only launch.
	})
	if err != nil {
		m.ActionTaken = "spawn_failed"
		return fmt.Errorf("launch instance: %w", err)
	}

	m.InstanceID = result.InstanceID
	m.ActionTaken = "spawned"
	return nil
}
