package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"

	"github.com/spore-host/lagotto/pkg/failure"
)

func TestValidateFSxBucket(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    SpawnConfigFile
		reject string // "" = accepted
	}{
		{"no fsx_create, any bucket", SpawnConfigFile{FSxS3Bucket: "aws-buckai"}, ""},
		{"no fsx_create, fsx_id", SpawnConfigFile{FSxLustreID: "fs-0123"}, ""},
		{"prefixed", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: "lagotto-fsx-data"}, ""},
		{"prefixed, padded", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: " lagotto-fsx-data "}, ""},
		{"no bucket", SpawnConfigFile{FSxCreate: true}, "needs fsx_s3_bucket"},
		{"outside the prefix", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: "aws-buckai"}, "must be named lagotto-fsx-"},
		{"spawn-ish name", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: "spawn-fsx-data"}, "must be named lagotto-fsx-"},
		{"prefix in the middle", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: "my-lagotto-fsx-data"}, "must be named lagotto-fsx-"},
		{"the prefix alone", SpawnConfigFile{FSxCreate: true, FSxS3Bucket: "lagotto-fsx-"}, "must be named lagotto-fsx-"},
	} {
		err := tc.cfg.ValidateFSxBucket()
		switch {
		case tc.reject == "" && err != nil:
			t.Errorf("%s: refused: %v", tc.name, err)
		case tc.reject != "" && (err == nil || !strings.Contains(err.Error(), tc.reject)):
			t.Errorf("%s: err = %v, want one containing %q", tc.name, err, tc.reject)
		}
	}
}

func storedFSxConfig(t *testing.T, bucket string) []byte {
	t.Helper()
	raw, err := json.Marshal(SpawnConfigFile{
		Name: "job", InstanceType: "g5.12xlarge", Region: "us-east-1", TTL: "24h",
		FSxCreate: true, FSxLifecycle: "ephemeral", FSxS3Bucket: bucket,
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestSpawn_RefusesStoredFSxBucketOutsidePrefix (#242): a watch stored before
// the rule is refused by the poller before it launches anything (so no S3 call
// and no orphaned instance), and the refusal is terminal with the reason.
func TestSpawn_RefusesStoredFSxBucketOutsidePrefix(t *testing.T) {
	launches := 0
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		launches++
		return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
	})
	w := &Watch{WatchID: "w-old", LaunchConfigJSON: storedFSxConfig(t, "aws-buckai")}
	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}
	err := sp.Spawn(context.Background(), w, m)
	if err == nil {
		t.Fatal("Spawn launched an fsx_create config with a bucket outside the prefix")
	}
	if launches != 0 {
		t.Errorf("provision was called %d times; the refusal must come before any launch", launches)
	}
	if !errors.Is(err, failure.ErrConfigRefused) || ClassifyFailure(err) != FailureTerminal {
		t.Errorf("refusal is not terminal: %v (kind %v)", err, ClassifyFailure(err))
	}
	if !strings.Contains(err.Error(), "aws-buckai") || m.ActionTaken != "spawn_failed" {
		t.Errorf("refusal lacks the reason or the action: %v / %q", err, m.ActionTaken)
	}

	// A prefixed bucket launches.
	w.LaunchConfigJSON = storedFSxConfig(t, "lagotto-fsx-buckai")
	if err := sp.Spawn(context.Background(), w, &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}); err != nil {
		t.Fatalf("Spawn refused a prefixed bucket: %v", err)
	}
	if launches != 1 {
		t.Errorf("provision called %d times, want 1", launches)
	}
}

// TestLaunchScheduled_RefusesStoredFSxBucketOutsidePrefix: the scheduled path
// has the same check, and RunScheduled reports it as failed, not retryable.
func TestLaunchScheduled_RefusesStoredFSxBucketOutsidePrefix(t *testing.T) {
	launches := 0
	sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
		launches++
		return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
	})
	sl := &ScheduledLaunch{ScheduleID: "sl-old", Region: "us-east-1", AvailabilityZone: "us-east-1a",
		IfExists: IfExistsLaunch, LaunchConfigJSON: storedFSxConfig(t, "aws-buckai")}
	outcome, _, err := sp.RunScheduled(context.Background(), sl)
	if outcome != OutcomeFailed || !errors.Is(err, failure.ErrConfigRefused) {
		t.Errorf("RunScheduled = %v, %v; want OutcomeFailed with ErrConfigRefused", outcome, err)
	}
	if launches != 0 {
		t.Errorf("provision was called %d times", launches)
	}
}
