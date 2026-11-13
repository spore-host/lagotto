package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/failure"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// TestValidateLaunchPrivileges: every setting that would have the poller
// author IAM, read its own files, or launch with a reserved role is refused.
func TestValidateLaunchPrivileges(t *testing.T) {
	refused := map[string]SpawnConfigFile{
		"iam_policy":                    {IAMPolicies: stringList{"s3:ReadWrite"}},
		"iam_policy_file":               {IAMPolicyFile: "/tmp/p.json"},
		"iam_policy_document":           {IAMPolicyDocument: `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`},
		"iam_role spored-instance-role": {IAMRole: "spored-instance-role"},
		"iam_role Spored-anything":      {IAMRole: "Spored-anything"},
		"iam_role nested spawn":         {IAMRole: spawnaws.NestedSpawnRolePrefix + "x"},
		"iam_role poller role":          {IAMRole: "lagotto-capacity-poller-role"},
		"iam_role wildcard":             {IAMRole: "spawn-instance*"},
		"iam_role arn":                  {IAMRole: "arn:aws:iam::123456789012:role/x"},
		"user_data_file":                {UserDataFile: "/proc/self/environ"},
		"user_data @path":               {UserData: "@/proc/self/environ"},
		"key_name control char":         {KeyName: "k\n"},
	}
	for name, cfg := range refused {
		if err := cfg.ValidateLaunchPrivileges(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	allowed := map[string]SpawnConfigFile{
		"empty":           {},
		"authorized role": {IAMRole: "my-gpu-role"},
		"inline userdata": {UserData: "#!/bin/bash\necho hi\n"},
		"key":             {KeyName: "my key-1"},
		"command":         {Command: "python train.py"},
	}
	for name, cfg := range allowed {
		if err := cfg.ValidateLaunchPrivileges(); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}

func TestInstanceProfileName(t *testing.T) {
	if got := (&SpawnConfigFile{}).InstanceProfileName(); got != DefaultInstanceRoleName {
		t.Errorf("default = %q, want %q", got, DefaultInstanceRoleName)
	}
	if got := (&SpawnConfigFile{IAMRole: " my-role "}).InstanceProfileName(); got != "my-role" {
		t.Errorf("iam_role = %q, want my-role", got)
	}
}

// TestSpawn_RefusesEditedPrivilegedConfig: a stored item edited in DynamoDB
// after the CLI validated it is refused at match time, terminally, before any
// launch.
func TestSpawn_RefusesEditedPrivilegedConfig(t *testing.T) {
	for name, cfg := range map[string]SpawnConfigFile{
		"policy document": {IAMPolicyDocument: `{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`},
		"spored role":     {IAMRole: "spored-instance-role"},
		"poller env":      {UserData: "@/proc/self/environ"},
	} {
		t.Run(name, func(t *testing.T) {
			attempted := false
			sp := newSpawnerWithProvision(func(context.Context, *spawnaws.Client, spawnaws.LaunchConfig, launcher.Options) (*spawnaws.LaunchResult, error) {
				attempted = true
				return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
			})
			cfg.InstanceType, cfg.Region = "g5.xlarge", "us-east-1"
			raw, _ := json.Marshal(cfg)
			m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}
			err := sp.Spawn(context.Background(), &Watch{WatchID: "w-1", LaunchConfigJSON: raw}, m)
			if !errors.Is(err, failure.ErrConfigRefused) {
				t.Fatalf("err = %v, want ErrConfigRefused", err)
			}
			if ClassifyFailure(err) != FailureTerminal {
				t.Error("a refused config must be terminal")
			}
			if attempted {
				t.Error("launched despite a refused config")
			}

			attempted = false
			_, err = sp.LaunchScheduled(context.Background(), &ScheduledLaunch{ScheduleID: "sl-1", Region: "us-east-1", LaunchConfigJSON: raw})
			if !errors.Is(err, failure.ErrConfigRefused) || attempted {
				t.Fatalf("scheduled: err = %v, attempted = %v", err, attempted)
			}
		})
	}
}

// TestSpawn_AlwaysNamesAnAdministratorProfile: the launch always carries an
// instance profile, so spawn's launcher never runs its own IAM setup.
func TestSpawn_AlwaysNamesAnAdministratorProfile(t *testing.T) {
	for role, want := range map[string]string{"": DefaultInstanceRoleName, "my-gpu-role": "my-gpu-role"} {
		var got string
		sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			got = cfg.IamInstanceProfile
			return &spawnaws.LaunchResult{InstanceID: "i-x"}, nil
		})
		raw, _ := json.Marshal(SpawnConfigFile{InstanceType: "g5.xlarge", Region: "us-east-1", IAMRole: role})
		m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}
		if err := sp.Spawn(context.Background(), &Watch{WatchID: "w-1", LaunchConfigJSON: raw}, m); err != nil {
			t.Fatalf("Spawn(%q): %v", role, err)
		}
		if got != want {
			t.Errorf("iam_role %q: IamInstanceProfile = %q, want %q", role, got, want)
		}
	}
}

func TestSageMakerJobRoleARN(t *testing.T) {
	ok := []string{
		"arn:aws:iam::123456789012:role/SageMakerExecution",
		"arn:aws:iam::123456789012:role/service-role/AmazonSageMaker-ExecutionRole-2020",
	}
	for _, arn := range ok {
		if _, err := SageMakerJobRoleARN([]byte(`{"RoleArn":"` + arn + `"}`)); err != nil {
			t.Errorf("%s: %v", arn, err)
		}
	}
	bad := []string{"", "arn:aws:iam::123456789012:role/*", "arn:aws:iam::123456789012:user/x",
		"arn:aws:iam::123456789012:role/spored-instance-role", "SageMakerExecution"}
	for _, arn := range bad {
		if _, err := SageMakerJobRoleARN([]byte(`{"RoleArn":"` + arn + `"}`)); err == nil {
			t.Errorf("%q accepted", arn)
		}
	}
}

// TestSageMakerSubmit_RefusesBadRole: an edited job spec with a reserved or
// missing role is refused before CreateTrainingJob.
func TestSageMakerSubmit_RefusesBadRole(t *testing.T) {
	l := &SageMakerLauncher{}
	w := &Watch{WatchID: "w-sm", SageMakerJobJSON: []byte(`{"RoleArn":"arn:aws:iam::123456789012:role/spored-instance-role"}`)}
	out, err := l.submit(context.Background(), nil, w, &MatchResult{})
	if err == nil || out.Kind != FailureTerminal || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("submit = %+v, %v; want a terminal refusal", out, err)
	}
}
