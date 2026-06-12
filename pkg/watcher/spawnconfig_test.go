package watcher

import (
	"encoding/json"
	"testing"
)

// TestParseSpawnConfigYAML_SnakeCase is the core lagotto#19 regression: a user
// writing natural snake_case keys must have them map, not be silently dropped.
func TestParseSpawnConfigYAML_SnakeCase(t *testing.T) {
	yaml := []byte(`
instance_type: g5.12xlarge
region: us-west-2
ttl: 48h
idle_timeout: 30m
on_complete: stop
pre_stop: "aws s3 sync ~/output s3://my-bucket/results/"
command: "bash /tmp/run.sh"
iam_policy: s3:ReadWrite
`)
	cfg, err := ParseSpawnConfigYAML(yaml)
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if cfg.InstanceType != "g5.12xlarge" {
		t.Errorf("InstanceType = %q", cfg.InstanceType)
	}
	if cfg.IdleTimeout != "30m" {
		t.Errorf("IdleTimeout = %q (snake_case key dropped?)", cfg.IdleTimeout)
	}
	if cfg.OnComplete != "stop" {
		t.Errorf("OnComplete = %q (snake_case key dropped?)", cfg.OnComplete)
	}
	if cfg.PreStop == "" {
		t.Error("PreStop dropped")
	}
	if cfg.Command != "bash /tmp/run.sh" {
		t.Errorf("Command = %q", cfg.Command)
	}
	if len(cfg.IAMPolicies) != 1 || cfg.IAMPolicies[0] != "s3:ReadWrite" {
		t.Errorf("IAMPolicies = %v", cfg.IAMPolicies)
	}
}

// TestParseSpawnConfigYAML_CamelCase confirms the original CamelCase keys (the
// only ones that "worked" before, by luck of case-insensitive matching) still do.
func TestParseSpawnConfigYAML_CamelCase(t *testing.T) {
	yaml := []byte(`
InstanceType: g5.12xlarge
Region: us-west-2
IdleTimeout: 30m
OnComplete: stop
PreStop: "echo done"
Command: "run"
`)
	cfg, err := ParseSpawnConfigYAML(yaml)
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if cfg.IdleTimeout != "30m" || cfg.OnComplete != "stop" || cfg.PreStop != "echo done" {
		t.Errorf("CamelCase keys did not map: %+v", cfg)
	}
}

func TestParseSpawnConfigYAML_KebabCase(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("instance-type: m7i.large\non-complete: terminate\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if cfg.InstanceType != "m7i.large" || cfg.OnComplete != "terminate" {
		t.Errorf("kebab-case keys did not map: %+v", cfg)
	}
}

func TestParseSpawnConfigYAML_IAMPolicyList(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("iam_policy:\n  - s3:ReadOnly\n  - dynamodb:WriteOnly\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if len(cfg.IAMPolicies) != 2 {
		t.Fatalf("IAMPolicies = %v, want 2", cfg.IAMPolicies)
	}
	if cfg.IAMPolicies[0] != "s3:ReadOnly" || cfg.IAMPolicies[1] != "dynamodb:WriteOnly" {
		t.Errorf("IAMPolicies = %v", cfg.IAMPolicies)
	}
}

func TestParseSpawnConfigYAML_IAMPolicyCommaScalar(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("iam_policy: s3:ReadOnly, dynamodb:WriteOnly\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if len(cfg.IAMPolicies) != 2 || cfg.IAMPolicies[1] != "dynamodb:WriteOnly" {
		t.Errorf("comma scalar did not split: %v", cfg.IAMPolicies)
	}
}

func TestParseSpawnConfigYAML_BadYAML(t *testing.T) {
	if _, err := ParseSpawnConfigYAML([]byte("instance_type: [unterminated\n")); err == nil {
		t.Error("expected parse error for malformed YAML")
	}
}

// TestToLaunchConfig_CommandMapsToJobArrayCommand is the issue #2 regression:
// the workload Command must land in JobArrayCommand, the field whose value the
// spored bootstrap writes to the spawn:command tag and executes.
func TestToLaunchConfig_CommandMapsToJobArrayCommand(t *testing.T) {
	file := &SpawnConfigFile{
		InstanceType: "m7i.large",
		Command:      "bash /tmp/run.sh",
		OnComplete:   "stop",
		PreStop:      "sync",
		IdleTimeout:  "30m",
		TTL:          "48h",
	}
	lc := file.ToLaunchConfig()
	if lc.JobArrayCommand != "bash /tmp/run.sh" {
		t.Errorf("JobArrayCommand = %q, want the Command value", lc.JobArrayCommand)
	}
	if lc.OnComplete != "stop" || lc.PreStop != "sync" || lc.IdleTimeout != "30m" || lc.TTL != "48h" {
		t.Errorf("lifecycle fields did not carry through: %+v", lc)
	}
	// IAM policies are NOT a LaunchConfig field — the spawner turns them into an
	// instance profile separately, so they must not leak into the launch config.
}

// TestSpawnConfigFile_JSONRoundTrip ensures the struct the cmd loader stores is
// read back identically by the spawner (both use encoding/json on this type).
func TestSpawnConfigFile_JSONRoundTrip(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte(`
instance_type: g5.xlarge
on_complete: stop
command: "go run ."
iam_policy: s3:ReadWrite
`)) //nolint
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Round-trip through the same json the cmd loader writes / spawner reads.
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back SpawnConfigFile
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.OnComplete != "stop" || back.Command != "go run ." {
		t.Errorf("round-trip lost fields: %+v", back)
	}
	if len(back.IAMPolicies) != 1 || back.IAMPolicies[0] != "s3:ReadWrite" {
		t.Errorf("round-trip lost iam policies: %v", back.IAMPolicies)
	}
}
