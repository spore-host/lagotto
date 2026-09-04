package watcher

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// TestFSxCreatePassthrough verifies the FSx auto-create block (#43) parses from
// snake_case YAML and forwards through ToLaunchConfig to the LaunchConfig fields
// launcher.Provision consumes (spawn#202).
func TestFSxCreatePassthrough(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte(`
instance_type: g5.12xlarge
region: us-east-1
fsx_create: true
fsx_lifecycle: ephemeral
fsx_s3_bucket: aws-buckai
fsx_import_path: s3://aws-buckai/indices/
fsx_export_path: s3://aws-buckai/detections/
fsx_mount_point: /fsx
fsx_storage_capacity: 1200
`))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if !cfg.FSxCreate || cfg.FSxLifecycle != "ephemeral" || cfg.FSxS3Bucket != "aws-buckai" {
		t.Fatalf("FSx fields not parsed from snake_case: %+v", cfg)
	}

	lc := cfg.ToLaunchConfig()
	if !lc.FSxLustreCreate {
		t.Error("FSxLustreCreate not forwarded")
	}
	if lc.FSxLifecycle != "ephemeral" {
		t.Errorf("FSxLifecycle = %q, want ephemeral", lc.FSxLifecycle)
	}
	if lc.FSxS3Bucket != "aws-buckai" {
		t.Errorf("FSxS3Bucket = %q", lc.FSxS3Bucket)
	}
	if lc.FSxImportPath != "s3://aws-buckai/indices/" || lc.FSxExportPath != "s3://aws-buckai/detections/" {
		t.Errorf("import/export paths not forwarded: %q / %q", lc.FSxImportPath, lc.FSxExportPath)
	}
	if lc.FSxMountPoint != "/fsx" || lc.FSxStorageCapacity != 1200 {
		t.Errorf("mount-point/capacity not forwarded: %q / %d", lc.FSxMountPoint, lc.FSxStorageCapacity)
	}
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

// TestValidateAndDefaultTTL is the #38 watch-create guard: an empty TTL defaults
// to 24h, valid forms (Go duration + short "7d") pass, and malformed/non-positive
// values fail closed so a watch can never store a config that launches a
// TTL-less (unbounded) instance.
func TestValidateAndDefaultTTL(t *testing.T) {
	t.Run("empty defaults to 24h", func(t *testing.T) {
		c := &SpawnConfigFile{}
		if err := c.ValidateAndDefaultTTL(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.TTL != DefaultInstanceTTL {
			t.Errorf("TTL = %q, want %q", c.TTL, DefaultInstanceTTL)
		}
	})

	t.Run("valid forms pass unchanged", func(t *testing.T) {
		for _, v := range []string{"24h", "48h", "7d", "30m", "90m"} {
			c := &SpawnConfigFile{TTL: v}
			if err := c.ValidateAndDefaultTTL(); err != nil {
				t.Errorf("ttl %q: unexpected error %v", v, err)
			}
			if c.TTL != v {
				t.Errorf("ttl %q mutated to %q", v, c.TTL)
			}
		}
	})

	t.Run("malformed / non-positive fail closed", func(t *testing.T) {
		for _, v := range []string{"garbage", "10x", "-5h", "0h", "0"} {
			c := &SpawnConfigFile{TTL: v}
			if err := c.ValidateAndDefaultTTL(); err == nil {
				t.Errorf("ttl %q: expected an error, got none", v)
			}
		}
	})
}

// TestToLaunchConfig_ReservationPassthrough verifies the #49 capacity-reservation
// / Capacity Block fields forward to spawn's LaunchConfig (snake_case keys parse
// via normalizeKey, and ToLaunchConfig maps them).
func TestToLaunchConfig_ReservationPassthrough(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte(`
instance_type: p5.48xlarge
reservation_id: cr-0abc123
capacity_block: true
`))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if cfg.ReservationID != "cr-0abc123" || !cfg.CapacityBlock {
		t.Fatalf("fields not parsed: ReservationID=%q CapacityBlock=%v", cfg.ReservationID, cfg.CapacityBlock)
	}
	lc := cfg.ToLaunchConfig()
	if lc.ReservationID != "cr-0abc123" {
		t.Errorf("ReservationID not forwarded: %q", lc.ReservationID)
	}
	if !lc.CapacityBlock {
		t.Error("CapacityBlock not forwarded")
	}
}

// --- lagotto#129: the six previously-missing config-file keys ---

// TestToLaunchConfig_VolumeSize verifies volume_size maps to
// LaunchConfig.RootVolumeSizeGiB, and that an absent field leaves the
// LaunchConfig field at its zero value (spawn's own default: use the AMI's
// registered root size).
func TestToLaunchConfig_VolumeSize(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		cfg, err := ParseSpawnConfigYAML([]byte("volume_size: 200\n"))
		if err != nil {
			t.Fatalf("ParseSpawnConfigYAML: %v", err)
		}
		if cfg.VolumeSize != 200 {
			t.Fatalf("VolumeSize = %d, want 200", cfg.VolumeSize)
		}
		lc := cfg.ToLaunchConfig()
		if lc.RootVolumeSizeGiB != 200 {
			t.Errorf("RootVolumeSizeGiB = %d, want 200", lc.RootVolumeSizeGiB)
		}
	})
	t.Run("omitted stays zero", func(t *testing.T) {
		cfg, err := ParseSpawnConfigYAML([]byte("instance_type: m7i.large\n"))
		if err != nil {
			t.Fatalf("ParseSpawnConfigYAML: %v", err)
		}
		lc := cfg.ToLaunchConfig()
		if lc.RootVolumeSizeGiB != 0 {
			t.Errorf("RootVolumeSizeGiB = %d, want 0 (spawn's own default)", lc.RootVolumeSizeGiB)
		}
	})
}

// TestToLaunchConfig_SpotMaxPrice verifies spot_max_price maps to
// LaunchConfig.SpotMaxPrice, accepting both a quoted string and a bare YAML
// number (the latter would otherwise decode to a JSON number, not a string).
func TestToLaunchConfig_SpotMaxPrice(t *testing.T) {
	t.Run("string form", func(t *testing.T) {
		cfg, err := ParseSpawnConfigYAML([]byte(`spot_max_price: "0.50"` + "\n"))
		if err != nil {
			t.Fatalf("ParseSpawnConfigYAML: %v", err)
		}
		if got := cfg.ToLaunchConfig().SpotMaxPrice; got != "0.50" {
			t.Errorf("SpotMaxPrice = %q, want %q", got, "0.50")
		}
	})
	t.Run("bare number form", func(t *testing.T) {
		cfg, err := ParseSpawnConfigYAML([]byte("spot_max_price: 0.5\n"))
		if err != nil {
			t.Fatalf("ParseSpawnConfigYAML: %v", err)
		}
		if got := cfg.ToLaunchConfig().SpotMaxPrice; got != "0.5" {
			t.Errorf("SpotMaxPrice = %q, want %q", got, "0.5")
		}
	})
	t.Run("omitted stays empty", func(t *testing.T) {
		cfg, err := ParseSpawnConfigYAML([]byte("instance_type: m7i.large\n"))
		if err != nil {
			t.Fatalf("ParseSpawnConfigYAML: %v", err)
		}
		if got := cfg.ToLaunchConfig().SpotMaxPrice; got != "" {
			t.Errorf("SpotMaxPrice = %q, want empty", got)
		}
	})
}

// TestToLaunchConfig_CompletionFile verifies completion_file maps to
// LaunchConfig.CompletionFile, and is empty (spawn's own default,
// /tmp/SPAWN_COMPLETE) when omitted.
func TestToLaunchConfig_CompletionFile(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("completion_file: /tmp/my-job-done\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if got := cfg.ToLaunchConfig().CompletionFile; got != "/tmp/my-job-done" {
		t.Errorf("CompletionFile = %q, want /tmp/my-job-done", got)
	}

	empty, err := ParseSpawnConfigYAML([]byte("instance_type: m7i.large\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if got := empty.ToLaunchConfig().CompletionFile; got != "" {
		t.Errorf("CompletionFile = %q, want empty (omitted case)", got)
	}
}

// TestToLaunchConfig_Tags_Map verifies the tags field accepts a YAML map and
// maps to LaunchConfig.Tags.
func TestToLaunchConfig_Tags_Map(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("tags:\n  env: prod\n  team: fieldwork\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	lc := cfg.ToLaunchConfig()
	if lc.Tags["env"] != "prod" || lc.Tags["team"] != "fieldwork" {
		t.Errorf("Tags = %v, want env=prod team=fieldwork", lc.Tags)
	}
}

// TestToLaunchConfig_Tags_KVList verifies the tags field also accepts a list of
// "key=value" strings, mirroring spawn's repeatable --tag flag shape.
func TestToLaunchConfig_Tags_KVList(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("tags:\n  - env=prod\n  - team=fieldwork\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	lc := cfg.ToLaunchConfig()
	if lc.Tags["env"] != "prod" || lc.Tags["team"] != "fieldwork" {
		t.Errorf("Tags = %v, want env=prod team=fieldwork", lc.Tags)
	}
}

// TestToLaunchConfig_Tags_Omitted verifies an absent tags field leaves
// LaunchConfig.Tags at its zero value (nil), not an empty-but-non-nil map that
// would change buildTags' behavior downstream.
func TestToLaunchConfig_Tags_Omitted(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte("instance_type: m7i.large\n"))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if got := cfg.ToLaunchConfig().Tags; got != nil {
		t.Errorf("Tags = %v, want nil", got)
	}
}

func TestToLaunchConfig_Tags_KVList_BadEntry(t *testing.T) {
	if _, err := ParseSpawnConfigYAML([]byte("tags:\n  - noequalssign\n")); err == nil {
		t.Error("expected an error for a tags list entry with no '='")
	}
}

// TestResolveUserData_Inline verifies a plain user_data string is returned
// verbatim.
func TestResolveUserData_Inline(t *testing.T) {
	cfg := &SpawnConfigFile{UserData: "#!/bin/bash\necho hi\n"}
	got, err := cfg.ResolveUserData()
	if err != nil {
		t.Fatalf("ResolveUserData: %v", err)
	}
	if got != "#!/bin/bash\necho hi\n" {
		t.Errorf("ResolveUserData = %q", got)
	}
}

// TestResolveUserData_AtFile verifies a "@path" user_data value is read from
// disk, mirroring spawn CLI's --user-data @file form.
func TestResolveUserData_AtFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "script.sh")
	if err := os.WriteFile(path, []byte("#!/bin/bash\necho from-file\n"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	cfg := &SpawnConfigFile{UserData: "@" + path}
	got, err := cfg.ResolveUserData()
	if err != nil {
		t.Fatalf("ResolveUserData: %v", err)
	}
	if got != "#!/bin/bash\necho from-file\n" {
		t.Errorf("ResolveUserData = %q", got)
	}
}

// TestResolveUserData_FilePrecedence verifies user_data_file takes precedence
// over user_data when both are set, matching spawn CLI's own buildUserData
// order (userDataFile checked first).
func TestResolveUserData_FilePrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "winner.sh")
	if err := os.WriteFile(path, []byte("winner\n"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	cfg := &SpawnConfigFile{
		UserData:     "loser",
		UserDataFile: path,
	}
	got, err := cfg.ResolveUserData()
	if err != nil {
		t.Fatalf("ResolveUserData: %v", err)
	}
	if got != "winner\n" {
		t.Errorf("ResolveUserData = %q, want the user_data_file content (%q takes precedence)", got, "user_data_file")
	}
}

// TestResolveUserData_Omitted verifies no user_data/user_data_file set
// resolves to an empty string with no error.
func TestResolveUserData_Omitted(t *testing.T) {
	cfg := &SpawnConfigFile{}
	got, err := cfg.ResolveUserData()
	if err != nil {
		t.Fatalf("ResolveUserData: %v", err)
	}
	if got != "" {
		t.Errorf("ResolveUserData = %q, want empty", got)
	}
}

// TestResolveUserData_MissingFile verifies a nonexistent user_data_file path
// surfaces as an error rather than silently launching with no bootstrap
// payload.
func TestResolveUserData_MissingFile(t *testing.T) {
	cfg := &SpawnConfigFile{UserDataFile: "/nonexistent/path/does-not-exist.sh"}
	if _, err := cfg.ResolveUserData(); err == nil {
		t.Error("expected an error for a missing user_data_file")
	}
}

// TestSpawnConfigFile_IAMRoleAndPolicyFile verifies iam_role and
// iam_policy_file parse into their new fields (not into the existing
// IAMPolicies shorthand list).
func TestSpawnConfigFile_IAMRoleAndPolicyFile(t *testing.T) {
	cfg, err := ParseSpawnConfigYAML([]byte(`
iam_role: my-custom-role
iam_policy_file: /path/to/policy.json
`))
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}
	if cfg.IAMRole != "my-custom-role" {
		t.Errorf("IAMRole = %q", cfg.IAMRole)
	}
	if cfg.IAMPolicyFile != "/path/to/policy.json" {
		t.Errorf("IAMPolicyFile = %q", cfg.IAMPolicyFile)
	}
	// Neither is a LaunchConfig field directly — like IAMPolicies, the spawner
	// composes them into an IAMRoleConfig via buildIAMProfile, not ToLaunchConfig.
}

// TestFullExampleConfig_AllSixNewFields exercises all six #129 fields together
// from one YAML document, to catch any interaction/parsing issue not visible
// from isolated field tests (e.g. normalizeKey collisions, ordering).
func TestFullExampleConfig_AllSixNewFields(t *testing.T) {
	yaml := []byte(`
instance_type: g5.12xlarge
region: us-west-2
ttl: 48h
command: "bash /tmp/run.sh"
user_data: |
  #!/bin/bash
  echo "custom bootstrap"
iam_role: fieldwork-worker
iam_policy_file: policies/fieldwork.json
tags:
  project: fieldwork
  owner: buckai
volume_size: 500
spot_max_price: "0.75"
completion_file: /tmp/FIELDWORK_DONE
`)
	cfg, err := ParseSpawnConfigYAML(yaml)
	if err != nil {
		t.Fatalf("ParseSpawnConfigYAML: %v", err)
	}

	if cfg.UserData == "" {
		t.Error("UserData not parsed")
	}
	if cfg.IAMRole != "fieldwork-worker" {
		t.Errorf("IAMRole = %q", cfg.IAMRole)
	}
	if cfg.IAMPolicyFile != "policies/fieldwork.json" {
		t.Errorf("IAMPolicyFile = %q", cfg.IAMPolicyFile)
	}
	if cfg.VolumeSize != 500 {
		t.Errorf("VolumeSize = %d", cfg.VolumeSize)
	}
	if cfg.SpotMaxPrice != "0.75" {
		t.Errorf("SpotMaxPrice = %q", cfg.SpotMaxPrice)
	}
	if cfg.CompletionFile != "/tmp/FIELDWORK_DONE" {
		t.Errorf("CompletionFile = %q", cfg.CompletionFile)
	}

	lc := cfg.ToLaunchConfig()
	if lc.RootVolumeSizeGiB != 500 {
		t.Errorf("RootVolumeSizeGiB = %d", lc.RootVolumeSizeGiB)
	}
	if lc.SpotMaxPrice != "0.75" {
		t.Errorf("SpotMaxPrice = %q", lc.SpotMaxPrice)
	}
	if lc.CompletionFile != "/tmp/FIELDWORK_DONE" {
		t.Errorf("CompletionFile = %q", lc.CompletionFile)
	}
	if lc.Tags["project"] != "fieldwork" || lc.Tags["owner"] != "buckai" {
		t.Errorf("Tags = %v", lc.Tags)
	}

	userData, err := cfg.ResolveUserData()
	if err != nil {
		t.Fatalf("ResolveUserData: %v", err)
	}
	if userData == "" {
		t.Error("ResolveUserData returned empty for a set user_data field")
	}
}
