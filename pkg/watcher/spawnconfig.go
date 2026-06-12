package watcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"gopkg.in/yaml.v3"
)

// SpawnConfigFile is lagotto's view of the --spawn-config YAML. spawn's
// LaunchConfig has no JSON/YAML struct tags, so snake_case keys a user naturally
// writes (on_complete, pre_stop, idle_timeout, iam_policy) silently never map to
// its CamelCase fields and the settings are dropped — the instance launches but
// never stops and the hooks never run (lagotto#19 issue #3). This struct owns
// explicit, normalized keys so both snake_case and CamelCase map correctly, and
// converts to a spawnaws.LaunchConfig (mapping Command -> JobArrayCommand, the
// field spored's bootstrap actually reads — issue #2).
type SpawnConfigFile struct {
	// Core
	InstanceType string `json:"instancetype"`
	Region       string `json:"region"`
	AMI          string `json:"ami"`
	Spot         bool   `json:"spot"`

	// Lifecycle
	TTL         string `json:"ttl"`
	IdleTimeout string `json:"idletimeout"`
	OnComplete  string `json:"oncomplete"`

	// Pre-stop hook
	PreStop        string `json:"prestop"`
	PreStopTimeout string `json:"prestoptimeout"`

	// Workload: the command spored runs once the instance is ready. The reason
	// "watch -> launch -> run job -> wake up to results" exists.
	Command string `json:"command"`

	// IAM: service-level policy shorthands (e.g. "s3:ReadWrite"), accepting both
	// a single scalar and a YAML list. Not a LaunchConfig field — the spawner
	// builds an instance profile from these before launching.
	IAMPolicies stringList `json:"iampolicy"`

	// Storage / misc passthroughs commonly set in a spawn config.
	EFSID          string  `json:"efsid"`
	FSxLustreID    string  `json:"fsxid"`
	CostLimit      float64 `json:"costlimit"`
	KeyName        string  `json:"keyname"`
	SessionTimeout string  `json:"sessiontimeout"`
}

// stringList accepts either a scalar string ("s3:ReadWrite") or a sequence
// (["s3:ReadOnly", "dynamodb:WriteOnly"]) from JSON/YAML, mirroring spawn's
// --iam-policy StringSlice flag which takes comma-separated or repeated values.
type stringList []string

func (s *stringList) UnmarshalJSON(data []byte) error {
	// Try a list first.
	var list []string
	if err := json.Unmarshal(data, &list); err == nil {
		*s = list
		return nil
	}
	// Fall back to a scalar, splitting on commas like the CLI flag.
	var scalar string
	if err := json.Unmarshal(data, &scalar); err != nil {
		return fmt.Errorf("iam_policy must be a string or list of strings: %w", err)
	}
	if scalar == "" {
		*s = nil
		return nil
	}
	parts := strings.Split(scalar, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	*s = out
	return nil
}

// ParseSpawnConfigYAML parses a --spawn-config YAML document into a
// SpawnConfigFile, tolerating snake_case, kebab-case, and CamelCase keys
// (instance_type / instance-type / InstanceType all work). It does this by
// normalizing every top-level key to a lowercase, separator-free form that
// matches the struct's json tags, then JSON-unmarshaling.
func ParseSpawnConfigYAML(data []byte) (*SpawnConfigFile, error) {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}

	normalized := make(map[string]interface{}, len(raw))
	for k, v := range raw {
		normalized[normalizeKey(k)] = v
	}

	jsonBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("normalize config: %w", err)
	}

	var cfg SpawnConfigFile
	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return &cfg, nil
}

// normalizeKey lowercases a config key and strips underscores/hyphens, so
// "On_Complete", "on-complete", and "OnComplete" all collapse to "oncomplete".
func normalizeKey(k string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
}

// ToLaunchConfig converts the file into the spawnaws.LaunchConfig that
// launcher.Provision consumes. The crucial mapping is Command -> JobArrayCommand:
// JobArrayCommand is the field whose value spored's bootstrap writes to the
// spawn:command tag and executes after the instance is ready (issue #2).
func (s *SpawnConfigFile) ToLaunchConfig() spawnaws.LaunchConfig {
	return spawnaws.LaunchConfig{
		InstanceType:    s.InstanceType,
		Region:          s.Region,
		AMI:             s.AMI,
		Spot:            s.Spot,
		TTL:             s.TTL,
		IdleTimeout:     s.IdleTimeout,
		OnComplete:      s.OnComplete,
		PreStop:         s.PreStop,
		PreStopTimeout:  s.PreStopTimeout,
		JobArrayCommand: s.Command,
		EFSID:           s.EFSID,
		FSxLustreID:     s.FSxLustreID,
		CostLimit:       s.CostLimit,
		KeyName:         s.KeyName,
		SessionTimeout:  s.SessionTimeout,
	}
}
