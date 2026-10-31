package watcher

import (
	"context"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
)

// structSpawnConfigKeys discovers the json keys SpawnConfigFile actually
// decodes, by reflection, so a field added to the struct without a documented
// key (or vice versa) fails TestSpawnConfigKeysMatchStruct.
func structSpawnConfigKeys(t *testing.T) []string {
	t.Helper()
	typ := reflect.TypeOf(SpawnConfigFile{})
	var keys []string
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		if tag == "-" {
			continue
		}
		if tag == "" {
			t.Fatalf("SpawnConfigFile.%s has no json tag; every field needs an explicit normalized key", f.Name)
		}
		keys = append(keys, tag)
	}
	sort.Strings(keys)
	return keys
}

func normalizedDocKeys(t *testing.T, doc []string) []string {
	t.Helper()
	seen := map[string]bool{}
	var out []string
	for _, k := range doc {
		nk := normalizeKey(k)
		if seen[nk] {
			t.Errorf("documented key %q is listed twice (normalized %q)", k, nk)
		}
		seen[nk] = true
		out = append(out, nk)
	}
	sort.Strings(out)
	return out
}

// TestSpawnConfigKeysMatchStruct: the documented key list (used in the
// unknown-key error and the README) equals the struct's fields (#202).
func TestSpawnConfigKeysMatchStruct(t *testing.T) {
	want := structSpawnConfigKeys(t)
	got := normalizedDocKeys(t, SpawnConfigKeys)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SpawnConfigKeys (normalized) != SpawnConfigFile json tags\n doc:    %v\n struct: %v", got, want)
	}
	for _, k := range SpawnConfigKeys {
		if k != strings.ToLower(k) || strings.Contains(k, "-") {
			t.Errorf("documented key %q is not canonical snake_case", k)
		}
	}
}

// TestREADMESpawnConfigKeysMatch: the README's accepted-key list equals
// SpawnConfigKeys (and so the struct). The list is the backticked keys between
// the spawn-config-keys markers.
func TestREADMESpawnConfigKeysMatch(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	s := string(data)
	const begin, end = "<!-- spawn-config-keys:begin -->", "<!-- spawn-config-keys:end -->"
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatalf("README.md has no %s ... %s block", begin, end)
	}
	var readme []string
	for _, m := range regexp.MustCompile("`([a-z0-9_]+)`").FindAllStringSubmatch(s[i:j], -1) {
		readme = append(readme, m[1])
	}
	got := normalizedDocKeys(t, readme)
	want := normalizedDocKeys(t, SpawnConfigKeys)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("README spawn-config key list != SpawnConfigKeys\n readme: %v\n code:   %v", got, want)
	}
}

// TestParseSpawnConfigYAML_UnknownKeyRejected is the #202 regression: a key
// that matches no field after normalization is an error naming the key as the
// user wrote it, and the nearest known key.
func TestParseSpawnConfigYAML_UnknownKeyRejected(t *testing.T) {
	cases := []struct {
		name, yaml string
		want       []string
	}{
		{"typo", "instance_type: m7i.large\nidle_timout: 30m\n", []string{`"idle_timout"`, "did you mean idle_timeout?"}},
		{"camel typo", "instance_type: m7i.large\nOnCompete: stop\n", []string{`"OnCompete"`, "did you mean on_complete?"}},
		{"spawn flag az", "instance_type: m7i.large\naz: us-east-1a\n", []string{`"az"`, "--azs"}},
		{"spawn flag s3_read", "instance_type: m7i.large\ns3_read: my-bucket\n", []string{`"s3_read"`, "iam_policy"}},
		{"completion_delay", "instance_type: m7i.large\ncompletion_delay: 5m\n", []string{`"completion_delay"`}},
		{"unrelated", "instance_type: m7i.large\nfrobnicate: yes\n", []string{`"frobnicate"`, "no similar key", "Accepted keys:"}},
		{"several", "instance_type: m7i.large\nzzz: 1\naaa: 2\n", []string{`"aaa"`, `"zzz"`, "unknown spawn config keys"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseSpawnConfigYAML([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("accepted an unknown key: %+v", cfg)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

// TestParseSpawnConfigYAML_EverySpellingOfEveryKeyAccepted: strictness runs
// after normalization, so the snake, kebab and CamelCase spelling of every
// documented key is still accepted (#202 must not regress lagotto#19).
func TestParseSpawnConfigYAML_EverySpellingOfEveryKeyAccepted(t *testing.T) {
	camel := func(k string) string {
		parts := strings.Split(k, "_")
		for i, p := range parts {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
		return strings.Join(parts, "")
	}
	for _, k := range SpawnConfigKeys {
		for _, spelled := range []string{k, strings.ReplaceAll(k, "_", "-"), camel(k)} {
			// A null value decodes into any field type, so this checks key
			// recognition alone.
			if _, err := ParseSpawnConfigYAML([]byte(spelled + ": null\n")); err != nil {
				t.Errorf("key %q rejected: %v", spelled, err)
			}
		}
	}
}

// TestSpawn_StoredConfigUnknownKeyTolerated pins the stored-watch side of
// #202: a config already in DynamoDB is re-read at match time, and an unknown
// key there (written by a CLI with a different struct) must NOT fail the
// launch. Strictness belongs to creation only.
func TestSpawn_StoredConfigUnknownKeyTolerated(t *testing.T) {
	launches := 0
	sp := newSpawnerWithProvision(func(ctx context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		launches++
		return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
	})
	raw := []byte(`{"instancetype":"g5.12xlarge","region":"us-east-1","ttl":"24h","az":"us-east-1a","somefuturefield":true}`)
	w := &Watch{WatchID: "w-stored", LaunchConfigJSON: raw}
	m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}
	if err := sp.Spawn(context.Background(), w, m); err != nil {
		t.Fatalf("Spawn rejected a stored config with an unknown key: %v", err)
	}
	if launches != 1 {
		t.Errorf("provision called %d times, want 1", launches)
	}
}
