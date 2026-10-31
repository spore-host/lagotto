package watcher

import (
	"fmt"
	"sort"
	"strings"
)

// SpawnConfigKeys is the documented list of top-level --spawn-config keys, in
// their canonical snake_case spelling (kebab-case and CamelCase spellings are
// accepted too: ParseSpawnConfigYAML normalizes before matching). It is the
// source for the unknown-key error's suggestions and for the key list in the
// README; TestSpawnConfigKeysMatchStruct holds it equal to SpawnConfigFile's
// fields, and TestREADMESpawnConfigKeysMatch holds the README equal to it.
var SpawnConfigKeys = []string{
	"name", "instance_type", "region", "ami", "spot",
	"ttl", "idle_timeout", "on_complete", "pre_stop", "pre_stop_timeout",
	"command",
	"iam_policy", "iam_role", "iam_policy_file", "iam_policy_document",
	"efs_id", "fsx_id", "fsx_create", "fsx_lifecycle", "fsx_s3_bucket",
	"fsx_import_path", "fsx_export_path", "fsx_mount_point", "fsx_storage_capacity",
	"cost_limit", "key_name", "session_timeout",
	"reservation_id", "capacity_block",
	"user_data", "user_data_file",
	"tags", "volume_size", "spot_max_price", "completion_file",
}

// knownSpawnConfigKeys maps each normalized key to its documented spelling.
var knownSpawnConfigKeys = func() map[string]string {
	m := make(map[string]string, len(SpawnConfigKeys))
	for _, k := range SpawnConfigKeys {
		m[normalizeKey(k)] = k
	}
	return m
}()

// spawnConfigKeyHints covers keys people write because `spawn launch` has a
// flag of that name, but which have no --spawn-config equivalent. Edit
// distance would suggest something unrelated (az -> ami), so these say where
// the setting actually lives. Keys are normalized.
var spawnConfigKeyHints = map[string]string{
	"az":               "use `lagotto watch --azs` to restrict availability zones",
	"azs":              "use `lagotto watch --azs` to restrict availability zones",
	"availabilityzone": "use `lagotto watch --azs` to restrict availability zones",
	"s3read":           "grant bucket access with iam_policy (e.g. s3:ReadOnly) or iam_policy_document",
	"s3write":          "grant bucket access with iam_policy (e.g. s3:ReadWrite) or iam_policy_document",
	"instanceprofile":  "name an existing role with iam_role",
	"completiondelay":  "there is no completion delay setting; see completion_file and on_complete",
}

// unknownSpawnConfigKeysError names every unknown key (as the user wrote it)
// and, for each, either a hint or the nearest known key.
func unknownSpawnConfigKeysError(keys []string) error {
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%q (%s)", k, suggestSpawnConfigKey(k)))
	}
	noun := "key"
	if len(keys) > 1 {
		noun = "keys"
	}
	return fmt.Errorf("unknown spawn config %s: %s. Accepted keys: %s. Note: this is lagotto's own schema, not `spawn launch --config` (which takes only plugins:)",
		noun, strings.Join(parts, "; "), strings.Join(SpawnConfigKeys, ", "))
}

// suggestSpawnConfigKey returns the advice for one unknown key: a hint for a
// known spawn-flag name, else "did you mean X?" for the nearest known key when
// it is close enough to be a plausible typo, else a pointer to the list.
func suggestSpawnConfigKey(key string) string {
	nk := normalizeKey(key)
	if hint, ok := spawnConfigKeyHints[nk]; ok {
		return hint
	}
	best, bestDist := "", -1
	for _, k := range SpawnConfigKeys { // ordered, so ties resolve deterministically
		d := editDistance(nk, normalizeKey(k))
		if bestDist < 0 || d < bestDist {
			best, bestDist = k, d
		}
	}
	// Allow roughly one edit per three characters (minimum 2), so a typo
	// like "instnce_type" or "idle_timout" is matched but "foo" is not
	// pinned on "ami".
	limit := len(nk) / 3
	if limit < 2 {
		limit = 2
	}
	if bestDist >= 0 && bestDist <= limit {
		return fmt.Sprintf("did you mean %s?", best)
	}
	return "no similar key"
}

// editDistance is the Levenshtein distance between a and b (bytes; keys are
// ASCII after normalization).
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}
