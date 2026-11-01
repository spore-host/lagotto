package watcher

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
	spawnttl "github.com/spore-host/spawn/pkg/ttl"
)

// storedTTLCases are spawn-config ttl strings as they may sit in DynamoDB, and
// the lifetime the launched instance must get. The "legacy" rows were accepted
// by the pre-#201 truncating parser, so a watch created before the fix can
// hold them; they must still launch (with the duration they were accepted as),
// never fail at match time.
var storedTTLCases = []struct {
	stored string
	want   time.Duration
	legacy bool
}{
	{"", 24 * time.Hour, false}, // #38 default
	{"24h", 24 * time.Hour, false},
	{"90m", 90 * time.Minute, false},
	{"7d", 7 * 24 * time.Hour, false},
	{"1w", 7 * 24 * time.Hour, false}, // spawn's parser has no w unit
	{"1.5d", 36 * time.Hour, false},
	{"2d12h", 60 * time.Hour, false},
	{"5abcd", 5 * 24 * time.Hour, true},
	{"1.5xd", 24 * time.Hour, true},
}

// TestStoredLaunchTTL_ReachesSpawnParser pins the stored-watch side of #201
// through the real mechanism: the TTL a stored watch hands launcher.Provision
// must be one spawn's own TTL parser (spawn pkg/ttl, which spawn's launcher
// uses to write spawn:ttl-deadline and refuses to launch without) accepts, and
// it must be the intended duration. Both re-read paths are covered: watch
// match (Spawn) and scheduled fire (LaunchScheduled).
func TestStoredLaunchTTL_ReachesSpawnParser(t *testing.T) {
	for _, tc := range storedTTLCases {
		raw, err := json.Marshal(SpawnConfigFile{Name: "job", InstanceType: "g5.xlarge", Region: "us-east-1", TTL: tc.stored})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var gotTTLs []string
		sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
			gotTTLs = append(gotTTLs, cfg.TTL)
			return &spawnaws.LaunchResult{InstanceID: "i-ok"}, nil
		})
		w := &Watch{WatchID: "w-stored", LaunchConfigJSON: raw}
		m := &MatchResult{Region: "us-east-1", CandidateAZs: []string{"us-east-1a"}}
		if err := sp.Spawn(context.Background(), w, m); err != nil {
			t.Errorf("stored ttl %q: Spawn failed: %v", tc.stored, err)
			continue
		}
		sl := &ScheduledLaunch{ScheduleID: "sl-1", Region: "us-east-1", AvailabilityZone: "us-east-1a", InstanceName: "job", IfExists: IfExistsLaunch, LaunchConfigJSON: raw}
		if _, err := sp.LaunchScheduled(context.Background(), sl); err != nil {
			t.Errorf("stored ttl %q: LaunchScheduled failed: %v", tc.stored, err)
			continue
		}
		if len(gotTTLs) != 2 {
			t.Fatalf("stored ttl %q: provision called %d times, want 2", tc.stored, len(gotTTLs))
		}
		for _, got := range gotTTLs {
			d, err := spawnttl.ParseTTL(got)
			if err != nil {
				t.Errorf("stored ttl %q reached provision as %q, which spawn's launcher refuses: %v", tc.stored, got, err)
				continue
			}
			if d != tc.want {
				t.Errorf("stored ttl %q reached provision as %q = %v, want %v", tc.stored, got, d, tc.want)
			}
		}
	}
}

// TestStoredLaunchTTL_WarnsOnlyForLegacy: the tolerant re-read is not silent.
// A value only the old parser accepted produces a warning naming it; a valid
// value produces none.
func TestStoredLaunchTTL_WarnsOnlyForLegacy(t *testing.T) {
	for _, tc := range storedTTLCases {
		_, warning := storedLaunchTTL(tc.stored)
		if tc.legacy {
			if !strings.Contains(warning, `"`+tc.stored+`"`) {
				t.Errorf("storedLaunchTTL(%q) warning = %q, want one naming the stored value", tc.stored, warning)
			}
			// And creation would now refuse it (strict there).
			cfg := SpawnConfigFile{TTL: tc.stored}
			if err := cfg.ValidateAndDefaultTTL(); err == nil {
				t.Errorf("ValidateAndDefaultTTL accepted legacy-only ttl %q at creation", tc.stored)
			}
		} else if warning != "" {
			t.Errorf("storedLaunchTTL(%q) warned for a valid value: %q", tc.stored, warning)
		}
	}
}
