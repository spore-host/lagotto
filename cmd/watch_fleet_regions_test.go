package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// #275: a --maintain fleet with no --regions can't be counted, so the poller
// could never top it up. `watch` and `update` refuse it.

func TestValidateFleetRegions(t *testing.T) {
	tests := []struct {
		name     string
		maintain int
		regions  []string
		wantErr  bool
	}{
		{"single-shot, no regions (every enabled region)", 0, nil, false},
		{"fleet with a region", 4, []string{"us-east-1"}, false},
		{"fleet with no regions", 4, nil, true},
		{"fleet with only blank regions", 4, []string{" ", ""}, true},
	}
	for _, tt := range tests {
		err := validateFleetRegions(tt.maintain, tt.regions)
		if tt.wantErr != errors.Is(err, errFleetNeedsRegions) {
			t.Errorf("%s: err = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
	if !strings.Contains(errFleetNeedsRegions.Error(), "--regions") {
		t.Errorf("refusal does not point at --regions: %v", errFleetNeedsRegions)
	}
}

// TestValidateWatchSearch_FleetNeedsRegions: the shared check `watch` and
// `update` both run refuses the stored shape.
func TestValidateWatchSearch_FleetNeedsRegions(t *testing.T) {
	w := baseUpdateWatch()
	w.Action = watcher.ActionSpawn
	w.LaunchConfigJSON = []byte(`{"instance_type":"g5.xlarge"}`)
	w.DesiredCount = 4
	if err := validateWatchSearch(w); err != nil {
		t.Fatalf("fleet with regions: %v", err)
	}
	w.Regions = nil
	if err := validateWatchSearch(w); !errors.Is(err, errFleetNeedsRegions) {
		t.Errorf("fleet with no regions: err = %v, want errFleetNeedsRegions", err)
	}
}

// TestRunWatch_MaintainNeedsRegions: `lagotto watch --maintain 4` with no
// --regions is refused before any AWS call (the hermetic guard would panic on
// one), so nothing is stored.
func TestRunWatch_MaintainNeedsRegions(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(cfgPath, []byte("instance_type: g5.xlarge\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pa, pc, pm, pr, pq := watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota
	t.Cleanup(func() {
		watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota = pa, pc, pm, pr, pq
	})
	watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota = "spawn", cfgPath, 4, nil, false
	if err := runWatch(watchCmd, []string{"g5.xlarge"}); !errors.Is(err, errFleetNeedsRegions) {
		t.Fatalf("err = %v, want errFleetNeedsRegions", err)
	}
}

// TestUpdate_FleetWithNoRegionsRefused: updating a stored fleet that names no
// regions (stored before #275) is refused until it names some, and nothing is
// written.
func TestUpdate_FleetWithNoRegionsRefused(t *testing.T) {
	item := strings.Replace(updateWatchItem("active"), `"regions":{"L":[{"S":"us-east-1"}]},`, ``, 1)
	item = strings.Replace(item, `"action":{"S":"notify"}`,
		`"action":{"S":"spawn"},"desired_count":{"N":"4"},"launch_config_json":{"B":"e30="}`, 1)
	f := &fakeAWS{scheduleState: "DISABLED", item: item}
	_, err := runUpdateWithFake(t, f, map[string]string{"project": "renamed"})
	if !errors.Is(err, errFleetNeedsRegions) {
		t.Fatalf("err = %v, want errFleetNeedsRegions", err)
	}
	if n := f.called("dynamodb:UpdateItem"); n != 0 {
		t.Errorf("%d UpdateItem after the refusal; nothing may be written", n)
	}
}
