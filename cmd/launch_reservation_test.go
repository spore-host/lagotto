package cmd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

func fakeDescribe(cr *spawnaws.CapacityReservation) describeReservationFunc {
	return func(_ context.Context, _, id string) (*spawnaws.CapacityReservation, error) {
		c := *cr
		c.ReservationID = id
		return &c, nil
	}
}

var testBlock = &spawnaws.CapacityReservation{
	State:            "scheduled",
	ReservationType:  "capacity-block",
	InstanceType:     "p5.48xlarge",
	AvailabilityZone: "us-east-1a",
}

// storedLaunchConfig is what the poller hands spawn's launcher for a stored
// scheduled launch: the stored JSON, read back as LaunchScheduled reads it.
func storedLaunchConfig(t *testing.T, stored []byte) spawnaws.LaunchConfig {
	t.Helper()
	var f watcher.SpawnConfigFile
	if err := json.Unmarshal(stored, &f); err != nil {
		t.Fatalf("unmarshal stored config: %v", err)
	}
	return f.ToLaunchConfig()
}

// TestPrepareLaunchConfig_ReservationIDTargetsLaunch is #210: --reservation-id
// must make the launch target the reservation, not only time it. block.yaml
// here sets neither reservation_id nor capacity_block nor instance_type nor
// an AZ, the case that launched an On-Demand instance outside the paid block.
func TestPrepareLaunchConfig_ReservationIDTargetsLaunch(t *testing.T) {
	file := &watcher.SpawnConfigFile{Name: "block-job", AMI: "ami-0123", TTL: "6h", Command: "train.sh"}
	az := ""
	stored, cr, err := prepareLaunchConfig(context.Background(), fakeDescribe(testBlock), "us-east-1", file, "cr-0block", &az)
	if err != nil {
		t.Fatalf("prepareLaunchConfig: %v", err)
	}
	if cr == nil || cr.ReservationID != "cr-0block" {
		t.Errorf("returned reservation %+v, want cr-0block", cr)
	}
	lc := storedLaunchConfig(t, stored)
	if lc.ReservationID != "cr-0block" {
		t.Errorf("launch config ReservationID = %q, want cr-0block", lc.ReservationID)
	}
	if !lc.CapacityBlock {
		t.Error("launch config CapacityBlock = false for a capacity-block reservation")
	}
	if lc.InstanceType != "p5.48xlarge" {
		t.Errorf("launch config InstanceType = %q, want the reservation's p5.48xlarge", lc.InstanceType)
	}
	if az != "us-east-1a" {
		t.Errorf("launch AZ = %q, want the reservation's us-east-1a", az)
	}
	// The rest of the config survives.
	if lc.Name != "block-job" || lc.JobArrayCommand != "train.sh" || lc.AMI != "ami-0123" {
		t.Errorf("unrelated fields changed: %+v", lc)
	}
}

// TestPrepareLaunchConfig_ODCR: an ordinary capacity reservation (a hold's)
// is targeted without the capacity-block market type.
func TestPrepareLaunchConfig_ODCR(t *testing.T) {
	odcr := &spawnaws.CapacityReservation{State: "active", ReservationType: "default", InstanceType: "g7e.8xlarge", AvailabilityZone: "us-west-2b"}
	file := &watcher.SpawnConfigFile{InstanceType: "g7e.8xlarge", TTL: "6h"}
	az := "us-west-2b"
	stored, _, err := prepareLaunchConfig(context.Background(), fakeDescribe(odcr), "us-west-2", file, "cr-0odcr", &az)
	if err != nil {
		t.Fatalf("prepareLaunchConfig: %v", err)
	}
	lc := storedLaunchConfig(t, stored)
	if lc.ReservationID != "cr-0odcr" || lc.CapacityBlock {
		t.Errorf("launch config ReservationID=%q CapacityBlock=%v, want cr-0odcr, false", lc.ReservationID, lc.CapacityBlock)
	}
}

// TestPrepareLaunchConfig_Refusals: a value that can't launch into the
// reservation is an error at schedule time, naming the conflict.
func TestPrepareLaunchConfig_Refusals(t *testing.T) {
	cases := []struct {
		name string
		file watcher.SpawnConfigFile
		cr   *spawnaws.CapacityReservation
		az   string
		want string
	}{
		{"file names another reservation", watcher.SpawnConfigFile{ReservationID: "cr-0other"}, testBlock, "", "cr-0other"},
		{"instance type differs", watcher.SpawnConfigFile{InstanceType: "p4d.24xlarge"}, testBlock, "", "p4d.24xlarge"},
		{"az differs", watcher.SpawnConfigFile{}, testBlock, "us-east-1b", "us-east-1b"},
		{"spot", watcher.SpawnConfigFile{Spot: true}, testBlock, "", "spot"},
		{"capacity_block on an ODCR", watcher.SpawnConfigFile{CapacityBlock: true},
			&spawnaws.CapacityReservation{State: "active", ReservationType: "default", InstanceType: "p5.48xlarge"}, "", "capacity_block"},
		{"dead reservation", watcher.SpawnConfigFile{}, &spawnaws.CapacityReservation{State: "expired"}, "", "expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := tc.file
			az := tc.az
			_, _, err := prepareLaunchConfig(context.Background(), fakeDescribe(tc.cr), "us-east-1", &file, "cr-0block", &az)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want an error mentioning %q", err, tc.want)
			}
		})
	}

	// The same id in the file and the flag is fine.
	file := watcher.SpawnConfigFile{ReservationID: "cr-0block", CapacityBlock: true, InstanceType: "p5.48xlarge"}
	az := "us-east-1a"
	if _, _, err := prepareLaunchConfig(context.Background(), fakeDescribe(testBlock), "us-east-1", &file, "cr-0block", &az); err != nil {
		t.Errorf("matching file and flag: %v", err)
	}
}

// TestPrepareLaunchConfig_NoReservationNoDescribe: without --reservation-id
// the config is stored as loaded and nothing is described.
func TestPrepareLaunchConfig_NoReservationNoDescribe(t *testing.T) {
	calls := 0
	describe := func(context.Context, string, string) (*spawnaws.CapacityReservation, error) {
		calls++
		return testBlock, nil
	}
	file := &watcher.SpawnConfigFile{InstanceType: "t3.micro", TTL: "1h"}
	az := ""
	stored, cr, err := prepareLaunchConfig(context.Background(), describe, "us-east-1", file, "", &az)
	if err != nil || cr != nil || calls != 0 {
		t.Fatalf("err=%v cr=%v describes=%d, want nil, nil, 0", err, cr, calls)
	}
	if lc := storedLaunchConfig(t, stored); lc.ReservationID != "" || lc.InstanceType != "t3.micro" || az != "" {
		t.Errorf("config changed without a reservation: %+v az=%q", lc, az)
	}
}
