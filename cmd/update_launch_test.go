package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

type fakeLaunchUpdate struct {
	file    *watcher.SpawnConfigFile
	loadErr error
	roleErr error
	swapErr error
	swapped [][]byte
}

func (f *fakeLaunchUpdate) updater() scheduledLaunchUpdater {
	return scheduledLaunchUpdater{
		load: func(context.Context, string) (*watcher.SpawnConfigFile, error) { return f.file, f.loadErr },
		describe: func(context.Context, string, string) (*spawnaws.CapacityReservation, error) {
			return &spawnaws.CapacityReservation{ReservationType: "capacity-block", InstanceType: "p5.48xlarge", AvailabilityZone: "us-east-1a", State: "scheduled"}, nil
		},
		checkRole: func(context.Context, []byte) error { return f.roleErr },
		swap: func(_ context.Context, _ *watcher.ScheduledLaunch, c []byte) error {
			if f.swapErr != nil {
				return f.swapErr
			}
			f.swapped = append(f.swapped, c)
			return nil
		},
	}
}

func armedInlineLaunch() *watcher.ScheduledLaunch {
	return &watcher.ScheduledLaunch{ScheduleID: "AROAX:alice/sl-0011aabb", Status: watcher.ScheduledPending, Region: "us-east-1",
		LaunchConfigJSON: []byte(`{"iampolicydocument":"{}","instancetype":"p5.48xlarge"}`)}
}

// #309: a refused config leaves the launch exactly as it was (no swap, and the
// schedule is never an input), and says so.
func TestUpdateScheduledLaunch_RefusedConfigChangesNothing(t *testing.T) {
	for name, f := range map[string]*fakeLaunchUpdate{
		"invalid config":    {loadErr: errors.New("spawn config sets iam_policy_file: the hosted poller never creates or changes IAM roles")},
		"unauthorized role": {file: &watcher.SpawnConfigFile{InstanceType: "p5.48xlarge", IAMRole: "r"}, roleErr: errors.New("not authorized to use it")},
		"conflict":          {file: &watcher.SpawnConfigFile{InstanceType: "p5.48xlarge", IAMRole: "r"}, swapErr: watcher.ErrScheduledLaunchConfigChanged},
	} {
		var out strings.Builder
		err := updateScheduledLaunch(context.Background(), &out, armedInlineLaunch(), "new.yaml", false, f.updater())
		if err == nil || !strings.Contains(err.Error(), "was not changed: its schedule still fires its current config") {
			t.Errorf("%s: err = %v, want a refusal that says the launch is unchanged", name, err)
		}
		if name != "conflict" && len(f.swapped) != 0 {
			t.Errorf("%s: swapped %d time(s) after a refusal", name, len(f.swapped))
		}
	}
}

func TestUpdateScheduledLaunch_SwapsAValidConfig(t *testing.T) {
	f := &fakeLaunchUpdate{file: &watcher.SpawnConfigFile{InstanceType: "p5.48xlarge", IAMRole: "research-role", Name: "other"}}
	sl := armedInlineLaunch()
	sl.InstanceName = "train"
	var out strings.Builder
	if err := updateScheduledLaunch(context.Background(), &out, sl, "new.yaml", false, f.updater()); err != nil {
		t.Fatal(err)
	}
	if len(f.swapped) != 1 || !strings.Contains(string(f.swapped[0]), `"iamrole":"research-role"`) {
		t.Fatalf("swapped = %q", f.swapped)
	}
	if !strings.Contains(out.String(), "sl-0011aabb updated") || !strings.Contains(out.String(), `stays "train"`) {
		t.Errorf("output = %q", out.String())
	}

	// Dry run: every check, no write.
	f.swapped = nil
	out.Reset()
	if err := updateScheduledLaunch(context.Background(), &out, armedInlineLaunch(), "new.yaml", true, f.updater()); err != nil || len(f.swapped) != 0 {
		t.Errorf("dry run: err %v, swapped %d", err, len(f.swapped))
	}
}

// A launch into a reservation keeps targeting it: the new config gets the
// reservation re-applied, as `lagotto launch` applies it.
func TestUpdateScheduledLaunch_ReappliesTheReservation(t *testing.T) {
	f := &fakeLaunchUpdate{file: &watcher.SpawnConfigFile{IAMRole: "research-role"}}
	sl := armedInlineLaunch()
	sl.ReservationID = "cr-0123456789abcdef0"
	if err := updateScheduledLaunch(context.Background(), &strings.Builder{}, sl, "new.yaml", false, f.updater()); err != nil {
		t.Fatal(err)
	}
	got := string(f.swapped[0])
	for _, want := range []string{`"reservationid":"cr-0123456789abcdef0"`, `"capacityblock":true`, `"instancetype":"p5.48xlarge"`} {
		if !strings.Contains(got, want) {
			t.Errorf("config %s lacks %s", got, want)
		}
	}
}

func TestUpdateScheduledLaunch_NotArmed(t *testing.T) {
	sl := armedInlineLaunch()
	sl.Status = watcher.ScheduledFailed
	sl.LastError = "launch config refused: spawn config sets iam_policy_document"
	f := &fakeLaunchUpdate{file: &watcher.SpawnConfigFile{InstanceType: "p5.48xlarge"}}
	err := updateScheduledLaunch(context.Background(), &strings.Builder{}, sl, "new.yaml", false, f.updater())
	if err == nil || !strings.Contains(err.Error(), "will not fire again") || !strings.Contains(err.Error(), sl.LastError) {
		t.Errorf("err = %v", err)
	}
}

func TestValidateForScheduledLaunch(t *testing.T) {
	if err := (updateOpts{}).validateForScheduledLaunch(); err == nil || !strings.Contains(err.Error(), "--spawn-config") {
		t.Errorf("no --spawn-config: %v", err)
	}
	if err := (updateOpts{spawnConfig: "a.yaml", regionsSet: true}).validateForScheduledLaunch(); err == nil || !strings.Contains(err.Error(), "only --spawn-config") {
		t.Errorf("--regions: %v", err)
	}
	if err := (updateOpts{spawnConfig: "a.yaml", dryRun: true}).validateForScheduledLaunch(); err != nil {
		t.Errorf("--spawn-config --dry-run: %v", err)
	}
}
