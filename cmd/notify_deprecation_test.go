package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// --action notify is deprecated (#283): `watch` and `update` warn on stderr,
// keep stdout and -o json clean, and -o json carries a "deprecations" field.

// runWatchCaptured runs `lagotto watch g5.xlarge` against f with action ("" =
// no --action, the notify default) and format, returning stdout and stderr.
func runWatchCaptured(t *testing.T, f *fakeAWS, action, format string) (string, string) {
	t.Helper()
	prevAction, prevFormat := watchAction, outputFormat
	t.Cleanup(func() {
		watchAction, outputFormat = prevAction, prevFormat
		watchCmd.Flags().Lookup("action").Changed = false
		watchCmd.SetErr(nil)
	})
	outputFormat = format
	if action != "" {
		if err := watchCmd.Flags().Set("action", action); err != nil {
			t.Fatal(err)
		}
	}
	var errOut bytes.Buffer
	watchCmd.SetErr(&errOut)
	var out bytes.Buffer
	installFakeAWS(t, f)
	stubSubnetCoverage(t)
	watchCmd.SetOut(&out)
	t.Cleanup(func() { watchCmd.SetOut(nil) })
	if err := runWatch(watchCmd, []string{"g5.xlarge"}); err != nil {
		t.Fatalf("watch --action %q: %v\nstderr:\n%s", action, err, errOut.String())
	}
	return out.String(), errOut.String()
}

func stubSubnetCoverage(t *testing.T) {
	t.Helper()
	prev := newSubnetCoverageChecker
	newSubnetCoverageChecker = func(aws.Config) *watcher.SubnetCoverageChecker {
		return watcher.NewSubnetCoverageCheckerWith(
			func(string) watcher.SubnetCoverageEC2 { return subnetEC2{} },
			func(context.Context) ([]string, error) { return nil, errors.New("no EC2 in the fake") })
	}
	t.Cleanup(func() { newSubnetCoverageChecker = prev })
}

func TestWatchNotify_WarnsOnStderr(t *testing.T) {
	for _, tc := range []struct {
		name, action string
		defaulted    bool
	}{
		{"no --action (notify default)", "", true},
		{"--action notify", "notify", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut := runWatchCaptured(t, &fakeAWS{scheduleState: "ENABLED"}, tc.action, "table")
			if !strings.Contains(errOut, "warning: "+strings.SplitN(watcher.NotifyDeprecationMessage(!tc.defaulted), "\n", 2)[0]) ||
				!strings.Contains(errOut, "not a capacity check") || !strings.Contains(errOut, "--action hold") ||
				!strings.Contains(errOut, "--action spawn") {
				t.Errorf("stderr:\n%s", errOut)
			}
			if got := strings.Contains(errOut, watcher.NotifyDefaultActionNote); got != tc.defaulted {
				t.Errorf("default-action note present = %v, want %v:\n%s", got, tc.defaulted, errOut)
			}
			if !strings.Contains(out, "Created watch") || strings.Contains(out, "deprecated") || strings.Contains(out, "warning:") {
				t.Errorf("stdout is not clean:\n%s", out)
			}
		})
	}
}

func TestWatchNotify_JSONHasDeprecations(t *testing.T) {
	out, errOut := runWatchCaptured(t, &fakeAWS{scheduleState: "ENABLED"}, "", "json")
	var got struct {
		WatchID      string                `json:"watch_id"`
		Deprecations []watcher.Deprecation `json:"deprecations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, out)
	}
	if got.WatchID == "" || len(got.Deprecations) != 1 || got.Deprecations[0].ID != watcher.DeprecationActionNotify ||
		got.Deprecations[0].Message != watcher.NotifyDeprecationMessage(false) {
		t.Errorf("json: %+v", got)
	}
	if !strings.Contains(errOut, "warning: no --action given") {
		t.Errorf("stderr:\n%s", errOut)
	}
}

func TestWatchHold_NoDeprecation(t *testing.T) {
	_, errOut := runWatchCaptured(t, &fakeAWS{scheduleState: "ENABLED"}, "hold", "table")
	if strings.Contains(errOut, "deprecated") {
		t.Errorf("a hold watch warned:\n%s", errOut)
	}
	out, errOut := runWatchCaptured(t, &fakeAWS{scheduleState: "ENABLED"}, "hold", "json")
	if strings.Contains(out, "deprecations") || strings.Contains(errOut, "deprecated") {
		t.Errorf("a hold watch carries a deprecation:\nstdout %s\nstderr %s", out, errOut)
	}
}

// TestEncodeWatchJSON_Spawn: a spawn watch's JSON (its create path needs a
// spawn config and the instance-role check, so it is checked here directly)
// has no deprecations field; a notify one does.
func TestEncodeWatchJSON_Spawn(t *testing.T) {
	w := &watcher.Watch{WatchID: "w-1", Action: watcher.ActionSpawn}
	var b bytes.Buffer
	if err := encodeWatchJSON(&b, w, preflightResult{}, watcher.ActionDeprecations(w.Action, true)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "deprecations") {
		t.Errorf("spawn JSON:\n%s", b.String())
	}
	b.Reset()
	w.Action = watcher.ActionNotify
	if err := encodeWatchJSON(&b, w, preflightResult{}, watcher.ActionDeprecations(w.Action, true)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `"id": "action-notify"`) || !strings.Contains(b.String(), `"watch_id": "w-1"`) {
		t.Errorf("notify JSON:\n%s", b.String())
	}
}

func runUpdateCaptured(t *testing.T, item string, flags map[string]string) (string, string, error) {
	t.Helper()
	var errOut bytes.Buffer
	updateCmd.SetErr(&errOut)
	t.Cleanup(func() { updateCmd.SetErr(nil) })
	out, err := runUpdateWithFake(t, &fakeAWS{scheduleState: "ENABLED", item: item}, flags)
	return out, errOut.String(), err
}

func TestUpdateNotify_Warns(t *testing.T) {
	out, errOut, err := runUpdateCaptured(t, updateWatchItem("active"), map[string]string{"spot": "true"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errOut, "warning: --action notify is deprecated") || strings.Contains(errOut, "no --action given") ||
		!strings.Contains(errOut, "lagotto cancel w-1") {
		t.Errorf("stderr:\n%s", errOut)
	}
	if !strings.Contains(out, "Updated watch w-1") || strings.Contains(out, "deprecated") {
		t.Errorf("stdout is not clean:\n%s", out)
	}
}

func TestUpdateNotify_DryRunWarns(t *testing.T) {
	_, errOut, err := runUpdateCaptured(t, updateWatchItem("active"), map[string]string{"spot": "true", "dry-run": "true"})
	if err != nil || !strings.Contains(errOut, "warning: --action notify is deprecated") {
		t.Errorf("err %v, stderr:\n%s", err, errOut)
	}
}

func TestUpdateNotify_JSON(t *testing.T) {
	prev := outputFormat
	outputFormat = "json"
	t.Cleanup(func() { outputFormat = prev })
	out, errOut, err := runUpdateCaptured(t, updateWatchItem("active"), map[string]string{"spot": "true"})
	if err != nil {
		t.Fatal(err)
	}
	var got updateResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, out)
	}
	if len(got.Deprecations) != 1 || got.Deprecations[0].ID != watcher.DeprecationActionNotify ||
		!strings.HasPrefix(got.Deprecations[0].Message, watcher.NotifyDeprecation) {
		t.Errorf("deprecations %+v", got.Deprecations)
	}
	if !strings.Contains(errOut, "warning: --action notify") {
		t.Errorf("stderr:\n%s", errOut)
	}
}

func TestUpdateHold_NoDeprecation(t *testing.T) {
	item := strings.Replace(updateWatchItem("active"), `"action":{"S":"notify"}`, `"action":{"S":"hold"}`, 1)
	prev := outputFormat
	outputFormat = "json"
	t.Cleanup(func() { outputFormat = prev })
	out, errOut, err := runUpdateCaptured(t, item, map[string]string{"spot": "true"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "deprecations") || strings.Contains(errOut, "deprecated") {
		t.Errorf("a hold watch carries a deprecation:\nstdout %s\nstderr %s", out, errOut)
	}
}

func TestAppendNotifyWatchCheck(t *testing.T) {
	r := &doctor.Report{}
	appendNotifyWatchCheck(context.Background(), r, func(context.Context) ([]watcher.Watch, error) {
		return nil, errors.New("AccessDenied")
	})
	appendNotifyWatchCheck(context.Background(), r, func(context.Context) ([]watcher.Watch, error) {
		return []watcher.Watch{{WatchID: "w-s", Action: watcher.ActionSpawn}}, nil
	})
	if len(r.Checks) != 0 {
		t.Fatalf("a failed read or no notify watch added a check: %+v", r.Checks)
	}
	appendNotifyWatchCheck(context.Background(), r, func(context.Context) ([]watcher.Watch, error) {
		return []watcher.Watch{{WatchID: "w-n", Action: watcher.ActionNotify}}, nil
	})
	if len(r.Checks) != 1 || r.Checks[0].Status != doctor.StatusWarn || r.Failed() {
		t.Errorf("checks %+v", r.Checks)
	}
	if err := doctorExitError(r); err != nil {
		t.Errorf("a notify watch failed doctor: %v", err)
	}
}
