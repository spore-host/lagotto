package deploy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// The name `lagotto launch` builds is one the teardown prefix matches, and the
// poller's own schedule is not.
func TestLaunchScheduleNameMatchesPrefix(t *testing.T) {
	name := LaunchScheduleName("sl-0a1b2c3d")
	if name != "lagotto-launch-sl-0a1b2c3d" || !IsLaunchSchedule(name) {
		t.Errorf("LaunchScheduleName = %q (matches prefix: %v)", name, IsLaunchSchedule(name))
	}
	if !IsLaunchSchedule(name + "-r3") {
		t.Error("the poller's boundary-retry schedule must match the prefix")
	}
	if IsLaunchSchedule(PollerScheduleName) {
		t.Error("the poller schedule must never match the per-launch prefix")
	}
}

// ListLaunchSchedules asks the default group by prefix and follows every page.
func TestListLaunchSchedules_FollowsPages(t *testing.T) {
	sc := &fakeScheduler{listPages: [][]string{
		{"lagotto-launch-sl-bbbb"},
		{"lagotto-launch-sl-aaaa", "someone-elses"},
	}}
	d, _ := testDeployer(nil, nil, sc)
	got, err := d.ListLaunchSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-bbbb"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v (sorted, prefix only, both pages)", got, want)
	}
	if len(sc.listInputs) != 2 {
		t.Fatalf("ListSchedules called %d times, want 2", len(sc.listInputs))
	}
	in := sc.listInputs[0]
	if aws.ToString(in.GroupName) != "default" || aws.ToString(in.NamePrefix) != LaunchSchedulePrefix {
		t.Errorf("listed group %q prefix %q", aws.ToString(in.GroupName), aws.ToString(in.NamePrefix))
	}
}

// DeleteLaunchSchedules deletes the union of recorded and listed schedules
// once each, reports a recorded one that is gone as Absent, and never deletes
// a recorded name lagotto did not create.
func TestDeleteLaunchSchedules_RecordedAndListed(t *testing.T) {
	gone := &schedulertypes.ResourceNotFoundException{Message: aws.String("not found")}
	sc := &fakeScheduler{
		listPages:  [][]string{{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-aaaa-r1", "lagotto-launch-sl-orph"}},
		deleteErrs: map[string]error{"lagotto-launch-sl-fired": gone},
	}
	d, _ := testDeployer(nil, nil, sc)
	res, err := d.DeleteLaunchSchedules(context.Background(),
		[]string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-fired", "not-lagottos", ""})
	if err != nil {
		t.Fatal(err)
	}
	wantDeleted := []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-aaaa-r1", "lagotto-launch-sl-orph"}
	if !reflect.DeepEqual(res.Deleted, wantDeleted) || !reflect.DeepEqual(sc.deleted, wantDeleted) {
		t.Errorf("Deleted %v (calls %v), want %v", res.Deleted, sc.deleted, wantDeleted)
	}
	if !reflect.DeepEqual(res.Absent, []string{"lagotto-launch-sl-fired"}) {
		t.Errorf("Absent = %v, want the self-deleted one-shot", res.Absent)
	}
}

// A listing failure deletes nothing; a delete failure stops and names the
// schedule, keeping what was already deleted.
func TestDeleteLaunchSchedules_Errors(t *testing.T) {
	sc := &fakeScheduler{listErr: errors.New("AccessDeniedException: scheduler:ListSchedules")}
	d, _ := testDeployer(nil, nil, sc)
	if _, err := d.DeleteLaunchSchedules(context.Background(), []string{"lagotto-launch-sl-aaaa"}); err == nil ||
		!strings.Contains(err.Error(), "ListSchedules") {
		t.Errorf("err = %v, want the listing failure", err)
	}
	if sc.deleteCalls != 0 {
		t.Errorf("a failed listing still deleted %d schedule(s)", sc.deleteCalls)
	}

	sc = &fakeScheduler{
		listPages:  [][]string{{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-bbbb"}},
		deleteErrs: map[string]error{"lagotto-launch-sl-bbbb": errors.New("ThrottlingException")},
	}
	d, _ = testDeployer(nil, nil, sc)
	res, err := d.DeleteLaunchSchedules(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "lagotto-launch-sl-bbbb") {
		t.Errorf("err = %v, want one naming the failed schedule", err)
	}
	if !reflect.DeepEqual(res.Deleted, []string{"lagotto-launch-sl-aaaa"}) {
		t.Errorf("Deleted = %v, want the one deleted before the failure", res.Deleted)
	}
}

// IsScheduleOfLaunch matches a launch's own schedule and its -r<n> retries,
// and nothing of another launch whose id merely shares a prefix.
func TestIsScheduleOfLaunch(t *testing.T) {
	for name, want := range map[string]bool{
		"lagotto-launch-sl-aaaa":     true,
		"lagotto-launch-sl-aaaa-r1":  true,
		"lagotto-launch-sl-aaaa-r12": true,
		"lagotto-launch-sl-aaaa9":    false,
		"lagotto-launch-sl-aaaa-r":   false,
		"lagotto-launch-sl-aaaa-rx":  false,
		"lagotto-launch-sl-aaaa-r1x": false,
		"lagotto-launch-sl-bbbb":     false,
		"lagotto-capacity-poller":    false,
		"":                           false,
	} {
		if got := IsScheduleOfLaunch(name, "sl-aaaa"); got != want {
			t.Errorf("IsScheduleOfLaunch(%q, sl-aaaa) = %v, want %v", name, got, want)
		}
	}
	if IsScheduleOfLaunch("lagotto-launch-w-1", "w-1") {
		t.Error("a non-sl id must not produce a deletable schedule name")
	}
}

// DeleteScheduledLaunchSchedules (`cancel sl-…`, #279) deletes the launch's
// schedule and its boundary retries, lists by the launch's own prefix, treats a
// schedule already gone as success, and leaves every other schedule alone.
func TestDeleteScheduledLaunchSchedules(t *testing.T) {
	gone := &schedulertypes.ResourceNotFoundException{Message: aws.String("not found")}
	sc := &fakeScheduler{
		// The fake ignores NamePrefix, as an emulator might.
		listPages: [][]string{
			{"lagotto-launch-sl-aaaa-r1", "lagotto-launch-sl-aaaa9", "lagotto-launch-sl-bbbb"},
			{"lagotto-launch-sl-aaaa-r2", "lagotto-capacity-poller"},
		},
		// The one-shot already fired and removed itself; r1 likewise.
		deleteErrs: map[string]error{"lagotto-launch-sl-aaaa": gone, "lagotto-launch-sl-aaaa-r1": gone},
	}
	d, _ := testDeployer(nil, nil, sc)
	res, err := d.DeleteScheduledLaunchSchedules(context.Background(), "sl-aaaa", "lagotto-launch-sl-aaaa")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-aaaa-r2"}; !reflect.DeepEqual(res.Deleted, want) {
		t.Errorf("Deleted = %v, want %v", res.Deleted, want)
	}
	if want := []string{"lagotto-launch-sl-aaaa", "lagotto-launch-sl-aaaa-r1"}; !reflect.DeepEqual(res.Absent, want) {
		t.Errorf("Absent = %v, want %v", res.Absent, want)
	}
	if want := []string{"lagotto-launch-sl-aaaa-r2"}; !reflect.DeepEqual(sc.deleted, want) {
		t.Errorf("deleted %v, want only %v", sc.deleted, want)
	}
	if sc.deleteCalls != 3 {
		t.Errorf("DeleteSchedule called %d times, want 3", sc.deleteCalls)
	}
	in := sc.listInputs[0]
	if aws.ToString(in.GroupName) != "default" || aws.ToString(in.NamePrefix) != "lagotto-launch-sl-aaaa" {
		t.Errorf("listed group %q prefix %q", aws.ToString(in.GroupName), aws.ToString(in.NamePrefix))
	}
}

// A recorded name that is not the launch's own is never deleted, and with no
// recorded name the launch's own schedule is still tried.
func TestDeleteScheduledLaunchSchedules_RecordedNameIsChecked(t *testing.T) {
	sc := &fakeScheduler{}
	d, _ := testDeployer(nil, nil, sc)
	res, err := d.DeleteScheduledLaunchSchedules(context.Background(), "sl-aaaa", "lagotto-capacity-poller")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-aaaa"}; !reflect.DeepEqual(res.Deleted, want) {
		t.Errorf("Deleted = %v, want %v", res.Deleted, want)
	}
}

// A listing error deletes nothing; a delete error other than not-found stops.
func TestDeleteScheduledLaunchSchedules_Errors(t *testing.T) {
	sc := &fakeScheduler{listErr: errors.New("AccessDeniedException")}
	d, _ := testDeployer(nil, nil, sc)
	if _, err := d.DeleteScheduledLaunchSchedules(context.Background(), "sl-aaaa", ""); err == nil {
		t.Fatal("a list error was swallowed")
	}
	if sc.deleteCalls != 0 {
		t.Errorf("deleted %d schedules after a failed listing", sc.deleteCalls)
	}

	sc = &fakeScheduler{deleteErrs: map[string]error{"lagotto-launch-sl-aaaa": errors.New("AccessDeniedException")}}
	d, _ = testDeployer(nil, nil, sc)
	if _, err := d.DeleteScheduledLaunchSchedules(context.Background(), "sl-aaaa", ""); err == nil ||
		!strings.Contains(err.Error(), "lagotto-launch-sl-aaaa") {
		t.Fatalf("delete error = %v, want it naming the schedule", err)
	}
}
