package deploy

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
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
	// lagotto's group (absent here: not deployed by a #324 lagotto), then both
	// pages of the default group.
	if len(sc.listInputs) != 3 {
		t.Fatalf("ListSchedules called %d times, want 3", len(sc.listInputs))
	}
	for i, group := range []string{"lagotto", "default", "default"} {
		in := sc.listInputs[i]
		if aws.ToString(in.GroupName) != group || aws.ToString(in.NamePrefix) != LaunchSchedulePrefix {
			t.Errorf("list %d: group %q prefix %q, want %q", i, aws.ToString(in.GroupName), aws.ToString(in.NamePrefix), group)
		}
	}
}

// #324: per-launch schedules are listed in both groups, and each is deleted
// in the group it is in, so neither a new one (lagotto's group) nor one made
// before #324 (default) is left firing.
func TestDeleteLaunchSchedules_BothGroups(t *testing.T) {
	sc := &fakeScheduler{groupExists: true, listByGroup: map[string][][]string{
		"lagotto": {{"lagotto-launch-sl-new", "lagotto-launch-sl-both"}},
		"default": {{"lagotto-launch-sl-old", "lagotto-launch-sl-both"}},
	}}
	d, _ := testDeployer(nil, nil, sc)
	names, err := d.ListLaunchSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-both", "lagotto-launch-sl-new", "lagotto-launch-sl-old"}; !reflect.DeepEqual(names, want) {
		t.Errorf("ListLaunchSchedules = %v, want %v (deduplicated)", names, want)
	}

	res, err := d.DeleteLaunchSchedules(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"default/lagotto-launch-sl-both", "lagotto/lagotto-launch-sl-both", "lagotto/lagotto-launch-sl-new", "default/lagotto-launch-sl-old"}
	got := append([]string(nil), sc.deletedRefs...)
	if len(got) != len(want) {
		t.Fatalf("deleted %v, want %v", got, want)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || g == w
		}
		if !found {
			t.Errorf("did not delete %s (deleted %v)", w, got)
		}
	}
	if len(res.Deleted) != 3 {
		t.Errorf("Deleted = %v, want each name once", res.Deleted)
	}
	// The fake keeps listing what it was seeded with, so the group is not empty.
	if res.GroupDeleted || sc.groupDeletes != 0 {
		t.Error("deleted a schedule group that still lists schedules")
	}
}

// The group goes once nothing is left in it, and a recorded name the listing
// does not show is tried in both groups; an AccessDenied from lagotto's group
// (a pre-#324 CLI-user policy) then reads as absent, since the listing
// covered that group.
func TestDeleteLaunchSchedules_DeletesTheEmptyGroup(t *testing.T) {
	sc := &fakeScheduler{groupExists: true, listByGroup: map[string][][]string{},
		deleteByGroup: map[string]error{"lagotto/lagotto-launch-sl-rec": &smithyAccessDenied{}}}
	d, _ := testDeployer(nil, nil, sc)
	res, err := d.DeleteLaunchSchedules(context.Background(), []string{"lagotto-launch-sl-rec"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"lagotto-launch-sl-rec"}; !reflect.DeepEqual(res.Deleted, want) {
		t.Errorf("Deleted = %v, want %v (deleted from the default group)", res.Deleted, want)
	}
	if !res.GroupDeleted || sc.groupExists {
		t.Error("the empty schedule group was not deleted")
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
	// r1 and r2 in the default group, where they are listed, and the launch's
	// own (not listed) in both groups.
	if sc.deleteCalls != 4 {
		t.Errorf("DeleteSchedule called %d times, want 4", sc.deleteCalls)
	}
	for i, group := range []string{"lagotto", "default"} {
		in := sc.listInputs[i]
		if aws.ToString(in.GroupName) != group || aws.ToString(in.NamePrefix) != "lagotto-launch-sl-aaaa" {
			t.Errorf("list %d: group %q prefix %q", i, aws.ToString(in.GroupName), aws.ToString(in.NamePrefix))
		}
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

// smithyAccessDenied is an AccessDenied API error.
type smithyAccessDenied struct{}

func (*smithyAccessDenied) Error() string                 { return "AccessDeniedException: not authorized" }
func (*smithyAccessDenied) ErrorCode() string             { return "AccessDeniedException" }
func (*smithyAccessDenied) ErrorMessage() string          { return "not authorized" }
func (*smithyAccessDenied) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }
