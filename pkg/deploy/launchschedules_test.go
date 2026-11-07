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
