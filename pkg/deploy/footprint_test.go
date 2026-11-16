package deploy

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// TestDeleteScheduleGroupForFootprint: the plan-driven teardown (#325)
// deletes only lagotto's groups, and never one that still holds a schedule
// (DeleteScheduleGroup would delete it too).
func TestDeleteScheduleGroupForFootprint(t *testing.T) {
	ctx := context.Background()
	sc := &fakeScheduler{groupExists: true, listByGroup: map[string][][]string{"lagotto": {{"lagotto-launch-sl-1"}}}}
	d, _ := testDeployer(nil, nil, sc)
	if _, err := d.DeleteScheduleGroup(ctx, "default"); err == nil || sc.groupDeletes != 0 {
		t.Errorf("deleted the default group: err %v, deletes %d", err, sc.groupDeletes)
	}
	if gone, err := d.DeleteScheduleGroup(ctx, "lagotto"); !errors.Is(err, ErrScheduleGroupNotEmpty) || gone || sc.groupDeletes != 0 {
		t.Errorf("non-empty group: gone %v, err %v, deletes %d", gone, err, sc.groupDeletes)
	}
	sc.listByGroup = map[string][][]string{}
	if gone, err := d.DeleteScheduleGroup(ctx, "lagotto"); !gone || err != nil {
		t.Errorf("empty group: gone %v, err %v", gone, err)
	}
	if gone, err := d.DeleteScheduleGroup(ctx, "lagotto-teardown"); gone || err != nil {
		t.Errorf("absent group: gone %v, err %v", gone, err)
	}
	// The fixed teardown still keeps a non-empty group without an error.
	sc.groupExists, sc.listByGroup = true, map[string][][]string{"lagotto": {{"lagotto-capacity-poller"}}}
	if gone, err := d.deleteScheduleGroupIfEmpty(ctx); gone || err != nil {
		t.Errorf("deleteScheduleGroupIfEmpty on a non-empty group: %v, %v", gone, err)
	}
}

func TestDeleteScheduleForFootprint(t *testing.T) {
	ctx := context.Background()
	sc := &fakeScheduler{}
	d, _ := testDeployer(nil, nil, sc)
	for _, c := range [][2]string{{"lagotto", "someone-elses"}, {"other-group", "lagotto-launch-sl-1"}} {
		if _, err := d.DeleteSchedule(ctx, c[0], c[1]); err == nil {
			t.Errorf("deleted %s/%s", c[0], c[1])
		}
	}
	if sc.deleteCalls != 0 {
		t.Fatalf("%d DeleteSchedule calls for refused names", sc.deleteCalls)
	}
	if gone, err := d.DeleteSchedule(ctx, "default", "lagotto-launch-sl-1"); !gone || err != nil || sc.deletedRefs[0] != "default/lagotto-launch-sl-1" {
		t.Errorf("gone %v, err %v, deleted %v", gone, err, sc.deletedRefs)
	}
}

// TestPerResourceDeleters: each reports whether the resource was there, and a
// failed existence check deletes nothing.
func TestPerResourceDeleters(t *testing.T) {
	ctx := context.Background()
	denied := &smithyAccessDenied{}

	fl := &fakeLambda{getOut: &lambda.GetFunctionOutput{}}
	d, _ := testDeployer(fl, nil, nil)
	if gone, err := d.DeletePollerFunction(ctx); !gone || err != nil || fl.deleteCalls != 1 {
		t.Errorf("present function: %v, %v, %d deletes", gone, err, fl.deleteCalls)
	}
	fl = &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{}}
	d, _ = testDeployer(fl, nil, nil)
	if gone, err := d.DeletePollerFunction(ctx); gone || err != nil || fl.deleteCalls != 0 {
		t.Errorf("absent function: %v, %v, %d deletes", gone, err, fl.deleteCalls)
	}
	fl = &fakeLambda{getErr: denied}
	d, _ = testDeployer(fl, nil, nil)
	if _, err := d.DeletePollerFunction(ctx); err == nil || fl.deleteCalls != 0 {
		t.Errorf("unreadable function: %v, %d deletes", err, fl.deleteCalls)
	}

	fs := &fakeSNS{arn: AlertsTopicARN(testRegion, testAccount)}
	d, _ = testDeployer(nil, fs, nil)
	if gone, err := d.DeleteAlertsTopic(ctx, testRegion, testAccount); !gone || err != nil || fs.deleteCalls != 1 {
		t.Errorf("present topic: %v, %v, %d deletes", gone, err, fs.deleteCalls)
	}
	fs = &fakeSNS{absent: true}
	d, _ = testDeployer(nil, fs, nil)
	if gone, err := d.DeleteAlertsTopic(ctx, testRegion, testAccount); gone || err != nil || fs.deleteCalls != 0 {
		t.Errorf("absent topic: %v, %v, %d deletes", gone, err, fs.deleteCalls)
	}

	lg := &fakeLogs{exists: true}
	if gone, err := logsDeployer(lg).DeletePollerLogGroup(ctx, "eu-west-1"); !gone || err != nil {
		t.Errorf("present log group: %v, %v", gone, err)
	}

	sc := &fakeScheduler{deleteErr: denied}
	d, _ = testDeployer(nil, nil, sc)
	if _, err := d.DeleteSchedule(ctx, "lagotto", "lagotto-launch-sl-1"); err == nil {
		t.Error("a DeleteSchedule failure was swallowed")
	}
	sc = &fakeScheduler{deleteErr: &schedulertypes.ResourceNotFoundException{}}
	d, _ = testDeployer(nil, nil, sc)
	if gone, err := d.DeleteSchedule(ctx, "lagotto", "lagotto-launch-sl-1"); gone || err != nil {
		t.Errorf("absent schedule: %v, %v", gone, err)
	}
	sc = &fakeScheduler{groupExists: true, listErr: denied}
	d, _ = testDeployer(nil, nil, sc)
	if _, err := d.DeleteScheduleGroup(ctx, "lagotto"); err == nil || sc.groupDeletes != 0 {
		t.Errorf("unlistable group: %v, %d deletes", err, sc.groupDeletes)
	}
}
