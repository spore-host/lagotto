package deploy

import (
	"context"
	"errors"
	"testing"
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
