package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/doctor"
)

type fakeScheduleLister struct {
	names []string
	err   error
}

func (f fakeScheduleLister) ListLaunchSchedules(context.Context) ([]string, error) {
	return f.names, f.err
}

func TestAppendLaunchScheduleCheck(t *testing.T) {
	ctx := context.Background()
	mustNotRead := func(context.Context) ([]string, error) {
		t.Error("records read with no schedule to match")
		return nil, nil
	}

	r := &doctor.Report{}
	appendLaunchScheduleCheck(ctx, r, "us-west-2", fakeScheduleLister{}, mustNotRead)
	if len(r.Checks) != 0 {
		t.Errorf("no schedules added %+v", r.Checks)
	}

	r = &doctor.Report{}
	appendLaunchScheduleCheck(ctx, r, "us-west-2", fakeScheduleLister{err: errors.New("denied")}, mustNotRead)
	if len(r.Checks) != 1 || r.Checks[0].Status != doctor.StatusWarn {
		t.Errorf("list failure: %+v", r.Checks)
	}

	r = &doctor.Report{}
	appendLaunchScheduleCheck(ctx, r, "us-west-2", fakeScheduleLister{names: []string{"lagotto-launch-sl-orph"}},
		func(context.Context) ([]string, error) { return nil, nil })
	if len(r.Checks) != 1 || r.Checks[0].Name != doctor.CheckLaunchSchedules || r.Checks[0].Status != doctor.StatusWarn || r.Failed() {
		t.Errorf("orphan: %+v", r.Checks)
	}
}
