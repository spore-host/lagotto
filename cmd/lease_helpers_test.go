package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"
)

// installFakeLeases answers every command's lifecycle client from table, an
// in-memory spore-host-lifecycle table shared by every region, with opts
// applied on top of a no-wait clock (lagotto#326).
func installFakeLeases(t *testing.T, table *lifecycletest.Fake, opts ...func(*lifecycle.Options)) {
	t.Helper()
	prev := newLifecycleClient
	newLifecycleClient = func(_ aws.Config, owner string) (*lifecycle.Client, error) {
		o := lifecycle.Options{
			DynamoDB:      func(string) (lifecycle.API, error) { return table, nil },
			ControlRegion: "us-east-1",
			Owner:         owner,
			Sleep:         func(context.Context, time.Duration) error { return nil },
		}
		for _, f := range opts {
			f(&o)
		}
		return lifecycle.New(o)
	}
	t.Cleanup(func() { newLifecycleClient = prev })
	prevJobs := newTeardownJobAPI
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return &fakeTeardownJobs{schedule: true} }
	t.Cleanup(func() { newTeardownJobAPI = prevJobs })
}

// fakeTeardownJobs is a region's Scheduler for the creators' job cancel:
// schedule says whether the poller's rate schedule is there, jobs are the
// pending teardown jobs (nil: no group), err fails every call.
type fakeTeardownJobs struct {
	schedule bool
	jobs     []string
	deleted  []string
	err      error
	pages    int
}

func (f *fakeTeardownJobs) GetSchedule(_ context.Context, in *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.schedule && aws.ToString(in.GroupName) == "lagotto" {
		return &scheduler.GetScheduleOutput{}, nil
	}
	return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no schedule")}
}

func (f *fakeTeardownJobs) ListSchedules(_ context.Context, in *scheduler.ListSchedulesInput, _ ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.jobs == nil {
		return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no group")}
	}
	f.pages++
	out := &scheduler.ListSchedulesOutput{}
	if in.NextToken == nil && len(f.jobs) > 1 {
		out.Schedules = []schedulertypes.ScheduleSummary{{Name: aws.String(f.jobs[0])}}
		out.NextToken = aws.String("more")
		return out, nil
	}
	start := 0
	if in.NextToken != nil {
		start = 1
	}
	for _, j := range f.jobs[start:] {
		out.Schedules = append(out.Schedules, schedulertypes.ScheduleSummary{Name: aws.String(j)})
	}
	return out, nil
}

func (f *fakeTeardownJobs) DeleteSchedule(_ context.Context, in *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.deleted = append(f.deleted, aws.ToString(in.Name))
	return &scheduler.DeleteScheduleOutput{}, nil
}
