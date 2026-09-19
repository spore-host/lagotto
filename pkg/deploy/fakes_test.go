package deploy

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// In-package fakes that record every call, patterned on
// pkg/runtimeiam/runtimeiam_test.go's fakeIAM. They're in-package because
// lambdaAPI/snsAPI/schedulerAPI are unexported — which is deliberate: the seams
// are for tests, not for the package's public surface.

// fakeLambda records Lambda calls and answers from scripted state.
type fakeLambda struct {
	// getOut is returned by GetFunction; getErr takes precedence when non-nil.
	getOut *lambda.GetFunctionOutput
	getErr error
	// getSeq, when non-empty, is consumed one entry per GetFunction call (the
	// last entry repeats), so a test can script a function that settles.
	getSeq []getResult

	// createErrs is consumed one per CreateFunction call; a nil entry (or
	// running off the end) means success.
	createErrs []error
	// codeErrs / configErrs work the same way for the two update calls.
	codeErrs   []error
	configErrs []error

	createCalls []*lambda.CreateFunctionInput
	codeCalls   []*lambda.UpdateFunctionCodeInput
	configCalls []*lambda.UpdateFunctionConfigurationInput
	tagCalls    []*lambda.TagResourceInput
	deleteCalls int
	getCalls    int
	deleteErr   error
}

type getResult struct {
	out *lambda.GetFunctionOutput
	err error
}

func (f *fakeLambda) GetFunction(_ context.Context, _ *lambda.GetFunctionInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	f.getCalls++
	if len(f.getSeq) > 0 {
		i := f.getCalls - 1
		if i >= len(f.getSeq) {
			i = len(f.getSeq) - 1
		}
		return f.getSeq[i].out, f.getSeq[i].err
	}
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getOut, nil
}

func (f *fakeLambda) CreateFunction(_ context.Context, in *lambda.CreateFunctionInput, _ ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error) {
	f.createCalls = append(f.createCalls, in)
	if err := nextErr(f.createErrs, len(f.createCalls)-1); err != nil {
		return nil, err
	}
	// A successful create means a subsequent GetFunction (the active waiter) must
	// see an Active function.
	f.getOut = &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
		FunctionArn: strptr(PollerFunctionARN("us-east-1", "123456789012")),
		State:       lambdatypes.StateActive,
	}}
	f.getSeq = nil
	f.getErr = nil
	return &lambda.CreateFunctionOutput{FunctionArn: f.getOut.Configuration.FunctionArn}, nil
}

func (f *fakeLambda) UpdateFunctionCode(_ context.Context, in *lambda.UpdateFunctionCodeInput, _ ...func(*lambda.Options)) (*lambda.UpdateFunctionCodeOutput, error) {
	f.codeCalls = append(f.codeCalls, in)
	if err := nextErr(f.codeErrs, len(f.codeCalls)-1); err != nil {
		return nil, err
	}
	return &lambda.UpdateFunctionCodeOutput{}, nil
}

func (f *fakeLambda) UpdateFunctionConfiguration(_ context.Context, in *lambda.UpdateFunctionConfigurationInput, _ ...func(*lambda.Options)) (*lambda.UpdateFunctionConfigurationOutput, error) {
	f.configCalls = append(f.configCalls, in)
	if err := nextErr(f.configErrs, len(f.configCalls)-1); err != nil {
		return nil, err
	}
	return &lambda.UpdateFunctionConfigurationOutput{}, nil
}

func (f *fakeLambda) DeleteFunction(_ context.Context, _ *lambda.DeleteFunctionInput, _ ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error) {
	f.deleteCalls++
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &lambda.DeleteFunctionOutput{}, nil
}

func (f *fakeLambda) TagResource(_ context.Context, in *lambda.TagResourceInput, _ ...func(*lambda.Options)) (*lambda.TagResourceOutput, error) {
	f.tagCalls = append(f.tagCalls, in)
	return &lambda.TagResourceOutput{}, nil
}

// fakeSNS records SNS calls over a single in-memory topic.
type fakeSNS struct {
	arn        string
	attributes map[string]string

	createCalls []*sns.CreateTopicInput
	setCalls    []*sns.SetTopicAttributesInput
	tagCalls    []*sns.TagResourceInput
	deleteCalls int
	getCalls    int

	createErr error
	getErr    error
	deleteErr error
}

func (f *fakeSNS) CreateTopic(_ context.Context, in *sns.CreateTopicInput, _ ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	f.createCalls = append(f.createCalls, in)
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &sns.CreateTopicOutput{TopicArn: strptr(f.arn)}, nil
}

func (f *fakeSNS) GetTopicAttributes(_ context.Context, _ *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	attrs := map[string]string{}
	for k, v := range f.attributes {
		attrs[k] = v
	}
	return &sns.GetTopicAttributesOutput{Attributes: attrs}, nil
}

func (f *fakeSNS) SetTopicAttributes(_ context.Context, in *sns.SetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	f.setCalls = append(f.setCalls, in)
	if f.attributes == nil {
		f.attributes = map[string]string{}
	}
	f.attributes[deref(in.AttributeName)] = deref(in.AttributeValue)
	return &sns.SetTopicAttributesOutput{}, nil
}

func (f *fakeSNS) TagResource(_ context.Context, in *sns.TagResourceInput, _ ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	f.tagCalls = append(f.tagCalls, in)
	return &sns.TagResourceOutput{}, nil
}

func (f *fakeSNS) DeleteTopic(_ context.Context, _ *sns.DeleteTopicInput, _ ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	f.deleteCalls++
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &sns.DeleteTopicOutput{}, nil
}

// fakeScheduler records Scheduler calls.
type fakeScheduler struct {
	getOut *scheduler.GetScheduleOutput
	getErr error

	createErr error

	createCalls []*scheduler.CreateScheduleInput
	updateCalls []*scheduler.UpdateScheduleInput
	deleteCalls int
	getCalls    int
	deleteErr   error
}

func (f *fakeScheduler) GetSchedule(_ context.Context, _ *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getOut, nil
}

func (f *fakeScheduler) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	f.createCalls = append(f.createCalls, in)
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &scheduler.CreateScheduleOutput{}, nil
}

func (f *fakeScheduler) UpdateSchedule(_ context.Context, in *scheduler.UpdateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error) {
	f.updateCalls = append(f.updateCalls, in)
	return &scheduler.UpdateScheduleOutput{}, nil
}

func (f *fakeScheduler) DeleteSchedule(_ context.Context, _ *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	f.deleteCalls++
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &scheduler.DeleteScheduleOutput{}, nil
}

// --- helpers ----------------------------------------------------------------

func strptr(s string) *string { return &s }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nextErr(errs []error, i int) error {
	if i < len(errs) {
		return errs[i]
	}
	return nil
}

// fakeSleeper records how long the code under test asked to sleep, and never
// actually sleeps — so retry/poll loops run instantly.
type fakeSleeper struct {
	calls []time.Duration
}

func (s *fakeSleeper) sleep(d time.Duration) { s.calls = append(s.calls, d) }

// testDeployer builds a Deployer wired to the given fakes with an instant sleep.
func testDeployer(l lambdaAPI, sn snsAPI, sc schedulerAPI) (*Deployer, *fakeSleeper) {
	sl := &fakeSleeper{}
	return &Deployer{lambda: l, sns: sn, sched: sc, sleep: sl.sleep}, sl
}
