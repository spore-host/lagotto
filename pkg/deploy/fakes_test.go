package deploy

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// In-package fakes that record every call, patterned on
// pkg/runtimeiam/runtimeiam_test.go's fakeIAM. They're in-package because
// lambdaAPI/snsAPI/schedulerAPI are unexported — which is deliberate: the seams
// are for tests, not for the package's public surface.

// callLog records an ORDERED trace across several fakes. Teardown's guarantee is
// about sequence (schedule → function → topic), and per-fake counters can't
// express a sequence that spans three services.
type callLog struct{ calls []string }

func (c *callLog) add(s string) {
	if c != nil {
		c.calls = append(c.calls, s)
	}
}

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
	log         *callLog
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
	f.log.add("DeleteFunction")
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
	log       *callLog
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
	f.log.add("DeleteTopic")
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
	log         *callLog
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
	f.log.add("DeleteSchedule")
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &scheduler.DeleteScheduleOutput{}, nil
}

// fakeCFN is a tiny CloudFormation state machine: one stack, whose status and
// stored template body move as UpdateStack/DeleteStack are called. That is enough
// for MigrateFromStack, which is a sequence of five calls whose ORDER and
// PRECONDITIONS are the thing worth testing — in particular that DeleteStack is
// never reached when the read-back template doesn't show the retain policies.
type fakeCFN struct {
	stackName string
	status    cfntypes.StackStatus
	params    []cfntypes.Parameter
	outputs   []cfntypes.Output
	// template is what GetTemplate returns; UpdateStack replaces it, exactly as
	// CloudFormation would.
	template string
	// missing makes DescribeStacks answer the way CloudFormation does for an absent
	// stack — a ValidationError whose message contains "does not exist".
	missing bool

	describeErr    error
	updateErr      error
	getTemplateErr error
	deleteErr      error

	describeCalls    int
	createCalls      []*cloudformation.CreateStackInput
	updateCalls      []*cloudformation.UpdateStackInput
	getTemplateCalls []*cloudformation.GetTemplateInput
	deleteCalls      []*cloudformation.DeleteStackInput
}

func (f *fakeCFN) DescribeStacks(_ context.Context, _ *cloudformation.DescribeStacksInput, _ ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error) {
	f.describeCalls++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.missing {
		return nil, fmt.Errorf("ValidationError: Stack with id %s does not exist", f.stackName)
	}
	return &cloudformation.DescribeStacksOutput{Stacks: []cfntypes.Stack{{
		StackName:   strptr(f.stackName),
		StackStatus: f.status,
		Parameters:  f.params,
		Outputs:     f.outputs,
	}}}, nil
}

func (f *fakeCFN) GetTemplate(_ context.Context, in *cloudformation.GetTemplateInput, _ ...func(*cloudformation.Options)) (*cloudformation.GetTemplateOutput, error) {
	f.getTemplateCalls = append(f.getTemplateCalls, in)
	if f.getTemplateErr != nil {
		return nil, f.getTemplateErr
	}
	return &cloudformation.GetTemplateOutput{TemplateBody: strptr(f.template)}, nil
}

func (f *fakeCFN) CreateStack(_ context.Context, in *cloudformation.CreateStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.CreateStackOutput, error) {
	f.createCalls = append(f.createCalls, in)
	f.missing = false
	f.status = cfntypes.StackStatusCreateComplete
	f.template = deref(in.TemplateBody)
	return &cloudformation.CreateStackOutput{}, nil
}

func (f *fakeCFN) UpdateStack(_ context.Context, in *cloudformation.UpdateStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.UpdateStackOutput, error) {
	f.updateCalls = append(f.updateCalls, in)
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	f.status = cfntypes.StackStatusUpdateComplete
	f.template = deref(in.TemplateBody)
	return &cloudformation.UpdateStackOutput{}, nil
}

func (f *fakeCFN) DeleteStack(_ context.Context, in *cloudformation.DeleteStackInput, _ ...func(*cloudformation.Options)) (*cloudformation.DeleteStackOutput, error) {
	f.deleteCalls = append(f.deleteCalls, in)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	// The SDK's delete waiter matches on DELETE_COMPLETE.
	f.status = cfntypes.StackStatusDeleteComplete
	return &cloudformation.DeleteStackOutput{}, nil
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

// testMigrator builds a Deployer wired to all four fakes, for the CloudFormation
// migration path.
func testMigrator(c cfnAPI, l lambdaAPI, sn snsAPI, sc schedulerAPI) *Deployer {
	sl := &fakeSleeper{}
	return &Deployer{cfn: c, lambda: l, sns: sn, sched: sc, sleep: sl.sleep}
}
