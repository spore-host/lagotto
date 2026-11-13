package deploy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	logstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// fakeLogs models the one log group lagotto owns (#305) and records every call
// and the region it was made in.
type fakeLogs struct {
	exists    bool
	retention *int32
	tags      map[string]string

	createErr, tagErr, describeErr, retentionErr, deleteErr error

	calls   []string
	regions []string
	tagARNs []string
	log     *callLog
}

func (f *fakeLogs) record(op string, optFns []func(*cloudwatchlogs.Options)) {
	f.calls = append(f.calls, op)
	var o cloudwatchlogs.Options
	for _, fn := range optFns {
		fn(&o)
	}
	f.regions = append(f.regions, o.Region)
}

func (f *fakeLogs) CreateLogGroup(_ context.Context, in *cloudwatchlogs.CreateLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	f.record("CreateLogGroup", optFns)
	f.log.add("CreateLogGroup")
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.exists {
		return nil, &logstypes.ResourceAlreadyExistsException{Message: aws.String("The specified log group already exists")}
	}
	f.exists, f.tags, f.retention = true, map[string]string{}, nil
	for k, v := range in.Tags {
		f.tags[k] = v
	}
	return &cloudwatchlogs.CreateLogGroupOutput{}, nil
}

func (f *fakeLogs) DescribeLogGroups(_ context.Context, _ *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	f.record("DescribeLogGroups", optFns)
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	out := &cloudwatchlogs.DescribeLogGroupsOutput{LogGroups: []logstypes.LogGroup{
		// A prefix match that is NOT the poller's group, listed first.
		{LogGroupName: aws.String(PollerLogGroupName + "-other"), RetentionInDays: aws.Int32(1)},
	}}
	if f.exists {
		out.LogGroups = append(out.LogGroups, logstypes.LogGroup{LogGroupName: aws.String(PollerLogGroupName), RetentionInDays: f.retention})
	}
	return out, nil
}

func (f *fakeLogs) PutRetentionPolicy(_ context.Context, in *cloudwatchlogs.PutRetentionPolicyInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutRetentionPolicyOutput, error) {
	f.record("PutRetentionPolicy", optFns)
	if f.retentionErr != nil {
		return nil, f.retentionErr
	}
	f.retention = in.RetentionInDays
	return &cloudwatchlogs.PutRetentionPolicyOutput{}, nil
}

func (f *fakeLogs) TagResource(_ context.Context, in *cloudwatchlogs.TagResourceInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.TagResourceOutput, error) {
	f.record("TagResource", optFns)
	f.tagARNs = append(f.tagARNs, aws.ToString(in.ResourceArn))
	if f.tagErr != nil {
		return nil, f.tagErr
	}
	if f.tags == nil {
		f.tags = map[string]string{}
	}
	for k, v := range in.Tags {
		f.tags[k] = v
	}
	return &cloudwatchlogs.TagResourceOutput{}, nil
}

func (f *fakeLogs) DeleteLogGroup(_ context.Context, _ *cloudwatchlogs.DeleteLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DeleteLogGroupOutput, error) {
	f.record("DeleteLogGroup", optFns)
	f.log.add("DeleteLogGroup")
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	if !f.exists {
		return nil, &logstypes.ResourceNotFoundException{Message: aws.String("The specified log group does not exist.")}
	}
	f.exists = false
	return &cloudwatchlogs.DeleteLogGroupOutput{}, nil
}

func logsDeployer(f *fakeLogs) *Deployer { return &Deployer{logs: f} }

func TestPollerLogGroupNameIsLambdas(t *testing.T) {
	// Lambda writes to /aws/lambda/<function name>; any other name is a group
	// the poller never writes to, and the implicit one leaks again.
	if PollerLogGroupName != "/aws/lambda/lagotto-capacity-poller" {
		t.Errorf("PollerLogGroupName = %q", PollerLogGroupName)
	}
	if got, want := PollerLogGroupARN("us-west-2", "123456789012"),
		"arn:aws:logs:us-west-2:123456789012:log-group:/aws/lambda/lagotto-capacity-poller"; got != want {
		t.Errorf("PollerLogGroupARN = %q, want %q (TagResource takes the ARN without :*)", got, want)
	}
}

func TestEnsurePollerLogGroup_CreatesTaggedWithRetention(t *testing.T) {
	f := &fakeLogs{}
	action, err := logsDeployer(f).ensurePollerLogGroup(context.Background(), "us-west-2", "123456789012")
	if err != nil {
		t.Fatalf("ensurePollerLogGroup: %v", err)
	}
	if f.tags[ManagedTagKey] != ManagedTagValue {
		t.Errorf("created group tags = %v, want %s=%s on create", f.tags, ManagedTagKey, ManagedTagValue)
	}
	if f.retention == nil || *f.retention != PollerLogRetentionDays {
		t.Errorf("retention = %v, want %d", f.retention, PollerLogRetentionDays)
	}
	if want := []string{"CreateLogGroup", "PutRetentionPolicy"}; strings.Join(f.calls, ",") != strings.Join(want, ",") {
		t.Errorf("calls = %v, want %v", f.calls, want)
	}
	for i, r := range f.regions {
		if r != "us-west-2" {
			t.Errorf("%s ran in region %q, want the deploy's region us-west-2", f.calls[i], r)
		}
	}
	if !strings.HasPrefix(action, "created") {
		t.Errorf("action = %q, want created", action)
	}
}

// The pre-#305 shape: Lambda created the group implicitly, untagged and never
// expiring. Deploy adopts it.
func TestEnsurePollerLogGroup_AdoptsAnImplicitGroup(t *testing.T) {
	f := &fakeLogs{exists: true}
	action, err := logsDeployer(f).ensurePollerLogGroup(context.Background(), "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("ensurePollerLogGroup: %v", err)
	}
	if f.tags[ManagedTagKey] != ManagedTagValue {
		t.Errorf("adopted group tags = %v, want %s=%s back-filled", f.tags, ManagedTagKey, ManagedTagValue)
	}
	if len(f.tagARNs) != 1 || f.tagARNs[0] != PollerLogGroupARN("us-east-1", "123456789012") {
		t.Errorf("TagResource ARNs = %v", f.tagARNs)
	}
	if f.retention == nil || *f.retention != PollerLogRetentionDays {
		t.Errorf("retention = %v, want %d set on a group that had none", f.retention, PollerLogRetentionDays)
	}
	if !strings.Contains(action, "retention set") {
		t.Errorf("action = %q", action)
	}
}

// A retention someone chose is kept: shortening it would delete their logs.
func TestEnsurePollerLogGroup_KeepsAnExistingRetention(t *testing.T) {
	f := &fakeLogs{exists: true, retention: aws.Int32(365)}
	action, err := logsDeployer(f).ensurePollerLogGroup(context.Background(), "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("ensurePollerLogGroup: %v", err)
	}
	if *f.retention != 365 {
		t.Errorf("retention = %d, want the existing 365 kept", *f.retention)
	}
	for _, c := range f.calls {
		if c == "PutRetentionPolicy" {
			t.Errorf("PutRetentionPolicy called on a group that already had a retention: %v", f.calls)
		}
	}
	if !strings.Contains(action, "365-day retention kept") {
		t.Errorf("action = %q", action)
	}
}

func TestEnsurePollerLogGroup_FailuresAreErrors(t *testing.T) {
	denied := errors.New("AccessDeniedException: not authorized")
	for name, f := range map[string]*fakeLogs{
		"create":    {createErr: denied},
		"retention": {retentionErr: denied},
		"tag":       {exists: true, tagErr: denied},
		"describe":  {exists: true, describeErr: denied},
	} {
		if _, err := logsDeployer(f).ensurePollerLogGroup(context.Background(), "us-east-1", "123456789012"); err == nil {
			t.Errorf("%s failure was swallowed (calls %v)", name, f.calls)
		}
	}
}

func TestDeletePollerLogGroup(t *testing.T) {
	f := &fakeLogs{exists: true}
	d := logsDeployer(f)
	gone, err := d.deletePollerLogGroup(context.Background(), "eu-west-1")
	if err != nil || !gone {
		t.Fatalf("delete existing = (%v, %v), want (true, nil)", gone, err)
	}
	if f.regions[0] != "eu-west-1" {
		t.Errorf("DeleteLogGroup ran in %q, want the teardown's region", f.regions[0])
	}
	gone, err = d.deletePollerLogGroup(context.Background(), "eu-west-1")
	if err != nil || gone {
		t.Errorf("delete absent = (%v, %v), want (false, nil)", gone, err)
	}
	f.deleteErr = errors.New("AccessDeniedException: not authorized to perform logs:DeleteLogGroup")
	if _, err := d.deletePollerLogGroup(context.Background(), "eu-west-1"); err == nil {
		t.Error("an AccessDenied on DeleteLogGroup was read as an absent group")
	}
}

// The #305 leftover: an earlier teardown removed the function but not the
// group. A re-run deletes the group on its own.
func TestTeardown_DeletesALeftoverLogGroup(t *testing.T) {
	log := &callLog{}
	l := &fakeLambda{log: log, getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}}
	sc := &fakeScheduler{log: log, getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("Schedule not found")}}
	sn := &fakeSNS{log: log, getErr: &snstypes.NotFoundException{Message: aws.String("Topic not found")}}
	d, _ := testDeployer(l, sn, sc)
	lg := &fakeLogs{log: log, exists: true}
	d.logs = lg

	deleted, err := d.Teardown(context.Background(), "us-east-1", "123456789012")
	if err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != "log group "+PollerLogGroupName {
		t.Errorf("deleted = %v, want only the log group", deleted)
	}
	if lg.exists {
		t.Error("the log group survived the teardown")
	}
}

// A Deploy creates the group before the function, so Lambda never creates an
// unowned one on the first invocation.
func TestDeploy_CreatesTheLogGroupBeforeTheFunction(t *testing.T) {
	d, l, _, _ := hybridDeployer(t)
	log := &callLog{}
	l.log = log
	d.logs = &fakeLogs{log: log}

	res, err := d.Deploy(context.Background(), Options{Region: "us-east-1", AccountID: "123456789012", Version: "0.55.1"})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if strings.Join(log.calls, ",") != "CreateLogGroup,CreateFunction" {
		t.Errorf("create order = %v, want the log group first", log.calls)
	}
	found := false
	for _, a := range res.Actions {
		if strings.HasPrefix(a, "log group "+PollerLogGroupName+": created") {
			found = true
		}
	}
	if !found {
		t.Errorf("Actions = %v, want a log group line", res.Actions)
	}
}
