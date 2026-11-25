package inventory

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	logstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

var errDenied = &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "denied"}

// failingDynamo fails one operation, and pages every list in two.
type failingDynamo struct {
	failOn  string
	missing bool // every table is absent
	calls   map[string]int
}

func (f *failingDynamo) hit(op string) error {
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[op]++
	if f.missing {
		return notFound()
	}
	if f.failOn == op {
		return errDenied
	}
	return nil
}

var nextKey = map[string]ddbtypes.AttributeValue{"k": &ddbtypes.AttributeValueMemberS{Value: "1"}}

func (f *failingDynamo) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if err := f.hit("DescribeTable"); err != nil {
		return nil, err
	}
	return &dynamodb.DescribeTableOutput{Table: &ddbtypes.TableDescription{}}, nil
}

func (f *failingDynamo) ListTagsOfResource(_ context.Context, in *dynamodb.ListTagsOfResourceInput, _ ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	if err := f.hit("ListTagsOfResource"); err != nil {
		return nil, err
	}
	if in.NextToken == nil {
		return &dynamodb.ListTagsOfResourceOutput{Tags: []ddbtypes.Tag{{Key: aws.String("a"), Value: aws.String("1")}}, NextToken: aws.String("t2")}, nil
	}
	return &dynamodb.ListTagsOfResourceOutput{Tags: []ddbtypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}, nil
}

func (f *failingDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if err := f.hit("Scan"); err != nil {
		return nil, err
	}
	if in.Select == ddbtypes.SelectCount {
		return &dynamodb.ScanOutput{Count: 1}, nil
	}
	item := map[string]ddbtypes.AttributeValue{
		"schedule_id":   &ddbtypes.AttributeValueMemberS{Value: "sl-1"},
		"status":        &ddbtypes.AttributeValueMemberS{Value: "pending"},
		"schedule_name": &ddbtypes.AttributeValueMemberS{Value: "lagotto-launch-sl-1"},
	}
	if in.ExclusiveStartKey == nil {
		return &dynamodb.ScanOutput{Items: []map[string]ddbtypes.AttributeValue{item}, LastEvaluatedKey: nextKey}, nil
	}
	return &dynamodb.ScanOutput{Items: []map[string]ddbtypes.AttributeValue{item}}, nil
}

func (f *failingDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if err := f.hit("Query"); err != nil {
		return nil, err
	}
	if in.ExclusiveStartKey == nil {
		return &dynamodb.QueryOutput{Count: 2, LastEvaluatedKey: nextKey}, nil
	}
	return &dynamodb.QueryOutput{Count: 1}, nil
}

// TestAWSTablesPaths: tags follow every page; any read that fails names the
// table and stops; absent tables are skipped, not errors.
func TestAWSTablesPaths(t *testing.T) {
	ctx := context.Background()
	ok := &failingDynamo{}
	got, err := (&awsRegion{region: "us-east-1", accountID: acct, ddb: ok}).Tables(ctx, Options{})
	if err != nil || len(got) != 3 {
		t.Fatalf("tables %+v, %v", got, err)
	}
	// The DescribeTable answer had no ARN, so the table's is derived.
	if got[0].ARN != TableARN("us-east-1", acct, "lagotto-watches") || got[0].Tags["a"] != "1" || !tags.IsManagedBy(got[0].Tags, Tool) {
		t.Errorf("table %+v: want the derived ARN and both tag pages", got[0])
	}
	for _, op := range []string{"DescribeTable", "ListTagsOfResource", "Scan"} {
		f := &failingDynamo{failOn: op}
		if _, err := (&awsRegion{ddb: f}).Tables(ctx, Options{}); err == nil || !strings.Contains(err.Error(), "lagotto-watches") {
			t.Errorf("%s failure: %v, want an error naming the table", op, err)
		}
	}
	if got, err := (&awsRegion{ddb: &failingDynamo{missing: true}}).Tables(ctx, Options{}); err != nil || len(got) != 0 {
		t.Errorf("no tables: %+v, %v", got, err)
	}
}

// TestAWSActivityPaths: the active-watch count and the launch scan follow
// every page; a missing table is no activity; a failed read is an error.
func TestAWSActivityPaths(t *testing.T) {
	ctx := context.Background()
	act, err := (&awsRegion{ddb: &failingDynamo{}}).Activity(ctx, "w", "s")
	if err != nil || act.ActiveWatches != 3 || act.ArmedLaunches != 2 || len(act.Recorded) != 2 {
		t.Errorf("activity %+v, %v: want 3 watches and 2 armed launches over two pages", act, err)
	}
	if act, err := (&awsRegion{ddb: &failingDynamo{missing: true}}).Activity(ctx, "w", "s"); err != nil || act.ActiveWatches+act.ArmedLaunches != 0 {
		t.Errorf("missing tables: %+v, %v", act, err)
	}
	for _, op := range []string{"Query", "Scan"} {
		if _, err := (&awsRegion{ddb: &failingDynamo{failOn: op}}).Activity(ctx, "w", "s"); err == nil {
			t.Errorf("a failed %s read as no activity", op)
		}
	}
	f := &failingDynamo{}
	if _, err := (&awsRegion{ddb: f}).Activity(ctx, "", ""); err != nil || len(f.calls) != 0 {
		t.Errorf("no tables to read made calls %v (%v)", f.calls, err)
	}
}

type fakePollerReader struct {
	info    deploy.PollerInfo
	err     error
	tags    map[string]string
	regions []string
}

func (f fakePollerReader) InspectPoller(context.Context) (deploy.PollerInfo, error) {
	return f.info, f.err
}
func (f fakePollerReader) ArtifactBucketTags(context.Context, string, string) (map[string]string, error) {
	return f.tags, f.err
}
func (f fakePollerReader) ArtifactBucketRegions(context.Context, string) ([]string, error) {
	return f.regions, f.err
}

func TestAWSPoller(t *testing.T) {
	ctx := context.Background()
	r := &awsRegion{region: "us-east-1", accountID: acct, poller: fakePollerReader{}}
	if it, _, err := r.Poller(ctx); it != nil || err != nil {
		t.Errorf("no poller: %+v, %v", it, err)
	}
	r.poller = fakePollerReader{info: deploy.PollerInfo{Exists: true, RoleARN: "arn:role", Tags: map[string]string{"x": "y"}}}
	it, role, err := r.Poller(ctx)
	if err != nil || it.ARN != deploy.PollerFunctionARN("us-east-1", acct) || role != "arn:role" || it.Tags["x"] != "y" {
		t.Errorf("poller %+v, %q, %v", it, role, err)
	}
	r.poller = fakePollerReader{err: errDenied}
	if _, _, err := r.Poller(ctx); err == nil {
		t.Error("an unreadable poller read as absent")
	}
	r.poller = fakePollerReader{tags: map[string]string{"lagotto:managed": "cli"}}
	if tg, err := r.BucketTags(ctx, "us-east-1", acct); err != nil || tg["lagotto:managed"] != "cli" {
		t.Errorf("bucket tags %v, %v", tg, err)
	}
}

type fakeSNSReader struct{ getErr, tagsErr error }

func (f fakeSNSReader) GetTopicAttributes(context.Context, *sns.GetTopicAttributesInput, ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	return &sns.GetTopicAttributesOutput{}, f.getErr
}
func (f fakeSNSReader) ListTagsForResource(context.Context, *sns.ListTagsForResourceInput, ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error) {
	return &sns.ListTagsForResourceOutput{Tags: []snstypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}, f.tagsErr
}

func TestAWSTopic(t *testing.T) {
	ctx := context.Background()
	r := &awsRegion{region: "us-east-1", accountID: acct, sns: fakeSNSReader{}}
	if it, err := r.Topic(ctx); err != nil || it.ARN != deploy.AlertsTopicARN("us-east-1", acct) || !tags.IsManagedBy(it.Tags, Tool) {
		t.Errorf("topic %+v, %v", it, err)
	}
	r.sns = fakeSNSReader{getErr: &snstypes.NotFoundException{Message: aws.String("no")}}
	if it, err := r.Topic(ctx); it != nil || err != nil {
		t.Errorf("absent topic: %+v, %v", it, err)
	}
	for _, f := range []fakeSNSReader{{getErr: errDenied}, {tagsErr: errDenied}} {
		r.sns = f
		if _, err := r.Topic(ctx); err == nil {
			t.Errorf("%+v: failure swallowed", f)
		}
	}
}

type fakeLogsReader struct {
	names            []string
	descErr, tagsErr error
}

func (f fakeLogsReader) DescribeLogGroups(context.Context, *cloudwatchlogs.DescribeLogGroupsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error) {
	var out []logstypes.LogGroup
	for _, n := range f.names {
		out = append(out, logstypes.LogGroup{LogGroupName: aws.String(n)})
	}
	return &cloudwatchlogs.DescribeLogGroupsOutput{LogGroups: out}, f.descErr
}
func (f fakeLogsReader) ListTagsForResource(context.Context, *cloudwatchlogs.ListTagsForResourceInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.ListTagsForResourceOutput, error) {
	return &cloudwatchlogs.ListTagsForResourceOutput{Tags: map[string]string{tags.ManagedBy: "lagotto"}}, f.tagsErr
}

func TestAWSLogGroup(t *testing.T) {
	ctx := context.Background()
	r := &awsRegion{region: "us-east-1", accountID: acct}
	// A prefix match on a longer name is not the poller's group.
	r.logs = fakeLogsReader{names: []string{deploy.PollerLogGroupName + "-other"}}
	if it, err := r.LogGroup(ctx); it != nil || err != nil {
		t.Errorf("prefix-only match: %+v, %v", it, err)
	}
	r.logs = fakeLogsReader{names: []string{deploy.PollerLogGroupName}}
	if it, err := r.LogGroup(ctx); err != nil || it.ARN != deploy.PollerLogGroupARN("us-east-1", acct) || !tags.IsManagedBy(it.Tags, Tool) {
		t.Errorf("log group %+v, %v", it, err)
	}
	for _, f := range []fakeLogsReader{{descErr: errDenied}, {names: []string{deploy.PollerLogGroupName}, tagsErr: errDenied}} {
		r.logs = f
		if _, err := r.LogGroup(ctx); err == nil {
			t.Errorf("%+v: failure swallowed", f)
		}
	}
}

type fakeSageMaker struct {
	err   error
	pages int
}

func (f *fakeSageMaker) Search(_ context.Context, in *sagemaker.SearchInput, _ ...func(*sagemaker.Options)) (*sagemaker.SearchOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.pages++
	job := func(n string) smtypes.SearchRecord {
		return smtypes.SearchRecord{TrainingJob: &smtypes.TrainingJob{TrainingJobName: aws.String(n), TrainingJobArn: aws.String("arn:" + n),
			Tags: []smtypes.Tag{{Key: aws.String("lagotto:managed"), Value: aws.String("true")}}}}
	}
	if in.NextToken == nil {
		return &sagemaker.SearchOutput{Results: []smtypes.SearchRecord{job("j1"), {}}, NextToken: aws.String("p2")}, nil
	}
	return &sagemaker.SearchOutput{Results: []smtypes.SearchRecord{job("j2")}}, nil
}

func TestAWSJobs(t *testing.T) {
	sm := &fakeSageMaker{}
	jobs, err := (&awsRegion{sm: sm}).Jobs(context.Background())
	if err != nil || len(jobs) != 2 || jobs[1].Name != "j2" || jobs[0].Tags["lagotto:managed"] != "true" || sm.pages != 2 {
		t.Errorf("jobs %+v, %v, %d pages", jobs, err, sm.pages)
	}
	if _, err := (&awsRegion{sm: &fakeSageMaker{err: errDenied}}).Jobs(context.Background()); err == nil {
		t.Error("a failed Search read as no jobs")
	}
}

type pagedEC2 struct{ err error }

func (pagedEC2) DescribeRegions(context.Context, *ec2.DescribeRegionsInput, ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error) {
	return &ec2.DescribeRegionsOutput{}, nil
}
func (p pagedEC2) DescribeCapacityReservations(_ context.Context, in *ec2.DescribeCapacityReservationsInput, _ ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error) {
	if p.err != nil {
		return nil, p.err
	}
	cr := ec2types.CapacityReservation{CapacityReservationId: aws.String("cr-1"), Tags: []ec2types.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}
	if in.NextToken == nil {
		return &ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{cr}, NextToken: aws.String("p2")}, nil
	}
	cr.CapacityReservationId = aws.String("cr-2")
	return &ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{cr}}, nil
}

func TestAWSHoldsPagesAndRegionErrors(t *testing.T) {
	ctx := context.Background()
	holds, err := (&awsRegion{ec2: pagedEC2{}}).Holds(ctx)
	if err != nil || len(holds) != 2 {
		t.Errorf("holds over two pages: %+v, %v", holds, err)
	}
	if _, err := (&awsRegion{ec2: pagedEC2{err: errDenied}}).Holds(ctx); err == nil {
		t.Error("a failed DescribeCapacityReservations read as no holds")
	}
	if _, err := (&AWSSource{ec2: pagedEC2{}}).EnabledRegions(ctx); err == nil {
		t.Error("an empty region list accepted")
	}
}

type fakeStacks struct{ stacks []cfntypes.Stack }

func (f fakeStacks) DescribeStacks(context.Context, *cloudformation.DescribeStacksInput, ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error) {
	return &cloudformation.DescribeStacksOutput{Stacks: f.stacks}, nil
}

func TestAWSLegacyStackPresent(t *testing.T) {
	r := &awsRegion{cfn: fakeStacks{stacks: []cfntypes.Stack{
		{StackName: aws.String("lagotto"), StackStatus: cfntypes.StackStatusDeleteComplete},
		{StackName: aws.String("lagotto"), StackId: aws.String("arn:stack/lagotto/1"), StackStatus: cfntypes.StackStatusUpdateComplete,
			Tags: []cfntypes.Tag{{Key: aws.String("Application"), Value: aws.String("lagotto")}}},
	}}}
	st, err := r.LegacyStack(context.Background(), "lagotto")
	if err != nil || st == nil || st.ARN != "arn:stack/lagotto/1" || st.Tags["Application"] != "lagotto" {
		t.Errorf("stack %+v, %v", st, err)
	}
	r.cfn = fakeStacks{stacks: []cfntypes.Stack{{StackStatus: cfntypes.StackStatusDeleteComplete}}}
	if st, err := r.LegacyStack(context.Background(), "lagotto"); st != nil || err != nil {
		t.Errorf("deleted stack: %+v, %v", st, err)
	}
}

type failingScheduler struct {
	fakeScheduler
	failOn string
}

func (f failingScheduler) GetScheduleGroup(ctx context.Context, in *scheduler.GetScheduleGroupInput, o ...func(*scheduler.Options)) (*scheduler.GetScheduleGroupOutput, error) {
	if f.failOn == "GetScheduleGroup" {
		return nil, errDenied
	}
	return f.fakeScheduler.GetScheduleGroup(ctx, in, o...)
}
func (f failingScheduler) ListTagsForResource(ctx context.Context, in *scheduler.ListTagsForResourceInput, o ...func(*scheduler.Options)) (*scheduler.ListTagsForResourceOutput, error) {
	if f.failOn == "ListTagsForResource" {
		return nil, errDenied
	}
	return f.fakeScheduler.ListTagsForResource(ctx, in, o...)
}
func (f failingScheduler) ListSchedules(ctx context.Context, in *scheduler.ListSchedulesInput, o ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	if f.failOn == "ListSchedules "+aws.ToString(in.GroupName) {
		return nil, errDenied
	}
	return f.fakeScheduler.ListSchedules(ctx, in, o...)
}

// TestAWSSchedulerErrors: a group or schedule list that cannot be read is an
// error, never "none".
func TestAWSSchedulerErrors(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"GetScheduleGroup", "ListTagsForResource"} {
		if _, err := (&awsRegion{sched: failingScheduler{failOn: op}}).ScheduleGroups(ctx); err == nil {
			t.Errorf("%s failure swallowed", op)
		}
	}
	for _, g := range []string{"lagotto", "default"} {
		if _, err := (&awsRegion{sched: failingScheduler{failOn: "ListSchedules " + g}}).Schedules(ctx); err == nil {
			t.Errorf("ListSchedules failure in %s swallowed", g)
		}
	}
}

// accountIAM answers GetRolePolicy for AuthorizedRoles and fails on demand.
type accountIAM struct {
	fakeIAM
	policy  string
	failOn  string
	noRoles bool
}

func (a accountIAM) GetRole(ctx context.Context, in *iam.GetRoleInput, o ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if a.failOn == "GetRole" {
		return nil, errDenied
	}
	if a.noRoles {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{Arn: aws.String(IAMARN(acct, "role", aws.ToString(in.RoleName)))}}, nil
}

func (a accountIAM) GetRolePolicy(context.Context, *iam.GetRolePolicyInput, ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	switch {
	case a.failOn == "GetRolePolicy":
		return nil, errDenied
	case a.policy == "":
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(a.policy)}, nil
}

func (a accountIAM) GetPolicy(ctx context.Context, in *iam.GetPolicyInput, o ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	switch a.failOn {
	case "GetPolicy":
		return nil, errDenied
	case "NoPolicy":
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return a.fakeIAM.GetPolicy(ctx, in, o...)
}

func (a accountIAM) ListInstanceProfilesForRole(ctx context.Context, in *iam.ListInstanceProfilesForRoleInput, o ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error) {
	switch a.failOn {
	case "ListInstanceProfilesForRole":
		return nil, errDenied
	case "NoRole":
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return a.fakeIAM.ListInstanceProfilesForRole(ctx, in, o...)
}

func (a accountIAM) ListInstanceProfiles(ctx context.Context, in *iam.ListInstanceProfilesInput, o ...func(*iam.Options)) (*iam.ListInstanceProfilesOutput, error) {
	if a.failOn == "ListInstanceProfiles" {
		return nil, errDenied
	}
	return a.fakeIAM.ListInstanceProfiles(ctx, in, o...)
}

func (a accountIAM) GetInstanceProfile(ctx context.Context, in *iam.GetInstanceProfileInput, o ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error) {
	if a.failOn == "GetInstanceProfile" {
		return nil, errDenied
	}
	if a.failOn == "NoProfile" {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return a.fakeIAM.GetInstanceProfile(ctx, in, o...)
}

func TestAWSAccountPaths(t *testing.T) {
	ctx := context.Background()
	doc, err := runtimeiam.PolicyDocument(acct, []string{"team-gpu"})
	if err != nil {
		t.Fatal(err)
	}
	a := &awsAccount{iam: accountIAM{policy: doc}, accountID: acct}
	roles, err := a.AuthorizedRoles(ctx)
	if err != nil || !slices.Contains(roles, "team-gpu") {
		t.Errorf("authorized %v, %v", roles, err)
	}
	// No runtime policy and no poller role: nothing authorized.
	a.iam = accountIAM{noRoles: true}
	if roles, err := a.AuthorizedRoles(ctx); err != nil || len(roles) != 0 {
		t.Errorf("no policy: %v, %v", roles, err)
	}
	a.iam = accountIAM{failOn: "GetRolePolicy"}
	if _, err := a.AuthorizedRoles(ctx); err == nil {
		t.Error("an unreadable runtime policy read as no authorized roles")
	}
	a.iam = accountIAM{failOn: "NoPolicy"}
	if p, _, err := a.CLIPolicy(ctx); p != nil || err != nil {
		t.Errorf("absent policy: %+v, %v", p, err)
	}
	a.iam = accountIAM{failOn: "NoRole"}
	if ps, err := a.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName); ps != nil || err != nil {
		t.Errorf("absent role's profiles: %+v, %v", ps, err)
	}
	a.iam = accountIAM{failOn: "NoProfile"}
	if ps, err := a.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName); len(ps) != 0 || err != nil {
		t.Errorf("profile gone between list and get: %+v, %v", ps, err)
	}
	for _, op := range []string{"GetRole", "GetPolicy", "ListInstanceProfilesForRole", "GetInstanceProfile"} {
		a.iam = accountIAM{failOn: op}
		var err error
		switch op {
		case "GetRole":
			_, err = a.Role(ctx, "x")
		case "GetPolicy":
			_, _, err = a.CLIPolicy(ctx)
		default:
			_, err = a.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName)
		}
		if err == nil {
			t.Errorf("%s failure swallowed", op)
		}
	}
}

// TestNewAWSSource builds a client per region and reads the artifact buckets
// through deploy's lister, without a call.
func TestNewAWSSource(t *testing.T) {
	s := NewAWSSource(aws.Config{Region: "us-east-1"}, acct)
	r, ok := s.Region("eu-west-1").(*awsRegion)
	if !ok || r.region != "eu-west-1" || r.accountID != acct || r.ddb == nil || r.poller == nil {
		t.Errorf("region source %+v", r)
	}
	if a, ok := s.Account().(*awsAccount); !ok || a.accountID != acct {
		t.Errorf("account source %+v", s.Account())
	}
	s.buckets = fakePollerReader{regions: []string{"us-east-1"}}
	if got, err := s.ArtifactBucketRegions(context.Background(), acct); err != nil || len(got) != 1 {
		t.Errorf("bucket regions %v, %v", got, err)
	}
}
