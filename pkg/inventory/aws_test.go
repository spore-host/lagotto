package inventory

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedtypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// fakeDynamo has the watches and scheduled tables; the history table is
// absent. Scans answer in two pages, so the count follows pagination.
type fakeDynamo struct {
	scans   []*dynamodb.ScanInput
	queries []*dynamodb.QueryInput
}

func notFound() error {
	return &smithy.GenericAPIError{Code: "ResourceNotFoundException", Message: "not found"}
}

func (f *fakeDynamo) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if aws.ToString(in.TableName) == "lagotto-match-history" {
		return nil, notFound()
	}
	return &dynamodb.DescribeTableOutput{Table: &ddbtypes.TableDescription{TableArn: aws.String(TableARN("us-east-1", acct, aws.ToString(in.TableName)))}}, nil
}

func (f *fakeDynamo) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return &dynamodb.ListTagsOfResourceOutput{Tags: []ddbtypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}, nil
}

func (f *fakeDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	f.scans = append(f.scans, in)
	if in.Select == ddbtypes.SelectCount {
		if in.ExclusiveStartKey == nil {
			return &dynamodb.ScanOutput{Count: 2, LastEvaluatedKey: map[string]ddbtypes.AttributeValue{"k": &ddbtypes.AttributeValueMemberS{Value: "1"}}}, nil
		}
		return &dynamodb.ScanOutput{Count: 3}, nil
	}
	var items []map[string]ddbtypes.AttributeValue
	for _, sl := range []watcher.ScheduledLaunch{
		{ScheduleID: "sl-1", Status: watcher.ScheduledPending, ScheduleName: "lagotto-launch-sl-1"},
		{ScheduleID: "sl-2", Status: watcher.ScheduledLaunched, ScheduleName: "lagotto-launch-sl-2"},
		{ScheduleID: "sl-3", Status: watcher.ScheduledLaunched, CronExpr: "cron(0 * * * ? *)", ScheduleName: "lagotto-launch-sl-3"},
	} {
		m, _ := attributevalue.MarshalMap(sl)
		items = append(items, m)
	}
	return &dynamodb.ScanOutput{Items: items}, nil
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.queries = append(f.queries, in)
	return &dynamodb.QueryOutput{Count: 4}, nil
}

func TestAWSTablesAndActivity(t *testing.T) {
	ddb := &fakeDynamo{}
	r := &awsRegion{region: "us-east-1", accountID: acct, ddb: ddb}
	ctx := context.Background()
	got, err := r.Tables(ctx, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "lagotto-watches" || got[1].Name != "lagotto-scheduled-launches" {
		t.Fatalf("tables %+v", got)
	}
	for _, tb := range got {
		if tb.Records != 5 || !tags.IsManagedBy(tb.Tags, Tool) {
			t.Errorf("%s: records %d (want 5, both pages), tags %v", tb.Name, tb.Records, tb.Tags)
		}
	}
	// Only the watches table's count leaves out the poller-control item.
	if ddb.scans[0].FilterExpression == nil || ddb.scans[len(ddb.scans)-1].FilterExpression != nil {
		t.Errorf("control-item filter on the wrong scans")
	}
	for _, s := range ddb.scans {
		if s.Select != ddbtypes.SelectCount {
			t.Errorf("record count scan without Select=COUNT: %+v", s)
		}
	}

	act, err := r.Activity(ctx, "lagotto-watches", "lagotto-scheduled-launches")
	if err != nil {
		t.Fatal(err)
	}
	if act.ActiveWatches != 4 || act.ArmedLaunches != 2 || len(act.Recorded) != 3 {
		t.Errorf("activity %+v, want 4 active watches, 2 armed launches (pending, cron), 3 recorded schedules", act)
	}
	if q := ddb.queries[0]; aws.ToString(q.IndexName) != "status-index" || q.Select != ddbtypes.SelectCount {
		t.Errorf("active-watch query %+v", q)
	}
}

// fakeScheduler has lagotto's group (with two schedules, over two pages), no
// teardown group, and the default group, which ignores NamePrefix.
type fakeScheduler struct{}

func (fakeScheduler) GetScheduleGroup(_ context.Context, in *scheduler.GetScheduleGroupInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleGroupOutput, error) {
	if aws.ToString(in.Name) != "lagotto" {
		return nil, &schedtypes.ResourceNotFoundException{Message: aws.String("no group")}
	}
	return &scheduler.GetScheduleGroupOutput{Arn: aws.String(ScheduleGroupARN("us-east-1", acct, "lagotto"))}, nil
}

func (fakeScheduler) ListTagsForResource(context.Context, *scheduler.ListTagsForResourceInput, ...func(*scheduler.Options)) (*scheduler.ListTagsForResourceOutput, error) {
	return &scheduler.ListTagsForResourceOutput{Tags: []schedtypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}, nil
}

func (fakeScheduler) ListSchedules(_ context.Context, in *scheduler.ListSchedulesInput, _ ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	sum := func(n string) schedtypes.ScheduleSummary {
		return schedtypes.ScheduleSummary{Name: aws.String(n), State: schedtypes.ScheduleStateEnabled, Target: &schedtypes.TargetSummary{Arn: aws.String("arn:t")}}
	}
	switch aws.ToString(in.GroupName) {
	case "lagotto":
		if in.NextToken == nil {
			return &scheduler.ListSchedulesOutput{Schedules: []schedtypes.ScheduleSummary{sum("lagotto-capacity-poller")}, NextToken: aws.String("p2")}, nil
		}
		return &scheduler.ListSchedulesOutput{Schedules: []schedtypes.ScheduleSummary{sum("lagotto-launch-sl-1")}}, nil
	case "default":
		if aws.ToString(in.NamePrefix) != "lagotto-" {
			return nil, errors.New("default group listed without the lagotto- prefix")
		}
		return &scheduler.ListSchedulesOutput{Schedules: []schedtypes.ScheduleSummary{sum("lagotto-launch-sl-old"), sum("someone-elses")}}, nil
	}
	return nil, &schedtypes.ResourceNotFoundException{Message: aws.String("no group")}
}

func TestAWSSchedules(t *testing.T) {
	r := &awsRegion{region: "us-east-1", accountID: acct, sched: fakeScheduler{}}
	ctx := context.Background()
	groups, err := r.ScheduleGroups(ctx)
	if err != nil || len(groups) != 1 || groups[0].Name != "lagotto" || !tags.IsManagedBy(groups[0].Tags, Tool) {
		t.Fatalf("groups %+v, %v", groups, err)
	}
	sch, err := r.Schedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []Schedule{
		{Group: "lagotto", Name: "lagotto-capacity-poller", State: "ENABLED", TargetARN: "arn:t"},
		{Group: "lagotto", Name: "lagotto-launch-sl-1", State: "ENABLED", TargetARN: "arn:t"},
		{Group: "default", Name: "lagotto-launch-sl-old", State: "ENABLED", TargetARN: "arn:t"},
	}
	if len(sch) != len(want) {
		t.Fatalf("schedules %+v", sch)
	}
	for i := range want {
		if sch[i] != want[i] {
			t.Errorf("schedule %d = %+v, want %+v", i, sch[i], want[i])
		}
	}
}

type fakeEC2 struct{}

func (fakeEC2) DescribeRegions(context.Context, *ec2.DescribeRegionsInput, ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error) {
	return &ec2.DescribeRegionsOutput{Regions: []ec2types.Region{{RegionName: aws.String("us-east-1")}, {RegionName: aws.String("eu-west-1")}}}, nil
}

func (fakeEC2) DescribeCapacityReservations(context.Context, *ec2.DescribeCapacityReservationsInput, ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error) {
	cr := func(id string, t map[string]string) ec2types.CapacityReservation {
		var ts []ec2types.Tag
		for k, v := range t {
			ts = append(ts, ec2types.Tag{Key: aws.String(k), Value: aws.String(v)})
		}
		return ec2types.CapacityReservation{CapacityReservationId: aws.String(id), CapacityReservationArn: aws.String("arn:" + id), Tags: ts}
	}
	return &ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{
		cr("cr-new", map[string]string{tags.ManagedBy: "lagotto"}),
		cr("cr-old", map[string]string{"lagotto:managed": "true"}),
		cr("cr-spawn", map[string]string{tags.ManagedBy: "spawn"}),
	}}, nil
}

func TestAWSHoldsAndRegions(t *testing.T) {
	r := &awsRegion{region: "us-east-1", accountID: acct, ec2: fakeEC2{}}
	holds, err := r.Holds(context.Background())
	if err != nil || len(holds) != 2 || holds[0].Name != "cr-new" || holds[1].Name != "cr-old" {
		t.Errorf("holds %+v, %v", holds, err)
	}
	s := &AWSSource{ec2: fakeEC2{}}
	regions, err := s.EnabledRegions(context.Background())
	if err != nil || len(regions) != 2 {
		t.Errorf("regions %v, %v", regions, err)
	}
}

type fakeCFN struct{ err error }

func (f fakeCFN) DescribeStacks(context.Context, *cloudformation.DescribeStacksInput, ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error) {
	return nil, f.err
}

func TestAWSLegacyStackAbsent(t *testing.T) {
	r := &awsRegion{cfn: fakeCFN{err: &smithy.GenericAPIError{Code: "ValidationError", Message: "Stack with id lagotto does not exist"}}}
	if st, err := r.LegacyStack(context.Background(), "lagotto"); st != nil || err != nil {
		t.Errorf("absent stack = %+v, %v", st, err)
	}
	r.cfn = fakeCFN{err: &smithy.GenericAPIError{Code: "AccessDenied", Message: "no"}}
	if _, err := r.LegacyStack(context.Background(), "lagotto"); err == nil {
		t.Error("an AccessDenied read as no stack")
	}
}

// fakeIAM answers the account-wide reads.
type fakeIAM struct{ runtimeiam.IAMAPI }

func (fakeIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if aws.ToString(in.RoleName) != runtimeiam.InstanceRoleName {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("no")}
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{Arn: aws.String(IAMARN(acct, "role", runtimeiam.InstanceRoleName)), Tags: []iamtypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}}}, nil
}

func (fakeIAM) GetPolicy(context.Context, *iam.GetPolicyInput, ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	return &iam.GetPolicyOutput{Policy: &iamtypes.Policy{AttachmentCount: aws.Int32(2)}}, nil
}

func (fakeIAM) ListInstanceProfilesForRole(context.Context, *iam.ListInstanceProfilesForRoleInput, ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error) {
	return &iam.ListInstanceProfilesForRoleOutput{InstanceProfiles: []iamtypes.InstanceProfile{{InstanceProfileName: aws.String(runtimeiam.InstanceRoleName)}}}, nil
}

func (fakeIAM) GetInstanceProfile(_ context.Context, in *iam.GetInstanceProfileInput, _ ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error) {
	name := aws.ToString(in.InstanceProfileName)
	t := []iamtypes.Tag{{Key: aws.String(tags.ManagedBy), Value: aws.String("lagotto")}}
	if name == "team-gpu" {
		t = append(t, iamtypes.Tag{Key: aws.String(runtimeiam.ManagedProfileTagKey), Value: aws.String(runtimeiam.ManagedProfileTagValue)})
	}
	return &iam.GetInstanceProfileOutput{InstanceProfile: &iamtypes.InstanceProfile{
		Arn: aws.String(IAMARN(acct, "instance-profile", name)), Roles: []iamtypes.Role{{RoleName: aws.String(runtimeiam.InstanceRoleName)}},
		Tags: t,
	}}, nil
}

// ListInstanceProfiles lists lagotto's own profile, setup's for an
// authorized role (team-gpu, tagged), and someone else's (untagged).
func (fakeIAM) ListInstanceProfiles(context.Context, *iam.ListInstanceProfilesInput, ...func(*iam.Options)) (*iam.ListInstanceProfilesOutput, error) {
	return &iam.ListInstanceProfilesOutput{InstanceProfiles: []iamtypes.InstanceProfile{
		{InstanceProfileName: aws.String(runtimeiam.InstanceRoleName)}, {InstanceProfileName: aws.String("team-gpu")}, {InstanceProfileName: aws.String("someone-elses")}}}, nil
}

func TestAWSAccount(t *testing.T) {
	a := &awsAccount{iam: fakeIAM{}, accountID: acct}
	ctx := context.Background()
	if r, err := a.Role(ctx, runtimeiam.RoleName); r != nil || err != nil {
		t.Errorf("absent role = %+v, %v", r, err)
	}
	if r, err := a.Role(ctx, runtimeiam.InstanceRoleName); r == nil || err != nil || !tags.IsManagedBy(r.Tags, Tool) {
		t.Errorf("instance role = %+v, %v", r, err)
	}
	p, n, err := a.CLIPolicy(ctx)
	if err != nil || p == nil || p.ARN != cliiam.PolicyARN(acct) || n != 2 {
		t.Errorf("policy = %+v, %d attachments, %v", p, n, err)
	}
	profiles, err := a.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName)
	if err != nil || len(profiles) != 1 || len(profiles[0].Roles) != 1 || !tags.IsManagedBy(profiles[0].Tags, Tool) {
		t.Errorf("profiles = %+v, %v", profiles, err)
	}
	if p, err := a.InstanceProfile(ctx, "team-gpu"); err != nil || p == nil || p.Name != "team-gpu" {
		t.Errorf("authorized profile = %+v, %v", p, err)
	}
}

// TestPollerAWSAccount: the poller's account reader reads lagotto's own roles
// and instance profile, and skips what only `lagotto teardown` removes: the
// invoke role, the authorized instance roles and the lagotto-cli-user policy
// (lagotto#326).
func TestPollerAWSAccount(t *testing.T) {
	s := NewPollerAWSSource(aws.Config{Region: "us-east-1"}, acct)
	if _, ok := s.Account().(pollerAWSAccount); !ok {
		t.Fatalf("account source %T", s.Account())
	}
	a := pollerAWSAccount{iam: fakeIAM{}}
	ctx := context.Background()
	if r, err := a.Role(ctx, runtimeiam.InstanceRoleName); r == nil || err != nil {
		t.Errorf("instance role = %+v, %v", r, err)
	}
	if r, err := a.Role(ctx, runtimeiam.SchedulerInvokeRoleName); r != nil || err != nil {
		t.Errorf("invoke role read: %+v, %v", r, err)
	}
	if roles, err := a.AuthorizedRoles(ctx); roles != nil || err != nil {
		t.Errorf("authorized roles read: %v, %v", roles, err)
	}
	if p, n, err := a.CLIPolicy(ctx); p != nil || n != 0 || err != nil {
		t.Errorf("policy read: %+v", p)
	}
	if profiles, err := a.InstanceProfilesForRole(ctx, runtimeiam.InstanceRoleName); err != nil || len(profiles) != 1 {
		t.Errorf("profiles = %+v, %v", profiles, err)
	}
	if p, err := a.InstanceProfile(ctx, runtimeiam.InstanceRoleName); err != nil || p == nil {
		t.Errorf("profile = %+v, %v", p, err)
	}
}
