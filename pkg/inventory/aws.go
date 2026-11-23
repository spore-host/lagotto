package inventory

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/dynamodbtags"
	"github.com/spore-host/libs/tags/ec2tags"
	"github.com/spore-host/libs/tags/iamtags"
	"github.com/spore-host/libs/tags/schedulertags"
	"github.com/spore-host/libs/tags/snstags"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/schedgroup"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The read-only AWS calls the footprint makes, one narrow interface per
// service; the SDK clients satisfy them.

type dynamoReader interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	ListTagsOfResource(ctx context.Context, in *dynamodb.ListTagsOfResourceInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type snsReader interface {
	GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, optFns ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error)
	ListTagsForResource(ctx context.Context, in *sns.ListTagsForResourceInput, optFns ...func(*sns.Options)) (*sns.ListTagsForResourceOutput, error)
}

type logsReader interface {
	DescribeLogGroups(ctx context.Context, in *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
	ListTagsForResource(ctx context.Context, in *cloudwatchlogs.ListTagsForResourceInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.ListTagsForResourceOutput, error)
}

type schedulerReader interface {
	GetScheduleGroup(ctx context.Context, in *scheduler.GetScheduleGroupInput, optFns ...func(*scheduler.Options)) (*scheduler.GetScheduleGroupOutput, error)
	ListTagsForResource(ctx context.Context, in *scheduler.ListTagsForResourceInput, optFns ...func(*scheduler.Options)) (*scheduler.ListTagsForResourceOutput, error)
	ListSchedules(ctx context.Context, in *scheduler.ListSchedulesInput, optFns ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error)
}

type ec2Reader interface {
	DescribeRegions(ctx context.Context, in *ec2.DescribeRegionsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeRegionsOutput, error)
	DescribeCapacityReservations(ctx context.Context, in *ec2.DescribeCapacityReservationsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error)
}

type sagemakerReader interface {
	Search(ctx context.Context, in *sagemaker.SearchInput, optFns ...func(*sagemaker.Options)) (*sagemaker.SearchOutput, error)
}

type cfnReader interface {
	DescribeStacks(ctx context.Context, in *cloudformation.DescribeStacksInput, optFns ...func(*cloudformation.Options)) (*cloudformation.DescribeStacksOutput, error)
}

// pollerReader is what the footprint reads through pkg/deploy, the same
// readers `lagotto doctor` uses; *deploy.Deployer satisfies it.
type pollerReader interface {
	InspectPoller(ctx context.Context) (deploy.PollerInfo, error)
	ArtifactBucketTags(ctx context.Context, region, accountID string) (map[string]string, error)
	ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error)
}

type iamReader interface {
	runtimeiam.IAMAPI
	ListInstanceProfilesForRole(ctx context.Context, in *iam.ListInstanceProfilesForRoleInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfilesForRoleOutput, error)
	GetInstanceProfile(ctx context.Context, in *iam.GetInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error)
	GetPolicy(ctx context.Context, in *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error)
}

// maxPages bounds every paginated listing.
const maxPages = 1000

// AWSSource reads the footprint from AWS with one account's credentials.
type AWSSource struct {
	cfg       aws.Config
	accountID string
	// Seams: the client for each service in a region.
	newRegion func(region string) *awsRegion
	ec2       ec2Reader
	iam       iamReader
	buckets   pollerReader
	// account builds the account-wide reader: the full one, or the poller's.
	account func(*AWSSource) AccountSource
}

// NewAWSSource reads with cfg, whose region lists the account's regions.
func NewAWSSource(cfg aws.Config, accountID string) *AWSSource {
	return newAWSSource(cfg, accountID, newFullAccount)
}

func newFullAccount(s *AWSSource) AccountSource {
	return &awsAccount{iam: s.iam, accountID: s.accountID}
}

func newAWSSource(cfg aws.Config, accountID string, account func(*AWSSource) AccountSource) *AWSSource {
	s := &AWSSource{cfg: cfg, accountID: accountID, ec2: ec2.NewFromConfig(cfg), iam: iam.NewFromConfig(cfg), buckets: deploy.New(cfg), account: account}
	s.newRegion = func(region string) *awsRegion {
		c := cfg.Copy()
		c.Region = region
		return &awsRegion{
			region: region, accountID: accountID,
			ddb: dynamodb.NewFromConfig(c), sns: sns.NewFromConfig(c), logs: cloudwatchlogs.NewFromConfig(c),
			sched: scheduler.NewFromConfig(c), ec2: ec2.NewFromConfig(c), sm: sagemaker.NewFromConfig(c),
			cfn: cloudformation.NewFromConfig(c), poller: deploy.New(c),
		}
	}
	return s
}

// EnabledRegions lists the regions enabled for the account: without
// AllRegions, DescribeRegions returns only those, the only ones lagotto can
// have anything in.
func (s *AWSSource) EnabledRegions(ctx context.Context) ([]string, error) {
	out, err := s.ec2.DescribeRegions(ctx, &ec2.DescribeRegionsInput{})
	if err != nil {
		return nil, err
	}
	var regions []string
	for _, r := range out.Regions {
		regions = append(regions, aws.ToString(r.RegionName))
	}
	if len(regions) == 0 {
		return nil, errors.New("DescribeRegions returned none")
	}
	return regions, nil
}

// ArtifactBucketRegions lists the account's default-named artifact buckets.
func (s *AWSSource) ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error) {
	return s.buckets.ArtifactBucketRegions(ctx, accountID)
}

// Region reads one region.
func (s *AWSSource) Region(region string) RegionSource { return s.newRegion(region) }

// Account reads the account-wide resources.
func (s *AWSSource) Account() AccountSource { return s.account(s) }

type awsRegion struct {
	region, accountID string
	ddb               dynamoReader
	sns               snsReader
	logs              logsReader
	sched             schedulerReader
	ec2               ec2Reader
	sm                sagemakerReader
	cfn               cfnReader
	poller            pollerReader
}

func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func isNotFound(err error) bool {
	switch errorCode(err) {
	case "ResourceNotFoundException", "NotFound", "NotFoundException", "NoSuchEntity":
		return true
	}
	return false
}

func (r *awsRegion) Tables(ctx context.Context, o Options) ([]Table, error) {
	o = o.withDefaults()
	var out []Table
	for _, name := range o.Tables() {
		desc, err := r.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return out, fmt.Errorf("describe table %s: %w", name, err)
		}
		t := Table{Item: Item{Name: name, ARN: TableARN(r.region, r.accountID, name)}}
		if desc.Table != nil && desc.Table.TableArn != nil {
			t.ARN = aws.ToString(desc.Table.TableArn)
		}
		if t.Tags, err = r.tableTags(ctx, t.ARN); err != nil {
			return out, fmt.Errorf("read the tags of table %s: %w", name, err)
		}
		in := &dynamodb.ScanInput{TableName: aws.String(name), Select: ddbtypes.SelectCount}
		if name == o.WatchesTable {
			// The poller-control item (#323) is not a record.
			in.FilterExpression = aws.String("watch_id <> :ctl")
			in.ExpressionAttributeValues = map[string]ddbtypes.AttributeValue{":ctl": &ddbtypes.AttributeValueMemberS{Value: watcher.PollerControlKey}}
		}
		if t.Records, err = r.count(ctx, in); err != nil {
			return out, fmt.Errorf("count the records of table %s: %w", name, err)
		}
		out = append(out, t)
	}
	return out, nil
}

func (r *awsRegion) tableTags(ctx context.Context, arn string) (map[string]string, error) {
	t := map[string]string{}
	in := &dynamodb.ListTagsOfResourceInput{ResourceArn: aws.String(arn)}
	for page := 0; page < maxPages; page++ {
		out, err := r.ddb.ListTagsOfResource(ctx, in)
		if err != nil {
			return nil, err
		}
		for k, v := range dynamodbtags.ToMap(out.Tags) {
			t[k] = v
		}
		if aws.ToString(out.NextToken) == "" {
			return t, nil
		}
		in.NextToken = out.NextToken
	}
	return nil, fmt.Errorf("still paging after %d pages", maxPages)
}

// count runs a Select=COUNT scan over every page.
func (r *awsRegion) count(ctx context.Context, in *dynamodb.ScanInput) (int64, error) {
	var n int64
	for page := 0; page < maxPages; page++ {
		out, err := r.ddb.Scan(ctx, in)
		if err != nil {
			return 0, err
		}
		n += int64(out.Count)
		if len(out.LastEvaluatedKey) == 0 {
			return n, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return 0, fmt.Errorf("still scanning after %d pages", maxPages)
}

func (r *awsRegion) Activity(ctx context.Context, watchesTable, scheduledTable string) (Activity, error) {
	var a Activity
	if watchesTable != "" {
		in := &dynamodb.QueryInput{
			TableName:                 aws.String(watchesTable),
			IndexName:                 aws.String("status-index"),
			KeyConditionExpression:    aws.String("#st = :status"),
			ExpressionAttributeNames:  map[string]string{"#st": "status"},
			ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{":status": &ddbtypes.AttributeValueMemberS{Value: string(watcher.StatusActive)}},
			Select:                    ddbtypes.SelectCount,
		}
		for page := 0; ; page++ {
			if page >= maxPages {
				return a, fmt.Errorf("count the active watches: still paging after %d pages", maxPages)
			}
			out, err := r.ddb.Query(ctx, in)
			if isNotFound(err) {
				break
			}
			if err != nil {
				return a, fmt.Errorf("count the active watches in %s: %w", watchesTable, err)
			}
			a.ActiveWatches += int(out.Count)
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			in.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	if scheduledTable != "" {
		in := &dynamodb.ScanInput{TableName: aws.String(scheduledTable)}
		for page := 0; ; page++ {
			if page >= maxPages {
				return a, fmt.Errorf("read the scheduled launches: still paging after %d pages", maxPages)
			}
			out, err := r.ddb.Scan(ctx, in)
			if isNotFound(err) {
				break
			}
			if err != nil {
				return a, fmt.Errorf("read the scheduled launches in %s: %w", scheduledTable, err)
			}
			for _, item := range out.Items {
				var sl watcher.ScheduledLaunch
				if err := attributevalue.UnmarshalMap(item, &sl); err != nil {
					return a, fmt.Errorf("read a scheduled launch in %s: %w", scheduledTable, err)
				}
				if sl.Armed() {
					a.ArmedLaunches++
				}
				if sl.ScheduleName != "" {
					a.Recorded = append(a.Recorded, sl.ScheduleName)
				}
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			in.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	return a, nil
}

func (r *awsRegion) Poller(ctx context.Context) (*Item, string, error) {
	info, err := r.poller.InspectPoller(ctx)
	if err != nil || !info.Exists {
		return nil, "", err
	}
	arn := info.ARN
	if arn == "" {
		arn = deploy.PollerFunctionARN(r.region, r.accountID)
	}
	return &Item{ARN: arn, Name: deploy.PollerFunctionName, Tags: info.Tags, IdleGrace: info.IdleGrace}, info.RoleARN, nil
}

func (r *awsRegion) Topic(ctx context.Context) (*Item, error) {
	arn := deploy.AlertsTopicARN(r.region, r.accountID)
	if _, err := r.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}); err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	out, err := r.sns.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil {
		return nil, fmt.Errorf("read its tags: %w", err)
	}
	return &Item{ARN: arn, Name: deploy.AlertsTopicName, Tags: snstags.ToMap(out.Tags)}, nil
}

func (r *awsRegion) LogGroup(ctx context.Context) (*Item, error) {
	out, err := r.logs.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(deploy.PollerLogGroupName)})
	if err != nil {
		return nil, err
	}
	found := false
	for _, g := range out.LogGroups {
		found = found || aws.ToString(g.LogGroupName) == deploy.PollerLogGroupName
	}
	if !found {
		return nil, nil
	}
	arn := deploy.PollerLogGroupARN(r.region, r.accountID)
	tg, err := r.logs.ListTagsForResource(ctx, &cloudwatchlogs.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil {
		return nil, fmt.Errorf("read its tags: %w", err)
	}
	return &Item{ARN: arn, Name: deploy.PollerLogGroupName, Tags: tg.Tags}, nil
}

func (r *awsRegion) ScheduleGroups(ctx context.Context) ([]Item, error) {
	var out []Item
	for _, name := range deploy.ScheduleGroups {
		g, err := r.sched.GetScheduleGroup(ctx, &scheduler.GetScheduleGroupInput{Name: aws.String(name)})
		if schedgroup.IsNotFound(err) || isNotFound(err) {
			continue
		}
		if err != nil {
			return out, fmt.Errorf("read schedule group %s: %w", name, err)
		}
		arn := aws.ToString(g.Arn)
		if arn == "" {
			arn = ScheduleGroupARN(r.region, r.accountID, name)
		}
		tg, err := r.sched.ListTagsForResource(ctx, &scheduler.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
		if err != nil {
			return out, fmt.Errorf("read the tags of schedule group %s: %w", name, err)
		}
		out = append(out, Item{ARN: arn, Name: name, Tags: schedulertags.ToMap(tg.Tags)})
	}
	return out, nil
}

func (r *awsRegion) Schedules(ctx context.Context) ([]Schedule, error) {
	var out []Schedule
	list := func(group, prefix string) error {
		in := &scheduler.ListSchedulesInput{GroupName: aws.String(group)}
		if prefix != "" {
			in.NamePrefix = aws.String(prefix)
		}
		for page := 0; page < maxPages; page++ {
			res, err := r.sched.ListSchedules(ctx, in)
			if err != nil {
				if group != schedgroup.Default && (schedgroup.IsNotFound(err) || isNotFound(err)) {
					return nil
				}
				return fmt.Errorf("list the schedules in schedule group %s: %w", group, err)
			}
			for _, s := range res.Schedules {
				name := aws.ToString(s.Name)
				if !strings.HasPrefix(name, prefix) {
					continue // an endpoint that ignores NamePrefix
				}
				sc := Schedule{Group: group, Name: name, State: string(s.State)}
				if s.Target != nil {
					sc.TargetARN = aws.ToString(s.Target.Arn)
				}
				out = append(out, sc)
			}
			if aws.ToString(res.NextToken) == "" {
				return nil
			}
			in.NextToken = res.NextToken
		}
		return fmt.Errorf("list the schedules in schedule group %s: still paging after %d pages", group, maxPages)
	}
	for _, g := range deploy.ScheduleGroups {
		if err := list(g, ""); err != nil {
			return out, err
		}
	}
	return out, list(schedgroup.Default, deploy.SchedulePrefix)
}

// Holds lists the active and pending capacity reservations lagotto holds:
// tagged spore-host:managed-by=lagotto, or lagotto:managed=true (before #324).
func (r *awsRegion) Holds(ctx context.Context) ([]Item, error) {
	in := &ec2.DescribeCapacityReservationsInput{Filters: []ec2types.Filter{
		{Name: aws.String("tag-key"), Values: []string{tags.ManagedBy, "lagotto:managed"}},
		{Name: aws.String("state"), Values: []string{"active", "pending"}},
	}}
	var out []Item
	for page := 0; page < maxPages; page++ {
		res, err := r.ec2.DescribeCapacityReservations(ctx, in)
		if err != nil {
			return out, err
		}
		for _, cr := range res.CapacityReservations {
			t := ec2tags.ToMap(cr.Tags)
			if !tags.IsManagedBy(t, Tool) && t["lagotto:managed"] != "true" {
				continue
			}
			out = append(out, Item{ARN: aws.ToString(cr.CapacityReservationArn), Name: aws.ToString(cr.CapacityReservationId), Tags: t})
		}
		if aws.ToString(res.NextToken) == "" {
			return out, nil
		}
		in.NextToken = res.NextToken
	}
	return out, fmt.Errorf("still paging after %d pages", maxPages)
}

// Jobs lists the SageMaker training jobs in progress for a watch (tagged
// lagotto:managed=true).
func (r *awsRegion) Jobs(ctx context.Context) ([]Item, error) {
	in := &sagemaker.SearchInput{
		Resource: smtypes.ResourceTypeTrainingJob,
		SearchExpression: &smtypes.SearchExpression{Filters: []smtypes.Filter{
			{Name: aws.String("Tags.lagotto:managed"), Operator: smtypes.OperatorEquals, Value: aws.String("true")},
			{Name: aws.String("TrainingJobStatus"), Operator: smtypes.OperatorEquals, Value: aws.String(string(smtypes.TrainingJobStatusInProgress))},
		}},
	}
	var out []Item
	for page := 0; page < maxPages; page++ {
		res, err := r.sm.Search(ctx, in)
		if err != nil {
			return out, err
		}
		for _, rec := range res.Results {
			j := rec.TrainingJob
			if j == nil {
				continue
			}
			t := map[string]string{}
			for _, tg := range j.Tags {
				t[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}
			out = append(out, Item{ARN: aws.ToString(j.TrainingJobArn), Name: aws.ToString(j.TrainingJobName), Tags: t})
		}
		if aws.ToString(res.NextToken) == "" {
			return out, nil
		}
		in.NextToken = res.NextToken
	}
	return out, fmt.Errorf("still paging after %d pages", maxPages)
}

func (r *awsRegion) LegacyStack(ctx context.Context, name string) (*Item, error) {
	out, err := r.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(name)})
	if err != nil {
		if errorCode(err) == "ValidationError" && strings.Contains(err.Error(), "does not exist") {
			return nil, nil
		}
		return nil, err
	}
	for _, st := range out.Stacks {
		if st.StackStatus == cfntypes.StackStatusDeleteComplete {
			continue
		}
		t := map[string]string{}
		for _, tg := range st.Tags {
			t[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
		}
		return &Item{ARN: aws.ToString(st.StackId), Name: aws.ToString(st.StackName), Tags: t}, nil
	}
	return nil, nil
}

func (r *awsRegion) BucketTags(ctx context.Context, region, accountID string) (map[string]string, error) {
	return r.poller.ArtifactBucketTags(ctx, region, accountID)
}

type awsAccount struct {
	iam       iamReader
	accountID string
}

// NewPollerAWSSource is the footprint as the capacity poller reads it for its
// idle teardown (lagotto#326): [NewAWSSource], without the account-wide
// resources the poller neither deletes nor must keep alive. Those are the
// administrator's authorized instance roles and their profiles, the invoke
// role its own one-shot jobs run as, and the lagotto-cli-user policy, all
// left to `lagotto teardown`. Not reading them keeps the poller's runtime
// policy to what its teardown uses. The poller program never builds the full
// account reader (newFullAccount is not reachable from it), so the IAM
// coverage gate sees exactly these reads.
func NewPollerAWSSource(cfg aws.Config, accountID string) *AWSSource {
	return newAWSSource(cfg, accountID, newPollerAccount)
}

func newPollerAccount(s *AWSSource) AccountSource { return pollerAWSAccount{iam: s.iam} }

type pollerAWSAccount struct{ iam iamReader }

func (a pollerAWSAccount) Role(ctx context.Context, name string) (*Item, error) {
	if name == runtimeiam.SchedulerInvokeRoleName {
		return nil, nil
	}
	return readRole(ctx, a.iam, name)
}

func (a pollerAWSAccount) InstanceProfilesForRole(ctx context.Context, role string) ([]Profile, error) {
	return readProfilesForRole(ctx, a.iam, role)
}

func (a pollerAWSAccount) InstanceProfile(ctx context.Context, name string) (*Profile, error) {
	return readInstanceProfile(ctx, a.iam, name)
}

func (pollerAWSAccount) AuthorizedRoles(context.Context) ([]string, error) { return nil, nil }

func (pollerAWSAccount) CLIPolicy(context.Context) (*Item, int32, error) { return nil, 0, nil }

func (a *awsAccount) Role(ctx context.Context, name string) (*Item, error) {
	return readRole(ctx, a.iam, name)
}

func readRole(ctx context.Context, client iamReader, name string) (*Item, error) {
	out, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil || out.Role == nil {
		return nil, err
	}
	return &Item{ARN: aws.ToString(out.Role.Arn), Name: name, Tags: iamtags.ToMap(out.Role.Tags)}, nil
}

func (a *awsAccount) InstanceProfilesForRole(ctx context.Context, role string) ([]Profile, error) {
	return readProfilesForRole(ctx, a.iam, role)
}

func readProfilesForRole(ctx context.Context, client iamReader, role string) ([]Profile, error) {
	var names []string
	for p := iam.NewListInstanceProfilesForRolePaginator(client, &iam.ListInstanceProfilesForRoleInput{RoleName: aws.String(role)}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if isNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		for _, ip := range page.InstanceProfiles {
			names = append(names, aws.ToString(ip.InstanceProfileName))
		}
	}
	var out []Profile
	for _, n := range names {
		// The list does not carry tags; GetInstanceProfile does.
		p, err := readInstanceProfile(ctx, client, n)
		if err != nil {
			return out, err
		}
		if p != nil {
			out = append(out, *p)
		}
	}
	return out, nil
}

func (a *awsAccount) InstanceProfile(ctx context.Context, name string) (*Profile, error) {
	return readInstanceProfile(ctx, a.iam, name)
}

func readInstanceProfile(ctx context.Context, client iamReader, name string) (*Profile, error) {
	out, err := client.GetInstanceProfile(ctx, &iam.GetInstanceProfileInput{InstanceProfileName: aws.String(name)})
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil || out.InstanceProfile == nil {
		return nil, err
	}
	ip := out.InstanceProfile
	p := &Profile{Item: Item{ARN: aws.ToString(ip.Arn), Name: name, Tags: iamtags.ToMap(ip.Tags)}}
	for _, r := range ip.Roles {
		p.Roles = append(p.Roles, aws.ToString(r.RoleName))
	}
	return p, nil
}

func (a *awsAccount) AuthorizedRoles(ctx context.Context) ([]string, error) {
	doc, err := runtimeiam.ReadRuntimePolicy(ctx, a.iam)
	switch {
	case err == nil:
		return runtimeiam.AuthorizedInstanceRoles(doc)
	case errors.Is(err, runtimeiam.ErrRoleNotFound), errors.Is(err, runtimeiam.ErrPolicyNotFound):
		return nil, nil
	}
	return nil, err
}

func (a *awsAccount) CLIPolicy(ctx context.Context) (*Item, int32, error) {
	arn := cliiam.PolicyARN(a.accountID)
	out, err := a.iam.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(arn)})
	if isNotFound(err) {
		return nil, 0, nil
	}
	if err != nil || out.Policy == nil {
		return nil, 0, err
	}
	return &Item{ARN: arn, Name: cliiam.PolicyName, Tags: iamtags.ToMap(out.Policy.Tags)}, aws.ToInt32(out.Policy.AttachmentCount), nil
}
