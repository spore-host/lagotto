package deploy

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/tagtest"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// deployAllow are the deploy create calls that go out without the managed-by
// tag, each with why that is safe.
var deployAllow = []tagtest.Allow{
	{Service: "S3", Operation: "CreateBucket",
		Reason: "the artifact bucket is tagged by the PutBucketTagging that follows, which merges into the tags of a bucket that already exists too; its failure on a new bucket fails the deploy (ensureBucket)"},
	{Service: "S3", Operation: "PutObject",
		Reason: "the poller zip is an object in the tagged artifact bucket, deleted with it by deploy --teardown"},
}

// taggedDeployer is a Deployer whose clients are REAL SDK clients, with the
// tagtest untagged-create detector installed, answered by the in-package fakes
// (testutil.AnswerFrom). The detector sees exactly what production sends.
func taggedDeployer(t *testing.T, f3 *fakeS3, fs *fakeSNS, fl *fakeLogs, fn *fakeLambda, sc *fakeScheduler) *Deployer {
	t.Helper()
	cfg := aws.Config{Region: testRegion}
	tagtest.New(t, tags.ToolLagotto, deployAllow...).Install(&cfg)
	return &Deployer{
		s3: s3.NewFromConfig(cfg, func(o *s3.Options) { o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(f3)) }),
		sns: sns.NewFromConfig(cfg, func(o *sns.Options) {
			o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(fs))
		}),
		logs: cloudwatchlogs.NewFromConfig(cfg, func(o *cloudwatchlogs.Options) {
			o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(fl))
		}),
		lambda: lambda.NewFromConfig(cfg, func(o *lambda.Options) {
			o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(fn))
		}),
		sched: scheduler.NewFromConfig(cfg, func(o *scheduler.Options) {
			o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(sc))
		}),
		httpGet: fakeHTTPGet(t),
		sleep:   func(time.Duration) {},
		now:     time.Now,
	}
}

// TestDeploy_TagsEverythingItCreates (#324): a deploy into an empty account
// creates the artifact bucket, topic, log group, function, schedule group and
// schedule, and every one of those create calls carries
// spore-host:managed-by=lagotto (or is allow-listed above with its reason).
func TestDeploy_TagsEverythingItCreates(t *testing.T) {
	f3 := &fakeS3{headErr: s3NotFound}
	fs := &fakeSNS{arn: AlertsTopicARN(testRegion, testAccount), absent: true}
	fl := &fakeLogs{}
	fn := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}}
	sc := &fakeScheduler{schedules: map[string]*scheduler.GetScheduleOutput{}}
	d := taggedDeployer(t, f3, fs, fl, fn, sc)

	if _, err := d.Deploy(context.Background(), Options{
		Region: testRegion, AccountID: testAccount, Version: "0.72.0", Owner: "AIDAEXAMPLE",
	}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	// Each resource really was created, so the detector saw each create.
	if f3.called("CreateBucket") != 1 || len(fs.createCalls) != 1 || len(fn.createCalls) != 1 ||
		len(sc.groupCreateCalls) != 1 || len(sc.createCalls) != 1 || !fl.exists {
		t.Fatalf("not every resource was created: bucket %d topic %d function %d group %d schedule %d log group %v",
			f3.called("CreateBucket"), len(fs.createCalls), len(fn.createCalls), len(sc.groupCreateCalls), len(sc.createCalls), fl.exists)
	}
	// And the bucket, whose create is allow-listed, carries the tags after the deploy.
	if !tags.IsManagedBy(f3.tags, tags.ToolLagotto) || f3.tags[ManagedTagKey] != ManagedTagValue {
		t.Errorf("artifact bucket tags = %v", f3.tags)
	}
	for name, m := range map[string]map[string]string{
		"function": fn.createCalls[0].Tags, "log group": fl.tags, "schedule group": sc.groupTags, "topic": fs.tags,
	} {
		if (m[tags.Role] != "" || m[tags.Component] == "") || m[tags.Owner] != "AIDAEXAMPLE" || m[tags.Scope] != testRegion {
			t.Errorf("%s tags = %v, want a component and no role, owner and scope", name, m)
		}
	}
}

// TestDeploy_RedeployBackfillsTagsOntoAPreTagDeployment: a deployment an older
// lagotto made (resources present, no spore.host tags) gains them on the next
// deploy, without anything being re-created.
func TestDeploy_RedeployBackfillsTagsOntoAPreTagDeployment(t *testing.T) {
	f3 := &fakeS3{tags: map[string]string{ManagedTagKey: ManagedTagValue}}
	fs := &fakeSNS{arn: AlertsTopicARN(testRegion, testAccount), attributes: map[string]string{
		"DisplayName": alertsTopicDisplayName, "KmsMasterKeyId": alertsTopicKMSKey,
	}, tags: map[string]string{"Environment": "production", "Application": "lagotto", "Component": "alerts"}}
	retention := PollerLogRetentionDays
	fl := &fakeLogs{exists: true, retention: &retention, tags: map[string]string{ManagedTagKey: ManagedTagValue}}
	fn := &fakeLambda{getOut: &lambda.GetFunctionOutput{
		Configuration: &lambdatypes.FunctionConfiguration{
			FunctionArn: aws.String(PollerFunctionARN(testRegion, testAccount)), State: lambdatypes.StateActive,
		},
		Tags: PollerTags("production", "0.71.0"),
	}}
	sc := &fakeScheduler{groupExists: true, schedules: map[string]*scheduler.GetScheduleOutput{"default": preGroupSchedule()}}
	d := taggedDeployer(t, f3, fs, fl, fn, sc)

	if _, err := d.Deploy(context.Background(), Options{Region: testRegion, AccountID: testAccount, Version: "0.72.0"}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if f3.called("CreateBucket")+len(fs.createCalls)+len(fn.createCalls) != 0 {
		t.Fatal("an existing resource was re-created")
	}
	if len(fn.tagCalls) != 1 || !tags.IsManagedBy(fn.tagCalls[0].Tags, tags.ToolLagotto) {
		t.Errorf("function tag calls = %v", fn.tagCalls)
	}
	for name, m := range map[string]map[string]string{
		"bucket": f3.tags, "topic": fs.tags, "log group": fl.tags, "schedule group": sc.groupTags,
	} {
		if !tags.IsManagedBy(m, tags.ToolLagotto) {
			t.Errorf("%s was not back-filled: %v", name, m)
		}
	}
	if fl.tags[ManagedTagKey] != ManagedTagValue || f3.tags[ManagedTagKey] != ManagedTagValue {
		t.Error("the legacy lagotto:managed tag was dropped")
	}
	if sc.schedules["default"] != nil || sc.schedules["lagotto"] == nil {
		t.Errorf("schedule not moved into the tagged group: %v", sc.schedules)
	}
}
