package watcher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/dynamodbtags"
	"github.com/spore-host/libs/tags/tagtest"
	"github.com/spore-host/spawn/pkg/launcher"

	spawnaws "github.com/spore-host/spawn/pkg/aws"

	"github.com/spore-host/lagotto/pkg/managedtags"
	"github.com/spore-host/lagotto/pkg/testutil"
)

// The poller's create paths (#324): every one carries
// spore-host:managed-by=lagotto, checked by the tagtest detector on real SDK
// clients (answered by fakes, or by substrate), next to the lagotto:* tags.

func TestHold_TagsTheReservation(t *testing.T) {
	cfg := aws.Config{Region: "us-east-1"}
	tagtest.New(t, tags.ToolLagotto).Install(&cfg)
	fake := &fakeCreateEC2{}
	client := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(fake)) })
	h := &Holder{createClient: func(string) holderCreateAPI { return client }}

	w := &Watch{WatchID: "AIDAOWNER/w-1", OwnerID: "AIDAOWNER"}
	m := &MatchResult{Region: "us-east-1", InstanceType: "p5.48xlarge", AvailabilityZone: "us-east-1a"}
	if err := h.Hold(context.Background(), w, m); err != nil {
		t.Fatalf("Hold: %v", err)
	}
	if len(fake.createdAZs) != 1 {
		t.Fatalf("no reservation was created: %v", fake.createdAZs)
	}
}

// The SageMaker job is created with the spore.host tags; a spec that carries
// one of those keys itself has it replaced, not duplicated.
func TestSageMakerSubmit_TagsTheJob(t *testing.T) {
	cfg := aws.Config{Region: "us-west-2"}
	tagtest.New(t, tags.ToolLagotto).Install(&cfg)
	fake := &fakeSageMakerCreate{}
	client := sagemaker.NewFromConfig(cfg, func(o *sagemaker.Options) { o.APIOptions = append(o.APIOptions, testutil.AnswerFrom(fake)) })

	spec, err := json.Marshal(map[string]any{
		"RoleArn":                "arn:aws:iam::123456789012:role/SageMakerExecution",
		"AlgorithmSpecification": map[string]any{"TrainingImage": "img", "TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://bucket/out"},
		"ResourceConfig":         map[string]any{"InstanceType": "ml.g5.2xlarge", "InstanceCount": 1, "VolumeSizeInGB": 1},
		"StoppingCondition":      map[string]any{"MaxRuntimeInSeconds": 600},
		"Tags":                   []map[string]string{{"Key": "team", "Value": "a"}, {"Key": tags.ManagedBy, "Value": "spawn"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	w := &Watch{WatchID: "AIDAOWNER/w-1", OwnerID: "AIDAOWNER", SageMakerJobJSON: spec}
	if _, err := (&SageMakerLauncher{}).submit(context.Background(), client, w, &MatchResult{}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if fake.in == nil {
		t.Fatal("no job was created")
	}
	got := map[string][]string{}
	for _, tg := range fake.in.Tags {
		got[aws.ToString(tg.Key)] = append(got[aws.ToString(tg.Key)], aws.ToString(tg.Value))
	}
	for k, want := range map[string]string{
		tags.ManagedBy: "lagotto", tags.Scope: "us-west-2", tags.Owner: "AIDAOWNER",
		"lagotto:managed": "true", "lagotto:watch-id": "w-1", "team": "a",
	} {
		if len(got[k]) != 1 || got[k][0] != want {
			t.Errorf("tag %s = %v, want exactly [%s] (all: %v)", k, got[k], want, got)
		}
	}
}

type fakeSageMakerCreate struct {
	in *sagemaker.CreateTrainingJobInput
}

func (f *fakeSageMakerCreate) CreateTrainingJob(_ context.Context, in *sagemaker.CreateTrainingJobInput, _ ...func(*sagemaker.Options)) (*sagemaker.CreateTrainingJobOutput, error) {
	f.in = in
	return &sagemaker.CreateTrainingJobOutput{TrainingJobArn: aws.String("arn:aws:sagemaker:us-west-2:123456789012:training-job/" + aws.ToString(in.TrainingJobName))}, nil
}

// Instances are launched through spawn, which puts LaunchConfig.Tags on the
// instance and its volumes: the spore.host tags must be there, for a watch's
// launch, a scheduled launch and a snipe, and override any the stored config
// claims.
func TestLaunchConfigs_CarryTheSporeHostTags(t *testing.T) {
	old := runtimeTagsNow
	runtimeTagsNow = func() time.Time { return time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { runtimeTagsNow = old })

	var cfgs []spawnaws.LaunchConfig
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		cfgs = append(cfgs, cfg)
		return &spawnaws.LaunchResult{InstanceID: "i-1"}, nil
	})
	raw, err := json.Marshal(SpawnConfigFile{InstanceType: "g5.xlarge", Region: "us-east-1", TTL: "2h",
		Tags: map[string]string{tags.ManagedBy: "someone-else"}})
	if err != nil {
		t.Fatal(err)
	}
	w := &Watch{WatchID: "AIDAOWNER/w-1", OwnerID: "AIDAOWNER", LaunchConfigJSON: raw}
	if err := sp.Spawn(context.Background(), w, &MatchResult{Region: "us-east-1"}); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if _, err := sp.LaunchScheduled(context.Background(), &ScheduledLaunch{ScheduleID: "AIDAOTHER/sl-1", OwnerID: "AIDAOTHER",
		Region: "us-west-2", LaunchConfigJSON: raw}); err != nil {
		t.Fatalf("LaunchScheduled: %v", err)
	}
	if len(cfgs) != 2 {
		t.Fatalf("launches = %d", len(cfgs))
	}
	for i, want := range []struct{ region, owner string }{{"us-east-1", "AIDAOWNER"}, {"us-west-2", "AIDAOTHER"}} {
		got := cfgs[i].Tags
		if got[tags.ManagedBy] != "lagotto" || got[tags.Scope] != want.region || got[tags.Owner] != want.owner ||
			got[tags.Created] != "2026-11-16T00:00:00Z" || tags.IsService(got) {
			t.Errorf("launch %d tags = %v, want managed-by=lagotto, scope %s, owner %s, no role", i, got, want.region, want.owner)
		}
	}
	if cfgs[0].Tags[WatchIDTagKey] != "AIDAOWNER/w-1" {
		t.Errorf("lagotto:watch-id dropped: %v", cfgs[0].Tags)
	}

	snipeCfg, _, err := sp.buildSnipeConfig(context.Background(), SnipeTarget{Region: "eu-west-1", InstanceType: "g5.xlarge"})
	if err != nil {
		t.Fatal(err)
	}
	if !tags.IsManagedBy(snipeCfg.Tags, tags.ToolLagotto) || snipeCfg.Tags[tags.Scope] != "eu-west-1" {
		t.Errorf("snipe tags = %v", snipeCfg.Tags)
	}
}

// EnsureTables creates every table with the spore.host tags (checked by the
// detector against substrate), and EnsureTableTags back-fills tables an older
// lagotto created, but never a scheduled-launches table without lagotto's
// managed tag, nor lagotto:managed itself.
func TestStore_TablesAreTagged(t *testing.T) {
	env := testutil.SubstrateServer(t)
	cfg := env.AWSConfig.Copy()
	tagtest.New(t, tags.ToolLagotto).Install(&cfg)
	s := NewStore(cfg, "lagotto-watches", "lagotto-match-history")
	s.SetTagStamp(managedtags.Stamp{Owner: "AIDAOWNER"})
	ctx := context.Background()
	if _, err := s.EnsureTables(ctx); err != nil {
		t.Fatalf("EnsureTables: %v", err)
	}
	if _, err := s.EnsureScheduledTable(ctx); err != nil {
		t.Fatalf("EnsureScheduledTable: %v", err)
	}
	for _, name := range s.tableNames() {
		got := tableTags(t, s, name)
		if !tags.IsManagedBy(got, tags.ToolLagotto) || !tags.IsService(got) || got[tags.Owner] != "AIDAOWNER" ||
			got[tags.Scope] != "us-east-1" || got["lagotto:managed"] != "cli" {
			t.Errorf("table %s tags = %v", name, got)
		}
	}
	if tagged, err := s.EnsureTableTags(ctx); err != nil || len(tagged) != 0 {
		t.Errorf("EnsureTableTags on tagged tables = %v, %v; want nothing to do", tagged, err)
	}
}

func TestStore_EnsureTableTagsBackfills(t *testing.T) {
	env := testutil.SubstrateServer(t)
	// Tables as a CloudFormation stack (watches, history: no lagotto tag) and
	// someone else (a scheduled-launches table without lagotto's tag) made them.
	env.CreateWatchesTable(t, "lagotto-watches")
	env.CreateHistoryTable(t, "lagotto-match-history")
	env.CreateWatchesTable(t, DefaultScheduledTable)
	s := NewStore(env.AWSConfig, "lagotto-watches", "lagotto-match-history")
	tagged, err := s.EnsureTableTags(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tagged) != 2 || tagged[0] != "lagotto-watches" || tagged[1] != "lagotto-match-history" {
		t.Errorf("tagged %v, want the watches and history tables only", tagged)
	}
	for _, name := range []string{"lagotto-watches", "lagotto-match-history"} {
		got := tableTags(t, s, name)
		if !tags.IsManagedBy(got, tags.ToolLagotto) {
			t.Errorf("%s not back-filled: %v", name, got)
		}
		if _, ok := got["lagotto:managed"]; ok {
			t.Errorf("%s gained lagotto:managed, which the poller's AUTO_DELETE_TABLES deletes on: %v", name, got)
		}
	}
	if got := tableTags(t, s, DefaultScheduledTable); tags.IsManagedBy(got, tags.ToolLagotto) {
		t.Errorf("a scheduled-launches table lagotto did not create was tagged: %v", got)
	}
}

func tableTags(t *testing.T, s *Store, name string) map[string]string {
	t.Helper()
	d, err := s.client.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String(name)})
	if err != nil {
		t.Fatalf("describe %s: %v", name, err)
	}
	out, err := s.client.ListTagsOfResource(context.Background(), &dynamodb.ListTagsOfResourceInput{ResourceArn: d.Table.TableArn})
	if err != nil {
		t.Fatalf("tags of %s: %v", name, err)
	}
	return dynamodbtags.ToMap(out.Tags)
}

// With no tables there is nothing to tag, and no error.
func TestStore_EnsureTableTagsSkipsAbsentTables(t *testing.T) {
	env := testutil.SubstrateServer(t)
	s := NewStore(env.AWSConfig, "lagotto-watches", "lagotto-match-history")
	if tagged, err := s.EnsureTableTags(context.Background()); err != nil || len(tagged) != 0 {
		t.Errorf("EnsureTableTags = %v, %v", tagged, err)
	}
}
