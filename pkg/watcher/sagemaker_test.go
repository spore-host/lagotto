package watcher_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// minimalTrainingJobSpec is a CreateTrainingJobInput as the user would supply via
// --sagemaker-config, marshaled to JSON for storage on the watch.
func minimalTrainingJobSpec(t *testing.T) []byte {
	t.Helper()
	spec := map[string]interface{}{
		"RoleArn": "arn:aws:iam::123456789012:role/SageMakerExecution",
		"AlgorithmSpecification": map[string]interface{}{
			"TrainingImage":     "123456789012.dkr.ecr.us-east-1.amazonaws.com/probe:latest",
			"TrainingInputMode": "File",
		},
		"OutputDataConfig": map[string]interface{}{"S3OutputPath": "s3://bucket/out"},
		"ResourceConfig": map[string]interface{}{
			"InstanceType":   "ml.g5.2xlarge",
			"InstanceCount":  1,
			"VolumeSizeInGB": 1,
		},
		"StoppingCondition": map[string]interface{}{"MaxRuntimeInSeconds": 600},
	}
	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return b
}

// TestSageMakerWatch_SubmitsAndLaunches drives a --service sagemaker watch
// through the poller with a real SageMakerLauncher (against substrate). Substrate
// reports the training job Completed, so the flow is submit -> check -> success.
func TestSageMakerWatch_SubmitsAndLaunches(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, testWatchesTable)
	env.CreateHistoryTable(t, testHistoryTable)
	store := watcher.NewStore(env.AWSConfig, testWatchesTable, testHistoryTable)
	truffle := truffleaws.NewClientFromConfig(env.AWSConfig)
	p := watcher.NewPoller(truffle, store, false, watcher.PollerOpts{
		SageMaker: watcher.NewSageMakerLauncher(env.AWSConfig),
	})

	w := newTestWatch("w-sm", "arn:aws:iam::123456789012:user/erin")
	w.Service = watcher.ServiceSageMaker
	w.InstanceTypePattern = "ml.g5.2xlarge"
	w.Spot = false
	w.Action = watcher.ActionSpawn // submit the job (not notify-only)
	w.SageMakerJobJSON = minimalTrainingJobSpec(t)

	// Cycle 1: submit. No in-flight job yet → launcher submits and stores the
	// job name; the watch stays active (capacity outcome not yet known).
	matches, err := p.PollWatch(context.Background(), w)
	if err != nil {
		t.Fatalf("PollWatch (submit): %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("submit cycle should not record a match yet, got %d", len(matches))
	}

	// Simulate the stored job name carried to the next cycle.
	w.SageMakerJobName = "lagotto-w-sm-1"

	// Cycle 2: check. Substrate reports the job Completed → success.
	matches, err = p.PollWatch(context.Background(), w)
	if err != nil {
		t.Fatalf("PollWatch (check): %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("check cycle should record 1 match, got %d", len(matches))
	}
	m := matches[0]
	if m.Service != watcher.ServiceSageMaker {
		t.Errorf("match Service = %q, want sagemaker", m.Service)
	}
	if m.InstanceType != "ml.g5.2xlarge" {
		t.Errorf("match InstanceType = %q, want ml.g5.2xlarge", m.InstanceType)
	}
	if m.ActionTaken != "sagemaker_launched" {
		t.Errorf("ActionTaken = %q, want sagemaker_launched", m.ActionTaken)
	}
}

// TestSageMakerWatch_NotifyOnlyDoesNotSubmit verifies a notify-only SageMaker
// watch doesn't submit a job (nothing to launch) and just stays active.
func TestSageMakerWatch_NotifyOnlyDoesNotSubmit(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, testWatchesTable)
	env.CreateHistoryTable(t, testHistoryTable)
	store := watcher.NewStore(env.AWSConfig, testWatchesTable, testHistoryTable)
	truffle := truffleaws.NewClientFromConfig(env.AWSConfig)
	p := watcher.NewPoller(truffle, store, false, watcher.PollerOpts{
		SageMaker: watcher.NewSageMakerLauncher(env.AWSConfig),
	})

	w := newTestWatch("w-sm-notify", "arn:aws:iam::123456789012:user/erin")
	w.Service = watcher.ServiceSageMaker
	w.InstanceTypePattern = "ml.g5.2xlarge"
	w.Action = watcher.ActionNotify

	matches, err := p.PollWatch(context.Background(), w)
	if err != nil {
		t.Fatalf("PollWatch: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("notify-only SageMaker watch should not produce a match, got %d", len(matches))
	}
}
