package watcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// TestPollSageMaker_SubmitFailureEmailSaysNotLaunched is #212 end to end: a
// SageMaker watch whose CreateTrainingJob is rejected (here a
// ValidationException, as for a bad RoleArn) runs through the real launcher,
// recordOutcome and Notify, and the email the user gets says the job was NOT
// launched, with the watch stopped as failed.
func TestPollSageMaker_SubmitFailureEmailSaysNotLaunched(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, "smf-watches")
	env.CreateHistoryTable(t, "smf-history")
	store := NewStore(env.AWSConfig, "smf-watches", "smf-history")
	ctx := context.Background()

	// A SageMaker endpoint that rejects every CreateTrainingJob.
	var targets []string
	sm := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		targets = append(targets, r.Header.Get("X-Amz-Target"))
		rw.Header().Set("Content-Type", "application/x-amz-json-1.1")
		rw.Header().Set("X-Amzn-ErrorType", "ValidationException")
		rw.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(rw, `{"__type":"ValidationException","message":"Could not assume role arn:aws:iam::123456789012:role/Nope"}`)
	}))
	defer sm.Close()
	smCfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(sm.URL),
	}

	w := storedSageMakerNotifyWatch(t)
	w.WatchID = "w-sm-submitfail"
	w.Action = ActionSpawn // the only SageMaker action that submits
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}

	pub := &capturePublisher{}
	p := NewPoller(nil, store, false, PollerOpts{
		SageMaker: NewSageMakerLauncher(smCfg),
		Notifier:  &Notifier{snsClient: pub, topicArn: "arn:aws:sns:us-east-1:111122223333:lagotto-capacity-alerts"},
	})
	if _, err := p.PollAll(ctx); err != nil {
		t.Fatalf("PollAll: %v", err)
	}

	if len(targets) != 1 || !strings.HasSuffix(targets[0], ".CreateTrainingJob") {
		t.Fatalf("SageMaker calls = %v, want one CreateTrainingJob", targets)
	}
	if len(pub.msgs) != 1 {
		t.Fatalf("published %d emails, want 1", len(pub.msgs))
	}
	subject, body := aws.ToString(pub.msgs[0].Subject), aws.ToString(pub.msgs[0].Message)
	if strings.Contains(subject, "job launched") || strings.Contains(body, "has been provisioned") {
		t.Errorf("a rejected submit was reported as launched:\nsubject: %s\n%s", subject, body)
	}
	for _, want := range []string{"NOT launched", "watch stopped"} {
		if !strings.Contains(subject, want) {
			t.Errorf("subject lacks %q: %s", want, subject)
		}
	}
	for _, want := range []string{"rejected the job", "Action Taken: sagemaker_submit_failed"} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q:\n%s", want, body)
		}
	}

	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil || got == nil {
		t.Fatalf("GetWatch: %v (%v)", got, err)
	}
	if got.Status != StatusFailed {
		t.Errorf("stored status = %q, want failed (the email says the watch stopped)", got.Status)
	}
}
