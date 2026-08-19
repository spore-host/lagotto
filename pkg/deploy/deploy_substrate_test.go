package deploy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// fakeZipBody is a stand-in for the poller Lambda zip bytes; its content
// never matters to these tests since Substrate does not execute the Lambda,
// only stores/deploys it.
const fakeZipBody = "fake-lambda-zip-bytes"

// fakeHTTPGet returns a stub httpGet that always answers with fakeZipBody at
// 200 OK, so uploadArtifact's download step succeeds without a network call.
func fakeHTTPGet(*testing.T) func(string) (*http.Response, error) {
	return func(string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte(fakeZipBody))),
		}, nil
	}
}

// TestStackState_NoSuchStack verifies the "stack does not exist" branch of
// stackState, which createOrUpdate relies on to decide CreateStack vs
// UpdateStack (#1: pkg/deploy had 0% coverage before this file).
func TestStackState_NoSuchStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)

	exists, status, err := d.stackState(context.Background(), "no-such-stack")
	if err != nil {
		t.Fatalf("stackState: %v", err)
	}
	if exists {
		t.Errorf("exists = true for a stack never created, want false (status %v)", status)
	}
}

// TestDeploy_CreatesStackAndReturnsOutputs exercises Deploy end to end against
// Substrate: bucket creation, artifact upload (via a stubbed httpGet, so no
// real network/GitHub-release fetch), and CreateStack — then asserts the
// SAM-transformed stack's outputs come back, since callers (cmd/deploy.go,
// cmd/launch.go's scheduled-launch wiring) depend on those keys existing.
func TestDeploy_CreatesStackAndReturnsOutputs(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	outs, err := d.Deploy(context.Background(), Options{
		StackName: "lagotto-test", Region: "us-east-1", Version: "0.44.0",
		AccountID: "123456789012",
	})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	for _, key := range []string{
		"CapacityPollerFunctionArn", "SchedulerInvokeRoleArn",
		"WatchesTableName", "MatchHistoryTableName", "ScheduledTableName",
	} {
		if outs[key] == "" {
			t.Errorf("stack outputs missing %q (got %v)", key, outs)
		}
	}
}

// TestDeploy_RedeployIsAnUpdateNotAFailure verifies the createOrUpdate branch
// where the stack already exists in a healthy state: a second Deploy call
// must go through UpdateStack (not error out, not recreate) and still return
// outputs — this is the ordinary "run `lagotto deploy` again" path.
func TestDeploy_RedeployIsAnUpdateNotAFailure(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	opts := Options{StackName: "lagotto-redeploy", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012"}

	if _, err := d.Deploy(ctx, opts); err != nil {
		t.Fatalf("first Deploy: %v", err)
	}
	outs, err := d.Deploy(ctx, opts)
	if err != nil {
		t.Fatalf("second Deploy (update path): %v", err)
	}
	if outs["CapacityPollerFunctionArn"] == "" {
		t.Error("outputs missing CapacityPollerFunctionArn after redeploy")
	}
}

// TestTeardown_DeletesStack verifies Teardown removes a deployed stack and
// waits for completion — after it returns, stackState must report the stack
// gone, so a subsequent Deploy takes the CreateStack path rather than trying
// (and failing) an UpdateStack against nothing.
func TestTeardown_DeletesStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if _, err := d.Deploy(ctx, Options{
		StackName: "lagotto-teardown", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012",
	}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	if err := d.Teardown(ctx, "lagotto-teardown"); err != nil {
		t.Fatalf("Teardown: %v", err)
	}

	exists, _, err := d.stackState(ctx, "lagotto-teardown")
	if err != nil {
		t.Fatalf("stackState after teardown: %v", err)
	}
	if exists {
		t.Error("stack still exists after Teardown")
	}
}

// TestStackOutputs_ReadsDeployedStack is the direct regression guard for the
// public StackOutputs wrapper (used by cmd/launch.go to find the poller/
// scheduler ARNs for a scheduled launch) — distinct from Deploy's own return
// value, since a caller may query outputs for a stack deployed in a PRIOR
// process/invocation.
func TestStackOutputs_ReadsDeployedStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if _, err := d.Deploy(ctx, Options{
		StackName: "lagotto-outputs", Region: "us-east-1", Version: "0.44.0", AccountID: "123456789012",
	}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	outs, err := d.StackOutputs(ctx, "lagotto-outputs")
	if err != nil {
		t.Fatalf("StackOutputs: %v", err)
	}
	if outs["WatchesTableName"] == "" {
		t.Errorf("StackOutputs missing WatchesTableName (got %v)", outs)
	}
}

// TestStackOutputs_ErrorsForUndeployedStack: querying outputs for a stack that
// was never deployed must error, not return an empty map silently — a caller
// (e.g. a scheduled launch trying to wire an EventBridge target) needs to
// know deployment hasn't happened rather than proceed with nothing.
func TestStackOutputs_ErrorsForUndeployedStack(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)

	if _, err := d.StackOutputs(context.Background(), "never-deployed"); err == nil {
		t.Error("expected an error querying outputs for an undeployed stack")
	}
}

// TestEnsureBucket_IdempotentWhenAlreadyExists verifies the HeadBucket-first
// short-circuit: calling Deploy (which calls ensureBucket) twice for the same
// bucket must not error the second time around, since the bucket persists
// across the two calls and CreateBucket on an already-owned bucket would
// otherwise be a spurious failure on every redeploy.
func TestEnsureBucket_IdempotentWhenAlreadyExists(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = fakeHTTPGet(t)

	ctx := context.Background()
	if err := d.ensureBucket(ctx, "lagotto-lambda-123456789012-us-east-1", "us-east-1"); err != nil {
		t.Fatalf("first ensureBucket: %v", err)
	}
	// Second call against the SAME bucket must be a no-op success, not a
	// BucketAlreadyOwnedByYou failure.
	if err := d.ensureBucket(ctx, "lagotto-lambda-123456789012-us-east-1", "us-east-1"); err != nil {
		t.Fatalf("second ensureBucket (already exists): %v", err)
	}
}

// TestUploadArtifact_HTTPErrorPropagates verifies a network-level httpGet
// failure (as opposed to a non-200 response) surfaces as an error rather
// than a nil-body panic or a silently-empty upload.
func TestUploadArtifact_HTTPErrorPropagates(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = func(string) (*http.Response, error) {
		return nil, errors.New("connection refused")
	}

	err := d.uploadArtifact(context.Background(), "some-bucket", "some-key", "0.44.0")
	if err == nil {
		t.Fatal("expected an error when httpGet fails")
	}
}

// TestUploadArtifact_Non200StatusPropagates verifies a non-OK download (e.g.
// the requested version was never released, a 404) is reported with the
// status code and version — not treated as a successful upload of whatever
// GitHub's error page body happened to contain.
func TestUploadArtifact_Non200StatusPropagates(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	d.httpGet = func(string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	}

	err := d.uploadArtifact(context.Background(), "some-bucket", "some-key", "0.44.0")
	if err == nil {
		t.Fatal("expected an error for a 404 download")
	}
}
