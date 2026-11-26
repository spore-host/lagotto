package awscfg

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// stsServer answers every request as GetCallerIdentity for the account the
// returned pointer holds, and counts the requests.
func stsServer(t *testing.T) (*atomic.Value, *atomic.Int32) {
	t.Helper()
	var account atomic.Value
	account.Store("111122223333")
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/xml")
		body := fmt.Sprintf(`<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
<GetCallerIdentityResult><Arn>arn:aws:iam::%[1]s:user/u</Arn><UserId>AIDAEXAMPLE</UserId><Account>%[1]s</Account></GetCallerIdentityResult>
<ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>`, account.Load())
		// nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter -- a loopback stub answering the SDK with an STS XML response; no browser renders it.
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{
			Region:       "us-east-1",
			Credentials:  credentials.NewStaticCredentialsProvider("fake", "fake", ""),
			BaseEndpoint: aws.String(srv.URL),
			HTTPClient:   srv.Client(),
			Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	})
	return &account, &calls
}

func callerIdentity(t *testing.T) {
	t.Helper()
	cfg, err := Load(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Fatal(err)
	}
}

// TestCallerAccountIsObservedNotFetched: the account a command's own
// GetCallerIdentity reported is recorded, and reading it makes no call
// (libs#78: the last mile must not add an STS call).
func TestCallerAccountIsObservedNotFetched(t *testing.T) {
	ResetCallerAccountForTest(t)
	_, calls := stsServer(t)
	if got := CallerAccount(); got != "" {
		t.Fatalf("CallerAccount before any call = %q, want empty", got)
	}
	if calls.Load() != 0 {
		t.Fatalf("CallerAccount made %d request(s)", calls.Load())
	}
	callerIdentity(t)
	if got := CallerAccount(); got != "111122223333" {
		t.Errorf("CallerAccount = %q, want 111122223333", got)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("%d request(s), want only the command's own 1", n)
	}
}

// TestCallerAccountConflictIsEmpty: two calls that named different accounts
// leave the account unknown rather than picking one.
func TestCallerAccountConflictIsEmpty(t *testing.T) {
	ResetCallerAccountForTest(t)
	account, _ := stsServer(t)
	callerIdentity(t)
	account.Store("444455556666")
	callerIdentity(t)
	if got := CallerAccount(); got != "" {
		t.Errorf("CallerAccount after two accounts = %q, want empty", got)
	}
}

// TestObserveCallerIdentityAddsOnce: a config whose APIOptions carry the
// observer twice (a copy of a copy) still builds working clients.
func TestObserveCallerIdentityAddsOnce(t *testing.T) {
	ResetCallerAccountForTest(t)
	stsServer(t)
	cfg, err := Load(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	cfg.APIOptions = append(cfg.APIOptions, observeCallerIdentity)
	if _, err := sts.NewFromConfig(cfg).GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}); err != nil {
		t.Fatalf("a doubled observer broke the call: %v", err)
	}
	if got := CallerAccount(); got != "111122223333" {
		t.Errorf("CallerAccount = %q", got)
	}
}

// TestLoadErrorAddsNothing: a loader error is returned as is.
func TestLoadErrorAddsNothing(t *testing.T) {
	want := errors.New("no credentials")
	SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, want
	})
	cfg, err := Load(context.Background(), "")
	if !errors.Is(err, want) || len(cfg.APIOptions) != 0 {
		t.Errorf("Load = %d APIOptions, %v; want none and the loader's error", len(cfg.APIOptions), err)
	}
}

// TestCallerIdentityWithoutAccount: a response naming no account records
// nothing.
func TestCallerIdentityWithoutAccount(t *testing.T) {
	ResetCallerAccountForTest(t)
	account, _ := stsServer(t)
	account.Store("")
	callerIdentity(t)
	if got := CallerAccount(); got != "" {
		t.Errorf("CallerAccount = %q, want empty", got)
	}
}

// TestCallerPartitionIsObserved (lagotto#347): the partition of the caller's
// ARN is recorded too, and two that differ leave it unknown.
func TestCallerPartitionIsObserved(t *testing.T) {
	ResetCallerAccountForTest(t)
	stsServer(t)
	if got := CallerPartition(); got != "" {
		t.Fatalf("CallerPartition before any call = %q", got)
	}
	callerIdentity(t)
	if got := CallerPartition(); got != "aws" {
		t.Errorf("CallerPartition = %q, want aws", got)
	}
	recordPartition("aws-iso")
	if got := CallerPartition(); got != "" {
		t.Errorf("CallerPartition after two partitions = %q, want empty", got)
	}
	SetCallerPartitionForTest(t, "aws-iso-b")
	if got := CallerPartition(); got != "aws-iso-b" {
		t.Errorf("SetCallerPartitionForTest: %q", got)
	}
}
