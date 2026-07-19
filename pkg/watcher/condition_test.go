package watcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// fakeS3 is an in-memory S3Lister: prefix → object count.
type fakeS3 struct {
	counts map[string]int32
	err    error
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.err != nil {
		return nil, f.err
	}
	key := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Prefix)
	return &s3.ListObjectsV2Output{KeyCount: aws.Int32(f.counts[key])}, nil
}

func TestParseCondition_Kinds(t *testing.T) {
	f := &fakeS3{counts: map[string]int32{}}
	tests := []struct {
		name    string
		spec    string
		wantErr bool
	}{
		{"s3-empty ok", "s3-empty: s3://b/manifest/ minus s3://b/prepared/", false},
		{"http-200 ok", "http-200: https://example.com/done", false},
		{"shell ok", "shell: test -f /tmp/done", false},
		{"unknown kind", "carrier-pigeon: whatever", true},
		{"no colon", "s3-empty s3://b/x", true},
		{"empty arg", "http-200:   ", true},
		{"s3 missing minus", "s3-empty: s3://b/manifest", true},
		{"s3 bad uri", "s3-empty: nots3://b/x minus s3://b/y", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCondition(tt.spec, f)
			if tt.wantErr != (err != nil) {
				t.Errorf("ParseCondition(%q) err=%v, wantErr=%v", tt.spec, err, tt.wantErr)
			}
		})
	}
}

func TestParseCondition_S3NeedsClient(t *testing.T) {
	if _, err := ParseCondition("s3-empty: s3://b/m minus s3://b/d", nil); err == nil {
		t.Error("s3-empty with nil client should error")
	}
	// http/shell don't need the S3 client.
	if _, err := ParseCondition("http-200: https://x/y", nil); err != nil {
		t.Errorf("http-200 with nil S3 client should be fine: %v", err)
	}
}

func TestS3EmptyCondition_Done(t *testing.T) {
	tests := []struct {
		name       string
		want, done int32
		wantDone   bool
	}{
		{"work remaining", 1655, 1200, false},
		{"exactly done", 1655, 1655, true},
		{"over-done (idempotent re-runs)", 1655, 1700, true},
		{"nothing wanted", 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeS3{counts: map[string]int32{
				"b/manifest/": tt.want,
				"b/prepared/": tt.done,
			}}
			c, err := ParseCondition("s3-empty: s3://b/manifest/ minus s3://b/prepared/", f)
			if err != nil {
				t.Fatalf("ParseCondition: %v", err)
			}
			got, err := c.Done(context.Background())
			if err != nil {
				t.Fatalf("Done: %v", err)
			}
			if got != tt.wantDone {
				t.Errorf("Done = %v (want=%d done=%d), wantDone=%v", got, tt.want, tt.done, tt.wantDone)
			}
		})
	}
}

func TestHTTPCondition_Done(t *testing.T) {
	for _, code := range []int{200, 204, 404, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		c, err := ParseCondition("http-200: "+srv.URL, nil)
		if err != nil {
			t.Fatalf("ParseCondition: %v", err)
		}
		got, err := c.Done(context.Background())
		if err != nil {
			t.Fatalf("Done(%d): %v", code, err)
		}
		want := code >= 200 && code < 300
		if got != want {
			t.Errorf("http status %d → Done=%v, want %v", code, got, want)
		}
		srv.Close()
	}
}

func TestShellCondition_Done(t *testing.T) {
	// exit 0 → done
	c, _ := ParseCondition("shell: exit 0", nil)
	if done, err := c.Done(context.Background()); err != nil || !done {
		t.Errorf("shell 'exit 0' → done=%v err=%v, want done=true", done, err)
	}
	// exit 1 → not done, and NOT an evaluator error
	c, _ = ParseCondition("shell: exit 1", nil)
	if done, err := c.Done(context.Background()); err != nil || done {
		t.Errorf("shell 'exit 1' → done=%v err=%v, want done=false err=nil", done, err)
	}
}

func TestIsShellCondition(t *testing.T) {
	if !IsShellCondition("shell: whoami") {
		t.Error("shell spec not recognized")
	}
	if IsShellCondition("http-200: https://x") {
		t.Error("http spec misclassified as shell")
	}
}
