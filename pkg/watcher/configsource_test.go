package watcher

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestIsS3Ref(t *testing.T) {
	cases := map[string]bool{
		"s3://bucket/key":     true,
		"s3://bucket/a/b.yml": true,
		"/local/path.yaml":    false,
		"relative.yaml":       false,
		"-":                   false,
		"":                    false,
	}
	for ref, want := range cases {
		if got := IsS3Ref(ref); got != want {
			t.Errorf("IsS3Ref(%q) = %v, want %v", ref, got, want)
		}
	}
}

func TestParseS3Ref(t *testing.T) {
	b, k, err := parseS3Ref("s3://my-bucket/configs/block.yaml")
	if err != nil {
		t.Fatalf("parseS3Ref: %v", err)
	}
	if b != "my-bucket" || k != "configs/block.yaml" {
		t.Errorf("parseS3Ref = %q,%q; want my-bucket, configs/block.yaml", b, k)
	}
	for _, bad := range []string{"s3://bucket", "s3://bucket/", "s3://", "s3:///key"} {
		if _, _, err := parseS3Ref(bad); err == nil {
			t.Errorf("parseS3Ref(%q) = nil error, want error", bad)
		}
	}
}

// TestNewConfigReader_LocalNeverLoadsAWS verifies a purely-local reference reads
// from the filesystem without ever invoking the AWS config loader — a local
// config must not require credentials.
func TestNewConfigReader_LocalNeverLoadsAWS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("instance_type: g5.xlarge\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded := false
	read := NewConfigReader(func(ctx context.Context) (aws.Config, error) {
		loaded = true
		return aws.Config{}, nil
	}, nil)

	got, err := read(context.Background(), path)
	if err != nil {
		t.Fatalf("read local: %v", err)
	}
	if !strings.Contains(string(got), "g5.xlarge") {
		t.Errorf("read local = %q", got)
	}
	if loaded {
		t.Error("AWS config loaded for a local path; want lazy (s3-only) load")
	}
}

// TestNewConfigReader_Stdin verifies "-" reads from the provided stdin.
func TestNewConfigReader_Stdin(t *testing.T) {
	read := NewConfigReader(nil, strings.NewReader("instance_type: c7g.large\n"))
	got, err := read(context.Background(), "-")
	if err != nil {
		t.Fatalf("read stdin: %v", err)
	}
	if !strings.Contains(string(got), "c7g.large") {
		t.Errorf("read stdin = %q", got)
	}
}

// TestNewConfigReader_S3LoadError surfaces the config-load error on an s3:// ref
// (and only then — the loader is invoked lazily).
func TestNewConfigReader_S3LoadError(t *testing.T) {
	sentinel := errors.New("no creds")
	read := NewConfigReader(func(ctx context.Context) (aws.Config, error) {
		return aws.Config{}, sentinel
	}, nil)
	_, err := read(context.Background(), "s3://bucket/key.yaml")
	if err == nil || !errors.Is(err, sentinel) {
		t.Fatalf("read s3 = %v, want wrap of %v", err, sentinel)
	}
}
