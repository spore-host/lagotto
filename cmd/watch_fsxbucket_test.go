package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestLoadEC2SpawnConfig_FSxBucketPrefix (#242): both creation paths refuse an
// fsx_create config whose bucket is outside lagotto-fsx-, before anything is
// stored, and accept one inside it.
func TestLoadEC2SpawnConfig_FSxBucketPrefix(t *testing.T) {
	cfg := func(bucket string) watcher.ConfigReader {
		return func(context.Context, string) ([]byte, error) {
			return []byte("instance_type: g5.12xlarge\nfsx_create: true\nfsx_lifecycle: ephemeral\nfsx_s3_bucket: " + bucket +
				"\nfsx_import_path: s3://aws-buckai/indices/\n"), nil
		}
	}
	out, err := loadEC2SpawnConfig(context.Background(), cfg("aws-buckai"), "job.yaml")
	if err == nil {
		t.Fatalf("accepted fsx_create with a bucket outside %s; stored %s", watcher.FSxBucketPrefix, out)
	}
	for _, want := range []string{`"aws-buckai"`, watcher.FSxBucketPrefix, "fsx_import_path"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q: %v", want, err)
		}
	}
	if _, err := loadEC2SpawnConfig(context.Background(), cfg("lagotto-fsx-buckai"), "job.yaml"); err != nil {
		t.Errorf("refused a %s bucket: %v", watcher.FSxBucketPrefix, err)
	}
}
