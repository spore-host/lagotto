package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/deploy"
)

// The deploy --teardown prompt sends users to 'lagotto teardown' for the
// tables, so it must name every table that command deletes, the scheduled
// table included (#250).
func TestDeployTeardownPromptNamesEveryTable(t *testing.T) {
	p := deployTeardownPrompt("us-west-2", "123456789012", "lagotto-lambda-123456789012-us-west-2")
	for _, want := range []string{watchesTable, historyTable, "lagotto-scheduled-launches", "'lagotto teardown' deletes those"} {
		if !strings.Contains(p, want) {
			t.Errorf("prompt lacks %q:\n%s", want, p)
		}
	}
}

// deploy --teardown keeps the per-launch schedules with the scheduled-launches
// table and names the command that deletes both (#260).
func TestDeployTeardownPromptNamesTheScheduleDeleter(t *testing.T) {
	p := deployTeardownPrompt("us-west-2", "123456789012", "bucket")
	if !strings.Contains(p, "'lagotto-launch-sl-*' schedules") || !strings.Contains(p, "'lagotto teardown' deletes both") {
		t.Errorf("prompt does not say what deletes the per-launch schedules:\n%s", p)
	}
}

// The prompt says the default artifact bucket is deleted when lagotto
// manages it, and names a user-supplied --lambda-bucket as retained (#264).
func TestDeployTeardownPromptArtifactBucket(t *testing.T) {
	def := "lagotto-lambda-123456789012-us-west-2"
	p := deployTeardownPrompt("us-west-2", "123456789012", "")
	if !strings.Contains(p, "then the artifact bucket "+def) || !strings.Contains(p, "lagotto:managed=cli") || strings.Contains(p, "--lambda-bucket") {
		t.Errorf("default prompt:\n%s", p)
	}
	p = deployTeardownPrompt("us-west-2", "123456789012", "my-bucket")
	if !strings.Contains(p, "RETAINED: your --lambda-bucket my-bucket") || !strings.Contains(p, "then the artifact bucket "+def) {
		t.Errorf("user-bucket prompt:\n%s", p)
	}
	p = deployTeardownPrompt("us-west-2", "123456789012", def)
	if strings.Contains(p, "then the artifact bucket") || !strings.Contains(p, "RETAINED: your --lambda-bucket "+def) {
		t.Errorf("explicit default-named bucket must be retained, not deleted:\n%s", p)
	}
}

type fakeBucketDeleter struct {
	calls int
	res   deploy.BucketTeardown
}

func (f *fakeBucketDeleter) DeleteArtifactBucket(context.Context, string, string) (deploy.BucketTeardown, error) {
	f.calls++
	return f.res, nil
}

// A --lambda-bucket, even one spelled like the default, never reaches
// DeleteArtifactBucket; without one, the default bucket does.
func TestTeardownArtifactBucketRetainsUserSupplied(t *testing.T) {
	ctx := context.Background()
	def := deploy.DefaultBucketName("123456789012", "us-west-2")
	f := &fakeBucketDeleter{}
	if _, err := teardownArtifactBucket(ctx, f, "us-west-2", "123456789012", def); err != nil || f.calls != 0 {
		t.Errorf("explicit default-named --lambda-bucket: calls %d err %v", f.calls, err)
	}
	if _, err := teardownArtifactBucket(ctx, f, "us-west-2", "123456789012", ""); err != nil || f.calls != 1 {
		t.Errorf("no --lambda-bucket: calls %d err %v", f.calls, err)
	}
	if _, err := teardownArtifactBucket(ctx, f, "us-west-2", "123456789012", "my-bucket"); err != nil || f.calls != 2 {
		t.Errorf("other --lambda-bucket still deletes the managed default: calls %d err %v", f.calls, err)
	}
}

func TestReportArtifactBucketTeardown(t *testing.T) {
	var b bytes.Buffer
	reportArtifactBucketTeardown(&b, deploy.BucketTeardown{Bucket: "bk", Outcome: deploy.BucketDeleted, Versions: 3}, "", "us-west-2")
	if !strings.Contains(b.String(), "Deleted artifact bucket bk (3 object version(s)") {
		t.Errorf("deleted: %q", b.String())
	}
	b.Reset()
	reportArtifactBucketTeardown(&b, deploy.BucketTeardown{Bucket: "bk", Outcome: deploy.BucketRetainedUntagged}, "mine", "us-west-2")
	if !strings.Contains(b.String(), "bk is retained: it has no lagotto:managed=cli tag") ||
		!strings.Contains(b.String(), "'lagotto deploy --region us-west-2'") ||
		!strings.Contains(b.String(), "mine is retained: you supplied it with --lambda-bucket") {
		t.Errorf("retained: %q", b.String())
	}
}
