package deploy

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/spore-host/lagotto/pkg/testutil"
)

// TestSubstrate_ArtifactBucketLifecycle runs the #264 lifecycle against real S3
// semantics: an untagged pre-#264 bucket is retained, a deploy back-fills its
// tag, and teardown then empties a VERSIONED bucket (several versions of one
// key plus a delete marker) and deletes it.
func TestSubstrate_ArtifactBucketLifecycle(t *testing.T) {
	env := testutil.SubstrateServer(t)
	d := New(env.AWSConfig)
	raw := s3.NewFromConfig(env.AWSConfig)
	ctx := context.Background()
	const acct, region = "123456789012", "us-east-1"
	bucket := DefaultBucketName(acct, region)

	// A bucket as a lagotto before #264 left it: created, untagged.
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := raw.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("PutBucketVersioning: %v", err)
	}
	for _, body := range []string{"v1", "v2", "v3"} {
		if _, err := raw.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("lagotto/capacity-poller-v0.1.0.zip"),
			Body: strings.NewReader(body)}); err != nil {
			t.Fatalf("PutObject: %v", err)
		}
	}
	if _, err := raw.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("lagotto/capacity-poller-v0.1.0.zip")}); err != nil {
		t.Fatalf("DeleteObject (marker): %v", err)
	}

	res, err := d.DeleteArtifactBucket(ctx, region, acct)
	if err != nil || res.Outcome != BucketRetainedUntagged {
		t.Fatalf("untagged bucket: res %+v err %v, want retained", res, err)
	}

	act, err := d.ensureBucket(ctx, bucket, region, acct, true)
	if err != nil || !strings.Contains(act, "tagged") {
		t.Fatalf("back-fill: action %q err %v", act, err)
	}

	res, err = d.DeleteArtifactBucket(ctx, region, acct)
	if err != nil {
		t.Fatalf("DeleteArtifactBucket: %v", err)
	}
	if res.Outcome != BucketDeleted || res.Versions != 4 {
		t.Errorf("res = %+v, want deleted after removing 3 versions + 1 delete marker", res)
	}
	if _, err := raw.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		t.Error("bucket still exists after DeleteArtifactBucket")
	}
	if res, err := d.DeleteArtifactBucket(ctx, region, acct); err != nil || res.Outcome != BucketAbsent {
		t.Errorf("second teardown: res %+v err %v, want absent", res, err)
	}
}
