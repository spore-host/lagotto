package deploy

// The artifact bucket's lifecycle (#264).
//
// `lagotto deploy` creates lagotto-lambda-<account>-<region> when no
// --lambda-bucket is given and uploads the poller zip into it. Until #264 no
// command deleted it, so every region ever deployed kept a bucket. The rule is
// that everything lagotto creates has a named deleter, and for this bucket that
// is `lagotto deploy --teardown`.
//
// Deleting a bucket is the one teardown step that can destroy something lagotto
// did not make, so it is gated three ways, and every gate fails closed:
//
//  1. The NAME is exactly DefaultBucketName(account, region). A user-supplied
//     --lambda-bucket is never tagged, emptied or deleted.
//  2. The OWNER is this account. Every call on the bucket carries
//     ExpectedBucketOwner, so S3 itself refuses (403) if the name belongs to
//     another account.
//  3. The bucket carries ManagedTagKey=ManagedTagValue, which lagotto writes
//     only when it created the bucket or when a deploy back-fills it on a
//     default-named bucket this account owns. An untagged bucket is retained.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ManagedTagKey / ManagedTagValue mark an artifact bucket as lagotto-managed:
// created by `lagotto deploy`, so `lagotto deploy --teardown` may delete it.
const (
	ManagedTagKey   = "lagotto:managed"
	ManagedTagValue = "cli"
)

// maxVersionPages bounds the emptying loop. At 1000 versions a page that is a
// million object versions: far beyond anything a poller artifact bucket holds,
// and a hard stop if an endpoint keeps reporting IsTruncated forever.
const maxVersionPages = 1000

// s3API is the subset of *s3.Client pkg/deploy calls.
type s3API interface {
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, in *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetBucketTagging(ctx context.Context, in *s3.GetBucketTaggingInput, optFns ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error)
	PutBucketTagging(ctx context.Context, in *s3.PutBucketTaggingInput, optFns ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error)
	ListObjectVersions(ctx context.Context, in *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	DeleteBucket(ctx context.Context, in *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	ListBuckets(ctx context.Context, in *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
}

// ensureBucket makes sure the artifact bucket exists and returns what it did.
//
// managed is true only for the default-named bucket. Such a bucket is created
// tagged, and an existing one this account owns is tagged if it is not yet
// (the back-fill that makes pre-#264 deployments deletable). A failed tag on a
// bucket this call CREATED is an error, because an untagged bucket is one no
// command will delete; a failed back-fill is reported in the action and the
// deploy carries on, as it did before #264.
//
// A user-supplied bucket (managed false) is never tagged and its HeadBucket
// carries no ExpectedBucketOwner, exactly as before #264.
func (d *Deployer) ensureBucket(ctx context.Context, bucket, region, accountID string, managed bool) (string, error) {
	head := &s3.HeadBucketInput{Bucket: aws.String(bucket)}
	if managed {
		head.ExpectedBucketOwner = aws.String(accountID)
	}
	if _, err := d.s3.HeadBucket(ctx, head); err == nil {
		if !managed {
			return "exists", nil
		}
		tagged, terr := d.tagManagedBucket(ctx, bucket, accountID)
		switch {
		case terr != nil:
			return fmt.Sprintf("exists, NOT tagged %s=%s (%v): 'deploy --teardown' will retain it", ManagedTagKey, ManagedTagValue, terr), nil
		case tagged:
			return fmt.Sprintf("exists, tagged %s=%s", ManagedTagKey, ManagedTagValue), nil
		default:
			return "exists", nil
		}
	}
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	// us-east-1 must NOT set a LocationConstraint; every other region must.
	if region != "" && region != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	if _, err := d.s3.CreateBucket(ctx, in); err != nil {
		return "", fmt.Errorf("create artifact bucket %s: %w", bucket, err)
	}
	if !managed {
		return "created", nil
	}
	if _, err := d.tagManagedBucket(ctx, bucket, accountID); err != nil {
		return "", fmt.Errorf("tag new artifact bucket %s %s=%s (without the tag no lagotto command deletes it; "+
			"re-running 'lagotto deploy' retries the tag): %w", bucket, ManagedTagKey, ManagedTagValue, err)
	}
	return "created", nil
}

// tagManagedBucket adds the managed tag to a bucket this account owns, keeping
// every tag already on it (PutBucketTagging replaces the whole set). It reports
// whether it wrote anything.
func (d *Deployer) tagManagedBucket(ctx context.Context, bucket, accountID string) (bool, error) {
	tags, err := d.bucketTags(ctx, bucket, accountID)
	if err != nil {
		return false, err
	}
	if tags[ManagedTagKey] == ManagedTagValue {
		return false, nil
	}
	set := []s3types.Tag{{Key: aws.String(ManagedTagKey), Value: aws.String(ManagedTagValue)}}
	for k, v := range tags {
		if k != ManagedTagKey {
			set = append(set, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
		}
	}
	if _, err := d.s3.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket:              aws.String(bucket),
		ExpectedBucketOwner: aws.String(accountID),
		Tagging:             &s3types.Tagging{TagSet: set},
	}); err != nil {
		return false, fmt.Errorf("put bucket tagging: %w", err)
	}
	return true, nil
}

// bucketTags returns the bucket's tags; a bucket with none is an empty map.
func (d *Deployer) bucketTags(ctx context.Context, bucket, accountID string) (map[string]string, error) {
	out, err := d.s3.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
		Bucket:              aws.String(bucket),
		ExpectedBucketOwner: aws.String(accountID),
	})
	if err != nil {
		if apiErrorCode(err) == "NoSuchTagSet" {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("get bucket tagging: %w", err)
	}
	tags := make(map[string]string, len(out.TagSet))
	for _, t := range out.TagSet {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return tags, nil
}

// BucketOutcome is what DeleteArtifactBucket did with the default bucket.
type BucketOutcome string

const (
	// BucketDeleted: emptied of every object version and deleted.
	BucketDeleted BucketOutcome = "deleted"
	// BucketAbsent: there is no such bucket.
	BucketAbsent BucketOutcome = "absent"
	// BucketRetainedUntagged: the bucket lacks the managed tag, so lagotto
	// cannot tell it made it.
	BucketRetainedUntagged BucketOutcome = "retained-untagged"
)

// BucketTeardown reports DeleteArtifactBucket's result.
type BucketTeardown struct {
	Bucket  string
	Outcome BucketOutcome
	// Versions is how many object versions and delete markers were removed.
	Versions int
}

// DeleteArtifactBucket deletes the default artifact bucket
// (DefaultBucketName(accountID, region)) when lagotto manages it: owned by
// accountID and tagged ManagedTagKey=ManagedTagValue. It removes every object
// version and delete marker first, page by page, since S3 refuses to delete a
// non-empty bucket and the artifact bucket may be versioned.
//
// It never takes a bucket name: a user-supplied --lambda-bucket cannot reach
// it. Absent → BucketAbsent; untagged → BucketRetainedUntagged; both with a nil
// error. Any other failure, including a HeadBucket refused because another
// account owns the name, is an error and nothing is deleted.
func (d *Deployer) DeleteArtifactBucket(ctx context.Context, region, accountID string) (BucketTeardown, error) {
	if region == "" || accountID == "" {
		return BucketTeardown{}, fmt.Errorf("delete artifact bucket: region and accountID are required (the bucket name is derived from them)")
	}
	bucket := DefaultBucketName(accountID, region)
	res := BucketTeardown{Bucket: bucket}
	owner := aws.String(accountID)

	if _, err := d.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket), ExpectedBucketOwner: owner}); err != nil {
		if isBucketNotFound(err) {
			res.Outcome = BucketAbsent
			return res, nil
		}
		return res, fmt.Errorf("artifact bucket %s retained: could not confirm account %s owns it: %w", bucket, accountID, err)
	}
	tags, err := d.bucketTags(ctx, bucket, accountID)
	if err != nil {
		return res, fmt.Errorf("artifact bucket %s retained: %w", bucket, err)
	}
	if tags[ManagedTagKey] != ManagedTagValue {
		res.Outcome = BucketRetainedUntagged
		return res, nil
	}

	n, err := d.emptyBucket(ctx, bucket, accountID)
	res.Versions = n
	if err != nil {
		return res, err
	}
	if _, err := d.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket), ExpectedBucketOwner: owner}); err != nil {
		return res, fmt.Errorf("delete artifact bucket %s: %w", bucket, err)
	}
	res.Outcome = BucketDeleted
	return res, nil
}

// emptyBucket deletes every object version and delete marker in bucket,
// following every ListObjectVersions page, and returns how many it removed.
func (d *Deployer) emptyBucket(ctx context.Context, bucket, accountID string) (int, error) {
	owner := aws.String(accountID)
	in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), ExpectedBucketOwner: owner}
	deleted := 0
	for page := 0; ; page++ {
		if page >= maxVersionPages {
			return deleted, fmt.Errorf("empty artifact bucket %s: still listing versions after %d pages", bucket, maxVersionPages)
		}
		out, err := d.s3.ListObjectVersions(ctx, in)
		if err != nil {
			return deleted, fmt.Errorf("list object versions in %s: %w", bucket, err)
		}
		var ids []s3types.ObjectIdentifier
		for _, v := range out.Versions {
			ids = append(ids, s3types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range out.DeleteMarkers {
			ids = append(ids, s3types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(ids) > 0 {
			del, err := d.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket:              aws.String(bucket),
				ExpectedBucketOwner: owner,
				Delete:              &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return deleted, fmt.Errorf("delete object versions in %s: %w", bucket, err)
			}
			if len(del.Errors) > 0 {
				e := del.Errors[0]
				return deleted, fmt.Errorf("delete object versions in %s: %d failed, first %s (version %s): %s %s",
					bucket, len(del.Errors), aws.ToString(e.Key), aws.ToString(e.VersionId), aws.ToString(e.Code), aws.ToString(e.Message))
			}
			deleted += len(ids)
		}
		if !aws.ToBool(out.IsTruncated) {
			return deleted, nil
		}
		if out.NextKeyMarker == nil && out.NextVersionIdMarker == nil {
			return deleted, fmt.Errorf("list object versions in %s: truncated page without a next marker", bucket)
		}
		in.KeyMarker, in.VersionIdMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
}

// ArtifactBucketRegions returns the region of every default-named artifact
// bucket of accountID (lagotto-lambda-<account>-<region>), sorted as S3 lists
// them. Read-only; used by `lagotto doctor` to find buckets in regions with no
// poller.
func (d *Deployer) ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error) {
	prefix := DefaultBucketName(accountID, "")
	in := &s3.ListBucketsInput{Prefix: aws.String(prefix)}
	var regions []string
	for page := 0; page < maxVersionPages; page++ {
		out, err := d.s3.ListBuckets(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("list %s* buckets: %w", prefix, err)
		}
		for _, b := range out.Buckets {
			// Prefix already filters; checking again keeps an endpoint that ignores
			// it from handing back somebody else's bucket.
			if name := aws.ToString(b.Name); strings.HasPrefix(name, prefix) && len(name) > len(prefix) {
				regions = append(regions, strings.TrimPrefix(name, prefix))
			}
		}
		if aws.ToString(out.ContinuationToken) == "" {
			return regions, nil
		}
		in.ContinuationToken = out.ContinuationToken
	}
	return nil, fmt.Errorf("list %s* buckets: still paging after %d pages", prefix, maxVersionPages)
}

// ArtifactBucketManaged reports whether the default artifact bucket in region
// carries the managed tag. Read-only. d must be a Deployer for that region.
func (d *Deployer) ArtifactBucketManaged(ctx context.Context, region, accountID string) (bool, error) {
	tags, err := d.bucketTags(ctx, DefaultBucketName(accountID, region), accountID)
	if err != nil {
		return false, err
	}
	return tags[ManagedTagKey] == ManagedTagValue, nil
}

// isBucketNotFound reports whether a HeadBucket error means no such bucket.
func isBucketNotFound(err error) bool {
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var nsb *s3types.NoSuchBucket
	if errors.As(err, &nsb) {
		return true
	}
	switch apiErrorCode(err) {
	case "NotFound", "NoSuchBucket":
		return true
	}
	return false
}

// apiErrorCode returns the smithy API error code of err, or "".
func apiErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}
