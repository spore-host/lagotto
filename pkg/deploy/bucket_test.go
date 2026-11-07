package deploy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
)

const (
	bAcct   = "123456789012"
	bRegion = "us-west-2"
)

var bDefault = DefaultBucketName(bAcct, bRegion)

// fakeS3 records every S3 call. ExpectedBucketOwner is recorded per call so a
// test can assert the owner gate is on every request that touches the bucket.
type fakeS3 struct {
	headErr   error
	createErr error
	tags      map[string]string // nil: NoSuchTagSet
	getTagErr error
	putTagErr error
	// pages is consumed one per ListObjectVersions call.
	pages     []*s3.ListObjectVersionsOutput
	listErr   error
	delOut    *s3.DeleteObjectsOutput
	bucketsBy [][]string // ListBuckets pages of names

	calls      []string
	owners     map[string][]string // op → ExpectedBucketOwner values seen
	heads      []*s3.HeadBucketInput
	puts       []*s3.PutBucketTaggingInput
	lists      []*s3.ListObjectVersionsInput
	deletes    []*s3.DeleteObjectsInput
	listBucket []*s3.ListBucketsInput
}

func (f *fakeS3) rec(op string, owner *string) {
	f.calls = append(f.calls, op)
	if f.owners == nil {
		f.owners = map[string][]string{}
	}
	f.owners[op] = append(f.owners[op], aws.ToString(owner))
}

func (f *fakeS3) HeadBucket(_ context.Context, in *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	f.rec("HeadBucket", in.ExpectedBucketOwner)
	f.heads = append(f.heads, in)
	return &s3.HeadBucketOutput{}, f.headErr
}

func (f *fakeS3) CreateBucket(_ context.Context, _ *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	f.rec("CreateBucket", nil)
	if f.createErr == nil {
		f.headErr = nil
	}
	return &s3.CreateBucketOutput{}, f.createErr
}

func (f *fakeS3) PutObject(_ context.Context, _ *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.rec("PutObject", nil)
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) GetBucketTagging(_ context.Context, in *s3.GetBucketTaggingInput, _ ...func(*s3.Options)) (*s3.GetBucketTaggingOutput, error) {
	f.rec("GetBucketTagging", in.ExpectedBucketOwner)
	if f.getTagErr != nil {
		return nil, f.getTagErr
	}
	if f.tags == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchTagSet", Message: "The TagSet does not exist"}
	}
	out := &s3.GetBucketTaggingOutput{}
	for k, v := range f.tags {
		out.TagSet = append(out.TagSet, s3types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return out, nil
}

func (f *fakeS3) PutBucketTagging(_ context.Context, in *s3.PutBucketTaggingInput, _ ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error) {
	f.rec("PutBucketTagging", in.ExpectedBucketOwner)
	f.puts = append(f.puts, in)
	if f.putTagErr != nil {
		return nil, f.putTagErr
	}
	f.tags = map[string]string{}
	for _, t := range in.Tagging.TagSet {
		f.tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return &s3.PutBucketTaggingOutput{}, nil
}

func (f *fakeS3) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	f.rec("ListObjectVersions", in.ExpectedBucketOwner)
	cp := *in
	f.lists = append(f.lists, &cp)
	if f.listErr != nil {
		return nil, f.listErr
	}
	i := len(f.lists) - 1
	if i >= len(f.pages) {
		return &s3.ListObjectVersionsOutput{}, nil
	}
	return f.pages[i], nil
}

func (f *fakeS3) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	f.rec("DeleteObjects", in.ExpectedBucketOwner)
	f.deletes = append(f.deletes, in)
	if f.delOut != nil {
		return f.delOut, nil
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (f *fakeS3) DeleteBucket(_ context.Context, in *s3.DeleteBucketInput, _ ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	f.rec("DeleteBucket", in.ExpectedBucketOwner)
	return &s3.DeleteBucketOutput{}, nil
}

func (f *fakeS3) ListBuckets(_ context.Context, in *s3.ListBucketsInput, _ ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	f.rec("ListBuckets", nil)
	cp := *in
	f.listBucket = append(f.listBucket, &cp)
	i := len(f.listBucket) - 1
	out := &s3.ListBucketsOutput{}
	if i < len(f.bucketsBy) {
		for _, n := range f.bucketsBy[i] {
			out.Buckets = append(out.Buckets, s3types.Bucket{Name: aws.String(n)})
		}
		if i < len(f.bucketsBy)-1 {
			out.ContinuationToken = aws.String(fmt.Sprintf("tok-%d", i+1))
		}
	}
	return out, nil
}

func (f *fakeS3) called(op string) int {
	n := 0
	for _, c := range f.calls {
		if c == op {
			n++
		}
	}
	return n
}

// assertOwnerOnEvery fails unless every recorded call of each op carried
// ExpectedBucketOwner = bAcct.
func (f *fakeS3) assertOwnerOnEvery(t *testing.T, ops ...string) {
	t.Helper()
	for _, op := range ops {
		for _, o := range f.owners[op] {
			if o != bAcct {
				t.Errorf("%s ExpectedBucketOwner = %q, want %q", op, o, bAcct)
			}
		}
	}
}

func s3Deployer(f *fakeS3) *Deployer {
	return &Deployer{s3: f, sleep: func(time.Duration) {}}
}

var s3NotFound = &s3types.NotFound{}

func forbidden() error {
	return &smithy.GenericAPIError{Code: "Forbidden", Message: "Forbidden"}
}

// --- ensureBucket: tag at creation, back-fill, user-supplied ---------------

func TestEnsureBucket_NewDefaultBucketIsCreatedTagged(t *testing.T) {
	f := &fakeS3{headErr: s3NotFound}
	act, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true)
	if err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	if act != "created" || f.called("CreateBucket") != 1 || len(f.puts) != 1 {
		t.Fatalf("action %q, calls %v", act, f.calls)
	}
	if f.tags[ManagedTagKey] != ManagedTagValue {
		t.Errorf("tags after create = %v, want %s=%s", f.tags, ManagedTagKey, ManagedTagValue)
	}
	f.assertOwnerOnEvery(t, "HeadBucket", "GetBucketTagging", "PutBucketTagging")
}

func TestEnsureBucket_TagFailureOnANewBucketFailsTheDeploy(t *testing.T) {
	f := &fakeS3{headErr: s3NotFound, putTagErr: errors.New("AccessDenied")}
	_, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true)
	if err == nil || !strings.Contains(err.Error(), "no lagotto command deletes it") {
		t.Fatalf("err = %v, want the untagged-new-bucket error", err)
	}
}

// The back-fill: an existing default bucket this account owns gets the tag,
// and every tag already on it is kept (PutBucketTagging replaces the set).
func TestEnsureBucket_BackfillsTheTagKeepingExistingTags(t *testing.T) {
	f := &fakeS3{tags: map[string]string{"CostCenter": "42"}}
	act, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true)
	if err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	if f.called("CreateBucket") != 0 || !strings.Contains(act, "tagged") {
		t.Errorf("action %q, calls %v", act, f.calls)
	}
	if f.tags[ManagedTagKey] != ManagedTagValue || f.tags["CostCenter"] != "42" {
		t.Errorf("tags = %v, want the managed tag added and CostCenter kept", f.tags)
	}
	if len(f.heads) != 1 || aws.ToString(f.heads[0].ExpectedBucketOwner) != bAcct {
		t.Errorf("HeadBucket must carry ExpectedBucketOwner before a back-fill: %+v", f.heads)
	}
	f.assertOwnerOnEvery(t, "GetBucketTagging", "PutBucketTagging")
}

func TestEnsureBucket_AlreadyTaggedIsNotRewritten(t *testing.T) {
	f := &fakeS3{tags: map[string]string{ManagedTagKey: ManagedTagValue}}
	act, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true)
	if err != nil || act != "exists" || len(f.puts) != 0 {
		t.Errorf("action %q err %v puts %d", act, err, len(f.puts))
	}
}

// A back-fill that fails (e.g. an older IAM baseline without
// s3:PutBucketTagging) is reported, not fatal.
func TestEnsureBucket_FailedBackfillIsReportedNotFatal(t *testing.T) {
	f := &fakeS3{putTagErr: errors.New("AccessDenied")}
	act, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true)
	if err != nil {
		t.Fatalf("ensureBucket: %v", err)
	}
	if !strings.Contains(act, "NOT tagged") || !strings.Contains(act, "retain") {
		t.Errorf("action %q does not report the failed back-fill", act)
	}
}

// The back-fill gate: a default-named bucket this account does NOT own fails
// HeadBucket (S3 enforces ExpectedBucketOwner with a 403), so nothing is
// tagged and the create collides, as before #264.
func TestEnsureBucket_NeverTagsABucketAnotherAccountOwns(t *testing.T) {
	f := &fakeS3{headErr: forbidden(), createErr: &s3types.BucketAlreadyExists{}}
	if _, err := s3Deployer(f).ensureBucket(context.Background(), bDefault, bRegion, bAcct, true); err == nil {
		t.Fatal("want the CreateBucket collision")
	}
	if f.called("GetBucketTagging")+f.called("PutBucketTagging") != 0 {
		t.Errorf("a bucket of another account was tag-read or tagged: %v", f.calls)
	}
}

// A user-supplied --lambda-bucket is never tagged, created or not, and its
// HeadBucket is unchanged (no ExpectedBucketOwner).
func TestEnsureBucket_UserSuppliedBucketIsNeverTagged(t *testing.T) {
	for _, headErr := range []error{nil, s3NotFound} {
		f := &fakeS3{headErr: headErr}
		if _, err := s3Deployer(f).ensureBucket(context.Background(), "my-bucket", bRegion, bAcct, false); err != nil {
			t.Fatalf("ensureBucket: %v", err)
		}
		if f.called("GetBucketTagging")+f.called("PutBucketTagging") != 0 {
			t.Errorf("user bucket (head err %v) was tagged: %v", headErr, f.calls)
		}
		if f.heads[0].ExpectedBucketOwner != nil {
			t.Errorf("user bucket HeadBucket gained ExpectedBucketOwner")
		}
	}
}

// Deploy with --lambda-bucket spelled like the default still treats it as
// user-supplied: no tagging.
func TestDeploy_ExplicitDefaultNamedBucketIsNotTagged(t *testing.T) {
	l := &fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}}
	sc := &fakeScheduler{getErr: &schedulertypes.ResourceNotFoundException{Message: aws.String("Schedule not found")}}
	d, _ := testDeployer(l, &fakeSNS{arn: AlertsTopicARN(bRegion, bAcct)}, sc)
	f := &fakeS3{}
	d.s3 = f
	d.httpGet = fakeHTTPGet(t)
	res, err := d.Deploy(context.Background(), Options{Region: bRegion, AccountID: bAcct, Version: "0.55.1", Bucket: bDefault})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if f.called("GetBucketTagging")+f.called("PutBucketTagging") != 0 {
		t.Errorf("explicit --lambda-bucket was tagged: %v", f.calls)
	}
	if !strings.HasPrefix(res.Actions[0], "artifact bucket "+bDefault) {
		t.Errorf("Actions[0] = %q, want the bucket line first", res.Actions[0])
	}
}

// --- DeleteArtifactBucket: the tag gate and versioned emptying -------------

func TestDeleteArtifactBucket_AbsentIsNotAnError(t *testing.T) {
	f := &fakeS3{headErr: s3NotFound}
	res, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct)
	if err != nil || res.Outcome != BucketAbsent || len(f.calls) != 1 {
		t.Errorf("res %+v err %v calls %v", res, err, f.calls)
	}
}

func TestDeleteArtifactBucket_UntaggedOrMistaggedIsRetained(t *testing.T) {
	for _, tags := range []map[string]string{nil, {"Other": "x"}, {ManagedTagKey: "someone-else"}} {
		f := &fakeS3{tags: tags}
		res, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct)
		if err != nil || res.Outcome != BucketRetainedUntagged {
			t.Errorf("tags %v: res %+v err %v", tags, res, err)
		}
		if f.called("ListObjectVersions")+f.called("DeleteObjects")+f.called("DeleteBucket") != 0 {
			t.Errorf("tags %v: untagged bucket was touched: %v", tags, f.calls)
		}
	}
}

// HeadBucket refused (another account owns the name, or no permission):
// nothing is read, emptied or deleted, and the teardown says so.
func TestDeleteArtifactBucket_UnconfirmedOwnerDeletesNothing(t *testing.T) {
	f := &fakeS3{headErr: forbidden(), tags: map[string]string{ManagedTagKey: ManagedTagValue}}
	_, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct)
	if err == nil || !strings.Contains(err.Error(), "retained") {
		t.Fatalf("err = %v, want a retained error", err)
	}
	if len(f.calls) != 1 {
		t.Errorf("calls after a refused HeadBucket: %v", f.calls)
	}
}

func ver(key, id string) s3types.ObjectVersion {
	return s3types.ObjectVersion{Key: aws.String(key), VersionId: aws.String(id)}
}

func marker(key, id string) s3types.DeleteMarkerEntry {
	return s3types.DeleteMarkerEntry{Key: aws.String(key), VersionId: aws.String(id)}
}

// Versioned emptying across three pages: every version and delete marker of
// every page is deleted with its VersionId, the markers advance, and the bucket
// goes only after the last page.
func TestDeleteArtifactBucket_EmptiesEveryVersionAcrossPages(t *testing.T) {
	f := &fakeS3{
		tags: map[string]string{ManagedTagKey: ManagedTagValue},
		pages: []*s3.ListObjectVersionsOutput{
			{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("k2"), NextVersionIdMarker: aws.String("v2"),
				Versions: []s3types.ObjectVersion{ver("k1", "v1"), ver("k2", "v2")}},
			{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("k3"), NextVersionIdMarker: aws.String("m1"),
				DeleteMarkers: []s3types.DeleteMarkerEntry{marker("k3", "m1")}},
			{IsTruncated: aws.Bool(false), Versions: []s3types.ObjectVersion{ver("k3", "null")}},
		},
	}
	res, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct)
	if err != nil {
		t.Fatalf("DeleteArtifactBucket: %v", err)
	}
	if res.Outcome != BucketDeleted || res.Versions != 4 || res.Bucket != bDefault {
		t.Errorf("res = %+v, want deleted with 4 versions", res)
	}
	if len(f.lists) != 3 || len(f.deletes) != 3 {
		t.Fatalf("lists %d deletes %d, want 3 each", len(f.lists), len(f.deletes))
	}
	if f.lists[0].KeyMarker != nil || aws.ToString(f.lists[1].KeyMarker) != "k2" || aws.ToString(f.lists[1].VersionIdMarker) != "v2" ||
		aws.ToString(f.lists[2].KeyMarker) != "k3" || aws.ToString(f.lists[2].VersionIdMarker) != "m1" {
		t.Errorf("markers did not advance: %+v %+v %+v", f.lists[0], f.lists[1], f.lists[2])
	}
	var got []string
	for _, d := range f.deletes {
		for _, o := range d.Delete.Objects {
			got = append(got, aws.ToString(o.Key)+"@"+aws.ToString(o.VersionId))
		}
	}
	if strings.Join(got, ",") != "k1@v1,k2@v2,k3@m1,k3@null" {
		t.Errorf("deleted %v", got)
	}
	if last := f.calls[len(f.calls)-1]; last != "DeleteBucket" {
		t.Errorf("last call %q, want DeleteBucket", last)
	}
	f.assertOwnerOnEvery(t, "HeadBucket", "GetBucketTagging", "ListObjectVersions", "DeleteObjects", "DeleteBucket")
}

func TestDeleteArtifactBucket_PerObjectFailureKeepsTheBucket(t *testing.T) {
	f := &fakeS3{
		tags:   map[string]string{ManagedTagKey: ManagedTagValue},
		pages:  []*s3.ListObjectVersionsOutput{{Versions: []s3types.ObjectVersion{ver("k", "v")}}},
		delOut: &s3.DeleteObjectsOutput{Errors: []s3types.Error{{Key: aws.String("k"), VersionId: aws.String("v"), Code: aws.String("AccessDenied")}}},
	}
	if _, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("err = %v", err)
	}
	if f.called("DeleteBucket") != 0 {
		t.Error("bucket deleted after a failed object delete")
	}
}

func TestDeleteArtifactBucket_TruncatedPageWithoutMarkerStops(t *testing.T) {
	f := &fakeS3{
		tags:  map[string]string{ManagedTagKey: ManagedTagValue},
		pages: []*s3.ListObjectVersionsOutput{{IsTruncated: aws.Bool(true)}},
	}
	if _, err := s3Deployer(f).DeleteArtifactBucket(context.Background(), bRegion, bAcct); err == nil {
		t.Fatal("want an error for a truncated page with no marker")
	}
	if f.called("DeleteBucket") != 0 {
		t.Error("bucket deleted")
	}
}

func TestArtifactBucketRegions_PagesAndFilters(t *testing.T) {
	pfx := DefaultBucketName(bAcct, "")
	f := &fakeS3{bucketsBy: [][]string{
		{pfx + "us-east-1", "unrelated"},
		{pfx + "ca-central-1", DefaultBucketName("999999999999", "us-east-1"), pfx},
	}}
	got, err := s3Deployer(f).ArtifactBucketRegions(context.Background(), bAcct)
	if err != nil {
		t.Fatalf("ArtifactBucketRegions: %v", err)
	}
	if strings.Join(got, ",") != "us-east-1,ca-central-1" {
		t.Errorf("regions = %v", got)
	}
	if aws.ToString(f.listBucket[0].Prefix) != pfx || aws.ToString(f.listBucket[1].ContinuationToken) != "tok-1" {
		t.Errorf("ListBuckets inputs: %+v %+v", f.listBucket[0], f.listBucket[1])
	}
}
