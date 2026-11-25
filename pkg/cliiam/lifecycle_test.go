package cliiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/iamtags"

	"github.com/spore-host/lagotto/pkg/managedtags"
)

func doc(t *testing.T, buckets ...string) string {
	t.Helper()
	d, err := PolicyDocument(testAccount, buckets)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestEnsurePolicyCreatesATaggedPolicy(t *testing.T) {
	f := &fakeIAM{}
	res, err := EnsurePolicy(context.Background(), f, testAccount, doc(t))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.Updated || res.ARN != PolicyARN(testAccount) {
		t.Errorf("outcome = %+v", res)
	}
	if !hasManagedTag(f.tags) {
		t.Errorf("created without the %s=%s tag: %v", ManagedTagKey, ManagedTagValue, f.tags)
	}
	if strings.Join(f.calls, ",") != "CreatePolicy" {
		t.Errorf("calls = %v", f.calls)
	}
}

// TestEnsurePolicyLeavesAnIdenticalPolicyAlone: a re-run of setup must not
// burn one of IAM's five versions per run.
func TestEnsurePolicyLeavesAnIdenticalPolicyAlone(t *testing.T) {
	f := withPolicy(2, doc(t))
	res, err := EnsurePolicy(context.Background(), f, testAccount, doc(t))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Updated || len(f.calls) != 0 {
		t.Errorf("an identical policy was rewritten: %+v, calls %v", res, f.calls)
	}
}

func TestEnsurePolicyWritesANewDefaultVersion(t *testing.T) {
	f := withPolicy(2, doc(t))
	res, err := EnsurePolicy(context.Background(), f, testAccount, doc(t, "new-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Updated || res.DeletedVersion != "" {
		t.Errorf("outcome = %+v", res)
	}
	if strings.Join(f.calls, ",") != "CreatePolicyVersion" {
		t.Errorf("calls = %v, want only CreatePolicyVersion (2 < 5 versions)", f.calls)
	}
	if d := f.defaultVersion(); d == nil || aws.ToString(d.VersionId) != "v3" {
		t.Errorf("the new version is not the default: %+v", d)
	}
}

// TestEnsurePolicyRotatesAtFiveVersions: CreatePolicyVersion fails at IAM's
// limit, so the oldest non-default version goes first, chosen by date, never
// the default.
func TestEnsurePolicyRotatesAtFiveVersions(t *testing.T) {
	f := withPolicy(5, doc(t))
	// List order is not age order: make v3 the oldest.
	f.versions[2].CreateDate = aws.Time(f.versions[0].CreateDate.AddDate(-1, 0, 0))
	res, err := EnsurePolicy(context.Background(), f, testAccount, doc(t, "new-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	if res.DeletedVersion != "v3" || !res.Updated {
		t.Errorf("outcome = %+v, want v3 deleted", res)
	}
	if strings.Join(f.calls, ",") != "DeletePolicyVersion v3,CreatePolicyVersion" {
		t.Errorf("calls = %v", f.calls)
	}
	if len(f.versions) != 5 {
		t.Errorf("%d versions after rotation, want 5", len(f.versions))
	}

	// The default is never the one deleted, even when it is the oldest.
	f = withPolicy(5, doc(t))
	f.versions[4].CreateDate = aws.Time(f.versions[0].CreateDate.AddDate(-1, 0, 0))
	res, err = EnsurePolicy(context.Background(), f, testAccount, doc(t, "new-bucket"))
	if err != nil || res.DeletedVersion != "v1" {
		t.Errorf("deleted %q (err %v), want v1: the oldest NON-default", res.DeletedVersion, err)
	}
}

func TestEnsurePolicyRefusesAnUnmanagedPolicy(t *testing.T) {
	f := withPolicy(1, doc(t))
	f.tags = nil
	if _, err := EnsurePolicy(context.Background(), f, testAccount, doc(t, "x-bucket")); !errors.Is(err, ErrUnmanagedPolicy) {
		t.Errorf("err = %v, want ErrUnmanagedPolicy", err)
	}
	if len(f.calls) != 0 {
		t.Errorf("an unmanaged policy was written: %v", f.calls)
	}
}

func TestEnsurePolicyPropagatesErrors(t *testing.T) {
	f := &fakeIAM{errs: map[string]error{"GetPolicy": apiErr("AccessDenied")}}
	if _, err := EnsurePolicy(context.Background(), f, testAccount, doc(t)); err == nil || len(f.calls) != 0 {
		t.Errorf("a GetPolicy failure was not fatal (err %v, calls %v)", err, f.calls)
	}
	f = withPolicy(5, doc(t))
	f.errs = map[string]error{"DeletePolicyVersion": apiErr("AccessDenied")}
	if _, err := EnsurePolicy(context.Background(), f, testAccount, doc(t, "b-bucket")); err == nil {
		t.Error("a failed rotation was not reported")
	}
	for _, c := range f.calls {
		if c == "CreatePolicyVersion" {
			t.Error("CreatePolicyVersion was attempted after the rotation failed")
		}
	}
}

// TestDeletePolicyRefusesWhileAttached (lagotto#351): a policy attached to
// anything is left in place, untouched, and says to what; never detached.
// Unattached, its non-default versions go, then the policy (IAM refuses the
// other order; the fake enforces it).
func TestDeletePolicyRefusesWhileAttached(t *testing.T) {
	f := withPolicy(3, doc(t))
	f.users, f.groups, f.roles = []string{"alice"}, []string{"researchers"}, []string{"ci"}
	res, err := DeletePolicy(context.Background(), f, testAccount)
	if !errors.Is(err, ErrAttached) || res.Deleted || strings.Join(res.Attached, ",") != "user/alice,group/researchers,role/ci" || len(f.calls) != 0 {
		t.Errorf("attached: %+v %v calls %v", res, err, f.calls)
	}
	if !f.exists {
		t.Error("the policy was deleted")
	}
	f.users, f.groups, f.roles = nil, nil, nil
	res, err = DeletePolicy(context.Background(), f, testAccount)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(f.calls, ","), "DeletePolicyVersion v1,DeletePolicyVersion v2,DeletePolicy"; got != want {
		t.Errorf("calls:\n got %s\nwant %s", got, want)
	}
	if !res.Deleted || strings.Join(res.DeletedVersions, ",") != "v1,v2" || f.exists {
		t.Errorf("result = %+v", res)
	}
}

func TestDeletePolicyIsIdempotent(t *testing.T) {
	res, err := DeletePolicy(context.Background(), &fakeIAM{}, testAccount)
	if err != nil || !res.Absent || res.Deleted {
		t.Errorf("absent policy: %+v, %v", res, err)
	}
}

func TestDeletePolicyLeavesAnUnmanagedPolicy(t *testing.T) {
	f := withPolicy(1, doc(t))
	f.tags = []iamtypes.Tag{{Key: aws.String("owner"), Value: aws.String("someone-else")}}
	f.users = []string{"alice"}
	res, err := DeletePolicy(context.Background(), f, testAccount)
	if err != nil || !res.Unmanaged || res.Deleted || len(f.calls) != 0 {
		t.Errorf("an unmanaged policy was touched: %+v, %v, calls %v", res, err, f.calls)
	}
}

func TestDeletePolicyStopsBeforeDeletingOnAListFailure(t *testing.T) {
	f := withPolicy(2, doc(t))
	f.errs = map[string]error{"ListEntitiesForPolicy": apiErr("AccessDenied")}
	if _, err := DeletePolicy(context.Background(), f, testAccount); err == nil {
		t.Fatal("no error")
	}
	if len(f.calls) != 0 {
		t.Errorf("deleted after failing to list attachments: %v", f.calls)
	}
}

// A policy an older lagotto created gains the spore.host tags (#324), with no
// new version when the document is unchanged; a TagPolicy failure is an error.
func TestEnsurePolicy_BackfillsTags(t *testing.T) {
	doc, err := PolicyDocument(testAccount, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := withPolicy(1, doc)
	out, err := EnsurePolicy(context.Background(), f, testAccount, doc, managedtags.Stamp{Owner: "AIDAADMIN"})
	if err != nil || out.Created || out.Updated {
		t.Fatalf("EnsurePolicy = %+v, %v", out, err)
	}
	if len(f.tagCalls) != 1 {
		t.Fatalf("TagPolicy calls = %d, want 1", len(f.tagCalls))
	}
	got := iamtags.ToMap(f.tags)
	if !tags.IsManagedBy(got, tags.ToolLagotto) || got[ManagedTagKey] != ManagedTagValue || got[tags.Owner] != "AIDAADMIN" {
		t.Errorf("policy tags = %v", got)
	}
	if _, err := EnsurePolicy(context.Background(), f, testAccount, doc); err != nil || len(f.tagCalls) != 1 {
		t.Errorf("re-run: err %v, TagPolicy calls %d; want no new call", err, len(f.tagCalls))
	}

	f = withPolicy(1, doc)
	f.errs = map[string]error{"TagPolicy": errors.New("AccessDenied")}
	if _, err := EnsurePolicy(context.Background(), f, testAccount, doc); err == nil || !strings.Contains(err.Error(), "tag policy") {
		t.Errorf("TagPolicy failure = %v", err)
	}
}
