package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	ownA    = "AIDAOWNERA"
	ownAARN = "arn:aws:iam::123456789012:user/a"
)

func avs(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

// TestListCallerWatchesAndHistory: list and history show the caller's
// owner-scoped watches and its legacy ones, and nothing of another owner's
// even when that record names the caller.
func TestListCallerWatchesAndHistory(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	st := watcher.NewStore(env.AWSConfig, "lc-watches", "lc-history")
	if _, err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	db := dynamodb.NewFromConfig(env.AWSConfig)
	put := func(table string, item map[string]types.AttributeValue) {
		if _, err := db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: item}); err != nil {
			t.Fatal(err)
		}
	}
	w := func(key, owner, user string) map[string]types.AttributeValue {
		it := map[string]types.AttributeValue{"watch_id": avs(key), "user_id": avs(user), "status": avs("active"),
			"instance_type_pattern": avs("g5.xlarge")}
		if owner != "" {
			it["owner_id"] = avs(owner)
		}
		return it
	}
	put("lc-watches", w(ownA+"/w-1", ownA, ownAARN))
	put("lc-watches", w("AIDAOTHER/w-2", ownA, ownAARN)) // planted under another prefix
	put("lc-watches", w("w-3", "", ownAARN))             // legacy, the caller's
	put("lc-watches", w("w-4", "", "arn:aws:iam::123456789012:user/b"))
	for _, h := range []struct{ key, at string }{{ownA + "/w-1", "2026-11-02T00:00:00Z"}, {"w-3", "2026-11-03T00:00:00Z"}, {"AIDAOTHER/w-2", "2026-11-04T00:00:00Z"}} {
		put("lc-history", map[string]types.AttributeValue{"watch_id": avs(h.key), "matched_at": avs(h.at), "user_id": avs(ownAARN)})
	}
	id := &sts.GetCallerIdentityOutput{Arn: aws.String(ownAARN), UserId: aws.String(ownA)}
	ws, err := listCallerWatches(ctx, st, id, "")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, x := range ws {
		keys = append(keys, x.WatchID)
	}
	if strings.Join(keys, ",") != ownA+"/w-1,w-3" {
		t.Errorf("listCallerWatches = %v", keys)
	}
	hist, err := callerHistory(ctx, st, id, ws)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 || hist[0].WatchID != "w-3" || hist[1].WatchID != ownA+"/w-1" {
		t.Errorf("callerHistory = %+v", hist)
	}
	// getWatchOwned resolves the short id under the caller's prefix.
	got, err := getWatchOwned(ctx, st, fullIdentity{ownAARN, ownA}, "w-1")
	if err != nil || got.WatchID != ownA+"/w-1" {
		t.Errorf("getWatchOwned(w-1) = %v, %v", got, err)
	}
	if _, _, err := getWatchOwnedVersioned(ctx, st, fullIdentity{ownAARN, ownA}, "w-2"); err == nil {
		t.Error("another owner's watch resolved")
	}
}

type apiErr struct{ code, msg string }

func (e apiErr) Error() string                 { return e.code + ": " + e.msg }
func (e apiErr) ErrorCode() string             { return e.code }
func (e apiErr) ErrorMessage() string          { return e.msg }
func (e apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func TestErrorClassifiers(t *testing.T) {
	if !isAccessDenied(apiErr{"AccessDeniedException", "no"}) || isAccessDenied(errors.New("x")) {
		t.Error("isAccessDenied")
	}
	if !isMissingIndex(apiErr{"ValidationException", "The table does not have the specified index"}) || isMissingIndex(apiErr{"ValidationException", "bad key"}) {
		t.Error("isMissingIndex")
	}
	// A denied read of a key is "not found", never the error.
	_, _, err := resolveOwnedWatch(context.Background(), fullIdentity{ownAARN, ownA}, "w-1", func(string) (*watcher.Watch, string, error) {
		return nil, "", apiErr{"AccessDeniedException", "denied"}
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("denied read: %v", err)
	}
	_, _, err = resolveOwnedWatch(context.Background(), fullIdentity{ownAARN, ownA}, "w-1", func(string) (*watcher.Watch, string, error) {
		return nil, "", errors.New("throttled")
	})
	if err == nil || strings.Contains(err.Error(), "not found") {
		t.Errorf("other read errors pass through: %v", err)
	}
}

func TestNotifyGCDeniedSaysWhy(t *testing.T) {
	err := notifyGC(context.Background(), &bytes.Buffer{}, nil, func(context.Context) ([]watcher.Watch, error) {
		return nil, apiErr{"AccessDeniedException", "denied"}
	}, false, "")
	if err == nil || !strings.Contains(err.Error(), "administrator credentials") {
		t.Errorf("notify-gc denied: %v", err)
	}
}

// The real IAM-backed seams, against the emulator's IAM.
func TestInstanceRoleSeamsAgainstEmulator(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	td := newInstanceRoleTeardown(env.AWSConfig)
	if roles, err := td.AuthorizedRoles(ctx); err != nil || len(roles) != 0 {
		t.Errorf("AuthorizedRoles with no poller role = %v, %v", roles, err)
	}
	if gone, err := td.DeleteInstanceRole(ctx); err != nil || gone {
		t.Errorf("DeleteInstanceRole with none = %v, %v", gone, err)
	}
	if gone, err := td.DeleteManagedProfile(ctx, "x"); err != nil || gone {
		t.Errorf("DeleteManagedProfile with none = %v, %v", gone, err)
	}
	ps := newInstanceProfileSetup(env.AWSConfig)
	if gone, err := ps.DeleteManagedProfile(ctx, "x"); err != nil || gone {
		t.Errorf("setup DeleteManagedProfile = %v, %v", gone, err)
	}
	if err := ps.EnsureInstanceProfile(ctx, runtimeiam.InstanceRoleName); err != nil {
		t.Skipf("the emulator cannot run spawn's profile setup: %v", err)
	}
	if gone, err := td.DeleteInstanceRole(ctx); err != nil || !gone {
		t.Errorf("DeleteInstanceRole after setup = %v, %v", gone, err)
	}
	if out, err := iam.NewFromConfig(env.AWSConfig).GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(runtimeiam.InstanceRoleName)}); err == nil {
		t.Errorf("role still exists: %v", out)
	}
}

type flipIndex struct {
	ownerKeysDynamo
	calls int
}

func (f *flipIndex) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.calls++
	st := types.IndexStatusCreating
	if f.calls > 2 {
		st = types.IndexStatusActive
	}
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{GlobalSecondaryIndexes: []types.GlobalSecondaryIndexDescription{
		{IndexName: aws.String(watcher.OwnerIndexName), IndexStatus: st}}}}, nil
}

func TestWaitOwnerIndexAndResolver(t *testing.T) {
	old := ownerIndexPoll
	ownerIndexPoll = time.Millisecond
	t.Cleanup(func() { ownerIndexPoll = old })
	f := &flipIndex{}
	var progress strings.Builder
	if err := waitOwnerIndex(context.Background(), f, "t", &progress); err != nil || f.calls < 3 {
		t.Errorf("waitOwnerIndex = %v after %d calls", err, f.calls)
	}
	// #309: the wait says what it is waiting on, every poll, and when it ends.
	if got := progress.String(); !strings.Contains(got, watcher.OwnerIndexName+": CREATING") || !strings.Contains(got, "is ACTIVE") {
		t.Errorf("progress = %q, want a CREATING line and the ACTIVE line", got)
	}
	oldWait := ownerIndexWait
	ownerIndexWait = 5 * time.Millisecond
	t.Cleanup(func() { ownerIndexWait = oldWait })
	if err := waitOwnerIndex(context.Background(), &flipIndex{calls: -1 << 30}, "t", io.Discard); err == nil || !strings.Contains(err.Error(), "re-run") {
		t.Errorf("timeout: %v", err)
	}
	r := resolverFor(fakePrincipals{})
	if id, err := r(context.Background(), "arn:aws:iam::123456789012:user/alice"); err != nil || id != "AIDAALICE" {
		t.Errorf("user: %q %v", id, err)
	}
	if _, err := r(context.Background(), "arn:aws:iam::123456789012:user/nobody"); err == nil {
		t.Error("unknown user resolved")
	}
	if _, err := r(context.Background(), "arn:aws:sts::123456789012:assumed-role/Dev/x"); err == nil {
		t.Error("role lookup error swallowed")
	}
}

func sageMakerWatch(role string) *watcher.Watch {
	return &watcher.Watch{WatchID: "w-sm", Service: watcher.ServiceSageMaker, SageMakerJobJSON: []byte(`{"RoleArn":"` + role + `"}`)}
}

// The create-time check covers a SageMaker job's RoleArn: refused unless the
// deployed poller policy lets it pass that role to SageMaker.
func TestCheckSageMakerRoleAuthorized(t *testing.T) {
	ctx := context.Background()
	role := "arn:aws:iam::" + irTestAccount + ":role/service-role/sm-exec"
	_, err := checkInstanceRoleAuthorized(ctx, &fakeRoleIAM{policy: irStockPolicy(t)}, &bytes.Buffer{}, sageMakerWatch(role), irTestAccount)
	if err == nil || !strings.Contains(err.Error(), "lagotto setup --sagemaker-role "+role) {
		t.Errorf("unauthorized role: %v", err)
	}
	doc, _ := runtimeiam.PolicyDocumentFor(irTestAccount, runtimeiam.Authorizations{SageMakerRoles: []string{role}})
	res, err := checkInstanceRoleAuthorized(ctx, &fakeRoleIAM{policy: doc}, &bytes.Buffer{}, sageMakerWatch(role), irTestAccount)
	if err != nil || res == nil || !res.Authorized {
		t.Errorf("authorized role: %+v, %v", res, err)
	}
	if watchSageMakerRole(sageMakerWatch("not-an-arn")) != "" || watchSageMakerRole(&watcher.Watch{}) != "" {
		t.Error("watchSageMakerRole accepted a bad spec")
	}
}

func TestResolveAuthorizedSageMakerRoles(t *testing.T) {
	ctx := context.Background()
	role := "arn:aws:iam::123456789012:role/sm"
	oldA, oldR := setupSageMakerRoles, setupRevokeSageMaker
	t.Cleanup(func() { setupSageMakerRoles, setupRevokeSageMaker = oldA, oldR })

	doc, _ := runtimeiam.PolicyDocumentFor("123456789012", runtimeiam.Authorizations{SageMakerRoles: []string{role}})
	setupSageMakerRoles, setupRevokeSageMaker = nil, nil
	if got, err := resolveAuthorizedSageMakerRoles(ctx, &fakeRoleIAM{policy: doc}, "123456789012", &bytes.Buffer{}); err != nil || len(got) != 1 {
		t.Errorf("carried forward: %v, %v", got, err)
	}
	var out bytes.Buffer
	setupRevokeSageMaker = []string{role, "arn:aws:iam::123456789012:role/never"}
	if got, err := resolveAuthorizedSageMakerRoles(ctx, &fakeRoleIAM{policy: doc}, "123456789012", &out); err != nil || len(got) != 0 ||
		!strings.Contains(out.String(), "nothing to revoke") {
		t.Errorf("revoked: %v, %v, %q", got, err, out.String())
	}
	setupRevokeSageMaker = nil
	if _, err := resolveAuthorizedSageMakerRoles(ctx, &fakeRoleIAM{getErr: errors.New("AccessDenied")}, "123456789012", &bytes.Buffer{}); err == nil {
		t.Error("an unreadable policy was treated as no roles")
	}
	setupSageMakerRoles = []string{"arn:aws:iam::123456789012:role/*"}
	if _, err := resolveAuthorizedSageMakerRoles(ctx, &fakeRoleIAM{policy: doc}, "123456789012", &bytes.Buffer{}); err == nil {
		t.Error("a wildcard role was accepted")
	}
}

// indexBuilding is the emulator, except that the owner index starts absent
// and builds over two polls.
type indexBuilding struct {
	ownerKeysDynamo
	describes int
	updated   bool
	failDesc  bool
}

func (f *indexBuilding) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, o ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if f.failDesc {
		return nil, errors.New("AccessDenied")
	}
	f.describes++
	td := &types.TableDescription{}
	if f.updated {
		st := types.IndexStatusCreating
		if f.describes > 2 {
			st = types.IndexStatusActive
		}
		td.GlobalSecondaryIndexes = []types.GlobalSecondaryIndexDescription{{IndexName: aws.String(watcher.OwnerIndexName), IndexStatus: st}}
	}
	return &dynamodb.DescribeTableOutput{Table: td}, nil
}

func (f *indexBuilding) UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	f.updated = true
	return &dynamodb.UpdateTableOutput{}, nil
}

func TestEnsureOwnerKeys_BuildsIndexFirst(t *testing.T) {
	old := ownerIndexPoll
	ownerIndexPoll = time.Millisecond
	t.Cleanup(func() { ownerIndexPoll = old })
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	st := watcher.NewStore(env.AWSConfig, "bi-watches", "bi-history")
	if _, err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	db := &indexBuilding{ownerKeysDynamo: dynamodb.NewFromConfig(env.AWSConfig)}
	var out bytes.Buffer
	if _, err := ensureOwnerKeys(ctx, db, fakePrincipals{}, watcher.MigrateTables{Watches: "bi-watches", History: "bi-history"}, &out); err != nil {
		t.Fatal(err)
	}
	if !db.updated || !strings.Contains(out.String(), "Building "+watcher.OwnerIndexName) {
		t.Errorf("index not built first: %q", out.String())
	}
	if _, err := ensureOwnerKeys(ctx, &indexBuilding{ownerKeysDynamo: db.ownerKeysDynamo, failDesc: true}, fakePrincipals{},
		watcher.MigrateTables{Watches: "bi-watches"}, &bytes.Buffer{}); err == nil {
		t.Error("an unreadable table did not stop setup")
	}
	if _, err := ensureOwnerKeys(ctx, db, fakePrincipals{}, watcher.MigrateTables{Watches: "bi-watches", History: "absent-but-ok", Scheduled: "x"}, &bytes.Buffer{}); err != nil {
		t.Errorf("absent optional tables: %v", err)
	}
}

type failingInstanceRoleTeardown struct{ op string }

func (f failingInstanceRoleTeardown) AuthorizedRoles(context.Context) ([]string, error) {
	if f.op == "list" {
		return nil, errors.New("AccessDenied")
	}
	return []string{"gpu-role"}, nil
}

func (f failingInstanceRoleTeardown) DeleteInstanceRole(context.Context) (bool, error) {
	if f.op == "role" {
		return false, errors.New("DeleteConflict")
	}
	return false, nil
}

func (f failingInstanceRoleTeardown) DeleteManagedProfile(context.Context, string) (bool, error) {
	if f.op == "profile" {
		return false, errors.New("AccessDenied")
	}
	return false, nil
}

func TestDeleteInstanceRoles_Errors(t *testing.T) {
	for _, op := range []string{"list", "role", "profile"} {
		if err := deleteInstanceRoles(context.Background(), failingInstanceRoleTeardown{op}, &bytes.Buffer{}); err == nil {
			t.Errorf("%s failure was not reported", op)
		}
	}
	if err := deleteInstanceRoles(context.Background(), failingInstanceRoleTeardown{}, &bytes.Buffer{}); err != nil {
		t.Errorf("nothing to delete: %v", err)
	}
}
