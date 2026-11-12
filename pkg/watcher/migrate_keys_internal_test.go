package watcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spore-host/lagotto/pkg/testutil"
)

// Migration tests run against the substrate DynamoDB emulator (no AWS).

const (
	mkAcct  = "123456789012"
	aliceIA = "arn:aws:iam::" + mkAcct + ":user/alice"
	aliceID = "AIDAALICEEXAMPLE"
	bobSess = "arn:aws:sts::" + mkAcct + ":assumed-role/Dev/bob"
	bobID   = "AROADEVEXAMPLE:bob"
	goneARN = "arn:aws:iam::" + mkAcct + ":user/deleted"
)

type mkEnv struct {
	db    *dynamodb.Client
	store *Store
	t     MigrateTables
}

func newMKEnv(t *testing.T) *mkEnv {
	t.Helper()
	env := testutil.SubstrateServer(t)
	st := NewStore(env.AWSConfig, "mk-watches", "mk-history")
	st.SetScheduledTable("mk-scheduled")
	ctx := context.Background()
	if _, err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.EnsureScheduledTable(ctx); err != nil {
		t.Fatal(err)
	}
	return &mkEnv{db: dynamodb.NewFromConfig(env.AWSConfig), store: st,
		t: MigrateTables{Watches: "mk-watches", History: "mk-history", Scheduled: "mk-scheduled"}}
}

func mkResolve(_ context.Context, arn string) (string, error) {
	switch arn {
	case aliceIA:
		return aliceID, nil
	case bobSess:
		return bobID, nil
	}
	return "", fmt.Errorf("NoSuchEntity: %s", arn)
}

func (e *mkEnv) put(t *testing.T, table string, item map[string]types.AttributeValue) {
	t.Helper()
	if _, err := e.db.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String(table), Item: item}); err != nil {
		t.Fatal(err)
	}
}

func legacyWatch(id, userID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"watch_id": avS(id), "user_id": avS(userID), "status": avS("active"),
		"instance_type_pattern": avS("g5.xlarge"), "updated_at": avS("2026-11-01T00:00:00Z"),
	}
}

func (e *mkEnv) seedMixed(t *testing.T) {
	t.Helper()
	e.put(t, e.t.Watches, legacyWatch("w-alice1", aliceIA))
	w := legacyWatch("w-bob1", bobSess)
	w["group_name"] = avS("race")
	e.put(t, e.t.Watches, w)
	e.put(t, e.t.Watches, map[string]types.AttributeValue{
		"watch_id": avS("g-race"), "group_name": avS("race"), "group_owner": avS(bobSess), "group_state": avS("hunting"),
		"group_members": &types.AttributeValueMemberSS{Value: []string{"w-bob1", "w-alice1"}},
	})
	e.put(t, e.t.Watches, legacyWatch("w-orphan", goneARN))
	conflicted := legacyWatch("w-conflict", aliceIA)
	conflicted[OwnerIDAttr] = avS(bobID) // written by someone else before the move
	e.put(t, e.t.Watches, conflicted)
	scoped := legacyWatch(OwnerKey(aliceID, "w-new"), aliceIA)
	scoped[OwnerIDAttr] = avS(aliceID)
	e.put(t, e.t.Watches, scoped)
	for _, at := range []string{"2026-11-02T00:00:00Z", "2026-11-03T00:00:00Z"} {
		e.put(t, e.t.History, map[string]types.AttributeValue{
			"watch_id": avS("w-alice1"), "matched_at": avS(at), "user_id": avS(aliceIA), "action_taken": avS("notified"),
		})
	}
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{
		"schedule_id": avS("sl-alice1"), "user_id": avS(aliceIA), "status": avS("pending"),
		"region": avS("us-east-1"), "schedule_name": avS("lagotto-launch-sl-alice1"),
	})
}

func (e *mkEnv) get(t *testing.T, table string, key map[string]types.AttributeValue) map[string]types.AttributeValue {
	t.Helper()
	out, err := e.db.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String(table), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	return out.Item
}

func wk(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"watch_id": avS(id)}
}

// TestMigrateOwnerKeys_MixedTable: legacy watches, a group, history and a
// scheduled launch move under their owners; scoped records are untouched;
// unresolvable and conflicting owners stay and are reported.
func TestMigrateOwnerKeys_MixedTable(t *testing.T) {
	e := newMKEnv(t)
	e.seedMixed(t)
	ctx := context.Background()

	rep, err := MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Watches != 2 || rep.Groups != 1 || rep.History != 2 || rep.Scheduled != 1 {
		t.Errorf("report = %+v, want 2 watches, 1 group, 2 history, 1 scheduled", rep)
	}
	if len(rep.Skipped) != 2 || !strings.Contains(strings.Join(rep.Skipped, "\n"), "w-orphan") ||
		!strings.Contains(strings.Join(rep.Skipped, "\n"), "w-conflict") {
		t.Errorf("skipped = %v, want w-orphan and w-conflict", rep.Skipped)
	}

	// Each moved record is at its new key, with its old key kept, and gone
	// from the old one.
	a := e.get(t, e.t.Watches, wk(aliceID+"/w-alice1"))
	if a == nil || str(a, "legacy_watch_id") != "w-alice1" || str(a, OwnerIDAttr) != aliceID {
		t.Errorf("alice's watch at its new key = %v", a)
	}
	if e.get(t, e.t.Watches, wk("w-alice1")) != nil {
		t.Error("alice's watch still at its old key")
	}
	g := e.get(t, e.t.Watches, wk(bobID+"/g-race"))
	if g == nil {
		t.Fatal("bob's group not moved")
	}
	// Only bob's own member stays in bob's group: alice's watch is in another scope.
	if mem := g["group_members"].(*types.AttributeValueMemberSS).Value; len(mem) != 1 || mem[0] != bobID+"/w-bob1" {
		t.Errorf("group members = %v, want only bob's own", mem)
	}
	b, err := e.store.GetWatch(ctx, bobID+"/w-bob1")
	if err != nil || b == nil || b.GroupRef() != bobID+"/race" || GroupKey(b.GroupRef()) != bobID+"/g-race" {
		t.Errorf("bob's watch resolves its group to %v (%v)", b, err)
	}
	h, err := e.store.ListMatchHistory(ctx, aliceID+"/w-alice1")
	if err != nil || len(h) != 2 {
		t.Errorf("alice's history under the new key: %d rows (%v)", len(h), err)
	}
	// The scheduled launch: the armed schedule still sends the old id.
	sl, err := e.store.GetScheduledLaunch(ctx, "sl-alice1")
	if err != nil || sl == nil || sl.ScheduleID != aliceID+"/sl-alice1" || sl.LegacyID != "sl-alice1" {
		t.Errorf("old schedule id resolves to %+v (%v)", sl, err)
	}
	// Untouched: the scoped record, and the unmovable ones.
	for _, k := range []string{aliceID + "/w-new", "w-orphan", "w-conflict"} {
		if e.get(t, e.t.Watches, wk(k)) == nil {
			t.Errorf("%s was lost", k)
		}
	}
}

// TestMigrateOwnerKeys_AlreadyMigrated: a second run moves nothing and loses
// nothing.
func TestMigrateOwnerKeys_AlreadyMigrated(t *testing.T) {
	e := newMKEnv(t)
	e.seedMixed(t)
	ctx := context.Background()
	if _, err := MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now()); err != nil {
		t.Fatal(err)
	}
	before := e.count(t)
	rep, err := MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Watches+rep.Groups+rep.History+rep.Scheduled+rep.Fragments != 0 {
		t.Errorf("second run moved %+v", rep)
	}
	if after := e.count(t); after != before {
		t.Errorf("records %d -> %d across a no-op run", before, after)
	}
}

func (e *mkEnv) count(t *testing.T) int {
	t.Helper()
	n := 0
	for _, table := range []string{e.t.Watches, e.t.History, e.t.Scheduled} {
		out, err := e.db.Scan(context.Background(), &dynamodb.ScanInput{TableName: aws.String(table), Select: types.SelectCount})
		if err != nil {
			t.Fatal(err)
		}
		n += int(out.Count)
	}
	return n
}

// failAfter fails every TransactWriteItems after the first n: a run killed
// part-way.
type failAfter struct {
	MigrateAPI
	n int
}

var errKilled = errors.New("killed")

func (f *failAfter) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if f.n <= 0 {
		return nil, errKilled
	}
	f.n--
	return f.MigrateAPI.TransactWriteItems(ctx, in, o...)
}

// TestMigrateOwnerKeys_InterruptedResume: a run stopped after one record
// leaves every record at exactly one of its keys; the next run finishes.
func TestMigrateOwnerKeys_InterruptedResume(t *testing.T) {
	e := newMKEnv(t)
	e.seedMixed(t)
	ctx := context.Background()
	before := e.count(t)

	if _, err := MigrateOwnerKeys(ctx, &failAfter{MigrateAPI: e.db, n: 1}, e.t, mkResolve, time.Now()); !errors.Is(err, errKilled) {
		t.Fatalf("interrupted run: %v, want the injected failure", err)
	}
	if n := e.count(t); n != before {
		t.Fatalf("records %d -> %d after an interrupted run (lost or duplicated)", before, n)
	}
	rep, err := MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Watches+rep.Groups != 2 { // one of the three moved before the kill
		t.Errorf("resume moved %d watches+groups, want the remaining 2", rep.Watches+rep.Groups)
	}
	// One marker per scheduled launch is the only growth.
	if n := e.count(t); n != before+1 {
		t.Errorf("records %d -> %d after resume, want +1 (the scheduled-launch marker)", before, n)
	}
	if e.get(t, e.t.Watches, wk(aliceID+"/w-alice1")) == nil || e.get(t, e.t.Watches, wk(bobID+"/g-race")) == nil {
		t.Error("resume did not finish the move")
	}
}

// TestMigrateOwnerKeys_LeasedAndFragments: a watch a poller holds the lease on
// is left for the next run; what a poller writes to a moved watch's old key
// afterwards is removed as a fragment, never mistaken for a watch.
func TestMigrateOwnerKeys_LeasedAndFragments(t *testing.T) {
	e := newMKEnv(t)
	ctx := context.Background()
	w := legacyWatch("w-busy", aliceIA)
	w["lease_owner"] = avS("poller-1")
	w["lease_expires_at"] = avS(time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	e.put(t, e.t.Watches, w)

	rep, err := MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Watches != 0 || len(rep.Skipped) != 1 || !strings.Contains(rep.Skipped[0], "leased") {
		t.Fatalf("leased watch: %+v", rep)
	}
	// The lease expires; the next run moves it.
	rep, err = MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now().Add(2*time.Hour))
	if err != nil || rep.Watches != 1 {
		t.Fatalf("after the lease: %+v, %v", rep, err)
	}
	// A poller that still held the old record writes to its old key.
	if err := e.store.UpdateLastPolled(ctx, "w-busy"); err != nil {
		t.Fatal(err)
	}
	rep, err = MigrateOwnerKeys(ctx, e.db, e.t, mkResolve, time.Now().Add(2*time.Hour))
	if err != nil || rep.Fragments != 1 || rep.Remaining() {
		t.Fatalf("fragment run: %+v, %v", rep, err)
	}
	if e.get(t, e.t.Watches, wk("w-busy")) != nil {
		t.Error("the fragment was not removed")
	}
	if e.get(t, e.t.Watches, wk(aliceID+"/w-busy")) == nil {
		t.Error("the moved watch was lost")
	}
}

func TestResolveOwnerFromARN(t *testing.T) {
	user := func(_ context.Context, n string) (string, error) { return "AIDA" + strings.ToUpper(n), nil }
	role := func(_ context.Context, n string) (string, error) { return "AROA" + strings.ToUpper(n), nil }
	for arn, want := range map[string]string{
		"arn:aws:iam::123456789012:user/ops/alice":                                  "AIDAALICE",
		"arn:aws:sts::123456789012:assumed-role/Dev/bob":                            "AROADEV:bob",
		"arn:aws:sts::123456789012:federated-user/carol":                            "123456789012:carol",
		"arn:aws:iam::123456789012:root":                                            "123456789012",
		"arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_X_1/dan@example.com": "AROAAWSRESERVEDSSO_X_1:dan@example.com",
	} {
		got, err := ResolveOwnerFromARN(context.Background(), arn, user, role)
		if err != nil || got != want {
			t.Errorf("%s: %q, %v; want %q", arn, got, err, want)
		}
	}
	for _, bad := range []string{"", "nope", "arn:aws:iam::123456789012:role/Dev", "arn:aws:sts::123456789012:assumed-role/Dev"} {
		if _, err := ResolveOwnerFromARN(context.Background(), bad, user, role); err == nil {
			t.Errorf("%q resolved", bad)
		}
	}
}
