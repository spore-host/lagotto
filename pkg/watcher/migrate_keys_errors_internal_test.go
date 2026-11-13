package watcher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// errAPI fails one operation (optionally on one table) and passes the rest to
// the emulator.
type errAPI struct {
	MigrateAPI
	op, table string
}

var errInjected = errors.New("injected")

func (f *errAPI) hit(op string, table *string) bool {
	return f.op == op && (f.table == "" || aws.ToString(table) == f.table)
}

func (f *errAPI) Scan(ctx context.Context, in *dynamodb.ScanInput, o ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if f.hit("Scan", in.TableName) {
		return nil, errInjected
	}
	return f.MigrateAPI.Scan(ctx, in, o...)
}

func (f *errAPI) GetItem(ctx context.Context, in *dynamodb.GetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.hit("GetItem", in.TableName) {
		return nil, errInjected
	}
	if f.hit("GetItemEmpty", in.TableName) {
		return &dynamodb.GetItemOutput{}, nil
	}
	return f.MigrateAPI.GetItem(ctx, in, o...)
}

func (f *errAPI) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, o ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.hit("DeleteItem", in.TableName) {
		return nil, errInjected
	}
	return f.MigrateAPI.DeleteItem(ctx, in, o...)
}

func (f *errAPI) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, o ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	t := in.TransactItems[0].Put.TableName
	if f.hit("TransactWriteItems", t) {
		return nil, errInjected
	}
	return f.MigrateAPI.TransactWriteItems(ctx, in, o...)
}

// Every failure stops the run with the error and loses nothing.
func TestMigrateOwnerKeys_Errors(t *testing.T) {
	for _, c := range []struct{ op, table string }{
		{"Scan", "mk-watches"}, {"Scan", "mk-history"}, {"Scan", "mk-scheduled"},
		{"TransactWriteItems", "mk-watches"}, {"TransactWriteItems", "mk-history"}, {"TransactWriteItems", "mk-scheduled"},
		{"GetItem", "mk-watches"}, {"GetItemEmpty", "mk-watches"}, {"GetItem", "mk-history"}, {"GetItem", "mk-scheduled"},
	} {
		t.Run(c.op+"/"+c.table, func(t *testing.T) {
			e := newMKEnv(t)
			e.seedMixed(t)
			before := e.count(t)
			_, err := MigrateOwnerKeys(context.Background(), &errAPI{MigrateAPI: e.db, op: c.op, table: c.table}, e.t, mkResolve, time.Now())
			if err == nil {
				t.Fatal("the failure was not reported")
			}
			if n := e.count(t); n != before && (c.table != "mk-scheduled" || n != before+1) {
				t.Errorf("records %d -> %d", before, n)
			}
		})
	}
}

// A table that does not exist is nothing to migrate.
func TestMigrateOwnerKeys_NoTables(t *testing.T) {
	e := newMKEnv(t)
	rep, err := MigrateOwnerKeys(context.Background(), e.db, MigrateTables{Watches: "absent-w", History: "absent-h", Scheduled: "absent-s"}, mkResolve, time.Now())
	if err != nil || rep.Remaining() {
		t.Errorf("absent tables: %+v, %v", rep, err)
	}
}

// Owners that cannot be used, conflicts at the new key, and leftovers at a
// moved watch's old key are all reported, never moved over something.
func TestMigrateOwnerKeys_Refusals(t *testing.T) {
	e := newMKEnv(t)
	ctx := context.Background()
	slash := func(_ context.Context, arn string) (string, error) {
		if arn == aliceIA {
			return "bad/owner", nil
		}
		return mkResolve(ctx, arn)
	}
	e.put(t, e.t.Watches, legacyWatch("w-a", aliceIA))
	e.put(t, e.t.Watches, map[string]types.AttributeValue{"watch_id": avS("w-nocreator"), "status": avS("active"), "instance_type_pattern": avS("x")})
	e.put(t, e.t.History, map[string]types.AttributeValue{"watch_id": avS("w-gone"), "matched_at": avS("2026-11-01T00:00:00Z"), "user_id": avS(goneARN)})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS("sl-gone"), "user_id": avS(goneARN), "status": avS("pending")})
	// Bob's watch and history row whose new keys are already taken.
	e.put(t, e.t.Watches, legacyWatch("w-b", bobSess))
	taken := legacyWatch(OwnerKey(bobID, "w-b"), bobSess)
	e.put(t, e.t.Watches, taken)
	e.put(t, e.t.History, map[string]types.AttributeValue{"watch_id": avS("w-b2"), "matched_at": avS("2026-11-01T00:00:00Z"), "user_id": avS(bobSess)})
	e.put(t, e.t.History, map[string]types.AttributeValue{"watch_id": avS(OwnerKey(bobID, "w-b2")), "matched_at": avS("2026-11-01T00:00:00Z")})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS("sl-b"), "user_id": avS(bobSess), "status": avS("pending")})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS(OwnerKey(bobID, "sl-b")), "status": avS("pending")})

	rep, err := MigrateOwnerKeys(ctx, e.db, e.t, slash, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(rep.Skipped, "\n")
	for _, want := range []string{"w-a: owner unknown (resolved to an unusable owner id", "w-nocreator: owner unknown (it records no creator)",
		"w-gone @", "sl-gone: owner unknown", "w-b: changed or leased", "w-b2 @", "sl-b: changed"} {
		if !strings.Contains(all, want) {
			t.Errorf("skipped lacks %q:\n%s", want, all)
		}
	}

	// A whole record and a group at a moved watch's old key are left alone.
	e2 := newMKEnv(t)
	moved := legacyWatch(OwnerKey(aliceID, "w-x"), aliceIA)
	moved["legacy_watch_id"] = avS("w-x")
	e2.put(t, e2.t.Watches, moved)
	e2.put(t, e2.t.Watches, legacyWatch("w-x", aliceIA))
	moved2 := legacyWatch(OwnerKey(aliceID, "w-y"), aliceIA)
	moved2["legacy_watch_id"] = avS("w-y")
	e2.put(t, e2.t.Watches, moved2)
	e2.put(t, e2.t.Watches, map[string]types.AttributeValue{"watch_id": avS("w-y"), "group_name": avS("g")})
	rep, err = MigrateOwnerKeys(ctx, e2.db, e2.t, mkResolve, time.Now())
	if err != nil || len(rep.Skipped) != 2 || rep.Fragments != 0 {
		t.Errorf("leftovers: %+v, %v", rep, err)
	}
	// A fragment that cannot be deleted stops the run.
	e3 := newMKEnv(t)
	e3.put(t, e3.t.Watches, moved)
	e3.put(t, e3.t.Watches, map[string]types.AttributeValue{"watch_id": avS("w-x"), "last_polled_at": avS("now")})
	if _, err := MigrateOwnerKeys(ctx, &errAPI{MigrateAPI: e3.db, op: "DeleteItem"}, e3.t, mkResolve, time.Now()); err == nil {
		t.Error("a failed fragment delete was not reported")
	}
}
