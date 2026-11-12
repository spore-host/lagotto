package watcher

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	scopeA = "AIDAOWNERAEXAMPLE"
	scopeB = "AIDAOWNERBEXAMPLE"
)

// TestGroup_PollerStaysInTheOwnersScope: a group resolves only inside its
// member's owner scope, and a group item that lists a watch of another owner
// cannot make the poller cancel it. Two owners' groups of the same name are
// different groups.
func TestGroup_PollerStaysInTheOwnersScope(t *testing.T) {
	h := newLandHarness(t)
	withGroupNotes(h)
	a := memberWatch(t, OwnerKey(scopeA, "w-a"), "gpu")
	b := memberWatch(t, OwnerKey(scopeB, "w-b"), "gpu")
	h.join(t, a)
	h.join(t, b)
	if a.GroupRef() == b.GroupRef() || GroupKey(a.GroupRef()) != scopeA+"/g-gpu" {
		t.Fatalf("group refs %q / %q", a.GroupRef(), b.GroupRef())
	}
	// A's group item names B's watch as a member (written by A, under A's key).
	if _, err := h.store.client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                 &h.store.watchesTable,
		Key:                       map[string]types.AttributeValue{"watch_id": avS(GroupKey(a.GroupRef()))},
		UpdateExpression:          aws.String("ADD group_members :m"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":m": &types.AttributeValueMemberSS{Value: []string{b.WatchID}}},
	}); err != nil {
		t.Fatal(err)
	}
	if ms, err := h.store.GroupMembers(context.Background(), h.group(t, a.GroupRef())); err != nil || len(ms) != 1 || ms[0].WatchID != a.WatchID {
		t.Errorf("GroupMembers = %v, %v; want only %s", ms, err, a.WatchID)
	}

	// A's member lands and wins A's group.
	h.beginLandingAs(t, a, "i-a", "")
	if _, err := h.store.ReserveGroupSlot(context.Background(), a.GroupRef(), a.WatchID, h.poller.clock()); err != nil {
		t.Fatal(err)
	}
	h.describe = func(string) (landingCheck, error) { return landingCheck{State: "running"}, nil }
	h.cycle(t)

	if g := h.group(t, a.GroupRef()); g.State != GroupLanded {
		t.Fatalf("A's group = %+v, want landed", g)
	}
	if bw := h.get(t, b.WatchID); bw.Status != StatusActive {
		t.Errorf("B's watch = %s (%q): a group of another owner cancelled it", bw.Status, bw.CancelledReason)
	}
	if g := h.group(t, b.GroupRef()); g.State != GroupHunting {
		t.Errorf("B's group = %s, want still hunting", g.State)
	}
}

// TestWatchFilter_OwnerUsesTheKey: a local `poll --mine` services an
// owner-scoped watch only when its key is under the caller's id, whatever its
// user_id attribute claims.
func TestWatchFilter_OwnerUsesTheKey(t *testing.T) {
	f := &WatchFilter{Owner: "arn:aws:iam::123456789012:user/a", OwnerID: scopeA}
	mine := &Watch{WatchID: OwnerKey(scopeA, "w-1"), UserID: "arn:aws:iam::123456789012:user/a"}
	planted := &Watch{WatchID: OwnerKey(scopeB, "w-2"), UserID: "arn:aws:iam::123456789012:user/a"}
	legacy := &Watch{WatchID: "w-3", UserID: "arn:aws:iam::123456789012:user/a"}
	if !f.Matches(mine) || f.Matches(planted) || !f.Matches(legacy) {
		t.Errorf("mine %v planted %v legacy %v", f.Matches(mine), f.Matches(planted), f.Matches(legacy))
	}
}

// TestListWatchesByOwner: the owner index lists only records under the
// owner's key prefix; a record of another owner that sets owner_id to the
// caller's is not listed (and would be refused by IAM if it were).
func TestListWatchesByOwner(t *testing.T) {
	e := newMKEnv(t)
	ctx := context.Background()
	mine := legacyWatch(OwnerKey(scopeA, "w-1"), "arn:a")
	mine[OwnerIDAttr] = avS(scopeA)
	planted := legacyWatch(OwnerKey(scopeB, "w-2"), "arn:b")
	planted[OwnerIDAttr] = avS(scopeA)
	done := legacyWatch(OwnerKey(scopeA, "w-3"), "arn:a")
	done[OwnerIDAttr] = avS(scopeA)
	done["status"] = avS(string(StatusCancelled))
	for _, it := range []map[string]types.AttributeValue{mine, planted, done} {
		e.put(t, e.t.Watches, it)
	}
	all, err := e.store.ListWatchesByOwner(ctx, scopeA, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("ListWatchesByOwner = %d watches, %v; want 2", len(all), err)
	}
	active, err := e.store.ListWatchesByOwner(ctx, scopeA, StatusActive)
	if err != nil || len(active) != 1 || active[0].WatchID != OwnerKey(scopeA, "w-1") {
		t.Errorf("active = %v, %v", active, err)
	}
}

// batchFlaky returns some keys unprocessed once, then serves them; or fails.
type batchFlaky struct {
	dynamoAPI
	unprocessedOnce bool
	fail            bool
}

func (b *batchFlaky) BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, o ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if b.fail {
		return nil, errInjected
	}
	if b.unprocessedOnce {
		b.unprocessedOnce = false
		return &dynamodb.BatchGetItemOutput{UnprocessedKeys: in.RequestItems}, nil
	}
	return b.dynamoAPI.BatchGetItem(ctx, in, o...)
}

func TestListWatchesByOwner_BatchPaths(t *testing.T) {
	e := newMKEnv(t)
	ctx := context.Background()
	it := legacyWatch(OwnerKey(scopeA, "w-1"), "arn:a")
	it[OwnerIDAttr] = avS(scopeA)
	e.put(t, e.t.Watches, it)
	if _, err := e.store.ListWatchesByOwner(ctx, "", ""); err == nil {
		t.Error("no owner id was accepted")
	}
	real := e.store.client
	e.store.client = &batchFlaky{dynamoAPI: real, unprocessedOnce: true}
	if ws, err := e.store.ListWatchesByOwner(ctx, scopeA, ""); err != nil || len(ws) != 1 {
		t.Errorf("unprocessed keys retried: %v, %v", ws, err)
	}
	e.store.client = &batchFlaky{dynamoAPI: real, fail: true}
	if _, err := e.store.ListWatchesByOwner(ctx, scopeA, ""); err == nil {
		t.Error("a BatchGetItem failure was swallowed")
	}
}

// A moved-launch marker is followed only from a legacy id, to an
// owner-scoped record that names that id as its own.
func TestGetScheduledLaunch_MarkerRules(t *testing.T) {
	e := newMKEnv(t)
	ctx := context.Background()
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS(OwnerKey(scopeA, "sl-1")), "legacy_id": avS("sl-other"), "status": avS("pending")})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS("sl-1"), "moved_to": avS(OwnerKey(scopeA, "sl-1"))})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS("sl-2"), "moved_to": avS("sl-3")})
	e.put(t, e.t.Scheduled, map[string]types.AttributeValue{"schedule_id": avS("sl-4"), "moved_to": avS(OwnerKey(scopeA, "sl-missing"))})
	for _, id := range []string{"sl-1", "sl-2", "sl-4"} {
		if sl, err := e.store.GetScheduledLaunch(ctx, id); err != nil || sl != nil {
			t.Errorf("%s resolved to %+v (%v)", id, sl, err)
		}
	}
}
