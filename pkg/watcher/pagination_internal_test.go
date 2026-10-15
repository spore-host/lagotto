package watcher

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakePage is one page a fakePagedDynamo returns. count is the page's Count;
// when negative it defaults to len(items).
type fakePage struct {
	items []map[string]types.AttributeValue
	count int32
}

// fakePagedDynamo serves Query and Scan from fixed per-table pages, threading
// LastEvaluatedKey / ExclusiveStartKey the way DynamoDB does, so a caller that
// reads only the first page visibly loses the rest (#181). Every other
// dynamoAPI method panics through the nil embedded interface.
type fakePagedDynamo struct {
	dynamoAPI
	pages map[string][]fakePage // table name -> pages
	calls map[string]int        // table name -> Query/Scan calls
	last  map[string]any        // table name -> last input seen
}

func newFakePagedDynamo() *fakePagedDynamo {
	return &fakePagedDynamo{pages: map[string][]fakePage{}, calls: map[string]int{}, last: map[string]any{}}
}

func (f *fakePagedDynamo) page(table string, start map[string]types.AttributeValue) (fakePage, map[string]types.AttributeValue, error) {
	f.calls[table]++
	pages, ok := f.pages[table]
	if !ok {
		return fakePage{}, nil, &types.ResourceNotFoundException{Message: aws.String("no table " + table)}
	}
	i := 0
	if start != nil {
		n, ok := start["page"].(*types.AttributeValueMemberN)
		if !ok {
			return fakePage{}, nil, fmt.Errorf("unexpected ExclusiveStartKey %v", start)
		}
		i, _ = strconv.Atoi(n.Value)
	}
	if i >= len(pages) {
		return fakePage{}, nil, fmt.Errorf("ExclusiveStartKey page %d past the last page", i)
	}
	p := pages[i]
	if p.count < 0 {
		p.count = int32(len(p.items))
	}
	var next map[string]types.AttributeValue
	if i+1 < len(pages) {
		next = map[string]types.AttributeValue{"page": &types.AttributeValueMemberN{Value: strconv.Itoa(i + 1)}}
	}
	return p, next, nil
}

func (f *fakePagedDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	table := aws.ToString(in.TableName)
	f.last[table] = in
	p, next, err := f.page(table, in.ExclusiveStartKey)
	if err != nil {
		return nil, err
	}
	return &dynamodb.QueryOutput{Items: p.items, Count: p.count, LastEvaluatedKey: next}, nil
}

func (f *fakePagedDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	table := aws.ToString(in.TableName)
	f.last[table] = in
	p, next, err := f.page(table, in.ExclusiveStartKey)
	if err != nil {
		return nil, err
	}
	return &dynamodb.ScanOutput{Items: p.items, Count: p.count, LastEvaluatedKey: next}, nil
}

func watchItem(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"watch_id": &types.AttributeValueMemberS{Value: id},
		"user_id":  &types.AttributeValueMemberS{Value: "arn:aws:iam::123456789012:user/alice"},
		"status":   &types.AttributeValueMemberS{Value: string(StatusActive)},
	}
}

func matchItem(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"watch_id":      &types.AttributeValueMemberS{Value: id},
		"instance_type": &types.AttributeValueMemberS{Value: "g5.xlarge"},
	}
}

// pagesOf splits ids into pages of the given sizes using mk.
func pagesOf(mk func(string) map[string]types.AttributeValue, prefix string, sizes ...int) []fakePage {
	var pages []fakePage
	n := 0
	for _, size := range sizes {
		p := fakePage{count: -1}
		for j := 0; j < size; j++ {
			n++
			p.items = append(p.items, mk(fmt.Sprintf("%s-%d", prefix, n)))
		}
		pages = append(pages, p)
	}
	return pages
}

func newPagedStore(f *fakePagedDynamo) *Store {
	return &Store{client: f, watchesTable: "w", historyTable: "h", scheduledTable: "s"}
}

func ids[T any](xs []T, id func(T) string) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = id(x)
	}
	return out
}

func wantIDs(t *testing.T, what string, got []string, prefix string, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("%s returned %d records, want %d across every page: %v", what, len(got), n, got)
	}
	for i, g := range got {
		if want := fmt.Sprintf("%s-%d", prefix, i+1); g != want {
			t.Errorf("%s[%d] = %q, want %q (pages out of order?)", what, i, g, want)
		}
	}
}

// TestListWatchesByUserReadsEveryPage is #181: `lagotto list` and `history`
// silently dropped every watch past the first 1 MB Query page.
func TestListWatchesByUserReadsEveryPage(t *testing.T) {
	for _, status := range []WatchStatus{"", StatusActive} {
		t.Run("status="+string(status), func(t *testing.T) {
			f := newFakePagedDynamo()
			// A filtered query can return an empty page with more to come.
			f.pages["w"] = pagesOf(watchItem, "w", 2, 0, 3)
			got, err := newPagedStore(f).ListWatchesByUser(context.Background(), "alice", status)
			if err != nil {
				t.Fatal(err)
			}
			wantIDs(t, "ListWatchesByUser", ids(got, func(w Watch) string { return w.WatchID }), "w", 5)
			in := f.last["w"].(*dynamodb.QueryInput)
			if aws.ToString(in.IndexName) != "user_id-index" {
				t.Errorf("IndexName = %q, want user_id-index", aws.ToString(in.IndexName))
			}
			if (status != "") != (in.FilterExpression != nil) {
				t.Errorf("FilterExpression = %v for status %q", in.FilterExpression, status)
			}
		})
	}
}

func TestListActiveWatchesReadsEveryPage(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["w"] = pagesOf(watchItem, "w", 1, 1, 1)
	got, err := newPagedStore(f).ListActiveWatches(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantIDs(t, "ListActiveWatches", ids(got, func(w Watch) string { return w.WatchID }), "w", 3)
}

func TestListMatchHistoryReadsEveryPage(t *testing.T) {
	matchID := func(m MatchResult) string { return m.WatchID }
	t.Run("by watch", func(t *testing.T) {
		f := newFakePagedDynamo()
		f.pages["h"] = pagesOf(matchItem, "m", 2, 2)
		got, err := newPagedStore(f).ListMatchHistory(context.Background(), "w-1")
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListMatchHistory(watch)", ids(got, matchID), "m", 4)
	})
	t.Run("by user", func(t *testing.T) {
		f := newFakePagedDynamo()
		f.pages["h"] = pagesOf(matchItem, "m", 3, 0, 1)
		got, err := newPagedStore(f).ListMatchHistoryByUser(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListMatchHistoryByUser", ids(got, matchID), "m", 4)
	})
	t.Run("unscoped scan keeps its cap", func(t *testing.T) {
		f := newFakePagedDynamo()
		// A short first page (the 1 MB bound) must not end the scan early...
		f.pages["h"] = pagesOf(matchItem, "m", 30, 30)
		got, err := newPagedStore(f).ListMatchHistory(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListMatchHistory(all)", ids(got, matchID), "m", 60)

		// ...but the scan still stops at the cap rather than reading the table.
		f = newFakePagedDynamo()
		f.pages["h"] = pagesOf(matchItem, "m", 60, 60, 60)
		got, err = newPagedStore(f).ListMatchHistory(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		wantIDs(t, "ListMatchHistory(all, capped)", ids(got, matchID), "m", scanMatchHistoryCap)
		if f.calls["h"] != 2 {
			t.Errorf("capped scan made %d calls, want 2", f.calls["h"])
		}
	})
}

// TestHasPendingScheduledLaunchesReadsEveryPage: the pending test is a
// FilterExpression, so the first page can match nothing while a later one has
// an armed launch. Reading one page told the poller "none pending" (#181).
func TestHasPendingScheduledLaunchesReadsEveryPage(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["s"] = []fakePage{{count: 0}, {count: 0}, {count: 1}}
	got, err := newPagedStore(f).HasPendingScheduledLaunches(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("HasPendingScheduledLaunches = false; a pending launch on page 3 was missed")
	}

	f = newFakePagedDynamo()
	f.pages["s"] = []fakePage{{count: 0}, {count: 0}}
	if got, err := newPagedStore(f).HasPendingScheduledLaunches(context.Background()); err != nil || got {
		t.Fatalf("HasPendingScheduledLaunches over pages with nothing pending = %v, %v; want false, nil", got, err)
	}
	if f.calls["s"] != 2 {
		t.Errorf("made %d Scan calls, want 2 (every page)", f.calls["s"])
	}

	f = newFakePagedDynamo() // no table at all
	if got, err := newPagedStore(f).HasPendingScheduledLaunches(context.Background()); err != nil || got {
		t.Fatalf("HasPendingScheduledLaunches with no table = %v, %v; want false, nil", got, err)
	}
}

// TestTablesEmptyReadsPastAnEmptyFirstPage: TablesEmpty gates deleting the
// tables, so a first page with Count 0 and a LastEvaluatedKey is not "empty".
func TestTablesEmptyReadsPastAnEmptyFirstPage(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["w"] = []fakePage{{count: 0}}
	f.pages["h"] = []fakePage{{count: 0}, {count: 1}}
	// "s" absent: a missing table is trivially empty.
	empty, err := newPagedStore(f).TablesEmpty(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if empty {
		t.Fatal("TablesEmpty = true; the history table's second page has a record")
	}

	f = newFakePagedDynamo()
	f.pages["w"] = []fakePage{{count: 0}, {count: 0}}
	f.pages["h"] = []fakePage{{count: 0}}
	empty, err = newPagedStore(f).TablesEmpty(context.Background())
	if err != nil || !empty {
		t.Fatalf("TablesEmpty over empty pages = %v, %v; want true, nil", empty, err)
	}
}
