package watcher

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ScanMatchHistory reads every page of the history table, not only the first
// (lagotto#327: the export is the only copy left once a teardown deletes the
// table), and Scans only that table.
func TestScanMatchHistoryReadsEveryPage(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["h"] = pagesOf(matchItem, "w", 2, 3, 1)
	s := newPagedStore(f)

	var got []string
	err := s.ScanMatchHistory(context.Background(), func(it map[string]types.AttributeValue) error {
		got = append(got, it["watch_id"].(*types.AttributeValueMemberS).Value)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 || got[0] != "w-1" || got[5] != "w-6" {
		t.Errorf("scanned %v, want w-1 .. w-6 across three pages", got)
	}
	if f.calls["h"] != 3 {
		t.Errorf("%d Scan calls on the history table, want 3 (one per page)", f.calls["h"])
	}
	if len(f.calls) != 1 {
		t.Errorf("tables read: %v, want only the history table", f.calls)
	}
	if in, ok := f.last["h"].(*dynamodb.ScanInput); !ok || aws.ToString(in.TableName) != "h" {
		t.Errorf("last input %#v, want a Scan of the history table", f.last["h"])
	}
}

// An error from fn stops the scan and is returned as it is.
func TestScanMatchHistoryStopsOnCallbackError(t *testing.T) {
	f := newFakePagedDynamo()
	f.pages["h"] = pagesOf(matchItem, "w", 2, 2)
	stop := errors.New("disk full")
	n := 0
	err := newPagedStore(f).ScanMatchHistory(context.Background(), func(map[string]types.AttributeValue) error {
		n++
		return stop
	})
	if !errors.Is(err, stop) || n != 1 || f.calls["h"] != 1 {
		t.Errorf("err %v after %d items and %d pages, want the callback's error after 1 item, 1 page", err, n, f.calls["h"])
	}
}

// A missing table is an error naming it, wrapping DynamoDB's.
func TestScanMatchHistoryMissingTable(t *testing.T) {
	err := newPagedStore(newFakePagedDynamo()).ScanMatchHistory(context.Background(), func(map[string]types.AttributeValue) error { return nil })
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Errorf("err %v, want a wrapped ResourceNotFoundException", err)
	}
}
