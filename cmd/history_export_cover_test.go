package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spf13/cobra"

	"github.com/spore-host/lagotto/pkg/awscfg"
)

// The default store reads the history table in the configured region, and a
// config that cannot be loaded is an error, not a store. No AWS call: the
// loader is replaced, and building the store makes none.
func TestHistoryExportDefaultStore(t *testing.T) {
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "eu-west-2"}, nil
	})
	s, region, err := historyExportStore(context.Background())
	if err != nil || s == nil || region != "eu-west-2" {
		t.Fatalf("store %v region %q err %v, want a store in eu-west-2", s, region, err)
	}

	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("no such profile")
	})
	if _, _, err := historyExportStore(context.Background()); err == nil || !strings.Contains(err.Error(), "load AWS config") {
		t.Errorf("a failed config load: err %v", err)
	}
}

// When the store cannot be had, the export stops before --out is created: a
// failed export never leaves an empty file that a retry would then refuse.
func TestHistoryExportStoreFailureCreatesNoFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	withHistoryExport(t, nil, path)
	historyExportStore = func(context.Context) (historyScanner, string, error) {
		return nil, "", errors.New("load AWS config: no such profile")
	}
	c := &cobra.Command{}
	c.SetOut(&bytes.Buffer{})
	c.SetErr(&bytes.Buffer{})
	if err := runHistoryExport(c, nil); err == nil {
		t.Fatal("the export succeeded without a store")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("--out was created though nothing was read: %v", err)
	}
}

// failingWriter refuses every write, like a full disk or a closed pipe.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("no space left on device") }

// A write that fails part-way is an error, with fewer rows counted than were
// scanned, never a silent truncation.
func TestHistoryExportWriteFailure(t *testing.T) {
	var items []map[string]types.AttributeValue
	for i := 0; i < 200; i++ { // well past bufio's 4 KiB buffer
		items = append(items, historyRow(fmt.Sprintf("u1/w-%03d", i), "2026-11-01T00:00:00Z"))
	}
	n, err := exportHistory(context.Background(), &fakeHistoryScanner{items: items, failAt: -1}, failingWriter{})
	if err == nil || !strings.Contains(err.Error(), "no space left") {
		t.Fatalf("err %v, want the write failure", err)
	}
	if n >= len(items) {
		t.Errorf("%d of %d rows counted as written though every write failed", n, len(items))
	}
}

// A row whose number is not a number (a corrupt item) is an error, not a
// line of invalid JSON in the export.
func TestHistoryExportCorruptNumberIsAnError(t *testing.T) {
	bad := historyRow("u1/w-1", "a")
	bad["price"] = &types.AttributeValueMemberN{Value: "not-a-number"}
	var out bytes.Buffer
	n, err := exportHistory(context.Background(), &fakeHistoryScanner{items: []map[string]types.AttributeValue{bad}, failAt: -1}, &out)
	if err == nil || n != 0 {
		t.Fatalf("n %d err %v, want an error and no row", n, err)
	}
	if out.Len() != 0 {
		t.Errorf("the corrupt row was written: %q", out.String())
	}
}

// An attribute type this lagotto does not know is written as null rather
// than dropping the row.
func TestAvJSONUnknownMemberIsNull(t *testing.T) {
	if v := avJSON(&types.UnknownUnionMember{Tag: "FUTURE"}); v != nil {
		t.Errorf("unknown member: %v, want nil", v)
	}
	if got := itemJSON(map[string]types.AttributeValue{"x": &types.UnknownUnionMember{Tag: "FUTURE"}}); len(got) != 1 || got["x"] != nil {
		t.Errorf("itemJSON dropped or mangled the attribute: %v", got)
	}
}
