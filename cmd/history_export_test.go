package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spf13/cobra"
)

// fakeHistoryScanner serves fixed items, optionally failing after some.
type fakeHistoryScanner struct {
	items   []map[string]types.AttributeValue
	failAt  int // fail before the item at this index; <0 never
	scanned int
}

func (f *fakeHistoryScanner) ScanMatchHistory(_ context.Context, fn func(map[string]types.AttributeValue) error) error {
	for i, it := range f.items {
		if i == f.failAt {
			return errors.New("ProvisionedThroughputExceededException")
		}
		f.scanned++
		if err := fn(it); err != nil {
			return err
		}
	}
	return nil
}

func historyRow(watch, matchedAt string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"watch_id":      &types.AttributeValueMemberS{Value: watch},
		"matched_at":    &types.AttributeValueMemberS{Value: matchedAt},
		"instance_type": &types.AttributeValueMemberS{Value: "p5.48xlarge"},
		"price":         &types.AttributeValueMemberN{Value: "98.3200000001"},
	}
}

func withHistoryExport(t *testing.T, s historyScanner, out string) {
	t.Helper()
	savedStore, savedOut := historyExportStore, historyExportOut
	t.Cleanup(func() { historyExportStore, historyExportOut = savedStore, savedOut })
	historyExportStore = func(context.Context) (historyScanner, string, error) { return s, "us-east-1", nil }
	historyExportOut = out
}

// Every attribute of every row survives, losslessly: a number keeps
// DynamoDB's exact digits, a binary is base64, a set is a sorted array, and
// nested maps and lists keep their shape.
func TestItemJSONIsLossless(t *testing.T) {
	item := map[string]types.AttributeValue{
		"s":    &types.AttributeValueMemberS{Value: "x"},
		"n":    &types.AttributeValueMemberN{Value: "12345678901234567890.5"},
		"b":    &types.AttributeValueMemberB{Value: []byte("hi")},
		"bool": &types.AttributeValueMemberBOOL{Value: true},
		"null": &types.AttributeValueMemberNULL{Value: true},
		"ss":   &types.AttributeValueMemberSS{Value: []string{"b", "a"}},
		"ns":   &types.AttributeValueMemberNS{Value: []string{"2", "10"}},
		"bs":   &types.AttributeValueMemberBS{Value: [][]byte{[]byte("z"), []byte("a")}},
		"l":    &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberN{Value: "1"}, &types.AttributeValueMemberS{Value: "y"}}},
		"m":    &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{"k": &types.AttributeValueMemberS{Value: "v"}}},
	}
	b, err := json.Marshal(itemJSON(item))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"b":"aGk=","bool":true,"bs":["YQ==","eg=="],"l":[1,"y"],"m":{"k":"v"},"n":12345678901234567890.5,` +
		`"ns":[10,2],"null":null,"s":"x","ss":["a","b"]}`
	if string(b) != want {
		t.Errorf("itemJSON =\n  %s\nwant\n  %s", b, want)
	}
}

// To stdout: one JSON object per row and nothing else; the count goes to
// stderr.
func TestHistoryExportToStdout(t *testing.T) {
	f := &fakeHistoryScanner{items: []map[string]types.AttributeValue{
		historyRow("u1/w-1", "2026-11-01T00:00:00Z"), historyRow("u2/w-2", "2026-11-02T00:00:00Z"),
	}, failAt: -1}
	withHistoryExport(t, f, "")

	var stdout, stderr bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&stdout)
	c.SetErr(&stderr)
	if err := runHistoryExport(c, nil); err != nil {
		t.Fatal(err)
	}
	text := stdout.String()
	sc := bufio.NewScanner(strings.NewReader(text))
	var rows []map[string]any
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("stdout line is not one JSON object: %q: %v", sc.Text(), err)
		}
		rows = append(rows, m)
	}
	if len(rows) != 2 || rows[0]["watch_id"] != "u1/w-1" || rows[1]["watch_id"] != "u2/w-2" {
		t.Errorf("rows %v, want both owners' rows in scan order", rows)
	}
	if !strings.Contains(text, `"price":98.3200000001`) {
		t.Errorf("the price lost digits: %s", text)
	}
	if !strings.Contains(stderr.String(), "Exported 2 match-history row(s)") || !strings.Contains(stderr.String(), "to stdout") {
		t.Errorf("stderr %q, want the row count", stderr.String())
	}
}

// --out writes a new 0600 file and never overwrites one.
func TestHistoryExportToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	f := &fakeHistoryScanner{items: []map[string]types.AttributeValue{historyRow("u1/w-1", "2026-11-01T00:00:00Z")}, failAt: -1}
	withHistoryExport(t, f, path)

	var stdout, stderr bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&stdout)
	c.SetErr(&stderr)
	if err := runHistoryExport(c, nil); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Errorf("with --out, stdout got %q", stdout.String())
	}
	b, err := os.ReadFile(path) //nolint:gosec // the test's own temp file
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(b), "\n") != 1 || !strings.Contains(string(b), `"watch_id":"u1/w-1"`) {
		t.Errorf("file holds %q", b)
	}
	if runtime.GOOS != "windows" {
		if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
			t.Errorf("file mode %v, want 0600 (the history names every owner)", st.Mode().Perm())
		}
	}

	// A second export to the same path is refused, and the first is kept.
	f.scanned = 0
	if err := runHistoryExport(c, nil); err == nil || !strings.Contains(err.Error(), "never overwritten") {
		t.Errorf("an export over an existing file: err %v, want a refusal", err)
	}
	if f.scanned != 0 {
		t.Errorf("the refused export scanned %d rows; it must stop before reading", f.scanned)
	}
	if b2, _ := os.ReadFile(path); !bytes.Equal(b, b2) { //nolint:gosec // the test's own temp file
		t.Error("the refused export changed the existing file")
	}
}

// A scan that fails part-way is an error saying how many rows were written,
// never a success.
func TestHistoryExportScanFailure(t *testing.T) {
	f := &fakeHistoryScanner{items: []map[string]types.AttributeValue{
		historyRow("u1/w-1", "a"), historyRow("u1/w-2", "b"), historyRow("u1/w-3", "c"),
	}, failAt: 2}
	withHistoryExport(t, f, "")
	var stdout, stderr bytes.Buffer
	c := &cobra.Command{}
	c.SetOut(&stdout)
	c.SetErr(&stderr)
	err := runHistoryExport(c, nil)
	if err == nil || !strings.Contains(err.Error(), "after 2 row(s)") {
		t.Fatalf("err %v, want a failure after 2 rows", err)
	}
	if strings.Contains(stderr.String(), "Exported") {
		t.Errorf("a failed export reported success: %q", stderr.String())
	}
}

// The command is wired under `lagotto history`, takes no arguments, and is
// an advanced (administrator) command.
func TestHistoryExportIsWired(t *testing.T) {
	c, _, err := rootCmd.Find([]string{"history", "export"})
	if err != nil || c != historyExportCmd {
		t.Fatalf("lagotto history export resolves to %v (%v)", c, err)
	}
	if c.Annotations[levelAnnotation] != levelAdvanced {
		t.Errorf("level %q, want advanced", c.Annotations[levelAnnotation])
	}
	if c.Flags().Lookup("out") == nil {
		t.Error("no --out flag")
	}
	if err := c.Args(c, []string{"extra"}); err == nil {
		t.Error("an argument was accepted")
	}
}
