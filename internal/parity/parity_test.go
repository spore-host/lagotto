package parity

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sample() File {
	return File{Area: "a", Vectors: map[string]Vectors{"f": {Go: "pkg.F", Cases: []Case{
		{Name: "ok", Input: "<in>", Output: 1},
		{Name: "bad", Input: "x", Error: ErrText(errors.New("boom"))},
	}}}}
}

func TestEncodeIsCanonical(t *testing.T) {
	b, err := Encode(sample())
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`"$format": "` + Format + `"`, `"input": "<in>"`, `"output": null`, `"error": "boom"`} {
		if !strings.Contains(s, want) {
			t.Errorf("encoded file lacks %s:\n%s", want, s)
		}
	}
	if !strings.HasSuffix(s, "}\n") {
		t.Error("no trailing newline")
	}
	if ErrText(nil) != "" {
		t.Error("ErrText(nil) is not empty")
	}
}

func TestSyncWritesThenChecks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "testdata", "parity", "x.json")
	if err := sync(path, sample(), false); err == nil || !strings.Contains(err.Error(), "make parity-vectors") {
		t.Errorf("missing file: %v", err)
	}
	if err := sync(path, sample(), true); err != nil {
		t.Fatal(err)
	}
	if err := sync(path, sample(), false); err != nil {
		t.Errorf("fresh file reported stale: %v", err)
	}
	changed := sample()
	changed.Area = "b"
	if err := sync(path, changed, false); err == nil || !strings.Contains(err.Error(), "x.json is stale") {
		t.Errorf("changed vectors not reported: %v", err)
	}
}

func TestFindModuleRoot(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "internal", "parity")); err != nil {
		t.Errorf("module root %s does not hold internal/parity", root)
	}
	if _, err := findModuleRoot(t.TempDir()); err == nil {
		t.Error("found a lagotto module root in an empty temp dir")
	}
}

// Check against a committed file: decoding one and checking it again is a
// no-op, so the canonical encoding round-trips.
func TestCheckACommittedFile(t *testing.T) {
	root, err := moduleRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "testdata", "parity", "watcher-azs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	Check(t, "watcher-azs.json", f)
}

func TestSyncReportsWriteAndEncodeFailures(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sync(filepath.Join(blocker, "x.json"), sample(), true); err == nil {
		t.Error("writing under a regular file succeeded")
	}
	bad := File{Vectors: map[string]Vectors{"f": {Cases: []Case{{Output: make(chan int)}}}}}
	if err := sync(filepath.Join(dir, "y.json"), bad, true); err == nil || !strings.Contains(err.Error(), "encode y.json") {
		t.Errorf("unencodable vectors: %v", err)
	}
}
