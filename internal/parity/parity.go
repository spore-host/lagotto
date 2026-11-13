// Package parity writes and checks the parity test vectors lagotto publishes
// for its TypeScript port, lagotto-ts (#289).
//
// A vector file is JSON under testdata/parity/ at the module root. Each one
// covers one area (patterns, durations, watch validation, records, ...) and
// holds, per Go function, the cases {input, output or error} that lagotto's
// own code produced. The generators are tests in the package that owns each
// function, so they can reach unexported code without exporting it:
//
//	go test ./pkg/watcher ./cmd -run TestParityVectors -args -update-parity
//
// (`make parity-vectors`). Without the flag the same tests FAIL when a
// committed file differs from what the code now produces, so a behaviour
// change cannot ship without its vectors, and CI's ordinary `go test` is the
// drift gate.
//
// lagotto-ts consumes them the way spawn-ts consumes spawn's Go fixtures
// (spawn-ts scripts/gen-go-fixtures.sh): it downloads a published lagotto
// release from the Go module proxy, checked against the sum database, and
// copies testdata/parity/ out of it. The module zip carries testdata/, so the
// vectors are addressed by the release tag, with no release asset.
package parity

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Format names the vector file shape. Bump it on an incompatible change.
const Format = "lagotto/parity-vectors/v1"

var update = flag.Bool("update-parity", false, "rewrite testdata/parity/ from the code instead of checking it")

// Case is one vector. Output is the Go result (null when Error is set);
// Error is the Go error's text, which lagotto-ts reproduces word for word.
type Case struct {
	Name   string `json:"name"`
	Input  any    `json:"input"`
	Output any    `json:"output"`
	Error  string `json:"error,omitempty"`
}

// Vectors are the cases for one Go function. Go names it, as
// "<package>.<Func>"; Note says anything a port needs that the cases don't.
type Vectors struct {
	Go    string `json:"go"`
	Note  string `json:"note,omitempty"`
	Cases []Case `json:"cases"`
}

// File is one vector file.
type File struct {
	Format  string             `json:"$format"`
	Area    string             `json:"area"`
	Vectors map[string]Vectors `json:"vectors"`
}

// ErrText is err's text, or "".
func ErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Check writes f to testdata/parity/<name> under -update-parity, and
// otherwise fails t when the committed file differs.
func Check(t *testing.T, name string, f File) {
	t.Helper()
	root, err := moduleRoot()
	if err == nil {
		err = sync(filepath.Join(root, "testdata", "parity", name), f, *update)
	}
	if err != nil {
		t.Error(err)
	}
}

// Encode is the canonical bytes of a vector file: indented, keys sorted,
// HTML not escaped, a trailing newline.
func Encode(f File) ([]byte, error) {
	f.Format = Format
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(f); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// sync writes f to path when write is set, and otherwise reports whether the
// file at path is f.
func sync(path string, f File, write bool) error {
	b, err := Encode(f)
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	if write {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		return os.WriteFile(path, b, 0o644)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w (run 'make parity-vectors')", path, err)
	}
	if !bytes.Equal(got, b) {
		return fmt.Errorf("testdata/parity/%s is stale: the code now produces different vectors. "+
			"Run 'make parity-vectors', review the diff (lagotto-ts must match it), and commit", filepath.Base(path))
	}
	return nil
}

// moduleRoot is the directory of the lagotto module's go.mod, at or above
// the working directory.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return findModuleRoot(dir)
}

func findModuleRoot(dir string) (string, error) {
	for {
		if b, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil && bytes.Contains(b, []byte("module github.com/spore-host/lagotto\n")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no lagotto go.mod above %s", dir)
		}
		dir = parent
	}
}
