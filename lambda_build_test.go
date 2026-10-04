package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// goBuildByFilename matches a `go build` that names .go FILES rather than a
// package. The negated class stops it matching a flag like `-tags lambda.norpc`
// or an output path, so only a trailing source-file argument trips it.
var goBuildByFilename = regexp.MustCompile(`go build[^\n&|;]*\s[\w./-]+\.go\b`)

// TestLambdaIsBuiltAsAPackageNotAFileList guards lagotto#172.
//
// `go build ... main.go` compiles ONLY the named files. The loud failure is a
// second source file that main.go references — the build breaks, which at least
// stops the release. spawn's reaper hit exactly that (spore-host/spawn#466):
//
//	./main.go:533:21: undefined: accountOutcome
//	./main.go:538:14: undefined: classifyScanError
//
// The quiet failure is the one worth a gate: a new file main.go does NOT
// reference — a func init() registration, a side-effect import — compiles fine
// and produces a binary SILENTLY MISSING that code. The release succeeds, the
// asset publishes, and `lagotto deploy` installs a poller that is subtly wrong.
//
// lambda/capacity-poller is single-file today, which is the only reason the old
// form worked, so this asserts the invariant rather than the current state.
func TestLambdaIsBuiltAsAPackageNotAFileList(t *testing.T) {
	// Every place a build command could hide: the release config, Makefiles, and
	// any shell script.
	var checked int
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		base := d.Name()
		if base != "Makefile" && !strings.HasSuffix(base, ".yaml") &&
			!strings.HasSuffix(base, ".yml") && !strings.HasSuffix(base, ".sh") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		checked++
		for i, line := range strings.Split(string(b), "\n") {
			trimmed := strings.TrimSpace(line)
			// Skip comments — the explanatory comments beside the fix name main.go.
			if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "@#") ||
				strings.HasPrefix(trimmed, "//") {
				continue
			}
			if m := goBuildByFilename.FindString(line); m != "" {
				t.Errorf("%s:%d builds by FILENAME, not package:\n    %s\n\n"+
					"Use `go build ... .` instead. Naming files compiles only those files, so a "+
					"second source file either breaks the build or — if main.go does not "+
					"reference it — is silently omitted from the binary, and the release still "+
					"succeeds (lagotto#172).", path, i+1, strings.TrimSpace(m))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if checked == 0 {
		t.Fatal("scanned no build files — has the layout changed?")
	}
	t.Logf("scanned %d build files for go-build-by-filename", checked)
}
