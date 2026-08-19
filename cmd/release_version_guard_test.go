package cmd

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The release-time guard (scripts/check-release-version.sh, run by
// .github/workflows/release.yaml) proves the -X ldflag reaches a real
// variable AT TAG TIME, by actually building the binary. These two tests
// catch the same class of drift earlier, in ordinary PR CI, for the two
// things that could silently stop the guard itself from ever running:
//   - .goreleaser.yaml renaming/removing the ldflag target (#98)
//   - the release workflow no longer invoking the guard script
//
// Neither failure mode errors anywhere obvious: a `-X` for a symbol that
// doesn't exist is accepted silently by the Go linker, and a deleted
// workflow step just means the guard silently stops running.

// TestGoreleaserLdflagTargetsRealVariable guards the assumption
// scripts/check-release-version.sh depends on: .goreleaser.yaml's -X ldflag
// names the *cmd.Version* variable this package actually declares. If the
// variable were renamed or moved without updating .goreleaser.yaml, this
// fails in ordinary CI instead of silently reporting "dev" from every
// released binary.
func TestGoreleaserLdflagTargetsRealVariable(t *testing.T) {
	data, err := os.ReadFile("../.goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}

	re := regexp.MustCompile(`-X ([^ =]+)\.Version=\{\{\.Version\}\}`)
	m := re.FindStringSubmatch(string(data))
	if m == nil {
		t.Fatal(".goreleaser.yaml has no '-X <pkg>.Version={{.Version}}' ldflag — " +
			"released lagotto binaries would report \"dev\" (#98)")
	}
	wantPkg := "github.com/spore-host/lagotto/cmd"
	if m[1] != wantPkg {
		t.Errorf(".goreleaser.yaml's version ldflag targets package %q, want %q "+
			"(this package's own import path) — if cmd.Version moved, update the ldflag too",
			m[1], wantPkg)
	}
}

// TestReleaseWorkflowRunsTheVersionGuard: the guard script only helps if the
// release workflow actually calls it. Guards against the step being deleted
// or renamed away from invoking scripts/check-release-version.sh.
func TestReleaseWorkflowRunsTheVersionGuard(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/release.yaml")
	if err != nil {
		t.Fatalf("read release.yaml: %v", err)
	}
	if !strings.Contains(string(data), "scripts/check-release-version.sh") {
		t.Error("release.yaml does not run scripts/check-release-version.sh; " +
			"without it, a broken -X ldflag would publish a release before anyone notices (#98)")
	}
}
