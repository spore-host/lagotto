package cmd

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spore-host/libs/docgen"
)

// TestEveryCommandHasAnExplicitLevel (#297): a new command must choose basic
// or advanced. Defaulting it silently to advanced would hide a command the
// portal should offer; defaulting it to basic would fail the portal's gate for
// a command nobody meant to show.
func TestEveryCommandHasAnExplicitLevel(t *testing.T) {
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if _, ok := c.Annotations[levelAnnotation]; !ok {
			t.Errorf("%q has no %s annotation: add Annotations: level(levelBasic) or level(levelAdvanced) "+
				"(basic commands need a row in spore-host web/portal/parity/cli-gui-matrix.json)", c.CommandPath(), levelAnnotation)
		}
		for _, sub := range c.Commands() {
			if !skipInReference(sub) {
				walk(sub)
			}
		}
	}
	walk(rootCmd)
}

// TestBasicSetIsDeliberate pins what the portal shows by default (spore-host
// #645 brief §2, and the lagotto rows of the parity matrix). Changing it is a
// portal change too: update the matrix in the same breath.
func TestBasicSetIsDeliberate(t *testing.T) {
	got := basicEntries(t, mustLevels(t))
	want := []string{
		"lagotto",
		"lagotto ack",
		"lagotto cancel",
		"lagotto history",
		"lagotto list",
		"lagotto status",
		"lagotto watch",
		"lagotto watch --action",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("basic commands and flags =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

var referenceHeading = regexp.MustCompile("(?m)^#{2,6} `([^`]+)`$")

// TestLevelsCoverExactlyTheReference: the sidecar names every command the
// markdown reference documents, and no other, so skipInReference cannot drift
// from libs/docgen's own skip rule.
func TestLevelsCoverExactlyTheReference(t *testing.T) {
	frags, err := docgen.Generate(rootCmd, docgen.Options{CLIName: "lagotto"})
	if err != nil {
		t.Fatal(err)
	}
	documented := map[string]bool{"lagotto": true} // the root: _global-flags.md
	for name, b := range frags {
		if strings.HasSuffix(name, ".md") {
			for _, m := range referenceHeading.FindAllStringSubmatch(string(b), -1) {
				documented[m[1]] = true
			}
		}
	}
	doc := mustLevels(t)
	for k := range doc {
		if strings.HasPrefix(k, "$") {
			continue
		}
		if !documented[k] {
			t.Errorf("_levels.json names %q, which the reference does not document", k)
		}
		delete(documented, k)
	}
	for k := range documented {
		t.Errorf("the reference documents %q, which _levels.json does not name", k)
	}
}

func TestGenerateLevelsRefusesBadLevels(t *testing.T) {
	newRoot := func() (*cobra.Command, *cobra.Command) {
		root := &cobra.Command{Use: "x", Annotations: level(levelBasic)}
		sub := &cobra.Command{Use: "sub", Annotations: level(levelAdvanced), Run: func(*cobra.Command, []string) {}}
		sub.Flags().Bool("flag", false, "")
		root.AddCommand(sub)
		return root, sub
	}

	root, sub := newRoot()
	sub.Annotations[levelAnnotation] = "beginner"
	if _, err := generateLevels(root); err == nil || !strings.Contains(err.Error(), `"beginner"`) {
		t.Errorf("an unknown level was accepted: %v", err)
	}

	root, sub = newRoot()
	markBasicFlags(sub, "flag")
	if _, err := generateLevels(root); err == nil || !strings.Contains(err.Error(), "basic but the command is advanced") {
		t.Errorf("a basic flag on an advanced command was accepted: %v", err)
	}

	root, sub = newRoot()
	sub.Annotations[levelAnnotation] = levelBasic
	markBasicFlags(sub, "flag")
	b, err := generateLevels(root)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if string(doc["$format"]) != `"`+levelsFormat+`"` {
		t.Errorf("$format = %s", doc["$format"])
	}
	var e commandLevels
	if err := json.Unmarshal(doc["x sub"], &e); err != nil || e.Level != levelBasic || e.Flags["--flag"] != levelBasic || e.Flags["--help"] != "" {
		t.Errorf(`"x sub" = %+v (%v), want basic with --flag basic`, e, err)
	}
}

func TestMarkBasicFlagsPanicsOnAnUnknownFlag(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("marking a flag that does not exist did not panic")
		}
	}()
	markBasicFlags(&cobra.Command{Use: "x"}, "nope")
}

func mustLevels(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	b, err := generateLevels(rootCmd)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// basicEntries lists "<command>" and "<command> --flag" for everything basic.
func basicEntries(t *testing.T, doc map[string]json.RawMessage) []string {
	t.Helper()
	var out []string
	for k, raw := range doc {
		if strings.HasPrefix(k, "$") {
			continue
		}
		var e commandLevels
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
		if e.Level == levelBasic {
			out = append(out, k)
		}
		for f, l := range e.Flags {
			if l == levelBasic {
				out = append(out, k+" "+f)
			}
		}
	}
	sort.Strings(out)
	return out
}
