package cmd

// The disclosure level of every command and flag (#297), for the spore.host
// portal's CLI⇄GUI parity gate (spore-host web/portal/parity/). The portal
// shows "basic" commands by default; its matrix maps each basic command and
// flag to a GUI control. The gate can check a matrix row against the generated
// reference, but not which commands are basic: this sidecar is that list,
// emitted from the CLI itself so a new basic command, or a demoted one, shows
// up as a diff in docs-gen/ and fails the gate until the matrix agrees.
//
// The same format is proposed for spawn (spawn#1574) and truffle
// (truffle#284), so all three CLIs feed one gate. See levelsFormat.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	// levelAnnotation is the cobra annotation, on a command's Annotations or a
	// flag's Annotations, that sets its level.
	levelAnnotation = "spore.host/level"
	levelBasic      = "basic"
	levelAdvanced   = "advanced"

	// levelsFile is the sidecar's name in the gen-docs output directory. It
	// syncs with the markdown to docs/gen/lagotto/_levels.json; the leading
	// underscore matches _global-flags.md, a fragment that is not a command.
	levelsFile = "_levels.json"

	// levelsFormat names the sidecar's shape, so a consumer can refuse one it
	// does not understand. Keys starting with "$" are metadata, never commands
	// (the portal matrix uses the same convention).
	levelsFormat = "spore.host/cli-levels/v1"
)

// level returns a command's Annotations with its level set. A fresh map each
// time: cobra commands must not share one.
func level(l string) map[string]string { return map[string]string{levelAnnotation: l} }

// markBasicFlags annotates the named local flags of c as basic. Called from
// init, after the flags are defined; a name that is not a flag panics, so a
// renamed flag cannot silently drop out of the basic set.
func markBasicFlags(c *cobra.Command, names ...string) {
	for _, n := range names {
		if err := c.Flags().SetAnnotation(n, levelAnnotation, []string{levelBasic}); err != nil {
			panic(fmt.Sprintf("mark --%s of %q basic: %v", n, c.CommandPath(), err))
		}
	}
}

// commandLevels is one command's entry: its level, and the level of each of
// its documented flags keyed as "--name". The root's entry carries the global
// (persistent) flags, which _global-flags.md documents once.
type commandLevels struct {
	Level string            `json:"level"`
	Flags map[string]string `json:"flags"`
}

// generateLevels builds the sidecar for every command gen-docs documents: the
// same walk as libs/docgen (hidden commands, help, completion and gen-docs
// skipped; subcommands included), and the same flags (local, non-hidden; the
// root's persistent flags under the root).
//
// An unannotated command or flag is advanced. That default is for flags only
// in practice: TestEveryCommandHasAnExplicitLevel makes every command choose.
// A level other than basic/advanced is an error, and so is a basic flag on an
// advanced command, which no basic surface could show.
func generateLevels(root *cobra.Command) ([]byte, error) {
	entries := map[string]commandLevels{}
	var problems []string

	add := func(c *cobra.Command, fs *pflag.FlagSet) {
		path := c.CommandPath()
		lvl, err := levelOf(c.Annotations[levelAnnotation])
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", path, err))
		}
		e := commandLevels{Level: lvl, Flags: map[string]string{}}
		fs.VisitAll(func(f *pflag.Flag) {
			if f.Hidden {
				return
			}
			var v string
			if vs := f.Annotations[levelAnnotation]; len(vs) > 0 {
				v = vs[0]
			}
			fl, err := levelOf(v)
			if err != nil {
				problems = append(problems, fmt.Sprintf("%s --%s: %v", path, f.Name, err))
			}
			if fl == levelBasic && lvl != levelBasic {
				problems = append(problems, fmt.Sprintf("%s --%s is basic but the command is %s", path, f.Name, lvl))
			}
			e.Flags["--"+f.Name] = fl
		})
		entries[path] = e
	}

	add(root, root.PersistentFlags())
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			if skipInReference(sub) {
				continue
			}
			add(sub, sub.LocalFlags())
			walk(sub)
		}
	}
	walk(root)

	if len(problems) > 0 {
		sort.Strings(problems)
		return nil, fmt.Errorf("command levels (%s annotation): %s", levelAnnotation, strings.Join(problems, "; "))
	}

	doc := map[string]any{"$format": levelsFormat}
	for k, v := range entries {
		doc[k] = v
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil { // map keys are sorted: a stable diff
		return nil, err
	}
	return b.Bytes(), nil
}

// levelOf validates an annotation value; empty is advanced.
func levelOf(v string) (string, error) {
	switch v {
	case "":
		return levelAdvanced, nil
	case levelBasic, levelAdvanced:
		return v, nil
	}
	return levelAdvanced, fmt.Errorf("level %q is not %s or %s", v, levelBasic, levelAdvanced)
}

// skipInReference mirrors libs/docgen's skip: the commands the generated
// reference leaves out. TestLevelsCoverExactlyTheReference holds the two to
// the same set.
func skipInReference(c *cobra.Command) bool {
	if c.Hidden || !c.Runnable() && !c.HasAvailableSubCommands() {
		return true
	}
	switch c.Name() {
	case "completion", "help", "gen-docs":
		return true
	}
	return false
}
