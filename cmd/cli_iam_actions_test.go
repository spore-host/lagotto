package cmd

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"golang.org/x/tools/go/ssa"

	"github.com/spore-host/lagotto/internal/iamcoverage"
)

var updateCLIIAM = flag.Bool("update-cli-iam", false, "rewrite "+cliIAMDocPath+" from the code")

// cliIAMDocPath is the generated inventory, relative to the repository root.
const cliIAMDocPath = "docs/reference/cli-iam-actions.md"

const cmdPkgPath = "github.com/spore-host/lagotto/cmd"

// cliIAMPlatforms are the platforms lagotto ships for (.goreleaser.yaml); a
// command's actions are the union across them.
var cliIAMPlatforms = []string{"darwin", "linux", "windows"}

type cliIAMCommand struct {
	path string
	fn   any
	pre  []any
}

// cliIAMCommands walks the live command tree. Hidden commands (gen-docs) are
// plumbing and left out. rootPre are the root's persistent pre-runs, which
// every command inherits.
func cliIAMCommands() (rootPre []any, runnable []cliIAMCommand) {
	persistentOf := func(c *cobra.Command) []any {
		var out []any
		if c.PersistentPreRunE != nil {
			out = append(out, c.PersistentPreRunE)
		}
		if c.PersistentPreRun != nil {
			out = append(out, c.PersistentPreRun)
		}
		return out
	}
	rootPre = persistentOf(rootCmd)
	var walk func(c *cobra.Command, pre []any)
	walk = func(c *cobra.Command, pre []any) {
		if c.Hidden {
			return
		}
		if c != rootCmd {
			pre = append(append([]any{}, pre...), persistentOf(c)...)
		}
		switch {
		case c.RunE != nil:
			runnable = append(runnable, cliIAMCommand{path: c.CommandPath(), fn: c.RunE, pre: pre})
		case c.Run != nil:
			runnable = append(runnable, cliIAMCommand{path: c.CommandPath(), fn: c.Run, pre: pre})
		}
		for _, sub := range c.Commands() {
			walk(sub, pre)
		}
	}
	walk(rootCmd, nil)
	sort.Slice(runnable, func(i, j int) bool { return runnable[i].path < runnable[j].path })
	return rootPre, runnable
}

// ssaForFunc matches a function value from the live command tree to its SSA
// function, by declaration position.
func ssaForFunc(prog *iamcoverage.Program, fn any) (*ssa.Function, error) {
	rf := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
	if rf == nil {
		return nil, fmt.Errorf("no runtime function for %T", fn)
	}
	file, line := rf.FileLine(rf.Entry())
	matches := prog.FuncsAt(cmdPkgPath, filepath.Base(file), line)
	if len(matches) != 1 {
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.String()
		}
		return nil, fmt.Errorf("%s (%s:%d) matched %d SSA functions %v", rf.Name(), filepath.Base(file), line, len(matches), names)
	}
	return matches[0], nil
}

// TestCLIActionInventoryIsCurrent generates the IAM actions each lagotto command
// can call with the caller's own credentials (#206) and fails when the committed
// docs/reference/cli-iam-actions.md differs. Regenerate with
// `make cli-iam-actions`. The discovery is internal/iamcoverage (ported from
// spawn): it type-checks the CLI, builds its call graph, and records every AWS
// SDK operation reachable from each command, through spawn and truffle too.
func TestCLIActionInventoryIsCurrent(t *testing.T) {
	rootPre, runnable := cliIAMCommands()
	if len(runnable) < 10 {
		t.Fatalf("found only %d runnable commands; the walk has broken", len(runnable))
	}

	byCommand := map[string]map[string]bool{}
	every := map[string]bool{}
	for _, goos := range cliIAMPlatforms {
		prog, err := iamcoverage.Load(iamcoverage.LoadConfig{Dir: "..", GOOS: goos, GOARCH: "amd64", Patterns: []string{"."}, Root: ".."})
		if err != nil {
			t.Fatal(err)
		}
		var runFns []*ssa.Function
		for _, c := range runnable {
			var roots []*ssa.Function
			for _, fn := range append([]any{c.fn}, c.pre...) {
				f, err := ssaForFunc(prog, fn)
				if err != nil {
					t.Fatalf("%s (GOOS=%s): %v", c.path, goos, err)
				}
				roots = append(roots, f)
			}
			runFns = append(runFns, roots...)
			inv := prog.Calls(roots, nil)
			if inv.Reached == 0 {
				t.Fatalf("%s (GOOS=%s): reached no functions; the call graph is broken", c.path, goos)
			}
			failUnmappedOps(t, c.path, inv)
			if byCommand[c.path] == nil {
				byCommand[c.path] = map[string]bool{}
			}
			for a := range inv.Actions {
				byCommand[c.path][a] = true
			}
		}
		roots := prog.Mains()
		for _, fn := range rootPre {
			f, err := ssaForFunc(prog, fn)
			if err != nil {
				t.Fatalf("root persistent pre-run (GOOS=%s): %v", goos, err)
			}
			roots = append(roots, f)
		}
		inv := prog.Calls(roots, runFns)
		failUnmappedOps(t, "(every command)", inv)
		for a := range inv.Actions {
			every[a] = true
		}
	}

	// Documented intent (#206): cancelling a fired hold releases its capacity
	// reservation with the caller's credentials. If the generator stops seeing
	// that, it is broken, not the command fixed.
	for _, want := range []string{"ec2:CancelCapacityReservation", "ec2:DescribeCapacityReservations"} {
		if !byCommand["lagotto cancel"][want] {
			t.Errorf("the inventory for `lagotto cancel` lacks %s; #206's own example — the discovery is broken", want)
		}
	}

	for c := range nonEverydayCommands {
		if _, ok := byCommand[c]; !ok {
			t.Errorf("nonEverydayCommands names %q, which is not a command (renamed?)", c)
		}
	}

	readmeActionsAreInInventory(t, byCommand)
	checkCLIUserPolicy(t, byCommand)

	doc := renderCLIIAMInventory(every, byCommand)
	path := filepath.Join("..", cliIAMDocPath)
	if *updateCLIIAM {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(doc), 0o644); err != nil { //nolint:gosec // a doc in the repository
			t.Fatal(err)
		}
		return
	}
	got, err := os.ReadFile(path) //nolint:gosec // a doc in the repository
	if err != nil {
		t.Fatalf("read %s: %v (generate it with `make cli-iam-actions`)", cliIAMDocPath, err)
	}
	if string(got) != doc {
		t.Errorf("%s is stale: the CLI's AWS calls changed. Regenerate it with `make cli-iam-actions` "+
			"and review the diff: a new action there is a new permission every user needs.", cliIAMDocPath)
	}
}

// nonEverydayCommands are the commands that are NOT part of a watch user's
// everyday use, and why. Every other command is listed in the generated
// "beyond the tables" section, which is what a user who can only read and write
// lagotto's DynamoDB tables would still be missing.
var nonEverydayCommands = map[string]string{
	"lagotto deploy":   "administrator: creates the poller's Lambda, topic, schedule, tables and IAM roles",
	"lagotto setup":    "administrator: creates the tables and both IAM roles, writes their policies, and writes the lagotto-cli-user policy",
	"lagotto teardown": "administrator: deletes lagotto's DynamoDB tables and, with --iam-roles, its IAM roles and the lagotto-cli-user policy",
	"lagotto poll": "runs the poller locally, so it needs what the hosted poller's role has, including writing and passing " +
		"instance roles; lagotto-cli-user deliberately does not cover it",
}

// readmeActionsAreInInventory: the README's hand-written permissions summary
// names some actions; each must be one the generated inventory says some
// command needs, so the summary cannot drift from the code.
func readmeActionsAreInInventory(t *testing.T, byCommand map[string]map[string]bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	section := regexp.MustCompile(`(?s)### Permissions your own credentials need\n(.*?)\n### `).FindStringSubmatch(string(data))
	if section == nil {
		t.Fatal("README.md has no \"### Permissions your own credentials need\" section (#206)")
	}
	if !strings.Contains(section[1], cliIAMDocPath) {
		t.Errorf("the README permissions section does not link %s", cliIAMDocPath)
	}
	named := regexp.MustCompile("`([a-z0-9-]+:[A-Za-z*]+)`").FindAllStringSubmatch(section[1], -1)
	if len(named) == 0 {
		t.Fatal("found no `svc:Action` in the README permissions section; the matcher is broken")
	}
	for _, m := range named {
		if strings.HasSuffix(m[1], "*") {
			continue
		}
		found := false
		for _, acts := range byCommand {
			found = found || acts[m[1]]
		}
		if !found {
			t.Errorf("README names %s, which no command's generated inventory contains", m[1])
		}
	}
}

func failUnmappedOps(t *testing.T, what string, inv iamcoverage.Inventory) {
	t.Helper()
	for _, u := range inv.Unmapped {
		t.Errorf("%s: an AWS operation this inventory cannot map to an IAM action: %s", what, u)
	}
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func renderCLIIAMInventory(every map[string]bool, byCommand map[string]map[string]bool) string {
	var b strings.Builder
	b.WriteString("# IAM actions the lagotto CLI calls\n\n" +
		"<!-- Generated by `make cli-iam-actions` (internal/iamcoverage, #206). Do not edit by hand. -->\n\n" +
		"The lagotto CLI runs with **your** AWS credentials, so every action below is one\n" +
		"your IAM identity needs for the command that calls it. (The hosted poller runs\n" +
		"as its own role, whose policy `lagotto setup` writes; that is not this list.)\n" +
		"The list is discovered from the code, not maintained by hand: the generator\n" +
		"type-checks the CLI, builds its call graph, and records every AWS SDK operation\n" +
		"reachable from each command's run function, including calls made through spawn\n" +
		"and truffle. CI fails when the code and this page disagree, so a new AWS call\n" +
		"cannot ship without appearing here.\n\n" +
		"How to read it:\n\n" +
		"- **It over-approximates.** A command is listed with every action any of its\n" +
		"  code paths can reach, including flags you may not use and error paths.\n" +
		"  `lagotto poll` runs the poller locally, so it needs what the hosted poller's\n" +
		"  role has, including launching instances with `--action spawn`.\n" +
		"- **Tag-on-create and `iam:PassRole` are listed wherever the operation is\n" +
		"  reachable**, whether or not that command sets tags or passes a role:\n" +
		"  `ec2:RunInstances` also needs `ec2:CreateTags` (tags) and `iam:PassRole`\n" +
		"  (an instance profile), `scheduler:CreateSchedule` needs `iam:PassRole` on the\n" +
		"  role the schedule assumes (`lagotto-capacity-poller-scheduler-invoke`), and so on.\n" +
		"- **It names actions, not resources.** Grant `iam:PassRole` on the specific\n" +
		"  roles, not on `*`.\n" +
		"- `sts:GetCallerIdentity` needs no permission and is omitted.\n\n" +
		"## Outside any command\n\n" +
		"What runs outside every command's own code: startup and the root command's\n" +
		"persistent pre-run (the `--account` guard):\n\n")
	if len(every) == 0 {
		b.WriteString("None.\n")
	} else {
		for _, a := range sortedSet(every) {
			fmt.Fprintf(&b, "- `%s`\n", a)
		}
	}

	b.WriteString("\n## Everyday commands: what they need beyond lagotto's tables\n\n" +
		"The commands a watch user runs day to day, and the actions each needs besides\n" +
		"`dynamodb:*` on lagotto's own tables (`lagotto-watches`, `lagotto-match-history`,\n" +
		"`lagotto-scheduled-launches`). These are the ones a user who was granted only the\n" +
		"tables is missing. The administrator commands are listed after the table.\n\n" +
		"`lagotto setup` writes all of these, resource-scoped, as the customer-managed\n" +
		"policy `lagotto-cli-user` (lagotto#232): attach it to whoever runs these commands.\n" +
		"It is built in `pkg/cliiam` and held to this page in both directions: CI fails\n" +
		"when an everyday command calls an action the policy does not grant, or the\n" +
		"policy grants one no everyday command calls. `s3:GetObject` is granted only on\n" +
		"the buckets named with `setup --config-bucket`; `lagotto doctor` simulates your\n" +
		"identity against the policy and lists what is missing.\n\n" +
		"| Command | Beyond the tables |\n| --- | --- |\n")
	everyday := make([]string, 0, len(byCommand))
	for c := range byCommand {
		if _, skip := nonEverydayCommands[c]; !skip {
			everyday = append(everyday, c)
		}
	}
	sort.Strings(everyday)
	for _, c := range everyday {
		var extra []string
		for _, a := range sortedSet(byCommand[c]) {
			if !strings.HasPrefix(a, "dynamodb:") {
				extra = append(extra, "`"+a+"`")
			}
		}
		cell := "nothing"
		if len(extra) > 0 {
			cell = strings.Join(extra, ", ")
		}
		fmt.Fprintf(&b, "| `%s` | %s |\n", c, cell)
	}
	b.WriteString("\nNot everyday use:\n\n")
	nonEveryday := make([]string, 0, len(nonEverydayCommands))
	for c := range nonEverydayCommands {
		nonEveryday = append(nonEveryday, c)
	}
	sort.Strings(nonEveryday)
	for _, c := range nonEveryday {
		fmt.Fprintf(&b, "- `%s`: %s.\n", c, nonEverydayCommands[c])
	}

	b.WriteString("\n## By command\n\n| Command | IAM actions |\n| --- | --- |\n")
	byAction := map[string][]string{}
	cmds := make([]string, 0, len(byCommand))
	for c := range byCommand {
		cmds = append(cmds, c)
	}
	sort.Strings(cmds)
	for _, c := range cmds {
		acts := sortedSet(byCommand[c])
		cell := "none"
		if len(acts) > 0 {
			quoted := make([]string, len(acts))
			for i, a := range acts {
				quoted[i] = "`" + a + "`"
				byAction[a] = append(byAction[a], c)
			}
			cell = strings.Join(quoted, ", ")
		}
		fmt.Fprintf(&b, "| `%s` | %s |\n", c, cell)
	}

	b.WriteString("\n## By action\n\n| IAM action | Commands |\n| --- | --- |\n")
	actions := make([]string, 0, len(byAction))
	for a := range byAction {
		actions = append(actions, a)
	}
	sort.Strings(actions)
	for _, a := range actions {
		quoted := make([]string, len(byAction[a]))
		for i, c := range byAction[a] {
			quoted[i] = "`" + c + "`"
		}
		fmt.Fprintf(&b, "| `%s` | %s |\n", a, strings.Join(quoted, ", "))
	}
	return b.String()
}
