package cmd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestNoInertFlags is the gate for the #179 class of bug: a flag registered
// with a pflag *Var function (BoolVar, StringVar, IntVar, ...) whose bound
// variable nothing ever reads, so the flag parses and is documented in --help
// but has no effect. lagotto's `--account` ("Expected AWS account ID (optional
// guard)") shipped that way until #179, as truffle's did (truffle#199/#206);
// this gate is ported from truffle's cmd/inert_flags_test.go.
//
// It parses this package's non-test sources (no allowlist, no hand-kept list
// of flags), finds every `<x>.Flags().<T>Var(&v, "name", ...)` and
// `<x>.PersistentFlags().<T>Var(&v, ...)` registration — including through a
// local alias, `f := <x>.Flags(); f.<T>Var(...)`, which lagotto's deploy and
// launch commands use — and requires each
// bound variable to be read somewhere else in the package. A read is any use
// of the identifier other than its declaration, its registration, or the
// left-hand side of a plain assignment.
//
// Flags registered without a variable (`Flags().Bool("name", ...)`) are read
// by name, so for those it requires the name to be looked up somewhere
// (`GetBool("name")`, `Changed("name")`, `Lookup("name")`, ...).
//
// A flag marked deprecated (`Flags().MarkDeprecated("name", ...)`) is exempt:
// that is cobra's own declaration that it is kept only so old invocations
// still parse, and it tells the user so on every use. That is the sanctioned
// way out, not an allowlist in this test.
//
// It is syntactic, so it errs towards passing: a read in an unrelated function,
// or of a same-named local, counts. That makes it a floor, not a proof, but it
// never fails a flag that works.
func TestNoInertFlags(t *testing.T) {
	inert := findInertFlags(t, ".")
	if len(inert) > 0 {
		t.Errorf("flags registered but never read (they parse, show in --help, and do nothing):\n  %s\n"+
			"read the bound variable where the flag should take effect, or remove/deprecate the flag (#179)",
			strings.Join(inert, "\n  "))
	}
}

// TestInertFlagGateCatchesAnUnreadFlag proves the gate is not vacuous: a
// package with one read flag and one unread flag reports exactly the unread one.
func TestInertFlagGateCatchesAnUnreadFlag(t *testing.T) {
	dir := t.TempDir()
	src := `package fake

import "github.com/spf13/cobra"

var (
	used, unused, assignedOnly, viaAlias, viaAliasUsed bool
	named                      int
	c                          = &cobra.Command{}
)

func init() {
	c.Flags().BoolVar(&used, "used", false, "")
	c.Flags().BoolVar(&unused, "unused", false, "")
	c.PersistentFlags().BoolVar(&assignedOnly, "assigned-only", false, "")
	c.Flags().Int("named", 0, "")
	c.Flags().Int("named-unread", 0, "")
	c.Flags().Bool("deprecated", false, "")
	_ = c.Flags().MarkDeprecated("deprecated", "no effect")
	f := c.Flags()
	f.BoolVar(&viaAlias, "via-alias", false, "")
	f.BoolVarP(&viaAliasUsed, "via-alias-used", "a", false, "")
}

func run() {
	assignedOnly = false
	if used && viaAliasUsed {
		_, _ = c.Flags().GetInt("named")
	}
}
`
	if err := os.WriteFile(filepath.Join(dir, "fake.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	got := findInertFlags(t, dir)
	want := []string{
		`--assigned-only (bound to assignedOnly) in fake.go`,
		`--named-unread (no variable; never looked up by name) in fake.go`,
		`--unused (bound to unused) in fake.go`,
		`--via-alias (bound to viaAlias) in fake.go`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("findInertFlags =\n  %s\nwant\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// flagRegistration is one `Flags().<T>[Var][P](...)` call.
type flagRegistration struct {
	name    string    // flag name
	varName string    // bound variable identifier ("" for non-Var registrations)
	varPos  token.Pos // position of the &v identifier, excluded from reads
	file    string
}

// findInertFlags parses the non-test Go files in dir and returns a sorted
// description of every flag that nothing reads.
func findInertFlags(t *testing.T, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, p := range paths {
		if strings.HasSuffix(p, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatalf("no Go sources in %s", dir)
	}

	// Local aliases for a flag set: `f := cmd.Flags()`. Matched by name, which is
	// loose (any `f` counts once one is an alias) but only ever widens what is
	// recognised as a registration.
	flagSetAliases := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if a, ok := n.(*ast.AssignStmt); ok && len(a.Lhs) == len(a.Rhs) {
				for i, rhs := range a.Rhs {
					if id, ok := a.Lhs[i].(*ast.Ident); ok && isFlagSet(rhs, nil) {
						flagSetAliases[id.Name] = true
					}
				}
			}
			return true
		})
	}

	var regs []flagRegistration
	excluded := map[token.Pos]bool{} // idents that are not reads
	lookedUp := map[string]bool{}    // flag names passed to Get*/Changed/Lookup
	deprecated := map[string]bool{}  // flag names passed to MarkDeprecated
	for _, f := range files {
		file := filepath.Base(fset.Position(f.Pos()).Filename)
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ValueSpec:
				for _, id := range n.Names {
					excluded[id.Pos()] = true
				}
			case *ast.AssignStmt:
				if n.Tok == token.ASSIGN || n.Tok == token.DEFINE {
					for _, lhs := range n.Lhs {
						if id, ok := lhs.(*ast.Ident); ok {
							excluded[id.Pos()] = true
						}
					}
				}
			case *ast.CallExpr:
				if r, ok := parseFlagRegistration(n, flagSetAliases); ok {
					r.file = file
					if r.varName != "" {
						excluded[r.varPos] = true
					}
					regs = append(regs, r)
				} else if name, ok := flagNameCall(n, "MarkDeprecated"); ok {
					deprecated[name] = true
				} else if name, ok := flagNameLookup(n); ok {
					lookedUp[name] = true
				}
			}
			return true
		})
	}

	reads := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && !excluded[id.Pos()] {
				reads[id.Name] = true
			}
			return true
		})
	}

	// A discovery that found nothing has broken, not proven the package clean.
	if len(regs) == 0 {
		t.Fatalf("found no flag registrations in %s; the parser no longer recognises how flags are registered", dir)
	}

	var inert []string
	for _, r := range regs {
		switch {
		case deprecated[r.name]:
			// kept only for compatibility, and says so
		case r.varName != "" && !reads[r.varName]:
			inert = append(inert, "--"+r.name+" (bound to "+r.varName+") in "+r.file)
		case r.varName == "" && !lookedUp[r.name]:
			inert = append(inert, "--"+r.name+" (no variable; never looked up by name) in "+r.file)
		}
	}
	sort.Strings(inert)
	return inert
}

// parseFlagRegistration recognises `<x>.Flags().<Method>(...)` and
// `<x>.PersistentFlags().<Method>(...)` where Method is a pflag definer
// (Bool, BoolVar, BoolVarP, StringSlice, DurationVar, ...).
func parseFlagRegistration(call *ast.CallExpr, aliases map[string]bool) (flagRegistration, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isFlagSet(sel.X, aliases) {
		return flagRegistration{}, false
	}
	method := sel.Sel.Name
	if !isDefiner(method) {
		return flagRegistration{}, false
	}
	var r flagRegistration
	args := call.Args
	if strings.Contains(method, "Var") {
		if len(args) < 2 {
			return flagRegistration{}, false
		}
		u, ok := args[0].(*ast.UnaryExpr)
		if !ok || u.Op != token.AND {
			return flagRegistration{}, false
		}
		id, ok := u.X.(*ast.Ident)
		if !ok {
			// &s.field and similar: not tracked.
			return flagRegistration{}, false
		}
		r.varName, r.varPos = id.Name, id.Pos()
		args = args[1:]
	}
	if len(args) == 0 {
		return flagRegistration{}, false
	}
	name, ok := stringLit(args[0])
	if !ok {
		return flagRegistration{}, false
	}
	r.name = name
	return r, true
}

// isFlagSet reports whether e is `<x>.Flags()` / `<x>.PersistentFlags()` /
// `<x>.LocalFlags()`, or an identifier in aliases that was assigned one.
func isFlagSet(e ast.Expr, aliases map[string]bool) bool {
	if id, ok := e.(*ast.Ident); ok {
		return aliases[id.Name]
	}
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch sel.Sel.Name {
	case "Flags", "PersistentFlags", "LocalFlags":
		return true
	}
	return false
}

// pflagTypes are the value types pflag has definers for; a definer is
// <Type>, <Type>P, <Type>Var or <Type>VarP.
var pflagTypes = []string{
	"Bool", "BoolSlice", "BytesBase64", "BytesHex", "Count", "Duration", "DurationSlice",
	"Float32", "Float32Slice", "Float64", "Float64Slice", "IP", "IPMask", "IPNet", "IPSlice",
	"Int", "Int8", "Int16", "Int32", "Int32Slice", "Int64", "Int64Slice", "IntSlice",
	"String", "StringArray", "StringSlice", "StringToInt", "StringToInt64", "StringToString",
	"Uint", "Uint8", "Uint16", "Uint32", "Uint64", "UintSlice",
}

func isDefiner(method string) bool {
	if method == "Var" || method == "VarP" {
		return false // generic Value; its variable is the Value itself
	}
	for _, typ := range pflagTypes {
		for _, suffix := range []string{"", "P", "Var", "VarP"} {
			if method == typ+suffix {
				return true
			}
		}
	}
	return false
}

// flagNameLookup recognises a by-name read of a flag: any method call named
// Get*, Changed or Lookup whose first argument is a string literal.
func flagNameLookup(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) == 0 {
		return "", false
	}
	m := sel.Sel.Name
	if !strings.HasPrefix(m, "Get") && m != "Changed" && m != "Lookup" {
		return "", false
	}
	return stringLit(call.Args[0])
}

// flagNameCall recognises `<x>.<method>("name", ...)` and returns the name.
func flagNameCall(call *ast.CallExpr, method string) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != method || len(call.Args) == 0 {
		return "", false
	}
	return stringLit(call.Args[0])
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}
