package cmd

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// TestSignalHandlers (lagotto#351, seventh review): only the commands that
// hold a teardown lease or a creator's marks install a SIGINT/SIGTERM
// handler (commandContext), and their work honours its cancellation (no
// context.Background() in them); every other command keeps Go's default, so
// Ctrl-C exits it at once. It walks every command's RunE.
func TestSignalHandlers(t *testing.T) {
	fset := token.NewFileSet()
	files, _ := filepath.Glob("*.go")
	bodies := map[string]*ast.BlockStmt{}
	callers := map[string]bool{} // functions that call commandContext
	notify := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			bodies[fn.Name.Name] = fn.Body
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch f := call.Fun.(type) {
				case *ast.Ident:
					if f.Name == "commandContext" {
						callers[fn.Name.Name] = true
					}
				case *ast.SelectorExpr:
					if x, ok := f.X.(*ast.Ident); ok && x.Name == "signal" && f.Sel.Name == "NotifyContext" {
						notify++
						// runPollDaemon's foreground loop (#30) stops on its
						// own handler's context: the one other.
						if fn.Name.Name != "commandContext" && fn.Name.Name != "runPollDaemon" {
							t.Errorf("%s installs a signal handler; only commandContext may", fn.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
	if notify != 2 {
		t.Errorf("%d signal.NotifyContext calls, want commandContext's and runPollDaemon's", notify)
	}
	allowed := map[string]bool{}
	for _, n := range signalCommands {
		allowed[n] = true
		if !callers[n] {
			t.Errorf("%s holds a lease or marks but does not run under commandContext", n)
		}
		if b := bodies[n]; b != nil && strings.Contains(nodeText(t, fset, b), "context.Background()") {
			t.Errorf("%s runs some work with context.Background(), which a signal does not cancel", n)
		}
	}
	for n := range callers {
		if !allowed[n] {
			t.Errorf("%s calls commandContext but is not in signalCommands", n)
		}
	}
	// Every command: its RunE is a signal command, or installs nothing.
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if c.RunE != nil {
			name := runtime.FuncForPC(reflect.ValueOf(c.RunE).Pointer()).Name()
			name = name[strings.LastIndex(name, ".")+1:]
			if callers[name] && !allowed[name] {
				t.Errorf("%s (%s) installs a signal handler", c.CommandPath(), name)
			}
		}
		for _, s := range c.Commands() {
			walk(s)
		}
	}
	walk(rootCmd)
}

func nodeText(t *testing.T, fset *token.FileSet, n ast.Node) string {
	t.Helper()
	start, end := fset.Position(n.Pos()), fset.Position(n.End())
	b, err := os.ReadFile(start.Filename)
	if err != nil {
		t.Fatal(err)
	}
	return string(b[start.Offset:end.Offset])
}

// TestCommandContextCancelsOnSignal: a SIGINT cancels commandContext's
// context, and stop restores the default.
func TestCommandContextCancelsOnSignal(t *testing.T) {
	ctx, stop := commandContext(nil)
	defer stop()
	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("not cancelled by SIGINT")
	}
	c := &cobra.Command{}
	c.SetContext(ctx)
	ctx2, stop2 := commandContext(c)
	stop2()
	if ctx2.Err() == nil {
		t.Error("stop did not end the context")
	}
}
