package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHostedPollerUsesSharedNotifier gates the hosted path of #203: the
// Lambda's match notifications must be the ones pkg/watcher builds (which carry
// a fired hold's reservation id and AZ), so this package must construct its
// notifier with watcher.NewNotifier, pass it to the poller, and never publish
// to SNS itself. A Lambda-local Publish would be a second, unguarded body.
func TestHostedPollerUsesSharedNotifier(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	newNotifier, publishes, notifierOpt := 0, 0, 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok && id.Name == "watcher" && sel.Sel.Name == "NewNotifier" {
						newNotifier++
					}
					if sel.Sel.Name == "Publish" {
						publishes++
					}
				}
			case *ast.KeyValueExpr:
				if k, ok := x.Key.(*ast.Ident); ok && k.Name == "Notifier" {
					notifierOpt++
				}
			}
			return true
		})
	}
	if newNotifier != 1 {
		t.Errorf("watcher.NewNotifier called %d times in the Lambda, want 1", newNotifier)
	}
	if notifierOpt != 1 {
		t.Errorf("PollerOpts Notifier set %d times, want 1", notifierOpt)
	}
	if publishes != 0 {
		t.Errorf("the Lambda calls Publish %d times itself; match notifications must come from pkg/watcher", publishes)
	}
}
