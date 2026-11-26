package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// hardCodedPartitionAllowed are the non-test Go string literals that may name
// the aws partition, by file, with why. Everything else builds its ARNs with
// pkg/awsarn, in the account's partition.
var hardCodedPartitionAllowed = map[string]string{
	"pkg/watcher/launchpolicy.go": "an example of the format in ParseRoleARN's error; its pattern accepts every partition",
}

// TestNoHardCodedPartition (lagotto#347): no string in lagotto's Go code names
// the aws partition. lagotto wrote arn:aws: into every ARN it built, so its
// IAM policies named nothing in aws-us-gov or aws-cn, the lifecycle lease
// table included. A comment may still show one, as an example.
func TestNoHardCodedPartition(t *testing.T) {
	fset := token.NewFileSet()
	seen := map[string]bool{}
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == ".git" || n == "testdata" || n == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING || !strings.Contains(lit.Value, "arn:aws:") {
				return true
			}
			file := filepath.ToSlash(path)
			if _, ok := hardCodedPartitionAllowed[file]; ok {
				seen[file] = true
				return true
			}
			t.Errorf("%s: %s names the aws partition; build the ARN with pkg/awsarn, in the account's partition (lagotto#347)",
				fset.Position(lit.Pos()), lit.Value)
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for file, why := range hardCodedPartitionAllowed {
		if !seen[file] {
			t.Errorf("%s is allowed an aws-partition string (%s) but has none: drop it from the list", file, why)
		}
	}
}
