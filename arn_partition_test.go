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

// allowedLiteral is one non-test Go string literal, exactly as written in
// its file, that may name the aws partition.
type allowedLiteral struct{ file, literal string }

// hardCodedPartitionAllowed are those literals, with why. Each is allowed
// once: a second copy of one, or any other, fails. Everything else builds
// its ARNs with pkg/awsarn, in the account's partition.
var hardCodedPartitionAllowed = map[allowedLiteral]string{
	{"pkg/watcher/launchpolicy.go", `"%q is not an IAM role ARN (arn:aws:iam::<account>:role/<name>)"`}: "an example of the format in ParseRoleARN's error; its pattern accepts every partition",
}

// TestNoHardCodedPartition (lagotto#347): no string in lagotto's Go code names
// the aws partition. lagotto wrote arn:aws: into every ARN it built, so its
// IAM policies named nothing in aws-us-gov or aws-cn, the lifecycle lease
// table included. A comment may still show one, as an example.
func TestNoHardCodedPartition(t *testing.T) {
	fset := token.NewFileSet()
	seen := map[allowedLiteral]int{}
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
			key := allowedLiteral{filepath.ToSlash(path), lit.Value}
			if _, ok := hardCodedPartitionAllowed[key]; ok {
				if seen[key]++; seen[key] == 1 {
					return true
				}
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
	for key, why := range hardCodedPartitionAllowed {
		if seen[key] == 0 {
			t.Errorf("%s is allowed %s (%s) but no longer has it: drop it from the list", key.file, key.literal, why)
		}
	}
}
