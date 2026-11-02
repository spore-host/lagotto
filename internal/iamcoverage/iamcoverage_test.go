package iamcoverage

import (
	"go/types"
	"reflect"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

const probePkg = "github.com/spore-host/lagotto/internal/iamcoverage/testdata/probe"

func loadProbe(t *testing.T) *Program {
	t.Helper()
	prog, err := Load(LoadConfig{Dir: "../..", GOOS: "linux", GOARCH: "amd64", Patterns: []string{"./internal/iamcoverage/testdata/probe"}, Root: "../.."})
	if err != nil {
		t.Fatal(err)
	}
	return prog
}

func probeFunc(t *testing.T, prog *Program, name string) *ssa.Function {
	t.Helper()
	f, err := prog.Func(probePkg + "." + name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestCallsFindsEveryShapeOfOperation: each way the probe reaches AWS maps to
// its IAM action, attributed to the function that makes it.
func TestCallsFindsEveryShapeOfOperation(t *testing.T) {
	prog := loadProbe(t)
	for fn, want := range map[string][]string{
		"direct":       {"ec2:CreateTags", "ec2:RunInstances", "iam:PassRole"}, // implied rules apply
		"viaInterface": {"dynamodb:GetItem"},
		"methodValue":  {"s3:ListBucket"}, // HeadBucket is authorized as s3:ListBucket
		"paginator":    {"ec2:DescribeInstances"},
		"assumeRole":   {"sts:AssumeRole"},
		"identity":     nil, // sts:GetCallerIdentity needs no permission
	} {
		inv := prog.Calls([]*ssa.Function{probeFunc(t, prog, fn)}, nil)
		if len(inv.Unmapped) > 0 {
			t.Errorf("%s: unmapped %v", fn, inv.Unmapped)
		}
		if got := inv.Sorted(); !reflect.DeepEqual(got, want) && (len(got) != 0 || len(want) != 0) {
			t.Errorf("%s: actions %v, want %v", fn, got, want)
		}
		if inv.Reached == 0 {
			t.Errorf("%s: reached nothing", fn)
		}
	}
}

// TestCallsFromMainAndCuts: main reaches all of the probe's reachable calls,
// a cut removes its subtree, and the path to a call is reported.
func TestCallsFromMainAndCuts(t *testing.T) {
	prog := loadProbe(t)
	if len(prog.Mains()) != 1 {
		t.Fatalf("Mains = %v, want the probe's main", prog.Mains())
	}
	all := prog.Calls(prog.Mains(), nil)
	for _, a := range []string{"ec2:RunInstances", "dynamodb:GetItem", "s3:ListBucket", "ec2:DescribeInstances", "sts:AssumeRole"} {
		if _, ok := all.Actions[a]; !ok {
			t.Errorf("main does not reach %s: %v", a, all.Sorted())
		}
	}
	if sites := all.Actions["ec2:RunInstances"]; len(sites) == 0 || !strings.Contains(sites[0].Pos, "testdata/probe/main.go:") || sites[0].Func != probePkg+".direct" {
		t.Errorf("RunInstances site = %+v", sites)
	}
	if path := all.PathTo(probePkg + ".direct"); len(path) < 2 || path[0] != probePkg+".main" {
		t.Errorf("PathTo(direct) = %v", path)
	}
	if all.PathTo("no.such.Func") != nil {
		t.Error("PathTo of an unknown function is not nil")
	}

	cut := prog.Calls(prog.Mains(), []*ssa.Function{probeFunc(t, prog, "direct")})
	if _, ok := cut.Actions["ec2:RunInstances"]; ok {
		t.Error("cutting direct still reports its RunInstances")
	}
	if _, ok := cut.Actions["dynamodb:GetItem"]; !ok {
		t.Error("the cut removed more than its own subtree")
	}
}

func TestFuncAndFuncsAt(t *testing.T) {
	prog := loadProbe(t)
	if _, err := prog.Func(probePkg + ".renamedAway"); err == nil {
		t.Error("Func of a missing name is not an error")
	}
	d := probeFunc(t, prog, "direct")
	pos := prog.fset.Position(d.Pos())
	got := prog.FuncsAt(probePkg, "main.go", pos.Line)
	if len(got) != 1 || got[0] != d {
		t.Errorf("FuncsAt(main.go:%d) = %v, want [direct]", pos.Line, got)
	}
	if got := prog.FuncsAt(probePkg, "main.go", 1); len(got) != 0 {
		t.Errorf("FuncsAt(line 1) = %v, want none", got)
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(LoadConfig{Dir: "../..", Patterns: []string{"."}}); err == nil {
		t.Error("Load without GOOS/GOARCH did not fail")
	}
	if _, err := Load(LoadConfig{Dir: "../..", GOOS: "linux", GOARCH: "amd64", Patterns: []string{"./pkg/failure"}}); err == nil || !strings.Contains(err.Error(), "no main package") {
		t.Errorf("Load of a non-main package: %v", err)
	}
	if _, err := Load(LoadConfig{Dir: "../..", GOOS: "linux", GOARCH: "amd64", Patterns: []string{"./does/not/exist"}}); err == nil {
		t.Error("Load of a missing package did not fail")
	}
}

func TestMapOp(t *testing.T) {
	for _, tc := range []struct {
		pkg, op string
		want    []string
		errLike string
	}{
		{sdkServicePrefix + "ec2", "DescribeInstances", []string{"ec2:DescribeInstances"}, ""},
		{sdkServicePrefix + "s3", "HeadObject", []string{"s3:GetObject"}, ""},
		{sdkServicePrefix + "lambda", "Invoke", []string{"lambda:InvokeFunction"}, ""},
		{sdkServicePrefix + "sts", "GetCallerIdentity", []string{}, ""},
		{sdkServicePrefix + "nosuchservice", "Op", nil, "no IAM service prefix"},
		{sdkRoot + "feature/s3/transfermanager", "UploadObject", []string{"s3:PutObject", "s3:AbortMultipartUpload"}, ""},
		{sdkRoot + "feature/s3/transfermanager", "Unknown", nil, "add it to featureOps"},
		{sdkRoot + "feature/ec2/imds", "GetMetadata", nil, ""},
		{sdkRoot + "aws", "Thing", nil, "neither a service nor a mapped feature package"},
	} {
		op, ok := mapOp(tc.pkg, tc.op, "d")
		if !ok {
			t.Errorf("%s.%s: not recognised", tc.pkg, tc.op)
			continue
		}
		if tc.errLike != "" {
			if !strings.Contains(op.err, tc.errLike) {
				t.Errorf("%s.%s: err %q, want %q", tc.pkg, tc.op, op.err, tc.errLike)
			}
			continue
		}
		if op.err != "" || (len(op.actions) != 0 || len(tc.want) != 0) && !reflect.DeepEqual(op.actions, tc.want) {
			t.Errorf("%s.%s: actions %v err %q, want %v", tc.pkg, tc.op, op.actions, op.err, tc.want)
		}
	}
	if _, ok := mapOp(sdkServicePrefix+"ec2/types", "X", "d"); ok {
		t.Error("a types package was treated as an operation")
	}
}

// TestApplyImplied: a rule adds its actions to its operation only, and a rule
// naming a field the input type lacks is an error (an SDK rename cannot switch
// a rule off silently).
func TestApplyImplied(t *testing.T) {
	op := applyImplied(foundOp{key: "ec2:RunInstances", actions: []string{"ec2:RunInstances"}})
	sort.Strings(op.actions)
	if !reflect.DeepEqual(op.actions, []string{"ec2:CreateTags", "ec2:RunInstances", "iam:PassRole"}) {
		t.Errorf("RunInstances implied = %v", op.actions)
	}
	if got := applyImplied(foundOp{key: "ec2:DescribeInstances", actions: []string{"ec2:DescribeInstances"}}); len(got.actions) != 1 {
		t.Errorf("a rule-less op gained actions: %v", got.actions)
	}

	// A rule naming a field the input type lacks is an error, not a skip.
	in := types.NewPointer(types.NewNamed(types.NewTypeName(0, nil, "RunInstancesInput", nil),
		types.NewStruct([]*types.Var{types.NewField(0, nil, "ImageId", types.Typ[types.String], false)}, nil), nil))
	bad := applyImplied(foundOp{key: "ec2:RunInstances", actions: []string{"ec2:RunInstances"}, input: in})
	if !strings.Contains(bad.err, "does not have") || bad.actions != nil {
		t.Errorf("missing field: err %q actions %v", bad.err, bad.actions)
	}
}

func TestHasFieldPath(t *testing.T) {
	str := types.Typ[types.String]
	target := types.NewStruct([]*types.Var{types.NewField(0, nil, "RoleArn", str, false)}, nil)
	outer := types.NewPointer(types.NewStruct([]*types.Var{
		types.NewField(0, nil, "Target", types.NewPointer(target), false),
		types.NewField(0, nil, "Targets", types.NewSlice(target), false),
		types.NewField(0, nil, "Name", str, false),
	}, nil))
	for path, want := range map[string]bool{
		"Target.RoleArn": true, "Targets.RoleArn": true, "Name": true,
		"Target.Nope": false, "Missing": false, "Name.Deeper": false,
	} {
		if got := hasFieldPath(outer, strings.Split(path, ".")); got != want {
			t.Errorf("hasFieldPath(%s) = %v, want %v", path, got, want)
		}
	}
}

func TestActionsForAndHelpers(t *testing.T) {
	if got := ActionsFor("ec2", "RunInstances"); !reflect.DeepEqual(got, []string{"ec2:RunInstances"}) {
		t.Errorf("ActionsFor = %v", got)
	}
	if pkgBase("a/b/c") != "c" || pkgBase("c") != "c" {
		t.Error("pkgBase")
	}
	if !containsString([]string{"a", "b"}, "b") || containsString(nil, "a") {
		t.Error("containsString")
	}
}
