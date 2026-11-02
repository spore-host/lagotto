package hermetic

import (
	"bufio"
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The #214 gate. It DISCOVERS every function in lagotto (both modules) that can
// build an AWS config — directly, or through any chain of calls, including calls
// into spawn/truffle/libs — and asserts that none of them is reachable from a
// test without a fake:
//
//  1. Only three functions may call an aws-sdk-go-v2/config loader directly:
//     awscfg's loader (which panics under a test binary unless the test
//     installed a fake), the Lambda's setup (reachable only from main), and
//     testutil.SubstrateServer (pinned to the emulator with static credentials).
//     Any other direct call bypasses the awscfg guard.
//  2. No config loader is reachable from package init or a package-level var
//     initializer. Those run in the test binary BEFORE TestMain, so nothing in
//     the test can make them hermetic (the Lambda's init did this until #214).
//  3. Every test binary that links a config-loading entry point has a TestMain
//     that calls hermetic.Main or hermetic.Run, so a plain `go test` (not just
//     `make test` under the -exec wrapper) strips the ambient credentials and
//     fails on any request that would leave the machine.
//
// The entry points that load config through spawn or truffle (truffleaws.NewClient,
// spawnaws.NewClient, ...) never touch awscfg, so rule 3's environment is what
// stops them; the inventory below says which guard covers each.

const awsConfigPkg = "github.com/aws/aws-sdk-go-v2/config"

// directLoaderAllowed are the only functions that may call an awsConfigPkg
// loader directly, with the reason each one is safe.
var directLoaderAllowed = map[string]string{
	"github.com/spore-host/lagotto/pkg/awscfg.loader":            "panics under a test binary unless awscfg.SetLoaderForTest installed a fake",
	"github.com/spore-host/lagotto/lambda/capacity-poller.setup": "called only from main(), which no test runs",
	"github.com/spore-host/lagotto/pkg/testutil.SubstrateServer": "pinned to the Substrate emulator's loopback URL with static test credentials",
}

// wrapperSelfTest is the -exec wrapper's own package. Its tests run a helper
// that deliberately reaches the network under the wrapper.
const wrapperSelfTest = "github.com/spore-host/lagotto/test/hermetic"

// repoRoot is the root module's directory, relative to this package.
const repoRoot = "../.."

// modules are the Go modules in this repository, relative to repoRoot.
var modules = []string{".", "lambda/capacity-poller"}

type listedPkg struct {
	importPath string
	dir        string
	goFiles    []string
	deps       []string
	testFiles  []string // _test.go files, internal and external
}

// goList runs `go list` offline in dir. The module cache must already hold
// every dependency (CI downloads both modules' dependencies first).
func goList(t *testing.T, dir string, args ...string) []listedPkg {
	t.Helper()
	full := append([]string{"list", "-f", "{{.ImportPath}}\t{{.Dir}}\t{{join .GoFiles \",\"}}\t{{join .Deps \",\"}}\t{{join .TestGoFiles \",\"}},{{join .XTestGoFiles \",\"}}"}, args...)
	cmd := exec.Command("go", full...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOFLAGS=-mod=readonly")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go %s in %s: %v\n%s\n(run `go mod download` in every module first; this gate lists packages offline)", strings.Join(full, " "), dir, err, stderr.String())
	}
	var pkgs []listedPkg
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if len(f) != 5 {
			t.Fatalf("unexpected go list line %q", sc.Text())
		}
		split := func(s string) []string {
			if s == "" {
				return nil
			}
			var out []string
			for _, x := range strings.Split(s, ",") {
				if x != "" {
					out = append(out, x)
				}
			}
			return out
		}
		pkgs = append(pkgs, listedPkg{importPath: f[0], dir: f[1], goFiles: split(f[2]), deps: split(f[3]), testFiles: split(f[4])})
	}
	return pkgs
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// funcInfo is one function (or package initializer) and what it calls.
type funcInfo struct {
	key      string
	pkg      string
	pos      string
	calls    []string // resolved keys of every function it calls or references
	initLike bool     // init() or a package-level var initializer
	// initCalls are the calls an initializer EXECUTES at package init: calls
	// outside any func literal. A cobra command literal that names RunE: runWatch
	// references runWatch without running it.
	initCalls []string
	exported  bool
}

// callGraph parses pkgs' non-test files and returns every function, keyed
// pkgpath.Name / pkgpath.Recv.Name. Calls are resolved by name: pkg.F through
// the file's imports, F within the package, and x.M() — a method call whose
// receiver type can't be named without type-checking — to every method named M
// in the scanned set (an over-approximation, which is the safe direction for a
// gate that asks "could this reach a loader").
func callGraph(t *testing.T, pkgs []listedPkg) (map[string]*funcInfo, map[string][]string) {
	t.Helper()
	funcs := map[string]*funcInfo{}
	methodsByName := map[string][]string{}
	type pending struct {
		fi      *funcInfo
		methods []string
	}
	var unresolved, initMethods []pending
	fset := token.NewFileSet()
	for _, p := range pkgs {
		for _, name := range p.goFiles {
			path := filepath.Join(p.dir, name)
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			imports := map[string]string{}
			for _, imp := range file.Imports {
				ip, _ := strconv.Unquote(imp.Path.Value)
				local := ip[strings.LastIndex(ip, "/")+1:]
				if imp.Name != nil {
					local = imp.Name.Name
				}
				imports[local] = ip
			}
			collect := func(fi *funcInfo, body ast.Node) {
				var ms []string
				// References, not just calls: awscfg's loader RETURNS
				// config.LoadDefaultConfig as a value, and a function handed to
				// a seam is as reachable as one called in place.
				ast.Inspect(body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.Ident:
						fi.calls = append(fi.calls, p.importPath+"."+x.Name)
					case *ast.SelectorExpr:
						if id, ok := x.X.(*ast.Ident); ok {
							if ip, ok := imports[id.Name]; ok {
								fi.calls = append(fi.calls, ip+"."+x.Sel.Name)
								return false
							}
						}
						ms = append(ms, x.Sel.Name)
					}
					return true
				})
				if fi.initLike {
					ast.Inspect(body, func(n ast.Node) bool {
						switch x := n.(type) {
						case *ast.FuncLit:
							return false
						case *ast.CallExpr:
							switch fn := x.Fun.(type) {
							case *ast.Ident:
								fi.initCalls = append(fi.initCalls, p.importPath+"."+fn.Name)
							case *ast.SelectorExpr:
								if id, ok := fn.X.(*ast.Ident); ok {
									if ip, ok := imports[id.Name]; ok {
										fi.initCalls = append(fi.initCalls, ip+"."+fn.Sel.Name)
										break
									}
								}
								initMethods = append(initMethods, pending{fi, []string{fn.Sel.Name}})
							}
						}
						return true
					})
				}
				unresolved = append(unresolved, pending{fi, ms})
			}
			for _, decl := range file.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Body == nil {
						continue
					}
					key := p.importPath + "." + d.Name.Name
					if d.Recv != nil && len(d.Recv.List) == 1 {
						key = p.importPath + "." + recvName(d.Recv.List[0].Type) + "." + d.Name.Name
						methodsByName[d.Name.Name] = append(methodsByName[d.Name.Name], key)
					}
					isInit := d.Recv == nil && d.Name.Name == "init"
					if isInit {
						key = p.importPath + ".init@" + fset.Position(d.Pos()).String()
					}
					fi := &funcInfo{key: key, pkg: p.importPath, pos: fset.Position(d.Pos()).String(), initLike: isInit, exported: d.Recv == nil && d.Name.IsExported()}
					funcs[key] = fi
					collect(fi, d.Body)
				case *ast.GenDecl:
					if d.Tok != token.VAR {
						continue
					}
					for _, spec := range d.Specs {
						vs := spec.(*ast.ValueSpec)
						for _, v := range vs.Values {
							key := p.importPath + ".var@" + fset.Position(v.Pos()).String()
							fi := &funcInfo{key: key, pkg: p.importPath, pos: fset.Position(v.Pos()).String(), initLike: true}
							funcs[key] = fi
							collect(fi, v)
						}
					}
				}
			}
		}
	}
	for _, u := range unresolved {
		for _, m := range u.methods {
			u.fi.calls = append(u.fi.calls, methodsByName[m]...)
		}
	}
	for _, u := range initMethods {
		for _, m := range u.methods {
			u.fi.initCalls = append(u.fi.initCalls, methodsByName[m]...)
		}
	}
	return funcs, methodsByName
}

func recvName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.StarExpr:
		return recvName(x.X)
	case *ast.IndexExpr:
		return recvName(x.X)
	case *ast.IndexListExpr:
		return recvName(x.X)
	case *ast.Ident:
		return x.Name
	}
	return "?"
}

// seeds are the aws-sdk-go-v2/config functions that load a config
// (LoadDefaultConfig, LoadSharedConfigProfile, ...), discovered from that
// package's own source by scanRepo: its exported top-level funcs named Load*.
var seeds = map[string]bool{}

func isSeed(key string) bool { return seeds[key] }

// loaders returns every function that can reach a config loader, with the
// callee that makes it one.
func loaders(funcs map[string]*funcInfo) map[string]string {
	via := map[string]string{}
	for changed := true; changed; {
		changed = false
		for k, fi := range funcs {
			if _, done := via[k]; done {
				continue
			}
			for _, c := range fi.calls {
				if _, l := via[c]; isSeed(c) || l {
					via[k] = c
					changed = true
					break
				}
			}
		}
	}
	return via
}

type repoScan struct {
	ours     map[string]bool      // import paths of this repo's packages
	funcs    map[string]*funcInfo // every scanned function
	via      map[string]string    // loader -> the callee that makes it one
	binaries map[string][]string  // test binary package -> its deps
	dirs     map[string]string    // this repo's package -> dir
	// testsReach: this repo's packages whose own _test.go files call or
	// reference a config loader, e.g. a test that builds its own client.
	testsReach map[string]string
}

func scanRepo(t *testing.T) repoScan {
	t.Helper()
	s := repoScan{ours: map[string]bool{}, binaries: map[string][]string{}, dirs: map[string]string{}}
	var scan, ourTests []listedPkg
	seen := map[string]bool{}
	for _, m := range modules {
		dir := filepath.Join(repoRoot, m)
		for _, p := range goList(t, dir, "./...") {
			s.ours[p.importPath] = true
			s.dirs[p.importPath] = p.dir
			if len(p.testFiles) > 0 {
				ourTests = append(ourTests, listedPkg{importPath: p.importPath, dir: p.dir, goFiles: p.testFiles})
			}
		}
		// Every package any of ours depends on that itself depends on the
		// config package: only those can reach a loader. The SDK's own modules
		// are excluded; the seeds stand in for them.
		for _, p := range goList(t, dir, "-deps", "./...") {
			if p.importPath == awsConfigPkg && !seen[p.importPath] {
				seen[p.importPath] = true
				scan = append(scan, p)
				continue
			}
			if seen[p.importPath] || strings.HasPrefix(p.importPath, "github.com/aws/") {
				continue
			}
			if s.ours[p.importPath] || contains(p.deps, awsConfigPkg) {
				seen[p.importPath] = true
				scan = append(scan, p)
			}
		}
		for _, p := range goList(t, dir, "-test", "./...") {
			if bin, ok := strings.CutSuffix(p.importPath, ".test"); ok {
				s.binaries[bin] = p.deps
			}
		}
	}
	if len(s.ours) == 0 || len(scan) == 0 || len(s.binaries) == 0 {
		t.Fatalf("discovery found %d packages, %d to scan, %d test binaries — the discovery is broken, not the repo clean", len(s.ours), len(scan), len(s.binaries))
	}
	s.funcs, _ = callGraph(t, scan)
	for k, fi := range s.funcs {
		if fi.pkg == awsConfigPkg && fi.exported && strings.HasPrefix(strings.TrimPrefix(k, awsConfigPkg+"."), "Load") {
			seeds[k] = true
		}
	}
	if !seeds[awsConfigPkg+".LoadDefaultConfig"] {
		t.Fatalf("seed discovery did not find %s.LoadDefaultConfig (found %v) — the discovery is broken", awsConfigPkg, seeds)
	}
	// The config package's own functions are the seeds, not entry points.
	for k, fi := range s.funcs {
		if fi.pkg == awsConfigPkg {
			delete(s.funcs, k)
		}
	}
	s.via = loaders(s.funcs)

	s.testsReach = map[string]string{}
	testFuncs, _ := callGraph(t, ourTests)
	for _, fi := range testFuncs {
		for _, c := range fi.calls {
			if _, l := s.via[c]; (l || isSeed(c)) && s.testsReach[fi.pkg] == "" {
				s.testsReach[fi.pkg] = fi.pos + " -> " + c
			}
		}
	}
	return s
}

// entryPoints returns this repo's functions that can build an AWS config,
// sorted.
func (s repoScan) entryPoints() []string {
	var out []string
	for k := range s.via {
		if fi := s.funcs[k]; s.ours[fi.pkg] && !fi.initLike {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// guard names what stops an entry point from building a real config in a test.
func (s repoScan) guard(k string) string {
	const awscfgLoad = "github.com/spore-host/lagotto/pkg/awscfg.Load"
	if k == awscfgLoad || k == "github.com/spore-host/lagotto/pkg/awscfg.loader" {
		return "awscfg guard (panics without a fake)"
	}
	if why, ok := directLoaderAllowed[k]; ok {
		return why
	}
	var through []string
	for _, c := range s.funcs[k].calls {
		if _, l := s.via[c]; l || isSeed(c) {
			through = append(through, strings.TrimPrefix(strings.TrimPrefix(c, "github.com/spore-host/"), "lagotto/"))
		}
	}
	sort.Strings(through)
	return "via " + strings.Join(dedupe(through), ", ")
}

func dedupe(in []string) []string {
	var out []string
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// hasHermeticTestMain reports whether dir has a TestMain calling hermetic.Main
// or hermetic.Run.
func hasHermeticTestMain(t *testing.T, dir string) bool {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "*_test.go"))
	fset := token.NewFileSet()
	for _, f := range files {
		file, err := parser.ParseFile(fset, f, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		local := ""
		for _, imp := range file.Imports {
			if ip, _ := strconv.Unquote(imp.Path.Value); ip == "github.com/spore-host/lagotto/internal/hermetic" {
				local = "hermetic"
				if imp.Name != nil {
					local = imp.Name.Name
				}
			}
		}
		for _, d := range file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Name.Name != "TestMain" || fd.Body == nil {
				continue
			}
			found := false
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == local && (sel.Sel.Name == "Main" || sel.Sel.Name == "Run") {
							found = true
						}
					}
				}
				return true
			})
			if found {
				return true
			}
		}
	}
	return false
}

func TestNoTestCanReachARealAWSConfig(t *testing.T) {
	s := scanRepo(t)
	eps := s.entryPoints()

	// Documented intent: the path that wrote a live watch on 2026-11-01, and the
	// seam every CLI command goes through, must both be discovered. If they are
	// not, the discovery is broken.
	for _, want := range []string{
		"github.com/spore-host/lagotto/cmd.runWatch",
		"github.com/spore-host/lagotto/pkg/awscfg.Load",
		"github.com/spore-host/lagotto/lambda/capacity-poller.setup",
	} {
		if _, ok := s.via[want]; !ok {
			t.Errorf("discovery did not find %s as a config-loading entry point — the call graph is broken", want)
		}
	}

	var inv strings.Builder
	for _, k := range eps {
		fi := s.funcs[k]
		inv.WriteString("  " + strings.TrimPrefix(k, "github.com/spore-host/lagotto/") + "  [" + s.guard(k) + "]\n")

		// Rule 1: direct loader calls only where they are guarded.
		for _, c := range fi.calls {
			if isSeed(c) {
				if _, ok := directLoaderAllowed[k]; !ok {
					t.Errorf("%s (%s) calls %s directly. Load AWS config through awscfg.Load, whose test guard is what keeps tests off live accounts (#214)", k, fi.pos, c)
				}
			}
		}
	}
	// Rule 2: nothing loads config from package initialization.
	for _, fi := range s.funcs {
		if !fi.initLike || !s.ours[fi.pkg] {
			continue
		}
		for _, c := range fi.initCalls {
			if _, l := s.via[c]; l || isSeed(c) {
				t.Errorf("%s runs %s at package initialization, which can build an AWS config. Init runs in the test binary before TestMain, so no test can make it hermetic (#214); move it into a function main calls", fi.pos, c)
			}
		}
	}
	t.Logf("%d config-loading entry points discovered:\n%s", len(eps), inv.String())

	// Rule 3: every test binary that links any entry point is hermetic even
	// without the -exec wrapper.
	byPkg := map[string]bool{}
	for _, k := range eps {
		byPkg[s.funcs[k].pkg] = true
	}
	var bins []string
	for b := range s.binaries {
		bins = append(bins, b)
	}
	sort.Strings(bins)
	checked := 0
	var checkedBins []string
	for _, bin := range bins {
		deps := s.binaries[bin]
		links := byPkg[bin] || s.testsReach[bin] != ""
		if bin == wrapperSelfTest {
			// Its helper loads a config ON PURPOSE, as the wrapper's child, to
			// prove the wrapper fails it; a TestMain here would hide that.
			continue
		}
		for p := range byPkg {
			if contains(deps, p) {
				links = true
			}
		}
		if !links {
			continue
		}
		checked++
		checkedBins = append(checkedBins, strings.TrimPrefix(bin, "github.com/spore-host/lagotto"))
		if !hasHermeticTestMain(t, s.dirs[bin]) {
			why := "links a config-loading entry point"
			if r := s.testsReach[bin]; r != "" {
				why = "has a test that reaches a config loader (" + r + ")"
			}
			t.Errorf("the test binary for %s %s but has no `func TestMain(m *testing.M) { hermetic.Main(m) }`; add one (internal/hermetic, #214)", bin, why)
		}
	}
	if checked == 0 {
		t.Fatal("no test binary links a config-loading entry point — the discovery is broken")
	}
	t.Logf("%d test binaries must be hermetic under a plain `go test`: %v", checked, checkedBins)
}
