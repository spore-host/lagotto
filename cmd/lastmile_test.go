package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lastmile"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

const (
	lmAccount = "123456789012"
	lmEast    = "us-east-1" // also the control region
	lmWest    = "us-west-2"
)

var lmT0 = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// lmWriteOps are every lifecycletest operation that changes a table.
var lmWriteOps = []string{"PutItem", "UpdateItem", "DeleteItem", "TransactWriteItems", "CreateTable", "DeleteTable"}

// lmWorld stands in for everything the last mile reaches: the account the
// command resolved, the enabled regions, a lifecycle table per region, the
// pollers, the footprint, the clock and the state file. It counts the calls
// of lastMileRun and lastMileRegions.
type lmWorld struct {
	t       *testing.T
	mu      sync.Mutex
	now     time.Time
	fakes   map[string]*lifecycletest.Fake
	api     func(region string) lifecycle.API // overrides fakes when set
	regions []string
	pollers map[string]bool
	facts   inventory.Facts
	dir     string

	runs        int
	regionCalls int
	opts        lastmile.Options
	result      lastmile.Result
}

func newLMWorld(t *testing.T) *lmWorld {
	t.Helper()
	w := &lmWorld{
		t: t, now: lmT0, fakes: map[string]*lifecycletest.Fake{},
		regions: []string{lmEast, lmWest}, pollers: map[string]bool{}, dir: t.TempDir(),
	}
	w.facts = inventory.Facts{AccountID: lmAccount, AllRegions: true, Regions: []inventory.Region{{Region: lmEast}, {Region: lmWest}}}

	restore := func(p any) {
		v := reflect.ValueOf(p).Elem()
		old := reflect.ValueOf(v.Interface())
		t.Cleanup(func() { v.Set(old) })
	}
	restore(&lastMileRun)
	restore(&lastMileBudget)
	restore(&lastMileAccount)
	restore(&lastMileStatePath)
	restore(&lastMileNow)
	restore(&lastMileRegions)
	restore(&lastMileLifecycleAPI)
	restore(&lastMilePollerExists)
	restore(&gatherFootprint)
	restore(&footprintNow)
	restore(&verbose)

	verbose = false
	lastMileAccount = func() string { return lmAccount }
	lastMileStatePath = func() (string, error) { return filepath.Join(w.dir, ".spore-host", "lifecycle.json"), nil }
	lastMileNow = w.clock
	footprintNow = w.clock
	lastMileRegions = func(context.Context, aws.Config, string) ([]string, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.regionCalls++
		return append([]string(nil), w.regions...), nil
	}
	lastMileLifecycleAPI = func(_ aws.Config, region string) (lifecycle.API, error) {
		if w.api != nil {
			return w.api(region), nil
		}
		return w.fake(region), nil
	}
	lastMilePollerExists = func(_ context.Context, _ aws.Config, _, region string) (bool, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.pollers[region], nil
	}
	gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) {
		return w.facts, nil
	}
	lastMileRun = func(ctx context.Context, o lastmile.Options) lastmile.Result {
		res := lastmile.Run(ctx, o)
		w.mu.Lock()
		defer w.mu.Unlock()
		w.runs++
		w.opts, w.result = o, res
		return res
	}
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: lmEast, HTTPClient: refusingHTTP{}, Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1}, nil
	})
	return w
}

func (w *lmWorld) clock() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.now
}

func (w *lmWorld) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.now = w.now.Add(d)
}

func (w *lmWorld) fake(region string) *lifecycletest.Fake {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.fakes[region]
	if !ok {
		f = lifecycletest.New()
		w.fakes[region] = f
	}
	return f
}

func (w *lmWorld) runCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.runs
}

// participant is another lagotto in the account (a creator, the poller),
// writing to the same tables.
func (w *lmWorld) participant(owner string) *lifecycle.Client {
	w.t.Helper()
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(r string) (lifecycle.API, error) { return w.fake(r), nil },
		ControlRegion: lmEast,
		Owner:         owner,
		Now:           w.clock,
		Sleep:         func(ctx context.Context, d time.Duration) error { w.advance(d); return ctx.Err() },
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

// writes is how many writing calls every fake has seen.
func (w *lmWorld) writes() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, f := range w.fakes {
		for _, op := range lmWriteOps {
			n += f.Calls(op)
		}
	}
	return n
}

// staleLease leaves a teardown in region whose holder died: claimed, then
// its heartbeat let go past the stale threshold.
func (w *lmWorld) staleLease(region string) {
	w.t.Helper()
	ctx := context.Background()
	c := w.participant("lagotto-poller/" + region)
	if err := c.MarkActive(ctx, tags.ToolLagotto, region); err != nil {
		w.t.Fatal(err)
	}
	if _, _, err := c.Observe(ctx, tags.ToolLagotto, region, false); err != nil {
		w.t.Fatal(err)
	}
	if _, err := c.Claim(ctx, tags.ToolLagotto, region, 0); err != nil {
		w.t.Fatal(err)
	}
	w.advance(lifecycle.DefaultStaleAfter + time.Minute)
}

// probeCmd adds a command to the real root for the test: it writes payload to
// its stdout and returns err.
func probeCmd(t *testing.T, name, payload string, err error) {
	t.Helper()
	c := &cobra.Command{Use: name, SilenceUsage: true, RunE: func(cmd *cobra.Command, _ []string) error {
		_, _ = io.WriteString(cmd.OutOrStdout(), payload)
		return err
	}}
	rootCmd.AddCommand(c)
	t.Cleanup(func() { rootCmd.RemoveCommand(c) })
}

// execRoot runs the real root command with args and returns what reached
// stdout and stderr, through the command's writers or the process's own file
// descriptors, and the error Execute would turn into the exit status.
func execRoot(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	capture := func(f **os.File) func() string {
		r, w, perr := os.Pipe()
		if perr != nil {
			t.Fatal(perr)
		}
		old := *f
		*f = w
		var buf bytes.Buffer
		done := make(chan struct{})
		go func() { _, _ = io.Copy(&buf, r); close(done) }()
		return func() string {
			*f = old
			_ = w.Close()
			<-done
			_ = r.Close()
			return buf.String()
		}
	}
	stopOut, stopErr := capture(&os.Stdout), capture(&os.Stderr)
	var cmdOut, cmdErr bytes.Buffer
	rootCmd.SetOut(&cmdOut)
	rootCmd.SetErr(&cmdErr)
	rootCmd.SetArgs(args)
	defer func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs([]string{})
	}()
	_, err = rootCmd.ExecuteC()
	return cmdOut.String() + stopOut(), cmdErr.String() + stopErr(), err
}

// ---- when it runs ----------------------------------------------------------

// TestLastMileRunsAfterACommand: after a command succeeds, the root's
// post-run hook runs the last mile exactly once, for lagotto in the account
// the command resolved, and prints nothing at default verbosity.
func TestLastMileRunsAfterACommand(t *testing.T) {
	w := newLMWorld(t)
	probeCmd(t, "lm-probe", "the command's output\n", nil)
	stdout, stderr, err := execRoot(t, "lm-probe")
	if err != nil {
		t.Fatal(err)
	}
	if n := w.runCount(); n != 1 {
		t.Fatalf("lastmile.Run called %d times, want 1", n)
	}
	if w.opts.Tool != tags.ToolLagotto || w.opts.Account != lmAccount {
		t.Errorf("Run for %s in %q, want lagotto in %s", w.opts.Tool, w.opts.Account, lmAccount)
	}
	if !w.result.Ran {
		t.Errorf("the run did not run: %s", w.result.Summary())
	}
	if stdout != "the command's output\n" {
		t.Errorf("stdout = %q, want only the command's own output", stdout)
	}
	if stderr != "" {
		t.Errorf("stderr at default verbosity = %q, want nothing", stderr)
	}
}

// TestLastMileSkippedCommands: completion, help, version, footprint, teardown,
// doctor (and the hidden gen-docs) never run it; every other command does.
func TestLastMileSkippedCommands(t *testing.T) {
	w := newLMWorld(t)
	for name := range lastMileSkip {
		switch name {
		case "completion", "help", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
			continue // cobra adds these when it executes
		}
		c, _, err := rootCmd.Find([]string{name})
		if err != nil || c == rootCmd || c.Name() != name {
			t.Errorf("lastMileSkip names %q, which is not a lagotto command (renamed?)", name)
			continue
		}
		runLastMile(c)
	}
	for _, want := range []string{"version", "footprint", "teardown", "doctor", "gen-docs"} {
		if _, ok := lastMileSkip[want]; !ok {
			t.Errorf("lastMileSkip lacks %s", want)
		}
	}
	if n := w.runCount(); n != 0 {
		t.Fatalf("a skipped command ran the last mile %d time(s)", n)
	}

	// Through the real root, which builds the help and completion commands.
	for _, args := range [][]string{{"help"}, {"completion", "bash"}, {cobra.ShellCompRequestCmd, "wat"}} {
		if _, _, err := execRoot(t, args...); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if n := w.runCount(); n != 0 {
		t.Fatalf("help/completion ran the last mile %d time(s)", n)
	}

	// A subcommand of a skipped command is skipped with it; any other runs.
	c, _, err := rootCmd.Find([]string{"list"})
	if err != nil {
		t.Fatal(err)
	}
	runLastMile(c)
	if n := w.runCount(); n != 1 {
		t.Fatalf("after `lagotto list` the last mile ran %d time(s), want 1", n)
	}
}

// TestNoSubcommandShadowsTheLastMile: cobra runs only the nearest persistent
// post-run, so a subcommand that set its own would silently turn the last
// mile off for itself.
func TestNoSubcommandShadowsTheLastMile(t *testing.T) {
	if rootCmd.PersistentPostRunE == nil {
		t.Fatal("the root command has no PersistentPostRunE: the last mile is not wired")
	}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			if sub.PersistentPostRun != nil || sub.PersistentPostRunE != nil {
				t.Errorf("%s sets its own persistent post-run, which hides the root's last mile", sub.CommandPath())
			}
			walk(sub)
		}
	}
	walk(rootCmd)
}

// TestLastMileOff: SPORE_LASTMILE=off (any case) skips it before anything.
func TestLastMileOff(t *testing.T) {
	for _, v := range []string{"off", "OFF", " Off "} {
		t.Run(v, func(t *testing.T) {
			w := newLMWorld(t)
			t.Setenv(lastmile.EnvVar, v)
			probeCmd(t, "lm-probe", "", nil)
			if _, _, err := execRoot(t, "lm-probe"); err != nil {
				t.Fatal(err)
			}
			if w.runCount() != 0 || w.regionCalls != 0 {
				t.Errorf("%s=%q: ran %d time(s), listed regions %d time(s); want neither", lastmile.EnvVar, v, w.runCount(), w.regionCalls)
			}
		})
	}
}

// TestLastMileModes: unset and report run in report mode; reap is passed on
// (lastmile runs it as report until libs#79) and still writes nothing.
func TestLastMileModes(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want lastmile.Mode
	}{{"", lastmile.ModeReport}, {"report", lastmile.ModeReport}, {"reap", lastmile.ModeReap}, {"bogus", lastmile.ModeReport}} {
		t.Run("mode="+tc.env, func(t *testing.T) {
			w := newLMWorld(t)
			w.staleLease(lmWest)
			before := w.writes()
			t.Setenv(lastmile.EnvVar, tc.env)
			probeCmd(t, "lm-probe", "", nil)
			if _, _, err := execRoot(t, "lm-probe"); err != nil {
				t.Fatal(err)
			}
			if w.opts.Mode != tc.want {
				t.Errorf("mode %q, want %q", w.opts.Mode, tc.want)
			}
			if len(w.result.Did) != 0 || w.writes() != before {
				t.Errorf("did %v, %d write(s); want none", w.result.Did, w.writes()-before)
			}
			for _, wn := range w.result.Warnings {
				if strings.Contains(wn, "report-only") {
					t.Errorf("lastmile tried a write the guard refused: %s", wn)
				}
			}
		})
	}
}

// TestLastMileNoAccountNoCall: a command that resolved no account (made no
// GetCallerIdentity) skips the last mile, which makes no STS call of its own.
func TestLastMileNoAccountNoCall(t *testing.T) {
	w := newLMWorld(t)
	lastMileAccount = func() string { return "" }
	probeCmd(t, "lm-probe", "", nil)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	if w.runCount() != 0 || w.regionCalls != 0 {
		t.Errorf("with no account: ran %d, listed regions %d; want neither", w.runCount(), w.regionCalls)
	}
}

// ---- never in the way ------------------------------------------------------

// errExit3 is a command failure with its own exit status.
type errExit3 struct{}

func (errExit3) Error() string { return "probe failed" }
func (errExit3) ExitCode() int { return 3 }

// TestLastMileNeverTouchesStdoutOrExitStatus: whatever the last mile does
// (finds things, warns, errors, panics), stdout holds exactly the command's
// own output and the command's error (its exit status) is unchanged. At
// default verbosity stderr is empty; under --verbose every line is a debug
// line on stderr.
func TestLastMileNeverTouchesStdoutOrExitStatus(t *testing.T) {
	cases := map[string]func(w *lmWorld){
		"finds a stale lease and residue": func(w *lmWorld) { w.staleLease(lmWest) },
		"lifecycle reads fail": func(w *lmWorld) {
			w.api = func(string) lifecycle.API { return failingLifecycle{err: errors.New("dynamodb is down")} }
		},
		"Run panics": func(w *lmWorld) {
			lastMileRun = func(context.Context, lastmile.Options) lastmile.Result { panic("boom") }
		},
		"region listing panics": func(*lmWorld) {
			lastMileRegions = func(context.Context, aws.Config, string) ([]string, error) { panic("boom") }
		},
		"footprint panics": func(w *lmWorld) {
			gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) { panic("boom") }
		},
		"Run returns warnings and would-dos": func(*lmWorld) {
			lastMileRun = func(_ context.Context, o lastmile.Options) lastmile.Result {
				o.Logf("lastmile: warning: %s", "something")
				return lastmile.Result{Ran: true, Tool: o.Tool, Account: o.Account, Warnings: []string{"w"},
					WouldDo: []lastmile.Action{{Kind: lastmile.KindDelete, Target: "arn:x"}}}
			}
		},
	}
	for name, setup := range cases {
		for _, v := range []bool{false, true} {
			t.Run(name, func(t *testing.T) {
				w := newLMWorld(t)
				setup(w)
				probeCmd(t, "lm-probe", "payload\n", nil)
				args := []string{"lm-probe"}
				if v {
					args = append(args, "--verbose")
				}
				stdout, stderr, err := execRoot(t, args...)
				if err != nil {
					t.Fatalf("the command's error became %v", err)
				}
				if stdout != "payload\n" {
					t.Errorf("stdout = %q, want exactly the command's output", stdout)
				}
				if !v && stderr != "" {
					t.Errorf("stderr at default verbosity = %q, want nothing", stderr)
				}
				for _, line := range strings.Split(strings.TrimRight(stderr, "\n"), "\n") {
					if v && line != "" && !strings.HasPrefix(line, "debug: lastmile") {
						t.Errorf("verbose stderr line %q is not a last-mile debug line", line)
					}
				}
			})
		}
	}

	// A failed command keeps its own exit status, and runs no last mile.
	w := newLMWorld(t)
	probeCmd(t, "lm-fail", "partial\n", errExit3{})
	stdout, _, err := execRoot(t, "lm-fail")
	if code := exitStatus(err); err == nil || code != 3 {
		t.Errorf("exit status %d (%v), want the command's own 3", code, err)
	}
	if stdout != "partial\n" {
		t.Errorf("stdout = %q", stdout)
	}
	if w.runCount() != 0 {
		t.Errorf("the last mile ran after a failed command")
	}
}

// TestLastMileVerboseSummary: under --verbose the summary and each
// would-action reach stderr, as debug lines, never stdout.
func TestLastMileVerboseSummary(t *testing.T) {
	w := newLMWorld(t)
	w.staleLease(lmWest)
	probeCmd(t, "lm-probe", "", nil)
	stdout, stderr, err := execRoot(t, "lm-probe", "-v")
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "" {
		t.Errorf("stdout = %q, want nothing", stdout)
	}
	for _, want := range []string{
		"debug: lastmile REPORT tool=lagotto account=" + lmAccount,
		"debug: lastmile: would resume teardown of tool#lagotto#us-west-2",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("verbose stderr lacks %q:\n%s", want, stderr)
		}
	}
}

// TestLastMileBudget: a last mile that overruns (here one that ignores its
// context entirely) holds the command no longer than its budget plus the
// slack.
func TestLastMileBudget(t *testing.T) {
	newLMWorld(t)
	lastMileBudget = 300 * time.Millisecond
	release, returned := make(chan struct{}), make(chan struct{})
	lastMileRun = func(context.Context, lastmile.Options) lastmile.Result {
		defer close(returned)
		<-release // ignores ctx
		return lastmile.Result{}
	}
	start := time.Now()
	runLastMile(listCmdForTest(t))
	if took := time.Since(start); took > lastMileBudget+lastMileSlack+500*time.Millisecond {
		t.Errorf("the hook took %s with a %s budget", took, lastMileBudget)
	}
	// Let the abandoned run end before the seams change under it.
	close(release)
	<-returned

	release = make(chan struct{})
	t.Cleanup(func() { close(release) })

	// The same with a slow lifecycle table under the real lastmile.Run.
	w := newLMWorld(t)
	lastMileBudget = 300 * time.Millisecond
	w.api = func(string) lifecycle.API { return slowLifecycle{release: release} }
	start = time.Now()
	runLastMile(listCmdForTest(t))
	if took := time.Since(start); took > lastMileBudget+lastMileSlack+500*time.Millisecond {
		t.Errorf("the hook took %s with a %s budget and a table that never answers", took, lastMileBudget)
	}
}

// TestLastMileHookRecovers: the hook returns nil even when it is handed
// nothing to work with.
func TestLastMileHookRecovers(t *testing.T) {
	w := newLMWorld(t)
	if err := lastMileHook(nil, nil); err != nil {
		t.Errorf("lastMileHook(nil) = %v, want nil", err)
	}
	if w.runCount() != 0 {
		t.Error("ran with no command")
	}
}

func listCmdForTest(t *testing.T) *cobra.Command {
	t.Helper()
	c, _, err := rootCmd.Find([]string{"list"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// ---- what it passes and reports --------------------------------------------

// TestLastMileOptionsWiring pins the options: lagotto, every enabled region,
// a lifecycle client with a control region, lagotto's footprint, deleter and
// patterns, the residue, and FullVisibility false.
func TestLastMileOptionsWiring(t *testing.T) {
	w := newLMWorld(t)
	w.regions = []string{"eu-west-1", lmEast, lmWest}
	probeCmd(t, "lm-probe", "", nil)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	o := w.opts
	if w.regionCalls != 1 {
		t.Errorf("listed the regions %d times, want 1", w.regionCalls)
	}
	if !reflect.DeepEqual(o.Regions, w.regions) {
		t.Errorf("Regions = %v, want every enabled region %v", o.Regions, w.regions)
	}
	if o.FullVisibility || o.Activity != nil {
		t.Errorf("FullVisibility = %v, Activity set = %v; the CLI must never judge lagotto idle", o.FullVisibility, o.Activity != nil)
	}
	if o.Lifecycle == nil {
		t.Fatal("no Lifecycle client")
	}
	if r, err := o.Lifecycle.RegionFor(lifecycle.ScopeGlobal); err != nil || r != lmEast {
		t.Errorf("control region = %q, %v; want %s", r, err, lmEast)
	}
	if !reflect.DeepEqual(o.Patterns, inventory.Patterns(footprintOptions())) {
		t.Error("Patterns are not lagotto's (inventory.Patterns)")
	}
	if o.Footprint == nil || o.Deleter == nil {
		t.Fatal("no Footprint or Deleter")
	}
	if o.Budget <= 0 || o.Budget > lastmile.DefaultBudget || o.StatePath == "" || o.Logf == nil {
		t.Errorf("Budget %s, StatePath %q, Logf set %v", o.Budget, o.StatePath, o.Logf != nil)
	}

	// Only the regional groups: nothing account-wide is lastmile's to judge;
	// the account-wide teardown is the one actor for those (lagotto#340).
	want := map[string]string{}
	for _, r := range w.regions {
		want[inventory.ScheduleGroupARN(r, lmAccount, runtimeiam.TeardownScheduleGroup)] = r
	}
	got := map[string]string{}
	for _, r := range o.Residue {
		got[r.ARN] = r.Region
		if ok, why := fp.Deletable(r, tags.ToolLagotto, o.Patterns); !ok {
			t.Errorf("residue %s would be kept by lagotto's own guard: %s", r.ARN, why)
		}
		if r.Region == inventory.Global {
			t.Errorf("residue lists the account-wide %s", r.ARN)
		}
		if r.Name == lifecycle.TableName {
			t.Errorf("residue lists %s: the suite's table is lastmile's to report (CloseTableIfEmpty), not lagotto's to delete", r.ARN)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Residue = %v, want %v", got, want)
	}

	// The Deleter refuses every delete; the client refuses every write.
	if err := o.Deleter.Delete(context.Background(), o.Residue[0]); !errors.Is(err, errLastMileReportOnly) {
		t.Errorf("Deleter.Delete = %v, want the report-only refusal", err)
	}
	if err := o.Lifecycle.MarkActive(context.Background(), tags.ToolLagotto, lmWest); !errors.Is(err, errLastMileReportOnly) {
		t.Errorf("a write through the last mile's lifecycle client = %v, want the report-only refusal", err)
	}
}

// TestLastMileReportsStaleLeaseWithoutWriting: a teardown whose holder died
// is reported, with what it would still delete, and nothing is written.
func TestLastMileReportsStaleLeaseWithoutWriting(t *testing.T) {
	w := newLMWorld(t)
	w.staleLease(lmWest)
	w.pollers[lmWest] = true
	before := w.writes()
	item := w.fake(lmWest).Item(lifecycle.TableName, lifecycle.ItemKey(tags.ToolLagotto, lmWest))
	probeCmd(t, "lm-probe", "", nil)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	var resume bool
	for _, a := range w.result.WouldDo {
		resume = resume || (a.Kind == lastmile.KindResumeTeardown && a.Target == lifecycle.ItemKey(tags.ToolLagotto, lmWest))
	}
	if !resume {
		t.Errorf("no resume-teardown for the stale lease: %+v", w.result.WouldDo)
	}
	if w.writes() != before {
		t.Errorf("%d write(s) to the lifecycle tables; report mode writes none", w.writes()-before)
	}
	if after := w.fake(lmWest).Item(lifecycle.TableName, lifecycle.ItemKey(tags.ToolLagotto, lmWest)); !reflect.DeepEqual(after, item) {
		t.Error("the stale item changed")
	}
}

// TestLastMileResidueAndEmptyTable: with no lagotto item left anywhere, an
// empty lifecycle table is reported to be closed; the shared invoke role is
// not lastmile's residue but the account-wide teardown's, which reports it
// (the global grace not started), and nothing is written.
func TestLastMileResidueAndEmptyTable(t *testing.T) {
	w := newLMWorld(t)
	w.facts.Account.Roles = []inventory.Item{{
		ARN: runtimeiam.SchedulerInvokeRoleARN("aws", lmAccount), Name: runtimeiam.SchedulerInvokeRoleName,
		Tags: map[string]string{tags.ManagedBy: string(tags.ToolLagotto)},
	}}
	// An empty table, as a teardown that finished leaves it.
	f := w.fake(lmWest)
	if _, err := f.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:            aws.String(lifecycle.TableName),
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		BillingMode:          types.BillingModePayPerRequest,
	}); err != nil {
		t.Fatal(err)
	}
	f.SetStatus(lifecycle.TableName, types.TableStatusActive, 0)
	before := w.writes()
	probeCmd(t, "lm-probe", "", nil)
	_, stderr, err := execRoot(t, "lm-probe", "--verbose")
	if err != nil {
		t.Fatal(err)
	}
	var role, table bool
	for _, a := range w.result.WouldDo {
		role = role || a.Target == runtimeiam.SchedulerInvokeRoleARN("aws", lmAccount)
		table = table || (a.Kind == lastmile.KindCloseTable && a.Region == lmWest)
	}
	if role || !table {
		t.Errorf("lastmile would act on the invoke role: %v, close the empty table: %v; got %+v (warnings %v)", role, table, w.result.WouldDo, w.result.Warnings)
	}
	if !strings.Contains(stderr, `debug: lastmile: account-wide REPORT outcome=waiting why="the global scope's grace has not started`) {
		t.Errorf("no account-wide report:\n%s", stderr)
	}
	if w.writes() != before {
		t.Errorf("%d write(s); report mode writes none", w.writes()-before)
	}
}

// TestLastMilePollerlessRegion (lagotto#328): a `watch --local` in a region
// with no poller leaves a lagotto item nothing will ever observe idle. The
// last mile says so; with a poller there, it does not.
func TestLastMilePollerlessRegion(t *testing.T) {
	for _, poller := range []bool{false, true} {
		t.Run(map[bool]string{false: "no poller", true: "poller"}[poller], func(t *testing.T) { lastMilePollerlessCase(t, poller) })
	}
}

func lastMilePollerlessCase(t *testing.T, poller bool) {
	{
		w := newLMWorld(t)
		if err := w.participant("watch --local").MarkActive(context.Background(), tags.ToolLagotto, lmWest); err != nil {
			t.Fatal(err)
		}
		w.pollers[lmWest] = poller
		probeCmd(t, "lm-probe", "", nil)
		stdout, stderr, err := execRoot(t, "lm-probe", "--verbose")
		if err != nil {
			t.Fatal(err)
		}
		said := strings.Contains(stderr, "debug: lastmile: tool#lagotto#us-west-2: no lagotto poller in us-west-2")
		if said == poller {
			t.Errorf("poller in %s = %v: reported poller-less = %v\n%s", lmWest, poller, said, stderr)
		}
		if stdout != "" {
			t.Errorf("stdout = %q", stdout)
		}
		if !strings.Contains(stderr, "residue kept: tool#lagotto#us-west-2 remains") {
			t.Errorf("the item does not keep the residue:\n%s", stderr)
		}
	}
}

// TestLastMileAccessDeniedIsSilent: lagotto-cli-user grants none of the last
// mile's lifecycle reads. The refusal is silent at default verbosity, one
// debug line (never a warning) under --verbose, and costs one denied request
// per run, not one per region.
func TestLastMileAccessDeniedIsSilent(t *testing.T) {
	for _, v := range []bool{false, true} {
		t.Run(map[bool]string{false: "quiet", true: "verbose"}[v], func(t *testing.T) { lastMileDeniedCase(t, v) })
	}
}

func lastMileDeniedCase(t *testing.T, v bool) {
	{
		w := newLMWorld(t)
		w.regions = []string{"eu-west-1", lmEast, lmWest, "ap-south-1"}
		denied := &deniedLifecycle{}
		w.api = func(string) lifecycle.API { return denied }
		probeCmd(t, "lm-probe", "out\n", nil)
		args := []string{"lm-probe"}
		if v {
			args = append(args, "-v")
		}
		stdout, stderr, err := execRoot(t, args...)
		if err != nil {
			t.Fatal(err)
		}
		if stdout != "out\n" {
			t.Errorf("stdout = %q", stdout)
		}
		if !v && stderr != "" {
			t.Errorf("stderr at default verbosity = %q", stderr)
		}
		if strings.Contains(stderr, "lastmile: warning") {
			t.Errorf("a refused read was reported as a warning:\n%s", stderr)
		}
		if v && !strings.Contains(stderr, "debug: lastmile: not permitted, skipped: ") {
			t.Errorf("verbose stderr lacks the not-permitted debug line:\n%s", stderr)
		}
		if n := strings.Count(stderr, "not permitted"); v && n != 1 {
			t.Errorf("%d not-permitted line(s), want 1:\n%s", n, stderr)
		}
		if n := denied.requests(); n != 1 {
			t.Errorf("%d denied request(s) reached DynamoDB, want 1", n)
		}
	}
}

// TestLastMileRateLimitCostsNoAWSCall: once a run has checked every scope,
// the next invocations inside 6 h make no AWS call (not even the region
// listing) and do not run; after 6 h they do. This also holds the state-file
// reader (lastMileMaybeDue) to the file lastmile.Run really writes.
func TestLastMileRateLimitCostsNoAWSCall(t *testing.T) {
	w := newLMWorld(t)
	probeCmd(t, "lm-probe", "", nil)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	if w.runCount() != 1 || w.regionCalls != 1 || !w.result.Ran {
		t.Fatalf("first run: runs %d, region listings %d, ran %v", w.runCount(), w.regionCalls, w.result.Ran)
	}
	w.advance(lastmile.DefaultMinInterval - time.Minute)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	if w.runCount() != 1 || w.regionCalls != 1 {
		t.Errorf("inside 6 h: runs %d, region listings %d; want no new call", w.runCount(), w.regionCalls)
	}
	w.advance(2 * time.Minute)
	if _, _, err := execRoot(t, "lm-probe"); err != nil {
		t.Fatal(err)
	}
	if w.runCount() != 2 || !w.result.Ran {
		t.Errorf("after 6 h: runs %d, ran %v; want a second run", w.runCount(), w.result.Ran)
	}
}

// TestLastMileCutOffBacksOff: a run the budget cut off is not retried on the
// next invocation (lastmile.Run alone would retry it, and an account whose
// check never fits would hold every command for the whole budget), but is
// after lastMileRetryAfter.
func TestLastMileCutOffBacksOff(t *testing.T) {
	w := newLMWorld(t)
	lastMileBudget = 300 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	slow := slowLifecycle{release: release}
	w.api = func(region string) lifecycle.API {
		if region == lmWest {
			return slow // never answers: us-west-2 is cut off
		}
		return w.fake(region)
	}
	runLastMile(listCmdForTest(t))
	if w.runCount() != 1 || len(w.result.Warnings) == 0 {
		t.Fatalf("first run: %d run(s), warnings %v; want one run cut off by its budget", w.runCount(), w.result.Warnings)
	}
	w.advance(30 * time.Minute)
	runLastMile(listCmdForTest(t))
	if w.runCount() != 1 {
		t.Errorf("retried %s after a cut-off run; want a wait of %s", 30*time.Minute, lastMileRetryAfter)
	}
	w.advance(lastMileRetryAfter)
	runLastMile(listCmdForTest(t))
	if w.runCount() != 2 {
		t.Errorf("not retried %s after a cut-off run", lastMileRetryAfter+30*time.Minute)
	}
}

// TestLastMileMaybeDue pins the pre-check's reading of the state file.
func TestLastMileMaybeDue(t *testing.T) {
	dir := t.TempDir()
	now := lmT0
	write := func(body string) string {
		p := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	rec := func(key, lastRun, incomplete string) string {
		s := `"` + key + `":{"last_run":"` + lastRun + `"`
		if incomplete != "" {
			s += `,"incomplete":"` + incomplete + `"`
		}
		return s + "}"
	}
	cut := func(key, lastAttempt string) string {
		return `"` + key + `":{"last_attempt":"` + lastAttempt + `","incomplete":"budget exceeded"}`
	}
	global := "lagotto/" + lmAccount + "/global"
	fresh := now.Add(-time.Hour).Format(time.RFC3339)
	stale := now.Add(-7 * time.Hour).Format(time.RFC3339)
	future := now.Add(time.Hour).Format(time.RFC3339)
	cases := []struct {
		name, body string
		due        bool
	}{
		{"fresh", `{"version":1,"runs":{` + rec(global, fresh, "") + "," + rec("lagotto/"+lmAccount+"/us-east-1", fresh, "") + `}}`, false},
		{"stale", `{"version":1,"runs":{` + rec(global, stale, "") + `}}`, true},
		{"incomplete", `{"version":1,"runs":{` + rec(global, fresh, "budget exceeded") + `}}`, true},
		{"future", `{"version":1,"runs":{` + rec(global, future, "") + `}}`, true},
		{"another account", `{"version":1,"runs":{` + rec("lagotto/999999999999/global", fresh, "") + `}}`, true},
		{"another tool", `{"version":1,"runs":{` + rec("spawn/"+lmAccount+"/global", fresh, "") + `}}`, true},
		{"unknown version", `{"version":2,"runs":{` + rec(global, fresh, "") + `}}`, true},
		{"corrupt", `{not json`, true},
		{"cut off just now", `{"version":1,"runs":{` + cut("lagotto/"+lmAccount+"/us-west-2", now.Add(-10*time.Minute).Format(time.RFC3339)) + `}}`, false},
		{"cut off over an hour ago", `{"version":1,"runs":{` + cut("lagotto/"+lmAccount+"/us-west-2", now.Add(-61*time.Minute).Format(time.RFC3339)) + `}}`, true},
		{"another account cut off", `{"version":1,"runs":{` + cut("lagotto/999999999999/us-west-2", now.Add(-10*time.Minute).Format(time.RFC3339)) + `}}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lastMileMaybeDue(write(tc.body), lmAccount, now); got != tc.due {
				t.Errorf("due = %v, want %v", got, tc.due)
			}
		})
	}
	if !lastMileMaybeDue(filepath.Join(dir, "missing.json"), lmAccount, now) {
		t.Error("a missing state file is not due")
	}
}

// TestLastMileControlRegion: the global scope is read in the partition's
// home region, whatever region the command used; outside aws, aws-us-gov and
// aws-cn there is none (lagotto#340, lagotto#347).
func TestLastMileControlRegion(t *testing.T) {
	for in, want := range map[string]string{"": "us-east-1", "eu-west-1": "us-east-1", "ap-south-1": "us-east-1",
		"us-gov-east-1": "us-gov-west-1", "cn-northwest-1": "cn-north-1"} {
		if got, err := lastMileControlRegion(in); got != want || err != nil {
			t.Errorf("lastMileControlRegion(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"us-iso-east-1", "eu-isoe-west-1", "eusc-de-east-1"} {
		if got, err := lastMileControlRegion(in); got != "" || err == nil {
			t.Errorf("%s: %q, %v", in, got, err)
		}
	}
}

// ---- fakes -----------------------------------------------------------------

// failingLifecycle fails every call with err.
type failingLifecycle struct {
	lifecycle.API
	err error
}

func (f failingLifecycle) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, f.err
}

func (f failingLifecycle) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, f.err
}

func (f failingLifecycle) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return nil, f.err
}

// deniedLifecycle refuses every read as IAM would, counting the requests.
type deniedLifecycle struct {
	lifecycle.API
	mu sync.Mutex
	n  int
}

func (d *deniedLifecycle) deny() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.n++
	return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "User: arn:aws:sts::123456789012:assumed-role/u/s is not authorized to perform: dynamodb:GetItem"}
}

func (d *deniedLifecycle) requests() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.n
}

func (d *deniedLifecycle) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, d.deny()
}

func (d *deniedLifecycle) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, d.deny()
}

func (d *deniedLifecycle) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return nil, d.deny()
}

// slowLifecycle never answers until release is closed, whatever its context
// says.
type slowLifecycle struct {
	lifecycle.API
	release chan struct{}
}

func (s slowLifecycle) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	<-s.release
	return &dynamodb.GetItemOutput{}, nil
}

func (s slowLifecycle) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	<-s.release
	return &dynamodb.ScanOutput{}, nil
}

// ---- the edges: what the last mile does when a step cannot go on -----------

// The production seams, as built, before any test replaces them.
var (
	lmDefaultRegions      = lastMileRegions
	lmDefaultLifecycleAPI = lastMileLifecycleAPI
	lmDefaultPollerExists = lastMilePollerExists
)

// TestLastMileSkipsWhenAStepFails: each step before lastmile.Run that cannot
// go on (no state path, no AWS config, no region listing, no budget left)
// skips the run with one debug line, prints nothing at default verbosity,
// and leaves the command alone.
func TestLastMileSkipsWhenAStepFails(t *testing.T) {
	cases := map[string]struct {
		setup func(t *testing.T, w *lmWorld)
		line  string
	}{
		"no state path": {func(_ *testing.T, _ *lmWorld) {
			lastMileStatePath = func() (string, error) { return "", errors.New("no home directory") }
		}, "debug: lastmile: skipped: no home directory"},
		"no AWS config": {func(t *testing.T, _ *lmWorld) {
			awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
				return aws.Config{}, errors.New("no credentials")
			})
		}, "debug: lastmile: skipped: load AWS config: no credentials"},
		"region listing refused": {func(_ *testing.T, _ *lmWorld) {
			lastMileRegions = func(context.Context, aws.Config, string) ([]string, error) {
				return nil, &smithy.GenericAPIError{Code: "UnauthorizedOperation", Message: "not authorized to perform: ec2:DescribeRegions"}
			}
		}, "debug: lastmile: skipped: list the account's regions: api error UnauthorizedOperation"},
		"no budget left": {func(_ *testing.T, _ *lmWorld) {
			lastMileBudget = time.Nanosecond
		}, "debug: lastmile: skipped: no budget left"},
	}
	for name, tc := range cases {
		for _, v := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/verbose=%v", name, v), func(t *testing.T) {
				w := newLMWorld(t)
				tc.setup(t, w)
				probeCmd(t, "lm-probe", "out\n", nil)
				args := []string{"lm-probe"}
				if v {
					args = append(args, "-v")
				}
				stdout, stderr, err := execRoot(t, args...)
				if err != nil || stdout != "out\n" {
					t.Fatalf("the command became %q, %v", stdout, err)
				}
				if w.runCount() != 0 {
					t.Errorf("lastmile.Run ran %d time(s) after the step failed", w.runCount())
				}
				switch {
				case !v && stderr != "":
					t.Errorf("stderr at default verbosity = %q", stderr)
				case v && !strings.Contains(stderr, tc.line):
					t.Errorf("verbose stderr lacks %q:\n%s", tc.line, stderr)
				}
			})
		}
	}
}

// TestLastMileNoRegionUsesControlRegion: with no region configured the
// regions are listed from the control region.
func TestLastMileNoRegionUsesControlRegion(t *testing.T) {
	newLMWorld(t)
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{HTTPClient: refusingHTTP{}, Credentials: aws.AnonymousCredentials{}}, nil
	})
	var listedFrom string
	lastMileRegions = func(_ context.Context, cfg aws.Config, _ string) ([]string, error) {
		listedFrom = cfg.Region
		return []string{lmEast}, nil
	}
	runLastMile(listCmdForTest(t))
	if listedFrom != lmEast {
		t.Errorf("regions listed from %q, want the control region %s", listedFrom, lmEast)
	}
}

// TestLastMileUnreachableTable: a region whose lifecycle client cannot be
// built is a warning in the run, not a failure of it.
func TestLastMileUnreachableTable(t *testing.T) {
	w := newLMWorld(t)
	lastMileLifecycleAPI = func(_ aws.Config, region string) (lifecycle.API, error) {
		if region == lmWest {
			return nil, errors.New("no endpoint for us-west-2")
		}
		return w.fake(region), nil
	}
	runLastMile(listCmdForTest(t))
	if !w.result.Ran {
		t.Fatalf("did not run: %s", w.result.Summary())
	}
	var warned bool
	for _, wn := range w.result.Warnings {
		warned = warned || strings.Contains(wn, "no endpoint for us-west-2")
	}
	if !warned {
		t.Errorf("no warning for the unreachable table: %v", w.result.Warnings)
	}
}

// TestLastMileFootprintFails: with no lagotto item left, the residue is
// planned; when the footprint cannot be read its re-check fails, so nothing
// is reported removable.
func TestLastMileFootprintFails(t *testing.T) {
	w := newLMWorld(t)
	gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) {
		return inventory.Facts{}, errors.New("cannot list the account's regions")
	}
	runLastMile(listCmdForTest(t))
	for _, a := range w.result.WouldDo {
		if a.Kind == lastmile.KindDelete {
			t.Errorf("would delete %s without a footprint to re-check it against", a.Target)
		}
	}
	var failed bool
	for _, wn := range w.result.Warnings {
		failed = failed || strings.Contains(wn, "cannot list the account's regions")
	}
	if !failed {
		t.Errorf("the footprint failure is not in the warnings: %v", w.result.Warnings)
	}
}

// TestLastMilePollerCheckFails: where it cannot tell whether a poller runs,
// it says so and does not call the region poller-less.
func TestLastMilePollerCheckFails(t *testing.T) {
	w := newLMWorld(t)
	if err := w.participant("watch --local").MarkActive(context.Background(), tags.ToolLagotto, lmWest); err != nil {
		t.Fatal(err)
	}
	lastMilePollerExists = func(context.Context, aws.Config, string, string) (bool, error) {
		return false, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized to perform: lambda:GetFunction"}
	}
	probeCmd(t, "lm-probe", "", nil)
	stdout, stderr, err := execRoot(t, "lm-probe", "-v")
	if err != nil || stdout != "" {
		t.Fatalf("stdout %q, err %v", stdout, err)
	}
	if !strings.Contains(stderr, "debug: lastmile: tool#lagotto#us-west-2: could not tell whether lagotto's poller runs in us-west-2") {
		t.Errorf("no could-not-tell line:\n%s", stderr)
	}
	if strings.Contains(stderr, "no lagotto poller in") {
		t.Errorf("an unreadable poller was reported absent:\n%s", stderr)
	}
}

// TestReadOnlyLifecycle: every write is refused before it reaches the table;
// reads pass through, and after one refused by IAM the rest are answered
// without asking.
func TestReadOnlyLifecycle(t *testing.T) {
	ctx := context.Background()
	inner := lifecycletest.New()
	seen := &lagottoItemRegions{}
	r := readOnlyLifecycle{API: inner, region: lmWest, seen: seen}
	writes := map[string]func() error{
		"PutItem":            func() error { _, err := r.PutItem(ctx, &dynamodb.PutItemInput{}); return err },
		"UpdateItem":         func() error { _, err := r.UpdateItem(ctx, &dynamodb.UpdateItemInput{}); return err },
		"DeleteItem":         func() error { _, err := r.DeleteItem(ctx, &dynamodb.DeleteItemInput{}); return err },
		"TransactWriteItems": func() error { _, err := r.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{}); return err },
		"CreateTable":        func() error { _, err := r.CreateTable(ctx, &dynamodb.CreateTableInput{}); return err },
		"DeleteTable":        func() error { _, err := r.DeleteTable(ctx, &dynamodb.DeleteTableInput{}); return err },
	}
	for op, call := range writes {
		if err := call(); !errors.Is(err, errLastMileReportOnly) {
			t.Errorf("%s = %v, want the report-only refusal", op, err)
		}
		if n := inner.Calls(op); n != 0 {
			t.Errorf("%s reached the table %d time(s)", op, n)
		}
	}

	// DescribeTable passes through (the table does not exist: not found).
	if _, err := r.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(lifecycle.TableName)}); err == nil {
		t.Error("DescribeTable of a missing table succeeded")
	}
	if inner.Calls("DescribeTable") != 1 {
		t.Errorf("DescribeTable reached the table %d time(s), want 1", inner.Calls("DescribeTable"))
	}

	// One refusal by IAM, then no more requests.
	denied := &deniedLifecycle{}
	d := readOnlyLifecycle{API: denied, region: lmWest, seen: &lagottoItemRegions{}}
	if _, err := d.DescribeTable(ctx, &dynamodb.DescribeTableInput{}); err == nil {
		t.Fatal("a denied DescribeTable succeeded")
	}
	_, gerr := d.GetItem(ctx, &dynamodb.GetItemInput{})
	_, serr := d.Scan(ctx, &dynamodb.ScanInput{})
	_, derr := d.DescribeTable(ctx, &dynamodb.DescribeTableInput{})
	for _, err := range []error{gerr, serr, derr} {
		if err == nil || !strings.Contains(err.Error(), lastMileNotAsked) || !lastMileNotPermitted(err.Error()) {
			t.Errorf("after a refusal: %v, want the not-asked answer naming the refusal", err)
		}
	}
	if n := denied.requests(); n != 1 {
		t.Errorf("%d request(s) reached DynamoDB after one refusal, want 1", n)
	}
	// An error that is not a refusal does not stop the asking.
	other := readOnlyLifecycle{API: failingLifecycle{err: errors.New("throttled")}, region: lmWest, seen: &lagottoItemRegions{}}
	_, _ = other.Scan(ctx, &dynamodb.ScanInput{})
	if err := other.seen.deniedErr(); err != nil {
		t.Errorf("a throttle latched the refusal: %v", err)
	}
}

// TestLastMileMaybeDueUnreadable: a state path that cannot be read as a file
// (a directory) is due.
func TestLastMileMaybeDueUnreadable(t *testing.T) {
	if !lastMileMaybeDue(t.TempDir(), lmAccount, lmT0) {
		t.Error("an unreadable state file is not due")
	}
}

// TestLastMileDefaultSeams: the production readers, against a config whose
// HTTP client refuses every request, fail fast with an error and never
// claim a poller exists.
func TestLastMileDefaultSeams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := aws.Config{Region: lmEast, HTTPClient: refusingHTTP{}, Credentials: aws.AnonymousCredentials{}, RetryMaxAttempts: 1}
	if _, err := lmDefaultRegions(ctx, cfg, lmAccount); err == nil {
		t.Error("listing regions through a refusing client succeeded")
	}
	api, err := lmDefaultLifecycleAPI(cfg, lmWest)
	if err != nil || api == nil {
		t.Fatalf("lifecycle API = %v, %v", api, err)
	}
	if c, ok := api.(*dynamodb.Client); !ok {
		t.Errorf("lifecycle API is %T, want a DynamoDB client", api)
	} else if r := c.Options().Region; r != lmWest {
		t.Errorf("lifecycle API for region %q, want %s", r, lmWest)
	}
	if ok, _ := lmDefaultPollerExists(ctx, cfg, lmAccount, lmWest); ok {
		t.Error("a poller reported present through a refusing client")
	}
}

// ---- the account-wide teardown (lagotto#340) --------------------------------

// lmAccountWide gives the world lagotto's account-wide IAM and nothing else.
func lmAccountWide(w *lmWorld) {
	w.facts.Account = inventory.Account{
		Roles: []inventory.Item{
			{ARN: runtimeiam.RoleARN("aws", lmAccount), Name: runtimeiam.RoleName, Tags: map[string]string{tags.ManagedBy: string(tags.ToolLagotto)}},
			{ARN: runtimeiam.SchedulerInvokeRoleARN("aws", lmAccount), Name: runtimeiam.SchedulerInvokeRoleName, Tags: map[string]string{tags.ManagedBy: string(tags.ToolLagotto)}},
		},
	}
}

// idleGlobal records the global scope idle since d ago, as a reap-mode run
// of the account-wide teardown would have.
func (w *lmWorld) idleGlobal(d time.Duration) {
	w.t.Helper()
	w.advance(-d)
	if _, _, err := w.participant("earlier run").Observe(context.Background(), tags.ToolLagotto, lifecycle.ScopeGlobal, false); err != nil {
		w.t.Fatal(err)
	}
	w.advance(d)
}

// TestLastMileAccountWideReport: past the global grace, with no poller, job
// or item anywhere, the last mile reports what the account-wide teardown
// would remove, and writes nothing; with a poller left, it reports the
// blocker.
func TestLastMileAccountWideReport(t *testing.T) {
	w := newLMWorld(t)
	lmAccountWide(w)
	w.idleGlobal(25 * time.Hour)
	before := w.writes()
	probeCmd(t, "lm-probe", "", nil)
	_, stderr, err := execRoot(t, "lm-probe", "--verbose")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`debug: lastmile: account-wide REPORT outcome=would-delete type=AWS::IAM::Role name="lagotto-capacity-poller-role"`,
		`debug: lastmile: account-wide REPORT outcome=would-remove why="nothing needs them"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr)
		}
	}
	if w.writes() != before {
		t.Errorf("%d write(s); report mode writes none", w.writes()-before)
	}

	w = newLMWorld(t)
	lmAccountWide(w)
	w.facts.Regions[1].Function = inventorytest.Deployed("us-west-2", "123456789012").Function
	w.facts.Regions[1].Schedules = inventorytest.Deployed("us-west-2", "123456789012").Schedules
	probeCmd(t, "lm-probe", "", nil)
	if _, stderr, err = execRoot(t, "lm-probe", "--verbose"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr, `account-wide REPORT outcome=blocked`) || !strings.Contains(stderr, "us-west-2: a poller is deployed") {
		t.Errorf("no blocker reported:\n%s", stderr)
	}
	// A function lagotto does not remove (untagged: an older lagotto's, or
	// not lagotto's) is named as such: no poller will ever remove it.
	w = newLMWorld(t)
	lmAccountWide(w)
	w.facts.Regions[1].Function = &inventory.Item{Name: "lagotto-capacity-poller"}
	probeCmd(t, "lm-probe", "", nil)
	if _, stderr, err = execRoot(t, "lm-probe", "--verbose"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr, "us-west-2: a function named lagotto-capacity-poller is there that lagotto does not remove") {
		t.Errorf("an unremovable function not named:\n%s", stderr)
	}
}

// TestLastMileAccountWideReapIsGated: SPORE_LASTMILE=reap alone still only
// reports (this release ships report-only); with the gate open (C8, libs#79)
// the same run removes the account-wide resources under the global lease.
func TestLastMileAccountWideReapIsGated(t *testing.T) {
	for _, gate := range []bool{false, true} {
		t.Run(map[bool]string{false: "gate closed", true: "gate open"}[gate], func(t *testing.T) {
			w := newLMWorld(t)
			lmAccountWide(w)
			w.idleGlobal(25 * time.Hour)
			t.Setenv(lastmile.EnvVar, "reap")
			prev := lastMileAccountWideReap
			lastMileAccountWideReap = gate
			t.Cleanup(func() { lastMileAccountWideReap = prev })
			_, rec, _ := stubPlanTeardown(t, func() inventory.Facts { return w.facts })
			installFakeLeases(t, w.fake(lmEast))
			before := w.writes()
			probeCmd(t, "lm-probe", "", nil)
			_, stderr, err := execRoot(t, "lm-probe", "--verbose")
			if err != nil {
				t.Fatal(err)
			}
			if gate {
				if len(rec.calls) != 2 || !strings.Contains(stderr, "account-wide REAP outcome=removed") {
					t.Errorf("deleted %v\n%s", rec.calls, stderr)
				}
				return
			}
			if len(rec.calls) != 0 || w.writes() != before || !strings.Contains(stderr, "account-wide REPORT outcome=would-remove") {
				t.Errorf("gate closed: deleted %v, %d writes\n%s", rec.calls, w.writes()-before, stderr)
			}
		})
	}
}
