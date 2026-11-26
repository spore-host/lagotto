package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lastmile"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The lazy CLI last mile (libs#78, part of leave-no-trace
// spore-host/spore-host#752). After a command succeeds, the root command's
// post-run hook spends at most lastMileBudget checking, with the caller's own
// credentials, whether lagotto left something behind that no backstop will
// remove: a teardown whose holder died (a stale spore-host-lifecycle lease),
// the residue every region shares once no lagotto lifecycle item is left, an
// empty spore-host-lifecycle table, and a lagotto item in a region with no
// poller (`watch --local`, lagotto#328), which nothing will ever observe idle.
//
// It only reports, and never gets in the way of the command:
//
//   - It changes nothing. libs/lastmile runs in report mode (SPORE_LASTMILE=reap
//     still reports until libs#79), and on top of that the lifecycle client it
//     is given refuses every DynamoDB write and its Deleter refuses Delete.
//   - It writes nothing to stdout, and nothing at all unless --verbose, when
//     its lines go to stderr prefixed "debug: ". A read the caller is not
//     allowed (lagotto-cli-user grants none of its reads) is one such line,
//     never a warning.
//   - It cannot change the exit status: the hook always returns nil, recovers
//     any panic, and runs only after the command itself succeeded.
//   - It cannot hold the command past lastMileBudget (+ lastMileSlack): the
//     work runs in a goroutine the hook stops waiting for.
//   - It makes no STS call: the account is the one the command's own
//     GetCallerIdentity reported (awscfg.CallerAccount); a command that made
//     none skips it. And between checks (every 6 h per account) it makes no
//     AWS call at all: lastMileMaybeDue reads the rate-limit state first. A
//     check the budget cut off resumes an hour later, not on the next
//     command (lastMileRetryAfter).
//
// Regions are every region enabled for the account (ec2:DescribeRegions,
// which lagotto-cli-user grants): the residue may be judged removable only
// when no lagotto item is left in any region lagotto can have used, and
// `lagotto footprint --all-regions` reads the same set. Listing only the
// regions where lagotto's tables or schedules exist would need those same
// per-region reads just to build the list. The cost, once per 6 h per
// account, is one DescribeRegions and about three DynamoDB reads per region
// (the lagotto item, a scan of the table, the residue re-read), plus one
// footprint of every region, only when a stale lease is found or no lagotto
// item is left anywhere; all of it inside the budget, and a region the budget
// cuts off is checked an hour later.

// lastMileSkip are the top-level commands after which the last mile does not
// run, and why.
var lastMileSkip = map[string]string{
	"completion":                    "shell completion must stay silent and fast",
	cobra.ShellCompRequestCmd:       "shell completion must stay silent and fast",
	cobra.ShellCompNoDescRequestCmd: "shell completion must stay silent and fast",
	"help":                          "help text needs no AWS",
	"version":                       "version needs no AWS",
	"footprint":                     "it is lagotto's own inventory",
	"teardown":                      "it is lagotto's own teardown",
	"doctor":                        "its report must hold only what its own checks read",
	"gen-docs":                      "it builds the command reference offline (make check-docs)",
}

// Seams. Tests replace them; production uses the values here.
var (
	lastMileRun       = lastmile.Run
	lastMileBudget    = lastmile.DefaultBudget
	lastMileAccount   = awscfg.CallerAccount
	lastMileStatePath = lastmile.DefaultStatePath
	lastMileNow       = time.Now
	// lastMileRegions lists the regions enabled for the account.
	lastMileRegions = func(ctx context.Context, cfg aws.Config, account string) ([]string, error) {
		return inventory.NewAWSSource(cfg, account).EnabledRegions(ctx)
	}
	// lastMileLifecycleAPI is the DynamoDB client of region's lifecycle table.
	lastMileLifecycleAPI = func(cfg aws.Config, region string) (lifecycle.API, error) {
		c := cfg.Copy()
		c.Region = region
		return dynamodb.NewFromConfig(c), nil
	}
	// lastMilePollerExists reports whether lagotto's poller runs in region.
	lastMilePollerExists = func(ctx context.Context, cfg aws.Config, account, region string) (bool, error) {
		it, _, err := inventory.NewAWSSource(cfg, account).Region(region).Poller(ctx)
		return it != nil, err
	}
)

// lastMileSlack is how much longer than its budget the hook waits before it
// stops waiting.
const lastMileSlack = 250 * time.Millisecond

// lastMileReserve is the fraction of the budget kept back from lastmile.Run
// for the poller check and the account-wide teardown after it (10 s of the
// default 30 s, as 1/lastMileReserve).
const lastMileReserve = 3

// errLastMileReportOnly is what the last mile's lifecycle client and Deleter
// return for any write: lagotto's last mile is report-only (libs#79 and
// lagotto#328 decide when it may reap).
var errLastMileReportOnly = errors.New("lagotto's last mile is report-only: it writes and deletes nothing")

// lastMileHook is the root command's PersistentPostRunE. It always returns
// nil.
func lastMileHook(cmd *cobra.Command, _ []string) error {
	runLastMile(cmd)
	return nil
}

// lastMileCommandName is the top-level command cmd belongs to ("completion"
// for `completion bash`).
func lastMileCommandName(cmd *cobra.Command) string {
	for cmd.HasParent() && cmd.Parent().HasParent() {
		cmd = cmd.Parent()
	}
	return cmd.Name()
}

// runLastMile runs the last mile after cmd, unless cmd is one it skips or
// SPORE_LASTMILE=off, and waits for it at most lastMileBudget+lastMileSlack.
func runLastMile(cmd *cobra.Command) {
	defer func() { _ = recover() }() // nothing here may fail the command
	if _, skip := lastMileSkip[lastMileCommandName(cmd)]; skip {
		return
	}
	mode := lastmile.ModeFromEnv()
	if mode == lastmile.ModeOff {
		return
	}
	log := &lastMileLog{}
	if verbose {
		log.w = cmd.ErrOrStderr()
	}
	defer log.close()

	budget := lastMileBudget
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if p := recover(); p != nil {
				log.printf("lastmile: internal error (recovered): %v", p)
			}
		}()
		lastMile(ctx, mode, log.printf)
	}()
	timer := time.NewTimer(budget + lastMileSlack)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.printf("lastmile: not finished within its %s budget; not waiting for it", budget)
	}
}

// lastMile is the work: build the options and run lastmile.Run.
func lastMile(ctx context.Context, mode lastmile.Mode, logf func(string, ...any)) {
	account := lastMileAccount()
	if account == "" {
		logf("lastmile: skipped: this command resolved no AWS account, and the last mile makes no STS call of its own")
		return
	}
	statePath, err := lastMileStatePath()
	if err != nil {
		logf("lastmile: skipped: %v", err)
		return
	}
	if !lastMileMaybeDue(statePath, account, lastMileNow()) {
		logf("lastmile: skipped: rate-limited (checked within the last %s, or cut off by its budget within the last %s)",
			lastmile.DefaultMinInterval, lastMileRetryAfter)
		return
	}
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		logf("lastmile: skipped: load AWS config: %v", err)
		return
	}
	control, err := lastMileControlRegion(cfg.Region)
	if err != nil {
		// Outside aws and aws-us-gov there is no global scope to
		// read (lagotto#347): the regional checks still run, and the global
		// scope's, and the account-wide teardown, are skipped.
		logf("lastmile: the global scope is skipped: %v", err)
		control = ""
	}
	if cfg.Region == "" {
		cfg.Region = control
	}
	regions, err := lastMileRegions(ctx, cfg, account)
	if err != nil {
		logf("lastmile: skipped: list the account's regions: %v", err)
		return
	}
	lc, seen, err := newLastMileLifecycle(cfg, control)
	if err != nil {
		logf("lastmile: skipped: %v", err)
		return
	}
	live := &lastMileFootprint{load: func(ctx context.Context) (inventory.Facts, error) {
		return gatherFootprint(ctx, cfg, account, inventory.Scope{AllRegions: true})
	}}
	o := lastMileOptions(account, regions, lc, live)
	o.Mode, o.StatePath, o.Logf, o.Now = mode, statePath, logf, lastMileNow
	if deadline, ok := ctx.Deadline(); ok {
		left := time.Until(deadline)
		o.Budget = left - left/lastMileReserve
		if o.Budget <= 0 {
			logf("lastmile: skipped: no budget left")
			return
		}
	}
	res := lastMileRun(ctx, o)
	logf("%s", res.Summary())
	if res.Ran {
		lastMilePollerless(ctx, cfg, account, seen.list(), logf)
		lastMileAccountWide(ctx, mode, cfg, account, regions, lc, live, logf)
	}
}

// lastMileOptions are the lastmile.Options for lagotto in account: everything
// but the mode, state path, logger, clock and budget.
//
// The residue is what outlives every region's teardown: in each region the
// lagotto-teardown schedule group the poller's self-removal jobs run from.
// Nothing account-wide is listed: lagotto's account-wide resources (the
// Scheduler invoke role among them) are the account-wide teardown's alone
// (lastMileAccountWide, lagotto#340), which runs after lastmile.Run and
// checks every region's poller, jobs and lifecycle item before it would
// delete one. The spore-host-lifecycle tables lagotto leaves, including the one
// a `watch --local` makes in a region without a poller, are not listed: they
// are the suite's (managed-by=spore-host), so lagotto's delete guard refuses
// them, and lastmile already reports each empty one in every region of
// Regions, to be closed with CloseTableIfEmpty, which re-checks every tool's
// items under its sentinel. While the poller-less region's lagotto item
// remains, lastMilePollerless says so.
//
// FullVisibility is false: the CLI cannot see every user's watches, so it
// never judges lagotto idle; the poller in each region does.
func lastMileOptions(account string, regions []string, lc *lifecycle.Client, live *lastMileFootprint) lastmile.Options {
	return lastmile.Options{
		Tool:           tags.ToolLagotto,
		Account:        account,
		Regions:        regions,
		Lifecycle:      lc,
		Footprint:      live.get,
		Deleter:        reportOnlyDeleter{live: live},
		Patterns:       inventory.Patterns(footprintOptions()),
		Residue:        lastMileResidue(account, regions),
		FullVisibility: false,
	}
}

// lastMileResidue is lagotto's residue in account (see lastMileOptions).
func lastMileResidue(account string, regions []string) []fp.Resource {
	var out []fp.Resource
	for _, r := range regions {
		out = append(out, fp.Resource{
			ARN: inventory.ScheduleGroupARN(r, account, runtimeiam.TeardownScheduleGroup), Type: inventory.TypeScheduleGroup,
			Region: r, Name: runtimeiam.TeardownScheduleGroup, Ownership: fp.OwnershipTagged, Class: fp.ClassResidue,
			Tags: map[string]string{tags.ManagedBy: string(tags.ToolLagotto)},
		})
	}
	return out
}

// lastMileControlRegion is the region holding the global scope's lifecycle
// items, so every invocation reads the same one whatever --region says: the
// account-wide teardown's (idle.ControlRegion), whose global lease lives
// there (lagotto#340): us-east-1, or us-gov-west-1 in aws-us-gov. Outside aws
// and aws-us-gov there is none (lagotto#347).
func lastMileControlRegion(region string) (string, error) { return idle.ControlRegion(region) }

// lastMileFootprint is lagotto's footprint of every region, read at most once
// per run: lastmile's Footprint, the Deleter's re-checks and the account-wide
// teardown's first read share it.
//
// A mutex, not a sync.Once: the IAM inventory's call graph (internal/
// iamcoverage) merges every function passed to sync.Once.Do, so a Once here
// would charge the footprint's reads to every command that uses one.
type lastMileFootprint struct {
	load  func(ctx context.Context) (inventory.Facts, error)
	mu    sync.Mutex
	done  bool
	facts inventory.Facts
	rep   *fp.Report
	err   error
}

func (l *lastMileFootprint) read(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		return
	}
	l.done = true
	if l.facts, l.err = l.load(ctx); l.err != nil {
		l.err = fmt.Errorf("footprint: %w", l.err)
		return
	}
	l.rep = inventory.Classify(l.facts, footprintOptions(), footprintNow())
}

func (l *lastMileFootprint) get(ctx context.Context) (*fp.Report, error) {
	l.read(ctx)
	return l.rep, l.err
}

// facts is the footprint's facts, for the account-wide teardown.
func (l *lastMileFootprint) factsOf(ctx context.Context) (inventory.Facts, error) {
	l.read(ctx)
	return l.facts, l.err
}

// reportOnlyDeleter re-checks a planned resource against the live footprint
// as lagotto's own Deleter does (inventory.Recheck: gone is not-found, in use
// is refused), and refuses every Delete. It holds none of lagotto's deleters.
type reportOnlyDeleter struct {
	live *lastMileFootprint
}

func (d reportOnlyDeleter) Describe(ctx context.Context, r fp.Resource) (fp.Resource, error) {
	rep, err := d.live.get(ctx)
	if err != nil {
		return fp.Resource{}, err
	}
	return inventory.Recheck(rep, r)
}

func (reportOnlyDeleter) Delete(context.Context, fp.Resource) error { return errLastMileReportOnly }

// newLastMileLifecycle is the last mile's lifecycle client: reads only, each
// region's lifecycle table through lastMileLifecycleAPI, with the global
// scope in control. seen collects the regions where it read a lagotto item.
func newLastMileLifecycle(cfg aws.Config, control string) (*lifecycle.Client, *lagottoItemRegions, error) {
	seen := &lagottoItemRegions{}
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB: func(region string) (lifecycle.API, error) {
			a, err := lastMileLifecycleAPI(cfg, region)
			if err != nil {
				return nil, err
			}
			return readOnlyLifecycle{API: a, region: region, seen: seen}, nil
		},
		ControlRegion: control,
		Owner:         "lagotto last mile (report)",
		Now:           lastMileNow,
	})
	return c, seen, err
}

// readOnlyLifecycle passes the lifecycle table's reads through and refuses
// its writes, so the last mile changes nothing even if lastmile tried. It
// also records each region whose lagotto item a GetItem returned, and once
// IAM has refused one read it refuses the rest itself: a caller without the
// grant (lagotto-cli-user) costs one denied request per run, not one per
// region.
type readOnlyLifecycle struct {
	lifecycle.API
	region string
	seen   *lagottoItemRegions
}

func (r readOnlyLifecycle) GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if err := r.seen.deniedErr(); err != nil {
		return nil, err
	}
	out, err := r.API.GetItem(ctx, in, opts...)
	r.seen.noteDenied(err)
	if err == nil && out != nil && len(out.Item) > 0 && in != nil {
		want := lifecycle.ItemKey(tags.ToolLagotto, r.region)
		for _, v := range in.Key {
			if s, ok := v.(*types.AttributeValueMemberS); ok && s.Value == want {
				r.seen.add(r.region)
			}
		}
	}
	return out, err
}

func (r readOnlyLifecycle) Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if err := r.seen.deniedErr(); err != nil {
		return nil, err
	}
	out, err := r.API.Scan(ctx, in, opts...)
	r.seen.noteDenied(err)
	return out, err
}

func (r readOnlyLifecycle) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if err := r.seen.deniedErr(); err != nil {
		return nil, err
	}
	out, err := r.API.DescribeTable(ctx, in, opts...)
	r.seen.noteDenied(err)
	return out, err
}

func (readOnlyLifecycle) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return nil, errLastMileReportOnly
}

func (readOnlyLifecycle) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, errLastMileReportOnly
}

func (readOnlyLifecycle) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, errLastMileReportOnly
}

func (readOnlyLifecycle) TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, errLastMileReportOnly
}

func (readOnlyLifecycle) CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return nil, errLastMileReportOnly
}

func (readOnlyLifecycle) DeleteTable(context.Context, *dynamodb.DeleteTableInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	return nil, errLastMileReportOnly
}

// lastMileNotAsked starts the error of a read the latch answered itself.
const lastMileNotAsked = "not asked: an earlier read of the"

// lagottoItemRegions is what the last mile's lifecycle reads saw: the regions
// holding a lagotto item, in the order first seen, and the first refusal by
// IAM.
type lagottoItemRegions struct {
	mu      sync.Mutex
	regions []string
	denied  error
}

func (s *lagottoItemRegions) noteDenied(err error) {
	if err == nil || !lastMileNotPermitted(err.Error()) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denied == nil {
		s.denied = err
	}
}

func (s *lagottoItemRegions) deniedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denied == nil {
		return nil
	}
	return fmt.Errorf("%s %s table was refused (%w)", lastMileNotAsked, lifecycle.TableName, s.denied)
}

func (s *lagottoItemRegions) add(region string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.regions {
		if r == region {
			return
		}
	}
	s.regions = append(s.regions, region)
}

func (s *lagottoItemRegions) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.regions...)
}

// lastMilePollerless reports each of regions (where a lagotto lifecycle item
// is) that has no poller: nothing will ever observe lagotto idle there, so
// the item stays, and with it the residue and the region's lifecycle table
// (lagotto#328: `watch --local` in a poller-less region).
func lastMilePollerless(ctx context.Context, cfg aws.Config, account string, regions []string, logf func(string, ...any)) {
	for _, region := range regions {
		key := lifecycle.ItemKey(tags.ToolLagotto, region)
		ok, err := lastMilePollerExists(ctx, cfg, account, region)
		switch {
		case err != nil:
			logf("lastmile: %s: could not tell whether lagotto's poller runs in %s: %v", key, region, err)
		case !ok:
			logf("lastmile: %s: no lagotto poller in %s (a 'watch --local' region), so nothing will tear lagotto down there; "+
				"the item keeps lagotto's residue and the %s table until 'lagotto teardown --plan' (lagotto#328)",
				key, region, lifecycle.TableName)
		}
	}
}

// lastMileRetryAfter is how long after a run the budget cut off the next one
// waits. lastmile.Run alone would retry on the very next invocation, so an
// account whose check never fits the budget would hold every command for the
// whole budget; this bounds that to one wait an hour.
const lastMileRetryAfter = time.Hour

// lastMileMaybeDue reports whether lastmile.Run could have anything to check
// for lagotto in account, from the rate-limit state file alone, so that a run
// that is not due costs no AWS call (not even the DescribeRegions Regions
// needs). It reads the format libs/lastmile documents (version 1).
//
// It is not due within lastMileRetryAfter of a run the budget cut off (the
// scope it was in is marked incomplete, with the attempt's time). Otherwise
// it is due unless the global scope, which every run checks last, was checked
// to the end within the minimum interval; a region that falls due before it
// waits at most until then. Anything it cannot read is due, and lastmile.Run
// makes the exact decision.
func lastMileMaybeDue(path, account string, now time.Time) bool {
	f, err := os.Open(path) // #nosec G304 -- the caller's own lastmile state file
	if err != nil {
		return true
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return true
	}
	var st struct {
		Version int `json:"version"`
		Runs    map[string]struct {
			LastRun     time.Time `json:"last_run"`
			LastAttempt time.Time `json:"last_attempt"`
			Incomplete  string    `json:"incomplete"`
		} `json:"runs"`
	}
	if json.Unmarshal(b, &st) != nil || st.Version != 1 {
		return true
	}
	prefix := string(tags.ToolLagotto) + "/" + account + "/"
	for key, rec := range st.Runs {
		if strings.HasPrefix(key, prefix) && rec.Incomplete != "" &&
			!rec.LastAttempt.After(now) && now.Sub(rec.LastAttempt) < lastMileRetryAfter {
			return false
		}
	}
	rec, ok := st.Runs[prefix+lifecycle.ScopeGlobal]
	if !ok || rec.Incomplete != "" || rec.LastRun.IsZero() || rec.LastRun.After(now) {
		return true
	}
	return now.Sub(rec.LastRun) >= lastmile.DefaultMinInterval
}

// lastMileLog writes the last mile's lines to w (stderr under --verbose, else
// nowhere), each prefixed "debug: ". After close it drops them, so a run the
// hook stopped waiting for cannot write once the command has moved on.
type lastMileLog struct {
	mu     sync.Mutex
	w      io.Writer
	closed bool
}

func (l *lastMileLog) printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil || l.closed {
		return
	}
	msg := strings.TrimRight(fmt.Sprintf(format, args...), "\n")
	if strings.Contains(msg, lastMileNotAsked) {
		return // the first refusal was logged; the rest repeat it
	}
	if lastMileNotPermitted(msg) {
		// What lagotto-cli-user does not grant is expected: a debug note,
		// not a warning.
		msg = strings.Replace(msg, "lastmile: warning: ", "lastmile: not permitted, skipped: ", 1)
	}
	_, _ = fmt.Fprintf(l.w, "debug: %s\n", msg)
}

func (l *lastMileLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
}

// lastMileNotPermitted reports whether a line is about a read IAM refused.
func lastMileNotPermitted(msg string) bool {
	for _, s := range []string{"AccessDenied", "UnauthorizedOperation", "not authorized to perform"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
