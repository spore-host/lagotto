package watcher

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/spawn/pkg/launcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// ── #239 phase 1: heartbeat fields, folded into the writes a cycle already makes ──

// msgErr is a smithy.APIError whose message differs from its code.
type msgErr struct{ code, msg string }

func (e *msgErr) Error() string                 { return e.code + ": " + e.msg }
func (e *msgErr) ErrorCode() string             { return e.code }
func (e *msgErr) ErrorMessage() string          { return e.msg }
func (e *msgErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// recordingDynamo wraps the substrate-backed client and records every write, so
// a test can assert the exact UpdateItem expressions a cycle makes and that it
// makes no more writes than before.
type recordingDynamo struct {
	dynamoAPI
	updates []dynamodb.UpdateItemInput
	puts    []dynamodb.PutItemInput
}

func (r *recordingDynamo) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, o ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	r.updates = append(r.updates, *in)
	return r.dynamoAPI.UpdateItem(ctx, in, o...)
}

func (r *recordingDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, o ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	r.puts = append(r.puts, *in)
	return r.dynamoAPI.PutItem(ctx, in, o...)
}

func (r *recordingDynamo) reset() { r.updates, r.puts = nil, nil }

// exprs lists the recorded UpdateItem expressions, in order.
func (r *recordingDynamo) exprs() []string {
	out := make([]string, 0, len(r.updates))
	for _, u := range r.updates {
		out = append(out, aws.ToString(u.UpdateExpression))
	}
	return out
}

// watchPuts counts whole-record PutItems to the watches table.
func (r *recordingDynamo) watchPuts() int {
	n := 0
	for _, p := range r.puts {
		if aws.ToString(p.TableName) == fleetWatchesTable {
			n++
		}
	}
	return n
}

// errSearcher is a capacity search that can be made to fail.
type errSearcher struct {
	results []truffleaws.InstanceTypeResult
	err     error
	spotErr error
}

func (f *errSearcher) SearchInstanceTypes(context.Context, []string, *regexp.Regexp, truffleaws.FilterOptions) ([]truffleaws.InstanceTypeResult, error) {
	return f.results, f.err
}
func (f *errSearcher) GetSpotPricing(context.Context, []truffleaws.InstanceTypeResult, truffleaws.SpotOptions) ([]truffleaws.SpotPriceResult, error) {
	return nil, f.spotErr
}

// hbHarness is a single-shot spawn poller over a recording substrate store, whose
// provision returns next(type, region) for each attempt.
type hbHarness struct {
	store  *Store
	rec    *recordingDynamo
	poller *Poller
	search *errSearcher
	next   func(typ, region string) error
}

func newHBHarness(t *testing.T, cands ...qiCandidate) *hbHarness {
	t.Helper()
	store := fleetStore(t)
	rec := &recordingDynamo{dynamoAPI: store.client}
	store.client = rec
	h := &hbHarness{store: store, rec: rec, search: &errSearcher{results: qiResults(cands...)}}
	h.next = func(string, string) error { return nil }
	sp := newSpawnerWithProvision(func(_ context.Context, _ *spawnaws.Client, cfg spawnaws.LaunchConfig, _ launcher.Options) (*spawnaws.LaunchResult, error) {
		if err := h.next(cfg.InstanceType, cfg.Region); err != nil {
			return nil, err
		}
		return &spawnaws.LaunchResult{InstanceID: "i-" + cfg.Region}, nil
	})
	h.poller = &Poller{store: store, spawner: sp, truffle: h.search}
	h.poller.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }
	return h
}

// cycle reloads the watch (as the stateless hosted poller does), runs one
// pollGroup over it with the recorder cleared, and returns the stored result.
func (h *hbHarness) cycle(t *testing.T, id string) *Watch {
	t.Helper()
	ctx := context.Background()
	w, err := h.store.GetWatch(ctx, id)
	if err != nil || w == nil {
		t.Fatalf("GetWatch(%s): %v %v", id, w, err)
	}
	h.rec.reset()
	_ = h.poller.pollGroup(ctx, w.Regions, w.InstanceTypePattern, w.Spot, []*Watch{w}, &PollSummary{})
	got, err := h.store.GetWatch(ctx, id)
	if err != nil || got == nil {
		t.Fatalf("GetWatch(%s): %v %v", id, got, err)
	}
	return got
}

func (h *hbHarness) put(t *testing.T, w *Watch) {
	t.Helper()
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
}

const (
	// The pre-#239 expressions. A write with no heartbeat must still produce
	// exactly these (the public wrappers, and old behaviour).
	exprResetOld     = "SET last_polled_at = :now, consecutive_failures = :zero"
	exprIncrementOld = "SET last_polled_at = :now ADD consecutive_failures :one"
	exprQuotaSetOld  = "SET last_polled_at = :now, quota_infeasible = :q"

	// The #239 expressions: the same write, heartbeat folded in.
	exprResetHB     = "SET last_polled_at = :now, consecutive_failures = :zero, last_attempt_at = :now, last_error = :lasterr, last_error_at = :now ADD attempts :attempts"
	exprIncrementHB = "SET last_polled_at = :now, last_attempt_at = :now, last_error = :lasterr, last_error_at = :now ADD consecutive_failures :one, attempts :attempts"
	exprQuotaSetHB  = "SET last_polled_at = :now, quota_infeasible = :q, last_attempt_at = :now, last_error = :lasterr, last_error_at = :now ADD attempts :attempts"
	exprPollError   = "SET last_poll_error = :err, last_poll_error_at = :now"
	exprStatus      = "SET #st = :status, updated_at = :now, ttl_timestamp = :ttl"
)

func wantExprs(t *testing.T, rec *recordingDynamo, want ...string) {
	t.Helper()
	got := rec.exprs()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("UpdateItem expressions:\n got %q\nwant %q", got, want)
	}
}

// A capacity failure makes exactly the one UpdateItem it always made (the
// streak reset), now carrying the attempt. No PutItem, no second write.
func TestHeartbeat_CapacityCycleFoldsIntoReset(t *testing.T) {
	h := newHBHarness(t, qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})
	h.next = func(string, string) error { return &capErr{"InsufficientInstanceCapacity"} }
	w := qiWatch(t, "w-hb-cap")
	h.put(t, w)

	got := h.cycle(t, w.WatchID)

	wantExprs(t, h.rec, exprResetHB)
	if len(h.rec.puts) != 0 {
		t.Errorf("PutItems = %d, want 0", len(h.rec.puts))
	}
	if got.Attempts != 1 || got.LastAttemptAt.IsZero() || got.LastPolledAt.IsZero() {
		t.Errorf("attempts=%d last_attempt_at=%v last_polled_at=%v", got.Attempts, got.LastAttemptAt, got.LastPolledAt)
	}
	if got.LastError != "capacity: InsufficientInstanceCapacity" || got.LastErrorAt.IsZero() {
		t.Errorf("last_error=%q at %v", got.LastError, got.LastErrorAt)
	}
	if !got.LastErrorAt.Equal(got.LastAttemptAt) || !got.LastAttemptAt.Equal(got.LastPolledAt) {
		t.Errorf("one write, one :now: polled=%v attempt=%v error=%v", got.LastPolledAt, got.LastAttemptAt, got.LastErrorAt)
	}
}

// An unclassified failure makes the one Increment UpdateItem, now also adding
// to attempts, atomically.
func TestHeartbeat_UnknownCycleFoldsIntoIncrement(t *testing.T) {
	h := newHBHarness(t, qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})
	h.next = func(string, string) error { return &capErr{"SomethingNew"} }
	w := qiWatch(t, "w-hb-unk")
	w.Attempts = 41 // pre-existing count: the ADD builds on it
	h.put(t, w)

	got := h.cycle(t, w.WatchID)

	wantExprs(t, h.rec, exprIncrementHB)
	if len(h.rec.puts) != 0 {
		t.Errorf("PutItems = %d, want 0", len(h.rec.puts))
	}
	if got.Attempts != 42 || got.ConsecutiveFailures != 1 {
		t.Errorf("attempts=%d consecutive=%d, want 42/1", got.Attempts, got.ConsecutiveFailures)
	}
	if ErrorClass(got.LastError) != ErrClassUnknown || !strings.Contains(got.LastError, "SomethingNew") {
		t.Errorf("last_error=%q", got.LastError)
	}
}

// The ADD is atomic in the store: two increments from two stale in-memory
// copies both count (a read-modify-write would lose one).
func TestHeartbeat_AttemptsAddIsAtomic(t *testing.T) {
	h := newHBHarness(t)
	w := qiWatch(t, "w-hb-atomic")
	h.put(t, w)
	ctx := context.Background()
	hb := failedAttempt(FailureUnknown, errors.New("x"))
	if _, n, err := h.store.incrementConsecutiveFailures(ctx, w.WatchID, hb); err != nil || n != 1 {
		t.Fatalf("first: n=%d err=%v", n, err)
	}
	if err := h.store.resetConsecutiveFailures(ctx, w.WatchID, hb); err != nil {
		t.Fatal(err)
	}
	if _, n, err := h.store.incrementConsecutiveFailures(ctx, w.WatchID, hb); err != nil || n != 3 {
		t.Fatalf("third: n=%d err=%v, want the store's count (3) back", n, err)
	}
}

// A quota refusal that the watch moves past counts as an attempt, on the
// SetQuotaInfeasible write #238 already makes; the launch that follows is a
// second attempt, carried by RecordMatch's PutWatch. Same write count as #238.
func TestHeartbeat_QuotaFallthroughCountsBothAttempts(t *testing.T) {
	h := newHBHarness(t,
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	)
	h.next = refuse("g7.24xlarge@eu-north-1")
	w := qiWatch(t, "w-hb-qfall")
	h.put(t, w)

	got := h.cycle(t, w.WatchID)

	wantExprs(t, h.rec, exprQuotaSetHB, exprStatus)
	// history row (quota_infeasible) + RecordMatch's watch PutWatch + its history row.
	if len(h.rec.puts) != 3 || h.rec.watchPuts() != 1 {
		t.Errorf("PutItems = %d (watch %d), want 3 (watch 1)", len(h.rec.puts), h.rec.watchPuts())
	}
	if got.Status != StatusMatched || got.Attempts != 2 {
		t.Fatalf("status=%q attempts=%d, want matched/2", got.Status, got.Attempts)
	}
	// The success leaves the quota refusal as the last error.
	if !strings.HasPrefix(got.LastError, "quota: VcpuLimitExceeded: g7.24xlarge in eu-north-1; needs 96 vCPU") {
		t.Errorf("last_error=%q", got.LastError)
	}
}

// Every candidate refused: the final refusal is counted on RecordMatch's PutWatch
// and last_error carries the quota code.
func TestHeartbeat_QuotaTerminalCounts(t *testing.T) {
	h := newHBHarness(t, qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})
	h.next = refuse("g7.24xlarge@us-west-2")
	w := qiWatch(t, "w-hb-qterm")
	w.Regions = []string{"us-west-2"}
	h.put(t, w)

	got := h.cycle(t, w.WatchID)

	wantExprs(t, h.rec, exprStatus)
	if got.Status != StatusFailed || got.Attempts != 1 || ErrorClass(got.LastError) != ErrClassQuota ||
		!strings.Contains(got.LastError, "VcpuLimitExceeded") {
		t.Errorf("status=%q attempts=%d last_error=%q", got.Status, got.Attempts, got.LastError)
	}
}

// A failed capacity search stamps last_poll_error with one UpdateItem and does
// NOT touch last_polled_at (so LAST POLL goes stale) or attempts (nothing was
// tried). Before #239 this cycle wrote nothing at all.
func TestHeartbeat_SearchErrorStampsPollError(t *testing.T) {
	h := newHBHarness(t, qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})
	h.search.err = errors.New("throttled:\n  DescribeInstanceTypeOfferings rate exceeded")
	w := qiWatch(t, "w-hb-search")
	h.put(t, w)

	got := h.cycle(t, w.WatchID)

	wantExprs(t, h.rec, exprPollError)
	if c := aws.ToString(h.rec.updates[0].ConditionExpression); c != "attribute_exists(#st)" {
		t.Errorf("condition = %q", c)
	}
	if got.LastPollError != "search: throttled: DescribeInstanceTypeOfferings rate exceeded" || got.LastPollErrorAt.IsZero() {
		t.Errorf("last_poll_error=%q at %v", got.LastPollError, got.LastPollErrorAt)
	}
	if !got.LastPolledAt.IsZero() || got.Attempts != 0 {
		t.Errorf("last_polled_at=%v attempts=%d, want untouched", got.LastPolledAt, got.Attempts)
	}

	// The spot-pricing half of the search is stamped the same way.
	h.search.err = nil
	h.search.spotErr = errors.New("spot price history unavailable")
	ws := qiWatch(t, "w-hb-spot")
	ws.Spot = true
	h.put(t, ws)
	got = h.cycle(t, ws.WatchID)
	wantExprs(t, h.rec, exprPollError)
	if got.LastPollError != "search: spot price history unavailable" {
		t.Errorf("spot last_poll_error=%q", got.LastPollError)
	}
}

// A poll error never creates a stub item for a watch that has gone.
func TestHeartbeat_PollErrorNeedsExistingWatch(t *testing.T) {
	h := newHBHarness(t)
	h.poller.verbose = true
	h.poller.recordPollError(context.Background(), []*Watch{{WatchID: "w-gone"}}, errors.New("boom"))
	if w, err := h.store.GetWatch(context.Background(), "w-gone"); err != nil || w != nil {
		t.Fatalf("GetWatch = %+v, %v; want no item", w, err)
	}
}

// The whole story across cycles: capacity, unknown, quota (moved past, nothing
// else to try this cycle), search error, then a launch. Attempts counts the four
// attempts and skips the search error; the latest error is always visible.
func TestHeartbeat_AttemptsAcrossCycles(t *testing.T) {
	h := newHBHarness(t,
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	)
	w := qiWatch(t, "w-hb-story")
	h.put(t, w)

	type step struct {
		name      string
		next      func(typ, region string) error
		searchErr error
		attempts  int
		class     string // ErrorClass of LatestError()
		status    WatchStatus
	}
	capacity := func(string, string) error { return &capErr{"InsufficientInstanceCapacity"} }
	unknown := func(string, string) error { return errors.New("connection reset by peer") }
	// eu-north-1 refused on quota, us-west-2 short of capacity: the quota
	// refusal and the capacity failure are two attempts in one cycle.
	quotaThenCap := func(typ, region string) error {
		if region == "eu-north-1" {
			return &capErr{quotaCode}
		}
		return &capErr{"InsufficientInstanceCapacity"}
	}
	ok := func(string, string) error { return nil }
	steps := []step{
		{"capacity", capacity, nil, 1, ErrClassCapacity, StatusActive},
		{"unknown", unknown, nil, 2, ErrClassUnknown, StatusActive},
		{"quota+capacity", quotaThenCap, nil, 4, ErrClassCapacity, StatusActive},
		{"search error", ok, errors.New("RequestLimitExceeded"), 4, ErrClassSearch, StatusActive},
		{"launch", ok, nil, 5, ErrClassSearch, StatusMatched},
	}
	for _, s := range steps {
		h.next, h.search.err = s.next, s.searchErr
		got := h.cycle(t, w.WatchID)
		latest, _ := got.LatestError()
		if got.Attempts != s.attempts || ErrorClass(latest) != s.class || got.Status != s.status {
			t.Fatalf("after %s: attempts=%d latest=%q status=%q; want %d/%s/%s",
				s.name, got.Attempts, latest, got.Status, s.attempts, s.class, s.status)
		}
		if s.name == "quota+capacity" {
			// Timestamps are RFC3339 seconds: make the search error strictly later.
			time.Sleep(1100 * time.Millisecond)
		}
	}
}

// The fleet top-up closes its cycle with the write it always made, now carrying
// the cycle's attempts: two launches plus the refused one.
func TestHeartbeat_FleetCycleCountsAttempts(t *testing.T) {
	store := fleetStore(t)
	rec := &recordingDynamo{dynamoAPI: store.client}
	store.client = rec
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-hb-fleet")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	n := 0
	p, _ := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) {
		n++
		if n >= 3 {
			return nil, &capErr{quotaCode}
		}
		return &spawnaws.LaunchResult{InstanceID: fmt.Sprintf("i-%d", n)}, nil
	})
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }
	rec.reset()
	p.pollFleetWatch(ctx, w, &PollSummary{})

	exprs := rec.exprs()
	last := exprs[len(exprs)-1]
	if want := "SET last_polled_at = :now, last_attempt_at = :now, last_error = :lasterr, last_error_at = :now ADD attempts :attempts"; last != want {
		t.Errorf("closing write = %q, want %q (all: %q)", last, want, exprs)
	}
	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempts != 3 || ErrorClass(got.LastError) != ErrClassQuota {
		t.Errorf("attempts=%d last_error=%q, want 3 / quota", got.Attempts, got.LastError)
	}

	// A cycle with nothing to try closes with the plain pre-#239 touch.
	if err := store.recordFleetCycle(ctx, w.WatchID, 0, ""); err != nil {
		t.Fatal(err)
	}
	if e := rec.exprs()[len(rec.exprs())-1]; e != "SET last_polled_at = :now" {
		t.Errorf("no-attempt close = %q", e)
	}
}

// A fleet whose search fails stamps the poll error instead of touching
// last_polled_at, the same as a single-shot watch.
func TestHeartbeat_FleetSearchError(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-hb-fleet-search")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	p, _ := quotaPoller(t, store, w.WatchID, 0, func() (*spawnaws.LaunchResult, error) { return nil, nil })
	p.truffle = &errSearcher{err: errors.New("search down")}
	p.pollFleetWatch(ctx, w, &PollSummary{})
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.LastPollError == "" || !got.LastPolledAt.IsZero() {
		t.Errorf("last_poll_error=%q last_polled_at=%v", got.LastPollError, got.LastPolledAt)
	}
}

// The public pre-#239 store methods still write exactly what they always did.
func TestHeartbeat_PublicWrappersUnchanged(t *testing.T) {
	h := newHBHarness(t)
	w := qiWatch(t, "w-hb-wrap")
	h.put(t, w)
	ctx := context.Background()
	h.rec.reset()
	if _, err := h.store.IncrementConsecutiveFailures(ctx, w.WatchID); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ResetConsecutiveFailures(ctx, w.WatchID); err != nil {
		t.Fatal(err)
	}
	if err := h.store.SetQuotaInfeasible(ctx, w.WatchID, []QuotaInfeasibility{{InstanceType: "a", Region: "b", At: time.Now()}}); err != nil {
		t.Fatal(err)
	}
	if err := h.store.SetQuotaInfeasible(ctx, w.WatchID, nil); err != nil {
		t.Fatal(err)
	}
	wantExprs(t, h.rec, exprIncrementOld, exprResetOld, exprQuotaSetOld, "REMOVE quota_infeasible SET last_polled_at = :now")
	got, _ := h.store.GetWatch(ctx, w.WatchID)
	if got.Attempts != 0 || got.LastError != "" {
		t.Errorf("attempts=%d last_error=%q, want untouched", got.Attempts, got.LastError)
	}
}

// Store errors on the new write paths surface, wrapped.
func TestHeartbeat_StoreErrors(t *testing.T) {
	env := fleetStore(t)
	bad := &Store{client: env.client, watchesTable: "no-such-table", historyTable: "no-such-history"}
	ctx := context.Background()
	hb := &attempt{Err: "capacity"}
	if _, _, err := bad.incrementConsecutiveFailures(ctx, "w", hb); err == nil {
		t.Error("increment: want error")
	}
	if err := bad.recordFleetCycle(ctx, "w", 1, "x"); err == nil {
		t.Error("fleet cycle: want error")
	}
	if err := bad.recordPollError(ctx, "w", "x"); err == nil {
		t.Error("poll error: want error")
	}
}

func TestDescribeAttemptError(t *testing.T) {
	long := strings.Repeat("é", 300) // 600 bytes of two-byte runes
	cases := []struct {
		name string
		kind FailureKind
		err  error
		want string
	}{
		{"capacity", FailureCapacity, &msgErr{"InsufficientInstanceCapacity", "We currently do not have sufficient p5.48xlarge capacity"},
			"capacity: InsufficientInstanceCapacity: We currently do not have sufficient p5.48xlarge capacity"},
		{"quota wins over terminal", FailureTerminal, &msgErr{"VcpuLimitExceeded", "You have requested more vCPU capacity"},
			"quota: VcpuLimitExceeded: You have requested more vCPU capacity"},
		{"terminal", FailureTerminal, &msgErr{"InvalidAMIID.NotFound", "The image id '[ami-1]' does not exist"},
			"terminal: InvalidAMIID.NotFound: The image id '[ami-1]' does not exist"},
		{"unknown, no AWS code", FailureUnknown, errors.New("dial tcp:\n\ti/o timeout"), "unknown: dial tcp: i/o timeout"},
		{"wrapped API error", FailureCapacity, fmt.Errorf("spawn: %w", &msgErr{"InsufficientInstanceCapacity", "none"}),
			"capacity: InsufficientInstanceCapacity: none"},
		{"message repeats the code", FailureCapacity, &capErr{"InsufficientInstanceCapacity"}, "capacity: InsufficientInstanceCapacity"},
		{"nil error", FailureCapacity, nil, "capacity"},
	}
	for _, c := range cases {
		if got := describeAttemptError(c.kind, c.err); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}

	got := describeAttemptError(FailureUnknown, errors.New(long))
	if len(got) > MaxLastErrorBytes || !utf8.ValidString(got) || !strings.HasPrefix(got, "unknown: éé") {
		t.Errorf("truncated = %d bytes, valid=%v: %q", len(got), utf8.ValidString(got), got)
	}
	if got := describePollError(errors.New(long)); len(got) > MaxLastErrorBytes || !utf8.ValidString(got) {
		t.Errorf("poll error truncated = %d bytes", len(got))
	}
}

func TestErrorClassAndLatest(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "capacity": "capacity", "quota: VcpuLimitExceeded: x": "quota", "search: boom": "search",
	} {
		if got := ErrorClass(in); got != want {
			t.Errorf("ErrorClass(%q) = %q, want %q", in, got, want)
		}
	}
	t0 := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := &Watch{}
	if e, at := w.LatestError(); e != "" || !at.IsZero() {
		t.Errorf("empty = %q %v", e, at)
	}
	w.LastPollError, w.LastPollErrorAt = "search: a", t0
	if e, _ := w.LatestError(); e != "search: a" {
		t.Errorf("poll only = %q", e)
	}
	w.LastError, w.LastErrorAt = "capacity: b", t0.Add(time.Minute)
	if e, at := w.LatestError(); e != "capacity: b" || !at.Equal(t0.Add(time.Minute)) {
		t.Errorf("newer attempt error = %q %v", e, at)
	}
	w.LastPollErrorAt = t0.Add(2 * time.Minute)
	if e, _ := w.LatestError(); e != "search: a" {
		t.Errorf("newer poll error = %q", e)
	}
}

func TestUpdateExpr(t *testing.T) {
	if got := updateExpr([]string{"a", "b"}, []string{"c = :c"}, []string{"d :d"}); got != "REMOVE a, b SET c = :c ADD d :d" {
		t.Errorf("all clauses = %q", got)
	}
	if got := updateExpr(nil, nil, []string{"d :d"}); got != "ADD d :d" {
		t.Errorf("add only = %q", got)
	}
}

// SageMaker: a job that failed for capacity reports no error, but the attempt's
// heartbeat still says why.
func TestHeartbeat_SageMakerCapacityHasError(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := &Watch{WatchID: "w-hb-sm", Status: StatusActive, Service: ServiceSageMaker}
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	p := &Poller{store: store}
	p.recordOutcome(ctx, w, &MatchResult{WatchID: w.WatchID}, FailureCapacity,
		errors.New("SageMaker job failed for lack of capacity; resubmitting"), &PollSummary{})
	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.Attempts != 1 || got.LastError != "capacity: SageMaker job failed for lack of capacity; resubmitting" {
		t.Errorf("attempts=%d last_error=%q", got.Attempts, got.LastError)
	}
}

// Compatibility: a pre-#239 record (no heartbeat attributes) reads back as zero,
// and a pre-#239 poller's whole-record PutWatch, which knows nothing of the new
// attributes, drops them without error. The record is still a valid watch.
func TestHeartbeat_OldRecordsAndOldPollers(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()

	// A new record carrying every heartbeat field.
	now := time.Now().UTC().Truncate(time.Second)
	w := qiWatch(t, "w-hb-compat")
	w.Attempts, w.LastAttemptAt = 7, now
	w.LastError, w.LastErrorAt = "capacity: InsufficientInstanceCapacity", now
	w.LastPollError, w.LastPollErrorAt = "search: boom", now
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}

	// The old poller reads it into its old struct (unknown attributes ignored) and
	// writes the whole thing back, as RecordMatch does.
	type oldWatch struct {
		WatchID      string      `dynamodbav:"watch_id"`
		UserID       string      `dynamodbav:"user_id"`
		Status       WatchStatus `dynamodbav:"status"`
		LastPolledAt time.Time   `dynamodbav:"last_polled_at,omitempty"`
		MatchCount   int         `dynamodbav:"match_count"`
	}
	raw, err := store.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(fleetWatchesTable),
		Key:       map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: w.WatchID}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"attempts", "last_attempt_at", "last_error", "last_error_at", "last_poll_error", "last_poll_error_at"} {
		if _, ok := raw.Item[k]; !ok {
			t.Errorf("new record lacks %s", k)
		}
	}
	var old oldWatch
	if err := attributevalue.UnmarshalMap(raw.Item, &old); err != nil {
		t.Fatalf("old poller can't read a new record: %v", err)
	}
	old.Status = StatusMatched
	item, err := attributevalue.MarshalMap(old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(fleetWatchesTable), Item: item}); err != nil {
		t.Fatal(err)
	}

	// The new code reads the old-shaped record: heartbeat zero, nothing else wrong.
	got, err := store.GetWatch(ctx, w.WatchID)
	if err != nil {
		t.Fatalf("new code can't read an old-shaped record: %v", err)
	}
	if got.Status != StatusMatched || got.Attempts != 0 || got.LastError != "" || got.LastPollError != "" ||
		!got.LastAttemptAt.IsZero() || !got.LastErrorAt.IsZero() || !got.LastPollErrorAt.IsZero() {
		t.Errorf("old-shaped record read back as %+v", got)
	}

	// And a record that never had them is written without them (omitempty), so a
	// pre-#239 reader sees the shape it always did.
	fresh := qiWatch(t, "w-hb-fresh")
	if err := store.PutWatch(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	raw, _ = store.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(fleetWatchesTable),
		Key:       map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: fresh.WatchID}},
	})
	for _, k := range []string{"attempts", "last_error", "last_poll_error"} {
		if _, ok := raw.Item[k]; ok {
			t.Errorf("a watch with no heartbeat still wrote %s", k)
		}
	}
}
