package watcher

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/spore-host/lagotto/pkg/testutil"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
	"github.com/spore-host/truffle/pkg/quotas"
)

// More #238 paths: quota enrichment (read OK and read failed), the lease and
// verbose paths, hold, spot, the "all blocked, wait for re-probe" cycle, and the
// store write paths that the main scenario tests don't reach.

// fakeQuotaLookup is a quotacheck.Lookup: per-region snapshot, or an error.
type fakeQuotaLookup struct {
	info map[string]*quotas.QuotaInfo
	err  error
}

func (f *fakeQuotaLookup) GetQuotas(_ context.Context, region string) (*quotas.QuotaInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.info[region], nil
}

// The live run on #246 showed both branches: us-east-2's line carried quota
// numbers, eu-north-1's did not (its Service Quotas read came back empty). Both
// must produce a usable entry and message.
func TestQuotaInfeasible_QuotaNumbersKnownAndUnknown(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 0}, // no vCPU from search: falls back to the size table
		qiCandidate{"g7.24xlarge", "us-east-2", 2.0, 96},
	), refuse("g7.24xlarge@eu-north-1", "g7.24xlarge@us-east-2"))
	h.poller.quotas = &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{
		"us-east-2": {OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyG: 64}, Usage: map[quotas.QuotaFamily]int32{quotas.FamilyG: 8}},
		// eu-north-1: no snapshot — the read produced nothing usable.
	}}
	w := qiWatch(t, "w-numbers")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if got.Status != StatusFailed || len(got.QuotaInfeasible) != 2 {
		t.Fatalf("status=%q entries=%+v", got.Status, got.QuotaInfeasible)
	}
	byRegion := map[string]QuotaInfeasibility{}
	for _, q := range got.QuotaInfeasible {
		byRegion[q.Region] = q
	}
	known, unknown := byRegion["us-east-2"], byRegion["eu-north-1"]
	if !known.QuotaKnown || known.QuotaLimit != 64 || known.QuotaUsed != 8 || known.Family != quotas.FamilyG {
		t.Errorf("us-east-2 entry = %+v, want limit 64 / used 8 / family G", known)
	}
	if unknown.QuotaKnown || unknown.QuotaLimit != 0 {
		t.Errorf("eu-north-1 entry = %+v, want no quota numbers", unknown)
	}
	if unknown.VCPUs != 96 {
		t.Errorf("eu-north-1 VCPUs = %d, want 96 from the size table", unknown.VCPUs)
	}
	r := h.notified[0]
	if !strings.Contains(r, "G-family On-Demand quota 64 vCPU, 8 in use, 56 available") {
		t.Errorf("reason lacks us-east-2's numbers:\n%s", r)
	}
	if !strings.Contains(r, "g7.24xlarge in eu-north-1: refused by EC2 quota (VcpuLimitExceeded); needs 96 vCPU\n") {
		t.Errorf("eu-north-1's line should carry no quota numbers:\n%s", r)
	}
	// us-east-2 is sized off usage + need (8+96 = 104 > 64).
	if !strings.Contains(r, "--desired-value 104") {
		t.Errorf("remediation not sized to usage + need:\n%s", r)
	}
}

// A lookup that errors (no servicequotas permission, throttled) still records
// the refusal, without numbers.
func TestQuotaInfeasible_QuotaLookupError(t *testing.T) {
	h := newQIHarness(t, qiResults(qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96}), refuse("g7.24xlarge@eu-north-1"))
	h.poller.quotas = &fakeQuotaLookup{err: errors.New("AccessDeniedException: servicequotas:GetServiceQuota")}
	h.poller.verbose = true
	w := qiWatch(t, "w-lookup-err")
	w.Regions = []string{"eu-north-1"}
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if got.Status != StatusFailed || len(got.QuotaInfeasible) != 1 || got.QuotaInfeasible[0].QuotaKnown {
		t.Fatalf("status=%q entries=%+v, want failed with one numberless entry", got.Status, got.QuotaInfeasible)
	}
}

// With leasing on, the poller claims and releases the lease around the
// fall-through, and a verbose poller logs each step.
func TestQuotaInfeasible_WithLeaseAndVerbose(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96},
	), refuse("g7.24xlarge@eu-north-1"))
	h.poller.leaseOwner = "poller-a"
	h.poller.verbose = true
	w := qiWatch(t, "w-lease")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if got.Status != StatusMatched || got.LeaseOwner != "" {
		t.Fatalf("status=%q lease=%q, want matched and the lease released", got.Status, got.LeaseOwner)
	}
}

// A lease held by another poller skips the watch: nothing is attempted.
func TestQuotaInfeasible_LeaseHeldSkips(t *testing.T) {
	h := newQIHarness(t, qiResults(qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96}), refuse())
	h.poller.leaseOwner = "poller-a"
	h.poller.verbose = true
	w := qiWatch(t, "w-held")
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ClaimLease(context.Background(), w.WatchID, "poller-b", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, summary := h.cycle(t, w.WatchID)
	if len(h.attempts) != 0 || summary.Retrying != 1 || got.Status != StatusActive {
		t.Fatalf("attempts=%v Retrying=%d status=%q, want a skipped cycle", h.attempts, summary.Retrying, got.Status)
	}
}

// A lease claim that errors for another reason (here: the watch record is
// missing, so the conditional write fails differently) skips without acting.
func TestQuotaInfeasible_LeaseClaimError(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := NewStore(env.AWSConfig, "no-such-table", "no-such-history")
	h := &qiHarness{store: store}
	p := &Poller{store: store, leaseOwner: "poller-a",
		truffle: &fakeSearcher{results: qiResults(qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})}}
	w := qiWatch(t, "w-claim-err")
	summary := &PollSummary{}
	if err := p.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, false, []*Watch{w}, summary); err != nil {
		t.Fatal(err)
	}
	if summary.Launched+summary.Failed+summary.Retrying != 0 || len(h.attempts) != 0 {
		t.Errorf("summary=%+v, want nothing done when the lease can't be claimed", summary)
	}
}

// Every matched candidate is still blocked by a live entry: the poller waits for
// the re-probe without attempting anything and without failing the watch.
func TestQuotaInfeasible_AllBlockedWaitsForReprobe(t *testing.T) {
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-east-2", 2.0, 96},
	), refuse())
	h.poller.verbose = true
	w := qiWatch(t, "w-blocked")
	now := time.Now().UTC()
	w.QuotaInfeasible = []QuotaInfeasibility{
		{InstanceType: "g7.24xlarge", Region: "eu-north-1", At: now},
		{InstanceType: "g7.24xlarge", Region: "us-east-2", At: now},
	}
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, summary := h.cycle(t, w.WatchID)
	if len(h.attempts) != 0 || got.Status != StatusActive || summary.Retrying != 1 {
		t.Fatalf("attempts=%v status=%q Retrying=%d, want no attempt and still active", h.attempts, got.Status, summary.Retrying)
	}
	if got.LastPolledAt.IsZero() {
		t.Error("last_polled_at not touched on a waiting cycle")
	}
}

// Nothing matches (every offer is above --max-price): the watch is just polled.
func TestQuotaInfeasible_NoCandidatesJustPolls(t *testing.T) {
	h := newQIHarness(t, qiResults(qiCandidate{"g7.24xlarge", "us-west-2", 50, 96}), refuse())
	w := qiWatch(t, "w-pricey")
	w.MaxPrice = 1
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, _ := h.cycle(t, w.WatchID)
	if len(h.attempts) != 0 || got.Status != StatusActive || got.LastPolledAt.IsZero() {
		t.Fatalf("attempts=%v status=%q polled=%v", h.attempts, got.Status, got.LastPolledAt)
	}
}

// spotSearcher returns canned on-demand results and spot prices.
type spotSearcher struct {
	results []truffleaws.InstanceTypeResult
	spot    []truffleaws.SpotPriceResult
}

func (f *spotSearcher) SearchInstanceTypes(context.Context, []string, *regexp.Regexp, truffleaws.FilterOptions) ([]truffleaws.InstanceTypeResult, error) {
	return f.results, nil
}
func (f *spotSearcher) GetSpotPricing(context.Context, []truffleaws.InstanceTypeResult, truffleaws.SpotOptions) ([]truffleaws.SpotPriceResult, error) {
	return f.spot, nil
}

// Spot, end to end: the only candidate under --max-price is quota-refused, but a
// candidate above the cap was never tried, so the watch stays active.
func TestQuotaInfeasible_SpotOverPriceKeepsWatchAlive(t *testing.T) {
	h := newQIHarness(t, nil, refuse("g7.24xlarge@eu-north-1"))
	h.poller.truffle = &spotSearcher{
		results: qiResults(
			qiCandidate{"g7.24xlarge", "eu-north-1", 9, 96},
			qiCandidate{"g7.24xlarge", "us-west-2", 9, 96},
		),
		spot: []truffleaws.SpotPriceResult{
			{InstanceType: "g7.24xlarge", Region: "eu-north-1", AvailabilityZone: "eu-north-1a", SpotPrice: 1.0},
			{InstanceType: "g7.24xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2a", SpotPrice: 3.0},
		},
	}
	w := qiWatch(t, "w-spot")
	w.Spot, w.MaxPrice = true, 1.5
	if err := h.store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, summary := h.cycle(t, w.WatchID)
	if strings.Join(h.attempts, ",") != "g7.24xlarge@eu-north-1" {
		t.Fatalf("attempts = %v", h.attempts)
	}
	if got.Status != StatusActive || summary.Retrying != 1 || len(got.QuotaInfeasible) != 1 || !got.QuotaInfeasible[0].Spot {
		t.Fatalf("status=%q Retrying=%d entries=%+v, want active with one spot entry", got.Status, summary.Retrying, got.QuotaInfeasible)
	}
	if !strings.Contains(got.QuotaInfeasible[0].Describe(), "(spot)") {
		t.Errorf("Describe = %q", got.QuotaInfeasible[0].Describe())
	}
}

// quotaRefusingEC2 refuses every capacity reservation on quota.
type quotaRefusingEC2 struct{ calls int }

func (f *quotaRefusingEC2) CreateCapacityReservation(context.Context, *ec2.CreateCapacityReservationInput, ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error) {
	f.calls++
	return nil, &capErr{"InstanceLimitExceeded"}
}

// Hold watches get the same treatment: a quota-refused reservation in one region
// falls through to the next; both refused → failed.
func TestQuotaInfeasible_HoldAllRefused(t *testing.T) {
	store := fleetStore(t)
	f := &quotaRefusingEC2{}
	p := &Poller{store: store,
		truffle: &fakeSearcher{results: qiResults(
			qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
			qiCandidate{"g7.24xlarge", "us-east-2", 2.0, 96},
		)},
		holder: &Holder{
			createClient:  func(string) holderCreateAPI { return f },
			launchableAZs: func(context.Context, string) map[string]bool { return nil },
		},
	}
	var notified int
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { notified++; return errors.New("webhook down") }
	w := qiWatch(t, "w-hold")
	w.Action = ActionHold
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := p.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, false, []*Watch{w}, &PollSummary{}); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetWatch(context.Background(), w.WatchID)
	if got.Status != StatusFailed || f.calls != 2 || notified != 1 {
		t.Fatalf("status=%q holds=%d notified=%d, want failed after 2 refused holds, notified once", got.Status, f.calls, notified)
	}
	if got.LastMatch == nil || got.LastMatch.ActionTaken != ActionQuotaInfeasible {
		t.Errorf("LastMatch = %+v", got.LastMatch)
	}
}

// A hold that fails on capacity keeps today's behaviour (retry next cycle).
type capacityRefusingEC2 struct{}

func (capacityRefusingEC2) CreateCapacityReservation(context.Context, *ec2.CreateCapacityReservationInput, ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error) {
	return nil, &capErr{"InsufficientInstanceCapacity"}
}

func TestQuotaInfeasible_HoldCapacityUnchanged(t *testing.T) {
	store := fleetStore(t)
	p := &Poller{store: store,
		truffle: &fakeSearcher{results: qiResults(qiCandidate{"g7.24xlarge", "us-west-2", 2.0, 96})},
		holder: &Holder{
			createClient:  func(string) holderCreateAPI { return capacityRefusingEC2{} },
			launchableAZs: func(context.Context, string) map[string]bool { return nil },
		},
	}
	w := qiWatch(t, "w-hold-ice")
	w.Action = ActionHold
	if err := store.PutWatch(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	summary := &PollSummary{}
	if err := p.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, false, []*Watch{w}, summary); err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetWatch(context.Background(), w.WatchID)
	if got.Status != StatusActive || summary.Retrying != 1 || len(got.QuotaInfeasible) != 0 {
		t.Fatalf("status=%q Retrying=%d entries=%+v", got.Status, summary.Retrying, got.QuotaInfeasible)
	}
}

// No spawner / holder wired: a spawn or hold watch only notifies, as before.
func TestRunAction_NoLauncherNotifies(t *testing.T) {
	p := &Poller{}
	for _, a := range []ActionMode{ActionSpawn, ActionHold, ActionNotify} {
		m := &MatchResult{}
		if k, err := p.runAction(context.Background(), &Watch{Action: a}, m); k != FailureNone || err != nil || m.ActionTaken != "notified" {
			t.Errorf("%s: kind=%v err=%v action=%q, want notified", a, k, err, m.ActionTaken)
		}
	}
}

// Store write paths: SetQuotaInfeasible removes the attribute for an empty list,
// and both new writers report a failed write.
func TestStore_QuotaInfeasibleWrites(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := qiWatch(t, "w-store")
	w.QuotaInfeasible = []QuotaInfeasibility{{InstanceType: "g7.24xlarge", Region: "eu-north-1", At: time.Now().UTC()}}
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := store.SetQuotaInfeasible(ctx, w.WatchID, nil); err != nil {
		t.Fatalf("SetQuotaInfeasible(nil): %v", err)
	}
	got, _ := store.GetWatch(ctx, w.WatchID)
	if len(got.QuotaInfeasible) != 0 || got.LastPolledAt.IsZero() {
		t.Errorf("after clearing: entries=%+v polled=%v", got.QuotaInfeasible, got.LastPolledAt)
	}

	env := testutil.SubstrateServer(t)
	bad := NewStore(env.AWSConfig, "missing-watches", "missing-history")
	if err := bad.SetQuotaInfeasible(ctx, "w-x", w.QuotaInfeasible); err == nil {
		t.Error("SetQuotaInfeasible on a missing table: want an error")
	}
	if err := bad.SetQuotaInfeasible(ctx, "w-x", nil); err == nil {
		t.Error("SetQuotaInfeasible(nil) on a missing table: want an error")
	}
	if err := bad.RecordHistory(ctx, &MatchResult{WatchID: "w-x", MatchedAt: time.Now()}); err == nil {
		t.Error("RecordHistory on a missing table: want an error")
	}
}

// Store failures during a fall-through are logged, not fatal: the poller still
// tries the next candidate and still fails the watch when nothing is left.
func TestQuotaInfeasible_StoreFailuresAreLogged(t *testing.T) {
	env := testutil.SubstrateServer(t)
	store := NewStore(env.AWSConfig, "missing-watches", "missing-history")
	h := newQIHarness(t, qiResults(
		qiCandidate{"g7.24xlarge", "eu-north-1", 1.0, 96},
		qiCandidate{"g7.24xlarge", "us-east-2", 2.0, 96},
	), refuse("g7.24xlarge@eu-north-1", "g7.24xlarge@us-east-2"))
	h.poller.store = store
	w := qiWatch(t, "w-nostore")
	summary := &PollSummary{}
	if err := h.poller.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, false, []*Watch{w}, summary); err != nil {
		t.Fatal(err)
	}
	if len(h.attempts) != 2 {
		t.Fatalf("attempts = %v, want both tried despite store errors", h.attempts)
	}
	// The status write failed, so the watch is still active in the store: no
	// notification (it would repeat next cycle) and not counted as failed.
	if len(h.notified) != 0 || summary.Failed != 0 || len(summary.Matches) != 1 {
		t.Errorf("notified=%d Failed=%d matches=%d", len(h.notified), summary.Failed, len(summary.Matches))
	}
}

func TestQuotaInfeasible_SmallHelpers(t *testing.T) {
	if QuotaInfeasibleReason(nil) != "" {
		t.Error("empty reason for no entries")
	}
	if errorCode(errors.New("plain")) != "" {
		t.Error("errorCode of a non-AWS error")
	}
	if s := truncateBytes("ab€cd", 4); s != "ab" {
		t.Errorf("truncateBytes split a rune: %q", s)
	}
	if s := truncateBytes("abc", 10); s != "abc" {
		t.Errorf("truncateBytes = %q", s)
	}
	if allQuotaInfeasible(nil, nil) {
		t.Error("an empty universe is never all-infeasible")
	}
	// Over-used quota reports 0 available, not a negative number; spot wording.
	d := QuotaInfeasibility{InstanceType: "g7.xlarge", Region: "r", Spot: true, QuotaKnown: true, QuotaLimit: 4, QuotaUsed: 8, Family: "G"}.Describe()
	if !strings.Contains(d, "Spot quota 4 vCPU, 8 in use, 0 available") {
		t.Errorf("Describe = %q", d)
	}
	// Sorting and the live view: by type, then region; expired entries dropped.
	now := time.Now()
	w := &Watch{QuotaInfeasible: []QuotaInfeasibility{
		{InstanceType: "g7.xlarge", Region: "b", At: now},
		{InstanceType: "g7.12xlarge", Region: "a", At: now},
		{InstanceType: "g7.xlarge", Region: "a", At: now},
		{InstanceType: "g7.xlarge", Region: "z", At: now.Add(-time.Hour)},
	}}
	live := w.LiveQuotaInfeasible(now)
	var keys []string
	for _, q := range live {
		keys = append(keys, q.InstanceType+"@"+q.Region)
	}
	if strings.Join(keys, ",") != "g7.12xlarge@a,g7.xlarge@a,g7.xlarge@b" {
		t.Errorf("live = %v", keys)
	}
	// A bucket with no vCPU figure gets no remediation line.
	r := QuotaInfeasibleReason([]QuotaInfeasibility{{InstanceType: "g7.weird", Region: "r"}})
	if strings.Contains(r, "service-quotas") || strings.Contains(r, "console") {
		t.Errorf("remediation without a vCPU figure: %q", r)
	}
	// A known quota whose usage+need is under the limit is sized to limit+1.
	r = QuotaInfeasibleReason([]QuotaInfeasibility{{InstanceType: "g7.xlarge", Region: "r", VCPUs: 4, QuotaKnown: true, QuotaLimit: 64, Family: "G"}})
	if !strings.Contains(r, "--desired-value 65") {
		t.Errorf("remediation not sized to limit+1: %q", r)
	}
}
