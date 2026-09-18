package quotacheck

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/truffle/pkg/quotas"
)

// fakeLookup is an in-memory Lookup: it returns a canned snapshot (or an error),
// so no test touches the Service Quotas API.
type fakeLookup struct {
	info  *quotas.QuotaInfo
	err   error
	calls int
}

func (f *fakeLookup) GetQuotas(_ context.Context, _ string) (*quotas.QuotaInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.info, nil
}

func TestVCPUsForType(t *testing.T) {
	tests := []struct {
		in     string
		want   int32
		wantOK bool
	}{
		{"g6e.large", 2, true},
		{"g6e.xlarge", 4, true},
		{"g6e.2xlarge", 8, true},
		{"g6e.4xlarge", 16, true},
		{"p5.48xlarge", 192, true},
		{"t3.micro", 1, true},
		{"m8g.medium", 1, true},
		// The NxLarge = N*4 general pattern (a size the table doesn't list).
		{"x9.20xlarge", 80, true},
		// Not countable: wildcards, family-only, empty, unknown size.
		{"g6e.*", 0, false},
		{"g6e", 0, false},
		{"", 0, false},
		{"g6e.", 0, false},
		{".xlarge", 0, false},
		{"g6e.humongous", 0, false},
		{"^g6e\\.xlarge$", 0, false},
	}
	for _, tt := range tests {
		got, ok := VCPUsForType(tt.in)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("VCPUsForType(%q) = (%d, %v), want (%d, %v)", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}

// TestHeadroom_PresentZeroVsAbsent is the crux of the package: truffle's
// GetQuotas `continue`s past a per-quota error, so an ABSENT family key means the
// lookup failed while PRESENT-with-0 means the account genuinely has no quota.
// Collapsing the two would turn a missing read permission into a false "your
// quota is 0, this fleet cannot launch".
func TestHeadroom_PresentZeroVsAbsent(t *testing.T) {
	present := &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyG: 0},
		Usage:    map[quotas.QuotaFamily]int32{},
	}
	limit, used, known := Headroom(present, quotas.FamilyG, false)
	if !known || limit != 0 || used != 0 {
		t.Errorf("present-and-zero: got (%d, %d, %v), want (0, 0, true)", limit, used, known)
	}

	absent := &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyStandard: 100},
		Usage:    map[quotas.QuotaFamily]int32{},
	}
	if _, _, known := Headroom(absent, quotas.FamilyG, false); known {
		t.Error("absent key: known = true, want false (the lookup failed for this family)")
	}

	if _, _, known := Headroom(nil, quotas.FamilyG, false); known {
		t.Error("nil info: known = true, want false")
	}
}

func TestHeadroom_SpotReadsSpotMaps(t *testing.T) {
	info := &quotas.QuotaInfo{
		OnDemand:  map[quotas.QuotaFamily]int32{quotas.FamilyG: 64},
		Usage:     map[quotas.QuotaFamily]int32{quotas.FamilyG: 8},
		Spot:      map[quotas.QuotaFamily]int32{quotas.FamilyG: 16},
		SpotUsage: map[quotas.QuotaFamily]int32{quotas.FamilyG: 4},
	}
	limit, used, known := Headroom(info, quotas.FamilyG, true)
	if !known || limit != 16 || used != 4 {
		t.Errorf("spot: got (%d, %d, %v), want (16, 4, true)", limit, used, known)
	}
	limit, used, known = Headroom(info, quotas.FamilyG, false)
	if !known || limit != 64 || used != 8 {
		t.Errorf("on-demand: got (%d, %d, %v), want (64, 8, true)", limit, used, known)
	}
}

func gQuota(limit, used int32) *quotas.QuotaInfo {
	return &quotas.QuotaInfo{
		OnDemand:  map[quotas.QuotaFamily]int32{quotas.FamilyG: limit},
		Usage:     map[quotas.QuotaFamily]int32{quotas.FamilyG: used},
		Spot:      map[quotas.QuotaFamily]int32{quotas.FamilyG: limit},
		SpotUsage: map[quotas.QuotaFamily]int32{quotas.FamilyG: used},
	}
}

func TestFeasibility(t *testing.T) {
	ctx := context.Background()

	// The issue's scenario: G quota 8 vCPU, nothing in use, g6e.xlarge (4 vCPU),
	// --maintain 5 → only 2 fit.
	rep, err := Feasibility(ctx, &fakeLookup{info: gQuota(8, 0)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 5,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if !rep.Known || rep.Family != quotas.FamilyG {
		t.Fatalf("got Known=%v Family=%q, want true/G", rep.Known, rep.Family)
	}
	if rep.MaxWorkers != 2 || rep.Feasible {
		t.Errorf("MaxWorkers=%d Feasible=%v, want 2/false", rep.MaxWorkers, rep.Feasible)
	}
	if rep.ZeroQuota || rep.MixedRungs {
		t.Errorf("ZeroQuota=%v MixedRungs=%v, want false/false", rep.ZeroQuota, rep.MixedRungs)
	}
	if rep.Remediation == "" {
		t.Error("Remediation is empty for a known quota")
	}

	// Same quota, --maintain 2 → feasible.
	rep, err = Feasibility(ctx, &fakeLookup{info: gQuota(8, 0)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 2,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if !rep.Feasible || rep.MaxWorkers != 2 {
		t.Errorf("desired=2: MaxWorkers=%d Feasible=%v, want 2/true", rep.MaxWorkers, rep.Feasible)
	}

	// Comma-list (#135): the count comes off the SMALLEST literal rung, and
	// MixedRungs flags that which rung matches isn't fixed at create time.
	rep, err = Feasibility(ctx, &fakeLookup{info: gQuota(8, 0)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.4xlarge", "g6e.xlarge"}, Desired: 5,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if !rep.MixedRungs {
		t.Error("MixedRungs = false, want true for a two-rung comma list")
	}
	if rep.SmallestRung != "g6e.xlarge" || rep.VCPUsPerWorker != 4 || rep.MaxWorkers != 2 {
		t.Errorf("got rung=%q perWorker=%d max=%d, want g6e.xlarge/4/2",
			rep.SmallestRung, rep.VCPUsPerWorker, rep.MaxWorkers)
	}

	// Genuine zero quota: known, and distinguishable from a failed lookup.
	rep, err = Feasibility(ctx, &fakeLookup{info: gQuota(0, 0)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 1,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if !rep.Known || !rep.ZeroQuota || rep.Feasible {
		t.Errorf("zero quota: Known=%v ZeroQuota=%v Feasible=%v, want true/true/false",
			rep.Known, rep.ZeroQuota, rep.Feasible)
	}

	// A pure wildcard has no literal rung: MaxWorkers is -1 ("unknown"), and with
	// free headroom we must NOT claim infeasibility.
	rep, err = Feasibility(ctx, &fakeLookup{info: gQuota(8, 0)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.*"}, Desired: 5,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if rep.Family != quotas.FamilyG {
		t.Errorf("wildcard Family = %q, want G (letter-prefix mapping)", rep.Family)
	}
	if rep.MaxWorkers >= 0 {
		t.Errorf("wildcard MaxWorkers = %d, want < 0 (unknown)", rep.MaxWorkers)
	}
	if !rep.Feasible {
		t.Error("wildcard with free headroom: Feasible = false, want true (don't warn on a guess)")
	}

	// Spot reads the Spot/SpotUsage maps: 16 limit, 4 used → 12 free → 3 × xlarge.
	rep, err = Feasibility(ctx, &fakeLookup{info: &quotas.QuotaInfo{
		OnDemand:  map[quotas.QuotaFamily]int32{quotas.FamilyG: 1000},
		Usage:     map[quotas.QuotaFamily]int32{},
		Spot:      map[quotas.QuotaFamily]int32{quotas.FamilyG: 16},
		SpotUsage: map[quotas.QuotaFamily]int32{quotas.FamilyG: 4},
	}}, Request{Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Spot: true, Desired: 5})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if rep.Limit != 16 || rep.Used != 4 || rep.MaxWorkers != 3 || rep.Feasible {
		t.Errorf("spot: limit=%d used=%d max=%d feasible=%v, want 16/4/3/false",
			rep.Limit, rep.Used, rep.MaxWorkers, rep.Feasible)
	}
}

// TestFeasibility_FailsOpen: a lookup error, an absent family, and a nil Lookup
// all yield Known=false rather than a pessimistic verdict.
func TestFeasibility_FailsOpen(t *testing.T) {
	ctx := context.Background()

	rep, err := Feasibility(ctx, &fakeLookup{err: errors.New("AccessDenied")}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 5,
	})
	if err == nil {
		t.Error("lookup error: want the error surfaced for a verbose caller")
	}
	if rep.Known {
		t.Error("lookup error: Known = true, want false")
	}

	rep, err = Feasibility(ctx, &fakeLookup{info: &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyStandard: 100},
	}}, Request{Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 5})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	if rep.Known || rep.ZeroQuota {
		t.Errorf("absent family: Known=%v ZeroQuota=%v, want false/false (NOT a zero quota)", rep.Known, rep.ZeroQuota)
	}

	if rep, err := Feasibility(ctx, nil, Request{Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 5}); err != nil || rep.Known {
		t.Errorf("nil Lookup: got Known=%v err=%v, want false/nil", rep.Known, err)
	}

	if _, err := Feasibility(ctx, &fakeLookup{info: gQuota(8, 0)}, Request{Region: "us-east-1"}); err == nil {
		t.Error("no rungs: want an error")
	}
}

// TestRemediation_Truffle167Guard pins the upstream bug this package works
// around: truffle's QuotaIncreaseCommand only maps Standard/G/P/Inf/Trn
// (on-demand) and Standard/G/P (spot), and silently falls through to the Standard
// On-Demand code L-1216C47A for everything else — the wrong quota, and for spot
// the wrong lifecycle too. Mapped pairs must emit their own code; unmapped pairs
// must NOT emit L-1216C47A. When truffle#167 lands, extending
// remediableQuotaCodes is all that's needed and this test flips with it.
func TestRemediation_Truffle167Guard(t *testing.T) {
	if got := Remediation("us-east-1", quotas.FamilyG, 20, false); !strings.Contains(got, quotas.QuotaCodeG) {
		t.Errorf("(G, on-demand) remediation missing %s:\n%s", quotas.QuotaCodeG, got)
	}
	if got := Remediation("us-east-1", quotas.FamilyP, 20, true); !strings.Contains(got, quotas.QuotaCodeSpotP) {
		t.Errorf("(P, spot) remediation missing %s:\n%s", quotas.QuotaCodeSpotP, got)
	}
	for _, tt := range []struct {
		family quotas.QuotaFamily
		spot   bool
	}{
		{quotas.FamilyX, false},
		{quotas.FamilyTrn, true},
		{quotas.FamilyDL, false},
		{quotas.FamilyF, true},
	} {
		got := Remediation("us-east-1", tt.family, 20, tt.spot)
		if strings.Contains(got, quotas.QuotaCodeStandard) {
			t.Errorf("(%s, spot=%v) remediation leaks the Standard on-demand code %s (truffle#167):\n%s",
				tt.family, tt.spot, quotas.QuotaCodeStandard, got)
		}
		if !strings.Contains(got, "console") {
			t.Errorf("(%s, spot=%v) unmapped remediation should point at the console:\n%s", tt.family, tt.spot, got)
		}
	}

	// The allow-list must agree with truffle's switch: each mapped pair really does
	// emit its own quota code.
	for key, code := range remediableQuotaCodes {
		got := quotas.QuotaIncreaseCommand("us-east-1", key.family, 20, key.spot)
		if !strings.Contains(got, code) {
			t.Errorf("allow-listed (%s, spot=%v) expected code %s, truffle emitted:\n%s",
				key.family, key.spot, code, got)
		}
	}
}

func TestCapReason(t *testing.T) {
	// nil Report (the no-lookup path — the default on an un-upgraded runtime
	// policy): no panic, no invented numbers, still names the family + remediation.
	got := CapReason("us-east-1", "g6e.xlarge", false, 5, 2, nil)
	for _, want := range []string{"capped at 2/5", "G-family", "On-Demand", "us-east-1", quotas.QuotaCodeG} {
		if !strings.Contains(got, want) {
			t.Errorf("nil-report reason missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "quota is") {
		t.Errorf("nil-report reason must not state numbers:\n%s", got)
	}
	if len(got) > maxCapReasonBytes {
		t.Errorf("reason is %d bytes, want <= %d", len(got), maxCapReasonBytes)
	}

	// With a report, the numbers appear.
	rep, err := Feasibility(context.Background(), &fakeLookup{info: gQuota(8, 8)}, Request{
		Region: "us-east-1", Rungs: []string{"g6e.xlarge"}, Desired: 5,
	})
	if err != nil {
		t.Fatalf("Feasibility: %v", err)
	}
	got = CapReason("us-east-1", "g6e.xlarge", false, 5, 2, &rep)
	if !strings.Contains(got, "quota is 8 vCPU with 8 in use") {
		t.Errorf("enriched reason missing the numbers:\n%s", got)
	}
	if len(got) > maxCapReasonBytes {
		t.Errorf("reason is %d bytes, want <= %d", len(got), maxCapReasonBytes)
	}

	// An unmapped family still produces a usable sentence (no wrong quota code).
	got = CapReason("us-east-1", "x2iedn.24xlarge", false, 3, 1, nil)
	if strings.Contains(got, quotas.QuotaCodeStandard) {
		t.Errorf("unmapped family reason leaks the Standard code:\n%s", got)
	}
	if !strings.Contains(got, "capped at 1/3") {
		t.Errorf("unmapped family reason missing the level:\n%s", got)
	}

	// A very long region/type can't push the persisted string past the cap.
	long := CapReason(strings.Repeat("r", 400), strings.Repeat("g", 400)+".xlarge", true, 9, 1, nil)
	if len(long) > maxCapReasonBytes {
		t.Errorf("truncated reason is %d bytes, want <= %d", len(long), maxCapReasonBytes)
	}
}
