package quotacheck

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/truffle/pkg/quotas"
)

// regionLookup answers per region: a snapshot, an error, or (region absent) a
// snapshot with no G-family figure — the "couldn't read" shape.
type regionLookup struct {
	info map[string]*quotas.QuotaInfo
	errs map[string]error
}

func (r *regionLookup) GetQuotas(_ context.Context, region string) (*quotas.QuotaInfo, error) {
	if err := r.errs[region]; err != nil {
		return nil, err
	}
	if info, ok := r.info[region]; ok {
		return info, nil
	}
	return &quotas.QuotaInfo{}, nil
}

func gSnapshot(limit, used int32) *quotas.QuotaInfo {
	return &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyG: limit},
		Usage:    map[quotas.QuotaFamily]int32{quotas.FamilyG: used},
	}
}

// TestRequireFeasible pins the #157 semantics: refuse only on a quota READ in
// every region and provably short; anything unreadable is unknown, never zero.
func TestRequireFeasible(t *testing.T) {
	tests := []struct {
		name       string
		lookup     *regionLookup
		regions    []string
		rungs      []string
		desired    int
		want       GateVerdict
		wantMax    int
		wantRemedy bool
	}{
		{
			name:    "one region, fits",
			lookup:  &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(64, 0)}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.xlarge"}, desired: 8,
			want: GateFeasible, wantMax: 16,
		},
		{
			name:    "one region, provably short",
			lookup:  &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(16, 8)}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.xlarge"}, desired: 5,
			want: GateInfeasible, wantMax: 2, wantRemedy: true,
		},
		{
			name:    "zero quota is a definite zero",
			lookup:  &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(0, 0)}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.xlarge"}, desired: 1,
			want: GateInfeasible, wantMax: 0, wantRemedy: true,
		},
		{
			name: "regions are summed: 2 + 2 workers reach 4",
			lookup: &regionLookup{info: map[string]*quotas.QuotaInfo{
				"us-east-1": gSnapshot(8, 0), "us-west-2": gSnapshot(8, 0)}},
			regions: []string{"us-east-1", "us-west-2"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateFeasible, wantMax: 4,
		},
		{
			name: "every region read and short together",
			lookup: &regionLookup{info: map[string]*quotas.QuotaInfo{
				"us-east-1": gSnapshot(8, 0), "us-west-2": gSnapshot(8, 4)}},
			regions: []string{"us-east-1", "us-west-2"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateInfeasible, wantMax: 3, wantRemedy: true,
		},
		{
			name:    "lookup error is unknown, not zero",
			lookup:  &regionLookup{errs: map[string]error{"us-east-1": errors.New("AccessDeniedException")}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateUnknown, wantMax: 0,
		},
		{
			name:    "family not readable is unknown",
			lookup:  &regionLookup{},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateUnknown, wantMax: 0,
		},
		{
			name: "one short region plus one unreadable is unknown",
			lookup: &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(8, 0)},
				errs: map[string]error{"us-west-2": errors.New("throttled")}},
			regions: []string{"us-east-1", "us-west-2"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateUnknown, wantMax: 2,
		},
		{
			name: "the readable region alone suffices even with another unreadable",
			lookup: &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(64, 0)},
				errs: map[string]error{"us-west-2": errors.New("throttled")}},
			regions: []string{"us-east-1", "us-west-2"}, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateFeasible, wantMax: 16,
		},
		{
			name:    "wildcard with free vCPU can't be counted: unknown",
			lookup:  &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(8, 0)}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.*"}, desired: 4,
			want: GateUnknown, wantMax: 0,
		},
		{
			name:    "wildcard with no free vCPU is a definite zero",
			lookup:  &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gSnapshot(8, 8)}},
			regions: []string{"us-east-1"}, rungs: []string{"g6e.*"}, desired: 1,
			want: GateInfeasible, wantMax: 0, wantRemedy: true,
		},
		{
			name:    "no regions is unknown",
			lookup:  &regionLookup{},
			regions: nil, rungs: []string{"g6e.xlarge"}, desired: 4,
			want: GateUnknown, wantMax: -1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := RequireFeasible(context.Background(), tt.lookup, tt.regions, tt.rungs, false, tt.desired, time.Second)
			if g.Verdict != tt.want {
				t.Errorf("verdict = %s, want %s (reason %q)", g.Verdict, tt.want, g.Reason)
			}
			if g.MaxWorkers != tt.wantMax {
				t.Errorf("max workers = %d, want %d", g.MaxWorkers, tt.wantMax)
			}
			if (g.Remediation != "") != tt.wantRemedy {
				t.Errorf("remediation = %q, want present=%v", g.Remediation, tt.wantRemedy)
			}
			if g.Reason == "" {
				t.Error("every verdict must explain itself")
			}
			if g.Verdict == GateUnknown && len(tt.regions) > 0 {
				found := false
				for _, r := range g.Regions {
					if !r.Known && r.Unknown != "" {
						found = true
					}
				}
				if !found {
					t.Errorf("unknown verdict names no unknown region: %+v", g.Regions)
				}
			}
		})
	}
}

// TestRequireFeasible_NilLookup: no reader at all is unknown, never a refusal.
func TestRequireFeasible_NilLookup(t *testing.T) {
	g := RequireFeasible(context.Background(), nil, []string{"us-east-1"}, []string{"g6e.xlarge"}, false, 4, time.Second)
	if g.Verdict != GateUnknown || !strings.Contains(g.Reason, "no quota reader") {
		t.Errorf("nil lookup: %+v, want unknown with a reason", g)
	}
}

// TestRequireFeasible_Spot reads the Spot quota for a Spot fleet.
func TestRequireFeasible_Spot(t *testing.T) {
	l := &regionLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": {
		OnDemand:  map[quotas.QuotaFamily]int32{quotas.FamilyG: 64},
		Spot:      map[quotas.QuotaFamily]int32{quotas.FamilyG: 4},
		SpotUsage: map[quotas.QuotaFamily]int32{quotas.FamilyG: 0},
	}}}
	g := RequireFeasible(context.Background(), l, []string{"us-east-1"}, []string{"g6e.xlarge"}, true, 2, time.Second)
	if g.Verdict != GateInfeasible || g.MaxWorkers != 1 {
		t.Errorf("spot gate = %s/%d, want infeasible/1", g.Verdict, g.MaxWorkers)
	}
}
