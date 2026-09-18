package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/truffle/pkg/quotas"
)

func TestValidateFleetFlags(t *testing.T) {
	tests := []struct {
		name     string
		action   watcher.ActionMode
		maintain int
		until    string
		wantErr  bool
	}{
		{"single-shot (no fleet flags)", watcher.ActionNotify, 0, "", false},
		{"maintain requires spawn", watcher.ActionNotify, 4, "", true},
		{"maintain with spawn ok", watcher.ActionSpawn, 4, "", false},
		{"negative maintain", watcher.ActionSpawn, -1, "", true},
		{"until without maintain", watcher.ActionSpawn, 0, "http-200: https://x/done", true},
		{"maintain + valid s3 until", watcher.ActionSpawn, 4, "s3-empty: s3://b/m minus s3://b/d", false},
		{"maintain + valid http until", watcher.ActionSpawn, 4, "http-200: https://x/done", false},
		{"maintain + valid shell until", watcher.ActionSpawn, 2, "shell: test -f /tmp/done", false},
		{"maintain + bad until spec", watcher.ActionSpawn, 4, "nonsense-no-colon", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFleetFlags(tt.action, tt.maintain, tt.until)
			if tt.wantErr != (err != nil) {
				t.Errorf("validateFleetFlags(%q,%d,%q) err=%v, wantErr=%v", tt.action, tt.maintain, tt.until, err, tt.wantErr)
			}
		})
	}
}

// fakeQuotaLookup is an in-memory quotacheck.Lookup so the create-time
// feasibility check is exercised with ZERO AWS calls.
type fakeQuotaLookup struct {
	info  *quotas.QuotaInfo
	err   error
	calls int
}

func (f *fakeQuotaLookup) GetQuotas(_ context.Context, _ string) (*quotas.QuotaInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.info, nil
}

func gInfo(limit, used int32) *quotas.QuotaInfo {
	return &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyG: limit},
		Usage:    map[quotas.QuotaFamily]int32{quotas.FamilyG: used},
	}
}

// TestWarnFleetQuotaFeasibility covers the create-time warning (#153): loud for a
// zero quota, a warning plus "the watch will still run" for an infeasible goal,
// and — critically — SILENT when the quota simply can't be read, so a missing
// servicequotas permission never masquerades as "your quota is zero".
func TestWarnFleetQuotaFeasibility(t *testing.T) {
	fleet := func(maintain int, pattern string) *watcher.Watch {
		return &watcher.Watch{
			WatchID: "w-q", InstanceTypePattern: pattern,
			Regions: []string{"us-east-1"}, DesiredCount: maintain,
			Action: watcher.ActionSpawn,
		}
	}

	tests := []struct {
		name      string
		watch     *watcher.Watch
		lookup    *fakeQuotaLookup
		wantEmpty bool
		wantCalls int
		contains  []string
		omits     []string
	}{
		{
			name:      "infeasible fleet warns with the rung and the CLI hint",
			watch:     fleet(5, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: gInfo(8, 0)},
			wantCalls: 1,
			contains: []string{
				"--maintain 5", "g6e.xlarge", "4 vCPU each",
				"aws service-quotas", quotas.QuotaCodeG,
				"The watch will still run",
			},
		},
		{
			name:      "zero quota is the loudest case",
			watch:     fleet(5, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: gInfo(0, 0)},
			wantCalls: 1,
			contains:  []string{"cannot launch a single worker", "is 0"},
		},
		{
			name:      "mixed rungs say 'at best' and name the rung counted",
			watch:     fleet(5, "g6e.4xlarge,g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: gInfo(8, 0)},
			wantCalls: 1,
			contains:  []string{"at best 2 × g6e.xlarge"},
		},
		{
			name:      "pure wildcard reports vCPU headroom, not a worker count",
			watch:     fleet(5, "g6e.*"),
			lookup:    &fakeQuotaLookup{info: gInfo(8, 8)},
			wantCalls: 1,
			contains:  []string{"0 vCPU free", "no literal size"},
		},
		{
			name:      "feasible fleet says nothing",
			watch:     fleet(2, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: gInfo(64, 0)},
			wantCalls: 1,
			wantEmpty: true,
		},
		{
			name:      "unknown quota (family absent) is silent, not scary",
			watch:     fleet(5, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: &quotas.QuotaInfo{OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyStandard: 100}}},
			wantCalls: 1,
			wantEmpty: true,
		},
		{
			name:      "lookup error is silent and non-fatal",
			watch:     fleet(5, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{err: errors.New("AccessDenied: servicequotas:GetServiceQuota")},
			wantCalls: 1,
			wantEmpty: true,
		},
		{
			name:      "not a fleet watch → no lookup at all",
			watch:     fleet(0, "g6e.xlarge"),
			lookup:    &fakeQuotaLookup{info: gInfo(0, 0)},
			wantCalls: 0,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			rep := warnFleetQuotaFeasibility(context.Background(), tt.lookup, &buf, tt.watch, "us-east-1")
			if tt.lookup.calls != tt.wantCalls {
				t.Errorf("GetQuotas calls = %d, want %d", tt.lookup.calls, tt.wantCalls)
			}
			got := buf.String()
			if tt.wantEmpty {
				if got != "" {
					t.Errorf("want empty output, got:\n%s", got)
				}
				return
			}
			if rep == nil {
				t.Fatal("report is nil for a known quota")
			}
			for _, want := range tt.contains {
				if !strings.Contains(got, want) {
					t.Errorf("output missing %q:\n%s", want, got)
				}
			}
			for _, omit := range tt.omits {
				if strings.Contains(got, omit) {
					t.Errorf("output unexpectedly contains %q:\n%s", omit, got)
				}
			}
		})
	}
}

// TestWarnFleetQuotaFeasibility_CapsRegions: an empty --regions means ALL enabled
// regions at poll time; the create-time check must never sweep them (each
// GetQuotas is ~18 API calls).
func TestWarnFleetQuotaFeasibility_CapsRegions(t *testing.T) {
	w := &watcher.Watch{
		WatchID: "w-many", InstanceTypePattern: "g6e.xlarge", DesiredCount: 5,
		Regions: []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1"},
	}
	l := &fakeQuotaLookup{info: gInfo(8, 0)}
	var buf bytes.Buffer
	warnFleetQuotaFeasibility(context.Background(), l, &buf, w, "us-east-1")
	if l.calls != maxQuotaCheckRegions {
		t.Errorf("GetQuotas calls = %d, want %d (capped)", l.calls, maxQuotaCheckRegions)
	}
	if !strings.Contains(buf.String(), "first 3 of 5") {
		t.Errorf("output should note the truncation:\n%s", buf.String())
	}

	// A nil lookup (or no region to check) is a no-op, never a panic.
	if rep := warnFleetQuotaFeasibility(context.Background(), nil, &buf, w, ""); rep != nil {
		t.Errorf("nil lookup: report = %+v, want nil", rep)
	}
	empty := &watcher.Watch{WatchID: "w-noreg", InstanceTypePattern: "g6e.xlarge", DesiredCount: 2}
	l2 := &fakeQuotaLookup{info: gInfo(8, 0)}
	if rep := warnFleetQuotaFeasibility(context.Background(), l2, &buf, empty, ""); rep != nil || l2.calls != 0 {
		t.Errorf("no region: report=%+v calls=%d, want nil/0", rep, l2.calls)
	}
}
