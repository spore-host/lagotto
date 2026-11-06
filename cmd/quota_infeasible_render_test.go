package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// #238: `lagotto status` shows which candidates a quota refused, and when each
// is re-probed, for an active single-shot watch.
func TestStatus_QuotaInfeasibleActive(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := &watcher.Watch{
		WatchID: "w-q", Status: watcher.StatusActive,
		QuotaInfeasible: []watcher.QuotaInfeasibility{
			{InstanceType: "g7.24xlarge", Region: "eu-north-1", Code: "VcpuLimitExceeded", VCPUs: 96,
				QuotaKnown: true, QuotaLimit: 64, Family: "G", At: now.Add(-5 * time.Minute)},
			// Past the re-probe interval: no longer blocking, so not listed.
			{InstanceType: "g7.24xlarge", Region: "us-east-2", At: now.Add(-2 * watcher.QuotaCapReprobeInterval)},
		},
	}
	var out bytes.Buffer
	writeQuotaInfeasible(&out, w, now)
	s := out.String()
	for _, want := range []string{
		"1 candidate(s) skipped as quota-infeasible",
		"g7.24xlarge in eu-north-1: refused by EC2 quota (VcpuLimitExceeded); needs 96 vCPU, G-family On-Demand quota 64 vCPU, 0 in use, 64 available",
		"re-probe after 2026-11-06T12:25:00Z",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("status missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "us-east-2") {
		t.Errorf("an expired entry is listed as blocking:\n%s", s)
	}
}

// A watch that failed because every candidate was refused prints the message.
func TestStatus_QuotaInfeasibleFailed(t *testing.T) {
	entries := []watcher.QuotaInfeasibility{
		{InstanceType: "g7.24xlarge", Region: "eu-north-1", VCPUs: 96},
		{InstanceType: "g7.24xlarge", Region: "us-east-2", VCPUs: 96},
	}
	w := &watcher.Watch{WatchID: "w-f", Status: watcher.StatusFailed, QuotaInfeasible: entries,
		QuotaCapReason: watcher.QuotaInfeasibleReason(entries)}
	var out bytes.Buffer
	writeQuotaInfeasible(&out, w, time.Now())
	s := out.String()
	for _, want := range []string{"every candidate was refused", "All 2 candidates", "g7.24xlarge in eu-north-1", "g7.24xlarge in us-east-2"} {
		if !strings.Contains(s, want) {
			t.Errorf("status missing %q:\n%s", want, s)
		}
	}

	// Fleets and watches with no entries print nothing here.
	out.Reset()
	writeQuotaInfeasible(&out, &watcher.Watch{Status: watcher.StatusActive}, time.Now())
	writeQuotaInfeasible(&out, &watcher.Watch{Status: watcher.StatusActive, DesiredCount: 3, QuotaInfeasible: entries}, time.Now())
	if out.Len() != 0 {
		t.Errorf("unexpected quota section: %q", out.String())
	}
}

// History shows a quota refusal as quota_infeasible with its detail, and gives
// it no wait-to-acquire (nothing was acquired).
func TestRenderHistory_QuotaInfeasibleRow(t *testing.T) {
	wait := 60.0
	matches := []watcher.MatchResult{{
		WatchID: "w-q", InstanceType: "g7.24xlarge", Region: "eu-north-1", AvailabilityZone: "eu-north-1a",
		Price: 1, ActionTaken: watcher.ActionQuotaInfeasible, WaitToAcquireSeconds: &wait,
		QuotaDetail: "g7.24xlarge in eu-north-1: refused by EC2 quota (VcpuLimitExceeded); needs 96 vCPU",
		MatchedAt:   time.Date(2026, 11, 4, 6, 12, 24, 0, time.UTC),
	}}
	var buf bytes.Buffer
	if err := renderHistory(&buf, matches, nil); err != nil {
		t.Fatal(err)
	}
	s := buf.String()
	if !strings.Contains(s, "quota_infeasible ") || !strings.Contains(s, "needs 96 vCPU") {
		t.Errorf("history lacks the quota row/detail:\n%s", s)
	}
	if strings.Contains(s, "1m0s") {
		t.Errorf("a quota refusal shows a wait-to-acquire:\n%s", s)
	}
}

// An active watch whose entries have all passed the re-probe interval says so;
// a cancelled or expired watch prints no quota section.
func TestStatus_QuotaInfeasibleExpiredAndInactive(t *testing.T) {
	now := time.Now().UTC()
	old := []watcher.QuotaInfeasibility{{InstanceType: "g7.24xlarge", Region: "eu-north-1", At: now.Add(-2 * watcher.QuotaCapReprobeInterval)}}
	var out bytes.Buffer
	writeQuotaInfeasible(&out, &watcher.Watch{Status: watcher.StatusActive, QuotaInfeasible: old}, now)
	if !strings.Contains(out.String(), "no candidate is currently blocked") {
		t.Errorf("status = %q", out.String())
	}
	out.Reset()
	writeQuotaInfeasible(&out, &watcher.Watch{Status: watcher.StatusCancelled, QuotaInfeasible: old}, now)
	if out.Len() != 0 {
		t.Errorf("cancelled watch printed %q", out.String())
	}
}
