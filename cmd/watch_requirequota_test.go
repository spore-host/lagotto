package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	"github.com/spore-host/lagotto/pkg/watcher"
)

func fleetWatch(n int, pattern string, regions ...string) *watcher.Watch {
	return &watcher.Watch{WatchID: "w-rq", InstanceTypePattern: pattern, DesiredCount: n, Regions: regions,
		Action: watcher.ActionSpawn}
}

// TestEnforceFleetQuota covers the three verdicts of the #157 gate as the
// command sees them: feasible is silent, unknown is a note and NOT a refusal,
// infeasible is a refusal with its own exit status.
func TestEnforceFleetQuota(t *testing.T) {
	t.Run("feasible is silent", func(t *testing.T) {
		var buf bytes.Buffer
		g, err := enforceFleetQuota(context.Background(), &fakeQuotaLookup{info: gInfo(64, 0)}, &buf, fleetWatch(4, "g6e.xlarge", "us-east-1"))
		if err != nil || g == nil || g.Verdict != quotacheck.GateFeasible {
			t.Fatalf("gate=%+v err=%v, want feasible", g, err)
		}
		if buf.Len() != 0 {
			t.Errorf("feasible gate printed:\n%s", buf.String())
		}
	})
	t.Run("unreadable quota is unknown: created, and says so", func(t *testing.T) {
		var buf bytes.Buffer
		l := &fakeQuotaLookup{err: errors.New("AccessDeniedException: servicequotas:GetServiceQuota")}
		g, err := enforceFleetQuota(context.Background(), l, &buf, fleetWatch(4, "g6e.xlarge", "us-east-1"))
		if err != nil {
			t.Fatalf("an unreadable quota refused the watch: %v", err)
		}
		if g.Verdict != quotacheck.GateUnknown {
			t.Errorf("verdict = %s, want unknown", g.Verdict)
		}
		for _, want := range []string{"could not prove", "unknown, not zero", "us-east-1", "AccessDeniedException"} {
			if !strings.Contains(buf.String(), want) {
				t.Errorf("note missing %q:\n%s", want, buf.String())
			}
		}
	})
	t.Run("no regions is unknown", func(t *testing.T) {
		var buf bytes.Buffer
		g, err := enforceFleetQuota(context.Background(), &fakeQuotaLookup{info: gInfo(0, 0)}, &buf, fleetWatch(4, "g6e.xlarge"))
		if err != nil || g.Verdict != quotacheck.GateUnknown {
			t.Fatalf("gate=%+v err=%v, want unknown and no error", g, err)
		}
		if !strings.Contains(buf.String(), "no --regions") {
			t.Errorf("note does not say why:\n%s", buf.String())
		}
	})
	t.Run("provably short is refused with exit status 3", func(t *testing.T) {
		var buf bytes.Buffer
		l := &fakeQuotaLookup{info: gInfo(16, 8)}
		g, err := enforceFleetQuota(context.Background(), l, &buf, fleetWatch(5, "g6e.xlarge", "us-east-1", "us-west-2"))
		if err == nil {
			t.Fatal("a provably short fleet was not refused")
		}
		if g == nil || g.Verdict != quotacheck.GateInfeasible || g.MaxWorkers != 4 {
			t.Errorf("gate = %+v, want infeasible with 4 workers (2 per region, summed)", g)
		}
		if l.calls != 2 {
			t.Errorf("GetQuotas calls = %d, want 2 (every region)", l.calls)
		}
		if got := exitStatus(err); got != ExitQuotaInfeasible {
			t.Errorf("exit status = %d, want %d", got, ExitQuotaInfeasible)
		}
		for _, want := range []string{"--require-quota", "--maintain 5", "us-west-2: 16 vCPU limit, 8 in use, room for 2", "request-service-quota-increase", "Nothing was stored"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
	})
	t.Run("checks every region, not just the warning's first three", func(t *testing.T) {
		l := &fakeQuotaLookup{info: gInfo(4, 0)}
		w := fleetWatch(5, "g6e.xlarge", "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1")
		g, err := enforceFleetQuota(context.Background(), l, &bytes.Buffer{}, w)
		if l.calls != 5 || g.MaxWorkers != 5 || err != nil {
			t.Errorf("calls=%d max=%d err=%v, want 5/5/nil", l.calls, g.MaxWorkers, err)
		}
	})
	t.Run("not a fleet", func(t *testing.T) {
		if _, err := enforceFleetQuota(context.Background(), &fakeQuotaLookup{}, &bytes.Buffer{}, fleetWatch(0, "g6e.xlarge", "us-east-1")); !errors.Is(err, errRequireQuotaNeedsFleet) {
			t.Errorf("err = %v, want errRequireQuotaNeedsFleet", err)
		}
	})
}

// TestExitStatus: only an error that carries its own status changes the exit
// code, also when wrapped.
func TestExitStatus(t *testing.T) {
	if got := exitStatus(errors.New("boom")); got != 1 {
		t.Errorf("plain error exit = %d, want 1", got)
	}
	wrapped := fmt.Errorf("watch: %w", &quotaGateError{gate: &quotacheck.Gate{Reason: "short"}})
	if got := exitStatus(wrapped); got != ExitQuotaInfeasible {
		t.Errorf("wrapped gate error exit = %d, want %d", got, ExitQuotaInfeasible)
	}
}

// TestRunWatch_RequireQuotaNeedsMaintain: the flag is refused on a non-fleet
// watch before any AWS call (the hermetic guard would panic on one).
func TestRunWatch_RequireQuotaNeedsMaintain(t *testing.T) {
	setRequireQuotaFlags(t, 0, "", nil)
	if err := runWatch(watchCmd, []string{"g5.xlarge"}); !errors.Is(err, errRequireQuotaNeedsFleet) {
		t.Fatalf("err = %v, want errRequireQuotaNeedsFleet", err)
	}
}

// TestRunWatch_RequireQuotaRefusesBeforeStoring runs `lagotto watch` end to end
// against the fake AWS with a fake quota reader: a provably short fleet exits 3
// and no watch (and no table) is written.
func TestRunWatch_RequireQuotaRefusesBeforeStoring(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(cfgPath, []byte("instance_type: g5.xlarge\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setRequireQuotaFlags(t, 4, cfgPath, []string{"us-east-1"})
	l := &fakeQuotaLookup{info: gInfo(8, 0)} // 2 × g5.xlarge, short of 4
	prev := newQuotaLookup
	newQuotaLookup = func(aws.Config) quotacheck.Lookup { return l }
	t.Cleanup(func() { newQuotaLookup = prev })

	f := &fakeAWS{scheduleState: "ENABLED"}
	err := runWatchWithFake(t, f)
	if exitStatus(err) != ExitQuotaInfeasible {
		t.Fatalf("err = %v (exit %d), want the --require-quota refusal", err, exitStatus(err))
	}
	if l.calls == 0 {
		t.Error("the injected quota reader was never asked")
	}
	for _, call := range []string{"dynamodb:PutItem", "dynamodb:CreateTable"} {
		if n := f.called(call); n != 0 {
			t.Errorf("%d %s after a --require-quota refusal; nothing may be stored", n, call)
		}
	}
}

// setRequireQuotaFlags sets the watch flags a --require-quota test needs and
// restores them afterwards.
func setRequireQuotaFlags(t *testing.T, maintain int, spawnConfig string, regions []string) {
	t.Helper()
	pa, pc, pm, pr, pq := watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota
	t.Cleanup(func() {
		watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota = pa, pc, pm, pr, pq
	})
	watchAction, watchSpawnConfig, watchMaintain, watchRegions, watchRequireQuota = "spawn", spawnConfig, maintain, regions, true
	if maintain == 0 {
		watchAction = "notify"
	}
}

// TestWriteUpdate_QuotaGate: an update run with --require-quota names the
// verdict in the human output and in -o json.
func TestWriteUpdate_QuotaGate(t *testing.T) {
	r := updateResult{WatchID: "w-1", DryRun: true, Gate: &quotacheck.Gate{Verdict: quotacheck.GateUnknown}}
	var buf bytes.Buffer
	if err := writeUpdate(&buf, "text", r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "Quota gate: unknown (--require-quota)") {
		t.Errorf("human output missing the gate:\n%s", buf.String())
	}
	buf.Reset()
	if err := writeUpdate(&buf, "json", r); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"quota_gate"`) || !strings.Contains(buf.String(), `"verdict": "unknown"`) {
		t.Errorf("json output missing the gate:\n%s", buf.String())
	}
}
