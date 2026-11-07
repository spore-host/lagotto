package watcher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
	"github.com/spore-host/truffle/pkg/quotas"
)

// #158: a capped fleet reads the current family quota and skips the doomed
// #153 re-probe when the reading shows the ceiling has not moved.

func gQuota(limit, used int32) *quotas.QuotaInfo {
	return &quotas.QuotaInfo{
		OnDemand: map[quotas.QuotaFamily]int32{quotas.FamilyG: limit},
		Usage:    map[quotas.QuotaFamily]int32{quotas.FamilyG: used},
	}
}

func cappedWatch(regions ...string) *Watch {
	return &Watch{
		WatchID: "w-probe", Status: StatusActive, DesiredCount: 5, InstanceTypePattern: "g6e.xlarge",
		Regions: regions, QuotaCappedCount: 2, QuotaCapLimit: 8, QuotaCapRegion: "us-east-1",
	}
}

func TestProbeQuotaCap(t *testing.T) {
	full := gQuota(8, 8)
	tests := []struct {
		name    string
		lookup  *fakeQuotaLookup
		watch   *Watch
		nilRead bool
		want    capProbe
	}{
		{name: "no reader", nilRead: true, watch: cappedWatch("us-east-1"), want: capProbeUnknown},
		{name: "read error", lookup: &fakeQuotaLookup{err: errors.New("AccessDeniedException")},
			watch: cappedWatch("us-east-1"), want: capProbeUnknown},
		{name: "family not reported", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{}},
			watch: cappedWatch("us-east-1"), want: capProbeUnknown},
		{name: "unchanged and full", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": full}},
			watch: cappedWatch("us-east-1"), want: capProbeNoRoom},
		{name: "limit raised in the cap region", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gQuota(16, 8)}},
			watch: cappedWatch("us-east-1"), want: capProbeRaised},
		{name: "usage released: room at the same limit", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 4)}},
			watch: cappedWatch("us-east-1"), want: capProbeRoom},
		{name: "room in another region", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": full, "us-west-2": gQuota(64, 0)}},
			watch: cappedWatch("us-east-1", "us-west-2"), want: capProbeRoom},
		{name: "one region unreadable is unknown, never full", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": full}},
			watch: cappedWatch("us-east-1", "us-west-2"), want: capProbeUnknown},
		{name: "too many regions to read each cycle", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{}},
			watch: cappedWatch("us-east-1", "us-east-2", "us-west-1", "us-west-2"), want: capProbeUnknown},
		{name: "no recorded limit: a higher limit is not 'raised'", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gQuota(16, 16)}},
			watch: func() *Watch { w := cappedWatch("us-east-1"); w.QuotaCapLimit = 0; return w }(), want: capProbeNoRoom},
		{name: "a higher limit in another region is not 'raised'", lookup: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": full, "us-west-2": gQuota(16, 16)}},
			watch: cappedWatch("us-east-1", "us-west-2"), want: capProbeNoRoom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Poller{verbose: true}
			if !tt.nilRead {
				p.quotas = tt.lookup
			}
			if got := p.probeQuotaCap(context.Background(), tt.watch); got != tt.want {
				t.Errorf("probe = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestCapProbeString(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range []capProbe{capProbeUnknown, capProbeRoom, capProbeNoRoom, capProbeRaised} {
		s := c.String()
		if s == "" || seen[s] {
			t.Errorf("capProbe(%d).String() = %q: empty or duplicate", c, s)
		}
		seen[s] = true
	}
}

// TestQuotaCapped_ProbeDecidesTheWait drives pollFleetWatch on a capped fleet
// (2/5 running, ceiling 2) and counts the RunInstances attempts it makes.
func TestQuotaCapped_ProbeDecidesTheWait(t *testing.T) {
	tests := []struct {
		name      string
		info      map[string]*quotas.QuotaInfo
		err       error
		cappedAgo time.Duration
		wantCalls int
	}{
		{"full: the 30m re-probe is skipped", map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 8)}, nil,
			QuotaCapReprobeInterval + time.Minute, 0},
		{"full: the slow backstop still probes", map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 8)}, nil,
			QuotaCapBackstopInterval + time.Minute, 1},
		{"raised: tops up at once, inside the interval", map[string]*quotas.QuotaInfo{"us-east-1": gQuota(16, 8)}, nil,
			time.Minute, 1},
		{"room: keeps the 30m interval (inside)", map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 4)}, nil,
			time.Minute, 0},
		{"room: keeps the 30m interval (past)", map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 4)}, nil,
			QuotaCapReprobeInterval + time.Minute, 1},
		{"unreadable: falls back to the 30m re-probe", nil, errors.New("AccessDeniedException"),
			QuotaCapReprobeInterval + time.Minute, 1},
		{"unreadable: still suppressed inside the interval", nil, errors.New("AccessDeniedException"),
			time.Minute, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := fleetStore(t)
			ctx := context.Background()
			w := quotaFleetWatch(t, "w-probe-wait")
			w.QuotaCappedCount = 2
			w.QuotaCappedAt = time.Now().UTC().Add(-tt.cappedAgo)
			w.QuotaCapLimit = 8
			w.QuotaCapRegion = "us-east-1"
			if err := store.PutWatch(ctx, w); err != nil {
				t.Fatalf("PutWatch: %v", err)
			}
			p, calls := quotaPoller(t, store, w.WatchID, 2, func() (*spawnaws.LaunchResult, error) {
				return nil, &capErr{quotaCode}
			})
			p.quotas = &fakeQuotaLookup{info: tt.info, err: tt.err}
			p.verbose = true
			p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }

			summary := &PollSummary{}
			p.pollFleetWatch(ctx, w, summary)
			if *calls != tt.wantCalls {
				t.Errorf("RunInstances attempts = %d, want %d", *calls, tt.wantCalls)
			}
			if summary.QuotaCapped != 1 {
				t.Errorf("summary.QuotaCapped = %d, want 1 (suppressed or re-capped, still capped)", summary.QuotaCapped)
			}
		})
	}
}

// TestRecordQuotaCap_RecordsLimitAndRegion: the cap stores the limit it read
// (#158), so a later cycle has something to compare against; with no reading
// it stores none.
func TestRecordQuotaCap_RecordsLimitAndRegion(t *testing.T) {
	store := fleetStore(t)
	ctx := context.Background()
	w := quotaFleetWatch(t, "w-rec-limit")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatalf("PutWatch: %v", err)
	}
	p := &Poller{store: store, quotas: &fakeQuotaLookup{info: map[string]*quotas.QuotaInfo{"us-east-1": gQuota(8, 8)}}}
	p.notifyQuotaCap = func(context.Context, *Watch, int, string) error { return nil }
	m := &MatchResult{Region: "us-east-1", InstanceType: "g6e.xlarge"}
	p.recordQuotaCap(ctx, w, 2, m, &capErr{quotaCode}, &PollSummary{})

	got, _ := store.GetWatch(ctx, w.WatchID)
	if got.QuotaCapLimit != 8 || got.QuotaCapRegion != "us-east-1" {
		t.Errorf("stored limit/region = %d/%q, want 8/us-east-1", got.QuotaCapLimit, got.QuotaCapRegion)
	}
	if w.QuotaCapLimit != 8 || w.QuotaCapRegion != "us-east-1" {
		t.Errorf("in-memory limit/region = %d/%q, want 8/us-east-1", w.QuotaCapLimit, w.QuotaCapRegion)
	}

	// Unreadable on the next cap: the old limit must not linger.
	p.quotas = &fakeQuotaLookup{err: errors.New("throttled")}
	p.recordQuotaCap(ctx, w, 3, m, &capErr{quotaCode}, &PollSummary{})
	got, _ = store.GetWatch(ctx, w.WatchID)
	if got.QuotaCapLimit != 0 || w.QuotaCapLimit != 0 {
		t.Errorf("limit after an unreadable cap = %d (stored) / %d (memory), want 0", got.QuotaCapLimit, w.QuotaCapLimit)
	}

	// Reaching the goal clears it with the rest of the cap state.
	p.clearQuotaCap(ctx, w)
	got, _ = store.GetWatch(ctx, w.WatchID)
	if got.QuotaCapRegion != "" || w.QuotaCapRegion != "" {
		t.Errorf("region not cleared: stored %q, memory %q", got.QuotaCapRegion, w.QuotaCapRegion)
	}
}

// TestQuotaCapBody_NamesBothIntervals: the notification tells the user how
// often a capped fleet retries in both modes.
func TestQuotaCapBody_NamesBothIntervals(t *testing.T) {
	body := quotaCapBody(&Watch{WatchID: "w", DesiredCount: 5}, 2, "reason")
	for _, want := range []string{QuotaCapReprobeInterval.String(), QuotaCapBackstopInterval.String(), "as soon as\nthe limit is raised"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
}
