package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
)

// The creator/backstop race (lagotto#326), with the real `lagotto watch` and
// the real idle teardown sharing one lifecycle table: the watch runs while the
// poller's teardown holds the lease, at the moment of its first heartbeat.
// Either the watch waits and is refused (the lease is live), or it takes a
// stale lease over and the teardown stops with ErrLeaseLost at that very
// heartbeat. Never both proceed.

// raceTeardown runs the poller's report-mode teardown past its grace, and
// runs watch (on its own goroutine, waited for) just before the teardown's
// first heartbeat after the claim. It returns both outcomes.
func raceTeardown(t *testing.T, table *lifecycletest.Fake, watch func() error) (idle.Result, error) {
	t.Helper()
	const acct = "123456789012"
	src := inventorytest.New("us-east-1").SetRegion(inventorytest.Deployed("us-east-1", acct)).SetAccount(inventorytest.Account(acct))
	var clockMu sync.Mutex
	now := time.Now()
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	poller, err := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil },
		ControlRegion: idle.ControlRegion("us-east-1"), Owner: "lagotto-poller/us-east-1", Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	var logs []string
	var mu sync.Mutex
	cfg := idle.Config{Region: "us-east-1", AccountID: acct, Grace: time.Minute, Now: clock,
		Logf: func(f string, a ...any) { mu.Lock(); logs = append(logs, fmt.Sprintf(f, a...)); mu.Unlock() }}
	deps := idle.Deps{Lifecycle: poller, Source: src, Deleters: inventory.Deleters{AccountID: acct}}
	ctx := context.Background()
	if r := idle.Tick(ctx, cfg, deps); r.Outcome != idle.OutcomeWaiting {
		t.Fatalf("first tick %+v", r)
	}
	clockMu.Lock()
	now = now.Add(2 * time.Minute) // past the grace
	clockMu.Unlock()

	var watchErr error
	updates, fired := 0, false
	table.Hook = func(_ context.Context, op string) {
		if op != "UpdateItem" || fired {
			return
		}
		if updates++; updates == 2 { // the claim, then the first heartbeat
			fired = true
			done := make(chan struct{})
			go func() { defer close(done); watchErr = watch() }()
			<-done
		}
	}
	res := idle.Tick(ctx, cfg, deps)
	table.Hook = nil
	if !fired {
		t.Fatalf("the teardown never heartbeat (%+v):\n%s", res, strings.Join(logs, "\n"))
	}
	return res, watchErr
}

// TestRaceLiveLeaseBlocksTheWatch: the lease is live, so the watch waits it
// out, is refused and creates nothing; the teardown carries on and gives the
// lease back.
func TestRaceLiveLeaseBlocksTheWatch(t *testing.T) {
	table := lifecycletest.New()
	f := &fakeAWS{scheduleState: "ENABLED", leases: table}
	res, werr := raceTeardown(t, table, func() error { return runWatchWithFake(t, f) })
	var tip *idle.TeardownInProgressError
	if !errors.As(werr, &tip) {
		t.Fatalf("watch err = %v, want the teardown-in-progress refusal", werr)
	}
	if got := putStatuses(f); len(got) != 0 {
		t.Errorf("the refused watch stored %v", got)
	}
	if res.Outcome != idle.OutcomeReported || res.Err != nil {
		t.Errorf("teardown %+v", res)
	}
}

// TestRaceStaleLeaseStopsTheTeardown: to the watch the lease looks stale (its
// clock is an hour on), so it takes it over and creates the watch; the
// teardown's heartbeat then fails with ErrLeaseLost and it stops.
func TestRaceStaleLeaseStopsTheTeardown(t *testing.T) {
	table := lifecycletest.New()
	f := &fakeAWS{scheduleState: "ENABLED", leases: table,
		leaseOpts: []func(*lifecycle.Options){func(o *lifecycle.Options) { o.Now = func() time.Time { return time.Now().Add(time.Hour) } }}}
	res, werr := raceTeardown(t, table, func() error { return runWatchWithFake(t, f) })
	if werr != nil {
		t.Fatalf("watch: %v", werr)
	}
	if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
		t.Errorf("watch statuses %v", got)
	}
	if res.Outcome != idle.OutcomeLeaseLost || !errors.Is(res.Err, lifecycle.ErrLeaseLost) {
		t.Fatalf("teardown %+v, want it stopped by the lost lease", res)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }})
	if it, ok, _ := c.Get(context.Background(), inventory.Tool, "us-east-1"); !ok || it.State != lifecycle.StateActive || it.Lease != nil || it.Epoch != 1 {
		t.Errorf("lease item %+v", it)
	}
}
