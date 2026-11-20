package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// The teardown lease (lagotto#326): every creating command marks lagotto
// active in its region before it creates anything.

const leaseKey = "tool#lagotto#us-east-1"

// creators runs each creating command against f, returning its error. Each
// sets up what it needs to get as far as creating.
var creators = map[string]func(t *testing.T, f *fakeAWS) error{
	"watch": func(t *testing.T, f *fakeAWS) error {
		f.scheduleState = "ENABLED"
		return runWatchWithFake(t, f)
	},
	"watch --local": func(t *testing.T, f *fakeAWS) error {
		f.tablesAbsent = true
		setFlagsForTest(t, watchCmd, map[string]string{"local": "true"})
		return runWatchWithFake(t, f)
	},
	"launch": func(t *testing.T, f *fakeAWS) error {
		cfgPath := filepath.Join(t.TempDir(), "launch.yaml")
		if err := os.WriteFile(cfgPath, []byte("instance_type: g5.xlarge\nttl: 2h\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		setFlagsForTest(t, launchCmd, map[string]string{"spawn-config": cfgPath, "after": "6h"})
		f.pollerVersion = "0.70.0"
		installFakeAWS(t, f)
		return runLaunch(launchCmd, nil)
	},
	"deploy": func(t *testing.T, f *fakeAWS) error {
		setFlagsForTest(t, deployCmd, map[string]string{"version": "0.70.0", "yes": "true"})
		installFakeAWS(t, f)
		return runDeploy(deployCmd, nil)
	},
}

// TestWatchMarksActive: a watch, and a --local one, record lagotto active in
// the region (epoch 1) and then create the watch.
func TestWatchMarksActive(t *testing.T) {
	for _, name := range []string{"watch", "watch --local"} {
		t.Run(name, func(t *testing.T) {
			f := &fakeAWS{}
			if err := creators[name](t, f); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if f.leaseTable().Item(lifecycle.TableName, leaseKey) == nil {
				t.Fatalf("%s did not mark lagotto active", name)
			}
			c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return f.leaseTable(), nil }})
			if it, ok, err := c.Get(context.Background(), inventory.Tool, "us-east-1"); !ok || err != nil || it.Epoch != 1 || it.State != lifecycle.StateActive {
				t.Errorf("lease item %+v, %v, %v; want ACTIVE at epoch 1", it, ok, err)
			}
			if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
				t.Errorf("watch statuses %v", got)
			}
		})
	}
}

// TestCreatorsMarkActiveFirst: every creator's first write is the lease. With
// the lease table failing, each refuses having made no DynamoDB call and
// created nothing, which only a MarkActive before every create explains.
func TestCreatorsMarkActiveFirst(t *testing.T) {
	for name, run := range creators {
		t.Run(name, func(t *testing.T) {
			f := &fakeAWS{}
			f.leaseTable().Err = func(op string, _ any) error {
				if op == "TransactWriteItems" {
					return errors.New("scripted lease failure")
				}
				return nil
			}
			err := run(t, f)
			if err == nil || !strings.Contains(err.Error(), "scripted lease failure") || !strings.Contains(err.Error(), "Nothing was created") {
				t.Fatalf("%s: err = %v, want the lease failure", name, err)
			}
			if n := f.leaseTable().Calls("TransactWriteItems"); n == 0 {
				t.Errorf("%s never called MarkActive", name)
			}
			assertCreatedNothing(t, name, f)
		})
	}
}

// TestCreatorsRefuseDuringTeardown: a teardown holds the lease in the region
// for the whole wait. Each creator exits non-zero with the clear refusal, and
// creates nothing.
func TestCreatorsRefuseDuringTeardown(t *testing.T) {
	for name, run := range creators {
		t.Run(name, func(t *testing.T) {
			f := &fakeAWS{}
			holdLease(t, f.leaseTable(), "lagotto-poller/us-east-1")
			err := run(t, f)
			var tip *idle.TeardownInProgressError
			if !errors.As(err, &tip) {
				t.Fatalf("%s: err = %v, want a TeardownInProgressError", name, err)
			}
			for _, want := range []string{"refusing to", "lagotto is being torn down in us-east-1", "lagotto-poller/us-east-1", "Nothing was created"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal lacks %q: %v", name, want, err)
				}
			}
			assertCreatedNothing(t, name, f)
		})
	}
}

// TestCreatorsWarnWithoutLeaseAccess: a caller whose policy predates the
// lease table is warned and carries on (lagotto#328 makes it a refusal).
func TestCreatorsWarnWithoutLeaseAccess(t *testing.T) {
	f := &fakeAWS{}
	f.leaseTable().Err = func(string, any) error {
		return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "not authorized"}
	}
	if err := creators["watch"](t, f); err != nil {
		t.Fatalf("watch without lease access: %v", err)
	}
	if got := putStatuses(f); len(got) != 1 {
		t.Errorf("the watch was not created: %v", got)
	}
}

// holdLease claims the teardown lease in us-east-1 as owner.
func holdLease(t *testing.T, table *lifecycletest.Fake, owner string) *lifecycle.Lease {
	t.Helper()
	c, err := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, Owner: owner})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, _, err := c.Observe(ctx, inventory.Tool, "us-east-1", false); err != nil {
		t.Fatal(err)
	}
	l, err := c.Claim(ctx, inventory.Tool, "us-east-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// assertCreatedNothing: no DynamoDB call on lagotto's tables, no schedule
// switched on, and (deploy) nothing after the caller check.
func assertCreatedNothing(t *testing.T, name string, f *fakeAWS) {
	t.Helper()
	if n := dynamoCalls(f); n != 0 {
		t.Errorf("%s made %d DynamoDB call(s) on lagotto's tables after the refusal: %v", name, n, f.calls)
	}
	if n := f.called("scheduler:UpdateSchedule"); n != 0 {
		t.Errorf("%s switched the poller schedule on", name)
	}
	if name == "deploy" {
		for _, c := range f.calls {
			if c != "sts:GetCallerIdentity" {
				t.Errorf("deploy called %s after the refusal", c)
			}
		}
	}
}
