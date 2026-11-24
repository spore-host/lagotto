package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The fifth review of lagotto#351.

// TestReview5SettleDeployJobs (G3): a deploy settles the region's pending
// teardown jobs on every path, under its marks: with the poller's rate
// schedule there, they go; without it (the deploy failed before replacing
// the poller), they stay and finish the region's teardown. After a deploy
// that succeeded, a job that fired during it is an error.
func TestReview5SettleDeployJobs(t *testing.T) {
	ctx := context.Background()
	f := &fakeTeardownJobs{jobs: []string{"lagotto-teardown-function"}}
	newTeardownJobAPI = func(aws.Config, string) teardownJobAPI { return f }
	if err := settleDeployJobs(ctx, aws.Config{}, "us-west-2", false, &bytes.Buffer{}); err != nil || len(f.deleted) != 0 {
		t.Errorf("no schedule (a failed deploy): %v %v", err, f.deleted)
	}
	f.schedule = true
	prev := teardownPollerExists
	t.Cleanup(func() { teardownPollerExists = prev })
	teardownPollerExists = func(context.Context, aws.Config) (bool, error) { return true, nil }
	if err := settleDeployJobs(ctx, aws.Config{}, "us-west-2", true, &bytes.Buffer{}); err != nil || len(f.deleted) != 1 {
		t.Errorf("the schedule stands: %v %v", err, f.deleted)
	}
	teardownPollerExists = func(context.Context, aws.Config) (bool, error) { return false, nil }
	if err := settleDeployJobs(ctx, aws.Config{}, "us-west-2", true, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "deploy --region us-west-2' again") {
		t.Errorf("a job fired during the deploy: %v", err)
	}
	// The schedule cannot be read.
	f.err = errors.New("network down")
	if err := settleDeployJobs(ctx, aws.Config{}, "us-west-2", false, &bytes.Buffer{}); err == nil {
		t.Error("an unreadable schedule")
	}
	var warned bytes.Buffer
	f.err = &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "no"}
	if err := settleDeployJobs(ctx, aws.Config{}, "us-west-2", false, &warned); err != nil || !strings.Contains(warned.String(), "warning") {
		t.Errorf("denied: %v %q", err, warned.String())
	}
}

// syncBuffer is a bytes.Buffer safe for a writer goroutine and a reader.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *syncBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

// TestReview5UnfencedPendingJobs (G4): outside the aws partition the
// account-wide resources do not go while a lagotto-teardown job, which runs
// as the invoke role, is pending in any region, or a poller is left, or the
// footprint is incomplete.
func TestReview5UnfencedPendingJobs(t *testing.T) {
	for name, set := range map[string]func(r *inventory.Region, f *inventory.Facts){
		"a pending job": func(r *inventory.Region, _ *inventory.Facts) {
			r.Schedules = []inventory.Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: "lagotto-teardown-log-group"}}
		},
		"a poller": func(r *inventory.Region, _ *inventory.Facts) {
			r.Function = &inventory.Item{Name: "lagotto-capacity-poller"}
		},
		"incomplete": func(_ *inventory.Region, f *inventory.Facts) { f.AccountIncomplete = true },
	} {
		facts := func() inventory.Facts {
			f := orphanFacts()
			r := inventory.Region{Region: "us-gov-west-1"}
			set(&r, &f)
			f.Regions = []inventory.Region{r}
			return f
		}
		_, rec, _ := stubPlanTeardown(t, facts)
		r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
		if r.Outcome != idle.AccountWideBlocked || len(rec.calls) != 0 || accountWideError(r) == nil {
			t.Errorf("%s: %+v %v", name, r, rec.calls)
		}
	}
}

// TestReview5PreCheckPlanFailed (G5): `teardown --iam-roles`' pre-check
// that would go ahead but could not plan refuses, and nothing is deleted.
func TestReview5PreCheckPlanFailed(t *testing.T) {
	tables := &fakeTables{empty: true}
	calls := stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, true, false)
	stubPollerRegions(t, nil, nil)
	teardownAccountWideCheck = func(context.Context, aws.Config, string, string) idle.AccountWideResult {
		return idle.AccountWideResult{Outcome: idle.AccountWideWould, Why: "nothing needs them", Err: errors.New("plan: a cycle")}
	}
	_, err := runTeardownForTest(t)
	if err == nil || !strings.Contains(err.Error(), "could not plan") || tables.deleted || len(*calls) != 0 {
		t.Errorf("%v tables=%v calls=%v", err, tables.deleted, *calls)
	}
}

// TestReview5SetupMarksItsHomeRegion (G7): setup marks its home region as
// well as the global scope (the account-wide teardown's re-check before each
// delete sees the regional mark), and ends both.
func TestReview5SetupMarksItsHomeRegion(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	done, err := markGlobal(ctx, aws.Config{Region: "eu-west-1"}, "", "set up lagotto's IAM", &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	for _, scope := range []string{"eu-west-1", lifecycle.ScopeGlobal} {
		if it, ok, _ := c.Get(ctx, inventory.Tool, scope); !ok || len(it.Marks) != 1 {
			t.Errorf("%s: %+v", scope, it)
		}
	}
	done()
	for _, scope := range []string{"eu-west-1", lifecycle.ScopeGlobal} {
		if it, _, _ := c.Get(ctx, inventory.Tool, scope); len(it.Marks) != 0 {
			t.Errorf("%s after done: %+v", scope, it)
		}
	}
	// No region configured: the control region.
	done, err = markGlobal(ctx, aws.Config{}, "", "x", &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if it, ok, _ := c.Get(ctx, inventory.Tool, "us-east-1"); !ok || len(it.Marks) != 1 {
		t.Errorf("the control region: %+v", it)
	}
	done()
}

// TestReview5MarksRefreshed (libs L2, in lagotto): a creator's marks are
// refreshed while it runs, so one running longer than CreatorMarkBound is
// never taken for one that died; a mark that cannot be refreshed is a
// warning.
func TestReview5MarksRefreshed(t *testing.T) {
	ctx := context.Background()
	table := lifecycletest.New()
	installFakeLeases(t, table)
	prev := markRefresh
	t.Cleanup(func() { markRefresh = prev })
	markRefresh = 10 * time.Millisecond
	var out syncBuffer
	done, err := markGlobal(ctx, aws.Config{Region: "eu-west-1"}, "", "x", &out)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := lifecycle.New(lifecycle.Options{DynamoDB: func(string) (lifecycle.API, error) { return table, nil }, ControlRegion: "us-east-1"})
	first, _, _ := c.Get(ctx, inventory.Tool, "eu-west-1")
	deadline := time.Now().Add(5 * time.Second)
	for {
		it, _, _ := c.Get(ctx, inventory.Tool, "eu-west-1")
		if len(it.Marks) == 1 && it.Marks[0].At.After(first.Marks[0].At) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never refreshed: %+v", it.Marks)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The mark removed under it: warned, once.
	it, _, _ := c.Get(ctx, inventory.Tool, "eu-west-1")
	if _, err := c.MarkDone(ctx, inventory.Tool, "eu-west-1", it.Marks[0].Token); err != nil {
		t.Fatal(err)
	}
	for !strings.Contains(out.String(), "could not refresh") {
		if time.Now().After(deadline) {
			t.Fatalf("no warning: %q", out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	done()
	if n := strings.Count(out.String(), "could not refresh this command's mark on lagotto's eu-west-1"); n != 1 {
		t.Errorf("%d warnings: %q", n, out.String())
	}
}
