package idle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The eighth review of lagotto#351.

// srcRegion is recRegion whose deletes also remove what they delete from the
// harness's source, so the next tick sees the region as it is.
type srcRegion struct {
	recRegion
	h *harness
}

func (d srcRegion) DeleteTable(ctx context.Context, n string) (watcher.TableOutcome, error) {
	d.h.src.Update(d.region, func(r *inventory.Region) {
		var keep []inventory.Table
		for _, t := range r.Tables {
			if t.Name != n {
				keep = append(keep, t)
			}
		}
		r.Tables = keep
	})
	return d.recRegion.DeleteTable(ctx, n)
}

func (d srcRegion) DeleteArtifactBucket(ctx context.Context, a, b string) (deploy.BucketTeardown, error) {
	d.h.src.Update(d.region, func(r *inventory.Region) { r.Bucket = nil })
	return d.recRegion.DeleteArtifactBucket(ctx, a, b)
}

func (d srcRegion) DeleteAlertsTopic(ctx context.Context, a, b string) (bool, error) {
	d.h.src.Update(d.region, func(r *inventory.Region) { r.Topic = nil })
	return d.recRegion.DeleteAlertsTopic(ctx, a, b)
}

// TestReview8CommitFailureResumes (G2): a reap run whose direct deletes went
// (tables, bucket, topic) but whose commit failed rolls its jobs back and
// keeps its poller, in a region with no data resources left. The next run
// finds the region idle, waits its grace again, and finishes it: the
// poller's own schedule and its jobs, and the lease.
func TestReview8CommitFailureResumes(t *testing.T) {
	h := newHarness(t)
	d := h.deps()
	d.Deleters.Region = func(r string) inventory.RegionDeleter { return srcRegion{recRegion{h.ev, r}, h} }
	ctx := context.Background()
	if r := Tick(ctx, h.cfg(true), d); r.Outcome != OutcomeWaiting {
		t.Fatalf("first tick: %+v", r)
	}
	h.advance(25 * time.Hour)
	h.sched.err = errors.New("AccessDenied")
	r := Tick(ctx, h.cfg(true), d)
	if r.committed || !strings.Contains(strings.Join(h.ev.list(), "\n"), "delete "+region+" table") {
		t.Fatalf("the failed commit: %+v\n%s", r, h.log())
	}
	var f inventory.Region
	h.src.Update(region, func(r *inventory.Region) { f = *r })
	if len(f.Tables) != 0 || f.Function == nil {
		t.Fatalf("after the failed commit: %+v", f)
	}
	h.sched.err = nil
	if r := Tick(ctx, h.cfg(true), d); r.Outcome != OutcomeWaiting {
		t.Fatalf("the next tick waits its grace again: %+v\n%s", r, h.log())
	}
	h.advance(25 * time.Hour)
	if r := Tick(ctx, h.cfg(true), d); r.Outcome != OutcomeReaped || !r.committed {
		t.Errorf("a grace later: %+v\n%s", r, h.log())
	}
	if it, ok, _ := h.client.Get(ctx, Tool, region); ok {
		t.Errorf("the lease was not finished: %+v", it)
	}
}

// TestReview8WhyInUseUntimedMark (G4): a mark with no time, aged from the
// item's own times, gets the creator-mark reason.
func TestReview8WhyInUseUntimedMark(t *testing.T) {
	w := newAWWorld(t)
	c := w.client("reader")
	it := lifecycle.Item{State: lifecycle.StateActive, StampState: lifecycle.StateActive, LastActivityAt: t0,
		Marks: []lifecycle.MarkInfo{{Token: "no time"}}}
	if why := WhyInUse(c, it); !strings.HasPrefix(why, "a creator marked it") {
		t.Errorf("%q", why)
	}
}
