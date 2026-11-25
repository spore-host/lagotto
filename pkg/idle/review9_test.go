package idle

import (
	"context"
	"errors"
	"strings"
	"testing"

	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/inventory/inventorytest"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The ninth review of lagotto#351.

// authorizedProfile is the profile setup made for the administrator's role
// team-gpu, tagged as setup tags it.
func authorizedProfile() inventory.Profile {
	tags := inventorytest.Tags(inventory.Global)
	tags[runtimeiam.ManagedProfileTagKey] = runtimeiam.ManagedProfileTagValue
	return inventory.Profile{Item: inventory.Item{ARN: inventory.IAMARN(acct, "instance-profile", "team-gpu"), Name: "team-gpu", Tags: tags},
		Roles: []string{"team-gpu"}}
}

// TestReview9AuthorizedProfileKeepsThePollerRole (G1): a profile setup made
// for an authorized role goes before the poller role, whose policy is how
// it is found: when its delete fails the role stays, and the next run still
// finds it; once the role is gone, it is found by setup's tag.
func TestReview9AuthorizedProfileKeepsThePollerRole(t *testing.T) {
	w := newAWWorld(t)
	a := inventorytest.Account(acct)
	a.AuthorizedRoles = []inventory.Item{{ARN: inventory.IAMARN(acct, "role", "team-gpu"), Name: "team-gpu"}}
	a.AuthorizedProfiles = []inventory.Profile{authorizedProfile()}
	w.src.SetAccount(a, "team-gpu")
	w.acct.fail["profile team-gpu"] = errors.New("throttled")
	r := w.run(true, 0)
	if r.Outcome != AccountWideHeld {
		t.Fatalf("%+v\n%s", r, w.log())
	}
	if strings.Contains(strings.Join(w.ev.list(), "\n"), "delete global role "+runtimeiam.RoleName) {
		t.Errorf("the poller role went before the profile: %v", w.ev.list())
	}
	// The next run, the delete working, finds it through the role still.
	delete(w.acct.fail, "profile team-gpu")
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved || !strings.Contains(strings.Join(w.ev.list(), "\n"), "delete global profile team-gpu") {
		t.Errorf("the next run: %+v %v\n%s", r, w.ev.list(), w.log())
	}

	// A profile left once the role is gone (an older lagotto's teardown):
	// found by setup's tag.
	w = newAWWorld(t)
	a = inventorytest.Account(acct)
	var roles []inventory.Item
	for _, role := range a.Roles {
		if role.Name != runtimeiam.RoleName {
			roles = append(roles, role)
		}
	}
	a.Roles = roles
	w.src.SetAccount(a)
	w.src.SetTaggedProfiles(authorizedProfile())
	if r := w.run(true, 0); r.Outcome != AccountWideRemoved || !strings.Contains(strings.Join(w.ev.list(), "\n"), "delete global profile team-gpu") {
		t.Errorf("by its tag: %+v %v\n%s", r, w.ev.list(), w.log())
	}
	// With nothing of lagotto's account-wide left, nothing is listed.
	w = newAWWorld(t)
	w.src.SetAccount(inventory.Account{})
	w.run(false, 0)
	if n := w.src.Reads("global TaggedInstanceProfiles"); n != 0 {
		t.Errorf("listed the account's profiles %d times with nothing of lagotto's left", n)
	}
}

// TestReview9CreatorAfterTheFirstDelete (G2): a creator that arrives after
// the first delete stops the run at the next re-check (nothing more is
// deleted), and the lease is suspended (incomplete, no lease), not released.
func TestReview9CreatorAfterTheFirstDelete(t *testing.T) {
	w := newAWWorld(t)
	deletes := 0
	w.acct.before = func(string) {
		// During the second delete (after its re-check): that one goes, the
		// next re-check stops the run.
		if deletes++; deletes == 2 {
			if _, err := Mark(context.Background(), w.client("setup"), "us-west-2", "set up"); err != nil {
				t.Error(err)
			}
		}
	}
	r := w.run(true, 0)
	if r.Outcome != AccountWideHeld || len(w.ev.list()) != 2 {
		t.Fatalf("%+v %v\n%s", r, w.ev.list(), w.log())
	}
	g, _ := w.global()
	if g.Lease != nil || !g.Incomplete {
		t.Errorf("not suspended: %+v", g)
	}
	notRun := 0
	for _, a := range r.Result.Actions {
		if a.Outcome == fp.OutcomeNotRun {
			notRun++
		}
	}
	if notRun == 0 {
		t.Errorf("the run did not stop: %+v", r.Result.Actions)
	}
}

// TestReview9RefusedOrSkippedIsIncomplete (G4): a run whose steps were
// refused (changed since the plan) or skipped is not "removed": the global
// item stays, the lease is suspended (something deleted) or given back.
func TestReview9RefusedOrSkippedIsIncomplete(t *testing.T) {
	for _, o := range []fp.Outcome{fp.OutcomeRefused, fp.OutcomeSkipped} {
		w := newAWWorld(t)
		prev := execute
		execute = func(ctx context.Context, plan *fp.Plan, mode fp.Mode, d fp.Deleter, l *lifecycle.Lease) (*fp.Result, error) {
			res := &fp.Result{}
			for i, st := range plan.Steps {
				out := fp.OutcomeDeleted
				if i == len(plan.Steps)-1 {
					out = o
				}
				res.Actions = append(res.Actions, fp.Action{ARN: st.Resource.ARN, Type: st.Resource.Type, Name: st.Resource.Name, Outcome: out})
			}
			return res, nil
		}
		r := w.run(true, 0)
		execute = prev
		if r.Outcome != AccountWideHeld {
			t.Errorf("%s: %+v", o, r)
		}
		if g, ok := w.global(); !ok || g.Lease != nil || !g.Incomplete {
			t.Errorf("%s: the global item: %v %+v", o, ok, g)
		}
	}
}
