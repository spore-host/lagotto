package inventory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	fp "github.com/spore-host/libs/footprint"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// RegionDeleter deletes one region's resources with lagotto's existing
// deleters: a *deploy.Deployer (deploy --teardown's) and a *watcher.Store
// (teardown's tables) for the region satisfy it together.
type RegionDeleter interface {
	DeleteTable(ctx context.Context, name string) (watcher.TableOutcome, error)
	DeleteArtifactBucket(ctx context.Context, region, accountID string) (deploy.BucketTeardown, error)
	DeleteAlertsTopic(ctx context.Context, region, accountID string) (bool, error)
	DeletePollerFunction(ctx context.Context) (bool, error)
	DeletePollerLogGroup(ctx context.Context, region string) (bool, error)
	DeleteSchedule(ctx context.Context, group, name string) (bool, error)
	DeleteScheduleGroup(ctx context.Context, name string) (bool, error)
}

// AccountDeleter deletes the account-wide resources with lagotto's existing
// IAM deleters (runtimeiam.DeleteRole, runtimeiam.DeleteInstanceProfile,
// cliiam.DeletePolicy).
type AccountDeleter interface {
	DeleteRole(ctx context.Context, name string) (bool, error)
	DeleteInstanceProfile(ctx context.Context, name string) (bool, error)
	DeletePolicy(ctx context.Context) (cliiam.Deletion, error)
}

// Deleters are the deleters a teardown deletes with.
type Deleters struct {
	AccountID string
	Region    func(region string) RegionDeleter
	Account   AccountDeleter
}

// ErrInUse is what the Deleter's re-check returns for a resource the fresh
// footprint classes active. lagotto's teardown deletes nothing in use.
var ErrInUse = errors.New("in use (class active)")

// NewDeleter returns lagotto's footprint.Deleter. Describe answers from live,
// a footprint taken when the teardown starts: a resource it does not list is
// gone (footprint.ErrNotFound), and its live tags, name, class and ownership
// are what footprint.Execute re-applies the guard to. One it classes active
// is refused with ErrInUse. Delete removes a resource with the deleter that
// owns its type. The capacity reservations, training jobs and the legacy
// stack have none, so a plan naming one fails on it and deletes nothing it
// depends on.
func NewDeleter(live *fp.Report, d Deleters) fp.ByType {
	byARN := map[string]fp.Resource{}
	if live != nil {
		for _, r := range live.Resources {
			byARN[r.ARN] = r
		}
	}
	base := liveDescriber{byARN: byARN}
	reg := func(r fp.Resource) RegionDeleter { return d.Region(r.Region) }
	gone := func(ok bool, err error) error {
		if err == nil && !ok {
			return fp.ErrNotFound
		}
		return err
	}
	return fp.ByType{
		TypeTable: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			out, err := reg(r).DeleteTable(ctx, r.Name)
			switch {
			case err != nil:
				return err
			case out == watcher.TableAbsent:
				return fp.ErrNotFound
			case out == watcher.TableUnmanaged:
				return fmt.Errorf("table %s left in place: it lacks the lagotto:managed=cli tag", r.Name)
			}
			return nil
		}},
		TypeBucket: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			if r.Name != deploy.DefaultBucketName(d.AccountID, r.Region) {
				return fmt.Errorf("bucket %s is not this account's artifact bucket for %s", r.Name, r.Region)
			}
			out, err := reg(r).DeleteArtifactBucket(ctx, r.Region, d.AccountID)
			switch {
			case err != nil:
				return err
			case out.Outcome == deploy.BucketAbsent:
				return fp.ErrNotFound
			case out.Outcome == deploy.BucketRetainedUntagged:
				return fmt.Errorf("bucket %s left in place: it lacks the %s=%s tag", r.Name, deploy.ManagedTagKey, deploy.ManagedTagValue)
			}
			return nil
		}},
		TypeTopic: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			return gone(reg(r).DeleteAlertsTopic(ctx, r.Region, d.AccountID))
		}},
		TypeFunction: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			return gone(reg(r).DeletePollerFunction(ctx))
		}},
		TypeLogGroup: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			return gone(reg(r).DeletePollerLogGroup(ctx, r.Region))
		}},
		TypeSchedule: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			group, name, ok := scheduleOf(r.ARN)
			if !ok || name != r.Name {
				return fmt.Errorf("schedule ARN %s does not name schedule %s", r.ARN, r.Name)
			}
			return gone(reg(r).DeleteSchedule(ctx, group, name))
		}},
		TypeScheduleGroup: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			return gone(reg(r).DeleteScheduleGroup(ctx, r.Name))
		}},
		TypeRole: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			return gone(d.Account.DeleteRole(ctx, r.Name))
		}},
		TypeInstanceProfile: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			ok, err := d.Account.DeleteInstanceProfile(ctx, r.Name)
			switch {
			case err != nil:
				return err
			case !ok && r.Name == runtimeiam.InstanceRoleName:
				return fp.ErrNotFound
			case !ok:
				// It was there at the re-check, so it lacks setup's tag.
				return fmt.Errorf("instance profile %s left in place: absent, or it lacks the %s=%s tag",
					r.Name, runtimeiam.ManagedProfileTagKey, runtimeiam.ManagedProfileTagValue)
			}
			return nil
		}},
		TypePolicy: deleterFunc{base, func(ctx context.Context, r fp.Resource) error {
			out, err := d.Account.DeletePolicy(ctx)
			switch {
			case err != nil:
				return err
			case out.Absent:
				return fp.ErrNotFound
			case out.Unmanaged:
				return fmt.Errorf("policy %s left in place: it lacks the %s=%s tag", cliiam.PolicyName, cliiam.ManagedTagKey, cliiam.ManagedTagValue)
			}
			return nil
		}},
	}
}

// scheduleOf parses arn:aws:scheduler:<region>:<account>:schedule/<group>/<name>.
func scheduleOf(arn string) (group, name string, ok bool) {
	i := strings.Index(arn, ":schedule/")
	if i < 0 {
		return "", "", false
	}
	parts := strings.Split(arn[i+len(":schedule/"):], "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

type liveDescriber struct{ byARN map[string]fp.Resource }

func (l liveDescriber) Describe(_ context.Context, r fp.Resource) (fp.Resource, error) {
	cur, ok := l.byARN[r.ARN]
	if !ok {
		return fp.Resource{}, fmt.Errorf("%s: %w", r.ARN, fp.ErrNotFound)
	}
	if cur.Class == fp.ClassActive {
		return cur, fmt.Errorf("%s: %w", r.ARN, ErrInUse)
	}
	return cur, nil
}

type deleterFunc struct {
	liveDescriber
	del func(ctx context.Context, r fp.Resource) error
}

func (d deleterFunc) Delete(ctx context.Context, r fp.Resource) error { return d.del(ctx, r) }

// Kept is a planned resource a teardown leaves in place without trying it,
// and why.
type Kept struct {
	Resource fp.Resource
	Reason   string
}

// PlanTeardown builds the teardown of planned, as re-checked against live (a
// footprint taken now). Everything active, in either, is kept, and so is
// everything an active resource depends on, directly or not, in either: the
// schedule group of a pending launch, the role an active poller runs as. The
// rest is ordered by footprint.NewPlan, which footprint.Execute then runs,
// re-checking each against live before it is deleted.
func PlanTeardown(planned, live *fp.Report, o Options) (*fp.Plan, []Kept, error) {
	if planned == nil {
		return nil, nil, errors.New("no plan")
	}
	if planned.Tool != Tool {
		return nil, nil, fmt.Errorf("the plan is %q's footprint, not lagotto's", planned.Tool)
	}
	if live != nil && planned.Account != "" && live.Account != "" && planned.Account != live.Account {
		return nil, nil, fmt.Errorf("the plan is for account %s, but these credentials are for %s", planned.Account, live.Account)
	}
	deps := map[string][]string{}
	var seeds []string
	note := func(rs []fp.Resource) {
		for _, r := range rs {
			deps[r.ARN] = append(deps[r.ARN], r.DependsOn...)
			if r.Class == fp.ClassActive {
				seeds = append(seeds, r.ARN)
			}
		}
	}
	note(planned.Resources)
	if live != nil {
		note(live.Resources)
	}
	// keptBy[arn] is the active resource that keeps arn ("" when it is
	// active itself).
	keptBy := map[string]string{}
	for _, s := range seeds {
		if _, done := keptBy[s]; done && keptBy[s] == "" {
			continue
		}
		keptBy[s] = ""
		stack := []string{s}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			for _, dep := range deps[cur] {
				if _, done := keptBy[dep]; !done {
					keptBy[dep] = s
					stack = append(stack, dep)
				}
			}
		}
	}
	var remaining []fp.Resource
	var kept []Kept
	for _, r := range planned.Resources {
		by, isKept := keptBy[r.ARN]
		switch {
		case !isKept:
			remaining = append(remaining, r)
		case by == "":
			kept = append(kept, Kept{Resource: r, Reason: "active"})
		default:
			kept = append(kept, Kept{Resource: r, Reason: "an active resource depends on it: " + by})
		}
	}
	sort.SliceStable(kept, func(i, j int) bool { return kept[i].Resource.ARN < kept[j].Resource.ARN })
	plan, err := fp.NewPlan(Tool, Patterns(o), remaining)
	return plan, kept, err
}
