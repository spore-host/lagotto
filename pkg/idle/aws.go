package idle

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// ErrSelfDelete is what the poller's deleters answer for the poller's own
// function, log group or role, and for what it has no business deleting.
// Those are removed by the one-shot jobs (OneShot) or by `lagotto teardown`,
// never from inside the poller.
var ErrSelfDelete = errors.New("the poller never deletes this from inside itself")

// PollerDeleters are the deleters the poller's teardown runs with, built
// from concrete types so that what they can reach is exactly what the
// poller's runtime policy grants: lagotto's regional resources, and of IAM
// only lagotto's own instance role and profile. The poller's function, log
// group and role, the invoke role and the lagotto-cli-user policy are
// refused (ErrSelfDelete).
//
// Empty table names are lagotto's defaults, as in the poller's environment.
func PollerDeleters(cfg aws.Config, accountID, watchesTable, historyTable, scheduledTable string) inventory.Deleters {
	o := inventory.Options{WatchesTable: watchesTable, HistoryTable: historyTable, ScheduledTable: scheduledTable}
	names := o.Tables()
	watchesTable, historyTable, scheduledTable = names[0], names[1], names[2]
	regions := map[string]pollerRegion{}
	return inventory.Deleters{
		AccountID: accountID,
		Region: func(region string) inventory.RegionDeleter {
			if d, ok := regions[region]; ok {
				return d
			}
			c := cfg.Copy()
			c.Region = region
			s := watcher.NewStore(c, watchesTable, historyTable)
			s.SetScheduledTable(scheduledTable)
			d := pollerRegion{d: deploy.New(c), s: s}
			regions[region] = d
			return d
		},
		Account: pollerAccount{iam: iam.NewFromConfig(cfg)},
	}
}

type pollerRegion struct {
	d *deploy.Deployer
	s *watcher.Store
}

func (p pollerRegion) DeleteTable(ctx context.Context, name string) (watcher.TableOutcome, error) {
	return p.s.DeleteTable(ctx, name)
}

func (p pollerRegion) DeleteArtifactBucket(ctx context.Context, region, accountID string) (deploy.BucketTeardown, error) {
	return p.d.DeleteArtifactBucket(ctx, region, accountID)
}

func (p pollerRegion) DeleteAlertsTopic(ctx context.Context, region, accountID string) (bool, error) {
	return p.d.DeleteAlertsTopic(ctx, region, accountID)
}

func (p pollerRegion) DeletePollerFunction(context.Context) (bool, error) {
	return false, fmt.Errorf("function %s: %w (a one-shot job deletes it)", deploy.PollerFunctionName, ErrSelfDelete)
}

func (p pollerRegion) DeletePollerLogGroup(context.Context, string) (bool, error) {
	return false, fmt.Errorf("log group %s: %w (a one-shot job deletes it)", deploy.PollerLogGroupName, ErrSelfDelete)
}

func (p pollerRegion) DeleteSchedule(ctx context.Context, group, name string) (bool, error) {
	return p.d.DeleteSchedule(ctx, group, name)
}

func (p pollerRegion) DeleteScheduleGroup(ctx context.Context, name string) (bool, error) {
	return p.d.DeleteScheduleGroup(ctx, name)
}

type pollerAccount struct{ iam *iam.Client }

func (p pollerAccount) DeleteRole(ctx context.Context, name string) (bool, error) {
	if name != runtimeiam.InstanceRoleName {
		return false, fmt.Errorf("role %s: %w", name, ErrSelfDelete)
	}
	return runtimeiam.DeleteRole(ctx, p.iam, name)
}

func (p pollerAccount) DeleteInstanceProfile(ctx context.Context, name string) (bool, error) {
	if name != runtimeiam.InstanceRoleName {
		return false, fmt.Errorf("instance profile %s: %w", name, ErrSelfDelete)
	}
	return runtimeiam.DeleteInstanceProfile(ctx, p.iam, name)
}

func (p pollerAccount) DeletePolicy(context.Context) (cliiam.Deletion, error) {
	return cliiam.Deletion{}, fmt.Errorf("policy %s: %w", cliiam.PolicyName, ErrSelfDelete)
}
