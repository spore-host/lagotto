package cmd

// `lagotto update sl-… --spawn-config <file>` (#309): replace a scheduled
// launch's spawn config in place. Before this the only way to change one was
// `lagotto cancel` and a new `lagotto launch`, and a new config that no longer
// validated (an inline IAM policy, refused since 0.70.0) left nothing armed:
// the schedule was gone and the replacement refused. Here the new config is
// validated in full first (every check `lagotto launch` makes, the reservation
// re-applied, the instance-role authorization) and only then swapped in, with
// one conditional write. The EventBridge schedule is never touched, so a
// refused update leaves the launch exactly as it was.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

// validateForScheduledLaunch refuses the flags that mean nothing for a
// scheduled launch: only its spawn config can change.
func (o updateOpts) validateForScheduledLaunch() error {
	if o.spawnConfig == "" {
		return fmt.Errorf("a scheduled launch's spawn config is the only thing 'update' changes: pass --spawn-config <file>")
	}
	if o.instanceTypeSet || len(o.addTypes) > 0 || o.regionsSet || len(o.addRegions) > 0 || len(o.removeRegions) > 0 ||
		o.azsSet || o.spot || o.noSpot || o.maxPriceSet || o.notifySet || len(o.addNotify) > 0 || len(o.removeNotify) > 0 ||
		o.projectSet || o.reactivate || o.requireQuota {
		return fmt.Errorf("only --spawn-config (and --dry-run) apply to a scheduled launch; its time, region and reservation are fixed " +
			"(cancel it and 'lagotto launch' again to change those)")
	}
	return nil
}

// scheduledLaunchUpdater is what updateScheduledLaunch does to AWS; fakes in
// tests.
type scheduledLaunchUpdater struct {
	// load reads and validates a spawn config as `lagotto launch` does.
	load func(ctx context.Context, source string) (*watcher.SpawnConfigFile, error)
	// describe reads a Capacity Reservation (for a launch into one).
	describe describeReservationFunc
	// checkRole refuses a config whose iam_role the deployed poller may not
	// pass (checkInstanceRoleAuthorized).
	checkRole func(ctx context.Context, config []byte) error
	// swap is Store.UpdateScheduledLaunchConfig.
	swap func(ctx context.Context, sl *watcher.ScheduledLaunch, config []byte) error
}

// runUpdateScheduledLaunch is `lagotto update sl-…`: the AWS wiring around
// updateScheduledLaunch.
func runUpdateScheduledLaunch(cmd *cobra.Command, o updateOpts, id string) error {
	if err := o.validateForScheduledLaunch(); err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("get caller identity: %w", err)
	}
	sl, err := findOwnedScheduledLaunch(ctx, store, stsClient, aws.ToString(identity.UserId), id, cfg.Region)
	if err != nil {
		return err
	}
	read := watcher.NewConfigReader(func(context.Context) (aws.Config, error) { return cfg, nil }, os.Stdin)
	return updateScheduledLaunch(ctx, cmd.OutOrStdout(), sl, o.spawnConfig, o.dryRun, scheduledLaunchUpdater{
		load: func(ctx context.Context, src string) (*watcher.SpawnConfigFile, error) {
			return loadEC2SpawnConfigFile(ctx, read, src)
		},
		describe: spawnaws.NewClientFromConfig(cfg).DescribeCapacityReservation,
		checkRole: func(ctx context.Context, config []byte) error {
			_, err := checkInstanceRoleAuthorizedFromConfig(ctx, cfg, os.Stderr, &watcher.Watch{LaunchConfigJSON: config})
			return err
		},
		swap: store.UpdateScheduledLaunchConfig,
	})
}

// updateScheduledLaunch validates the config at source for sl and swaps it
// in. Every refusal is returned before anything is written, and says that the
// launch is unchanged.
func updateScheduledLaunch(ctx context.Context, out io.Writer, sl *watcher.ScheduledLaunch, source string, dryRun bool, u scheduledLaunchUpdater) error {
	short := watcher.ShortID(sl.ScheduleID)
	if !sl.Armed() {
		msg := fmt.Sprintf("scheduled launch %s is %s and will not fire again, so there is nothing to update", short, sl.Status)
		if sl.LastError != "" {
			msg += fmt.Sprintf(" (it failed: %s)", sl.LastError)
		}
		return fmt.Errorf("%s; arm a new one with 'lagotto launch ... --spawn-config <file>'", msg)
	}
	unchanged := fmt.Sprintf("scheduled launch %s was not changed: its schedule still fires its current config", short)

	file, err := u.load(ctx, source)
	if err != nil {
		return fmt.Errorf("%s. The new spawn config is refused: %w", unchanged, err)
	}
	az := sl.AvailabilityZone
	config, _, err := prepareLaunchConfig(ctx, u.describe, sl.Region, file, sl.ReservationID, &az)
	if err != nil {
		return fmt.Errorf("%s. %w", unchanged, err)
	}
	if err := u.checkRole(ctx, config); err != nil {
		return fmt.Errorf("%s.\n  %w", unchanged, err)
	}
	if bytes.Equal(config, sl.LaunchConfigJSON) {
		_, _ = fmt.Fprintf(out, "Scheduled launch %s already has that spawn config; nothing to update.\n", short)
		return nil
	}
	if dryRun {
		_, _ = fmt.Fprintf(out, "Scheduled launch %s: the new spawn config passes every check. Dry run: nothing written.\n", short)
		return nil
	}
	if err := u.swap(ctx, sl, config); err != nil {
		if errors.Is(err, watcher.ErrScheduledLaunchConfigChanged) {
			return fmt.Errorf("%s: it fired, was cancelled or was updated after it was read, so nothing was written; "+
				"check it and try again", unchanged)
		}
		return fmt.Errorf("%s: %w", unchanged, err)
	}
	_, _ = fmt.Fprintf(out, "Scheduled launch %s updated: its schedule (unchanged) now fires the new spawn config.\n", short)
	if sl.InstanceName != "" && file.Name != "" && file.Name != sl.InstanceName {
		_, _ = fmt.Fprintf(out, "  Note: its instance Name (and --if-exists key) stays %q; the new config's name %q is not used for that.\n", sl.InstanceName, file.Name)
	}
	return nil
}
