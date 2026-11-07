package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// scheduledLaunchIDPrefix starts every id `lagotto launch` assigns ("sl-…").
const scheduledLaunchIDPrefix = "sl-"

// isScheduledLaunchID reports whether `cancel` was given a scheduled launch's id
// rather than a watch's (#279).
func isScheduledLaunchID(id string) bool {
	return strings.HasPrefix(id, scheduledLaunchIDPrefix)
}

// cancelLaunchCmd is the cancel command's tail with the cancelled sl- id: the
// command `lagotto launch` prints, and the one a failed cancel says to rerun.
func cancelLaunchCmd(id, region string) string {
	c := "lagotto cancel " + id
	if region != "" {
		c += " --region " + region
	}
	return c
}

// runCancelScheduledLaunch is `lagotto cancel sl-…` (#279): the AWS wiring
// around cancelScheduledLaunch.
func runCancelScheduledLaunch(ctx context.Context, out, errOut io.Writer, cfg aws.Config, id string) error {
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	sl, err := store.GetScheduledLaunch(ctx, id)
	if err != nil {
		// No scheduled-launches table in this region: nothing was ever launched here.
		var noTable *types.ResourceNotFoundException
		if !errors.As(err, &noTable) {
			return err
		}
		sl = nil
	}
	sl, err = authorizeLaunchOwner(ctx, sts.NewFromConfig(cfg), id, cfg.Region, sl)
	if err != nil {
		return err
	}

	d := deploy.New(cfg)
	return cancelScheduledLaunch(ctx, out, errOut, sl, scheduledLaunchCanceller{
		markCancelled: func(ctx context.Context, id string, from watcher.ScheduledLaunchStatus) error {
			return store.CancelScheduledLaunch(ctx, id, from)
		},
		deleteSchedules: func(ctx context.Context, id, recorded string) (deploy.LaunchScheduleTeardown, error) {
			return d.DeleteScheduledLaunchSchedules(ctx, id, recorded)
		},
		confirm: func(prompt string) bool { return cancelYes || confirmCancel(prompt) },
	})
}

// authorizeLaunchOwner returns sl if the caller owns it, else the same "not
// found" error as a missing (nil) record, as authorizeWatchOwner does for a
// watch (#41).
func authorizeLaunchOwner(ctx context.Context, stsClient stsIdentityAPI, id, region string, sl *watcher.ScheduledLaunch) (*watcher.ScheduledLaunch, error) {
	notFound := fmt.Errorf("scheduled launch %s not found in %s (pass --region if 'lagotto launch' ran in another region)", id, region)
	if sl == nil {
		return nil, notFound
	}
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("get caller identity: %w", err)
	}
	if sl.UserID != "" && sl.UserID != aws.ToString(identity.Arn) {
		return nil, notFound
	}
	return sl, nil
}

// scheduledLaunchCanceller is what cancelScheduledLaunch does to AWS; fakes in
// tests.
type scheduledLaunchCanceller struct {
	// markCancelled is Store.CancelScheduledLaunch: cancelled only if the status
	// is still from.
	markCancelled func(ctx context.Context, id string, from watcher.ScheduledLaunchStatus) error
	// deleteSchedules is Deployer.DeleteScheduledLaunchSchedules.
	deleteSchedules func(ctx context.Context, id, recorded string) (deploy.LaunchScheduleTeardown, error)
	confirm         func(string) bool
}

// scheduledLaunchCancellable says whether cancelling sl stops anything, and if
// not, why. A pending launch has not fired; a retrying one (#62) fired but has
// launched nothing and is re-arming. A --cron launch stays armed after it fires,
// whatever its last fire did, so it is cancellable until it is cancelled. A
// one-shot that launched or failed has fired for good.
func scheduledLaunchCancellable(sl *watcher.ScheduledLaunch) (bool, string) {
	switch sl.Status {
	case watcher.ScheduledPending, watcher.ScheduledRetrying:
		return true, ""
	case watcher.ScheduledLaunched, watcher.ScheduledFailed:
		if sl.CronExpr != "" {
			return true, ""
		}
		msg := fmt.Sprintf("scheduled launch %s already fired (status: %s); there is nothing left to cancel", sl.ScheduleID, sl.Status)
		if sl.InstanceID != "" {
			msg += fmt.Sprintf(". It launched %s in %s, which cancel does not terminate; its TTL still applies. "+
				"To terminate it now: spawn terminate %s", sl.InstanceID, sl.Region, sl.InstanceID)
		}
		return false, msg
	default:
		return false, fmt.Sprintf("scheduled launch %s has status %q, which cancel does not recognise", sl.ScheduleID, sl.Status)
	}
}

// cancelScheduledLaunch is runCancelScheduledLaunch after the AWS wiring.
//
// It marks the record cancelled before deleting any schedule. The poller does
// not fire a cancelled launch and does not re-arm its boundary retry, so a
// schedule that fires between the two steps, or that the deletion misses (a
// retry created after the listing), launches nothing. The other order would
// leave a record that still counts as pending if the deletion succeeded and the
// update failed.
func cancelScheduledLaunch(ctx context.Context, out, errOut io.Writer, sl *watcher.ScheduledLaunch, c scheduledLaunchCanceller) error {
	if sl.Status == watcher.ScheduledCancelled {
		// A rerun after a deletion failed: the record is done; finish the schedules.
		_, _ = fmt.Fprintf(out, "Scheduled launch %s is already cancelled; deleting any schedule left behind.\n", sl.ScheduleID)
		return deleteCancelledLaunchSchedules(ctx, out, sl, c)
	}
	if ok, why := scheduledLaunchCancellable(sl); !ok {
		return errors.New(why)
	}

	what := sl.CronExpr
	if what == "" && !sl.LaunchAt.IsZero() {
		what = sl.LaunchAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	prompt := fmt.Sprintf("Cancel scheduled launch %s", sl.ScheduleID)
	if sl.InstanceName != "" {
		prompt += " (" + sl.InstanceName + ")"
	}
	if what != "" {
		prompt += ", " + what
	}
	if !c.confirm(prompt + "?") {
		_, _ = fmt.Fprintln(out, "Aborted.")
		return nil
	}

	if err := c.markCancelled(ctx, sl.ScheduleID, sl.Status); err != nil {
		if errors.Is(err, watcher.ErrScheduledLaunchChanged) {
			return fmt.Errorf("%w (it was %s): it fired or was cancelled while this ran; nothing was changed. "+
				"Run '%s' again to see where it stands", err, sl.Status, cancelLaunchCmd(sl.ScheduleID, sl.Region))
		}
		return err
	}
	_, _ = fmt.Fprintf(out, "Cancelled scheduled launch %s (was %s).\n", sl.ScheduleID, sl.Status)
	return deleteCancelledLaunchSchedules(ctx, out, sl, c)
}

// deleteCancelledLaunchSchedules deletes a cancelled launch's schedules and says what
// happened to each.
func deleteCancelledLaunchSchedules(ctx context.Context, out io.Writer, sl *watcher.ScheduledLaunch, c scheduledLaunchCanceller) error {
	res, err := c.deleteSchedules(ctx, sl.ScheduleID, sl.ScheduleName)
	for _, n := range res.Deleted {
		_, _ = fmt.Fprintf(out, "Deleted schedule %s\n", n)
	}
	for _, n := range res.Absent {
		_, _ = fmt.Fprintf(out, "Schedule %s was already gone\n", n)
	}
	if err != nil {
		return fmt.Errorf("scheduled launch %s is cancelled, but deleting its schedules failed: %w\n"+
			"A schedule left behind launches nothing when it fires (the poller skips a cancelled launch). "+
			"Run '%s' again to delete it", sl.ScheduleID, err, cancelLaunchCmd(sl.ScheduleID, sl.Region))
	}
	return nil
}
