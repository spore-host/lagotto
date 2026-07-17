package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

var (
	launchAt             string
	launchAfter          string
	launchCron           string
	launchSpawnConfig    string
	launchRegion         string
	launchAZ             string
	launchStackName      string
	launchName           string
	launchIfExists       string
	launchReservationID  string
	launchAtReserveStart bool
	launchFireEarly      time.Duration
	launchRetryInterval  time.Duration
)

var launchCmd = &cobra.Command{
	Use:   "launch",
	Short: "Schedule a future or recurring instance launch (--at / --after / --cron)",
	Long: `Schedule an instance launch to fire at a clock time (--at), after a delay
(--after), or on a recurring cron (--cron) — as opposed to 'watch', which fires
when capacity appears. The motivating case is launching into an EC2 Capacity
Block at its reserved start time:

  lagotto launch --at 2026-07-01T08:00:00Z --spawn-config block.yaml

where block.yaml sets reservation_id + capacity_block.

This is driven by EventBridge Scheduler in the hosted poller stack, so it
requires 'lagotto deploy' to have been run (the schedule targets the poller
Lambda in your account). The launched instance always carries a TTL (#38).`,
	RunE: runLaunch,
}

func init() {
	rootCmd.AddCommand(launchCmd)
	f := launchCmd.Flags()
	f.StringVar(&launchAt, "at", "", "Fire once at this RFC3339 time (e.g. 2026-07-01T08:00:00Z)")
	f.StringVar(&launchAfter, "after", "", "Fire once after this delay (e.g. 6h, 30m, 2d)")
	f.StringVar(&launchCron, "cron", "", "Fire on this cron schedule (e.g. '0 9 ? * MON-FRI *')")
	f.StringVar(&launchSpawnConfig, "spawn-config", "", "YAML file with the spawn LaunchConfig (required)")
	f.StringVar(&launchRegion, "region", "", "AWS region to launch in (default: from your AWS config)")
	f.StringVar(&launchAZ, "az", "", "Availability zone (required to match a Capacity Block's AZ)")
	f.StringVar(&launchStackName, "stack-name", "lagotto", "Deployed lagotto stack name (provides the poller target)")
	f.StringVar(&launchName, "name", "", "Instance Name tag (the overlap dedup key); defaults to the spawn config's name")
	f.StringVar(&launchIfExists, "if-exists", "", "If an instance with this Name already exists at fire time: skip|launch|replace (default: skip for --at/--after, launch for --cron)")
	// #62 Capacity-Block start-time launch.
	f.StringVar(&launchReservationID, "reservation-id", "", "Capacity Block reservation id (cr-…) to launch into")
	f.BoolVar(&launchAtReserveStart, "at-reservation-start", false, "Fire at the reservation's start time (derived from the reservation), retrying through the window open until the instance runs (requires --reservation-id)")
	f.DurationVar(&launchFireEarly, "fire-early", 2*time.Minute, "With --at-reservation-start: fire this long before the window open so a Scheduler delay doesn't burn paid time")
	f.DurationVar(&launchRetryInterval, "retry-interval", 30*time.Second, "With --at-reservation-start: how often to retry through the boundary until the launch succeeds")
}

func runLaunch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	out := cmd.OutOrStdout()

	if launchSpawnConfig == "" {
		return fmt.Errorf("--spawn-config is required")
	}
	if launchAtReserveStart && launchReservationID == "" {
		return fmt.Errorf("--at-reservation-start requires --reservation-id")
	}

	// Resolve the schedule expression. Two modes:
	//   - default (#49): from --at/--after/--cron (one-of, validated up front).
	//   - --at-reservation-start (#62): derived from the Capacity Block reservation
	//     below, after the AWS client is available. expr stays empty until then.
	var expr string
	oneShot := true
	if !launchAtReserveStart {
		var err error
		expr, oneShot, err = watcher.ScheduleExpression(launchAt, launchAfter, launchCron, time.Now())
		if err != nil {
			return err
		}
	}

	// Load + validate the spawn config (this applies the #38 TTL guarantee).
	launchConfigJSON, err := loadEC2SpawnConfig(launchSpawnConfig)
	if err != nil {
		return fmt.Errorf("load spawn config: %w", err)
	}

	// Resolve the overlap policy + dedup name (#49). The Name tag is the dedup key;
	// --name overrides the config's name. The default policy depends on the schedule
	// shape: a one-shot (--at/--after, e.g. a Capacity Block) must not double-launch,
	// so it defaults to skip; a cron is meant to produce a fresh box each fire, so it
	// defaults to launch.
	instanceName := launchName
	if instanceName == "" {
		var sc watcher.SpawnConfigFile
		if jerr := json.Unmarshal(launchConfigJSON, &sc); jerr == nil {
			instanceName = sc.Name
		}
	}
	ifExists, err := resolveIfExists(launchIfExists, oneShot)
	if err != nil {
		return err
	}

	// --region wins; otherwise the shared config's region (then ambient).
	cfg, err := awscfg.Load(ctx, launchRegion)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	region := cfg.Region
	if region == "" {
		return fmt.Errorf("no AWS region set; pass --region or configure one")
	}

	// --at-reservation-start (#62): derive the schedule + boundary-retry window from
	// the Capacity Block reservation itself, so the user never transcribes a time.
	var retryUntil time.Time
	if launchAtReserveStart {
		spawnClient := spawnaws.NewClientFromConfig(cfg)
		cr, cerr := spawnClient.DescribeCapacityReservation(ctx, region, launchReservationID)
		if cerr != nil {
			return fmt.Errorf("describe reservation %s: %w", launchReservationID, cerr)
		}
		switch cr.State {
		case "expired", "payment-failed", "cancelled":
			return fmt.Errorf("reservation %s is %s — not launchable", launchReservationID, cr.State)
		}
		if cr.StartDate == "" {
			return fmt.Errorf("reservation %s has no start date; cannot derive a launch time", launchReservationID)
		}
		start, perr := time.Parse(time.RFC3339, cr.StartDate)
		if perr != nil {
			return fmt.Errorf("parse reservation start %q: %w", cr.StartDate, perr)
		}
		// Fire a touch before the window so Scheduler latency doesn't burn paid time;
		// never schedule in the past (a reservation that already opened fires now).
		fireAt := start.Add(-launchFireEarly)
		if fireAt.Before(time.Now()) {
			fireAt = time.Now().Add(15 * time.Second)
		}
		expr = fmt.Sprintf("at(%s)", fireAt.UTC().Format("2006-01-02T15:04:05"))
		oneShot = true
		// Retry through the window open, but stop well before the block END (all CBs
		// end 11:30 UTC; AWS reclaims from 11:00). Default deadline: the block end
		// minus 1h if known, else 1h of retrying from the open.
		if cr.EndDate != "" {
			if end, eerr := time.Parse(time.RFC3339, cr.EndDate); eerr == nil {
				retryUntil = end.Add(-1 * time.Hour)
			}
		}
		if retryUntil.IsZero() {
			retryUntil = start.Add(1 * time.Hour)
		}
		// Pin the launch AZ to the reservation's AZ if the user didn't.
		if launchAZ == "" {
			launchAZ = cr.AvailabilityZone
		}
		fmt.Fprintf(out, "Reservation %s: state=%s window %s → %s; firing %s (retry until %s)\n",
			launchReservationID, cr.State, cr.StartDate, cr.EndDate, expr, retryUntil.UTC().Format(time.RFC3339))
	}

	userID := ""
	if id, ierr := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); ierr == nil && id.Arn != nil {
		userID = *id.Arn
	}

	// Discover the poller function + scheduler role from the deployed stack: the
	// per-launch schedule targets the poller Lambda with a routing payload.
	outputs, err := deploy.New(cfg).StackOutputs(ctx, launchStackName)
	if err != nil {
		return fmt.Errorf("could not read stack %q outputs (run 'lagotto deploy' first): %w", launchStackName, err)
	}
	fnArn := outputs["CapacityPollerFunctionArn"]
	roleArn := outputs["SchedulerInvokeRoleArn"]
	if fnArn == "" || roleArn == "" {
		return fmt.Errorf("stack %q is missing the poller function / scheduler role outputs — redeploy with 'lagotto deploy' (a stack deployed without the Lambda can't run scheduled launches)", launchStackName)
	}

	// Persist the scheduled launch, then arm the schedule. Store first so the
	// Lambda can always resolve the id the schedule will send.
	scheduleID := "sl-" + uuid.New().String()[:8]
	scheduleName := "lagotto-launch-" + scheduleID
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	if _, err := store.EnsureScheduledTable(ctx); err != nil {
		return fmt.Errorf("ensure scheduled-launches table: %w", err)
	}

	sl := &watcher.ScheduledLaunch{
		ScheduleID:       scheduleID,
		UserID:           userID,
		Status:           watcher.ScheduledPending,
		Region:           region,
		AvailabilityZone: launchAZ,
		CronExpr:         launchCron,
		LaunchConfigJSON: launchConfigJSON,
		ScheduleName:     scheduleName,
		InstanceName:     instanceName,
		IfExists:         ifExists,
		// #62 Capacity-Block start-time fields (zero when not --at-reservation-start).
		ReservationID:        launchReservationID,
		RetryUntil:           retryUntil,
		RetryIntervalSeconds: int(launchRetryInterval.Seconds()),
		CreatedAt:            time.Now().UTC(),
		// Age the record out ~30 days after creation (one-shots fire long before).
		TTLTimestamp: time.Now().Add(30 * 24 * time.Hour).Unix(),
	}
	if !oneShot {
		sl.CronExpr = launchCron
	}
	if err := store.PutScheduledLaunch(ctx, sl); err != nil {
		return err
	}

	payload := fmt.Sprintf(`{"scheduled_launch_id":%q}`, scheduleID)
	createIn := &scheduler.CreateScheduleInput{
		Name:               aws.String(scheduleName),
		ScheduleExpression: aws.String(expr),
		FlexibleTimeWindow: &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		Target: &schedulertypes.Target{
			Arn:     aws.String(fnArn),
			RoleArn: aws.String(roleArn),
			Input:   aws.String(payload),
		},
	}
	// A one-shot self-deletes after it fires so it doesn't linger; a cron stays.
	if oneShot {
		createIn.ActionAfterCompletion = schedulertypes.ActionAfterCompletionDelete
	}
	if _, err := scheduler.NewFromConfig(cfg).CreateSchedule(ctx, createIn); err != nil {
		// Roll back the stored record so we don't leave an orphan pending launch
		// that nothing will ever fire.
		_ = store.UpdateScheduledLaunchStatus(ctx, scheduleID, watcher.ScheduledFailed, "")
		return fmt.Errorf("create EventBridge schedule: %w", err)
	}

	fmt.Fprintf(out, "Scheduled launch %s armed (%s) in %s.\n", scheduleID, expr, region)
	if launchAZ != "" {
		fmt.Fprintf(out, "  AZ: %s\n", launchAZ)
	}
	if instanceName != "" {
		fmt.Fprintf(out, "  Name: %s (if it already exists at fire time: %s)\n", instanceName, ifExists)
	}
	fmt.Fprintf(out, "  Cancel with: aws scheduler delete-schedule --name %s --region %s\n", scheduleName, region)
	return nil
}

// resolveIfExists validates the --if-exists flag and applies the shape default:
// a one-shot (--at/--after) defaults to "skip" so a launch into, say, a Capacity
// Block can't double-book; a cron defaults to "launch" so each fire is a fresh box.
func resolveIfExists(flag string, oneShot bool) (string, error) {
	switch strings.ToLower(strings.TrimSpace(flag)) {
	case "":
		if oneShot {
			return watcher.IfExistsSkip, nil
		}
		return watcher.IfExistsLaunch, nil
	case watcher.IfExistsSkip:
		return watcher.IfExistsSkip, nil
	case watcher.IfExistsLaunch:
		return watcher.IfExistsLaunch, nil
	case watcher.IfExistsReplace:
		return watcher.IfExistsReplace, nil
	default:
		return "", fmt.Errorf("invalid --if-exists %q: want skip, launch, or replace", flag)
	}
}
