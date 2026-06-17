package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

var (
	cfg             aws.Config
	store           *watcher.Store
	poller          *watcher.Poller
	spawner         *watcher.Spawner
	schedulerClient *scheduler.Client
	scheduleName    string
)

func init() {
	ctx := context.Background()

	var err error
	cfg, err = config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	watchesTable := getEnv("WATCHES_TABLE", "lagotto-watches")
	historyTable := getEnv("HISTORY_TABLE", "lagotto-match-history")
	snsTopicArn := os.Getenv("SNS_TOPIC_ARN")
	scheduleName = getEnv("SCHEDULE_NAME", "lagotto-capacity-poller")

	store = watcher.NewStore(cfg, watchesTable, historyTable)

	truffleClient, err := truffleaws.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create truffle client: %v", err)
	}

	// Set up notifier
	var notifier *watcher.Notifier
	if snsTopicArn != "" {
		notifier = watcher.NewNotifier(cfg, snsTopicArn)
	}

	// Set up spawner (for action=spawn watches AND scheduled launches, #49)
	spawner, err = watcher.NewSpawner(ctx)
	if err != nil {
		log.Printf("Warning: auto-spawn unavailable: %v", err)
	}

	poller = watcher.NewPoller(truffleClient, store, true, watcher.PollerOpts{
		Notifier:  notifier,
		Spawner:   spawner,
		SageMaker: watcher.NewSageMakerLauncher(cfg),
	})

	schedulerClient = scheduler.NewFromConfig(cfg)
}

// event is the Lambda invocation payload. The recurring capacity-poll schedule
// sends no input (empty) → the poll sweep. A per-launch EventBridge Scheduler
// target (#49) sends {"scheduled_launch_id":"sl-..."} → a one-shot launch. This
// payload routing keeps a single Lambda/role/artifact for both jobs.
type event struct {
	ScheduledLaunchID string `json:"scheduled_launch_id"`
}

// handler routes by payload: a scheduled-launch id runs that one-shot launch;
// anything else runs the account-wide capacity-poll sweep (handlePoll).
func handler(ctx context.Context, raw json.RawMessage) error {
	var e event
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &e) // a non-JSON/empty trigger is just the poll sweep
	}
	if e.ScheduledLaunchID != "" {
		return handleScheduledLaunch(ctx, e.ScheduledLaunchID)
	}
	return handlePoll(ctx)
}

// handleScheduledLaunch fires a time-triggered launch (#49). It loads the
// ScheduledLaunch, launches via the shared spawner, and records the outcome.
// For a one-shot the EventBridge schedule self-deletes (ActionAfterCompletion);
// a cron schedule stays armed and fires again.
func handleScheduledLaunch(ctx context.Context, id string) error {
	log.Printf("Scheduled launch %s firing", id)
	sl, err := store.GetScheduledLaunch(ctx, id)
	if err != nil {
		return fmt.Errorf("get scheduled launch %s: %w", id, err)
	}
	if sl == nil {
		log.Printf("scheduled launch %s not found (already cancelled/aged out) — nothing to do", id)
		return nil
	}
	if spawner == nil {
		return fmt.Errorf("scheduled launch %s: spawner unavailable", id)
	}
	instanceID, lerr := spawner.LaunchScheduled(ctx, sl)
	if lerr != nil {
		if uerr := store.UpdateScheduledLaunchStatus(ctx, id, watcher.ScheduledFailed, ""); uerr != nil {
			log.Printf("warning: could not mark scheduled launch %s failed: %v", id, uerr)
		}
		return fmt.Errorf("scheduled launch %s: %w", id, lerr)
	}
	if uerr := store.UpdateScheduledLaunchStatus(ctx, id, watcher.ScheduledLaunched, instanceID); uerr != nil {
		log.Printf("warning: launched %s but could not record it on scheduled launch %s: %v", instanceID, id, uerr)
	}
	log.Printf("Scheduled launch %s launched instance %s", id, instanceID)
	return nil
}

// handlePoll runs one account-wide poll cycle. The lambda is a stateless,
// self-terminating singleton: one schedule per account drives it, every
// invocation sweeps all active watches, and watches drop out of the active set
// as they launch (matched), hit a terminal error (failed), or pass their TTL
// (expired). When zero active watches remain, the lambda disables its own
// schedule — no watches, no lambda. Creating a watch re-arms the schedule.
func handlePoll(ctx context.Context) error {
	log.Println("Starting capacity poll cycle")

	s, err := poller.PollAll(ctx)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}

	log.Printf("Poll complete: watched=%d launched=%d notified=%d retrying=%d failed=%d expired=%d",
		s.Watched, s.Launched, s.Notified, s.Retrying, s.Failed, s.Expired)

	// Check if any active watches remain. Retrying watches are still active, so
	// the schedule stays armed until every watch has launched, failed, or
	// expired.
	active, err := store.ListActiveWatches(ctx)
	if err != nil {
		log.Printf("Warning: failed to check active watches: %v", err)
		return nil
	}

	// Teardown refcount (#49): infra stays alive while EITHER an active watch OR a
	// pending scheduled launch references it. A scheduled --at next week must not
	// have its schedule disabled / tables deleted out from under it just because
	// no watches are active.
	pendingLaunches, perr := store.HasPendingScheduledLaunches(ctx)
	if perr != nil {
		log.Printf("Warning: could not check pending scheduled launches: %v — leaving infra in place", perr)
		return nil
	}

	if len(active) == 0 && !pendingLaunches {
		log.Println("No active watches or pending scheduled launches, disabling schedule")
		if err := disableSchedule(ctx); err != nil {
			log.Printf("Warning: failed to disable schedule: %v", err)
		}

		// No litter: once there are no active watches AND the tables have fully
		// drained (resolved watches + match history aged out via DynamoDB TTL),
		// delete the tables so lagotto leaves nothing behind. A future
		// `lagotto watch` recreates them. We only delete when EMPTY, so no
		// history is destroyed prematurely (#12).
		empty, err := store.TablesEmpty(ctx)
		if err != nil {
			log.Printf("Warning: could not check whether tables are empty: %v", err)
		} else if empty {
			log.Println("Tables empty, deleting CLI-managed lagotto tables (no litter)")
			// Only deletes tables tagged lagotto:managed=cli — CloudFormation-
			// managed tables are left for the stack to own.
			deleted, err := store.DeleteManagedTables(ctx)
			if err != nil {
				log.Printf("Warning: failed to delete tables: %v", err)
			} else if len(deleted) > 0 {
				log.Printf("Deleted tables: %v", deleted)
			}
		} else {
			log.Println("Tables still hold records (history retained until TTL); not deleting")
		}
	} else {
		log.Printf("%d active watches, pendingScheduledLaunches=%v — leaving infra armed", len(active), pendingLaunches)
	}

	return nil
}

func disableSchedule(ctx context.Context) error {
	current, err := schedulerClient.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name: aws.String(scheduleName),
	})
	if err != nil {
		return fmt.Errorf("get schedule: %w", err)
	}

	_, err = schedulerClient.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
		Name:               current.Name,
		ScheduleExpression: current.ScheduleExpression,
		FlexibleTimeWindow: current.FlexibleTimeWindow,
		Target:             current.Target,
		State:              schedulertypes.ScheduleStateDisabled,
	})
	if err != nil {
		return fmt.Errorf("disable schedule: %w", err)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	lambda.Start(handler)
}
