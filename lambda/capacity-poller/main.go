package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

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
	// pollerFunctionArn / schedulerInvokeRoleArn let a #62 boundary retry re-arm a
	// fresh EventBridge schedule targeting this same Lambda. Sourced from env (the
	// stack passes its own CapacityPollerFunctionArn / SchedulerInvokeRoleArn).
	pollerFunctionArn      string
	schedulerInvokeRoleArn string
)

// nowUTC returns the current time in UTC (wrapped for testability/consistency).
func nowUTC() time.Time { return time.Now().UTC() }

func init() {
	ctx := context.Background()

	var err error
	cfg, err = config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	watchesTable := getEnv("WATCHES_TABLE", "lagotto-watches")
	historyTable := getEnv("HISTORY_TABLE", "lagotto-match-history")
	scheduledTable := getEnv("SCHEDULED_TABLE", "lagotto-scheduled-launches")
	snsTopicArn := os.Getenv("SNS_TOPIC_ARN")
	scheduleName = getEnv("SCHEDULE_NAME", "lagotto-capacity-poller")
	pollerFunctionArn = os.Getenv("POLLER_FUNCTION_ARN")
	schedulerInvokeRoleArn = os.Getenv("SCHEDULER_INVOKE_ROLE_ARN")

	store = watcher.NewStore(cfg, watchesTable, historyTable)
	store.SetScheduledTable(scheduledTable)

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
		Notifier: notifier,
		Spawner:  spawner,
		// Without a Holder, --action hold silently degraded to notify in the
		// deployed poller (#39). Wire it so a hold watch actually reserves
		// capacity (CreateCapacityReservation).
		Holder:    watcher.NewHolder(cfg),
		SageMaker: watcher.NewSageMakerLauncher(cfg),
		// Lease each watch before acting (#47) so the hosted poller and any local
		// `poll --daemon` can't both fire the same watch. No Filter — the hosted
		// poller is the one account-wide poller and services every watch.
		LeaseOwner: "hosted-lambda",
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

// handleScheduledLaunch fires a time-triggered launch (#49/#62). It loads the
// ScheduledLaunch and runs it through the spawner's RunScheduled, which applies
// Capacity-Block start-time semantics when the launch carries a ReservationID:
//   - launched  → record the instance, done (one-shot schedule self-deletes).
//   - retry     → re-arm a tight-interval EventBridge schedule to try again at
//     the window boundary, until RetryUntil — the #62 "fire reliably the moment
//     the window opens" behavior, since EventBridge one-shots don't retry.
//   - failed    → mark failed (dead reservation, bad config, or budget exhausted).
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

	outcome, instanceID, lerr := spawner.RunScheduled(ctx, sl)
	switch outcome {
	case watcher.OutcomeLaunched:
		if uerr := store.UpdateScheduledLaunchStatus(ctx, id, watcher.ScheduledLaunched, instanceID); uerr != nil {
			log.Printf("warning: launched %s but could not record it on scheduled launch %s: %v", instanceID, id, uerr)
		}
		log.Printf("Scheduled launch %s launched instance %s", id, instanceID)
		return nil

	case watcher.OutcomeRetry:
		return rearmBoundaryRetry(ctx, sl)

	default: // OutcomeFailed
		if uerr := store.UpdateScheduledLaunchStatus(ctx, id, watcher.ScheduledFailed, ""); uerr != nil {
			log.Printf("warning: could not mark scheduled launch %s failed: %v", id, uerr)
		}
		return fmt.Errorf("scheduled launch %s: %w", id, lerr)
	}
}

// rearmBoundaryRetry re-arms a tight-interval EventBridge schedule for a #62
// Capacity-Block launch that hit a retryable boundary condition, unless the retry
// deadline (RetryUntil) has passed. EventBridge one-shots don't retry themselves,
// so we self-reschedule: create a fresh at(now+interval) schedule targeting this
// same Lambda with the same routing payload. The schedule uses
// ActionAfterCompletion=DELETE so each attempt's schedule self-removes after it
// fires. State is moved to "retrying" so the teardown refcount keeps the infra up.
func rearmBoundaryRetry(ctx context.Context, sl *watcher.ScheduledLaunch) error {
	now := nowUTC()
	if sl.RetryUntil.IsZero() || now.After(sl.RetryUntil) {
		log.Printf("Scheduled launch %s: retry deadline passed (until %s), giving up", sl.ScheduleID, sl.RetryUntil.Format(time.RFC3339))
		if uerr := store.UpdateScheduledLaunchStatus(ctx, sl.ScheduleID, watcher.ScheduledFailed, ""); uerr != nil {
			log.Printf("warning: could not mark scheduled launch %s failed: %v", sl.ScheduleID, uerr)
		}
		return fmt.Errorf("scheduled launch %s: boundary retry budget exhausted (deadline %s)", sl.ScheduleID, sl.RetryUntil.Format(time.RFC3339))
	}

	if pollerFunctionArn == "" || schedulerInvokeRoleArn == "" {
		return fmt.Errorf("scheduled launch %s: cannot re-arm boundary retry — POLLER_FUNCTION_ARN/SCHEDULER_INVOKE_ROLE_ARN not configured", sl.ScheduleID)
	}

	interval := sl.RetryIntervalSeconds
	if interval <= 0 {
		interval = 30
	}
	next := now.Add(time.Duration(interval) * time.Second)
	// Don't overshoot the deadline — if the next tick would land past RetryUntil,
	// clamp to the deadline so we get one last attempt right at the edge.
	if next.After(sl.RetryUntil) {
		next = sl.RetryUntil
	}

	sl.Attempts++
	sl.Status = watcher.ScheduledRetrying
	if uerr := store.PutScheduledLaunch(ctx, sl); uerr != nil {
		log.Printf("warning: could not persist retry state for %s: %v", sl.ScheduleID, uerr)
	}

	// A fresh schedule name per attempt so the prior one's pending DELETE can't
	// collide with the new create.
	retryName := fmt.Sprintf("lagotto-launch-%s-r%d", sl.ScheduleID, sl.Attempts)
	expr := fmt.Sprintf("at(%s)", next.Format("2006-01-02T15:04:05"))
	payload := fmt.Sprintf(`{"scheduled_launch_id":%q}`, sl.ScheduleID)
	_, err := schedulerClient.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:                  aws.String(retryName),
		ScheduleExpression:    aws.String(expr),
		FlexibleTimeWindow:    &schedulertypes.FlexibleTimeWindow{Mode: schedulertypes.FlexibleTimeWindowModeOff},
		ActionAfterCompletion: schedulertypes.ActionAfterCompletionDelete,
		Target: &schedulertypes.Target{
			Arn:     aws.String(pollerFunctionArn),
			RoleArn: aws.String(schedulerInvokeRoleArn),
			Input:   aws.String(payload),
		},
	})
	if err != nil {
		return fmt.Errorf("scheduled launch %s: re-arm boundary retry: %w", sl.ScheduleID, err)
	}
	log.Printf("Scheduled launch %s: re-armed boundary retry #%d at %s (deadline %s)", sl.ScheduleID, sl.Attempts, expr, sl.RetryUntil.Format(time.RFC3339))
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

		// Table auto-deletion is opt-in via AUTO_DELETE_TABLES (#59). A poller
		// deployed by `lagotto deploy` is deliberate, persistent infra and now
		// REFERENCES the CLI-owned tables by name — it must not delete them out from
		// under itself when they idle to empty (the user tears down explicitly with
		// `lagotto deploy --teardown` / `lagotto teardown`). The env var is left
		// unset by the stack, so a deployed poller disables its schedule (no cost
		// when idle) but never deletes data tables.
		if getEnv("AUTO_DELETE_TABLES", "") == "true" {
			empty, err := store.TablesEmpty(ctx)
			if err != nil {
				log.Printf("Warning: could not check whether tables are empty: %v", err)
			} else if empty {
				log.Println("Tables empty, deleting CLI-managed lagotto tables (no litter)")
				deleted, err := store.DeleteManagedTables(ctx)
				if err != nil {
					log.Printf("Warning: failed to delete tables: %v", err)
				} else if len(deleted) > 0 {
					log.Printf("Deleted tables: %v", deleted)
				}
			} else {
				log.Println("Tables still hold records (history retained until TTL); not deleting")
			}
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
