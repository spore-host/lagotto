package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/spore-host/lagotto/pkg/pollcadence"
	"github.com/spore-host/lagotto/pkg/watcher"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
	"github.com/spore-host/truffle/pkg/quotas"
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

// setup loads the AWS config and builds the store, notifier, spawner and poller.
// It runs from main, NOT from init (#214): package init also runs in this
// package's test binary, before any TestMain can make the environment hermetic,
// so an init that loads the default config would build a real one from the
// developer's ambient profile every time the tests ran.
func setup() {
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
		// Hosted: this poller has no shell sandbox, so it refuses shell-based
		// fleet completion conditions (#70); those run only on a local daemon.
		Hosted: true,
		// Optional Service Quotas numbers for a #153 quota-cap report. Nil-safe and
		// fail-open: the cap itself comes from the RunInstances error, so a poller
		// whose runtime policy predates the servicequotas grant still reports
		// "capped at M/N" — just without the exact limit/usage figures.
		Quotas: quotas.NewClientFromConfig(cfg),
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

	case watcher.OutcomeCancelled:
		// `lagotto cancel` marked it cancelled (#279); a schedule it raced with or
		// could not delete still fired. Nothing to launch, nothing to record.
		log.Printf("Scheduled launch %s was cancelled — not launching", id)
		return nil

	default: // OutcomeFailed
		// The reason is kept on the record (#309), so the owner can read it with
		// 'lagotto update'/'cancel' or an administrator with 'lagotto doctor',
		// rather than only in this log. A scheduled launch has no notify channel.
		reason := scheduledFailureReason(sl, lerr)
		if uerr := store.FailScheduledLaunch(ctx, id, reason); uerr != nil {
			log.Printf("warning: could not mark scheduled launch %s failed: %v", id, uerr)
		}
		return fmt.Errorf("scheduled launch %s: %w", id, lerr)
	}
}

// scheduledFailureReason is the last_error a failed scheduled launch records:
// the error, and for a config the poller refused on its own rules, the fix
// (watcher.CheckStoredLaunchConfig), since nothing else will tell the owner.
func scheduledFailureReason(sl *watcher.ScheduledLaunch, err error) string {
	if err == nil {
		return "the launch failed"
	}
	reason := err.Error()
	if watcher.IsConfigRefused(err) {
		if f := watcher.CheckStoredLaunchConfig(sl.ScheduleID, sl.LaunchConfigJSON); f != nil {
			reason += ". Fix: " + strings.Join(f.Fix, "; then ")
		}
	}
	return reason
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
		// Cancelled since this firing read the record (#279): do not re-arm it.
		if errors.Is(uerr, watcher.ErrScheduledLaunchCancelled) {
			log.Printf("Scheduled launch %s was cancelled — not re-arming its boundary retry", sl.ScheduleID)
			return nil
		}
		log.Printf("warning: could not persist retry state for %s: %v", sl.ScheduleID, uerr)
	}

	// A fresh schedule name per attempt so the prior one's pending DELETE can't
	// collide with the new create.
	retryName := fmt.Sprintf("lagotto-launch-%s-r%d", watcher.ShortID(sl.ScheduleID), sl.Attempts) // as deploy.LaunchScheduleName
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

// handlePoll runs one account-wide poll cycle. The lambda is a stateless
// singleton: one schedule per account drives it, every invocation sweeps all
// active watches, and watches drop out of the active set as they launch
// (matched), hit a terminal error (failed), or pass their TTL (expired).
//
// When nothing is left to poll the lambda does NOT disable its schedule (#323):
// it slows it to the idle cadence, hourly (about 720 invocations a month), and
// arming a watch sets it back to every 5 minutes. Disabling raced with arming
// and could leave a new watch unpolled; pkg/pollcadence has the protocol.
func handlePoll(ctx context.Context) error {
	log.Println("Starting capacity poll cycle")

	s, err := poller.PollAll(ctx)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}

	log.Printf("Poll complete: watched=%d launched=%d notified=%d retrying=%d failed=%d expired=%d quotaCapped=%d reminded=%d",
		s.Watched, s.Launched, s.Notified, s.Retrying, s.Failed, s.Expired, s.QuotaCapped, s.Reminded)

	res, err := reconcileCadence(ctx, schedulerClient, scheduleName, store, idleCheck(store))
	if err != nil {
		log.Printf("Warning: %v", err)
	}
	if res.Outcome != pollcadence.Idle {
		return nil
	}

	// Table auto-deletion is opt-in via AUTO_DELETE_TABLES (#59). A poller
	// deployed by `lagotto deploy` is deliberate, persistent infra and
	// REFERENCES the CLI-owned tables by name — it must not delete them out from
	// under itself when they idle to empty (the user tears down explicitly with
	// `lagotto deploy --teardown` / `lagotto teardown`). The env var is left
	// unset by the stack, so a deployed poller only slows its schedule when
	// idle and never deletes data tables.
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
	return nil
}

// workCounter is what idleCheck reads. *watcher.Store satisfies it.
type workCounter interface {
	ListActiveWatches(ctx context.Context) ([]watcher.Watch, error)
	HasPendingScheduledLaunches(ctx context.Context) (bool, error)
}

// idleCheck reports whether nothing is left to poll. Retrying watches are
// still active. Teardown refcount (#49): the poller stays at the normal
// cadence while EITHER an active watch OR a pending scheduled launch exists,
// so a scheduled --at next week does not have its infra idled under it.
func idleCheck(wc workCounter) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		active, err := wc.ListActiveWatches(ctx)
		if err != nil {
			return false, fmt.Errorf("could not check active watches, leaving the schedule as it is: %w", err)
		}
		pending, err := wc.HasPendingScheduledLaunches(ctx)
		if err != nil {
			return false, fmt.Errorf("could not check pending scheduled launches, leaving the schedule as it is: %w", err)
		}
		if len(active) > 0 || pending {
			log.Printf("%d active watches, pendingScheduledLaunches=%v — polling every 5 minutes", len(active), pending)
			return false, nil
		}
		return true, nil
	}
}

// reconcileCadence runs pollcadence.Reconcile and logs what it decided.
func reconcileCadence(ctx context.Context, sched pollcadence.ScheduleAPI, name string, ep pollcadence.Epoch, idle func(context.Context) (bool, error)) (pollcadence.Result, error) {
	res, err := pollcadence.Reconcile(ctx, sched, name, ep, idle)
	switch res.Outcome {
	case pollcadence.Idle:
		log.Printf("No active watches or pending scheduled launches: schedule %s at the idle cadence %s (%d update(s))", name, pollcadence.IdleExpression, res.Changed)
	case pollcadence.Armed:
		log.Printf("A watch was armed during the idle check: schedule %s kept at %s (%d update(s))", name, pollcadence.NormalExpression, res.Changed)
	case pollcadence.Unguarded:
		log.Printf("No active watches or pending scheduled launches, but the idle check could not be guarded: schedule %s left as it is", name)
	case pollcadence.Active:
		if res.Changed > 0 {
			log.Printf("Restored schedule %s to the normal cadence %s", name, pollcadence.NormalExpression)
		}
	}
	return res, err
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	setup()
	lambda.Start(handler)
}
