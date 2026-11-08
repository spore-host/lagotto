package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// The spored check-in follow-up (#255, owner's option B: advisory).
//
// A spawn watch matches once its instance is running (#239), exactly as before.
// "Running" says nothing about spored, though: a bootstrap can still fail after
// it, and then nothing enforces the instance's TTL or runs its job. So after the
// match the poller looks, once per cycle, for a tag spored writes when it is up
// (spawn:last-heartbeat or spawn:spored-version), using the DescribeInstances the
// landing check already makes: no new IAM.
//
//	matched + spored_check_* --tag seen------------------> clear; spored_confirmed_at
//	matched + spored_check_* --instance gone/stopped-----> spored_unconfirmed (notify)
//	matched + spored_check_* --SporedCheckWindow passes--> spored_unconfirmed (notify)
//
// It is ADVISORY. It never terminates the instance: both tags are best-effort
// writes by the instance's own role, and a watch may name an iam_role without
// ec2:CreateTags, in which case a healthy spored never shows up here. Killing
// that instance would trade one false alarm for a lost launch.
//
// Storage: the pending check is three attributes on the matched watch record
// (spored_check_instance_id / _region / _until), written in the same PutWatch
// that records the match. Scheduling: each sweep queries status-index for
// status = matched with attribute_exists(spored_check_instance_id), the same
// index and Query action ListActiveWatches uses. The filter is applied after
// the read, so a sweep reads every retained matched watch (a few KB each), and
// describes only the ones with a pending check, one call each. The check is
// removed by a write conditional on it still being there, so when two pollers
// race only one records and notifies.

// SporedCheckWindow is how long after a spawn is seen running the poller waits
// for spored's tag before recording spored_unconfirmed.
const SporedCheckWindow = 10 * time.Minute

// ActionSporedUnconfirmed is the history action for a matched spawn whose spored
// never checked in within SporedCheckWindow (or whose instance went away first).
const ActionSporedUnconfirmed = "spored_unconfirmed"

// sporedTags are the instance tags spored writes once it is running.
var sporedTags = []string{"spawn:last-heartbeat", "spawn:spored-version"}

func hasSporedTag(tags []ec2types.Tag) bool {
	for _, t := range tags {
		for _, k := range sporedTags {
			if aws.ToString(t.Key) == k && aws.ToString(t.Value) != "" {
				return true
			}
		}
	}
	return false
}

// armSporedCheck sets up the follow-up on a watch about to be recorded as
// matched by a spawn (the caller's RecordMatch writes it). seen means the
// landing check already saw spored's tag, so there is nothing to follow up.
// Without a describer there is no way to check, so nothing is armed.
func (p *Poller) armSporedCheck(w *Watch, m *MatchResult, seen bool, now time.Time) {
	if m.InstanceID == "" || p.spawner == nil || p.spawner.describeInstance == nil {
		return
	}
	if seen {
		t := now
		m.SporedConfirmedAt = &t
		return
	}
	until := m.LandedAt
	if until.IsZero() {
		until = now
	}
	until = until.Add(SporedCheckWindow)
	w.SporedCheckInstanceID, w.SporedCheckRegion, w.SporedCheckUntil = m.InstanceID, m.Region, &until
}

// pollSporedChecks runs one follow-up per matched spawn with a pending check.
// Failures are logged, never fatal to the sweep.
func (p *Poller) pollSporedChecks(ctx context.Context, summary *PollSummary) {
	if p.spawner == nil || p.spawner.describeInstance == nil {
		return
	}
	watches, err := p.store.listSporedChecks(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not list spored check-ins to follow up: %v\n", err)
		return
	}
	now := p.clock()
	for i := range watches {
		w := &watches[i]
		if !p.filter.matches(w) {
			continue
		}
		summary.SporedChecks++
		p.checkSpored(ctx, w, now, summary)
	}
}

// checkSpored is one follow-up for one watch.
func (p *Poller) checkSpored(ctx context.Context, w *Watch, now time.Time, summary *PollSummary) {
	id := w.SporedCheckInstanceID
	until := time.Time{}
	if w.SporedCheckUntil != nil {
		until = *w.SporedCheckUntil
	}
	due := !now.Before(until)

	chk, err := p.spawner.describeInstance(ctx, w.SporedCheckRegion, id)
	if err != nil {
		if !due {
			fmt.Fprintf(os.Stderr, "Warning: watch %s: spored check-in for %s: %v (will retry)\n", w.WatchID, id, err)
			return
		}
		// Bounded: past the window the check stops, but an unknown is not reported
		// as spored failing.
		fmt.Fprintf(os.Stderr, "Warning: watch %s: spored check-in for %s could not be read before the window closed (%v); dropping the check\n", w.WatchID, id, err)
		_ = p.store.endSporedCheck(ctx, w, id, nil)
		return
	}

	if chk.SporedSeen {
		t := now
		if err := p.store.endSporedCheck(ctx, w, id, &t); err != nil && !errors.Is(err, errLandingResolved) {
			fmt.Fprintf(os.Stderr, "Warning: watch %s: could not record spored check-in for %s: %v\n", w.WatchID, id, err)
		}
		return
	}

	var note string
	switch {
	case chk.NotFound || instancePhase(chk.State) == phaseGone || instancePhase(chk.State) == phaseStopped:
		state := chk.State
		if chk.NotFound {
			state = "gone"
		}
		note = fmt.Sprintf("instance %s is %s and spored never checked in", id, state)
		if r := landingReason(chk); r != "" {
			note += " (" + r + ")"
		}
	case due:
		note = fmt.Sprintf("instance %s is %s but spored has not checked in", id, dashIfBlank(chk.State))
	default:
		return // still inside the window: look again next cycle
	}
	if m := w.LastMatch; m != nil && !m.LandedAt.IsZero() {
		note += fmt.Sprintf(" %s after it was seen running", now.Sub(m.LandedAt).Round(time.Second))
	}
	note += ". lagotto did not terminate it. If the watch's spawn config names its own iam_role, " +
		"that role may lack ec2:CreateTags, which spored needs to write spawn:last-heartbeat; " +
		"otherwise check the instance's console output for a failed bootstrap."

	if err := p.store.endSporedCheck(ctx, w, id, nil); err != nil {
		if !errors.Is(err, errLandingResolved) {
			fmt.Fprintf(os.Stderr, "Warning: watch %s: could not close spored check for %s: %v (will retry)\n", w.WatchID, id, err)
		}
		return
	}
	m := MatchResult{WatchID: w.WatchID, UserID: w.UserID, Region: w.SporedCheckRegion, InstanceID: id}
	if w.LastMatch != nil {
		m = *w.LastMatch
		m.SporedConfirmedAt, m.LostAt, m.LandingReason, m.ChangedFields = nil, nil, "", nil
	}
	// A row of its own: matched_at is the history sort key, so the spawned row
	// keeps its launch time and this one is when the poller gave up waiting.
	m.MatchedAt = now
	m.ActionTaken = ActionSporedUnconfirmed
	m.InstanceState = chk.State
	if chk.NotFound {
		m.InstanceState = "not-found"
	}
	m.SporedDetail = note
	if err := p.store.RecordHistory(ctx, &m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record spored_unconfirmed for %s: %v\n", w.WatchID, err)
	}
	fmt.Fprintf(os.Stderr, "Watch %s: %s\n", w.WatchID, note)
	summary.SporedUnconfirmed++
	if notify := p.sporedNotifier(); notify != nil {
		if err := notify(ctx, w, &m); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: spored_unconfirmed notification failed for %s: %v\n", w.WatchID, err)
		}
	}
}

// listSporedChecks returns the matched watches with a pending spored check-in.
func (s *Store) listSporedChecks(ctx context.Context) ([]Watch, error) {
	items, err := queryAll(ctx, s.client, &dynamodb.QueryInput{
		TableName:                &s.watchesTable,
		IndexName:                aws.String("status-index"),
		KeyConditionExpression:   aws.String("#st = :status"),
		FilterExpression:         aws.String("attribute_exists(spored_check_instance_id)"),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(StatusMatched)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list spored checks: %w", err)
	}
	out := make([]Watch, 0, len(items))
	for _, item := range items {
		var w Watch
		if err := attributevalue.UnmarshalMap(item, &w); err != nil {
			return nil, fmt.Errorf("unmarshal watch: %w", err)
		}
		out = append(out, w)
	}
	return out, nil
}

// endSporedCheck removes the pending check, conditional on it still naming id
// (errLandingResolved otherwise). confirmedAt, when set, is recorded as
// last_match.spored_confirmed_at, and on the spawned history row best-effort.
func (s *Store) endSporedCheck(ctx context.Context, w *Watch, id string, confirmedAt *time.Time) error {
	values := map[string]types.AttributeValue{":pid": &types.AttributeValueMemberS{Value: id}}
	expr := "REMOVE spored_check_instance_id, spored_check_region, spored_check_until"
	if confirmedAt != nil && w.LastMatch != nil {
		// The whole last_match, not a nested path: it is this record's own copy,
		// and the conditional write keeps a concurrent change from being lost.
		lm := *w.LastMatch
		lm.SporedConfirmedAt = confirmedAt
		av, err := attributevalue.Marshal(lm)
		if err != nil {
			return fmt.Errorf("marshal last_match: %w", err)
		}
		expr = "SET last_match = :lm " + expr
		values[":lm"] = av
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &s.watchesTable,
		Key:                       map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: w.WatchID}},
		ConditionExpression:       aws.String("spored_check_instance_id = :pid"),
		UpdateExpression:          aws.String(expr),
		ExpressionAttributeValues: values,
	})
	if err != nil {
		var cond *types.ConditionalCheckFailedException
		if errors.As(err, &cond) {
			return errLandingResolved
		}
		return fmt.Errorf("end spored check: %w", err)
	}
	if confirmedAt == nil || w.LastMatch == nil {
		return nil
	}
	// The spawned history row, if it is still there (best-effort: the watch
	// record above is the one that matters).
	key, err := attributevalue.Marshal(w.LastMatch.MatchedAt)
	if err != nil {
		return nil
	}
	_, _ = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.historyTable,
		Key: map[string]types.AttributeValue{
			"watch_id":   &types.AttributeValueMemberS{Value: w.WatchID},
			"matched_at": key,
		},
		ConditionExpression: aws.String("attribute_exists(watch_id) AND instance_id = :pid"),
		UpdateExpression:    aws.String("SET spored_confirmed_at = :t"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pid": values[":pid"],
			":t":   &types.AttributeValueMemberS{Value: confirmedAt.UTC().Format(time.RFC3339Nano)},
		},
	})
	return nil
}
