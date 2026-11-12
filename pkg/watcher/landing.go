package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/smithy-go"
)

// Two-phase landing (#239 phase 2).
//
// RunInstances returning is not a landing: the instance is "pending", and EC2
// can still terminate it (Server.InsufficientInstanceCapacity), or its spored
// bootstrap can fail closed and shut it down. A created On-Demand Capacity
// Reservation can likewise still be "pending" and then "failed". Before #239 the
// watch was marked matched at that point and the hunt ended, whatever happened
// next.
//
// Now a single-shot spawn or hold watch moves through:
//
//	active (hunting) --launch/create--> active + pending_* (landing)
//	landing --running / active-------------------------> matched (notify, history)
//	landing --terminated / shutting-down / failed-------> active (hunting; last_error)
//	landing --stopping / stopped------------------------> terminate -> active
//	landing --still pending after PendingLandingTimeout-> terminate/cancel -> active
//	landing --watch deadline passed---------------------> terminate/cancel -> expired
//
// While landing, the poller never launches again for the watch (the
// double-spend guard): PollAll sends it to pollLanding instead of the capacity
// search. Fleets and scheduled launches are deliberately not part of this; see
// the CHANGELOG entry and the #239 PR for why.

// PendingLandingTimeout is how long a launched instance (or a created
// reservation) may stay pending before the poller gives up on it: terminates
// the instance through spawn's terminate path (which also cancels a Spot request
// and releases a spawn-allocated Elastic IP) or cancels the reservation, and
// resumes hunting. A normal instance leaves pending within a minute or two.
const PendingLandingTimeout = 10 * time.Minute

// landingNotFoundGrace is how long after the launch a describe that cannot find
// the resource still reads as "pending": right after RunInstances, EC2 is
// eventually consistent. A terminated instance stays describable for about an
// hour, so after this grace not-found means gone.
const landingNotFoundGrace = 2 * time.Minute

// LandingLeasePrefix marks the lease a landing watch carries. While a watch is
// landing, its lease_owner is "landing:<instance or reservation id>", expiring
// landingMarkerTTL after the last cycle that checked it.
//
// This is the guard against a poller that predates #239. Such a poller ignores
// pending_*, so it would see an active watch, find capacity and launch again.
// But every poller since #47 (v0.47.0) claims the watch's lease before acting,
// and that claim fails while another owner's lease is live. A current poller
// claims over the marker (claimLandingLease); an old one cannot.
const LandingLeasePrefix = "landing:"

// landingMarkerTTL is how long the landing marker blocks an older poller after
// the last cycle that refreshed it. Each landing cycle pushes it out again, so
// it only lapses if no current poller has looked at the watch for this long.
const landingMarkerTTL = 30 * time.Minute

// History actions for a landing that did not land.
const (
	// ActionLandingFailed: the instance terminated (or the reservation failed)
	// before it was running (active).
	ActionLandingFailed = "landing_failed"
	// ActionLandingTimedOut: still pending after PendingLandingTimeout, so the
	// poller terminated (cancelled) it.
	ActionLandingTimedOut = "landing_timed_out"
)

// landingCheck is what one describe of a pending resource saw.
type landingCheck struct {
	State string // EC2 instance state name, or capacity reservation state
	AZ    string
	// ReasonCode / ReasonMessage are EC2's StateReason for an instance (e.g.
	// "Server.InsufficientInstanceCapacity"). Empty for a reservation.
	ReasonCode    string
	ReasonMessage string
	// TransitionReason is EC2's StateTransitionReason, which carries EC2's own
	// timestamp of the transition, e.g. "Server.InternalError (2026-11-06
	// 23:49:12 GMT)" (#255). Empty for a reservation.
	TransitionReason string
	// SporedSeen: the instance carries a tag spored writes once it is up
	// (spawn:last-heartbeat or spawn:spored-version, #255). Read from the same
	// DescribeInstances, so it costs no extra call and no extra IAM.
	SporedSeen bool
	// NotFound: the describe succeeded but does not know the resource.
	NotFound bool
}

// errLandingResolved is returned by the conditional landing writes when the
// watch no longer carries the pending resource they were about: another poller
// resolved it first.
var errLandingResolved = errors.New("landing already resolved by another poller")

// Landing reports whether the watch has a launch or hold waiting to land.
func (w *Watch) Landing() bool {
	return w.PendingInstanceID != "" || w.PendingReservationID != ""
}

// PendingSinceTime is PendingSince, or the zero time when it is unset.
func (w *Watch) PendingSinceTime() time.Time {
	if w.PendingSince == nil {
		return time.Time{}
	}
	return *w.PendingSince
}

// PendingResourceID is the pending instance or reservation id, or "".
func (w *Watch) PendingResourceID() string {
	if w.PendingInstanceID != "" {
		return w.PendingInstanceID
	}
	return w.PendingReservationID
}

// pendingAttr is the attribute holding the pending resource id.
func (w *Watch) pendingAttr() string {
	if w.PendingInstanceID != "" {
		return "pending_instance_id"
	}
	return "pending_reservation_id"
}

// clearLanding drops the landing state from the in-memory watch, so a later
// whole-record write (RecordMatch) leaves no pending_* or marker behind.
func (w *Watch) clearLanding() {
	w.PendingInstanceID, w.PendingReservationID, w.PendingRegion = "", "", ""
	w.PendingSince = nil
	w.PendingMatch = nil
	if strings.HasPrefix(w.LeaseOwner, LandingLeasePrefix) {
		w.LeaseOwner, w.LeaseExpiresAt = "", time.Time{}
	}
}

func landingMarker(id string) string { return LandingLeasePrefix + id }

type landingPhase int

const (
	phaseWaiting landingPhase = iota
	phaseLanded
	phaseGone
	// phaseStopped: the instance exists but is stopping/stopped. Within a
	// landing window that is spawn's fail-closed bootstrap falling back to a
	// shutdown when it could not terminate itself (spawn sets no
	// InstanceInitiatedShutdownBehavior, so a shutdown stops it): spored is not
	// running, nothing enforces its TTL, and its volumes bill. It is not a
	// landing; it is given up on (terminated) like a stuck pending one.
	phaseStopped
)

// instancePhase maps an EC2 instance state.
func instancePhase(state string) landingPhase {
	switch state {
	case "running":
		return phaseLanded
	case "shutting-down", "terminated":
		return phaseGone
	case "stopping", "stopped":
		return phaseStopped
	default: // "pending"
		return phaseWaiting
	}
}

// reservationPhase maps a capacity reservation state.
func reservationPhase(state string) landingPhase {
	switch state {
	case "active":
		return phaseLanded
	case "failed", "expired", "cancelled", "unsupported", "payment-failed":
		return phaseGone
	default: // "pending", "assessing", "delayed", ...
		return phaseWaiting
	}
}

// landingError is a failed landing as an error carrying EC2's reason code, so
// describeAttemptError renders it as "<class>: <code>: <message>".
type landingError struct{ code, msg string }

func (e *landingError) Error() string {
	if e.code == "" {
		return e.msg
	}
	return e.code + ": " + e.msg
}
func (e *landingError) ErrorCode() string             { return e.code }
func (e *landingError) ErrorMessage() string          { return e.msg }
func (e *landingError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// landingFailureKind classifies a landing that did not land. Lack of capacity
// (the instance terminated with an InsufficientInstanceCapacity reason, a Spot
// instance reclaimed before it ran, or a reservation that failed) is a capacity
// failure, retried without a cap like a refused RunInstances. Anything else (a
// fail-closed bootstrap's shutdown, a timeout) is unclassified, so it counts
// toward MaxConsecutiveFailures: a launch that dies the same way every time
// stops after ten, rather than paying for a launch every cycle until expiry.
func landingFailureKind(reservation bool, action string, chk landingCheck) FailureKind {
	if action == ActionLandingTimedOut {
		return FailureUnknown
	}
	if reservation {
		if chk.State == "failed" {
			return FailureCapacity
		}
		return FailureUnknown
	}
	code := chk.ReasonCode
	if strings.Contains(code, "InsufficientInstanceCapacity") || strings.Contains(code, "InsufficientCapacity") ||
		strings.Contains(code, "SpotInstanceTermination") {
		return FailureCapacity
	}
	return FailureUnknown
}

// startLanding is the success half of a launch or hold attempt. If the resource
// has not landed yet and the poller can check it later, it records the watch as
// landing (pending_*, the lease marker, and the attempt's heartbeat, in one
// write) and returns true: the caller must NOT mark it matched. It returns false
// when the match should be recorded now: the resource is already running
// (active), it is not a spawn/hold, no landing check is wired, or the write
// failed. In that last case a premature match beats a second launch.
func (p *Poller) startLanding(ctx context.Context, w *Watch, m *MatchResult, now time.Time, summary *PollSummary) bool {
	var instanceID, reservationID string
	switch {
	case m.ActionTaken == "spawned" && m.InstanceID != "" && p.spawner != nil && p.spawner.describeInstance != nil:
		if m.InstanceState == "running" {
			m.LandedAt = now
			return false
		}
		instanceID = m.InstanceID
	case m.ActionTaken == "held" && m.ReservationID != "" && p.holder != nil && p.holder.describeReservation != nil:
		if m.ReservationState == "active" {
			m.LandedAt = now
			return false
		}
		reservationID = m.ReservationID
	default:
		return false
	}
	id := instanceID + reservationID
	markerExp := now.Add(landingMarkerTTL)
	if err := p.store.beginLanding(ctx, w.WatchID, instanceID, reservationID, m.Region, m, markerExp); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not record %s as landing (%v); recording the match now instead\n", w.WatchID, id, err)
		return false
	}
	w.PendingInstanceID, w.PendingReservationID, w.PendingRegion = instanceID, reservationID, m.Region
	since := now
	w.PendingSince = &since
	w.PendingMatch = m
	w.LeaseOwner, w.LeaseExpiresAt = landingMarker(id), markerExp
	w.noteAttempt(&attempt{}, now)
	summary.Landing++
	// Unconditional: for the hosted poller CloudWatch is the audience.
	fmt.Fprintf(os.Stderr, "Watch %s: %s %s in %s; waiting for it to land (checked each cycle, given up after %s)\n",
		w.WatchID, m.ActionTaken, id, m.Region, PendingLandingTimeout)
	return true
}

// pollLanding runs one landing cycle for a watch with a pending resource. It
// returns true when the watch is hunting again (the landing failed and the
// watch is still active), so the caller can go on to expire it if its deadline
// passed, or search and launch again in this same cycle.
func (p *Poller) pollLanding(ctx context.Context, w *Watch, summary *PollSummary) bool {
	now := time.Now().UTC()
	id := w.PendingResourceID()
	marker := landingMarker(id)

	// The #47 lease. The claim may take over this watch's own landing marker, but
	// not another live poller's lease.
	if p.leaseOwner != "" {
		if err := p.store.claimLandingLease(ctx, w.WatchID, p.leaseOwner, marker, now.Add(leaseTTL)); err != nil {
			if errors.Is(err, ErrLeaseHeld) {
				if p.verbose {
					fmt.Fprintf(os.Stderr, "Watch %s: lease held by another poller; skipping its landing check this cycle\n", w.WatchID)
				}
				summary.Landing++
				return false
			}
			fmt.Fprintf(os.Stderr, "Warning: could not claim lease on %s: %v\n", w.WatchID, err)
			return false
		}
	}

	expired := !w.ExpiresAt.IsZero() && now.After(w.ExpiresAt)
	age := now.Sub(w.PendingSinceTime())
	reservation := w.PendingReservationID != ""

	chk, err := p.checkLanding(ctx, w)
	if err != nil {
		msg := landingAttemptError(fmt.Sprintf("landing check for %s", id), err)
		fmt.Fprintf(os.Stderr, "Warning: watch %s: %s\n", w.WatchID, msg)
		if expired {
			// The state is unknown, so don't terminate what may be running. Let the
			// watch expire; a launched instance is bounded by its own TTL.
			return p.dropLanding(ctx, w, msg)
		}
		// Unknown is not a reason to launch again: stay landing.
		p.keepLanding(ctx, w, marker, now, msg, summary)
		return false
	}

	var phase landingPhase
	if reservation {
		phase = reservationPhase(chk.State)
	} else {
		phase = instancePhase(chk.State)
	}
	if chk.NotFound {
		phase = phaseWaiting
		if age >= landingNotFoundGrace {
			phase = phaseGone
			chk.State = "not-found"
			chk.ReasonMessage = "the resource no longer exists"
		}
	}

	switch phase {
	case phaseLanded:
		p.completeLanding(ctx, w, chk, now, summary)
		return false
	case phaseGone:
		return p.failLanding(ctx, w, chk, ActionLandingFailed, now, summary)
	}

	if phase == phaseWaiting && !expired && age < PendingLandingTimeout {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: %s still %s after %s\n", w.WatchID, id, dashIfBlank(chk.State), age.Round(time.Second))
		}
		p.keepLanding(ctx, w, marker, now, "", summary)
		return false
	}

	// Stuck, or the watch ran out of time first: give up on this resource.
	why := fmt.Sprintf("still %s after %s", dashIfBlank(chk.State), age.Round(time.Second))
	switch {
	case phase == phaseStopped:
		why = fmt.Sprintf("%s before it was seen running", chk.State)
		if r := strings.Join(nonEmpty(chk.ReasonCode, chk.ReasonMessage), ": "); r != "" {
			why += " (" + r + ")"
		}
	case expired:
		why = fmt.Sprintf("the watch expired while it was still %s", dashIfBlank(chk.State))
	}
	verb := "terminated"
	if reservation {
		verb = "cancelled"
	}
	if phase == phaseStopped {
		// A stopped instance never starts again on its own, so it cannot become a
		// second running instance: hunt again even if the terminate fails, and say
		// so (its volumes bill until it is terminated).
		note := "lagotto terminated it"
		if err := p.cancelPending(ctx, w); err != nil {
			note = "lagotto could not terminate it (" + oneLine(err.Error()) + "); terminate it with spawn terminate " + id
		}
		fmt.Fprintf(os.Stderr, "Watch %s: %s %s; %s\n", w.WatchID, id, why, note)
		chk.ReasonMessage = strings.Join(nonEmpty(chk.ReasonMessage, note), "; ")
		return p.failLanding(ctx, w, chk, ActionLandingFailed, now, summary)
	}
	if err := p.cancelPending(ctx, w); err != nil {
		msg := landingAttemptError(fmt.Sprintf("%s %s; giving up on it failed", id, why), err)
		fmt.Fprintf(os.Stderr, "Warning: watch %s: %s\n", w.WatchID, msg)
		if isAccessDenied(err) {
			fmt.Fprintf(os.Stderr, "  The poller's runtime role cannot terminate it; redeploy the poller (lagotto deploy) so its role has the #242 grant.\n")
		}
		if expired {
			return p.dropLanding(ctx, w, msg)
		}
		// Never launch again while this one might still land.
		p.keepLanding(ctx, w, marker, now, msg, summary)
		return false
	}
	fmt.Fprintf(os.Stderr, "Watch %s: %s %s; %s it\n", w.WatchID, id, why, verb)
	chk.ReasonMessage = fmt.Sprintf("%s; lagotto %s it", why, verb)
	return p.failLanding(ctx, w, chk, ActionLandingTimedOut, now, summary)
}

// landingAttemptError renders a landing problem caused by err as an unknown-class
// last_error, keeping our context ahead of the AWS code and message (plain
// describeAttemptError would keep only the AWS message).
func landingAttemptError(context string, err error) string {
	msg := err.Error()
	var ae smithy.APIError
	if errors.As(err, &ae) {
		msg = ae.ErrorMessage()
	}
	return describeAttemptError(FailureUnknown, &landingError{code: apiErrorCode(err), msg: context + ": " + msg})
}

func dashIfBlank(s string) string {
	if s == "" {
		return "pending"
	}
	return s
}

// checkLanding describes the pending resource.
func (p *Poller) checkLanding(ctx context.Context, w *Watch) (landingCheck, error) {
	if w.PendingInstanceID != "" {
		if p.spawner == nil || p.spawner.describeInstance == nil {
			return landingCheck{}, errors.New("this poller has no instance describer")
		}
		return p.spawner.describeInstance(ctx, w.PendingRegion, w.PendingInstanceID)
	}
	if p.holder == nil || p.holder.describeReservation == nil {
		return landingCheck{}, errors.New("this poller has no capacity reservation describer")
	}
	return p.holder.describeReservation(ctx, w.PendingRegion, w.PendingReservationID)
}

// cancelPending gives up on a stuck resource: spawn's terminate path for an
// instance (it cancels a Spot request first and releases a spawn-allocated
// Elastic IP; the poller's grant is gated on spawn:managed=true, #242), or a
// release for a reservation.
func (p *Poller) cancelPending(ctx context.Context, w *Watch) error {
	if w.PendingInstanceID != "" {
		if p.spawner == nil || p.spawner.terminateInstance == nil {
			return errors.New("this poller cannot terminate instances")
		}
		return p.spawner.terminateInstance(ctx, w.PendingRegion, w.PendingInstanceID)
	}
	if p.holder == nil {
		return errors.New("this poller cannot release capacity reservations")
	}
	_, err := p.holder.Release(ctx, w.PendingRegion, w.PendingReservationID)
	return err
}

// landingMatch is the match the landing resolves: the stored pending match, or
// (for a record without one) a minimal one rebuilt from the watch.
func landingMatch(w *Watch) *MatchResult {
	var m MatchResult
	if w.PendingMatch != nil {
		m = *w.PendingMatch
	} else {
		m = MatchResult{
			WatchID: w.WatchID, UserID: w.UserID, Region: w.PendingRegion,
			InstanceType: w.InstanceTypePattern, IsSpot: w.Spot, MatchedAt: w.PendingSinceTime(),
		}
	}
	m.InstanceID, m.ReservationID = w.PendingInstanceID, w.PendingReservationID
	if m.Region == "" {
		m.Region = w.PendingRegion
	}
	return &m
}

// completeLanding records a landed resource: the match (true AZ, landed_at,
// state) in the watch and history, the notification, and status matched.
func (p *Poller) completeLanding(ctx context.Context, w *Watch, chk landingCheck, now time.Time, summary *PollSummary) {
	reservation := w.PendingReservationID != ""
	m := landingMatch(w)
	if chk.AZ != "" {
		m.AvailabilityZone = chk.AZ
	}
	m.LandedAt = now
	if reservation {
		m.ReservationState, m.ActionTaken = chk.State, "held"
	} else {
		m.InstanceState, m.ActionTaken = chk.State, "spawned"
	}
	// A watch-group member wins its group at landing, or its launch is an
	// extra (#287). Decided before anything is recorded as a match.
	var group *WatchGroup
	groupWon := false
	if groupManaged(w) {
		won, ok := p.winGroup(ctx, w, m, now, summary)
		if !ok {
			// The group could not be written: stay landing and decide next cycle.
			p.keepLanding(ctx, w, landingMarker(w.PendingResourceID()), now, "", summary)
			return
		}
		if !won {
			return
		}
		groupWon, group = true, p.groups[w.GroupRef()]
	}
	w.clearLanding()
	if !reservation {
		p.armSporedCheck(w, m, chk.SporedSeen, now)
	}
	w.ConsecutiveFailures = 0
	// RecordMatch writes the whole record, so the pending fields and the marker
	// go with it. If it fails they are still there, and the next cycle retries.
	if err := p.store.RecordMatch(ctx, w, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record landing for %s: %v (will retry)\n", w.WatchID, err)
		return
	}
	fmt.Fprintf(os.Stderr, "Watch %s: %s landed (%s in %s)\n", w.WatchID, m.InstanceID+m.ReservationID, chk.State, m.AvailabilityZone)
	if notify := p.matchNotifier(); notify != nil && !groupWon {
		if err := notify(ctx, w, m); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: notification failed for %s: %v\n", w.WatchID, err)
		}
	}
	if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusMatched); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to update watch status for %s: %v\n", w.WatchID, err)
	}
	w.Status = StatusMatched
	summary.Launched++
	summary.Matches = append(summary.Matches, *m)
	if groupWon {
		// Cancel the siblings and send the group's one notification, which
		// replaces the member's own (#287).
		p.finishGroup(ctx, w, m, group, now, summary)
	}
}

// failLanding records a landing that did not land and returns the watch to
// hunting: it clears pending_* and the marker, sets last_error (the launch
// already counted the attempt), writes a history row, and either resets or
// bumps the unclassified-failure streak. At MaxConsecutiveFailures the watch
// fails, as for a launch that keeps failing. Returns true when the watch is
// hunting again.
func (p *Poller) failLanding(ctx context.Context, w *Watch, chk landingCheck, action string, now time.Time, summary *PollSummary) bool {
	reservation := w.PendingReservationID != ""
	kind := landingFailureKind(reservation, action, chk)
	reason := landingReason(chk)
	id := w.PendingResourceID()
	what := "instance " + id
	if reservation {
		what = "capacity reservation " + id
	}
	detail := fmt.Sprintf("%s %s before it landed", what, dashIfBlank(chk.State))
	if action == ActionLandingTimedOut {
		detail = fmt.Sprintf("%s did not land: %s", what, chk.ReasonMessage)
	} else if chk.ReasonMessage != "" {
		detail += ": " + chk.ReasonMessage
	}
	msg := describeAttemptError(kind, &landingError{code: chk.ReasonCode, msg: detail})

	n, err := p.store.endLanding(ctx, w.WatchID, w.pendingAttr(), id, msg, kind != FailureCapacity)
	if errors.Is(err, errLandingResolved) {
		return false
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s: could not clear landing %s: %v (will retry)\n", w.WatchID, id, err)
		return false
	}
	fmt.Fprintf(os.Stderr, "Watch %s: %s; hunting again\n", w.WatchID, msg)
	// A lost landing is not a win: free the group's slot so the group stays
	// armed (#287).
	p.releaseGroupSlot(ctx, w)

	m := landingMatch(w)
	if chk.AZ != "" {
		m.AvailabilityZone = chk.AZ
	}
	m.ActionTaken = action
	m.LandingReason = reason
	if action == ActionLandingTimedOut {
		m.LandingReason = chk.ReasonMessage
	}
	// LostAt is when this poller saw the landing fail (#255): MatchedAt stays the
	// launch time, and EC2 forgets a terminated instance after about an hour, so
	// the history row is the only lasting record of when and why it went.
	lostAt := now
	m.LostAt = &lostAt
	if reservation {
		m.ReservationState = chk.State
	} else {
		m.InstanceState = chk.State
	}
	if err := p.store.RecordHistory(ctx, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record landing failure for %s: %v\n", w.WatchID, err)
	}

	w.clearLanding()
	w.LastError, w.LastErrorAt = msg, now
	w.LastPolledAt = now

	// An instance EC2 killed in pending for lack of capacity says its AZ cannot
	// place launches right now: cool the AZ off (#254's API, reason
	// "early-death") so the relaunch below, and the next sweeps, try elsewhere.
	if earlyDeathCoolsOff(reservation, chk) {
		c := p.CoolOffAZ(ctx, w, m.Region, chk.AZ, "", "early-death")
		fmt.Fprintf(os.Stderr, "Watch %s: %s died in pending (%s); cooling off %s\n", w.WatchID, id, chk.ReasonCode, c.Describe())
	}

	if kind == FailureCapacity {
		w.ConsecutiveFailures = 0
		p.sendLost(ctx, w, m, false)
		return true
	}
	if n <= 0 {
		n = w.ConsecutiveFailures + 1
	}
	w.ConsecutiveFailures = n
	if n < MaxConsecutiveFailures {
		p.sendLost(ctx, w, m, false)
		return true
	}
	fmt.Fprintf(os.Stderr, "Watch %s: %d consecutive launches failed to land; stopping watch\n", w.WatchID, n)
	w.Status = StatusFailed
	p.sendLost(ctx, w, m, true)
	if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusFailed); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to update watch status for %s: %v\n", w.WatchID, err)
	}
	w.Status = StatusFailed
	summary.Failed++
	return false
}

// sendLost tells the watch's channels that a launch or hold was lost before
// it landed (#255): which resource, the state EC2 reported, its reason, and
// whether the watch is hunting again or stopped. Every loss is sent, not only
// the one that stops the watch: a launched instance that disappears is the
// thing a person waiting on an unattended hunt most needs to hear about.
func (p *Poller) sendLost(ctx context.Context, w *Watch, m *MatchResult, stopped bool) {
	notify := p.lostNotifier()
	if notify == nil {
		return
	}
	if err := notify(ctx, w, m, stopped); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: lost-launch notification failed for %s: %v\n", w.WatchID, err)
	}
}

// landingReason is the history reason for a failed landing: EC2's StateReason
// code and message, then its StateTransitionReason (which carries EC2's own
// timestamp) when it says something the StateReason does not.
func landingReason(chk landingCheck) string {
	parts := nonEmpty(chk.ReasonCode, chk.ReasonMessage)
	// EC2 often repeats the code at the start of the message.
	if len(parts) == 2 && strings.HasPrefix(parts[1], parts[0]) {
		parts = parts[1:]
	}
	reason := strings.Join(parts, ": ")
	if tr := strings.TrimSpace(chk.TransitionReason); tr != "" && !strings.Contains(reason, tr) {
		if reason == "" {
			reason = tr
		} else {
			reason += " (transition: " + tr + ")"
		}
	}
	return oneLine(strings.TrimSpace(reason))
}

// earlyDeathCoolsOff reports whether a failed landing should cool its AZ off
// (#254/#255): an instance (not a reservation) in a known AZ that EC2
// terminated with an insufficient-capacity reason. A user- or
// instance-initiated shutdown, a Spot reclaim, a timeout, a stop and an
// internal error are not evidence that the AZ cannot place launches.
func earlyDeathCoolsOff(reservation bool, chk landingCheck) bool {
	if reservation || chk.AZ == "" || chk.NotFound || instancePhase(chk.State) != phaseGone {
		return false
	}
	code := chk.ReasonCode
	return strings.Contains(code, "InsufficientInstanceCapacity") || strings.Contains(code, "InsufficientCapacity")
}

func nonEmpty(ss ...string) []string {
	out := ss[:0:0]
	for _, s := range ss {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// keepLanding records a cycle that left the watch landing: it re-arms the lease
// marker, touches last_polled_at, and records msg (if any) as last_error.
func (p *Poller) keepLanding(ctx context.Context, w *Watch, marker string, now time.Time, msg string, summary *PollSummary) {
	summary.Landing++
	exp := now.Add(landingMarkerTTL)
	if err := p.store.refreshLanding(ctx, w.WatchID, w.pendingAttr(), w.PendingResourceID(), marker, exp, msg); err != nil {
		if !errors.Is(err, errLandingResolved) {
			fmt.Fprintf(os.Stderr, "Warning: watch %s: could not refresh landing: %v\n", w.WatchID, err)
		}
		return
	}
	w.LeaseOwner, w.LeaseExpiresAt, w.LastPolledAt = marker, exp, now
	if msg != "" {
		w.LastError, w.LastErrorAt = msg, now
	}
}

// dropLanding clears the landing without judging it (its state is unknown, or
// it could not be given up on) so that an expired watch can expire. Returns
// true when the watch should go on to its expiry.
func (p *Poller) dropLanding(ctx context.Context, w *Watch, msg string) bool {
	if _, err := p.store.endLanding(ctx, w.WatchID, w.pendingAttr(), w.PendingResourceID(), msg, false); err != nil {
		if !errors.Is(err, errLandingResolved) {
			fmt.Fprintf(os.Stderr, "Warning: watch %s: could not clear landing: %v\n", w.WatchID, err)
		}
		return false
	}
	fmt.Fprintf(os.Stderr, "Watch %s: expiring with %s unresolved; it is bounded by its own TTL\n", w.WatchID, w.PendingResourceID())
	p.releaseGroupSlot(ctx, w)
	w.clearLanding()
	w.LastError, w.LastErrorAt = msg, time.Now().UTC()
	return true
}

// ── describes ──

// instanceDescribeAPI is the slice of EC2 the instance landing check uses.
type instanceDescribeAPI interface {
	DescribeInstances(ctx context.Context, in *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
}

// newInstanceDescriber returns the production landing check for instances: one
// DescribeInstances in the instance's region.
func newInstanceDescriber(cfg aws.Config) func(ctx context.Context, region, instanceID string) (landingCheck, error) {
	return func(ctx context.Context, region, instanceID string) (landingCheck, error) {
		return describeInstanceState(ctx, ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.Region = region }), instanceID)
	}
}

func describeInstanceState(ctx context.Context, api instanceDescribeAPI, instanceID string) (landingCheck, error) {
	out, err := api.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	if err != nil {
		if apiErrorCode(err) == "InvalidInstanceID.NotFound" {
			return landingCheck{NotFound: true}, nil
		}
		return landingCheck{}, err
	}
	for _, r := range out.Reservations {
		for _, in := range r.Instances {
			if aws.ToString(in.InstanceId) != instanceID {
				continue
			}
			var c landingCheck
			if in.State != nil {
				c.State = string(in.State.Name)
			}
			if in.Placement != nil {
				c.AZ = aws.ToString(in.Placement.AvailabilityZone)
			}
			if in.StateReason != nil {
				c.ReasonCode = aws.ToString(in.StateReason.Code)
				c.ReasonMessage = aws.ToString(in.StateReason.Message)
			}
			c.TransitionReason = aws.ToString(in.StateTransitionReason)
			c.SporedSeen = hasSporedTag(in.Tags)
			return c, nil
		}
	}
	return landingCheck{NotFound: true}, nil
}

// describeReservationState is the landing check for a capacity reservation.
func describeReservationState(ctx context.Context, api holderReleaseAPI, reservationID string) (landingCheck, error) {
	out, err := api.DescribeCapacityReservations(ctx, &ec2.DescribeCapacityReservationsInput{
		CapacityReservationIds: []string{reservationID},
	})
	if err != nil {
		if isReservationNotFound(err) {
			return landingCheck{NotFound: true}, nil
		}
		return landingCheck{}, err
	}
	if out == nil || len(out.CapacityReservations) == 0 {
		return landingCheck{NotFound: true}, nil
	}
	r := out.CapacityReservations[0]
	return landingCheck{State: string(r.State), AZ: aws.ToString(r.AvailabilityZone)}, nil
}

// ── store ──

// pendingAttrs is every attribute a landing sets, removed when it resolves.
const pendingRemove = "pending_instance_id, pending_reservation_id, pending_region, pending_since, pending_match, lease_owner, lease_expires_at"

// beginLanding records a launch (instanceID) or hold (reservationID) as landing,
// in the one UpdateItem the attempt makes: the pending_* fields, the lease
// marker, last_polled_at, and the attempt's heartbeat. It leaves the
// unclassified-failure streak alone: a launch is not a success until it lands
// (completeLanding resets it), so launches that keep dying the same way still
// reach MaxConsecutiveFailures.
func (s *Store) beginLanding(ctx context.Context, watchID, instanceID, reservationID, region string, m *MatchResult, markerExp time.Time) error {
	attr, id := "pending_instance_id", instanceID
	if reservationID != "" {
		attr, id = "pending_reservation_id", reservationID
	}
	pm, err := attributevalue.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal pending match: %w", err)
	}
	values := map[string]types.AttributeValue{
		":now":    &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		":pid":    &types.AttributeValueMemberS{Value: id},
		":preg":   &types.AttributeValueMemberS{Value: region},
		":pm":     pm,
		":marker": &types.AttributeValueMemberS{Value: landingMarker(id)},
		":mexp":   &types.AttributeValueMemberS{Value: markerExp.UTC().Format(time.RFC3339)},
	}
	set := []string{
		attr + " = :pid", "pending_region = :preg", "pending_since = :now", "pending_match = :pm",
		"last_polled_at = :now", "lease_owner = :marker", "lease_expires_at = :mexp",
	}
	set, add := withAttempt(set, nil, values, &attempt{})
	if _, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		ConditionExpression:       aws.String("attribute_exists(#st)"),
		UpdateExpression:          aws.String(updateExpr(nil, set, add)),
		ExpressionAttributeNames:  map[string]string{"#st": "status"},
		ExpressionAttributeValues: values,
	}); err != nil {
		return fmt.Errorf("begin landing: %w", err)
	}
	return nil
}

// claimLandingLease is ClaimLease for a landing watch: it may also take over
// the watch's own landing marker, which an older poller's ClaimLease cannot.
func (s *Store) claimLandingLease(ctx context.Context, watchID, owner, marker string, expiresAt time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		ConditionExpression: aws.String("attribute_not_exists(lease_owner) OR lease_owner = :me OR lease_owner = :marker OR lease_expires_at < :now"),
		UpdateExpression:    aws.String("SET lease_owner = :me, lease_expires_at = :exp"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me":     &types.AttributeValueMemberS{Value: owner},
			":marker": &types.AttributeValueMemberS{Value: marker},
			":exp":    &types.AttributeValueMemberS{Value: expiresAt.UTC().Format(time.RFC3339)},
			":now":    &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		},
	})
	if err != nil {
		var cond *types.ConditionalCheckFailedException
		if errors.As(err, &cond) {
			return ErrLeaseHeld
		}
		return fmt.Errorf("claim landing lease on %q: %w", watchID, err)
	}
	return nil
}

// refreshLanding closes a cycle that left the watch landing: it re-arms the
// marker (replacing this poller's lease), touches last_polled_at, and sets
// last_error when lastErr is non-empty. Conditional on the watch still landing
// the same resource, so it never resurrects a landing another poller resolved.
func (s *Store) refreshLanding(ctx context.Context, watchID, attr, id, marker string, markerExp time.Time, lastErr string) error {
	values := map[string]types.AttributeValue{
		":now":    &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		":pid":    &types.AttributeValueMemberS{Value: id},
		":marker": &types.AttributeValueMemberS{Value: marker},
		":mexp":   &types.AttributeValueMemberS{Value: markerExp.UTC().Format(time.RFC3339)},
	}
	set := []string{"last_polled_at = :now", "lease_owner = :marker", "lease_expires_at = :mexp"}
	if lastErr != "" {
		set = append(set, "last_error = :lasterr", "last_error_at = :now")
		values[":lasterr"] = &types.AttributeValueMemberS{Value: lastErr}
	}
	return s.landingUpdate(ctx, watchID, attr, nil, set, nil, values, nil)
}

// endLanding clears a landing and returns the watch to hunting, in one write:
// it removes pending_* and the marker, touches last_polled_at, records lastErr
// as last_error, and either bumps the unclassified-failure streak (countFailure)
// or resets it. Returns the new streak when it was bumped. Conditional on the
// watch still landing the same resource (errLandingResolved otherwise).
func (s *Store) endLanding(ctx context.Context, watchID, attr, id, lastErr string, countFailure bool) (int, error) {
	values := map[string]types.AttributeValue{
		":now": &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
		":pid": &types.AttributeValueMemberS{Value: id},
	}
	set := []string{"last_polled_at = :now"}
	if lastErr != "" {
		set = append(set, "last_error = :lasterr", "last_error_at = :now")
		values[":lasterr"] = &types.AttributeValueMemberS{Value: lastErr}
	}
	var add []string
	if countFailure {
		add = []string{"consecutive_failures :one"}
		values[":one"] = &types.AttributeValueMemberN{Value: "1"}
	} else {
		set = append(set, "consecutive_failures = :zero")
		values[":zero"] = &types.AttributeValueMemberN{Value: "0"}
	}
	var updated struct {
		ConsecutiveFailures int `dynamodbav:"consecutive_failures"`
	}
	if err := s.landingUpdate(ctx, watchID, attr, []string{pendingRemove}, set, add, values, &updated); err != nil {
		return 0, err
	}
	return updated.ConsecutiveFailures, nil
}

// landingUpdate runs one landing UpdateItem conditional on attr = :pid,
// decoding the updated attributes into out when it is non-nil.
func (s *Store) landingUpdate(ctx context.Context, watchID, attr string, remove, set, add []string,
	values map[string]types.AttributeValue, out any) error {
	in := &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		ConditionExpression:       aws.String("#pend = :pid"),
		UpdateExpression:          aws.String(updateExpr(remove, set, add)),
		ExpressionAttributeNames:  map[string]string{"#pend": attr},
		ExpressionAttributeValues: values,
	}
	if out != nil {
		in.ReturnValues = types.ReturnValueUpdatedNew
	}
	res, err := s.client.UpdateItem(ctx, in)
	if err != nil {
		var cond *types.ConditionalCheckFailedException
		if errors.As(err, &cond) {
			return errLandingResolved
		}
		return fmt.Errorf("update landing: %w", err)
	}
	if out != nil && res != nil {
		if err := attributevalue.UnmarshalMap(res.Attributes, out); err != nil {
			return fmt.Errorf("unmarshal landing update: %w", err)
		}
	}
	return nil
}
