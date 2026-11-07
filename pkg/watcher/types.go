// Package watcher provides EC2 capacity watching, polling, and matching.
package watcher

import (
	"time"
)

// ActionMode defines what happens when capacity is found.
type ActionMode string

const (
	// ActionNotify sends a notification only.
	ActionNotify ActionMode = "notify"
	// ActionSpawn sends a notification and auto-launches an instance.
	ActionSpawn ActionMode = "spawn"
	// ActionHold creates an On-Demand Capacity Reservation to hold capacity.
	ActionHold ActionMode = "hold"
)

// WatchStatus represents the lifecycle state of a watch.
type WatchStatus string

const (
	// StatusActive means the watch is being polled.
	StatusActive WatchStatus = "active"
	// StatusMatched means capacity was found and action was taken.
	StatusMatched WatchStatus = "matched"
	// StatusExpired means the watch TTL elapsed without a match.
	StatusExpired WatchStatus = "expired"
	// StatusCancelled means the user cancelled the watch.
	StatusCancelled WatchStatus = "cancelled"
	// StatusFailed means a launch/hold hit a terminal error (bad AMI/IAM) that
	// retrying cannot resolve, or that EC2 refused EVERY candidate on quota
	// (#238; a single quota refusal only skips that candidate). The watch stops
	// polling.
	StatusFailed WatchStatus = "failed"
	// StatusCompleted means a goal-driven fleet watch's completion condition
	// (--until) became true: the work is done, so the watch stops maintaining the
	// fleet and retires. Distinct from StatusMatched (a single-shot watch that
	// fired its action once) — a fleet watch relaunches toward its goal across
	// many polls and only reaches Completed when the external condition holds (#70).
	StatusCompleted WatchStatus = "completed"
)

// MaxConsecutiveFailures caps consecutive *unclassified* launch/hold failures
// (FailureUnknown) before a watch is stopped as failed. It's a poll-count
// backstop — distinct from genuine capacity waits (FailureCapacity), which are
// uncapped — so a persistently-broken watch (bad IAM/region, sustained non-AWS
// fault) doesn't burn a launch attempt every poll for its whole TTL (#41).
const MaxConsecutiveFailures = 10

// QuotaCapReprobeInterval is how long a fleet watch that has hit its EC2 vCPU
// quota ceiling waits before attempting another top-up (#153). Backoff applies
// ONLY to attempts we already know will fail (running is at or above the known
// ceiling); a worker lost below the ceiling is replaced on the very next cycle.
//
// At the hosted poller's 5-minute cadence, a 48h watch capped one worker short
// makes ~576 doomed RunInstances calls under the old behavior; at 30 minutes
// that's ~96 — still frequent enough that a granted quota increase is picked up
// within half an hour with no user action.
const QuotaCapReprobeInterval = 30 * time.Minute

// Watch represents a user's request to monitor for instance capacity.
type Watch struct {
	WatchID string `json:"watch_id" dynamodbav:"watch_id"`
	UserID  string `json:"user_id" dynamodbav:"user_id"`
	// Project is an optional grouping label (set at watch-create via --project or
	// $LAGOTTO_PROJECT) so a local `poll --daemon --project X` services only its
	// own project's watches in a shared account, instead of every watch (#47).
	Project             string      `json:"project,omitempty" dynamodbav:"project,omitempty"`
	Status              WatchStatus `json:"status" dynamodbav:"status"`
	Service             Service     `json:"service,omitempty" dynamodbav:"service,omitempty"`
	InstanceTypePattern string      `json:"instance_type_pattern" dynamodbav:"instance_type_pattern"`
	Regions             []string    `json:"regions" dynamodbav:"regions"`
	// AvailabilityZones optionally pins/orders which AZs within the watched
	// region(s) are eligible (e.g. ["us-west-2b","us-west-2c"]). Empty = all AZs
	// in the region. Widening across AZs is free (same-region data locality), so
	// the default is "every AZ"; this lever only narrows or reorders (#34).
	AvailabilityZones []string        `json:"availability_zones,omitempty" dynamodbav:"availability_zones,omitempty"`
	Spot              bool            `json:"spot" dynamodbav:"spot"`
	MaxPrice          float64         `json:"max_price,omitempty" dynamodbav:"max_price,omitempty"`
	Action            ActionMode      `json:"action" dynamodbav:"action"`
	NotifyChannels    []NotifyChannel `json:"notify_channels,omitempty" dynamodbav:"notify_channels,omitempty"`
	LaunchConfigJSON  []byte          `json:"launch_config_json,omitempty" dynamodbav:"launch_config_json,omitempty"`
	// DesiredCount, when > 0, makes this a GOAL-DRIVEN fleet watch (#70): instead
	// of firing --action once and retiring, the supervisor maintains ~DesiredCount
	// running workers (relaunching toward the goal, even from zero) until
	// CompletionCondition holds. 0 = legacy single-shot behavior. Requires
	// --action spawn.
	DesiredCount int `json:"desired_count,omitempty" dynamodbav:"desired_count,omitempty"`
	// CompletionCondition is the --until spec (see ParseCondition: s3-empty /
	// http-200 / shell) evaluated each poll; when it's Done the fleet watch
	// retires as StatusCompleted. Only meaningful when DesiredCount > 0.
	CompletionCondition string `json:"completion_condition,omitempty" dynamodbav:"completion_condition,omitempty"`
	// SageMakerJobJSON is the user's SageMaker job definition (training/processing
	// job spec), submitted on each attempt for a --service sagemaker watch.
	// Symmetric to LaunchConfigJSON for EC2.
	SageMakerJobJSON []byte `json:"sagemaker_job_json,omitempty" dynamodbav:"sagemaker_job_json,omitempty"`
	// SageMakerJobName tracks the name of an in-flight submitted SageMaker job
	// between poll cycles (capacity failure is async — we submit, then check the
	// job's status on a later cycle). Empty means no job is currently in flight.
	SageMakerJobName string       `json:"sagemaker_job_name,omitempty" dynamodbav:"sagemaker_job_name,omitempty"`
	CreatedAt        time.Time    `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt        time.Time    `json:"updated_at" dynamodbav:"updated_at"`
	ExpiresAt        time.Time    `json:"expires_at" dynamodbav:"expires_at"`
	TTLTimestamp     int64        `json:"ttl_timestamp" dynamodbav:"ttl_timestamp"`
	LastPolledAt     time.Time    `json:"last_polled_at,omitempty" dynamodbav:"last_polled_at,omitempty"`
	MatchCount       int          `json:"match_count" dynamodbav:"match_count"`
	LastMatch        *MatchResult `json:"last_match,omitempty" dynamodbav:"last_match,omitempty"`
	// ConsecutiveFailures counts back-to-back unclassified launch/hold failures
	// (FailureUnknown) on this watch; it's incremented per failing poll and reset
	// to zero on a capacity failure or a successful launch. Once it reaches
	// MaxConsecutiveFailures the watch is stopped as failed (#41). Genuine
	// capacity failures never touch it, so a real capacity wait stays uncapped.
	ConsecutiveFailures int `json:"consecutive_failures,omitempty" dynamodbav:"consecutive_failures,omitempty"`
	// Heartbeat (#239 phase 1): how hard the watch has been trying, and what last
	// went wrong, so a long-lived hunt is visibly alive. They ride on the write
	// each poll cycle already makes (no extra DynamoDB writes), and are omitempty,
	// so pre-#239 records read back as zero and a pre-#239 poller ignores them.
	//
	// Attempts counts action attempts (a launch, hold, SageMaker submit/check or
	// notify), not poll cycles: a cycle with no candidate, or a skipped one, makes
	// no attempt. It is bumped with an atomic ADD on the retrying paths.
	Attempts      int       `json:"attempts,omitempty" dynamodbav:"attempts,omitempty"`
	LastAttemptAt time.Time `json:"last_attempt_at,omitzero" dynamodbav:"last_attempt_at,omitempty"`
	// LastError is the most recent failed attempt, as "<class>: <AWS code>:
	// <message>" (class is one of capacity, quota, terminal, unknown), truncated
	// to MaxLastErrorBytes. A later success leaves it in place; LastErrorAt says
	// when it happened.
	LastError   string    `json:"last_error,omitempty" dynamodbav:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitzero" dynamodbav:"last_error_at,omitempty"`
	// LastPollError is stamped when the capacity search itself (truffle) fails,
	// as "search: <message>". That cycle deliberately does NOT touch
	// LastPolledAt, so a search that keeps failing shows as a stale LAST POLL.
	LastPollError   string    `json:"last_poll_error,omitempty" dynamodbav:"last_poll_error,omitempty"`
	LastPollErrorAt time.Time `json:"last_poll_error_at,omitzero" dynamodbav:"last_poll_error_at,omitempty"`
	// QuotaCappedCount / QuotaCappedAt / QuotaCapReason record that a fleet
	// watch has hit its EC2 vCPU quota ceiling (#153). These are PERSISTED (unlike
	// the derived duration fields below) precisely because the hosted poller is
	// stateless across invocations: without them there is nowhere to remember
	// "already reported this", so the user would be notified every 5 minutes for
	// the whole TTL — or, as before the fix, never.
	//
	// QuotaCappedCount is the ceiling M in "capped at M/N" — the number of workers
	// that were running when the quota refused the next launch. 0 means not
	// capped, so it doubles as the already-reported flag AND the suppression
	// threshold (a cap at a different level overwrites and re-notifies). All three
	// are omitempty, so pre-#153 watch records read back as zero — no migration.
	QuotaCappedCount int `json:"quota_capped_count,omitempty" dynamodbav:"quota_capped_count,omitempty"`
	// QuotaCappedAt anchors the QuotaCapReprobeInterval backoff.
	QuotaCappedAt time.Time `json:"quota_capped_at,omitempty" dynamodbav:"quota_capped_at,omitempty"`
	// QuotaCapReason is the human explanation (family, numbers when known, and the
	// quota-increase hint) shown by `lagotto status`.
	QuotaCapReason string `json:"quota_cap_reason,omitempty" dynamodbav:"quota_cap_reason,omitempty"`
	// QuotaInfeasible lists the single-shot candidates (instance type, region)
	// that EC2 refused because of an account quota (#238). A live entry (younger
	// than QuotaCapReprobeInterval) makes the poller skip that candidate and try
	// the next; the watch fails only when every candidate has a live entry.
	// omitempty, so pre-#238 records read back empty, and a pre-#238 poller
	// ignores it (and keeps its old terminal-on-quota behaviour). When the watch
	// does fail this way, QuotaCapReason holds the message listing them all.
	QuotaInfeasible []QuotaInfeasibility `json:"quota_infeasible,omitempty" dynamodbav:"quota_infeasible,omitempty"`
	// Two-phase landing (#239 phase 2). A spawn watch whose RunInstances
	// returned, or a hold watch whose reservation was created, is not yet
	// matched: the instance is usually still "pending" and can still be
	// terminated for lack of capacity, and the reservation can still fail. Until
	// a later cycle sees it "running" (or the reservation "active"), the watch
	// stays ACTIVE with these set, and the poller checks the resource each cycle
	// instead of launching again. Exactly one of PendingInstanceID and
	// PendingReservationID is set while landing; neither on any other watch.
	//
	// All omitempty: a pre-#239 record has none, and is polled exactly as before.
	// A pre-#239 poller ignores them, which is why a landing watch also carries
	// a lease marker (LandingLeasePrefix) in LeaseOwner, which every poller since
	// #47 already refuses to override. See landing.go.
	PendingInstanceID    string `json:"pending_instance_id,omitempty" dynamodbav:"pending_instance_id,omitempty"`
	PendingReservationID string `json:"pending_reservation_id,omitempty" dynamodbav:"pending_reservation_id,omitempty"`
	PendingRegion        string `json:"pending_region,omitempty" dynamodbav:"pending_region,omitempty"`
	// PendingSince is a pointer because the DynamoDB encoder writes a zero
	// time.Time even with omitempty; nil keeps it off every non-landing record.
	PendingSince *time.Time `json:"pending_since,omitempty" dynamodbav:"pending_since,omitempty"`
	// PendingMatch is the match the pending launch/hold came from, so the history
	// row written when it lands (or fails to) describes the real candidate.
	PendingMatch *MatchResult `json:"pending_match,omitempty" dynamodbav:"pending_match,omitempty"`
	// LeaseOwner / LeaseExpiresAt guard the double-poller race (#47): before a
	// poller acts on a match it claims a short lease, so two daemons (or a daemon
	// + the hosted Lambda) can't both fire the same watch. A lease past its expiry
	// is stale and can be re-claimed (a crashed poller never blocks the watch).
	LeaseOwner     string    `json:"lease_owner,omitempty" dynamodbav:"lease_owner,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty" dynamodbav:"lease_expires_at,omitempty"`
	// WaitToAcquireSeconds / TimeToGiveUpSeconds are DERIVED, display-only durations
	// (#139) filled by ComputeDurations at read time — never persisted (dynamodbav
	// "-"). wait-to-acquire = matched_at − created_at for a watch that matched/
	// spawned; time-to-give-up = ended_at (last update) − created_at for a watch
	// that ended failed OR expired without acquiring (#161). Both are nil (omitted)
	// when they don't apply.
	WaitToAcquireSeconds *float64 `json:"wait_to_acquire_seconds,omitempty" dynamodbav:"-"`
	TimeToGiveUpSeconds  *float64 `json:"time_to_give_up_seconds,omitempty" dynamodbav:"-"`
}

// MatchResult records a capacity match event.
type MatchResult struct {
	WatchID          string  `json:"watch_id" dynamodbav:"watch_id"`
	UserID           string  `json:"user_id" dynamodbav:"user_id"`
	Service          Service `json:"service,omitempty" dynamodbav:"service,omitempty"`
	Region           string  `json:"region" dynamodbav:"region"`
	AvailabilityZone string  `json:"availability_zone" dynamodbav:"availability_zone"`
	// CandidateAZs are all AZs (in preference order) where this type was offered
	// this poll, so the spawner can retry the next AZ on InsufficientInstance
	// Capacity within a cycle. AvailabilityZone is CandidateAZs[0] (#34).
	CandidateAZs []string `json:"candidate_azs,omitempty" dynamodbav:"candidate_azs,omitempty"`
	InstanceType string   `json:"instance_type" dynamodbav:"instance_type"`
	// VCPUs is the matched type's vCPU count, carried from truffle's
	// DescribeInstanceTypes result — the AUTHORITATIVE figure, not a size-suffix
	// estimate. The quota ceiling is expressed in vCPUs per family, so the poller
	// needs it to say how much quota a refused launch was asking for (#153).
	VCPUs         int32     `json:"vcpus,omitempty" dynamodbav:"vcpus,omitempty"`
	Price         float64   `json:"price" dynamodbav:"price"`
	IsSpot        bool      `json:"is_spot" dynamodbav:"is_spot"`
	MatchedAt     time.Time `json:"matched_at" dynamodbav:"matched_at"`
	ActionTaken   string    `json:"action_taken" dynamodbav:"action_taken"`
	InstanceID    string    `json:"instance_id,omitempty" dynamodbav:"instance_id,omitempty"`
	ReservationID string    `json:"reservation_id,omitempty" dynamodbav:"reservation_id,omitempty"`
	// Landing (#239 phase 2). LandedAt is when the poller saw the instance
	// "running" (or the reservation "active"); MatchedAt stays the launch time,
	// the history sort key. InstanceState / ReservationState is the state the
	// poller saw at that point, and LandingReason says why a landing failed
	// (EC2's state reason, e.g. "Server.InsufficientInstanceCapacity: ...").
	LandedAt         time.Time `json:"landed_at,omitzero" dynamodbav:"landed_at,omitempty"`
	InstanceState    string    `json:"instance_state,omitempty" dynamodbav:"instance_state,omitempty"`
	ReservationState string    `json:"reservation_state,omitempty" dynamodbav:"reservation_state,omitempty"`
	LandingReason    string    `json:"landing_reason,omitempty" dynamodbav:"landing_reason,omitempty"`
	// QuotaDetail explains a quota_infeasible row (#238): the error code, the
	// vCPUs the type needs, and the quota numbers when known. Empty otherwise.
	QuotaDetail  string `json:"quota_detail,omitempty" dynamodbav:"quota_detail,omitempty"`
	TTLTimestamp int64  `json:"ttl_timestamp" dynamodbav:"ttl_timestamp"`
	// WaitToAcquireSeconds is the DERIVED time from the parent watch's creation to
	// this match, in seconds (#139). Display-only, filled by the history command
	// from the watch's created_at; never persisted (dynamodbav "-").
	WaitToAcquireSeconds *float64 `json:"wait_to_acquire_seconds,omitempty" dynamodbav:"-"`
}

// clone returns a shallow copy of the match, so each worker in a fleet top-up
// gets its own MatchResult for Spawn to stamp with a distinct InstanceID/AZ
// (#70). CandidateAZs is shared read-only (Spawn only reads it).
func (m *MatchResult) clone() *MatchResult {
	c := *m
	return &c
}

// NotifyChannel specifies how to reach the user on a match.
type NotifyChannel struct {
	Type   string `json:"type" dynamodbav:"type"`     // "email", "webhook", "sns"
	Target string `json:"target" dynamodbav:"target"` // address, URL, or ARN
	// Subscription is an email channel's delivery state on the alerts topic
	// (#240), filled in by list/status/watch for display. Never stored.
	Subscription SubscriptionState `json:"subscription,omitempty" dynamodbav:"-"`
}
