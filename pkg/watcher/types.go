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
	// StatusFailed means a launch/hold hit a terminal error (bad AMI/IAM,
	// exhausted quota) that retrying cannot resolve. The watch stops polling.
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
	// LeaseOwner / LeaseExpiresAt guard the double-poller race (#47): before a
	// poller acts on a match it claims a short lease, so two daemons (or a daemon
	// + the hosted Lambda) can't both fire the same watch. A lease past its expiry
	// is stale and can be re-claimed (a crashed poller never blocks the watch).
	LeaseOwner     string    `json:"lease_owner,omitempty" dynamodbav:"lease_owner,omitempty"`
	LeaseExpiresAt time.Time `json:"lease_expires_at,omitempty" dynamodbav:"lease_expires_at,omitempty"`
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
	CandidateAZs  []string  `json:"candidate_azs,omitempty" dynamodbav:"candidate_azs,omitempty"`
	InstanceType  string    `json:"instance_type" dynamodbav:"instance_type"`
	Price         float64   `json:"price" dynamodbav:"price"`
	IsSpot        bool      `json:"is_spot" dynamodbav:"is_spot"`
	MatchedAt     time.Time `json:"matched_at" dynamodbav:"matched_at"`
	ActionTaken   string    `json:"action_taken" dynamodbav:"action_taken"`
	InstanceID    string    `json:"instance_id,omitempty" dynamodbav:"instance_id,omitempty"`
	ReservationID string    `json:"reservation_id,omitempty" dynamodbav:"reservation_id,omitempty"`
	TTLTimestamp  int64     `json:"ttl_timestamp" dynamodbav:"ttl_timestamp"`
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
}
