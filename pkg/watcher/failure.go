package watcher

import (
	"strings"

	"github.com/spore-host/lagotto/pkg/failure"
)

// The launch-failure classifier now lives in the dependency-free leaf
// github.com/spore-host/lagotto/pkg/failure (lagotto#75), so a stateless consumer
// can key a retry loop on it without importing pkg/watcher's AWS SDK tree. These
// aliases keep every existing pkg/watcher caller (and external caller of
// watcher.ClassifyFailure) unchanged.

// FailureKind classifies why a launch/hold attempt failed. Alias of
// [failure.FailureKind].
type FailureKind = failure.FailureKind

const (
	// FailureNone means no failure.
	FailureNone = failure.FailureNone
	// FailureCapacity means AWS had no capacity for the type/AZ right now.
	FailureCapacity = failure.FailureCapacity
	// FailureTerminal means the attempt can never succeed as configured.
	FailureTerminal = failure.FailureTerminal
	// FailureUnknown means an unrecognized-but-plausibly-transient error: retried
	// like capacity, but counted toward the per-watch consecutive-failure cap.
	FailureUnknown = failure.FailureUnknown
)

// ClassifyFailure inspects a spawn/hold error and decides whether to retry.
// Alias of [failure.ClassifyFailure].
var ClassifyFailure = failure.ClassifyFailure

// failureLabel returns a short human label for log lines.
func failureLabel(k FailureKind) string {
	return failure.Label(k)
}

// ClassifySageMakerFailure decides what to do with a SageMaker job attempt.
// Unlike EC2 (synchronous InsufficientInstanceCapacity), SageMaker capacity
// failure is asynchronous: CreateTrainingJob succeeds, and the job later reaches
// status Failed with a FailureReason containing "CapacityError: ...". This
// classifier maps the job status + FailureReason to the same FailureKind the
// poller already acts on:
//
//   - terminal job status (Failed) with a CapacityError reason -> FailureCapacity
//     (no capacity right now; retry next cycle).
//   - terminal Failed with any other reason -> FailureTerminal (bad config/IAM/
//     image; retrying won't help).
//   - InProgress/Completed/Stopping/Stopped -> FailureNone (the job launched;
//     capacity exists — success).
//
// This stays in pkg/watcher (not the leaf): it's SageMaker-domain, keyed on job
// status strings rather than a launch error, and only the poller consumes it.
func ClassifySageMakerFailure(jobStatus, failureReason string) FailureKind {
	switch jobStatus {
	case "Failed":
		if strings.Contains(failureReason, "CapacityError") ||
			strings.Contains(failureReason, "Unable to provision requested ML compute capacity") {
			return FailureCapacity
		}
		return FailureTerminal
	default:
		// InProgress, Completed, Stopping, Stopped — instances were provisioned,
		// so capacity exists. Not a failure.
		return FailureNone
	}
}
