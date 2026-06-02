package watcher

import (
	"errors"
	"strings"

	"github.com/aws/smithy-go"
)

// FailureKind classifies why a launch/hold attempt failed, which decides whether
// the watch should retry (capacity will likely free up) or stop (a launch will
// never succeed as configured).
type FailureKind int

const (
	// FailureNone means no failure.
	FailureNone FailureKind = iota
	// FailureCapacity means AWS had no capacity for the type/AZ right now. The
	// launch IS the capacity test (no read-only API reports this), so the watch
	// stays active and retries on the next poll.
	FailureCapacity
	// FailureTerminal means the attempt can never succeed as configured (bad
	// AMI/IAM, exhausted quota, malformed request). Retrying wastes poll cycles,
	// so the watch stops and the user is notified.
	FailureTerminal
)

// capacityErrorCodes are AWS API error codes that indicate a transient lack of
// capacity — the one thing worth retrying. These are the only signal AWS gives
// that "capacity is unavailable right now"; there is no read-only equivalent.
var capacityErrorCodes = map[string]bool{
	"InsufficientInstanceCapacity":         true, // RunInstances / CreateCapacityReservation, On-Demand & Spot
	"InsufficientHostCapacity":             true, // Dedicated Hosts
	"InsufficientReservedInstanceCapacity": true,
	"InsufficientCapacity":                 true,
	"Server.InsufficientInstanceCapacity":  true, // sometimes server-namespaced
	"SpotMaxPriceTooLow":                   true, // spot bid below market — clears when price drops
}

// terminalErrorCodes are AWS API error codes that will never resolve by waiting.
// Quota limits count as terminal: the user must request a quota increase or
// change the watch; polling will not help.
var terminalErrorCodes = map[string]bool{
	"InstanceLimitExceeded":        true, // On-Demand vCPU/instance quota
	"VcpuLimitExceeded":            true,
	"MaxSpotInstanceCountExceeded": true, // Spot quota
	"InvalidAMIID.NotFound":        true,
	"InvalidAMIID.Malformed":       true,
	"UnauthorizedOperation":        true,
	"AuthFailure":                  true,
	"InvalidParameterValue":        true,
	"InvalidParameterCombination":  true,
	"InvalidSubnetID.NotFound":     true,
	"InvalidGroup.NotFound":        true,
	"Unsupported":                  true, // type not supported in this AZ/config
}

// failureLabel returns a short human label for log lines.
func failureLabel(k FailureKind) string {
	switch k {
	case FailureCapacity:
		return "capacity, will retry"
	case FailureTerminal:
		return "terminal, stopping watch"
	default:
		return "none"
	}
}

// ClassifyFailure inspects a spawn/hold error and decides whether to retry.
// Unknown AWS errors and non-AWS errors default to FailureCapacity (retry):
// transient infrastructure blips should not permanently kill a watch, and the
// watch TTL bounds any retry loop regardless.
func ClassifyFailure(err error) FailureKind {
	if err == nil {
		return FailureNone
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if capacityErrorCodes[code] {
			return FailureCapacity
		}
		if terminalErrorCodes[code] {
			return FailureTerminal
		}
		// Substring fallback for code variants AWS may namespace differently.
		if strings.Contains(code, "InsufficientInstanceCapacity") ||
			strings.Contains(code, "InsufficientCapacity") {
			return FailureCapacity
		}
		// A recognized-but-unlisted AWS error: be conservative and retry, bounded
		// by the watch TTL.
		return FailureCapacity
	}

	// Non-AWS error (network, marshalling, client init). Treat as transient.
	return FailureCapacity
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
