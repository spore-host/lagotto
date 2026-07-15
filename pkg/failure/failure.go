// Package failure classifies launch/hold failures into a retry decision. It is a
// dependency-free leaf: it imports only the standard library, smithy-go (for AWS
// error codes), and spawn's leaf launchererr sentinel — NO AWS service SDKs. A
// stateless consumer that wants "is this launch error retryable?" (e.g. a
// block-and-wait acquire loop) can import this without pulling the poller's
// stateful dependency tree (DynamoDB/S3/SageMaker/…). See lagotto#75.
//
// pkg/watcher aliases FailureKind, the constants, and ClassifyFailure, so
// existing callers are unchanged; the taxonomy's single source of truth lives here.
package failure

import (
	"errors"
	"strings"

	"github.com/aws/smithy-go"
	"github.com/spore-host/spawn/pkg/launchererr"
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

// Label returns a short human label for log lines.
func Label(k FailureKind) string {
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

	// A post-launch failure (spawn#220): RunInstances already SUCCEEDED, and spawn's
	// Provision tore the instance back down because a downstream step (ephemeral FSx
	// setup) failed. The launch itself worked — capacity exists — so retrying other
	// AZs can't help and would just churn launch+terminate cycles, orphaning a
	// filesystem per attempt under the old behavior. Treat as terminal so the AZ
	// sweep stops immediately. Matched against spawn's dependency-free leaf sentinel
	// (spawn#354) so this classifier stays free of the AWS SDK tree.
	if errors.Is(err, launchererr.ErrPostLaunch) {
		return FailureTerminal
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
