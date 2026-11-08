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
	"encoding/json"
	"errors"
	"regexp"
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
	// FailureUnknown means the error is unrecognized but plausibly transient (an
	// unlisted AWS code, or a non-AWS network/client-init blip). It's retried like
	// capacity — a single blip must not kill a watch — but unlike FailureCapacity
	// it counts toward a per-watch consecutive-failure cap, so a persistently
	// broken watch stops after a bounded number of polls instead of burning launch
	// attempts for its whole TTL (lagotto#41). Genuine capacity waits stay
	// FailureCapacity and remain uncapped.
	FailureUnknown
	// FailureThrottled means AWS throttled the request (RequestLimitExceeded,
	// Throttling, ThrottlingException), or lagotto skipped it because it is
	// backing off from such a throttle (ErrThrottled). It is transient by
	// definition, so it never ends a watch and never counts toward the
	// unknown-failure cap; unlike FailureCapacity the caller should back off
	// before trying again, because retrying straight away is what EC2's
	// throttling penalises (lagotto#254). Appended after FailureUnknown so the
	// existing values are unchanged; a consumer that only knows the first four
	// kinds sees a non-terminal, non-unknown kind and retries it like capacity.
	FailureThrottled
)

// throttleErrorCodes are the AWS API error codes for a throttled request
// (lagotto#254). EC2 uses RequestLimitExceeded, including the AZ-scoped form
// it sends during an operational issue ("... throttled due to an operational
// issue in availability zone usw2-az1"); most other services use Throttling
// or ThrottlingException.
var throttleErrorCodes = map[string]bool{
	"RequestLimitExceeded": true,
	"Throttling":           true,
	"ThrottlingException":  true,
}

// ErrThrottled marks an attempt lagotto made no AWS call for, because it is
// backing off from a throttle in that region or every candidate AZ is in its
// cool-off (lagotto#254). ClassifyFailure treats it as FailureThrottled.
var ErrThrottled = errors.New("throttled: backing off")

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

// quotaErrorCodes are AWS API error codes specifically for an exhausted
// account quota/limit (#116). They classify as FailureTerminal like any other
// terminal code — retrying an exhausted quota wastes poll cycles just as
// retrying a bad AMI does — but are distinguishable via IsQuotaExceeded for a
// caller that wants to react differently to "this quota ceiling might free up
// soon" than to "nothing about this will ever work." E.g. a caller running
// several concurrent pkg/snipe.Snipe calls against the SAME account, where the
// quota being hit might be the caller's OWN other in-flight requests rather
// than a hard account wall needing a support ticket. ClassifyFailure checks
// this map FIRST (before terminalErrorCodes) precisely so a code only needs
// to be listed once — see the "not double-counted" assertion in the tests.
var quotaErrorCodes = map[string]bool{
	"InstanceLimitExceeded":        true, // On-Demand vCPU/instance quota
	"VcpuLimitExceeded":            true,
	"MaxSpotInstanceCountExceeded": true, // Spot quota
}

// terminalErrorCodes are AWS API error codes that will never resolve by
// waiting, EXCLUDING the quota-specific codes above (they're checked
// separately in ClassifyFailure so quotaErrorCodes stays the single source of
// truth for "is this a quota error" — see IsQuotaExceeded).
var terminalErrorCodes = map[string]bool{
	"InvalidAMIID.NotFound":       true,
	"InvalidAMIID.Malformed":      true,
	"UnauthorizedOperation":       true,
	"AuthFailure":                 true,
	"InvalidParameterValue":       true,
	"InvalidParameterCombination": true,
	"InvalidSubnetID.NotFound":    true,
	"InvalidGroup.NotFound":       true,
	"Unsupported":                 true, // type not supported in this AZ/config
	// Config/setup errors surfaced by a pre-launch step (AMI resolution via SSM,
	// IAM) rather than by RunInstances itself. These never resolve by waiting;
	// retrying just masks a misconfiguration as a capacity wait (observed: a GPU
	// AL2023 SSM parameter that doesn't exist yields ParameterNotFound on every
	// attempt). lagotto#105.
	"ParameterNotFound":     true,
	"AccessDenied":          true,
	"AccessDeniedException": true,
	"ValidationError":       true,
	"ValidationException":   true,
}

// ErrConfigRefused marks a launch lagotto refuses on its own, before any AWS
// call, because the stored config breaks a rule it enforces (e.g. an fsx_create
// bucket outside lagotto's prefix, #242). ClassifyFailure treats it as terminal.
var ErrConfigRefused = errors.New("launch config refused")

// Label returns a short human label for log lines.
func Label(k FailureKind) string {
	switch k {
	case FailureCapacity:
		return "capacity, will retry"
	case FailureTerminal:
		return "terminal, stopping watch"
	case FailureUnknown:
		return "unknown, will retry (capped)"
	case FailureThrottled:
		return "throttled, backing off"
	default:
		return "none"
	}
}

// ClassifyFailure inspects a spawn/hold error and decides whether to retry.
// The taxonomy (lagotto#41):
//   - recognized capacity codes -> FailureCapacity (retry, uncapped — a watch may
//     legitimately wait out scarce capacity for days).
//   - recognized terminal codes, a post-launch teardown, or a deterministic
//     serialization error (a malformed stored config) -> FailureTerminal (stop).
//   - throttling codes (RequestLimitExceeded, Throttling, ThrottlingException)
//     or ErrThrottled -> FailureThrottled (retry after a backoff, uncapped,
//     never terminal; lagotto#254).
//   - anything else — an unlisted AWS code or a non-AWS network/client blip ->
//     FailureUnknown (retry, but bounded by a per-watch consecutive-failure cap,
//     so a single blip never kills a watch while a persistent fault eventually
//     does).
func ClassifyFailure(err error) FailureKind {
	if err == nil {
		return FailureNone
	}

	// A stored config this lagotto refuses to launch (ErrConfigRefused): it will
	// be refused the same way on every retry, so the watch stops.
	if errors.Is(err, ErrConfigRefused) {
		return FailureTerminal
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

	// A launch lagotto skipped while backing off from a throttle (#254).
	if errors.Is(err, ErrThrottled) {
		return FailureThrottled
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if capacityErrorCodes[code] {
			return FailureCapacity
		}
		if throttleErrorCodes[code] {
			return FailureThrottled
		}
		if quotaErrorCodes[code] || terminalErrorCodes[code] {
			return FailureTerminal
		}
		// Substring fallback for code variants AWS may namespace differently.
		if strings.Contains(code, "InsufficientInstanceCapacity") ||
			strings.Contains(code, "InsufficientCapacity") {
			return FailureCapacity
		}
		// A recognized-but-unlisted AWS error: retry, but count it toward the cap
		// so a persistent unknown fault doesn't retry for the whole watch TTL.
		return FailureUnknown
	}

	// A deterministic serialization error (a malformed stored launch/job config)
	// will never succeed on retry — treat as terminal so the watch stops instead
	// of re-failing every poll.
	var syntaxErr *json.SyntaxError
	var unmarshalErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &unmarshalErr) {
		return FailureTerminal
	}

	// Raw-message capacity fallback (lagotto#140): a capacity failure that reached
	// us WITHOUT a discoverable smithy.APIError in its chain — flattened somewhere
	// in wrapping (a `%v`/`%s`-formatted layer, an aggregation, a non-SDK relay) —
	// must still be recognized, or the flagship "wait out scarce capacity" watch
	// gives up early: it'd fall to FailureUnknown and hit the consecutive-failure
	// cap instead of retrying to TTL. The message is a strong, specific signal
	// ("insufficient ... capacity"); quota/terminal errors phrase themselves as
	// "limit exceeded" and are already caught above, so this doesn't over-match.
	if messageLooksLikeCapacity(err.Error()) {
		return FailureCapacity
	}
	// The same fallback for a throttle whose APIError was lost (#254).
	if messageLooksLikeThrottle(err.Error()) {
		return FailureThrottled
	}

	// Other non-AWS errors (network, client init): plausibly transient. Retry,
	// but count toward the cap so a persistent blip eventually stops the watch.
	return FailureUnknown
}

// messageLooksLikeCapacity reports whether an error message carries a strong
// AWS "no capacity right now" signal, for the case the underlying
// smithy.APIError was lost in wrapping (lagotto#140). Matched case-insensitively
// against the specific capacity phrasings — not the generic word "capacity",
// which appears in unrelated messages (e.g. EBS volume capacity).
func messageLooksLikeCapacity(msg string) bool {
	m := strings.ToLower(msg)
	for _, s := range []string{
		"insufficientinstancecapacity",
		"insufficienthostcapacity",
		"insufficientreservedinstancecapacity",
		"insufficientcapacity",
		"insufficient capacity",
		"insufficient instance capacity",
	} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

// messageLooksLikeThrottle is messageLooksLikeCapacity for a throttle
// (lagotto#254): the codes, and EC2's own "Request limit exceeded" phrasing.
func messageLooksLikeThrottle(msg string) bool {
	m := strings.ToLower(msg)
	for _, s := range []string{"requestlimitexceeded", "request limit exceeded", "throttlingexception"} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

// IsThrottled reports whether err is a throttle (lagotto#254): exactly the
// errors ClassifyFailure returns FailureThrottled for.
func IsThrottled(err error) bool {
	return ClassifyFailure(err) == FailureThrottled
}

// throttledAZRe matches the AZ ID in EC2's AZ-scoped throttle message:
// "... throttled due to an operational issue in availability zone usw2-az1.
// Throttling will be removed once the issue is resolved." AZ IDs are
// "<region code>-az<n>", and a Local Zone's "<region code>-<zone>-az<n>"
// (e.g. usw2-lax1-az1).
var throttledAZRe = regexp.MustCompile(`(?i)availability zone ([a-z]{2,6}[0-9]+(?:-[a-z0-9]+)*-az[0-9]+)`)

// ThrottledAZID returns the AZ ID a throttle names as impaired, or "" when err
// is not a throttle or names no AZ (lagotto#254). It is an AZ ID (usw2-az1),
// which is the same physical zone in every account, not an AZ name
// (us-west-2a), whose mapping differs per account: map it with the account's
// subnets or DescribeAvailabilityZones before comparing with an AZ name.
func ThrottledAZID(err error) string {
	if err == nil || !IsThrottled(err) {
		return ""
	}
	msg := err.Error()
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorMessage() != "" {
		msg = apiErr.ErrorMessage()
	}
	if m := throttledAZRe.FindStringSubmatch(msg); m != nil {
		return strings.ToLower(m[1])
	}
	return ""
}

// IsQuotaExceeded reports whether err is specifically an exhausted account
// quota/limit (#116) — a finer-grained question than ClassifyFailure's
// FailureTerminal, which also covers bad AMI/IAM/malformed-request errors
// that IsQuotaExceeded correctly reports false for. Does NOT change the
// default retry behavior of ClassifyFailure/FailureTerminal callers (a quota
// error still classifies as terminal there, unchanged); this is purely an
// additional, optional signal for a caller that wants to build its own
// backoff-and-retry or reduce-concurrency logic on top — e.g. several
// concurrent pkg/snipe.Snipe calls against the same account, where a quota
// ceiling saturated by the caller's OWN other in-flight requests may free up
// shortly, unlike a hard account wall that genuinely needs a support ticket.
func IsQuotaExceeded(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return quotaErrorCodes[apiErr.ErrorCode()]
	}
	return false
}
