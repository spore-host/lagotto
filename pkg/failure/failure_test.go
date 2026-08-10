package failure_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/failure"
	"github.com/spore-host/spawn/pkg/launchererr"
)

// apiErr is a minimal smithy.APIError for exercising ClassifyFailure.
type apiErr struct{ code string }

func (e *apiErr) Error() string                 { return e.code }
func (e *apiErr) ErrorCode() string             { return e.code }
func (e *apiErr) ErrorMessage() string          { return e.code }
func (e *apiErr) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

// jsonSyntaxErr returns a real *json.SyntaxError as produced by unmarshalling
// malformed JSON — the deterministic-failure case ClassifyFailure treats as
// terminal.
func jsonSyntaxErr() error {
	return json.Unmarshal([]byte(`{not valid json`), &struct{}{})
}

// jsonUnmarshalTypeErr returns a real *json.UnmarshalTypeError (a field of the
// wrong type), the other deterministic serialization failure.
func jsonUnmarshalTypeErr() error {
	var dst struct {
		N int `json:"n"`
	}
	return json.Unmarshal([]byte(`{"n":"not-a-number"}`), &dst)
}

func TestClassifyFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want failure.FailureKind
	}{
		{"nil", nil, failure.FailureNone},
		{"insufficient capacity", &apiErr{"InsufficientInstanceCapacity"}, failure.FailureCapacity},
		{"spot price too low", &apiErr{"SpotMaxPriceTooLow"}, failure.FailureCapacity},
		{"namespaced capacity", &apiErr{"Server.InsufficientInstanceCapacity"}, failure.FailureCapacity},
		{"instance limit (quota)", &apiErr{"InstanceLimitExceeded"}, failure.FailureTerminal},
		{"spot quota", &apiErr{"MaxSpotInstanceCountExceeded"}, failure.FailureTerminal},
		{"bad ami", &apiErr{"InvalidAMIID.NotFound"}, failure.FailureTerminal},
		{"unauthorized", &apiErr{"UnauthorizedOperation"}, failure.FailureTerminal},
		{"unsupported type", &apiErr{"Unsupported"}, failure.FailureTerminal},
		// #105: config/setup errors (SSM param missing, IAM denial, bad request
		// shape) surfaced by a pre-launch step — terminal, not capacity, so a
		// misconfiguration doesn't retry silently for the whole watch TTL.
		{"missing ssm param", &apiErr{"ParameterNotFound"}, failure.FailureTerminal},
		{"access denied", &apiErr{"AccessDenied"}, failure.FailureTerminal},
		{"access denied exception", &apiErr{"AccessDeniedException"}, failure.FailureTerminal},
		{"validation error", &apiErr{"ValidationError"}, failure.FailureTerminal},
		{"validation exception", &apiErr{"ValidationException"}, failure.FailureTerminal},
		// #41: an unlisted AWS code and a plain non-AWS blip are retryable but
		// capped — FailureUnknown, not FailureCapacity (which is uncapped).
		{"unknown aws code", &apiErr{"SomeNewErrorCode"}, failure.FailureUnknown},
		{"capacity substring", &apiErr{"FooInsufficientCapacityBar"}, failure.FailureCapacity},
		{"plain error", errors.New("dial tcp: timeout"), failure.FailureUnknown},
		// #41: deterministic serialization errors (a malformed stored config) will
		// never succeed on retry — terminal, both bare and wrapped.
		{"json syntax error", jsonSyntaxErr(), failure.FailureTerminal},
		{"json unmarshal type error", jsonUnmarshalTypeErr(), failure.FailureTerminal},
		{"json syntax wrapped", fmt.Errorf("unmarshal spawn config: %w", jsonSyntaxErr()), failure.FailureTerminal},
		// spawn#354: the post-launch sentinel is matched via spawn's dependency-free
		// leaf, both bare and wrapped through the AZ-loop's fmt.Errorf chain.
		{"post-launch sentinel", launchererr.ErrPostLaunch, failure.FailureTerminal},
		{"post-launch wrapped", fmt.Errorf("launch instance (tried 1 AZ): %w", launchererr.ErrPostLaunch), failure.FailureTerminal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := failure.ClassifyFailure(c.err); got != c.want {
				t.Errorf("ClassifyFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestIsQuotaExceeded is the #116 regression guard: a quota error must be
// distinguishable from the rest of the FailureTerminal bucket via
// IsQuotaExceeded, while a non-quota terminal error (bad AMI, IAM denial,
// malformed request) must report false — the whole point is that "terminal"
// and "quota-exceeded" are not the same question.
func TestIsQuotaExceeded(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"instance limit (on-demand quota)", &apiErr{"InstanceLimitExceeded"}, true},
		{"vcpu limit (on-demand quota)", &apiErr{"VcpuLimitExceeded"}, true},
		{"spot quota", &apiErr{"MaxSpotInstanceCountExceeded"}, true},
		// Every other FailureTerminal code must NOT read as a quota error —
		// this is the actual thing #116 asks to distinguish.
		{"bad ami (terminal, not quota)", &apiErr{"InvalidAMIID.NotFound"}, false},
		{"unauthorized (terminal, not quota)", &apiErr{"UnauthorizedOperation"}, false},
		{"missing ssm param (terminal, not quota)", &apiErr{"ParameterNotFound"}, false},
		{"access denied (terminal, not quota)", &apiErr{"AccessDenied"}, false},
		// Non-terminal codes must also report false.
		{"insufficient capacity (not terminal at all)", &apiErr{"InsufficientInstanceCapacity"}, false},
		{"unknown aws code", &apiErr{"SomeNewErrorCode"}, false},
		{"plain non-AWS error", errors.New("dial tcp: timeout"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := failure.IsQuotaExceeded(c.err); got != c.want {
				t.Errorf("IsQuotaExceeded(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestIsQuotaExceeded_StillClassifiesTerminal confirms #116 changed NOTHING
// about ClassifyFailure's existing behavior for quota codes — they still
// classify as FailureTerminal exactly as before. IsQuotaExceeded is a purely
// additive signal, not a behavior change to the default retry loop.
func TestIsQuotaExceeded_StillClassifiesTerminal(t *testing.T) {
	quotaCodes := []string{"InstanceLimitExceeded", "VcpuLimitExceeded", "MaxSpotInstanceCountExceeded"}
	for _, code := range quotaCodes {
		t.Run(code, func(t *testing.T) {
			err := &apiErr{code}
			if got := failure.ClassifyFailure(err); got != failure.FailureTerminal {
				t.Errorf("ClassifyFailure(%s) = %v, want FailureTerminal (unchanged)", code, got)
			}
			if !failure.IsQuotaExceeded(err) {
				t.Errorf("IsQuotaExceeded(%s) = false, want true", code)
			}
		})
	}
}

func TestLabel(t *testing.T) {
	cases := map[failure.FailureKind]string{
		failure.FailureNone:     "none",
		failure.FailureCapacity: "capacity, will retry",
		failure.FailureTerminal: "terminal, stopping watch",
		failure.FailureUnknown:  "unknown, will retry (capped)",
	}
	for k, want := range cases {
		if got := failure.Label(k); got != want {
			t.Errorf("Label(%v) = %q, want %q", k, got, want)
		}
	}
}
