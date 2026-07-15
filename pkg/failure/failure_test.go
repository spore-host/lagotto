package failure_test

import (
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
		{"unknown aws code", &apiErr{"SomeNewErrorCode"}, failure.FailureCapacity},
		{"capacity substring", &apiErr{"FooInsufficientCapacityBar"}, failure.FailureCapacity},
		{"plain error", errors.New("dial tcp: timeout"), failure.FailureCapacity},
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

func TestLabel(t *testing.T) {
	cases := map[failure.FailureKind]string{
		failure.FailureNone:     "none",
		failure.FailureCapacity: "capacity, will retry",
		failure.FailureTerminal: "terminal, stopping watch",
	}
	for k, want := range cases {
		if got := failure.Label(k); got != want {
			t.Errorf("Label(%v) = %q, want %q", k, got, want)
		}
	}
}
