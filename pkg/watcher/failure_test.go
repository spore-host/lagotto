package watcher_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/spawn/pkg/launcher"
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
		want watcher.FailureKind
	}{
		{"nil", nil, watcher.FailureNone},
		{"insufficient capacity", &apiErr{"InsufficientInstanceCapacity"}, watcher.FailureCapacity},
		{"spot price too low", &apiErr{"SpotMaxPriceTooLow"}, watcher.FailureCapacity},
		{"namespaced capacity", &apiErr{"Server.InsufficientInstanceCapacity"}, watcher.FailureCapacity},
		{"instance limit (quota)", &apiErr{"InstanceLimitExceeded"}, watcher.FailureTerminal},
		{"spot quota", &apiErr{"MaxSpotInstanceCountExceeded"}, watcher.FailureTerminal},
		{"bad ami", &apiErr{"InvalidAMIID.NotFound"}, watcher.FailureTerminal},
		{"unauthorized", &apiErr{"UnauthorizedOperation"}, watcher.FailureTerminal},
		{"unsupported type", &apiErr{"Unsupported"}, watcher.FailureTerminal},
		// Unknown AWS code → conservative retry.
		{"unknown aws code", &apiErr{"SomeNewErrorCode"}, watcher.FailureCapacity},
		// Substring fallback for an unlisted capacity variant.
		{"capacity substring", &apiErr{"FooInsufficientCapacityBar"}, watcher.FailureCapacity},
		// Non-AWS error → transient retry.
		{"plain error", errors.New("dial tcp: timeout"), watcher.FailureCapacity},
		// spawn#220: a post-launch failure (RunInstances succeeded, downstream FSx
		// setup failed and the instance was torn down) is terminal — retrying other
		// AZs can't help and would orphan a filesystem per attempt. Wrapped, to
		// confirm errors.Is unwrapping works through the AZ-loop's fmt.Errorf chain.
		{"post-launch sentinel", launcher.ErrPostLaunch, watcher.FailureTerminal},
		{"post-launch wrapped", fmt.Errorf("launch instance (tried 1 AZ): %w", launcher.ErrPostLaunch), watcher.FailureTerminal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := watcher.ClassifyFailure(c.err); got != c.want {
				t.Errorf("ClassifyFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestClassifySageMakerFailure covers the async SageMaker job-status mapping
// used by the SageMaker launcher (#14).
func TestClassifySageMakerFailure(t *testing.T) {
	cases := []struct {
		name          string
		status        string
		failureReason string
		want          watcher.FailureKind
	}{
		{"capacity error", "Failed", "CapacityError: Unable to provision requested ML compute capacity.", watcher.FailureCapacity},
		{"capacity phrase only", "Failed", "Unable to provision requested ML compute capacity. Please retry.", watcher.FailureCapacity},
		{"terminal bad image", "Failed", "ClientError: image does not exist", watcher.FailureTerminal},
		{"terminal quota", "Failed", "ResourceLimitExceeded: account limit", watcher.FailureTerminal},
		{"in progress is success", "InProgress", "", watcher.FailureNone},
		{"completed is success", "Completed", "", watcher.FailureNone},
		{"stopped is success", "Stopped", "", watcher.FailureNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := watcher.ClassifySageMakerFailure(c.status, c.failureReason); got != c.want {
				t.Errorf("ClassifySageMakerFailure(%q, %q) = %v, want %v", c.status, c.failureReason, got, c.want)
			}
		})
	}
}
