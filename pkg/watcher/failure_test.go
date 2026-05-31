package watcher_test

import (
	"errors"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/watcher"
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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := watcher.ClassifyFailure(c.err); got != c.want {
				t.Errorf("ClassifyFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
