package watcher

import (
	"testing"

	"github.com/spore-host/lagotto/internal/hermetic"
)

// TestMain makes this package's tests hermetic even under a plain `go test`
// (#214): the ambient AWS credentials, profile and config are replaced with
// fakes, and any request that would leave the machine fails the binary.
// internal/hermetic's gate requires it of every test binary that links a
// config-loading entry point.
func TestMain(m *testing.M) { hermetic.Main(m) }
