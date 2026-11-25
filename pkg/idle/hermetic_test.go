package idle

import (
	"context"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/testutil/hermetic"
)

// TestMain makes this package's tests hermetic even under a plain `go test`
// (#214): the ambient AWS credentials, profile and config are replaced with
// fakes, and any request that would leave the machine fails the binary.
// EndMark's pause between tries is none: the tests' and the model's time is
// their own.
func TestMain(m *testing.M) {
	endMarkSleep = func(context.Context, time.Duration) error { return nil }
	hermetic.Main(m)
}
