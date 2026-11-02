package cmd

import (
	"testing"

	"github.com/spore-host/lagotto/internal/hermetic"
	"github.com/spore-host/lagotto/pkg/awscfg"
)

// TestMain makes this package's tests hermetic even under a plain `go test`
// (#214): the ambient AWS credentials, profile and config are replaced with
// fakes, and any request that would leave the machine fails the binary.
// internal/hermetic's gate requires it of every test binary that links a
// config-loading entry point.
func TestMain(m *testing.M) { hermetic.Main(m) }

// TestRunWatchCannotReachAWSWithoutAFake replays the #214 incident: a test
// called runWatch with flags that pass validation, and runWatch went on to
// EnsureTables + PutWatch against the developer's ambient account. Now the
// first AWS config load in that path panics with the hermetic guard instead.
func TestRunWatchCannotReachAWSWithoutAFake(t *testing.T) {
	// The defaults (ec2, notify, 24h) are exactly the shape that leaked.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("runWatch returned without reaching the awscfg guard; a valid watch must stop at the config load (#214)")
		}
		if r != awscfg.ErrHermetic {
			panic(r)
		}
	}()
	err := runWatch(watchCmd, []string{"g5.xlarge"})
	t.Fatalf("runWatch returned %v instead of stopping at the hermetic guard", err)
}
