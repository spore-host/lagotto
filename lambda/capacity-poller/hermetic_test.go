package main

import (
	"testing"

	"github.com/spore-host/lagotto/internal/hermetic"
)

// TestMain makes this package's tests hermetic even under a plain `go test`
// (#214): no ambient AWS credentials, profile or config, and any request that
// would leave the machine fails the binary.
func TestMain(m *testing.M) { hermetic.Main(m) }
