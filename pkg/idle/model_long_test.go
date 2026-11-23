//go:build modelcheck

package idle

import "testing"

const longModel = true

// TestModelCheckLong is the model check at its long bounds: two preemptions
// everywhere, and three regions (go test -tags modelcheck -run ModelCheckLong).
func TestModelCheckLong(t *testing.T) { runModel(t, true) }
