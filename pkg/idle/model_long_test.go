//go:build modelcheck

package idle

import "testing"

const longModel = true

// TestModelCheckLong is the model check at its long bounds: two preemptions,
// and two kinds of perturbation combined, everywhere (go test -tags
// modelcheck -run ModelCheckLong).
func TestModelCheckLong(t *testing.T) { runModel(t, true) }
