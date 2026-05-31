package watcher

import (
	"strings"
	"testing"
)

func TestFailureLabel(t *testing.T) {
	cases := map[FailureKind]string{
		FailureNone:     "none",
		FailureCapacity: "retry",
		FailureTerminal: "stopping",
	}
	for kind, want := range cases {
		if got := failureLabel(kind); !strings.Contains(got, want) {
			t.Errorf("failureLabel(%v) = %q, want substring %q", kind, got, want)
		}
	}
}
