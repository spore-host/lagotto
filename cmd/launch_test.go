package cmd

import (
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestResolveIfExists covers the --if-exists resolution, including the two
// different empty-flag defaults (one-shot launch skips if the instance exists;
// a persisted watch launches) and the invalid-value error.
func TestResolveIfExists(t *testing.T) {
	tests := []struct {
		name    string
		flag    string
		oneShot bool
		want    string
		wantErr bool
	}{
		{"empty one-shot defaults to skip", "", true, watcher.IfExistsSkip, false},
		{"empty watch defaults to launch", "", false, watcher.IfExistsLaunch, false},
		{"explicit skip", "skip", false, watcher.IfExistsSkip, false},
		{"explicit launch", "launch", true, watcher.IfExistsLaunch, false},
		{"explicit replace", "replace", false, watcher.IfExistsReplace, false},
		{"case-insensitive + trimmed", "  SKIP  ", false, watcher.IfExistsSkip, false},
		{"invalid value errors", "bogus", false, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveIfExists(tt.flag, tt.oneShot)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveIfExists(%q,%v) = %q, want error", tt.flag, tt.oneShot, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveIfExists(%q,%v): unexpected error %v", tt.flag, tt.oneShot, err)
			}
			if got != tt.want {
				t.Errorf("resolveIfExists(%q,%v) = %q, want %q", tt.flag, tt.oneShot, got, tt.want)
			}
		})
	}
}
