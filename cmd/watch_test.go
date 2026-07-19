package cmd

import (
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestValidateFleetFlags(t *testing.T) {
	tests := []struct {
		name     string
		action   watcher.ActionMode
		maintain int
		until    string
		wantErr  bool
	}{
		{"single-shot (no fleet flags)", watcher.ActionNotify, 0, "", false},
		{"maintain requires spawn", watcher.ActionNotify, 4, "", true},
		{"maintain with spawn ok", watcher.ActionSpawn, 4, "", false},
		{"negative maintain", watcher.ActionSpawn, -1, "", true},
		{"until without maintain", watcher.ActionSpawn, 0, "http-200: https://x/done", true},
		{"maintain + valid s3 until", watcher.ActionSpawn, 4, "s3-empty: s3://b/m minus s3://b/d", false},
		{"maintain + valid http until", watcher.ActionSpawn, 4, "http-200: https://x/done", false},
		{"maintain + valid shell until", watcher.ActionSpawn, 2, "shell: test -f /tmp/done", false},
		{"maintain + bad until spec", watcher.ActionSpawn, 4, "nonsense-no-colon", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFleetFlags(tt.action, tt.maintain, tt.until)
			if tt.wantErr != (err != nil) {
				t.Errorf("validateFleetFlags(%q,%d,%q) err=%v, wantErr=%v", tt.action, tt.maintain, tt.until, err, tt.wantErr)
			}
		})
	}
}
