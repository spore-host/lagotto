package cmd

import (
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// TestValidateServiceFlags_SageMakerNeverRecommendsNotify is the CLI side of
// #195. The --sagemaker-config error used to say "use --action notify to only
// be told", the one mode that never tells anyone, and notify (the default
// --action) was accepted for SageMaker. Pure: it never reaches AWS.
func TestValidateServiceFlags_SageMakerNeverRecommendsNotify(t *testing.T) {
	sm := watcher.ServiceSageMaker
	cases := []struct {
		name      string
		action    watcher.ActionMode
		actionSet bool
		config    string
		wantErr   []string // substrings; nil = accepted
	}{
		{"default action", watcher.ActionNotify, false, "job.yaml", []string{"--action spawn", "notify is the default"}},
		{"explicit notify", watcher.ActionNotify, true, "job.yaml", []string{"--action spawn"}},
		{"explicit notify, no config", watcher.ActionNotify, true, "", []string{"--action spawn"}},
		{"hold", watcher.ActionHold, true, "job.yaml", []string{"hold is not supported"}},
		{"spawn, no config", watcher.ActionSpawn, true, "", []string{"--sagemaker-config is required"}},
		{"spawn with config", watcher.ActionSpawn, true, "job.yaml", nil},
	}
	for _, tc := range cases {
		err := validateServiceFlags(sm, tc.action, tc.actionSet, tc.config)
		if tc.wantErr == nil {
			if err != nil {
				t.Errorf("%s: rejected: %v", tc.name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: accepted, want an error", tc.name)
			continue
		}
		for _, want := range tc.wantErr {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: error missing %q: %v", tc.name, want, err)
			}
		}
		if strings.Contains(err.Error(), "use --action notify") {
			t.Errorf("%s: error still recommends notify: %v", tc.name, err)
		}
	}
	for _, a := range []watcher.ActionMode{watcher.ActionNotify, watcher.ActionSpawn, watcher.ActionHold} {
		if err := validateServiceFlags(watcher.ServiceEC2, a, true, ""); err != nil {
			t.Errorf("ec2 + %s rejected: %v", a, err)
		}
	}
}
