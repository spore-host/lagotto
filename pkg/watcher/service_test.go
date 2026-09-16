package watcher_test

import (
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestService_Valid(t *testing.T) {
	cases := map[watcher.Service]bool{
		watcher.ServiceEC2:       true,
		watcher.ServiceSageMaker: true,
		"":                       false,
		"lambda":                 false,
		"EC2":                    false, // case-sensitive
	}
	for svc, want := range cases {
		if got := svc.Valid(); got != want {
			t.Errorf("Service(%q).Valid() = %v, want %v", svc, got, want)
		}
	}
}

func TestValidateWatchPattern(t *testing.T) {
	cases := []struct {
		name    string
		service watcher.Service
		pattern string
		wantErr bool
	}{
		{"ec2 plain", watcher.ServiceEC2, "g5.2xlarge", false},
		{"ec2 wildcard", watcher.ServiceEC2, "g5.*", false},
		{"ec2 rejects ml prefix", watcher.ServiceEC2, "ml.g5.2xlarge", true},
		{"sagemaker ml type", watcher.ServiceSageMaker, "ml.g5.2xlarge", false},
		{"sagemaker wildcard", watcher.ServiceSageMaker, "ml.g5.*", false},
		{"sagemaker rejects bare type", watcher.ServiceSageMaker, "g5.2xlarge", true},
		{"empty pattern", watcher.ServiceEC2, "", true},
		{"whitespace-only pattern", watcher.ServiceEC2, "  ,  ", true},
		{"default service treats as ec2", "", "g5.2xlarge", false},
		{"ec2 comma-list ok", watcher.ServiceEC2, "g6.8xlarge,g6.4xlarge,g6.2xlarge", false},
		{"ec2 comma-list with spaces ok", watcher.ServiceEC2, " g6.4xlarge , g6.2xlarge ", false},
		{"ec2 comma-list rejects ml rung", watcher.ServiceEC2, "g6.4xlarge,ml.g5.2xlarge", true},
		{"sagemaker comma-list ok", watcher.ServiceSageMaker, "ml.g5.2xlarge,ml.p4d.24xlarge", false},
		{"sagemaker comma-list rejects bare rung", watcher.ServiceSageMaker, "ml.g5.2xlarge,g6.2xlarge", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := watcher.ValidateWatchPattern(c.service, c.pattern)
			if (err != nil) != c.wantErr {
				t.Errorf("ValidateWatchPattern(%q, %q) err = %v, wantErr %v", c.service, c.pattern, err, c.wantErr)
			}
		})
	}
}
