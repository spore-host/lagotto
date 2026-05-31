package watcher_test

import (
	"context"
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

func TestEC2EquivalentPattern(t *testing.T) {
	cases := []struct {
		service watcher.Service
		pattern string
		want    string
	}{
		{watcher.ServiceSageMaker, "ml.g5.2xlarge", "g5.2xlarge"},
		{watcher.ServiceSageMaker, "ml.g5.*", "g5.*"},
		{watcher.ServiceEC2, "g5.2xlarge", "g5.2xlarge"},
		{"", "g5.2xlarge", "g5.2xlarge"}, // empty service defaults to EC2
		// SageMaker pattern lacking the prefix is left as-is (validation rejects
		// it before it reaches here).
		{watcher.ServiceSageMaker, "g5.2xlarge", "g5.2xlarge"},
	}
	for _, c := range cases {
		if got := watcher.EC2EquivalentPattern(c.service, c.pattern); got != c.want {
			t.Errorf("EC2EquivalentPattern(%q, %q) = %q, want %q", c.service, c.pattern, got, c.want)
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
		{"default service treats as ec2", "", "g5.2xlarge", false},
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

// TestPollWatch_SageMakerProxy verifies a SageMaker watch searches the EC2
// family and relabels matches as ml.* with ProxiedFrom set.
func TestPollWatch_SageMakerProxy(t *testing.T) {
	p, _, _ := pollerEnv(t, false)
	w := newTestWatch("w-sm", "arn:aws:iam::123456789012:user/erin")
	w.Service = watcher.ServiceSageMaker
	w.InstanceTypePattern = "ml.t3.micro" // proxied to t3.micro, which substrate seeds
	w.Spot = false
	w.Action = watcher.ActionNotify

	matches, err := p.PollWatch(context.Background(), w)
	if err != nil {
		t.Fatalf("PollWatch error = %v", err)
	}
	for _, m := range matches {
		if m.Service != watcher.ServiceSageMaker {
			t.Errorf("match Service = %q, want sagemaker", m.Service)
		}
		if m.InstanceType != "ml."+m.ProxiedFrom {
			t.Errorf("expected InstanceType=ml.<ProxiedFrom>, got InstanceType=%q ProxiedFrom=%q", m.InstanceType, m.ProxiedFrom)
		}
		if m.ProxiedFrom == "" {
			t.Errorf("ProxiedFrom should be set for a SageMaker match: %+v", m)
		}
	}
}
