package watcher

import (
	"fmt"
	"strings"
)

// Service identifies which AWS capacity surface a watch targets.
type Service string

const (
	// ServiceEC2 watches EC2 instance capacity directly (the default): lagotto
	// attempts to launch the requested instance via spawn and retries until it
	// succeeds or the watch TTL expires.
	ServiceEC2 Service = "ec2"
	// ServiceSageMaker watches SageMaker ml.* capacity by submitting the user's
	// SageMaker job directly and retrying on CapacityError until it launches or
	// the TTL expires. SageMaker has its own AWS-managed compute pool, separate
	// from EC2, so the attempt must target SageMaker itself — there is no useful
	// EC2 proxy, and no read-only capacity API on either service.
	ServiceSageMaker Service = "sagemaker"
)

// Valid reports whether s is a recognized service.
func (s Service) Valid() bool {
	switch s {
	case ServiceEC2, ServiceSageMaker:
		return true
	default:
		return false
	}
}

// normalizeService returns the effective service, defaulting an empty value to
// ServiceEC2 (watches created before the service field existed).
func normalizeService(s Service) Service {
	if s == "" {
		return ServiceEC2
	}
	return s
}

// ValidateWatchPattern checks that a pattern is well-formed for the given
// service. SageMaker watches must target ml.* instance types; EC2 watches must
// not.
func ValidateWatchPattern(service Service, pattern string) error {
	if pattern == "" {
		return fmt.Errorf("instance type pattern must not be empty")
	}
	switch normalizeService(service) {
	case ServiceSageMaker:
		if !strings.HasPrefix(pattern, "ml.") {
			return fmt.Errorf("SageMaker pattern %q must start with \"ml.\" (e.g. ml.g5.2xlarge)", pattern)
		}
	case ServiceEC2:
		if strings.HasPrefix(pattern, "ml.") {
			return fmt.Errorf("EC2 pattern %q must not start with \"ml.\"; use --service sagemaker for SageMaker types", pattern)
		}
	}
	return nil
}
