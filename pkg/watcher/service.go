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
// service. A pattern may be a comma-separated list of sub-patterns (e.g.
// "g6.4xlarge,g6.2xlarge"), each of which must satisfy the service rule:
// SageMaker watches must target ml.* instance types; EC2 watches must not (#135).
func ValidateWatchPattern(service Service, pattern string) error {
	subs := splitInstanceTypePatterns(pattern)
	if len(subs) == 0 {
		return fmt.Errorf("instance type pattern must not be empty")
	}
	svc := normalizeService(service)
	for _, p := range subs {
		switch svc {
		case ServiceSageMaker:
			if !strings.HasPrefix(p, "ml.") {
				return fmt.Errorf("SageMaker pattern %q must start with \"ml.\" (e.g. ml.g5.2xlarge)", p)
			}
		case ServiceEC2:
			if strings.HasPrefix(p, "ml.") {
				return fmt.Errorf("EC2 pattern %q must not start with \"ml.\"; use --service sagemaker for SageMaker types", p)
			}
		}
	}
	return nil
}

// sageMakerNotifyUnsupported explains why a notify-only SageMaker watch can't
// work and names the mode that does. Shared by the creation-time refusal
// (ValidateServiceAction) and the notice a stored watch gets when the poller
// retires it, so the two never disagree.
const sageMakerNotifyUnsupported = "--action notify is not supported for --service sagemaker: " +
	"SageMaker has no capacity signal short of submitting a job, so a notify-only watch has nothing to notify on and could never fire (#195). " +
	"Use --action spawn --sagemaker-config <job.yaml> to submit your job and retry it until SageMaker provisions it; " +
	"add --notify to be told when it does"

// ValidateServiceAction rejects an action the service can't carry out, at watch
// creation. SageMaker supports only --action spawn (submit the job): hold
// creates an EC2 Capacity Reservation, which has no SageMaker equivalent, and
// notify has no capacity signal to fire on (#195), so before this a notify-only
// SageMaker watch was accepted and then sat silently until its TTL.
func ValidateServiceAction(service Service, action ActionMode) error {
	if normalizeService(service) != ServiceSageMaker {
		return nil
	}
	switch action {
	case ActionHold:
		return fmt.Errorf("--action hold is not supported for --service sagemaker (no capacity-reservation equivalent)")
	case ActionNotify:
		return fmt.Errorf("%s", sageMakerNotifyUnsupported)
	}
	return nil
}
