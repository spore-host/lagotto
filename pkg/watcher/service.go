package watcher

import (
	"fmt"
	"strings"
)

// Service identifies which AWS capacity surface a watch targets.
type Service string

const (
	// ServiceEC2 watches EC2 instance capacity directly (the default).
	ServiceEC2 Service = "ec2"
	// ServiceSageMaker watches SageMaker ml.* capacity via its correlated EC2
	// family as a proxy (ml.g5.2xlarge -> g5.2xlarge).
	//
	// Important: neither EC2 nor SageMaker exposes a true "will a launch
	// succeed" capacity API. The only certain test is an actual launch that may
	// return InsufficientInstanceCapacity. lagotto's underlying EC2 signal is a
	// heuristic, and its strength differs by purchase model:
	//
	//   - Spot (--spot): DescribeSpotPriceHistory — a market-liveness signal
	//     (the type was clearing recently in that AZ). Grounded in real recent
	//     transactions, but spot capacity fluctuates and is reclaimable.
	//   - On-Demand (default): DescribeInstanceTypeOfferings only — catalog
	//     presence ("this type is sold here"). This carries NO capacity signal;
	//     an offered type can still throw InsufficientInstanceCapacity.
	//
	// Spot and On-Demand also draw from different capacity pools, so one says
	// little about the other. The SageMaker proxy inherits whichever signal the
	// watch uses, with that mode's caveat — it promises SageMaker capacity no
	// more (and no less) than the EC2 watch promises EC2 capacity. Matches are
	// labeled as a proxy accordingly.
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

// EC2EquivalentPattern maps a watch pattern to the EC2 instance-type pattern the
// poller actually searches. For SageMaker it strips the "ml." prefix
// (ml.g5.2xlarge -> g5.2xlarge) so the existing EC2 poller acts as a proxy. For
// EC2 (or empty/default) the pattern is returned unchanged.
func EC2EquivalentPattern(service Service, pattern string) string {
	if normalizeService(service) == ServiceSageMaker {
		return strings.TrimPrefix(pattern, "ml.")
	}
	return pattern
}

// sageMakerType reconstructs the ml.* instance type from the EC2 type that the
// proxy search matched (g5.2xlarge -> ml.g5.2xlarge).
func sageMakerType(ec2Type string) string {
	if strings.HasPrefix(ec2Type, "ml.") {
		return ec2Type
	}
	return "ml." + ec2Type
}

// ValidateWatchPattern checks that a pattern is well-formed for the given
// service. SageMaker watches must target ml.* instance types; EC2 watches must
// not (to avoid a silently-empty EC2 search for an "ml." pattern).
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
