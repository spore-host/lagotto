package cmd

import (
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/awscfg"
)

// refuseUnsupportedRegion is every creating command's first check, right
// after it loads its AWS config and before its first AWS call, lease mark or
// create (lagotto#347). A region lagotto cannot work in (the ISO partitions,
// the European Sovereign Cloud, a string that is no region name) is refused
// there, so a refusal leaves nothing behind. writesIAM (setup, deploy) also
// refuses a partition lagotto writes no IAM in yet: aws-cn. An empty region is
// left to the command, which says how to set one.
func refuseUnsupportedRegion(region, what string, writesIAM bool) error {
	if region == "" {
		return nil
	}
	check := awsarn.Partition
	if writesIAM {
		check = awsarn.SupportedPartition
	}
	if _, err := check(region); err != nil {
		// The error names the region already.
		return fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	return nil
}

// teardownPartition is the partition a command that reads or removes
// lagotto's resources (footprint, teardown, deploy --teardown) names them in
// (lagotto#347): region's, which its name gives in aws, aws-us-gov and
// aws-cn; elsewhere (an ISO partition, the European Sovereign Cloud) caller's,
// the partition of the caller's ARN as STS reported it. With neither it is an
// error, and the command refuses: ARNs built from the region alone would be
// arn:aws: there, and name nothing.
func teardownPartition(region, caller string) (string, error) {
	if p, err := awsarn.Partition(region); err == nil {
		return p, nil
	}
	if caller != "" {
		return caller, nil
	}
	return "", fmt.Errorf("the partition of %q is unknown: its name does not say, and STS reported no caller ARN to read it from, "+
		"so lagotto cannot name its resources there", region)
}

// observedTeardownPartition is teardownPartition for cfg's region and the
// caller's partition this process's own sts:GetCallerIdentity reported.
func observedTeardownPartition(region string) (string, error) {
	return teardownPartition(region, awscfg.CallerPartition())
}

// refuseUnknownPartition is the check a reading or removing command makes
// once it has resolved its account (and so the caller's identity): it refuses
// when it cannot tell which partition region's resources are named in. An
// empty region is left to the command, which says how to set one.
func refuseUnknownPartition(region string) error {
	if region == "" {
		return nil
	}
	_, err := observedTeardownPartition(region)
	return err
}

// refuseRegionsOutsidePartition refuses the regions a watch searches
// (--regions) that lagotto cannot work in, or that are outside home's
// partition (lagotto#347): one account's credentials reach only their own
// partition's regions, so such a watch would never be polled there. home
// itself is checked by refuseUnsupportedRegion; an empty one checks nothing.
func refuseRegionsOutsidePartition(home string, regions []string) error {
	hp, err := awsarn.Partition(home)
	if err != nil {
		return nil
	}
	var bad []string
	for _, r := range regions {
		switch p, err := awsarn.Partition(r); {
		case err != nil:
			bad = append(bad, err.Error())
		case p != hp:
			bad = append(bad, fmt.Sprintf("%s is in %s, not in %s with %s", r, p, hp, home))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("refusing --regions: %s. Nothing was created", strings.Join(bad, "; "))
	}
	return nil
}
