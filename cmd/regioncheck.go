package cmd

import (
	"fmt"

	"github.com/spore-host/lagotto/pkg/awsarn"
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
		return fmt.Errorf("refusing to %s in %s: %w. Nothing was created", what, region, err)
	}
	return nil
}
