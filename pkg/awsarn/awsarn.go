// Package awsarn builds the ARNs lagotto writes into its IAM policies and
// names in its footprint and its deletes, in the partition the account is in
// (lagotto#347).
//
// Every ARN starts arn:<partition>:. lagotto used to write arn:aws:, the
// commercial partition's, everywhere: in aws-us-gov and aws-cn its policies
// then named nothing in the account (IAM refuses such a policy: the partition
// is not valid there), and its footprint read and compared ARNs that no
// resource has. The partition comes from the region ([Partition]), or from an
// ARN the caller already has ([PartitionOfARN]), such as the one STS reports.
//
// The package imports nothing of lagotto's, so every package that builds an
// ARN can use it.
package awsarn

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// The partitions lagotto supports.
const (
	// AWS is the commercial partition.
	AWS = "aws"
	// GovCloud is AWS GovCloud (US).
	GovCloud = "aws-us-gov"
	// China is the AWS China regions.
	China = "aws-cn"
)

// The region names of each supported partition: the regionRegex of each in
// the AWS SDK's partitions.json (aws-sdk-go-v2 internal/endpoints/awsrulesfn).
// The commercial one matches none of the others' regions (us-gov-west-1,
// us-iso-east-1 and eu-isoe-west-1 have two words between the dashes).
var (
	commercialRegion = regexp.MustCompile(`^(us|eu|ap|sa|ca|me|af|il|mx)-\w+-\d+$`)
	govCloudRegion   = regexp.MustCompile(`^us-gov-\w+-\d+$`)
	chinaRegion      = regexp.MustCompile(`^cn-\w+-\d+$`)
)

// ErrUnsupportedPartition is [Partition]'s refusal of a region in a partition
// lagotto does not support: the ISO partitions (us-iso-*, us-isob-*,
// us-isof-*, eu-isoe-*) and the European Sovereign Cloud (eusc-*).
var ErrUnsupportedPartition = errors.New("lagotto supports the aws, aws-us-gov and aws-cn partitions only")

// Partition returns the partition region is in: aws-us-gov for us-gov-*,
// aws-cn for cn-*, aws for the commercial regions. A region in the ISO
// partitions or the European Sovereign Cloud is an error wrapping
// [ErrUnsupportedPartition]; an empty region, or one that is in no known
// partition, is an error too.
func Partition(region string) (string, error) {
	switch {
	case region == "":
		return "", errors.New("no region, so no partition: set --region, SPORE_REGION or AWS_REGION")
	case govCloudRegion.MatchString(region):
		return GovCloud, nil
	case chinaRegion.MatchString(region):
		return China, nil
	case commercialRegion.MatchString(region):
		return AWS, nil
	case strings.Contains(region, "-iso"), strings.HasPrefix(region, "eusc-"):
		return "", fmt.Errorf("%s: %w", region, ErrUnsupportedPartition)
	}
	return "", fmt.Errorf("%s is in no partition lagotto knows (%w)", region, ErrUnsupportedPartition)
}

// PartitionOrAWS is [Partition] for the ARN builders that cannot fail: those
// of one region's own resources (its poller function, its topic, its tables,
// its schedules). A region Partition refuses gets the aws partition, as every
// ARN lagotto built did before lagotto#347. Nothing lagotto writes there
// depends on it: setup and deploy refuse such a region before they write any
// IAM ([Partition]), and the account-wide teardown has no global lease there.
func PartitionOrAWS(region string) string {
	if p, err := Partition(region); err == nil {
		return p
	}
	return AWS
}

// PartitionOfARN returns the partition of arn: "aws-us-gov" for
// arn:aws-us-gov:sts::111122223333:assumed-role/r/s.
func PartitionOfARN(arn string) (string, error) {
	parts := strings.SplitN(arn, ":", 3)
	if len(parts) != 3 || parts[0] != "arn" || parts[1] == "" {
		return "", fmt.Errorf("%q is not an ARN", arn)
	}
	return parts[1], nil
}

// IAM is the ARN of an IAM resource of accountID ("role/NAME",
// "instance-profile/NAME", "policy/NAME", "user/*"). IAM is global: there is
// no region. An AWS-managed policy is in account "aws".
func IAM(partition, accountID, resource string) string {
	return "arn:" + partition + ":iam::" + accountID + ":" + resource
}

// S3 is the ARN of a bucket ("NAME"), or of objects in one ("NAME/*"). S3
// ARNs carry neither a region nor an account.
func S3(partition, resource string) string {
	return "arn:" + partition + ":s3:::" + resource
}

// Regional is the ARN of resource of service in region and accountID. A region
// of "*" is any region of the partition.
func Regional(partition, service, region, accountID, resource string) string {
	return "arn:" + partition + ":" + service + ":" + region + ":" + accountID + ":" + resource
}

// InRegion is [Regional] in region's own partition ([PartitionOrAWS]).
func InRegion(service, region, accountID, resource string) string {
	return Regional(PartitionOrAWS(region), service, region, accountID, resource)
}

// SchedulerTarget is EventBridge Scheduler's universal target for an AWS API
// action (arn:<partition>:scheduler:::aws-sdk:<service>:<action>), which a
// schedule calls with its role's credentials.
func SchedulerTarget(partition, service, action string) string {
	return "arn:" + partition + ":scheduler:::aws-sdk:" + service + ":" + action
}
