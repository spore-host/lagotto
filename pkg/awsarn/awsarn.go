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

// The partitions lagotto builds ARNs in.
const (
	// AWS is the commercial partition.
	AWS = "aws"
	// GovCloud is AWS GovCloud (US).
	GovCloud = "aws-us-gov"
	// China is the AWS China regions.
	China = "aws-cn"
)

// regionName is the shape of an AWS region name: lower-case words joined by
// dashes, ending in a number (us-east-1, ap-southeast-4, us-gov-west-1).
var regionName = regexp.MustCompile(`^[a-z]{2,}(-[a-z]+)+-[0-9]+$`)

// ErrUnsupportedPartition is [Partition]'s refusal of a region in a partition
// lagotto does not support: the ISO partitions (us-iso-*, us-isob-*,
// us-isof-*, eu-isoe-*) and the European Sovereign Cloud (eusc-*).
var ErrUnsupportedPartition = errors.New("lagotto does not support the ISO partitions or the European Sovereign Cloud")

// ErrPartitionNotYetSupported is [SupportedPartition]'s refusal of a
// partition whose ARNs lagotto builds but where it does not yet write IAM or
// keep a global lease: aws-cn (lagotto#347).
var ErrPartitionNotYetSupported = errors.New("not yet supported; tracked in lagotto#347")

// Partition returns the partition region is in: aws-us-gov for us-gov-*,
// aws-cn for cn-*, and aws for any other region name, so a new commercial
// region needs no lagotto release. A region in the ISO partitions or the
// European Sovereign Cloud is an error wrapping [ErrUnsupportedPartition]; an
// empty region, or a string that is not a region name, is an error too.
func Partition(region string) (string, error) {
	switch {
	case region == "":
		return "", errors.New("no region, so no partition: set --region, SPORE_REGION or AWS_REGION")
	case strings.HasPrefix(region, "us-gov-"):
		return GovCloud, nil
	case strings.HasPrefix(region, "cn-"):
		return China, nil
	case strings.Contains(region, "-iso"), strings.HasPrefix(region, "eusc-"):
		return "", fmt.Errorf("%s: %w", region, ErrUnsupportedPartition)
	case !regionName.MatchString(region):
		return "", fmt.Errorf("%q is not an AWS region name", region)
	}
	return AWS, nil
}

// enabled are the partitions lagotto writes its IAM in and keeps a global
// lease in. aws-cn is not one yet (lagotto#347): its ARNs are built
// (arn:aws-cn:), but whether China wants other service principals and
// iam:PassedToService values (ec2.amazonaws.com.cn, historically) is not
// verified. Enabling it is this entry, once they are.
var enabled = map[string]bool{AWS: true, GovCloud: true}

// SupportedPartition is [Partition] for the commands that write lagotto's IAM
// or use its global lease (setup, deploy, the account-wide teardown's control
// region): it also refuses a partition lagotto does not support yet, aws-cn,
// with an error wrapping [ErrPartitionNotYetSupported].
func SupportedPartition(region string) (string, error) {
	p, err := Partition(region)
	if err != nil {
		return "", err
	}
	if !enabled[p] {
		return "", fmt.Errorf("%s is in %s: %s %w", region, p, p, ErrPartitionNotYetSupported)
	}
	return p, nil
}

// PartitionOrAWS is [Partition] for the ARN builders that cannot fail: those
// of one region's own resources (its poller function, its topic, its tables,
// its schedules). A region Partition refuses gets the aws partition, as every
// ARN lagotto built did before lagotto#347. Nothing lagotto writes there
// depends on it: every creating command refuses such a region before it
// creates anything, and the account-wide teardown has no global lease there.
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
