package awsarn

import (
	"errors"
	"strings"
	"testing"
)

// TestPartition: us-gov-* is aws-us-gov, cn-* is aws-cn, and any other region
// name is aws (a new commercial region needs no release); the ISO partitions,
// the European Sovereign Cloud, an empty region and a string that is not a
// region name are refused (lagotto#347).
func TestPartition(t *testing.T) {
	for region, want := range map[string]string{
		"us-east-1": AWS, "us-west-2": AWS, "eu-west-1": AWS, "ap-southeast-4": AWS, "ca-west-1": AWS,
		"sa-east-1": AWS, "me-central-1": AWS, "af-south-1": AWS, "il-central-1": AWS, "mx-central-1": AWS,
		"xx-mars-1": AWS, "ap-southeast-10": AWS, // a region lagotto has not heard of yet
		"us-gov-west-1": GovCloud, "us-gov-east-1": GovCloud,
		"cn-north-1": China, "cn-northwest-1": China,
	} {
		if got, err := Partition(region); got != want || err != nil {
			t.Errorf("Partition(%q) = %q, %v, want %q", region, got, err, want)
		}
		if got := PartitionOrAWS(region); got != want {
			t.Errorf("PartitionOrAWS(%q) = %q, want %q", region, got, want)
		}
	}
	// eu-isoe-west-1 starts like a commercial region; eusc-de-east-1 is the
	// European Sovereign Cloud.
	for _, region := range []string{"us-iso-east-1", "us-iso-west-1", "us-isob-east-1", "us-isof-south-1", "eu-isoe-west-1", "eusc-de-east-1"} {
		got, err := Partition(region)
		if got != "" || !errors.Is(err, ErrUnsupportedPartition) || !strings.Contains(err.Error(), region) {
			t.Errorf("Partition(%q) = %q, %v, want the unsupported-partition refusal", region, got, err)
		}
		if got := PartitionOrAWS(region); got != AWS {
			t.Errorf("PartitionOrAWS(%q) = %q, want the aws fallback", region, got)
		}
	}
	for _, region := range []string{"local", "us-east", "US-EAST-1", "us-east-1a", "-1", "us east 1"} {
		if got, err := Partition(region); got != "" || err == nil || errors.Is(err, ErrUnsupportedPartition) || !strings.Contains(err.Error(), "not an AWS region name") {
			t.Errorf("Partition(%q) = %q, %v, want a not-a-region error", region, got, err)
		}
	}
	if got, err := Partition(""); got != "" || err == nil || errors.Is(err, ErrUnsupportedPartition) || !strings.Contains(err.Error(), "no region") {
		t.Errorf("Partition(\"\") = %q, %v, want a no-region error", got, err)
	}
	if got := PartitionOrAWS(""); got != AWS {
		t.Errorf("PartitionOrAWS(\"\") = %q", got)
	}
}

// TestSupportedPartition: aws and aws-us-gov are supported; aws-cn is not yet,
// with a message that says so; what Partition refuses, it refuses too.
func TestSupportedPartition(t *testing.T) {
	for region, want := range map[string]string{"us-east-1": AWS, "xx-mars-1": AWS, "us-gov-east-1": GovCloud, "us-gov-west-1": GovCloud} {
		if got, err := SupportedPartition(region); got != want || err != nil {
			t.Errorf("SupportedPartition(%q) = %q, %v, want %q", region, got, err, want)
		}
	}
	for _, region := range []string{"cn-north-1", "cn-northwest-1"} {
		got, err := SupportedPartition(region)
		if got != "" || !errors.Is(err, ErrPartitionNotYetSupported) ||
			!strings.Contains(err.Error(), region+" is in aws-cn: aws-cn not yet supported; tracked in lagotto#347") {
			t.Errorf("SupportedPartition(%q) = %q, %v", region, got, err)
		}
	}
	for _, region := range []string{"us-iso-east-1", "eusc-de-east-1", "local", ""} {
		if got, err := SupportedPartition(region); got != "" || err == nil || errors.Is(err, ErrPartitionNotYetSupported) {
			t.Errorf("SupportedPartition(%q) = %q, %v", region, got, err)
		}
	}
}

func TestPartitionOfARN(t *testing.T) {
	for arn, want := range map[string]string{
		"arn:aws:iam::111122223333:user/alice":                      AWS,
		"arn:aws-us-gov:sts::111122223333:assumed-role/Dev/s":       GovCloud,
		"arn:aws-cn:iam::111122223333:role/x":                       China,
		"arn:aws-iso-b:lambda:us-isob-east-1:111122223333:function": "aws-iso-b",
	} {
		if got, err := PartitionOfARN(arn); got != want || err != nil {
			t.Errorf("PartitionOfARN(%q) = %q, %v, want %q", arn, got, err, want)
		}
	}
	for _, bad := range []string{"", "arn", "arn::iam::1:user/x", "urn:aws:iam::1:user/x", "aws"} {
		if got, err := PartitionOfARN(bad); got != "" || err == nil {
			t.Errorf("PartitionOfARN(%q) = %q, %v, want an error", bad, got, err)
		}
	}
}

// TestBuilders: each ARN is in the partition it is given; for the aws
// partition, exactly what lagotto built before lagotto#347.
func TestBuilders(t *testing.T) {
	const acct = "111122223333"
	for _, p := range []string{AWS, GovCloud, China} {
		pre := "arn:" + p + ":"
		for got, want := range map[string]string{
			IAM(p, acct, "role/lagotto-capacity-poller-role"):                pre + "iam::111122223333:role/lagotto-capacity-poller-role",
			IAM(p, "aws", "policy/service-role/AWSLambdaBasicExecutionRole"): pre + "iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			S3(p, "lagotto-fsx-*"): pre + "s3:::lagotto-fsx-*",
			S3(p, "bucket/*"):      pre + "s3:::bucket/*",
			Regional(p, "dynamodb", "*", acct, "table/spore-host-lifecycle"):       pre + "dynamodb:*:111122223333:table/spore-host-lifecycle",
			SchedulerTarget(p, "lambda", "deleteFunction"):                         pre + "scheduler:::aws-sdk:lambda:deleteFunction",
			Regional(p, "lambda", "r-1", acct, "function:lagotto-capacity-poller"): pre + "lambda:r-1:111122223333:function:lagotto-capacity-poller",
		} {
			if got != want {
				t.Errorf("%s: got %q, want %q", p, got, want)
			}
		}
	}
	for region, want := range map[string]string{
		"us-gov-west-1":  "arn:aws-us-gov:sns:us-gov-west-1:111122223333:lagotto-capacity-alerts",
		"cn-northwest-1": "arn:aws-cn:sns:cn-northwest-1:111122223333:lagotto-capacity-alerts",
		"eu-west-1":      "arn:aws:sns:eu-west-1:111122223333:lagotto-capacity-alerts",
		"us-iso-east-1":  "arn:aws:sns:us-iso-east-1:111122223333:lagotto-capacity-alerts",
	} {
		if got := InRegion("sns", region, acct, "lagotto-capacity-alerts"); got != want {
			t.Errorf("InRegion(%s) = %q, want %q", region, got, want)
		}
	}
}
