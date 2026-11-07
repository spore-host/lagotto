package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// subnetEC2 is #256's us-west-2: p5.4xlarge offered in a-d, default subnets
// in a-c.
type subnetEC2 struct{}

func (subnetEC2) DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	out := &ec2.DescribeSubnetsOutput{}
	for _, az := range []string{"us-west-2a", "us-west-2b", "us-west-2c"} {
		out.Subnets = append(out.Subnets, ec2types.Subnet{AvailabilityZone: aws.String(az)})
	}
	return out, nil
}

func (subnetEC2) DescribeInstanceTypeOfferings(context.Context, *ec2.DescribeInstanceTypeOfferingsInput, ...func(*ec2.Options)) (*ec2.DescribeInstanceTypeOfferingsOutput, error) {
	out := &ec2.DescribeInstanceTypeOfferingsOutput{}
	for _, az := range []string{"us-west-2a", "us-west-2b", "us-west-2c", "us-west-2d"} {
		out.InstanceTypeOfferings = append(out.InstanceTypeOfferings, ec2types.InstanceTypeOffering{InstanceType: "p5.4xlarge", Location: aws.String(az)})
	}
	return out, nil
}

func subnetChecker() *watcher.SubnetCoverageChecker {
	return watcher.NewSubnetCoverageCheckerWith(
		func(string) watcher.SubnetCoverageEC2 { return subnetEC2{} },
		func(context.Context) ([]string, error) { return []string{"us-west-2"}, nil })
}

func TestCheckWatchSubnetCoverage(t *testing.T) {
	ctx := context.Background()
	w := &watcher.Watch{InstanceTypePattern: "p5.4xlarge", Regions: []string{"us-west-2"}, Action: watcher.ActionSpawn, Service: watcher.ServiceEC2}

	var out bytes.Buffer
	cov, err := checkWatchSubnetCoverage(ctx, subnetChecker(), &out, w)
	if err != nil || cov == nil || !cov.Checked {
		t.Fatalf("cov = %+v, err = %v", cov, err)
	}
	if !strings.Contains(out.String(), "Warning: us-west-2d offers p5.4xlarge but has no default subnet") {
		t.Errorf("stderr = %q", out.String())
	}

	// Pinned to the AZ with no subnet: refused, nothing printed.
	w.AvailabilityZones = []string{"us-west-2d"}
	out.Reset()
	if _, err := checkWatchSubnetCoverage(ctx, subnetChecker(), &out, w); err == nil ||
		!strings.Contains(err.Error(), "aws ec2 create-default-subnet --region us-west-2 --availability-zone us-west-2d") {
		t.Errorf("err = %v, want a refusal naming the fix", err)
	}
	if out.Len() != 0 {
		t.Errorf("a refusal also printed warnings: %q", out.String())
	}

	// A notify watch is not checked.
	w.Action = watcher.ActionNotify
	out.Reset()
	if cov, err := checkWatchSubnetCoverage(ctx, subnetChecker(), &out, w); err != nil || cov.Checked || out.Len() != 0 {
		t.Errorf("notify: cov = %+v, err = %v, out = %q", cov, err, out.String())
	}
}

func TestActiveSubnetCoverage(t *testing.T) {
	watches := []watcher.Watch{
		{WatchID: "w-spawn", InstanceTypePattern: "p5.4xlarge", Regions: []string{"us-west-2"}, Action: watcher.ActionSpawn},
		{WatchID: "w-notify", InstanceTypePattern: "p5.4xlarge", Regions: []string{"us-west-2"}, Action: watcher.ActionNotify},
		{WatchID: "w-hold", InstanceTypePattern: "p5.4xlarge", Action: watcher.ActionHold},
	}
	covs := activeSubnetCoverage(context.Background(), subnetChecker(), watches)
	if len(covs) != 2 || covs[0].WatchID != "w-spawn" || covs[1].WatchID != "w-hold" {
		t.Fatalf("coverage for %d watches, want w-spawn and w-hold", len(covs))
	}
	if !covs[1].AllRegions || !covs[1].HasUnreachable() {
		t.Errorf("w-hold: %+v", covs[1])
	}
}
