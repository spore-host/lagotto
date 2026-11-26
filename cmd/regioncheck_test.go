package cmd

import (
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/awsarn"
)

// TestCreatorsRefuseUnsupportedRegionsFirst (lagotto#347): in a region
// lagotto does not support, every creating command refuses before its first
// AWS request: no lease mark, no table, no IAM, not even an STS call, so a
// refusal leaves nothing behind. setup and deploy, which write lagotto's IAM,
// also refuse aws-cn, which lagotto does not support yet.
func TestCreatorsRefuseUnsupportedRegionsFirst(t *testing.T) {
	all := map[string]func(t *testing.T, f *fakeAWS) error{
		"setup": func(t *testing.T, f *fakeAWS) error {
			installFakeAWS(t, f)
			return runSetup(setupCmd, nil)
		},
		"poll": func(t *testing.T, f *fakeAWS) error {
			installFakeAWS(t, f)
			return runPoll(pollCmd, nil)
		},
	}
	for name, run := range creators {
		all[name] = run
	}
	writesIAM := map[string]bool{"setup": true, "deploy": true}
	for name, run := range all {
		regions := map[string]error{"us-iso-east-1": awsarn.ErrUnsupportedPartition, "eusc-de-east-1": awsarn.ErrUnsupportedPartition}
		if writesIAM[name] {
			regions["cn-north-1"] = awsarn.ErrPartitionNotYetSupported
		}
		for region, want := range regions {
			t.Run(name+" in "+region, func(t *testing.T) {
				f := &fakeAWS{region: region}
				err := run(t, f)
				if !errors.Is(err, want) || !strings.Contains(err.Error(), "refusing to") ||
					!strings.Contains(err.Error(), region) || !strings.Contains(err.Error(), "Nothing was created") {
					t.Fatalf("err = %v, want the refusal for %s", err, region)
				}
				if f.requests != 0 || len(f.calls) != 0 {
					t.Errorf("%d AWS request(s) before the refusal: %v", f.requests, f.calls)
				}
				for _, op := range []string{"GetItem", "PutItem", "UpdateItem", "DeleteItem", "TransactWriteItems", "CreateTable", "DescribeTable"} {
					if n := f.leaseTable().Calls(op); n != 0 {
						t.Errorf("%d lifecycle %s call(s) before the refusal", n, op)
					}
				}
			})
		}
	}
}

func TestRefuseUnsupportedRegion(t *testing.T) {
	if err := refuseUnsupportedRegion("", "deploy the poller", true); err != nil {
		t.Errorf("an empty region is the command's to report: %v", err)
	}
	for region, iam := range map[string]bool{"us-east-1": true, "us-gov-west-1": true, "cn-north-1": false, "xx-mars-1": true} {
		if err := refuseUnsupportedRegion(region, "create a watch", iam); err != nil {
			t.Errorf("%s (writes IAM %v): %v", region, iam, err)
		}
	}
	if err := refuseUnsupportedRegion("cn-north-1", "set up lagotto's IAM", true); err == nil ||
		!strings.Contains(err.Error(), "aws-cn not yet supported; tracked in lagotto#347") {
		t.Errorf("aws-cn for setup: %v", err)
	}
}
