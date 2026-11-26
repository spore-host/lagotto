package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/spf13/cobra"
	"github.com/spore-host/libs/lastmile"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/awscfg"
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
					strings.Count(err.Error(), region) != 1 || !strings.Contains(err.Error(), "Nothing was created") {
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

// TestTeardownPartition (lagotto#347): a reading or removing command names
// lagotto's resources in the region's partition where its name gives one, and
// in the caller's elsewhere; with neither it refuses.
func TestTeardownPartition(t *testing.T) {
	for _, c := range []struct{ region, caller, want string }{
		{"us-east-1", "", "aws"}, {"us-gov-west-1", "aws", "aws-us-gov"}, {"cn-north-1", "", "aws-cn"},
		{"us-iso-east-1", "aws-iso", "aws-iso"}, {"eusc-de-east-1", "aws-eusc", "aws-eusc"},
	} {
		if got, err := teardownPartition(c.region, c.caller); got != c.want || err != nil {
			t.Errorf("teardownPartition(%q, %q) = %q, %v, want %q", c.region, c.caller, got, err, c.want)
		}
	}
	if got, err := teardownPartition("us-iso-east-1", ""); got != "" || err == nil || !strings.Contains(err.Error(), "is unknown") {
		t.Errorf("no caller partition: %q, %v", got, err)
	}
	if err := refuseUnknownPartition(""); err != nil {
		t.Errorf("an empty region is the command's to report: %v", err)
	}
	awscfg.ResetCallerAccountForTest(t)
	if err := refuseUnknownPartition("us-iso-east-1"); err == nil {
		t.Error("an ISO region with no caller partition was accepted")
	}
	awscfg.SetCallerPartitionForTest(t, "aws-iso")
	if err := refuseUnknownPartition("us-iso-east-1"); err != nil {
		t.Errorf("an ISO region with the caller's partition: %v", err)
	}
}

// TestTeardownDeletersInTheCallersPartition: in an ISO region the deleters
// name lagotto-cli-user in the caller's partition; with none known they
// refuse to delete it.
func TestTeardownDeletersInTheCallersPartition(t *testing.T) {
	awscfg.SetCallerPartitionForTest(t, "aws-iso")
	d := newTeardownDeleters(aws.Config{Region: "us-iso-east-1"}, "123456789012")
	if a, ok := d.Account.(iamAccountDeleter); !ok || a.partition != "aws-iso" {
		t.Errorf("account deleter %+v", d.Account)
	}
	if _, ok := d.Region("us-iso-east-1").(regionDeleter); !ok {
		t.Error("no region deleter")
	}
	if src := newAWSSource(aws.Config{Region: "us-iso-east-1"}, "123456789012"); src.PartitionIn("us-iso-east-1") != "aws-iso" {
		t.Errorf("source partition %s", src.PartitionIn("us-iso-east-1"))
	}
	awscfg.ResetCallerAccountForTest(t)
	d = newTeardownDeleters(aws.Config{Region: "us-iso-east-1"}, "123456789012")
	if _, err := d.Account.DeletePolicy(context.Background()); err == nil || !strings.Contains(err.Error(), "partition is unknown") {
		t.Errorf("DeletePolicy with no partition: %v", err)
	}
}

// TestDeployTeardownNeedsAPartition: deploy --teardown in an ISO region names
// the poller's resources in the caller's partition; when STS reports none it
// deletes nothing.
func TestDeployTeardownNeedsAPartition(t *testing.T) {
	f := &fakeAWS{region: "us-iso-east-1", callerARN: "arn::iam::123456789012:user/alice"}
	setFlagsForTest(t, deployCmd, map[string]string{"teardown": "true", "yes": "true"})
	installFakeAWS(t, f)
	err := runDeploy(deployCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "not deleting anything") || !strings.Contains(err.Error(), "is unknown") {
		t.Fatalf("err = %v", err)
	}
	for _, c := range f.calls {
		if c != "sts:GetCallerIdentity" {
			t.Errorf("called %s", c)
		}
	}
}

// TestWatchRefusesRegionsOutsideItsPartition: a watch whose --regions are in
// another partition, or one lagotto does not support, is refused before any
// AWS request.
func TestWatchRefusesRegionsOutsideItsPartition(t *testing.T) {
	for regions, want := range map[string]string{
		"us-west-2,us-gov-west-1": "us-gov-west-1 is in aws-us-gov, not in aws with us-east-1",
		"us-iso-east-1":           "us-iso-east-1: lagotto does not support",
		"local":                   `"local" is not an AWS region name`,
	} {
		t.Run(regions, func(t *testing.T) {
			f := &fakeAWS{scheduleState: "ENABLED"}
			// Set directly: setFlagsForTest cannot restore a slice flag.
			old := watchRegions
			t.Cleanup(func() { watchRegions = old })
			watchRegions = strings.Split(regions, ",")
			err := runWatchWithFake(t, f)
			if err == nil || !strings.Contains(err.Error(), "refusing --regions") || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want %q", err, want)
			}
			if f.requests != 0 {
				t.Errorf("%d AWS request(s) before the refusal: %v", f.requests, f.calls)
			}
		})
	}
	if err := refuseRegionsOutsidePartition("us-gov-west-1", []string{"us-gov-east-1"}); err != nil {
		t.Errorf("GovCloud to GovCloud: %v", err)
	}
	if err := refuseRegionsOutsidePartition("us-gov-west-1", []string{"us-east-1"}); err == nil {
		t.Error("GovCloud to aws accepted")
	}
	if err := refuseRegionsOutsidePartition("", []string{"us-gov-east-1"}); err != nil {
		t.Errorf("no home region checks nothing: %v", err)
	}
}

// TestUpdateRefusesRegionsOutsideItsPartition: `update --add-region` into
// another partition is refused before anything is written.
func TestUpdateRefusesRegionsOutsideItsPartition(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", item: updateWatchItem("active")}
	_, err := runUpdateWithFake(t, f, map[string]string{"add-region": "us-gov-west-1"})
	if err == nil || !strings.Contains(err.Error(), "us-gov-west-1 is in aws-us-gov") {
		t.Fatalf("err = %v", err)
	}
	if n := f.called("dynamodb:UpdateItem"); n != 0 {
		t.Errorf("%d UpdateItem(s) after the refusal", n)
	}
}

// isoLoader makes every config load answer an ISO region.
func isoLoader(t *testing.T) {
	t.Helper()
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "us-iso-east-1", HTTPClient: refusingHTTP{}, Credentials: aws.AnonymousCredentials{}}, nil
	})
}

// TestReadersRefuseAnUnknownPartition: footprint, `teardown --plan` and the
// last mile, in an ISO region where STS reported no caller partition, refuse
// (or skip) before they read or delete anything.
func TestReadersRefuseAnUnknownPartition(t *testing.T) {
	_, rec, scopes := stubPlanTeardown(t, fpFacts)
	isoLoader(t)
	awscfg.ResetCallerAccountForTest(t)
	c := &cobra.Command{}
	c.SetOut(&bytes.Buffer{})
	if err := runFootprint(c, nil); err == nil || !strings.Contains(err.Error(), "footprint: the partition of") {
		t.Errorf("footprint: %v", err)
	}
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "not deleting anything: the partition of") {
		t.Errorf("teardown --plan: %v", err)
	}
	if len(*scopes) != 0 || len(rec.calls) != 0 {
		t.Errorf("read %v, deleted %v", *scopes, rec.calls)
	}

	newLMWorld(t)
	isoLoader(t)
	awscfg.ResetCallerAccountForTest(t)
	var logs []string
	lastMile(context.Background(), lastmile.ModeReport, func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })
	if s := strings.Join(logs, "\n"); !strings.Contains(s, "lastmile: skipped: the partition of") {
		t.Errorf("last mile:\n%s", s)
	}
}

// TestIAMRolesTeardownRefusesAnUnknownPartition: `teardown --iam-roles` in an
// ISO region with no caller partition deletes nothing.
func TestIAMRolesTeardownRefusesAnUnknownPartition(t *testing.T) {
	tables := &fakeTables{empty: true}
	calls := stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, true, false)
	stubPollerRegions(t, nil, nil)
	isoLoader(t)
	awscfg.ResetCallerAccountForTest(t)
	if _, err := runTeardownForTest(t); err == nil || !strings.Contains(err.Error(), "not deleting anything: the partition of") ||
		len(*calls) != 0 || tables.deleted {
		t.Errorf("%v calls=%v tables deleted=%v", err, *calls, tables.deleted)
	}
}
