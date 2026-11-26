package inventory

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The footprint in aws-us-gov and aws-cn (lagotto#347): the ARNs it builds
// are in the account's partition, so they match the ones AWS returns there.
// A dependency on an ARN no resource has protects nothing: before, a
// pending teardown job in GovCloud depended on arn:aws:...:role/<invoke>, so
// a plan did not keep the invoke role it runs as.

// govAccount is what setup creates, as IAM names it in partition p.
func govAccount(p string) Account {
	g := lt(Global)
	return Account{
		Roles: []Item{
			{ARN: runtimeiam.RoleARN(p, acct), Name: runtimeiam.RoleName, Tags: g},
			{ARN: runtimeiam.SchedulerInvokeRoleARN(p, acct), Name: runtimeiam.SchedulerInvokeRoleName, Tags: g},
			{ARN: IAMARN(p, acct, "role", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: g},
		},
		InstanceProfiles: []Profile{{
			Item:  Item{ARN: IAMARN(p, acct, "instance-profile", runtimeiam.InstanceRoleName), Name: runtimeiam.InstanceRoleName, Tags: with(g, "lagotto:managed", "instance-profile")},
			Roles: []string{runtimeiam.InstanceRoleName},
		}},
		Policy: &Item{ARN: IAMARN(p, acct, "policy", cliiam.PolicyName), Name: cliiam.PolicyName, Tags: with(g, "lagotto:managed", "cli")},
	}
}

func TestClassifyInEachPartition(t *testing.T) {
	for region, p := range map[string]string{"us-gov-west-1": awsarn.GovCloud, "cn-north-1": awsarn.China} {
		// A poller whose role the read did not return, and a pending job.
		f := Facts{AccountID: acct, AllRegions: true, Account: govAccount(p), Regions: []Region{{Region: region,
			Function:  &Item{ARN: deploy.PollerFunctionARN(region, acct), Name: deploy.PollerFunctionName, Tags: lt(region)},
			Schedules: []Schedule{{Group: runtimeiam.TeardownScheduleGroup, Name: "lagotto-teardown-function"}}}}}
		live := Classify(f, Options{}, now)
		var job *Item
		for _, r := range live.Resources {
			for _, a := range append([]string{r.ARN}, r.DependsOn...) {
				if !strings.HasPrefix(a, "arn:"+p+":") {
					t.Errorf("%s: %s %s names %s, outside the partition", p, r.Type, r.Name, a)
				}
			}
			if r.Type == TypeFunction && !contains(r.DependsOn, runtimeiam.RoleARN(p, acct)) {
				t.Errorf("%s: the poller does not depend on its role: %v", p, r.DependsOn)
			}
			if r.Type == TypeSchedule {
				job = &Item{ARN: r.ARN}
				if !contains(r.DependsOn, runtimeiam.SchedulerInvokeRoleARN(p, acct)) {
					t.Errorf("%s: the pending job does not depend on the invoke role: %v", p, r.DependsOn)
				}
			}
		}
		if job == nil || job.ARN != ScheduleARN(region, acct, runtimeiam.TeardownScheduleGroup, "lagotto-teardown-function") {
			t.Fatalf("%s: job %+v", p, job)
		}
		// The account-wide plan keeps the invoke role while the job is pending.
		scoped := *live
		scoped.Resources = nil
		for _, r := range live.Resources {
			if r.Region == Global {
				scoped.Resources = append(scoped.Resources, r)
			}
		}
		plan, kept, err := PlanTeardown(&scoped, live, Options{})
		if err != nil {
			t.Fatal(err)
		}
		keptInvoke := false
		for _, k := range kept {
			keptInvoke = keptInvoke || k.Resource.Name == runtimeiam.SchedulerInvokeRoleName
		}
		for _, s := range plan.Steps {
			if s.Resource.Name == runtimeiam.SchedulerInvokeRoleName {
				t.Errorf("%s: the plan deletes the invoke role a pending job runs as", p)
			}
		}
		if !keptInvoke {
			t.Errorf("%s: the invoke role is not kept: %+v", p, kept)
		}
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// TestFactsPartition: an ARN IAM returned names the partition first; then the
// regions read; none read, aws.
func TestFactsPartition(t *testing.T) {
	for regions, want := range map[string]string{"": awsarn.AWS, "us-gov-east-1": awsarn.GovCloud, "cn-northwest-1": awsarn.China, "eu-west-1": awsarn.AWS} {
		var f Facts
		if regions != "" {
			f.Regions = []Region{{Region: regions}}
		}
		if got := f.partition(); got != want {
			t.Errorf("%q: %s, want %s", regions, got, want)
		}
	}
	// The account's own ARNs win over the regions: each kind of resource.
	gov := govAccount(awsarn.GovCloud)
	for name, a := range map[string]Account{
		"role":               {Roles: []Item{{ARN: "not an ARN"}, gov.Roles[0]}},
		"authorized role":    {AuthorizedRoles: gov.Roles},
		"profile":            {InstanceProfiles: gov.InstanceProfiles},
		"authorized profile": {AuthorizedProfiles: gov.InstanceProfiles},
		"policy":             {Policy: gov.Policy},
	} {
		f := Facts{Account: a, Regions: []Region{{Region: "eu-west-1"}}}
		if got := f.partition(); got != awsarn.GovCloud {
			t.Errorf("%s: %s, want aws-us-gov", name, got)
		}
	}
	if got := (Facts{Account: Account{Roles: []Item{{ARN: ""}}}, Regions: []Region{{Region: "cn-north-1"}}}).partition(); got != awsarn.China {
		t.Errorf("no usable ARN: %s, want the region's", got)
	}
}

// TestGatherInEachPartition: an artifact bucket's ARN is its region's
// partition's.
func TestGatherInEachPartition(t *testing.T) {
	region := "us-gov-west-1"
	name := deploy.DefaultBucketName(acct, region)
	src := &fakeSource{enabled: []string{region}, regions: map[string]Region{region: {Region: region,
		Bucket: &Item{Name: name, Tags: map[string]string{tags.ManagedBy: string(Tool)}}}}}
	f, err := Gather(context.Background(), src, acct, Scope{AllRegions: true}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b := f.Regions[0].Bucket; b == nil || b.ARN != "arn:aws-us-gov:s3:::"+name {
		t.Errorf("bucket %+v", b)
	}
}

// TestAWSAccountInEachPartition: lagotto-cli-user is read by its ARN in the
// source's partition (a GetPolicy on the aws one would find nothing, and the
// policy would never be removed), which comes from the source's region.
func TestAWSAccountInEachPartition(t *testing.T) {
	for region, p := range map[string]string{"us-gov-west-1": awsarn.GovCloud, "cn-north-1": awsarn.China, "us-east-1": awsarn.AWS} {
		a, ok := NewAWSSource(aws.Config{Region: region}, acct).Account().(*awsAccount)
		if !ok || a.partition != p {
			t.Fatalf("%s: account source %+v", region, a)
		}
		a.iam = fakeIAM{}
		if item, _, err := a.CLIPolicy(context.Background()); err != nil || item == nil || item.ARN != cliiam.PolicyARN(p, acct) {
			t.Errorf("%s: policy %+v, %v", region, item, err)
		}
	}
}
