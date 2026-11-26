package runtimeiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/awsarn"
)

// The partition tests (lagotto#347): every ARN the poller's policies name is
// in the account's partition, so in aws-us-gov and aws-cn the grants reach
// lagotto's resources, the spore-host-lifecycle table among them.

const ptAccount = "123456789012"

var otherPartitions = []string{awsarn.GovCloud, awsarn.China}

// ptAuth authorizes an instance role and a SageMaker role of partition p.
func ptAuth(p string) Authorizations {
	return Authorizations{
		InstanceRoles:  []string{"my-instance-role"},
		SageMakerRoles: []string{awsarn.IAM(p, ptAccount, "role/service-role/my-sagemaker-role")},
	}
}

// inPartition is doc for the aws partition moved to partition p: every
// "arn:aws:" becomes "arn:<p>:", and nothing else changes.
func inPartition(doc, p string) string { return strings.ReplaceAll(doc, "arn:aws:", "arn:"+p+":") }

// TestPolicyDocumentsInEachPartition: each policy lagotto writes onto the
// poller's roles is, in aws-us-gov and aws-cn, the aws one with every ARN in
// that partition and nothing else changed, and names no aws-partition ARN.
func TestPolicyDocumentsInEachPartition(t *testing.T) {
	awsRuntime, err := PolicyDocumentFor(awsarn.AWS, ptAccount, ptAuth(awsarn.AWS))
	if err != nil {
		t.Fatal(err)
	}
	awsInvoke, _ := SchedulerInvokePolicyDocument(awsarn.AWS, ptAccount)
	awsTeardown, _ := SchedulerTeardownPolicyDocument(awsarn.AWS, ptAccount)
	for _, p := range otherPartitions {
		runtime, err := PolicyDocumentFor(p, ptAccount, ptAuth(p))
		if err != nil {
			t.Fatal(err)
		}
		invoke, err := SchedulerInvokePolicyDocument(p, ptAccount)
		if err != nil {
			t.Fatal(err)
		}
		teardown, err := SchedulerTeardownPolicyDocument(p, ptAccount)
		if err != nil {
			t.Fatal(err)
		}
		for name, c := range map[string]struct{ got, aws string }{
			"runtime": {runtime, awsRuntime}, "invoke": {invoke, awsInvoke}, "teardown": {teardown, awsTeardown},
		} {
			if c.got != inPartition(c.aws, p) {
				t.Errorf("%s: the %s policy is not the aws one in %s:\n%s", p, name, p, c.got)
			}
			if strings.Contains(c.got, "arn:aws:") {
				t.Errorf("%s: the %s policy names the aws partition", p, name)
			}
			grants, err := ParsePolicy(c.got)
			if err != nil {
				t.Fatal(err)
			}
			for _, g := range grants {
				if g.Resource != "*" && !strings.HasPrefix(g.Resource, "arn:"+p+":") {
					t.Errorf("%s: %s policy grant %s is not in the partition", p, name, g)
				}
			}
		}
	}
}

// TestRuntimePolicyGrantsInEachPartition names the grants the global-lease
// protocol and the idle teardown depend on, in each partition.
func TestRuntimePolicyGrantsInEachPartition(t *testing.T) {
	for _, p := range append([]string{awsarn.AWS}, otherPartitions...) {
		doc, err := PolicyDocumentFor(p, ptAccount, ptAuth(p))
		if err != nil {
			t.Fatal(err)
		}
		grants, err := ParsePolicy(doc)
		if err != nil {
			t.Fatal(err)
		}
		pre := "arn:" + p + ":"
		for _, want := range []struct{ action, resource string }{
			// The lifecycle lease table (lagotto#347's subject).
			{"dynamodb:UpdateItem", pre + "dynamodb:*:" + ptAccount + ":table/" + LifecycleTableName},
			{"dynamodb:CreateTable", pre + "dynamodb:*:" + ptAccount + ":table/" + LifecycleTableName},
			{"dynamodb:ConditionCheckItem", pre + "dynamodb:*:" + ptAccount + ":table/" + LifecycleTableName},
			{"dynamodb:GetItem", pre + "dynamodb:*:" + ptAccount + ":table/lagotto-watches"},
			// The scheduler grants.
			{"scheduler:UpdateSchedule", pre + "scheduler:*:" + ptAccount + ":schedule/lagotto/lagotto-capacity-poller"},
			{"scheduler:CreateSchedule", pre + "scheduler:*:" + ptAccount + ":schedule/lagotto/lagotto-launch-*"},
			{"scheduler:CreateSchedule", pre + "scheduler:*:" + ptAccount + ":schedule/" + TeardownScheduleGroup + "/*"},
			{"scheduler:DeleteScheduleGroup", pre + "scheduler:*:" + ptAccount + ":schedule-group/lagotto"},
			// IAM: the roles it passes and reads.
			{"iam:PassRole", SchedulerInvokeRoleARN(p, ptAccount)},
			{"iam:PassRole", pre + "iam::" + ptAccount + ":role/" + InstanceRoleName},
			{"iam:PassRole", pre + "iam::" + ptAccount + ":role/my-instance-role"},
			{"iam:PassRole", pre + "iam::" + ptAccount + ":role/service-role/my-sagemaker-role"},
			{"iam:GetRole", RoleARN(p, ptAccount)},
			{"iam:GetInstanceProfile", pre + "iam::" + ptAccount + ":instance-profile/" + InstanceRoleName},
			// S3 and the rest.
			{"s3:CreateBucket", pre + "s3:::" + FSxBucketPrefix + "*"},
			{"s3:DeleteBucket", ArtifactBucketARN(p, ptAccount)},
			{"lambda:GetFunction", pre + "lambda:*:" + ptAccount + ":function:lagotto-capacity-poller"},
			{"ec2:TerminateInstances", pre + "ec2:*:" + ptAccount + ":instance/*"},
		} {
			if !hasGrant(grants, "Allow", want.action, want.resource) {
				t.Errorf("%s: no %s on %s", p, want.action, want.resource)
			}
		}
		if !hasGrant(grants, "Deny", "iam:Put*", pre+"iam::"+ptAccount+":policy/"+BoundaryPolicyName) {
			t.Errorf("%s: the boundary guard is not on the partition's boundary policy", p)
		}
		// The authorized roles are read back from a deployed policy of any
		// partition.
		if roles, err := AuthorizedInstanceRoles(doc); err != nil || len(roles) != 1 || roles[0] != "my-instance-role" {
			t.Errorf("%s: AuthorizedInstanceRoles = %v, %v", p, roles, err)
		}
		if sm, err := AuthorizedSageMakerRoles(doc); err != nil || len(sm) != 1 || sm[0] != ptAuth(p).SageMakerRoles[0] {
			t.Errorf("%s: AuthorizedSageMakerRoles = %v, %v", p, sm, err)
		}
		if missing := MissingInstanceRoleGrants(grants, p, ptAccount, "my-instance-role"); len(missing) != 0 {
			t.Errorf("%s: the authorized role is reported missing %v", p, missing)
		}
	}
	// A role ARN in the wrong partition is not covered by a policy of this one.
	gov, _ := PolicyDocumentFor(awsarn.GovCloud, ptAccount, ptAuth(awsarn.GovCloud))
	grants, _ := ParsePolicy(gov)
	if missing := MissingInstanceRoleGrants(grants, awsarn.AWS, ptAccount, "my-instance-role"); len(missing) == 0 {
		t.Error("a GovCloud policy covers an aws-partition role ARN")
	}
}

func hasGrant(grants []Grant, effect, action, resource string) bool {
	for _, g := range grants {
		if strings.EqualFold(g.Effect, effect) && g.Action == action && g.Resource == resource {
			return true
		}
	}
	return false
}

// TestRoleARNsInEachPartition: the roles' ARNs and the AWS-managed
// basic-execution policy's.
func TestRoleARNsInEachPartition(t *testing.T) {
	for p, want := range map[string][4]string{
		awsarn.AWS: {"arn:aws:iam::123456789012:role/lagotto-capacity-poller-role",
			"arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke",
			"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			"arn:aws:lambda:*:123456789012:function:lagotto-capacity-poller"},
		awsarn.GovCloud: {"arn:aws-us-gov:iam::123456789012:role/lagotto-capacity-poller-role",
			"arn:aws-us-gov:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke",
			"arn:aws-us-gov:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			"arn:aws-us-gov:lambda:*:123456789012:function:lagotto-capacity-poller"},
		awsarn.China: {"arn:aws-cn:iam::123456789012:role/lagotto-capacity-poller-role",
			"arn:aws-cn:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke",
			"arn:aws-cn:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
			"arn:aws-cn:lambda:*:123456789012:function:lagotto-capacity-poller"},
	} {
		got := [4]string{RoleARN(p, ptAccount), SchedulerInvokeRoleARN(p, ptAccount), BasicExecutionPolicyARN(p),
			SchedulerInvokeFunctionARN(p, ptAccount)}
		if got != want {
			t.Errorf("%s: %v, want %v", p, got, want)
		}
	}
}

// TestPolicyBuildersNeedAPartition: an empty partition is refused, never an
// "arn::" policy.
func TestPolicyBuildersNeedAPartition(t *testing.T) {
	if _, err := PolicyDocumentFor("", ptAccount, Authorizations{}); err == nil {
		t.Error("PolicyDocumentFor with no partition")
	}
	if _, err := SchedulerInvokePolicyDocument("", ptAccount); err == nil {
		t.Error("SchedulerInvokePolicyDocument with no partition")
	}
	if _, err := SchedulerTeardownPolicyDocument("", ptAccount); err == nil {
		t.Error("SchedulerTeardownPolicyDocument with no partition")
	}
}

// TestEnsureRolesInEachPartition: setup and deploy write the partition's
// policies and attach the partition's managed policy, derived from the region.
func TestEnsureRolesInEachPartition(t *testing.T) {
	for region, p := range map[string]string{"us-gov-east-1": awsarn.GovCloud, "eu-west-1": awsarn.AWS} {
		f := &fakeIAM{}
		if _, err := EnsureSetupRoles(context.Background(), f, region, ptAccount, ptAuth(p)); err != nil {
			t.Fatal(err)
		}
		want, _ := PolicyDocumentFor(p, ptAccount, ptAuth(p))
		if f.inline[RoleName+"/"+PolicyName] != want {
			t.Errorf("%s: the runtime policy is not the %s one", region, p)
		}
		invoke, _ := SchedulerInvokePolicyDocument(p, ptAccount)
		if f.inline[SchedulerInvokeRoleName+"/"+SchedulerInvokePolicyName] != invoke {
			t.Errorf("%s: the invoke policy is not the %s one", region, p)
		}
		teardown, _ := SchedulerTeardownPolicyDocument(p, ptAccount)
		if f.inline[SchedulerInvokeRoleName+"/"+SchedulerTeardownPolicyName] != teardown {
			t.Errorf("%s: the teardown policy is not the %s one", region, p)
		}
		if len(f.policyARNs) != 1 || f.policyARNs[0] != BasicExecutionPolicyARN(p) {
			t.Errorf("%s: attached %v, want %s", region, f.policyARNs, BasicExecutionPolicyARN(p))
		}

		f = &fakeIAM{}
		if err := EnsureRoles(context.Background(), f, region, ptAccount); err != nil {
			t.Fatal(err)
		}
		if f.inline[SchedulerInvokeRoleName+"/"+SchedulerInvokePolicyName] != invoke || f.policyARNs[0] != BasicExecutionPolicyARN(p) {
			t.Errorf("%s: EnsureRoles wrote %v, attached %v", region, f.inline, f.policyARNs)
		}
		f = &fakeIAM{}
		if err := EnsureRuntimeRole(context.Background(), f, region, ptAccount, ptAuth(p)); err != nil {
			t.Fatal(err)
		}
		if f.inline[RoleName+"/"+PolicyName] != want {
			t.Errorf("%s: EnsureRuntimeRole wrote another policy", region)
		}
	}
}

// TestEnsureRolesRefuseUnsupportedPartitions: in aws-cn (not yet supported),
// the ISO partitions and the European Sovereign Cloud nothing is created or
// written, and the error says why.
func TestEnsureRolesRefuseUnsupportedPartitions(t *testing.T) {
	for region, want := range map[string]error{
		"cn-north-1": awsarn.ErrPartitionNotYetSupported, "cn-northwest-1": awsarn.ErrPartitionNotYetSupported,
		"us-iso-east-1": awsarn.ErrUnsupportedPartition, "eu-isoe-west-1": awsarn.ErrUnsupportedPartition,
		"eusc-de-east-1": awsarn.ErrUnsupportedPartition,
	} {
		f := &fakeIAM{}
		_, err1 := EnsureSetupRoles(context.Background(), f, region, ptAccount, Authorizations{})
		err2 := EnsureRoles(context.Background(), f, region, ptAccount)
		err3 := EnsureRuntimeRole(context.Background(), f, region, ptAccount, Authorizations{})
		for i, err := range []error{err1, err2, err3} {
			if !errors.Is(err, want) || !strings.Contains(err.Error(), region) {
				t.Errorf("%s (%d): %v", region, i, err)
			}
		}
		if len(f.created) != 0 || f.calls != 0 || len(f.attached) != 0 {
			t.Errorf("%s: wrote something: created %v, %d puts, attached %v", region, f.created, f.calls, f.attached)
		}
	}
}

// TestDiffRuntimePolicyInEachPartition: doctor's drift check compares a
// deployed policy against the one for its region's partition. The right one
// is no drift; one in the aws partition, there, is missing every ARN grant.
func TestDiffRuntimePolicyInEachPartition(t *testing.T) {
	for region, p := range map[string]string{"us-gov-west-1": awsarn.GovCloud, "cn-north-1": awsarn.China} {
		deployed, _ := PolicyDocumentFor(p, ptAccount, ptAuth(p))
		diff, err := DiffRuntimePolicy(deployed, region, ptAccount)
		if err != nil || !diff.Empty() {
			t.Errorf("%s: the partition's own policy drifts: %+v, %v", region, diff, err)
		}
		wrong, _ := PolicyDocumentFor(awsarn.AWS, ptAccount, Authorizations{InstanceRoles: []string{"my-instance-role"}})
		diff, err = DiffRuntimePolicy(wrong, region, ptAccount)
		if err != nil || diff.Empty() || !contains(diff.MissingActions(), "dynamodb:UpdateItem") || len(diff.Extra) == 0 {
			t.Errorf("%s: an aws-partition policy is not drift: %+v, %v", region, diff, err)
		}
		for _, g := range diff.Missing {
			if strings.HasPrefix(g.Resource, "arn:") && !strings.HasPrefix(g.Resource, "arn:"+p+":") {
				t.Errorf("%s: missing grant outside the partition: %s", region, g)
			}
		}
	}
	if _, err := DiffRuntimePolicy(mustPolicy(t), "us-iso-east-1", ptAccount); !errors.Is(err, awsarn.ErrUnsupportedPartition) {
		t.Errorf("an ISO region: %v", err)
	}
}
