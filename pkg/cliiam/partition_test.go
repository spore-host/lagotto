package cliiam

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/spore-host/lagotto/pkg/awsarn"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// The lagotto-cli-user policy in each partition (lagotto#347): the creators'
// global marks reach the control region's lifecycle table under it, so its
// ARNs must be in the account's partition.

// TestCLIPolicyInEachPartition: in aws-us-gov and aws-cn the policy is the aws
// one with every ARN in that partition and nothing else changed; its lease
// grant names the partition's lifecycle tables, in any region (the control
// region's among them), and its ARN is the partition's.
func TestCLIPolicyInEachPartition(t *testing.T) {
	buckets := []string{"my-config-bucket"}
	awsDoc, err := PolicyDocument(awsarn.AWS, testAccount, buckets)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{awsarn.GovCloud, awsarn.China} {
		doc, err := PolicyDocument(p, testAccount, buckets)
		if err != nil {
			t.Fatal(err)
		}
		if doc != strings.ReplaceAll(awsDoc, "arn:aws:", "arn:"+p+":") || strings.Contains(doc, "arn:aws:") {
			t.Errorf("%s: the policy is not the aws one in %s:\n%s", p, p, doc)
		}
		grants, err := runtimeiam.ParsePolicy(doc)
		if err != nil {
			t.Fatal(err)
		}
		lease := "arn:" + p + ":dynamodb:*:" + testAccount + ":table/" + runtimeiam.LifecycleTableName
		for _, action := range []string{"dynamodb:UpdateItem", "dynamodb:ConditionCheckItem", "dynamodb:CreateTable", "dynamodb:DeleteItem"} {
			if !granted(grants, action, lease) {
				t.Errorf("%s: no %s on %s", p, action, lease)
			}
		}
		for _, g := range grants {
			if g.Resource != "*" && !strings.HasPrefix(g.Resource, "arn:"+p+":") {
				t.Errorf("%s: grant %s is not in the partition", p, g)
			}
		}
		if got, want := PolicyARN(p, testAccount), "arn:"+p+":iam::"+testAccount+":policy/lagotto-cli-user"; got != want {
			t.Errorf("PolicyARN(%s) = %q, want %q", p, got, want)
		}
		// setup carries the config buckets forward from a deployed policy of
		// any partition.
		if got, err := ConfigBuckets(doc); err != nil || len(got) != 1 || got[0] != "my-config-bucket" {
			t.Errorf("%s: ConfigBuckets = %v, %v", p, got, err)
		}
	}
	if _, err := PolicyDocument("", testAccount, nil); err == nil {
		t.Error("a policy with no partition")
	}
	// The inventory of granted actions is the same in every partition.
	if got := GrantedActions(testAccount, buckets); len(got) == 0 {
		t.Error("no granted actions")
	}
}

func granted(grants []runtimeiam.Grant, action, resource string) bool {
	for _, g := range grants {
		if g.Action == action && g.Resource == resource && strings.EqualFold(g.Effect, "Allow") {
			return true
		}
	}
	return false
}

// TestConfigBucketsIgnoresNonS3: only S3 object grants are config buckets.
func TestConfigBucketsIgnoresNonS3(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":["not-an-arn","arn:aws-cn:dynamodb:*:1:table/x","arn:aws-us-gov:s3:::gov-bucket/*","arn:aws-cn:s3:::no-suffix"]}]}`
	got, err := ConfigBuckets(doc)
	if err != nil || len(got) != 1 || got[0] != "gov-bucket" {
		t.Errorf("ConfigBuckets = %v, %v", got, err)
	}
}

// arnRecorder records the policy ARNs IAM is asked about.
type arnRecorder struct {
	*fakeIAM
	arns []string
}

func (r *arnRecorder) GetPolicy(ctx context.Context, in *iam.GetPolicyInput, o ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	r.arns = append(r.arns, aws.ToString(in.PolicyArn))
	return r.fakeIAM.GetPolicy(ctx, in, o...)
}

func (r *arnRecorder) DeletePolicy(ctx context.Context, in *iam.DeletePolicyInput, o ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	r.arns = append(r.arns, aws.ToString(in.PolicyArn))
	return r.fakeIAM.DeletePolicy(ctx, in, o...)
}

// TestPolicyLifecycleInEachPartition: setup writes, reads and teardown
// deletes the policy by its ARN in the account's partition.
func TestPolicyLifecycleInEachPartition(t *testing.T) {
	for _, p := range []string{awsarn.GovCloud, awsarn.China} {
		want := PolicyARN(p, testAccount)
		doc, _ := PolicyDocument(p, testAccount, nil)
		r := &arnRecorder{fakeIAM: &fakeIAM{}}
		out, err := EnsurePolicy(context.Background(), r, p, testAccount, doc)
		if err != nil || !out.Created || out.ARN != want {
			t.Fatalf("%s: EnsurePolicy = %+v, %v", p, out, err)
		}
		if _, found, err := ReadPolicy(context.Background(), r, p, testAccount); err != nil || !found {
			t.Fatalf("%s: ReadPolicy: %v %v", p, found, err)
		}
		if d, err := DeletePolicy(context.Background(), r, p, testAccount); err != nil || !d.Deleted {
			t.Fatalf("%s: DeletePolicy = %+v, %v", p, d, err)
		}
		for _, a := range r.arns {
			if a != want {
				t.Errorf("%s: asked IAM about %s, want %s", p, a, want)
			}
		}
		if len(r.arns) < 3 {
			t.Errorf("%s: %d ARNs recorded", p, len(r.arns))
		}
	}
}

// TestCheckCallerInTheCallersPartition: doctor simulates the policy's
// resources in the caller's partition (from its ARN), so a GovCloud caller
// is checked against GovCloud ARNs; a caller ARN without one falls back to
// the region's.
func TestCheckCallerInTheCallersPartition(t *testing.T) {
	for _, c := range []struct{ caller, region, partition string }{
		{"arn:aws-us-gov:iam::123456789012:user/alice", "us-gov-west-1", awsarn.GovCloud},
		{"arn:aws-cn:sts::123456789012:assumed-role/Dev/s", "cn-north-1", awsarn.China},
		{"arn::iam::123456789012:user/alice", "us-gov-east-1", awsarn.GovCloud}, // no partition: the region's
	} {
		var resources []string
		f := &fakeIAM{simulate: func(_, _, r string, _ []iamtypes.ContextEntry) bool {
			resources = append(resources, r)
			return true
		}}
		if res := CheckCaller(context.Background(), f, c.caller, testAccount, c.region); res.Status != CallerAllowed {
			t.Fatalf("%s: %+v", c.caller, res)
		}
		for _, r := range resources {
			if r != "" && !strings.HasPrefix(r, "arn:"+c.partition+":") {
				t.Errorf("%s: simulated %s outside %s", c.caller, r, c.partition)
			}
		}
		if len(resources) == 0 {
			t.Errorf("%s: nothing simulated", c.caller)
		}
	}
}
