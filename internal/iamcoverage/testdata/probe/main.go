// Command probe is a fixture for internal/iamcoverage's tests: a tiny program
// whose AWS calls are known, reached in each of the ways the discovery must
// see (a direct call, an interface, a method value, a paginator, an assume-role
// credentials provider) and one it must not (unreachable).
package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type tableGetter interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

func direct(ctx context.Context, c *ec2.Client) {
	_, _ = c.RunInstances(ctx, &ec2.RunInstancesInput{})
}

func viaInterface(ctx context.Context, g tableGetter) {
	_, _ = g.GetItem(ctx, &dynamodb.GetItemInput{})
}

func methodValue(ctx context.Context, c *s3.Client) {
	head := c.HeadBucket
	_, _ = head(ctx, &s3.HeadBucketInput{})
}

func paginator(ctx context.Context, c *ec2.Client) {
	p := ec2.NewDescribeInstancesPaginator(c, &ec2.DescribeInstancesInput{})
	_, _ = p.NextPage(ctx)
}

func assumeRole(c *sts.Client) aws.CredentialsProvider {
	return stscreds.NewAssumeRoleProvider(c, "arn:aws:iam::111122223333:role/x")
}

func identity(ctx context.Context, c *sts.Client) {
	_, _ = c.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
}

// unreachable is never called: its action must not appear.
func unreachable(ctx context.Context, c *ec2.Client) {
	_, _ = c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{})
}

var _ = unreachable

func main() {
	ctx := context.Background()
	cfg := aws.Config{}
	direct(ctx, ec2.NewFromConfig(cfg))
	viaInterface(ctx, dynamodb.NewFromConfig(cfg))
	methodValue(ctx, s3.NewFromConfig(cfg))
	paginator(ctx, ec2.NewFromConfig(cfg))
	_ = assumeRole(sts.NewFromConfig(cfg))
	identity(ctx, sts.NewFromConfig(cfg))
}
