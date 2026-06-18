// Package deploy stands up (and tears down) the hosted lagotto capacity-poller
// stack in the caller's own AWS account (#48): it fetches the published poller
// Lambda zip (#29), uploads it to a bucket in the user's account, and deploys the
// embedded CloudFormation/SAM template. This is the "arm a watch, walk away, it's
// genuinely server-side" path that the laptop-bound `poll --daemon` can't be.
package deploy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

const (
	// lambdaAsset is the release asset name attached by GoReleaser (#29).
	lambdaAsset = "capacity-poller_lambda_linux_arm64.zip"
	releaseRepo = "spore-host/lagotto"
)

// Options configure a Deploy.
type Options struct {
	StackName   string // CloudFormation stack name (default "lagotto")
	Region      string
	Version     string // release version to pull the Lambda zip from, e.g. "0.44.0" (no leading v)
	Environment string // SAM Environment param (default "production")
	Bucket      string // S3 bucket for the Lambda zip; empty → derive lagotto-lambda-<account>-<region>
	AccountID   string // caller account (for the derived bucket name + messaging)
	// The CLI-owned DynamoDB table names the stack wires the poller to (#59). The
	// stack references them by name and never creates them; empty falls back to
	// the template defaults (lagotto-watches / -match-history / -scheduled-launches).
	WatchesTable   string
	HistoryTable   string
	ScheduledTable string
}

// LambdaArtifactURL returns the GitHub Release download URL for the poller Lambda
// zip at the given version. Exposed (and pure) so it's unit-testable without network.
func LambdaArtifactURL(version string) string {
	v := strings.TrimPrefix(version, "v")
	return fmt.Sprintf("https://github.com/%s/releases/download/v%s/%s", releaseRepo, v, lambdaAsset)
}

// DefaultBucketName derives the per-account artifact bucket name when the caller
// doesn't supply one. Account + region keep it unique and discoverable.
func DefaultBucketName(accountID, region string) string {
	return fmt.Sprintf("lagotto-lambda-%s-%s", accountID, region)
}

// LambdaObjectKey is the S3 key the Lambda zip is uploaded under for a version.
func LambdaObjectKey(version string) string {
	return fmt.Sprintf("lagotto/capacity-poller-v%s.zip", strings.TrimPrefix(version, "v"))
}

// Deployer performs deploy/teardown against AWS. The httpGet field is indirected
// so tests can stub the release download.
type Deployer struct {
	cfn     *cloudformation.Client
	s3      *s3.Client
	httpGet func(url string) (*http.Response, error)
}

// New builds a Deployer from an AWS config.
func New(cfg aws.Config) *Deployer {
	return &Deployer{
		cfn:     cloudformation.NewFromConfig(cfg),
		s3:      s3.NewFromConfig(cfg),
		httpGet: http.Get,
	}
}

// Deploy creates or updates the stack: ensure the artifact bucket exists, upload
// the release Lambda zip into it, then CreateStack/UpdateStack the embedded
// template pointing at that bucket/key. Returns the resolved stack outputs.
func (d *Deployer) Deploy(ctx context.Context, opts Options) (map[string]string, error) {
	bucket := opts.Bucket
	if bucket == "" {
		bucket = DefaultBucketName(opts.AccountID, opts.Region)
	}
	key := LambdaObjectKey(opts.Version)

	if err := d.ensureBucket(ctx, bucket, opts.Region); err != nil {
		return nil, err
	}
	if err := d.uploadArtifact(ctx, bucket, key, opts.Version); err != nil {
		return nil, err
	}

	env := opts.Environment
	if env == "" {
		env = "production"
	}
	params := []cfntypes.Parameter{
		{ParameterKey: aws.String("Environment"), ParameterValue: aws.String(env)},
		{ParameterKey: aws.String("LambdaCodeBucket"), ParameterValue: aws.String(bucket)},
		{ParameterKey: aws.String("LambdaCodeKey"), ParameterValue: aws.String(key)},
	}
	// Wire the poller to the CLI-owned tables by name (#59). Only set a param when
	// non-empty so the template's defaults still apply for the standard names.
	if opts.WatchesTable != "" {
		params = append(params, cfntypes.Parameter{ParameterKey: aws.String("WatchesTableName"), ParameterValue: aws.String(opts.WatchesTable)})
	}
	if opts.HistoryTable != "" {
		params = append(params, cfntypes.Parameter{ParameterKey: aws.String("HistoryTableName"), ParameterValue: aws.String(opts.HistoryTable)})
	}
	if opts.ScheduledTable != "" {
		params = append(params, cfntypes.Parameter{ParameterKey: aws.String("ScheduledTableName"), ParameterValue: aws.String(opts.ScheduledTable)})
	}
	caps := []cfntypes.Capability{cfntypes.CapabilityCapabilityIam, cfntypes.CapabilityCapabilityAutoExpand}

	if err := d.createOrUpdate(ctx, opts.StackName, params, caps); err != nil {
		return nil, err
	}
	return d.stackOutputs(ctx, opts.StackName)
}

// Teardown deletes the stack and waits for completion.
func (d *Deployer) Teardown(ctx context.Context, stackName string) error {
	if _, err := d.cfn.DeleteStack(ctx, &cloudformation.DeleteStackInput{
		StackName: aws.String(stackName),
	}); err != nil {
		return fmt.Errorf("delete stack %s: %w", stackName, err)
	}
	w := cloudformation.NewStackDeleteCompleteWaiter(d.cfn)
	if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
		return fmt.Errorf("waiting for stack %s deletion: %w", stackName, err)
	}
	return nil
}

func (d *Deployer) ensureBucket(ctx context.Context, bucket, region string) error {
	_, err := d.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil // already exists (and we can reach it)
	}
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	// us-east-1 must NOT set a LocationConstraint; every other region must.
	if region != "" && region != "us-east-1" {
		in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	if _, err := d.s3.CreateBucket(ctx, in); err != nil {
		return fmt.Errorf("create artifact bucket %s: %w", bucket, err)
	}
	return nil
}

func (d *Deployer) uploadArtifact(ctx context.Context, bucket, key, version string) error {
	url := LambdaArtifactURL(version)
	resp, err := d.httpGet(url)
	if err != nil {
		return fmt.Errorf("download poller Lambda %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download poller Lambda %s: HTTP %d (is v%s released?)", url, resp.StatusCode, strings.TrimPrefix(version, "v"))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read poller Lambda zip: %w", err)
	}
	if _, err := d.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(body)),
	}); err != nil {
		return fmt.Errorf("upload poller Lambda to s3://%s/%s: %w", bucket, key, err)
	}
	return nil
}

// failedCreateStates are terminal states a stack can be left in by a failed
// CreateStack. Such a stack can never be updated — it must be deleted and
// recreated. `lagotto deploy` does this automatically so a prior failure (e.g.
// the #59 AlreadyExists rollback) doesn't wedge every subsequent retry.
var failedCreateStates = map[cfntypes.StackStatus]bool{
	cfntypes.StackStatusRollbackComplete: true,
	cfntypes.StackStatusRollbackFailed:   true,
	cfntypes.StackStatusReviewInProgress: true,
	cfntypes.StackStatusCreateFailed:     true,
	cfntypes.StackStatusDeleteFailed:     true,
}

func (d *Deployer) createOrUpdate(ctx context.Context, stackName string, params []cfntypes.Parameter, caps []cfntypes.Capability) error {
	exists, status, err := d.stackState(ctx, stackName)
	if err != nil {
		return err
	}
	// A stack stranded in a failed-create state can't be updated; delete it first
	// so the CreateStack below starts clean (#59).
	if exists && failedCreateStates[status] {
		if err := d.Teardown(ctx, stackName); err != nil {
			return fmt.Errorf("delete prior failed stack %s (status %s) before redeploy: %w", stackName, status, err)
		}
		exists = false
	}
	if exists {
		_, err := d.cfn.UpdateStack(ctx, &cloudformation.UpdateStackInput{
			StackName:    aws.String(stackName),
			TemplateBody: aws.String(cfn.StackTemplate),
			Parameters:   params,
			Capabilities: caps,
		})
		if err != nil {
			// "No updates are to be performed" is a benign no-op, not a failure.
			if strings.Contains(err.Error(), "No updates are to be performed") {
				return nil
			}
			return fmt.Errorf("update stack %s: %w", stackName, err)
		}
		w := cloudformation.NewStackUpdateCompleteWaiter(d.cfn)
		if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
			return fmt.Errorf("waiting for stack %s update: %w", stackName, err)
		}
		return nil
	}
	_, err = d.cfn.CreateStack(ctx, &cloudformation.CreateStackInput{
		StackName:    aws.String(stackName),
		TemplateBody: aws.String(cfn.StackTemplate),
		Parameters:   params,
		Capabilities: caps,
	})
	if err != nil {
		return fmt.Errorf("create stack %s: %w", stackName, err)
	}
	w := cloudformation.NewStackCreateCompleteWaiter(d.cfn)
	if err := w.Wait(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)}, 15*time.Minute); err != nil {
		return fmt.Errorf("waiting for stack %s creation: %w", stackName, err)
	}
	return nil
}

// stackState reports whether the stack exists and, if so, its current status.
// A failed-create status (see failedCreateStates) tells createOrUpdate to delete
// and recreate rather than attempt an impossible UpdateStack (#59).
func (d *Deployer) stackState(ctx context.Context, stackName string) (bool, cfntypes.StackStatus, error) {
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)})
	if err != nil {
		// CloudFormation returns a ValidationError "does not exist" for an absent stack.
		if strings.Contains(err.Error(), "does not exist") {
			return false, "", nil
		}
		return false, "", fmt.Errorf("describe stack %s: %w", stackName, err)
	}
	for _, s := range out.Stacks {
		if aws.ToString(s.StackName) == stackName {
			return true, s.StackStatus, nil
		}
	}
	return false, "", nil
}

// StackOutputs returns the deployed stack's outputs (e.g. the poller function
// ARN and scheduler-invoke-role ARN that `lagotto launch` needs to wire a
// per-launch EventBridge schedule, #49). Errors if the stack isn't deployed.
func (d *Deployer) StackOutputs(ctx context.Context, stackName string) (map[string]string, error) {
	return d.stackOutputs(ctx, stackName)
}

func (d *Deployer) stackOutputs(ctx context.Context, stackName string) (map[string]string, error) {
	out, err := d.cfn.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(stackName)})
	if err != nil {
		return nil, fmt.Errorf("describe stack %s: %w", stackName, err)
	}
	res := map[string]string{}
	for _, s := range out.Stacks {
		for _, o := range s.Outputs {
			res[aws.ToString(o.OutputKey)] = aws.ToString(o.OutputValue)
		}
	}
	return res, nil
}
