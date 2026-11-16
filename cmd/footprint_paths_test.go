package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/testutil"
)

// answering is a config whose clients are answered by fake (testutil.AnswerFrom).
func answering(fake any) aws.Config {
	return aws.Config{Region: "us-east-1", APIOptions: []func(*middleware.Stack) error{testutil.AnswerFrom(fake)}}
}

// probeAnswers is the poller and bucket the doctor probe reads.
type probeAnswers struct{}

func (p *probeAnswers) GetFunction(_ context.Context, _ *lambda.GetFunctionInput) (*lambda.GetFunctionOutput, error) {
	return nil, &lambdatypes.ResourceNotFoundException{Message: aws.String("no function")}
}

func (p *probeAnswers) GetBucketTagging(_ context.Context, in *s3.GetBucketTaggingInput) (*s3.GetBucketTaggingOutput, error) {
	return &s3.GetBucketTaggingOutput{TagSet: []s3types.Tag{{Key: aws.String("lagotto:managed"), Value: aws.String("cli")}}}, nil
}

// TestRegionalArtifactProbe: the doctor probe reads the poller and the bucket
// tags with real SDK clients built for the bucket's region.
func TestRegionalArtifactProbe(t *testing.T) {
	p := regionalArtifactProbe{cfg: answering(&probeAnswers{})}
	ctx := context.Background()
	if exists, err := p.PollerExists(ctx, "eu-west-1"); exists || err != nil {
		t.Errorf("PollerExists = %v, %v", exists, err)
	}
	if tg, err := p.BucketTags(ctx, "eu-west-1", fpAcct); err != nil || tg["lagotto:managed"] != "cli" {
		t.Errorf("BucketTags = %v, %v", tg, err)
	}
}

// iamAnswers has none of lagotto's IAM resources.
type iamAnswers struct{}

func (iamAnswers) GetInstanceProfile(context.Context, *iam.GetInstanceProfileInput) (*iam.GetInstanceProfileOutput, error) {
	return nil, noEntity()
}
func (iamAnswers) GetPolicy(context.Context, *iam.GetPolicyInput) (*iam.GetPolicyOutput, error) {
	return nil, noEntity()
}
func (iamAnswers) ListRolePolicies(context.Context, *iam.ListRolePoliciesInput) (*iam.ListRolePoliciesOutput, error) {
	return nil, noEntity()
}

// TestIAMAccountDeleter: the account-wide deleters reach IAM through
// runtimeiam and cliiam, report an absent resource as absent, and refuse a
// role that is not lagotto's before any call.
func TestIAMAccountDeleter(t *testing.T) {
	d := newTeardownDeleters(answering(iamAnswers{}), fpAcct)
	ctx := context.Background()
	if gone, err := d.Account.DeleteRole(ctx, runtimeiam.RoleName); gone || err != nil {
		t.Errorf("absent role: %v, %v", gone, err)
	}
	if _, err := d.Account.DeleteRole(ctx, "team-gpu"); err == nil {
		t.Error("DeleteRole accepted an administrator's role")
	}
	if gone, err := d.Account.DeleteInstanceProfile(ctx, runtimeiam.InstanceRoleName); gone || err != nil {
		t.Errorf("absent profile: %v, %v", gone, err)
	}
	if res, err := d.Account.DeletePolicy(ctx); !res.Absent || err != nil {
		t.Errorf("absent policy: %+v, %v", res, err)
	}
	// One region's deleters are built once.
	if a, b := d.Region("eu-west-1"), d.Region("eu-west-1"); a != b {
		t.Error("the region's deleters were rebuilt")
	}
}

// TestTeardownPlanFailedStep: a deletion that fails is reported, skips what
// the failed resource depends on, and fails the command.
func TestTeardownPlanFailedStep(t *testing.T) {
	_, rec, _ := stubPlanTeardown(t, fpFacts)
	rec.failTable = "lagotto-watches"
	teardownMode, teardownYes, teardownNoLease = "reap", true, true
	out, err := runTeardownPlanForTest(t, "")
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("err = %v\n%s", err, out)
	}
	if !strings.Contains(out, "failed") || !strings.Contains(out, "AccessDenied") {
		t.Errorf("output does not report the failure:\n%s", out)
	}
	if len(rec.calls) == 0 {
		t.Error("one failure stopped the others")
	}
}

// TestTeardownPlanInputErrors: an unreadable or invalid plan, another
// account's plan, and a footprint that cannot be taken all delete nothing.
func TestTeardownPlanInputErrors(t *testing.T) {
	path, rec, _ := stubPlanTeardown(t, fpFacts)

	teardownPlan = filepath.Join(t.TempDir(), "missing.json")
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "read the plan") {
		t.Errorf("missing plan: %v", err)
	}
	teardownPlan = "-"
	if _, err := runTeardownPlanForTest(t, `{"schema": 99}`); err == nil {
		t.Error("a plan of an unknown schema accepted")
	}
	teardownPlan = filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(teardownPlan, []byte(`{"schema":1,"tool":"lagotto","account":"`+fpAcct+`","resources":[],"warnings":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := runTeardownPlanForTest(t, ""); err != nil || !strings.Contains(out, "nothing to do") {
		t.Errorf("empty plan: %q, %v", out, err)
	}

	teardownPlan = path
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "210987654321", nil }
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "account") {
		t.Errorf("another account's plan: %v", err)
	}
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "", errors.New("expired token") }
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "account ID") {
		t.Errorf("no account: %v", err)
	}
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return fpAcct, nil }
	gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) {
		return inventory.Facts{}, errors.New("DescribeRegions: AccessDenied")
	}
	if _, err := runTeardownPlanForTest(t, ""); err == nil || !strings.Contains(err.Error(), "not deleting anything") {
		t.Errorf("no live footprint: %v", err)
	}
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("no profile")
	})
	if _, err := runTeardownPlanForTest(t, ""); err == nil {
		t.Error("an unloadable config accepted")
	}
	if len(rec.calls) != 0 {
		t.Errorf("deleted %v", rec.calls)
	}
}

// TestFootprintErrors: no region, no account, no config and a failed read
// are each an error, not an empty footprint.
func TestFootprintErrors(t *testing.T) {
	stubFootprint(t, fpFacts)
	outputFormat = "json"
	run := func() error { return runFootprint(&cobra.Command{}, nil) }

	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "", errors.New("expired token") }
	if err := run(); err == nil || !strings.Contains(err.Error(), "account ID") {
		t.Errorf("no account: %v", err)
	}
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return fpAcct, nil }
	gatherFootprint = func(context.Context, aws.Config, string, inventory.Scope) (inventory.Facts, error) {
		return inventory.Facts{}, errors.New("DescribeRegions: AccessDenied")
	}
	if err := run(); err == nil || !strings.Contains(err.Error(), "footprint") {
		t.Errorf("failed read: %v", err)
	}
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, nil
	})
	if err := run(); err == nil || !strings.Contains(err.Error(), "no AWS region") {
		t.Errorf("no region: %v", err)
	}
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{}, errors.New("no profile")
	})
	if err := run(); err == nil || !strings.Contains(err.Error(), "AWS config") {
		t.Errorf("no config: %v", err)
	}
}

// TestWriteTeardownResultJSONReap: reap's JSON names each step's region and
// error, and an in-use refusal says so.
func TestWriteTeardownResultJSONReap(t *testing.T) {
	plan := &fp.Plan{Steps: []fp.Step{{Resource: fp.Resource{ARN: "a", Region: "eu-west-1"}}}}
	res := &fp.Result{Mode: fp.ModeReap, Actions: []fp.Action{{ARN: "a", Outcome: fp.OutcomeFailed, Err: inventory.ErrInUse}}}
	var b strings.Builder
	if err := writeTeardownResult(&b, plan, nil, res, nil, "table"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"eu-west-1", "in use since the plan was made", "reap: deleted 0", "1 failed"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, b.String())
		}
	}
}
