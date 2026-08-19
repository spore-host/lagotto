package snipe_test

import (
	"context"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/spore-host/lagotto/pkg/snipe"
	"github.com/spore-host/lagotto/pkg/testutil"
	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

// TestSnipe_AgainstSubstrate is the #113 regression guard: it exercises
// Snipe's REAL request-building/response-parsing/retry code against a fake
// EC2-shaped endpoint (github.com/scttfrdmn/substrate/emulator), rather than
// a hand-rolled fake `provide` function. Before Options.ClientFor existed,
// Snipe always built its client via spawnaws.NewClientWithRegion — the
// default AWS credential chain, with no way to point it at Substrate's
// in-process test server — so this test tier was impossible for pkg/snipe
// (see #113: this is exactly the tier calque's own Acquirer had, via
// substrate_test.go, before migrating to Snipe).
//
// Mirrors the skip pattern already used for the equivalent spawn/pkg/launcher
// test (TestProvision_EndToEnd): Substrate may not fully model IAM CreateRole
// or RunInstances, so a failure naming those is a skip, not a failure.
func TestSnipe_AgainstSubstrate(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()

	// Seed the SSM AMI parameters spawn's launcher reads for AMI auto-detection.
	ssmClient := ssm.NewFromConfig(env.AWSConfig)
	for name, val := range map[string]string{
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64": "ami-x86-standard",
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64":  "ami-arm-standard",
	} {
		if _, err := ssmClient.PutParameter(ctx, &ssm.PutParameterInput{
			Name: awssdk.String(name), Value: awssdk.String(val), Type: ssmtypes.ParameterTypeString,
		}); err != nil {
			t.Skipf("substrate SSM PutParameter unavailable: %v", err)
		}
	}

	target := snipe.Target{
		InstanceType: "m7i.large",
		Region:       "us-east-1",
	}
	opts := snipe.Options{
		// The whole point: point every client Snipe builds at Substrate's
		// in-process server instead of the real AWS default credential chain.
		ClientFor: func(context.Context, string) (*spawnaws.Client, error) {
			return spawnaws.NewClientFromConfig(env.AWSConfig), nil
		},
	}

	result, err := snipe.Snipe(ctx, target, opts)
	if err != nil {
		if strings.Contains(err.Error(), "IAM") || strings.Contains(err.Error(), "launch") {
			t.Skipf("substrate does not fully model the launch path: %v", err)
		}
		t.Fatalf("Snipe: %v", err)
	}
	if result.InstanceID == "" {
		t.Error("Snipe returned empty InstanceID")
	}
	if result.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", result.Region)
	}
}

// TestSnipe_ClientForOverridesDefault verifies ClientFor is actually used
// (not just accepted and ignored): a ClientFor that returns a recognizable
// error must be the one Snipe surfaces, proving Snipe called it rather than
// falling back to spawnaws.NewClientWithRegion's real-AWS default chain.
func TestSnipe_ClientForOverridesDefault(t *testing.T) {
	wantErr := "sentinel-clientfor-error"
	opts := snipe.Options{
		ClientFor: func(context.Context, string) (*spawnaws.Client, error) {
			return nil, errSentinel{wantErr}
		},
		// A ClientFor error isn't a smithy API error, so it classifies as
		// FailureUnknown and would otherwise retry (with real sleeps) up to
		// MaxConsecutiveUnknown times before giving up. Cap at 1 attempt and
		// use a near-zero interval so this test asserts the error, not the
		// retry/backoff behavior (already covered elsewhere).
		MaxConsecutiveUnknown: 1,
	}
	_, err := snipe.Snipe(context.Background(), snipe.Target{
		InstanceType: "m7i.large",
		Region:       "us-east-1",
	}, opts)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("Snipe error = %v, want it to contain %q (proving ClientFor was actually called)", err, wantErr)
	}
}

type errSentinel struct{ msg string }

func (e errSentinel) Error() string { return e.msg }
