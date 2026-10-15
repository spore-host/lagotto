package cmd

import (
	"context"
	"fmt"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/awscfg"
)

// accountGuardTimeout bounds the one STS call --account costs.
const accountGuardTimeout = 30 * time.Second

// checkAccountGuard enforces --account ("Expected AWS account ID (optional
// guard)"): when the user names an account, the credentials lagotto resolved
// (via --profile / SPORE_PROFILE / AWS_PROFILE / the shared config) must belong
// to it, or the command stops before making any other AWS call. An empty want
// means no guard. Before #179 the flag was parsed and ignored, so a guard the
// user asked for silently never ran — including in front of `--action spawn`,
// which launches billable instances.
//
// Only the explicit flag is enforced. An account set in SPORE_ACCOUNT or the
// shared config file is resolved by libs/sporeconfig but read by nothing in
// lagotto, and whether it should act as a guard is a suite-level question
// (spore-host/truffle#206), so it is not enforced here either.
func checkAccountGuard(ctx context.Context, want string, callerAccount func(context.Context) (string, error)) error {
	if want == "" {
		return nil
	}
	got, err := callerAccount(ctx)
	if err != nil {
		return fmt.Errorf("--account %s: could not determine the AWS account of the current credentials: %w", want, err)
	}
	if got == "" {
		return fmt.Errorf("--account %s: STS returned no account for the current credentials; refusing to continue", want)
	}
	if got != want {
		return fmt.Errorf("--account %s: the current AWS credentials belong to account %s; refusing to continue (check --profile / AWS_PROFILE)", want, got)
	}
	return nil
}

// stsCallerAccount returns the account ID of the credentials lagotto resolves,
// via STS GetCallerIdentity (read-only; needs no IAM permission). It uses the
// shared region so a non-commercial partition resolves the right STS endpoint,
// and falls back to us-east-1 only when no region is configured at all.
func stsCallerAccount(ctx context.Context) (string, error) {
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return "", err
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return awssdk.ToString(out.Account), nil
}

// callerAccount is how runAccountGuard learns the current account; a variable
// so tests can stand in for STS.
var callerAccount = stsCallerAccount

// runAccountGuard is the PersistentPreRunE hook for --account.
func runAccountGuard() error {
	ctx, cancel := context.WithTimeout(context.Background(), accountGuardTimeout)
	defer cancel()
	return checkAccountGuard(ctx, sharedAccount, callerAccount)
}
