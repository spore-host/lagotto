// Package awscfg loads an AWS SDK config for lagotto through the shared
// spore.host config base (libs/sporeconfig), so lagotto resolves the AWS
// profile and default region the same way every other tool does:
// flag > env (SPORE_*/AWS_*) > ~/.config/spore/config.toml > default.
//
// The CLI records its --profile/--region flag values via SetFlags during
// PersistentPreRun; command handlers then call Load instead of
// config.LoadDefaultConfig. An unset profile/region means the ambient AWS chain,
// so lagotto's behavior is unchanged unless the suite config is set.
package awscfg

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/spore-host/libs/sporeconfig"
)

var (
	mu    sync.RWMutex
	flags sporeconfig.Flags
)

// SetFlags records the CLI flag values for shared-config resolution. Called once
// from the root command's PersistentPreRun. Empty fields fall through to
// env/file/default.
func SetFlags(profile, region string) {
	mu.Lock()
	defer mu.Unlock()
	flags = sporeconfig.Flags{Profile: profile, Region: region}
}

// Resolved returns the shared config (profile/region/account/output) using the
// recorded flags plus env/file/default. A malformed config file is tolerated
// (the flag/env/default layers still resolve).
func Resolved() sporeconfig.Config {
	mu.RLock()
	f := flags
	mu.RUnlock()
	cfg, _ := sporeconfig.Resolve(f)
	return cfg
}

// Load builds an aws.Config applying the shared profile and region. An explicit
// regionOverride (e.g. a command's own --region) wins over the shared region;
// pass "" to use the shared region (or the ambient chain if none is set).
func Load(ctx context.Context, regionOverride string) (aws.Config, error) {
	sc := Resolved()

	region := regionOverride
	if region == "" {
		region = sc.Region
	}

	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	if sc.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(sc.Profile))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}
