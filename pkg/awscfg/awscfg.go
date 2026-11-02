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
	"testing"

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

// LoaderFunc builds an aws.Config from load options. In production it is
// config.LoadDefaultConfig.
type LoaderFunc func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error)

// testLoader is the loader a test installed with SetLoaderForTest; nil means
// none. Guarded by mu.
var testLoader LoaderFunc

// ErrHermetic is the panic value when a test reaches Load without a fake.
const ErrHermetic = "lagotto hermetic guard (#214): awscfg.Load was called from a test without a fake loader. " +
	"Outside a test this builds a REAL AWS config from the ambient profile, and the command would then " +
	"read and write a live account (on 2026-11-01 a test wrote a real watch this way). " +
	"Install a fake with awscfg.SetLoaderForTest(t, ...), or test a pure function instead"

// Load builds an aws.Config applying the shared profile and region. An explicit
// regionOverride (e.g. a command's own --region) wins over the shared region;
// pass "" to use the shared region (or the ambient chain if none is set).
//
// Under `go test` Load PANICS unless the test installed a loader with
// SetLoaderForTest (#214): every CLI command loads its AWS config here, so this
// one seam makes every command path unable to reach a real account from a test.
func Load(ctx context.Context, regionOverride string) (aws.Config, error) {
	return loader()(ctx, loadOptions(regionOverride)...)
}

// loader returns the installed test loader, else the real one. Under a test
// binary with no test loader installed it panics.
func loader() LoaderFunc {
	mu.RLock()
	l := testLoader
	mu.RUnlock()
	if l != nil {
		return l
	}
	if testing.Testing() {
		panic(ErrHermetic)
	}
	return awsconfig.LoadDefaultConfig
}

// loadOptions resolves the shared profile and region into load options.
func loadOptions(regionOverride string) []func(*awsconfig.LoadOptions) error {
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
	return opts
}

// SetLoaderForTest installs fn as Load's loader until t's cleanup. It is the
// only way a test can get past Load (#214). fn receives the options Load
// resolved, so a fake can assert on the region and profile.
func SetLoaderForTest(t testing.TB, fn LoaderFunc) {
	t.Helper()
	if fn == nil {
		t.Fatal("awscfg.SetLoaderForTest: nil loader")
	}
	mu.Lock()
	prev := testLoader
	testLoader = fn
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		testLoader = prev
		mu.Unlock()
	})
}
