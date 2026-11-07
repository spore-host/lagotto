package awscfg

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/spore-host/lagotto/pkg/testutil/hermetic"
)

func TestMain(m *testing.M) { hermetic.Main(m) }

// TestSetFlagsResolved verifies the CLI flag values recorded via SetFlags flow
// through to Resolved (the flag layer of flag > env > file > default).
func TestSetFlagsResolved(t *testing.T) {
	t.Cleanup(func() { SetFlags("", "") }) // don't leak state into other tests

	SetFlags("acme-prof", "eu-west-1")
	got := Resolved()
	if got.Profile != "acme-prof" {
		t.Errorf("Profile = %q, want acme-prof", got.Profile)
	}
	if got.Region != "eu-west-1" {
		t.Errorf("Region = %q, want eu-west-1", got.Region)
	}

	// Clearing the flags falls back through env/file/default (no flag override).
	SetFlags("", "")
	if p := Resolved().Profile; p == "acme-prof" {
		t.Error("Profile should no longer be the cleared flag value")
	}
}

// resolvedOptions installs a capturing fake loader and returns the
// LoadOptions that Load(regionOverride) would have handed the real loader.
func resolvedOptions(t *testing.T, regionOverride string) awsconfig.LoadOptions {
	t.Helper()
	var got awsconfig.LoadOptions
	SetLoaderForTest(t, func(_ context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		got = awsconfig.LoadOptions{}
		for _, fn := range optFns {
			if err := fn(&got); err != nil {
				return aws.Config{}, err
			}
		}
		return aws.Config{Region: got.Region}, nil
	})
	if _, err := Load(context.Background(), regionOverride); err != nil {
		t.Fatalf("Load(%q): %v", regionOverride, err)
	}
	return got
}

// TestLoadRegionOverride verifies Load's precedence: an explicit override wins
// over the shared region, the shared region applies when there is no override,
// a shared profile is passed through, and nothing at all means the ambient chain.
func TestLoadRegionOverride(t *testing.T) {
	t.Cleanup(func() { SetFlags("", "") })
	SetFlags("acme-prof", "us-east-1") // shared profile + region

	if o := resolvedOptions(t, "us-west-2"); o.Region != "us-west-2" || o.SharedConfigProfile != "acme-prof" {
		t.Errorf("override: region=%q profile=%q, want us-west-2 / acme-prof", o.Region, o.SharedConfigProfile)
	}
	if o := resolvedOptions(t, ""); o.Region != "us-east-1" {
		t.Errorf("shared: region=%q, want us-east-1", o.Region)
	}
	SetFlags("", "")
	if o := resolvedOptions(t, ""); o.Region != "" || o.SharedConfigProfile != "" {
		t.Errorf("ambient: region=%q profile=%q, want both empty (the hermetic env strips AWS_REGION and AWS_PROFILE)", o.Region, o.SharedConfigProfile)
	}
}

// TestLoadPanicsInATestWithoutAFake is the #214 seam: a test that reaches Load
// without installing a loader must fail at the point it would have built a
// real AWS config, not go on to read and write a live account.
func TestLoadPanicsInATestWithoutAFake(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Load returned under a test with no fake loader; it must panic (#214)")
		}
		if s, _ := r.(string); !strings.Contains(s, "#214") {
			t.Fatalf("panic value %v does not name the hermetic guard", r)
		}
	}()
	_, _ = Load(context.Background(), "us-east-1")
}

// TestSetLoaderForTestRestores: the fake is scoped to the test that installed
// it, so a later test without one still hits the guard.
func TestSetLoaderForTestRestores(t *testing.T) {
	t.Run("installs", func(t *testing.T) {
		_ = resolvedOptions(t, "us-east-1")
	})
	mu.RLock()
	l := testLoader
	mu.RUnlock()
	if l != nil {
		t.Fatal("SetLoaderForTest left its loader installed after the subtest ended")
	}
}

// TestSetFlagsConcurrent exercises the RWMutex under concurrent readers/writers
// (SetFlags is called from PersistentPreRun while handlers call Resolved/Load) —
// run with -race to catch a regression in the locking.
func TestSetFlagsConcurrent(t *testing.T) {
	t.Cleanup(func() { SetFlags("", "") })
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			SetFlags("p", "us-east-1")
		}
		close(done)
	}()
	for i := 0; i < 100; i++ {
		_ = Resolved()
	}
	<-done
}
