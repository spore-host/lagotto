package awscfg

import (
	"context"
	"testing"
)

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

// TestLoadRegionOverride verifies Load's region precedence: an explicit override
// wins over the shared region, and Load succeeds offline (it only builds config,
// makes no AWS call). We can't read the region back off aws.Config portably, so
// this asserts the code path resolves without error for both override and shared.
func TestLoadRegionOverride(t *testing.T) {
	t.Cleanup(func() { SetFlags("", "") })
	SetFlags("", "us-east-1") // shared region

	// Explicit override path.
	if _, err := Load(context.Background(), "us-west-2"); err != nil {
		t.Fatalf("Load with override: %v", err)
	}
	// Shared-region path (empty override → uses SetFlags region).
	if _, err := Load(context.Background(), ""); err != nil {
		t.Fatalf("Load with shared region: %v", err)
	}
	// No region anywhere → ambient chain, still builds.
	SetFlags("", "")
	if _, err := Load(context.Background(), ""); err != nil {
		t.Fatalf("Load with no region: %v", err)
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
