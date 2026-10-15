package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestAccountGuard pins what --account ("Expected AWS account ID (optional
// guard)") does: nothing when unset, pass on a match, refuse on a mismatch or
// when the account cannot be determined. Before #179 it was never read.
func TestAccountGuard(t *testing.T) {
	stub := func(acct string, err error) func(context.Context) (string, error) {
		return func(context.Context) (string, error) { return acct, err }
	}
	called := false
	if err := checkAccountGuard(context.Background(), "", func(context.Context) (string, error) {
		called = true
		return "", nil
	}); err != nil || called {
		t.Errorf("no --account: err=%v, STS called=%v; want no error and no call", err, called)
	}
	if err := checkAccountGuard(context.Background(), "111122223333", stub("111122223333", nil)); err != nil {
		t.Errorf("matching account: %v", err)
	}
	err := checkAccountGuard(context.Background(), "111122223333", stub("444455556666", nil))
	if err == nil || !strings.Contains(err.Error(), "444455556666") {
		t.Errorf("mismatched account: err=%v, want a refusal naming the actual account", err)
	}
	if err := checkAccountGuard(context.Background(), "111122223333", stub("", errors.New("no credentials"))); err == nil {
		t.Error("STS error: want an error (a guard that cannot check must not pass)")
	}
	if err := checkAccountGuard(context.Background(), "111122223333", stub("", nil)); err == nil {
		t.Error("STS returned no account: want an error (a guard that cannot check must not pass)")
	}
}

// TestAccountFlagRunsTheGuard drives --account through cobra flag parsing and
// the root PersistentPreRunE, so the flag cannot go back to being inert.
func TestAccountFlagRunsTheGuard(t *testing.T) {
	oldCaller, oldAccount := callerAccount, sharedAccount
	t.Cleanup(func() {
		callerAccount, sharedAccount = oldCaller, oldAccount
		if f := rootCmd.PersistentFlags().Lookup("account"); f != nil {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		}
	})
	calls := 0
	callerAccount = func(context.Context) (string, error) { calls++; return "444455556666", nil }

	if err := rootCmd.PersistentFlags().Parse([]string{"--account", "111122223333"}); err != nil {
		t.Fatal(err)
	}
	err := rootCmd.PersistentPreRunE(versionCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "444455556666") {
		t.Errorf("--account 111122223333 with credentials for 444455556666: err=%v, want a refusal", err)
	}
	if calls != 1 {
		t.Errorf("STS consulted %d times, want 1", calls)
	}

	// Matching credentials pass.
	callerAccount = func(context.Context) (string, error) { return "111122223333", nil }
	if err := rootCmd.PersistentPreRunE(versionCmd, nil); err != nil {
		t.Errorf("--account 111122223333 with matching credentials: %v", err)
	}
}

// TestAccountFlagUnsetCostsNoSTSCall: without --account the guard must not touch
// AWS, so offline commands stay offline.
func TestAccountFlagUnsetCostsNoSTSCall(t *testing.T) {
	oldCaller, oldAccount := callerAccount, sharedAccount
	t.Cleanup(func() { callerAccount, sharedAccount = oldCaller, oldAccount })
	sharedAccount = ""
	callerAccount = func(context.Context) (string, error) {
		t.Error("STS called without --account")
		return "", nil
	}
	if err := rootCmd.PersistentPreRunE(versionCmd, nil); err != nil {
		t.Errorf("no --account: %v", err)
	}
}
