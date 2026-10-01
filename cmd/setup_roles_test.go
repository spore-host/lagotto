package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// resetSetupRoleFlags isolates the package-level flag vars between cases.
func resetSetupRoleFlags(t *testing.T, authorize, revoke []string) {
	t.Helper()
	oldA, oldR := setupInstanceRoles, setupRevokeRoles
	setupInstanceRoles, setupRevokeRoles = authorize, revoke
	t.Cleanup(func() { setupInstanceRoles, setupRevokeRoles = oldA, oldR })
}

// TestSetupPreservesAuthorizedRolesOnRerun is the footgun this guards against: the
// policy is written with a wholesale PutRolePolicy, so a plain `lagotto setup` that
// didn't carry authorizations forward would silently revoke them — returning the
// account to exactly the state where watches match capacity and then die (#170).
func TestSetupPreservesAuthorizedRolesOnRerun(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	deployed := irAuthorizedPolicy(t, "loop-node-role", "other-role")

	var out bytes.Buffer
	got, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{policy: deployed}, &out)
	if err != nil {
		t.Fatalf("resolveAuthorizedInstanceRoles: %v", err)
	}
	if len(got) != 2 || got[0] != "loop-node-role" || got[1] != "other-role" {
		t.Errorf("plain setup must carry existing authorizations forward, got %v", got)
	}
}

func TestSetupAddsAndRevokes(t *testing.T) {
	deployed := irAuthorizedPolicy(t, "keep-me", "drop-me")

	t.Run("add", func(t *testing.T) {
		resetSetupRoleFlags(t, []string{"new-role"}, nil)
		var out bytes.Buffer
		got, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{policy: deployed}, &out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if strings.Join(got, ",") != "drop-me,keep-me,new-role" {
			t.Errorf("got %v, want the union, sorted", got)
		}
	})

	t.Run("revoke", func(t *testing.T) {
		resetSetupRoleFlags(t, nil, []string{"drop-me"})
		var out bytes.Buffer
		got, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{policy: deployed}, &out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if strings.Join(got, ",") != "keep-me" {
			t.Errorf("got %v, want only keep-me", got)
		}
	})

	t.Run("revoke wins over add for the same name", func(t *testing.T) {
		resetSetupRoleFlags(t, []string{"drop-me"}, []string{"drop-me"})
		var out bytes.Buffer
		got, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{policy: deployed}, &out)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if strings.Join(got, ",") != "keep-me" {
			t.Errorf("got %v, want revoke to win so the result is order-independent", got)
		}
	})
}

// TestSetupRejectsWildcardRole is the security guard: a wildcard name would be
// interpolated into a Resource ARN and grant iam:PutRolePolicy/PassRole account-wide.
func TestSetupRejectsWildcardRole(t *testing.T) {
	resetSetupRoleFlags(t, []string{"*"}, nil)
	var out bytes.Buffer
	if _, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{policy: irStockPolicy(t)}, &out); err == nil {
		t.Error("--instance-role '*' must be refused")
	}
}

// TestSetupRefusesToGuessWhenPolicyUnreadable: reporting "no authorizations" on an
// AccessDenied would make setup silently NARROW the policy it is about to write.
func TestSetupRefusesToGuessWhenPolicyUnreadable(t *testing.T) {
	resetSetupRoleFlags(t, nil, nil)
	var out bytes.Buffer
	_, err := resolveAuthorizedInstanceRoles(context.Background(),
		&fakeRoleIAM{getErr: fmt.Errorf("AccessDenied")}, &out)
	if err == nil {
		t.Error("an unreadable policy must be an error, not an empty authorization list")
	}
}

// TestSetupStartsCleanOnAFreshAccount: no role/policy yet is not an error — setup
// also runs pre-deploy.
func TestSetupStartsCleanOnAFreshAccount(t *testing.T) {
	resetSetupRoleFlags(t, []string{"new-role"}, nil)
	var out bytes.Buffer
	got, err := resolveAuthorizedInstanceRoles(context.Background(), &fakeRoleIAM{roleMissing: true}, &out)
	if err != nil {
		t.Fatalf("a fresh account must not error: %v", err)
	}
	if len(got) != 1 || got[0] != "new-role" {
		t.Errorf("got %v, want just the flag value", got)
	}
}

var _ = runtimeiam.RoleName
