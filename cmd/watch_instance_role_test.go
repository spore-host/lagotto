package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	irTestAccount = "123456789012"
	irTestRegion  = "us-west-2"
	irTestRole    = "loop-node-role"
)

// fakeRoleIAM serves a canned runtime policy (or a canned error) for
// ReadRuntimePolicy, which is the only call the check makes.
type fakeRoleIAM struct {
	policy  string
	getErr  error
	roleErr error
	// roleMissing makes GetRole report NoSuchEntity, which ReadRuntimePolicy uses
	// to tell "never deployed" from "never set up".
	roleMissing bool
}

func (f *fakeRoleIAM) GetRolePolicy(_ context.Context, _ *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.policy == "" {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(f.policy)}, nil
}

func (f *fakeRoleIAM) GetRole(_ context.Context, _ *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if f.roleErr != nil {
		return nil, f.roleErr
	}
	if f.roleMissing {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: aws.String(runtimeiam.RoleName)}}, nil
}

func (f *fakeRoleIAM) CreateRole(_ context.Context, _ *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	return nil, fmt.Errorf("unexpected CreateRole: the check must be read-only")
}

func (f *fakeRoleIAM) AttachRolePolicy(_ context.Context, _ *iam.AttachRolePolicyInput, _ ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return nil, fmt.Errorf("unexpected AttachRolePolicy: the check must be read-only")
}

func (f *fakeRoleIAM) PutRolePolicy(_ context.Context, _ *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	return nil, fmt.Errorf("unexpected PutRolePolicy: the check must be read-only")
}

// watchWithIAMRole builds a watch whose stored spawn config names role (or none).
func watchWithIAMRole(t *testing.T, role string) *watcher.Watch {
	t.Helper()
	cfg := map[string]interface{}{"name": "w", "instancetype": "g6.8xlarge", "region": "us-east-1"}
	if role != "" {
		cfg["iamrole"] = role
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal spawn config: %v", err)
	}
	return &watcher.Watch{WatchID: "w-test", LaunchConfigJSON: raw}
}

func irStockPolicy(t *testing.T) string {
	t.Helper()
	doc, err := runtimeiam.PolicyDocument(irTestRegion, irTestAccount, nil)
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	return doc
}

func irAuthorizedPolicy(t *testing.T, roles ...string) string {
	t.Helper()
	doc, err := runtimeiam.PolicyDocument(irTestRegion, irTestAccount, roles)
	if err != nil {
		t.Fatalf("PolicyDocument: %v", err)
	}
	return doc
}

// TestCheckInstanceRoleRefusesUnauthorized is lagotto#170's fix: the watch is
// refused at creation instead of being stored to wait for capacity it cannot use.
func TestCheckInstanceRoleRefusesUnauthorized(t *testing.T) {
	var out bytes.Buffer
	res, err := checkInstanceRoleAuthorized(context.Background(),
		&fakeRoleIAM{policy: irStockPolicy(t)}, &out, watchWithIAMRole(t, irTestRole), irTestAccount)

	if err == nil {
		t.Fatal("an unauthorized named iam_role must refuse watch creation")
	}
	msg := err.Error()
	for _, want := range []string{
		irTestRole, // names the role
		"lagotto setup --instance-role " + irTestRole, // names the exact remediation
		"iam:GetRole", // names what is missing
		"iam:PassRole",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal must contain %q:\n%s", want, msg)
		}
	}
	if res == nil || res.Authorized || !res.Checked {
		t.Errorf("verdict = %+v, want checked and unauthorized", res)
	}
	if len(res.MissingActions) == 0 {
		t.Error("the verdict must carry the missing actions for -o json")
	}
}

// TestCheckInstanceRoleAcceptsAuthorized — once authorized, creation proceeds
// silently. A check that still complained would train users to ignore it.
func TestCheckInstanceRoleAcceptsAuthorized(t *testing.T) {
	var out bytes.Buffer
	res, err := checkInstanceRoleAuthorized(context.Background(),
		&fakeRoleIAM{policy: irAuthorizedPolicy(t, irTestRole)}, &out, watchWithIAMRole(t, irTestRole), irTestAccount)

	if err != nil {
		t.Fatalf("an authorized role must not refuse: %v", err)
	}
	if res == nil || !res.Authorized || !res.Checked {
		t.Errorf("verdict = %+v, want checked and authorized", res)
	}
	if out.Len() != 0 {
		t.Errorf("an authorized role should print nothing, got:\n%s", out.String())
	}
}

// TestCheckInstanceRoleSkipsWithoutANamedRole: the common case. An empty iam_role
// becomes spawn-instance-<hash>, which the stock policy already covers, so there is
// nothing to check and no reason to call AWS.
func TestCheckInstanceRoleSkipsWithoutANamedRole(t *testing.T) {
	var out bytes.Buffer
	res, err := checkInstanceRoleAuthorized(context.Background(),
		&fakeRoleIAM{getErr: fmt.Errorf("GetRolePolicy must not be called")}, &out, watchWithIAMRole(t, ""), irTestAccount)
	if err != nil || res != nil {
		t.Errorf("no named role => no check; got res=%+v err=%v", res, err)
	}
}

// TestCheckInstanceRoleSkipsWhenNoHostedPoller: a watch may be serviced by a local
// `poll --daemon` under the user's own credentials, so with no hosted poller
// deployed the poller policy says nothing and refusing would be a guess.
func TestCheckInstanceRoleSkipsWhenNoHostedPoller(t *testing.T) {
	for name, fake := range map[string]*fakeRoleIAM{
		"role absent":   {roleMissing: true},
		"policy absent": {}, // role exists, no inline policy
	} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			res, err := checkInstanceRoleAuthorized(context.Background(), fake, &out,
				watchWithIAMRole(t, irTestRole), irTestAccount)
			if err != nil {
				t.Errorf("must not refuse when no hosted poller is deployed: %v", err)
			}
			if res == nil || res.Checked {
				t.Errorf("verdict = %+v, want Checked=false", res)
			}
			if res != nil && res.Note == "" {
				t.Error("an unchecked verdict should explain why")
			}
		})
	}
}

// TestCheckInstanceRoleWarnsWhenPolicyUnreadable: an AccessDenied on
// iam:GetRolePolicy proves nothing about the named role, so warn and proceed —
// refusing here would block watch creation over a missing READ permission.
func TestCheckInstanceRoleWarnsWhenPolicyUnreadable(t *testing.T) {
	var out bytes.Buffer
	res, err := checkInstanceRoleAuthorized(context.Background(),
		&fakeRoleIAM{getErr: fmt.Errorf("AccessDenied: not authorized to perform iam:GetRolePolicy")},
		&out, watchWithIAMRole(t, irTestRole), irTestAccount)

	if err != nil {
		t.Errorf("an unreadable policy must not refuse: %v", err)
	}
	if res == nil || res.Checked {
		t.Errorf("verdict = %+v, want Checked=false", res)
	}
	if !strings.Contains(out.String(), "warning") || !strings.Contains(out.String(), irTestRole) {
		t.Errorf("expected a warning naming the role, got:\n%s", out.String())
	}
}

// TestCheckInstanceRoleSkipsWithoutAnAccountID: the policy's resource ARNs are
// account-scoped, so without a resolved account there is nothing to compare.
func TestCheckInstanceRoleSkipsWithoutAnAccountID(t *testing.T) {
	var out bytes.Buffer
	res, err := checkInstanceRoleAuthorized(context.Background(),
		&fakeRoleIAM{policy: irStockPolicy(t)}, &out, watchWithIAMRole(t, irTestRole), "")
	if err != nil || res != nil {
		t.Errorf("no account id => no check; got res=%+v err=%v", res, err)
	}
}

func TestWatchSpawnConfigIAMRole(t *testing.T) {
	if got := watchSpawnConfigIAMRole(watchWithIAMRole(t, "  padded-role  ")); got != "padded-role" {
		t.Errorf("got %q, want the trimmed role name", got)
	}
	if got := watchSpawnConfigIAMRole(watchWithIAMRole(t, "")); got != "" {
		t.Errorf("got %q, want empty", got)
	}
	if got := watchSpawnConfigIAMRole(nil); got != "" {
		t.Errorf("got %q for a nil watch, want empty", got)
	}
	if got := watchSpawnConfigIAMRole(&watcher.Watch{LaunchConfigJSON: []byte("not json")}); got != "" {
		t.Errorf("got %q for an unparseable config, want empty (never block on a parse failure)", got)
	}
}

// TestWatchSilencesUsageOnFinding mirrors doctor's SilenceUsage test. The #170
// refusal is a multi-line explanation containing the remediation command; cobra's
// full flag block printed above it buries the only part the user needs.
func TestWatchSilencesUsageOnFinding(t *testing.T) {
	if !watchCmd.SilenceUsage {
		t.Error("watch must set SilenceUsage: a refused watch is a finding, not a usage error")
	}
}

// TestRootSilencesCobraErrors guards the duplicate-output fix: Execute() prints the
// error and sets the exit code itself, so cobra must not also print it. A seven-line
// refusal printed twice reads as noise.
func TestRootSilencesCobraErrors(t *testing.T) {
	if !rootCmd.SilenceErrors {
		t.Error("rootCmd must set SilenceErrors: Execute() already prints the error")
	}
}
