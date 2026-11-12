package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/doctor"
)

// ---- the inventory gate (#232) ---------------------------------------------

// cliUserNotGranted are actions an everyday command can reach that the
// lagotto-cli-user policy deliberately does not grant, each with why. Empty
// today: every reachable action is granted, resource-scoped. An entry must
// still be reachable and must not be granted; the gate checks both.
var cliUserNotGranted = map[string]string{
	// Item access is owner-scoped (dynamodb:LeadingKeys under the caller's own
	// prefix), and a Scan has no partition key to scope: it would return every
	// owner's records. doctor's table checks that scan report "not checked"
	// for a CLI user; an administrator's doctor runs them.
	"dynamodb:Scan": "owner-scoped item access: a Scan reads every owner's records",
}

// cliUserPolicyProblems is the gate's decision, in both directions:
//
//   - every action an everyday command can reach is granted (or listed in
//     notGranted with a reason), so a new AWS call on an everyday path cannot
//     ship without the policy following;
//   - every granted action is one some everyday command reaches, so the
//     policy cannot grow a permission nothing uses.
//
// byCommand is the generated inventory; granted the policy's actions.
func cliUserPolicyProblems(byCommand map[string]map[string]bool, everyday, granted []string, notGranted map[string]string) []string {
	var problems []string
	needed := map[string][]string{}
	for _, c := range everyday {
		acts, ok := byCommand[c]
		if !ok {
			problems = append(problems, fmt.Sprintf("cliiam.EverydayCommands names %q, which is not a command (renamed?)", c))
			continue
		}
		for a := range acts {
			needed[a] = append(needed[a], c)
		}
	}
	isGranted := map[string]bool{}
	for _, g := range granted {
		isGranted[g] = true
	}
	for a, cmds := range needed {
		if isGranted[a] {
			continue
		}
		if _, ok := notGranted[a]; ok {
			continue
		}
		sort.Strings(cmds)
		problems = append(problems, fmt.Sprintf("%s calls %s, but the %s policy does not grant it. Grant it, "+
			"resource-scoped, in pkg/cliiam statements, or add it to cliUserNotGranted with the reason.",
			strings.Join(cmds, ", "), a, cliiam.PolicyName))
	}
	for _, g := range granted {
		if _, ok := needed[g]; !ok {
			problems = append(problems, fmt.Sprintf("the %s policy grants %s, which no everyday command calls; remove it "+
				"(or, if a command needs it, the inventory is broken)", cliiam.PolicyName, g))
		}
	}
	for a, reason := range notGranted {
		if strings.TrimSpace(reason) == "" {
			problems = append(problems, fmt.Sprintf("cliUserNotGranted[%s] has no reason", a))
		}
		if isGranted[a] {
			problems = append(problems, fmt.Sprintf("cliUserNotGranted lists %s, which the policy grants; remove the entry", a))
		}
		if _, ok := needed[a]; !ok {
			problems = append(problems, fmt.Sprintf("cliUserNotGranted lists %s, which no everyday command calls; remove the stale entry", a))
		}
	}
	sort.Strings(problems)
	return problems
}

// checkCLIUserPolicy runs the gate against the generated inventory. Called
// from TestCLIActionInventoryIsCurrent, which owns the (slow) discovery.
func checkCLIUserPolicy(t *testing.T, byCommand map[string]map[string]bool) {
	t.Helper()
	// Every command is classified exactly once.
	everyday := map[string]bool{}
	for _, c := range cliiam.EverydayCommands {
		everyday[c] = true
		if _, admin := nonEverydayCommands[c]; admin {
			t.Errorf("%s is both an everyday command (cliiam.EverydayCommands) and in nonEverydayCommands", c)
		}
	}
	for c := range byCommand {
		if _, admin := nonEverydayCommands[c]; !admin && !everyday[c] {
			t.Errorf("%s is neither in cliiam.EverydayCommands nor in nonEverydayCommands; classify it "+
				"(an everyday command's actions go into the %s policy)", c, cliiam.PolicyName)
		}
	}
	// A config bucket, so the conditional s3:GetObject grant is in the set.
	granted := cliiam.GrantedActions("123456789012", []string{"config-bucket"})
	for _, p := range cliUserPolicyProblems(byCommand, cliiam.EverydayCommands, granted, cliUserNotGranted) {
		t.Error(p)
	}
}

// TestCLIUserPolicyProblemsDecision pins the gate's rules without the
// discovery, including both revert cases.
func TestCLIUserPolicyProblemsDecision(t *testing.T) {
	inv := map[string]map[string]bool{
		"lagotto watch":  {"dynamodb:PutItem": true, "scheduler:UpdateSchedule": true},
		"lagotto cancel": {"ec2:CancelCapacityReservation": true},
		"lagotto setup":  {"iam:CreatePolicy": true},
	}
	everyday := []string{"lagotto watch", "lagotto cancel"}
	all := []string{"dynamodb:PutItem", "ec2:CancelCapacityReservation", "scheduler:UpdateSchedule"}
	if p := cliUserPolicyProblems(inv, everyday, all, nil); len(p) != 0 {
		t.Errorf("a matching policy reported problems: %v", p)
	}

	// An everyday command gains a call the policy does not grant.
	p := cliUserPolicyProblems(inv, everyday, []string{"dynamodb:PutItem", "ec2:CancelCapacityReservation"}, nil)
	if len(p) != 1 || !strings.Contains(p[0], "lagotto watch calls scheduler:UpdateSchedule") {
		t.Errorf("a missing grant: %v", p)
	}
	// The policy grants what no everyday command calls (an admin action here).
	p = cliUserPolicyProblems(inv, everyday, append(all, "iam:CreatePolicy"), nil)
	if len(p) != 1 || !strings.Contains(p[0], "grants iam:CreatePolicy, which no everyday command calls") {
		t.Errorf("an extra grant: %v", p)
	}
	// Allowlist hygiene and a renamed command.
	p = cliUserPolicyProblems(inv, append(everyday, "lagotto gone"), []string{"dynamodb:PutItem", "ec2:CancelCapacityReservation"},
		map[string]string{"scheduler:UpdateSchedule": "", "dynamodb:PutItem": "x", "s3:GetObject": "y"})
	joined := strings.Join(p, "\n")
	for _, want := range []string{`names "lagotto gone"`, "cliUserNotGranted[scheduler:UpdateSchedule] has no reason",
		"lists dynamodb:PutItem, which the policy grants", "lists s3:GetObject, which no everyday command calls"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in:\n%s", want, joined)
		}
	}
}

// ---- setup -----------------------------------------------------------------

// cmdPolicyFake is a minimal managed policy for setup and teardown.
type cmdPolicyFake struct {
	exists   bool
	tagged   bool
	doc      string
	versions int
	attached []string // "user/x", "group/x", "role/x"
	calls    []string
	getErr   error
}

func (f *cmdPolicyFake) GetPolicy(context.Context, *iam.GetPolicyInput, ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if !f.exists {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("none")}
	}
	p := &iamtypes.Policy{DefaultVersionId: aws.String(fmt.Sprintf("v%d", f.versions))}
	if f.tagged {
		p.Tags = []iamtypes.Tag{{Key: aws.String(cliiam.ManagedTagKey), Value: aws.String(cliiam.ManagedTagValue)}}
	}
	return &iam.GetPolicyOutput{Policy: p}, nil
}

func (f *cmdPolicyFake) GetPolicyVersion(_ context.Context, in *iam.GetPolicyVersionInput, _ ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return &iam.GetPolicyVersionOutput{PolicyVersion: &iamtypes.PolicyVersion{VersionId: in.VersionId,
		IsDefaultVersion: true, Document: aws.String(url.QueryEscape(f.doc))}}, nil
}

func (f *cmdPolicyFake) CreatePolicy(_ context.Context, in *iam.CreatePolicyInput, _ ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	f.calls = append(f.calls, "CreatePolicy")
	f.exists, f.tagged, f.doc, f.versions = true, true, aws.ToString(in.PolicyDocument), 1
	return &iam.CreatePolicyOutput{}, nil
}

func (f *cmdPolicyFake) CreatePolicyVersion(_ context.Context, in *iam.CreatePolicyVersionInput, _ ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	f.calls = append(f.calls, "CreatePolicyVersion")
	f.doc = aws.ToString(in.PolicyDocument)
	f.versions++
	return &iam.CreatePolicyVersionOutput{}, nil
}

func (f *cmdPolicyFake) versionList() []iamtypes.PolicyVersion {
	var out []iamtypes.PolicyVersion
	for i := 1; i <= f.versions; i++ {
		out = append(out, iamtypes.PolicyVersion{VersionId: aws.String(fmt.Sprintf("v%d", i)), IsDefaultVersion: i == f.versions})
	}
	return out
}

func (f *cmdPolicyFake) ListPolicyVersions(context.Context, *iam.ListPolicyVersionsInput, ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return &iam.ListPolicyVersionsOutput{Versions: f.versionList()}, nil
}

func (f *cmdPolicyFake) DeletePolicyVersion(_ context.Context, in *iam.DeletePolicyVersionInput, _ ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	f.calls = append(f.calls, "DeletePolicyVersion "+aws.ToString(in.VersionId))
	return &iam.DeletePolicyVersionOutput{}, nil
}

func (f *cmdPolicyFake) ListEntitiesForPolicy(context.Context, *iam.ListEntitiesForPolicyInput, ...func(*iam.Options)) (*iam.ListEntitiesForPolicyOutput, error) {
	out := &iam.ListEntitiesForPolicyOutput{}
	for _, a := range f.attached {
		kind, name, _ := strings.Cut(a, "/")
		switch kind {
		case "user":
			out.PolicyUsers = append(out.PolicyUsers, iamtypes.PolicyUser{UserName: aws.String(name)})
		case "group":
			out.PolicyGroups = append(out.PolicyGroups, iamtypes.PolicyGroup{GroupName: aws.String(name)})
		case "role":
			out.PolicyRoles = append(out.PolicyRoles, iamtypes.PolicyRole{RoleName: aws.String(name)})
		}
	}
	return out, nil
}

func (f *cmdPolicyFake) DetachUserPolicy(_ context.Context, in *iam.DetachUserPolicyInput, _ ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	f.calls = append(f.calls, "detach user/"+aws.ToString(in.UserName))
	return &iam.DetachUserPolicyOutput{}, nil
}

func (f *cmdPolicyFake) DetachGroupPolicy(_ context.Context, in *iam.DetachGroupPolicyInput, _ ...func(*iam.Options)) (*iam.DetachGroupPolicyOutput, error) {
	f.calls = append(f.calls, "detach group/"+aws.ToString(in.GroupName))
	return &iam.DetachGroupPolicyOutput{}, nil
}

func (f *cmdPolicyFake) DetachRolePolicy(_ context.Context, in *iam.DetachRolePolicyInput, _ ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	f.calls = append(f.calls, "detach role/"+aws.ToString(in.RoleName))
	return &iam.DetachRolePolicyOutput{}, nil
}

func (f *cmdPolicyFake) DeletePolicy(context.Context, *iam.DeletePolicyInput, ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	f.calls = append(f.calls, "DeletePolicy")
	f.exists = false
	return &iam.DeletePolicyOutput{}, nil
}

// fakePolicyDeleter is the teardown default: no policy deployed.
type fakePolicyDeleter = cmdPolicyFake

// stubPolicyDeleter points teardown at f and at a fixed account.
func stubPolicyDeleter(t *testing.T, f *cmdPolicyFake) {
	t.Helper()
	oldDel, oldAcct := newPolicyDeleter, setupCallerAccount
	newPolicyDeleter = func(aws.Config) cliiam.DeleteAPI { return f }
	setupCallerAccount = func(context.Context, aws.Config) (string, error) { return "123456789012", nil }
	t.Cleanup(func() { newPolicyDeleter, setupCallerAccount = oldDel, oldAcct })
}

func setConfigBucketFlags(t *testing.T, add, revoke []string) {
	t.Helper()
	oldAdd, oldRevoke := setupConfigBuckets, setupRevokeConfigBuckets
	setupConfigBuckets, setupRevokeConfigBuckets = add, revoke
	t.Cleanup(func() { setupConfigBuckets, setupRevokeConfigBuckets = oldAdd, oldRevoke })
}

func TestApplyCLIUserPolicyCreatesAndReportsTheAttachCommand(t *testing.T) {
	setConfigBucketFlags(t, []string{"configs"}, nil)
	f := &cmdPolicyFake{}
	var out bytes.Buffer
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Created IAM policy lagotto-cli-user") ||
		!strings.Contains(out.String(), "aws iam attach-group-policy --group-name <group> --policy-arn arn:aws:iam::123456789012:policy/lagotto-cli-user") ||
		!strings.Contains(out.String(), "readable from: configs") {
		t.Errorf("output:\n%s", out.String())
	}
	if !strings.Contains(f.doc, "arn:aws:s3:::configs/*") {
		t.Errorf("the config bucket was not granted:\n%s", f.doc)
	}
}

// TestApplyCLIUserPolicyCarriesConfigBucketsForward: setup rewrites the policy
// wholesale, so a plain re-run must not revoke a bucket granted earlier (the
// #170 footgun), and revoke wins over add.
func TestApplyCLIUserPolicyCarriesConfigBucketsForward(t *testing.T) {
	setConfigBucketFlags(t, []string{"first", "second"}, nil)
	f := &cmdPolicyFake{}
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}

	setConfigBucketFlags(t, nil, nil)
	var out bytes.Buffer
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "is up to date") || strings.Join(f.calls, ",") != "CreatePolicy" {
		t.Errorf("a plain re-run changed the policy (calls %v):\n%s", f.calls, out.String())
	}

	setConfigBucketFlags(t, []string{"third", "second"}, []string{"second", "never-granted"})
	out.Reset()
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.doc, "first/*") || !strings.Contains(f.doc, "third/*") || strings.Contains(f.doc, "second/*") {
		t.Errorf("buckets after revoke:\n%s", f.doc)
	}
	if !strings.Contains(out.String(), `config bucket "never-granted" was not granted`) || !strings.Contains(out.String(), "new default version") {
		t.Errorf("output:\n%s", out.String())
	}
}

func TestApplyCLIUserPolicyRefusesToGuess(t *testing.T) {
	setConfigBucketFlags(t, nil, nil)
	f := &cmdPolicyFake{getErr: errors.New("AccessDenied: iam:GetPolicy")}
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &bytes.Buffer{}); err == nil || len(f.calls) != 0 {
		t.Errorf("an unreadable policy was overwritten (err %v, calls %v)", err, f.calls)
	}
	f = &cmdPolicyFake{exists: true, versions: 1, doc: `{"Version":"2012-10-17","Statement":[]}`}
	if err := applyCLIUserPolicy(context.Background(), f, "123456789012", &bytes.Buffer{}); !errors.Is(err, cliiam.ErrUnmanagedPolicy) {
		t.Errorf("an untagged same-name policy: err %v", err)
	}
	setConfigBucketFlags(t, []string{"bad*"}, nil)
	if err := applyCLIUserPolicy(context.Background(), &cmdPolicyFake{}, "123456789012", &bytes.Buffer{}); err == nil {
		t.Error("a wildcard bucket was accepted")
	}
}

func TestSetupHasConfigBucketFlags(t *testing.T) {
	for _, name := range []string{"config-bucket", "revoke-config-bucket"} {
		if setupCmd.Flags().Lookup(name) == nil {
			t.Errorf("setup has no --%s", name)
		}
	}
	if newSetupPolicyClient(aws.Config{Region: "us-east-1"}) == nil || newPolicyDeleter(aws.Config{Region: "us-east-1"}) == nil {
		t.Error("a policy client seam is nil")
	}
}

// ---- teardown --------------------------------------------------------------

// TestRunTeardown_IAMRolesDeletesTheCLIUserPolicy: the named deleter (#232),
// detaching from everyone first, and only with --iam-roles.
func TestRunTeardown_IAMRolesDeletesTheCLIUserPolicy(t *testing.T) {
	tables := &fakeTables{empty: true}
	stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, true, false)
	stubPollerRegions(t, nil, nil)
	pol := &cmdPolicyFake{exists: true, tagged: true, versions: 2, attached: []string{"group/researchers", "role/ci"}}
	stubPolicyDeleter(t, pol)

	out, err := runTeardownForTest(t)
	if err != nil {
		t.Fatal(err)
	}
	want := "detach group/researchers,detach role/ci,DeletePolicyVersion v1,DeletePolicy"
	if got := strings.Join(pol.calls, ","); got != want {
		t.Errorf("calls %s, want %s", got, want)
	}
	for _, line := range []string{"Detached IAM policy lagotto-cli-user from group/researchers",
		"Detached IAM policy lagotto-cli-user from role/ci", "Deleted IAM policy lagotto-cli-user"} {
		if !strings.Contains(out, line) {
			t.Errorf("output lacks %q:\n%s", line, out)
		}
	}

	// Without --iam-roles the policy is untouched.
	stubTeardown(t, tables, &cmdRoleDeleter{exists: map[string]bool{}}, false, false)
	pol = &cmdPolicyFake{exists: true, tagged: true, versions: 1, attached: []string{"user/alice"}}
	stubPolicyDeleter(t, pol)
	if _, err := runTeardownForTest(t); err != nil {
		t.Fatal(err)
	}
	if len(pol.calls) != 0 {
		t.Errorf("teardown without --iam-roles touched the policy: %v", pol.calls)
	}
}

func TestDeleteCLIUserPolicyReportsAbsentAndUnmanaged(t *testing.T) {
	var out bytes.Buffer
	if err := deleteCLIUserPolicy(context.Background(), &cmdPolicyFake{}, "123456789012", &out); err != nil ||
		!strings.Contains(out.String(), "No IAM policy lagotto-cli-user to delete") {
		t.Errorf("absent: %v\n%s", err, out.String())
	}
	out.Reset()
	f := &cmdPolicyFake{exists: true, versions: 1}
	if err := deleteCLIUserPolicy(context.Background(), f, "123456789012", &out); err != nil ||
		!strings.Contains(out.String(), "Left IAM policy lagotto-cli-user in place") || len(f.calls) != 0 {
		t.Errorf("unmanaged: %v, calls %v\n%s", err, f.calls, out.String())
	}
}

// ---- doctor ----------------------------------------------------------------

type fakeSimulator struct {
	deny   map[string]bool
	err    error
	policy []string
}

func (f *fakeSimulator) SimulatePrincipalPolicy(_ context.Context, in *iam.SimulatePrincipalPolicyInput, _ ...func(*iam.Options)) (*iam.SimulatePrincipalPolicyOutput, error) {
	f.policy = append(f.policy, aws.ToString(in.PolicySourceArn))
	if f.err != nil {
		return nil, f.err
	}
	out := &iam.SimulatePrincipalPolicyOutput{}
	for _, a := range in.ActionNames {
		d := iamtypes.PolicyEvaluationDecisionTypeAllowed
		if f.deny[a] {
			d = iamtypes.PolicyEvaluationDecisionTypeImplicitDeny
		}
		ev := iamtypes.EvaluationResult{EvalActionName: aws.String(a), EvalResourceName: aws.String("*"), EvalDecision: d}
		for _, r := range in.ResourceArns {
			ev.ResourceSpecificResults = append(ev.ResourceSpecificResults,
				iamtypes.ResourceSpecificResult{EvalResourceName: aws.String(r), EvalResourceDecision: d})
		}
		out.EvaluationResults = append(out.EvaluationResults, ev)
	}
	return out, nil
}

func (f *fakeSimulator) GetRole(context.Context, *iam.GetRoleInput, ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	return nil, errors.New("not used")
}

func TestAppendCallerCheck(t *testing.T) {
	report := &doctor.Report{}
	sim := &fakeSimulator{deny: map[string]bool{"iam:PassRole": true}}
	appendCallerCheck(context.Background(), report, sim, "arn:aws:sts::123456789012:assumed-role/Dev/alice", "123456789012", "us-east-1")
	c := report.Checks[len(report.Checks)-1]
	if c.Name != doctor.CheckCallerPermissions || c.Status != doctor.StatusWarn ||
		!strings.Contains(strings.Join(c.Detail, "\n"), "iam:PassRole on arn:aws:iam::123456789012:role/lagotto-capacity-poller-scheduler-invoke") ||
		!strings.Contains(strings.Join(c.Fix, "\n"), "aws iam attach-role-policy --role-name Dev") {
		t.Errorf("check = %+v", c)
	}
	if sim.policy[0] != "arn:aws:iam::123456789012:role/Dev" {
		t.Errorf("simulated %s, not the role", sim.policy[0])
	}

	report = &doctor.Report{}
	appendCallerCheck(context.Background(), report, &fakeSimulator{err: errors.New("api error AccessDenied: no")},
		"arn:aws:iam::123456789012:user/bob", "123456789012", "us-east-1")
	if c := report.Checks[0]; c.Status != doctor.StatusWarn || !strings.HasPrefix(c.Summary, "unknown:") || report.Failed() {
		t.Errorf("an unsimulable caller: %+v", c)
	}
}

// TestCLIUserPolicyGrantsLaunchCancel (#279): `cancel sl-…` deletes and lists
// per-launch schedules, the policy grants both, and the gate fails if the
// DeleteSchedule grant is dropped.
func TestCLIUserPolicyGrantsLaunchCancel(t *testing.T) {
	granted := cliiam.GrantedActions("123456789012", nil)
	has := map[string]bool{}
	for _, a := range granted {
		has[a] = true
	}
	for _, a := range []string{"scheduler:DeleteSchedule", "scheduler:ListSchedules"} {
		if !has[a] {
			t.Errorf("the %s policy does not grant %s, which `lagotto cancel sl-…` calls", cliiam.PolicyName, a)
		}
	}
	inv := map[string]map[string]bool{"lagotto cancel": {"scheduler:DeleteSchedule": true, "scheduler:ListSchedules": true}}
	p := cliUserPolicyProblems(inv, []string{"lagotto cancel"}, []string{"scheduler:ListSchedules"}, nil)
	if len(p) != 1 || !strings.Contains(p[0], "lagotto cancel calls scheduler:DeleteSchedule") {
		t.Errorf("dropping the DeleteSchedule grant: %v", p)
	}
}
