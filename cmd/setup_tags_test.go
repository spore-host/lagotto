package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/iamtags"
	"github.com/spore-host/libs/tags/tagtest"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/managedtags"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/testutil"
)

// memIAM is an in-memory IAM for setup's create paths (#324): roles,
// instance profiles and their tags, and the CLI-user managed policy. It
// answers real SDK clients through testutil.AnswerFrom, so the tagtest
// detector sees exactly what setup and spawn send.
type memIAM struct {
	roles    map[string]map[string]string // role -> tags
	profiles map[string]map[string]string // profile -> tags
	inProf   map[string][]string          // profile -> roles
	inline   map[string]string            // role/policy -> doc
	policy   map[string]string            // CLI-user policy tags; nil = absent
	policyV  string
}

func newMemIAM() *memIAM {
	return &memIAM{roles: map[string]map[string]string{}, profiles: map[string]map[string]string{},
		inProf: map[string][]string{}, inline: map[string]string{}}
}

func noEntity() error { return &iamtypes.NoSuchEntityException{Message: aws.String("not found")} }

func merge(dst map[string]string, ts []iamtypes.Tag) {
	for k, v := range iamtags.ToMap(ts) {
		dst[k] = v
	}
}

func (m *memIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	tg, ok := m.roles[aws.ToString(in.RoleName)]
	if !ok {
		return nil, noEntity()
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName, Arn: aws.String("arn:aws:iam::123456789012:role/" + aws.ToString(in.RoleName)),
		Tags: iamtags.FromMap(tg)}}, nil
}

func (m *memIAM) CreateRole(_ context.Context, in *iam.CreateRoleInput, _ ...func(*iam.Options)) (*iam.CreateRoleOutput, error) {
	tg := map[string]string{}
	merge(tg, in.Tags)
	m.roles[aws.ToString(in.RoleName)] = tg
	return &iam.CreateRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName}}, nil
}

func (m *memIAM) TagRole(_ context.Context, in *iam.TagRoleInput, _ ...func(*iam.Options)) (*iam.TagRoleOutput, error) {
	tg, ok := m.roles[aws.ToString(in.RoleName)]
	if !ok {
		return nil, noEntity()
	}
	merge(tg, in.Tags)
	return &iam.TagRoleOutput{}, nil
}

func (m *memIAM) AttachRolePolicy(context.Context, *iam.AttachRolePolicyInput, ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error) {
	return &iam.AttachRolePolicyOutput{}, nil
}

func (m *memIAM) ListAttachedRolePolicies(context.Context, *iam.ListAttachedRolePoliciesInput, ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{}, nil
}

func (m *memIAM) PutRolePolicy(_ context.Context, in *iam.PutRolePolicyInput, _ ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error) {
	m.inline[aws.ToString(in.RoleName)+"/"+aws.ToString(in.PolicyName)] = aws.ToString(in.PolicyDocument)
	return &iam.PutRolePolicyOutput{}, nil
}

func (m *memIAM) GetRolePolicy(_ context.Context, in *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	doc, ok := m.inline[aws.ToString(in.RoleName)+"/"+aws.ToString(in.PolicyName)]
	if !ok {
		return nil, noEntity()
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(doc)}, nil
}

func (m *memIAM) GetInstanceProfile(_ context.Context, in *iam.GetInstanceProfileInput, _ ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error) {
	name := aws.ToString(in.InstanceProfileName)
	if _, ok := m.profiles[name]; !ok {
		return nil, noEntity()
	}
	p := &iamtypes.InstanceProfile{InstanceProfileName: in.InstanceProfileName}
	for _, r := range m.inProf[name] {
		p.Roles = append(p.Roles, iamtypes.Role{RoleName: aws.String(r)})
	}
	return &iam.GetInstanceProfileOutput{InstanceProfile: p}, nil
}

func (m *memIAM) CreateInstanceProfile(_ context.Context, in *iam.CreateInstanceProfileInput, _ ...func(*iam.Options)) (*iam.CreateInstanceProfileOutput, error) {
	tg := map[string]string{}
	merge(tg, in.Tags)
	m.profiles[aws.ToString(in.InstanceProfileName)] = tg
	return &iam.CreateInstanceProfileOutput{InstanceProfile: &iamtypes.InstanceProfile{InstanceProfileName: in.InstanceProfileName}}, nil
}

func (m *memIAM) AddRoleToInstanceProfile(_ context.Context, in *iam.AddRoleToInstanceProfileInput, _ ...func(*iam.Options)) (*iam.AddRoleToInstanceProfileOutput, error) {
	n := aws.ToString(in.InstanceProfileName)
	m.inProf[n] = append(m.inProf[n], aws.ToString(in.RoleName))
	return &iam.AddRoleToInstanceProfileOutput{}, nil
}

func (m *memIAM) ListInstanceProfileTags(_ context.Context, in *iam.ListInstanceProfileTagsInput, _ ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error) {
	tg, ok := m.profiles[aws.ToString(in.InstanceProfileName)]
	if !ok {
		return nil, noEntity()
	}
	return &iam.ListInstanceProfileTagsOutput{Tags: iamtags.FromMap(tg)}, nil
}

func (m *memIAM) TagInstanceProfile(_ context.Context, in *iam.TagInstanceProfileInput, _ ...func(*iam.Options)) (*iam.TagInstanceProfileOutput, error) {
	tg, ok := m.profiles[aws.ToString(in.InstanceProfileName)]
	if !ok {
		return nil, noEntity()
	}
	merge(tg, in.Tags)
	return &iam.TagInstanceProfileOutput{}, nil
}

func (m *memIAM) GetPolicy(_ context.Context, in *iam.GetPolicyInput, _ ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	if m.policy == nil {
		return nil, noEntity()
	}
	return &iam.GetPolicyOutput{Policy: &iamtypes.Policy{Arn: in.PolicyArn, DefaultVersionId: aws.String("v1"), Tags: iamtags.FromMap(m.policy)}}, nil
}

func (m *memIAM) GetPolicyVersion(context.Context, *iam.GetPolicyVersionInput, ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	return &iam.GetPolicyVersionOutput{PolicyVersion: &iamtypes.PolicyVersion{VersionId: aws.String("v1"), IsDefaultVersion: true,
		Document: aws.String(m.policyV)}}, nil
}

func (m *memIAM) CreatePolicy(_ context.Context, in *iam.CreatePolicyInput, _ ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	m.policy, m.policyV = map[string]string{}, aws.ToString(in.PolicyDocument)
	merge(m.policy, in.Tags)
	return &iam.CreatePolicyOutput{Policy: &iamtypes.Policy{Arn: aws.String(cliiam.PolicyARN("123456789012"))}}, nil
}

func (m *memIAM) TagPolicy(_ context.Context, in *iam.TagPolicyInput, _ ...func(*iam.Options)) (*iam.TagPolicyOutput, error) {
	merge(m.policy, in.Tags)
	return &iam.TagPolicyOutput{}, nil
}

func (m *memIAM) CreatePolicyVersion(_ context.Context, in *iam.CreatePolicyVersionInput, _ ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	m.policyV = aws.ToString(in.PolicyDocument)
	return &iam.CreatePolicyVersionOutput{}, nil
}

func (m *memIAM) ListPolicyVersions(context.Context, *iam.ListPolicyVersionsInput, ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	return &iam.ListPolicyVersionsOutput{Versions: []iamtypes.PolicyVersion{{VersionId: aws.String("v1"), IsDefaultVersion: true}}}, nil
}

func (m *memIAM) DeletePolicyVersion(context.Context, *iam.DeletePolicyVersionInput, ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	return &iam.DeletePolicyVersionOutput{}, nil
}

// GetCallerIdentity answers spawn's account lookup (STS is built from the
// same config).
func (m *memIAM) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Account: aws.String("123456789012"), UserId: aws.String("AIDAADMIN"),
		Arn: aws.String("arn:aws:iam::123456789012:user/admin")}, nil
}

// taggedIAMConfig is an aws.Config whose IAM clients (setup's own and the one
// spawn builds from it) are answered by m, with the tagtest detector on.
func taggedIAMConfig(t *testing.T, m *memIAM) aws.Config {
	t.Helper()
	cfg := aws.Config{Region: "us-west-2"}
	tagtest.New(t, tags.ToolLagotto).Install(&cfg)
	cfg.APIOptions = append(cfg.APIOptions, testutil.AnswerFrom(m))
	return cfg
}

func withSetupStamp(t *testing.T, stamp managedtags.Stamp) {
	old := setupStamp
	setupStamp = stamp
	t.Cleanup(func() { setupStamp = old })
}

// TestSetup_TagsEverythingItCreates (#324): setup's IAM create paths, the two
// poller roles, lagotto's instance role and profile (through spawn) and the
// lagotto-cli-user policy, each carry spore-host:managed-by=lagotto at create.
func TestSetup_TagsEverythingItCreates(t *testing.T) {
	withSetupStamp(t, managedtags.Stamp{Owner: "AIDAADMIN"})
	m := newMemIAM()
	cfg := taggedIAMConfig(t, m)
	var out bytes.Buffer
	if err := applySetupRoles(context.Background(), iam.NewFromConfig(cfg), spawnProfileSetup{cfg: cfg}, "us-west-2", "123456789012", &out); err != nil {
		t.Fatalf("applySetupRoles: %v\n%s", err, out.String())
	}
	if err := applyCLIUserPolicy(context.Background(), iam.NewFromConfig(cfg), "123456789012", &out); err != nil {
		t.Fatalf("applyCLIUserPolicy: %v", err)
	}

	for name, tg := range map[string]map[string]string{
		"role " + runtimeiam.RoleName:                     m.roles[runtimeiam.RoleName],
		"role " + runtimeiam.SchedulerInvokeRoleName:      m.roles[runtimeiam.SchedulerInvokeRoleName],
		"role " + runtimeiam.InstanceRoleName:             m.roles[runtimeiam.InstanceRoleName],
		"instance profile " + runtimeiam.InstanceRoleName: m.profiles[runtimeiam.InstanceRoleName],
		"policy " + cliiam.PolicyName:                     m.policy,
	} {
		if !tags.IsManagedBy(tg, tags.ToolLagotto) || !tags.IsService(tg) || tg[tags.Scope] != managedtags.ScopeGlobal || tg[tags.Owner] != "AIDAADMIN" {
			t.Errorf("%s tags = %v, want the spore.host service tags, scope global, owner AIDAADMIN", name, tg)
		}
	}
	// The legacy markers the deleters are gated on are still there.
	if m.profiles[runtimeiam.InstanceRoleName][runtimeiam.ManagedProfileTagKey] != runtimeiam.ManagedProfileTagValue ||
		m.policy[cliiam.ManagedTagKey] != cliiam.ManagedTagValue {
		t.Errorf("legacy lagotto:managed tags lost: profile %v, policy %v", m.profiles[runtimeiam.InstanceRoleName], m.policy)
	}
}

// TestSetup_BackfillsTagsOntoWhatAnOlderLagottoCreated: roles, profile and
// policy that exist without the spore.host tags gain them; an
// administrator's authorized role is never tagged, and its profile only when
// setup created it; a re-run keeps the first spore-host:created.
func TestSetup_BackfillsTagsOntoWhatAnOlderLagottoCreated(t *testing.T) {
	withSetupStamp(t, managedtags.Stamp{Owner: "AIDAADMIN", Now: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }})
	m := newMemIAM()
	m.roles[runtimeiam.RoleName] = map[string]string{"Application": "lagotto"}
	m.roles[runtimeiam.SchedulerInvokeRoleName] = map[string]string{}
	m.roles[runtimeiam.InstanceRoleName] = map[string]string{"spawn:managed": "true"}
	m.profiles[runtimeiam.InstanceRoleName] = map[string]string{runtimeiam.ManagedProfileTagKey: runtimeiam.ManagedProfileTagValue}
	m.inProf[runtimeiam.InstanceRoleName] = []string{runtimeiam.InstanceRoleName}
	m.roles["admins-role"] = map[string]string{"owner": "admin"}
	m.profiles["admins-role"] = map[string]string{} // not created by setup
	m.inProf["admins-role"] = []string{"admins-role"}
	m.policy = map[string]string{cliiam.ManagedTagKey: cliiam.ManagedTagValue}

	cfg := taggedIAMConfig(t, m)
	ctx := context.Background()
	for _, name := range []string{runtimeiam.InstanceRoleName, "admins-role"} {
		if err := (spawnProfileSetup{cfg: cfg}).EnsureInstanceProfile(ctx, name); err != nil {
			t.Fatalf("EnsureInstanceProfile(%s): %v", name, err)
		}
	}
	if err := runtimeiam.EnsureRoles(ctx, iam.NewFromConfig(cfg), "us-west-2", "123456789012", setupStamp); err != nil {
		t.Fatal(err)
	}
	if _, err := cliiam.EnsurePolicy(ctx, iam.NewFromConfig(cfg), "123456789012", m.policyV, setupStamp); err != nil {
		t.Fatal(err)
	}

	for name, tg := range map[string]map[string]string{
		"role " + runtimeiam.RoleName:                     m.roles[runtimeiam.RoleName],
		"role " + runtimeiam.SchedulerInvokeRoleName:      m.roles[runtimeiam.SchedulerInvokeRoleName],
		"role " + runtimeiam.InstanceRoleName:             m.roles[runtimeiam.InstanceRoleName],
		"instance profile " + runtimeiam.InstanceRoleName: m.profiles[runtimeiam.InstanceRoleName],
		"policy": m.policy,
	} {
		if !tags.IsManagedBy(tg, tags.ToolLagotto) || tg[tags.Created] != "2026-01-01T00:00:00Z" {
			t.Errorf("%s not back-filled: %v", name, tg)
		}
	}
	if tags.IsManagedBy(m.roles["admins-role"], tags.ToolLagotto) || tags.IsManagedBy(m.profiles["admins-role"], tags.ToolLagotto) {
		t.Errorf("an administrator's role or profile was claimed: role %v, profile %v", m.roles["admins-role"], m.profiles["admins-role"])
	}

	// A later run by someone else keeps the first created and owner.
	withSetupStamp(t, managedtags.Stamp{Owner: "AIDAOTHER"})
	if err := runtimeiam.EnsureRoles(ctx, iam.NewFromConfig(cfg), "us-west-2", "123456789012", setupStamp); err != nil {
		t.Fatal(err)
	}
	if tg := m.roles[runtimeiam.RoleName]; tg[tags.Created] != "2026-01-01T00:00:00Z" || tg[tags.Owner] != "AIDAADMIN" {
		t.Errorf("re-run rewrote created/owner: %v", tg)
	}
}

// scheduleMoveHold keeps the poller schedule where it is while a legacy stack
// owns it or the IAM policies predate lagotto's schedule group (#324).
func TestScheduleMoveHold(t *testing.T) {
	ctx := context.Background()
	acct := "123456789012"
	newPolicy, err := runtimeiam.PolicyDocument(acct, nil)
	if err != nil {
		t.Fatal(err)
	}
	cliDoc, err := cliiam.PolicyDocument(acct, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldPolicy := strings.ReplaceAll(newPolicy, "schedule/lagotto/", "schedule/default/")
	oldCLI := strings.ReplaceAll(cliDoc, "schedule/lagotto/", "schedule/default/")

	world := func(runtimeDoc, cli string) *memIAM {
		m := newMemIAM()
		m.roles[runtimeiam.RoleName] = map[string]string{}
		if runtimeDoc != "" {
			m.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName] = runtimeDoc
		}
		if cli != "" {
			m.policy, m.policyV = map[string]string{cliiam.ManagedTagKey: cliiam.ManagedTagValue}, cli
		}
		return m
	}
	for _, tc := range []struct {
		name   string
		stack  legacyStack
		iam    *memIAM
		expect string // substring of the hold; "" = no hold
	}{
		{"both policies current", legacyStack{}, world(newPolicy, cliDoc), ""},
		{"nothing written yet", legacyStack{}, world("", ""), ""},
		{"legacy stack", legacyStack{exists: true}, world(newPolicy, cliDoc), "legacy CloudFormation stack lagotto owns it"},
		{"stack check fails", legacyStack{err: errors.New("AccessDenied")}, world(newPolicy, cliDoc), "could not check"},
		{"old runtime policy", legacyStack{}, world(oldPolicy, cliDoc), "runtime IAM policy does not grant"},
		{"old CLI-user policy", legacyStack{}, world(newPolicy, oldCLI), cliiam.PolicyName + " IAM policy does not grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := scheduleMoveHold(ctx, tc.stack, iam.NewFromConfig(aws.Config{Region: "us-west-2",
				APIOptions: []func(*middleware.Stack) error{testutil.AnswerFrom(tc.iam)}}), acct, "lagotto")
			if (tc.expect == "") != (got == "") || !strings.Contains(got, tc.expect) {
				t.Errorf("hold = %q, want %q", got, tc.expect)
			}
		})
	}
}

type legacyStack struct {
	exists bool
	err    error
}

func (l legacyStack) LegacyStackState(context.Context, string) (bool, string, error) {
	return l.exists, "", l.err
}

// A policy that cannot be read holds the move: it cannot be told whether it
// grants the group.
func TestScheduleMoveHold_ReadErrorsHold(t *testing.T) {
	ctx := context.Background()
	for op, want := range map[string]string{
		"GetRolePolicy": "could not read the poller's runtime policy",
		"GetPolicy":     "could not read the " + cliiam.PolicyName + " policy",
	} {
		m := newMemIAM()
		m.roles[runtimeiam.RoleName] = map[string]string{}
		doc, _ := runtimeiam.PolicyDocument("123456789012", nil)
		m.inline[runtimeiam.RoleName+"/"+runtimeiam.PolicyName] = doc
		f := failingIAM{memIAM: m, op: op}
		got := scheduleMoveHold(ctx, legacyStack{}, iam.NewFromConfig(aws.Config{Region: "us-west-2",
			APIOptions: []func(*middleware.Stack) error{testutil.AnswerFrom(f)}}), "123456789012", "")
		if !strings.Contains(got, want) {
			t.Errorf("%s failing: hold = %q, want %q", op, got, want)
		}
	}
	if grantsScheduleGroup("not json") {
		t.Error("an unparsable policy grants the group")
	}
}

// failingIAM is memIAM with one operation failing AccessDenied.
type failingIAM struct {
	*memIAM
	op string
}

var errAccessDenied = errors.New("AccessDenied")

func (f failingIAM) GetRolePolicy(ctx context.Context, in *iam.GetRolePolicyInput, o ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	if f.op == "GetRolePolicy" {
		return nil, errAccessDenied
	}
	return f.memIAM.GetRolePolicy(ctx, in, o...)
}

func (f failingIAM) GetPolicy(ctx context.Context, in *iam.GetPolicyInput, o ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	if f.op == "GetPolicy" {
		return nil, errAccessDenied
	}
	return f.memIAM.GetPolicy(ctx, in, o...)
}
