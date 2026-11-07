package cliiam

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/smithy-go"
)

const testAccount = "123456789012"

// fakeIAM is an in-memory managed policy plus a scripted simulator. It records
// every mutating call in order, so tests can assert detach-before-delete.
type fakeIAM struct {
	exists   bool
	tags     []iamtypes.Tag
	versions []iamtypes.PolicyVersion // Document set on each
	nextVer  int

	users, groups, roles []string

	calls []string

	// errs makes the named operation fail.
	errs map[string]error

	// simulate decides one action on one resource ("" for "*"); nil allows all.
	simulate func(principal, action, resource string, ctx []iamtypes.ContextEntry) bool
	// simErr, when set, fails SimulatePrincipalPolicy for this principal.
	simErr map[string]error
	// roleARN is what GetRole returns.
	roleARN string
	// simulated records the principals simulated.
	simulated []string
}

func managedTags() []iamtypes.Tag {
	return []iamtypes.Tag{{Key: aws.String(ManagedTagKey), Value: aws.String(ManagedTagValue)}}
}

// withPolicy returns a fake whose policy exists with n versions, the last the
// default, created a day apart.
func withPolicy(n int, defaultDoc string) *fakeIAM {
	f := &fakeIAM{exists: true, tags: managedTags()}
	for i := 1; i <= n; i++ {
		doc := `{"Version":"2012-10-17","Statement":[]}`
		if i == n {
			doc = defaultDoc
		}
		f.versions = append(f.versions, iamtypes.PolicyVersion{
			VersionId: aws.String(fmt.Sprintf("v%d", i)), IsDefaultVersion: i == n,
			CreateDate: aws.Time(time.Date(2026, 1, i, 0, 0, 0, 0, time.UTC)),
			Document:   aws.String(url.QueryEscape(doc)),
		})
	}
	f.nextVer = n + 1
	return f
}

func (f *fakeIAM) err(op string) error {
	if e, ok := f.errs[op]; ok {
		return e
	}
	return nil
}

func noSuchEntity() error {
	return &iamtypes.NoSuchEntityException{Message: aws.String("no such entity")}
}

func (f *fakeIAM) defaultVersion() *iamtypes.PolicyVersion {
	for i := range f.versions {
		if f.versions[i].IsDefaultVersion {
			return &f.versions[i]
		}
	}
	return nil
}

func (f *fakeIAM) GetPolicy(_ context.Context, in *iam.GetPolicyInput, _ ...func(*iam.Options)) (*iam.GetPolicyOutput, error) {
	if err := f.err("GetPolicy"); err != nil {
		return nil, err
	}
	if !f.exists {
		return nil, noSuchEntity()
	}
	p := &iamtypes.Policy{Arn: in.PolicyArn, PolicyName: aws.String(PolicyName), Tags: f.tags}
	if d := f.defaultVersion(); d != nil {
		p.DefaultVersionId = d.VersionId
	}
	return &iam.GetPolicyOutput{Policy: p}, nil
}

func (f *fakeIAM) GetPolicyVersion(_ context.Context, in *iam.GetPolicyVersionInput, _ ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error) {
	if err := f.err("GetPolicyVersion"); err != nil {
		return nil, err
	}
	for i := range f.versions {
		if aws.ToString(f.versions[i].VersionId) == aws.ToString(in.VersionId) {
			v := f.versions[i]
			return &iam.GetPolicyVersionOutput{PolicyVersion: &v}, nil
		}
	}
	return nil, noSuchEntity()
}

func (f *fakeIAM) CreatePolicy(_ context.Context, in *iam.CreatePolicyInput, _ ...func(*iam.Options)) (*iam.CreatePolicyOutput, error) {
	f.calls = append(f.calls, "CreatePolicy")
	if err := f.err("CreatePolicy"); err != nil {
		return nil, err
	}
	f.exists, f.tags = true, in.Tags
	f.versions = []iamtypes.PolicyVersion{{VersionId: aws.String("v1"), IsDefaultVersion: true,
		CreateDate: aws.Time(time.Now()), Document: aws.String(url.QueryEscape(aws.ToString(in.PolicyDocument)))}}
	f.nextVer = 2
	return &iam.CreatePolicyOutput{}, nil
}

func (f *fakeIAM) CreatePolicyVersion(_ context.Context, in *iam.CreatePolicyVersionInput, _ ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error) {
	f.calls = append(f.calls, "CreatePolicyVersion")
	if err := f.err("CreatePolicyVersion"); err != nil {
		return nil, err
	}
	if len(f.versions) >= maxPolicyVersions {
		return nil, &iamtypes.LimitExceededException{Message: aws.String("version limit")}
	}
	if in.SetAsDefault {
		for i := range f.versions {
			f.versions[i].IsDefaultVersion = false
		}
	}
	f.versions = append(f.versions, iamtypes.PolicyVersion{VersionId: aws.String(fmt.Sprintf("v%d", f.nextVer)),
		IsDefaultVersion: in.SetAsDefault, CreateDate: aws.Time(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)),
		Document: aws.String(url.QueryEscape(aws.ToString(in.PolicyDocument)))})
	f.nextVer++
	return &iam.CreatePolicyVersionOutput{}, nil
}

func (f *fakeIAM) ListPolicyVersions(_ context.Context, _ *iam.ListPolicyVersionsInput, _ ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error) {
	if err := f.err("ListPolicyVersions"); err != nil {
		return nil, err
	}
	out := make([]iamtypes.PolicyVersion, len(f.versions))
	copy(out, f.versions)
	return &iam.ListPolicyVersionsOutput{Versions: out}, nil
}

func (f *fakeIAM) DeletePolicyVersion(_ context.Context, in *iam.DeletePolicyVersionInput, _ ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error) {
	id := aws.ToString(in.VersionId)
	f.calls = append(f.calls, "DeletePolicyVersion "+id)
	if err := f.err("DeletePolicyVersion"); err != nil {
		return nil, err
	}
	for i := range f.versions {
		if aws.ToString(f.versions[i].VersionId) == id {
			if f.versions[i].IsDefaultVersion {
				return nil, &iamtypes.DeleteConflictException{Message: aws.String("cannot delete the default version")}
			}
			f.versions = append(f.versions[:i], f.versions[i+1:]...)
			return &iam.DeletePolicyVersionOutput{}, nil
		}
	}
	return nil, noSuchEntity()
}

func (f *fakeIAM) ListEntitiesForPolicy(_ context.Context, _ *iam.ListEntitiesForPolicyInput, _ ...func(*iam.Options)) (*iam.ListEntitiesForPolicyOutput, error) {
	if err := f.err("ListEntitiesForPolicy"); err != nil {
		return nil, err
	}
	out := &iam.ListEntitiesForPolicyOutput{}
	for _, u := range f.users {
		out.PolicyUsers = append(out.PolicyUsers, iamtypes.PolicyUser{UserName: aws.String(u)})
	}
	for _, g := range f.groups {
		out.PolicyGroups = append(out.PolicyGroups, iamtypes.PolicyGroup{GroupName: aws.String(g)})
	}
	for _, r := range f.roles {
		out.PolicyRoles = append(out.PolicyRoles, iamtypes.PolicyRole{RoleName: aws.String(r)})
	}
	return out, nil
}

func remove(ss []string, s string) []string {
	var out []string
	for _, v := range ss {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}

func (f *fakeIAM) DetachUserPolicy(_ context.Context, in *iam.DetachUserPolicyInput, _ ...func(*iam.Options)) (*iam.DetachUserPolicyOutput, error) {
	f.calls = append(f.calls, "DetachUserPolicy "+aws.ToString(in.UserName))
	f.users = remove(f.users, aws.ToString(in.UserName))
	return &iam.DetachUserPolicyOutput{}, f.err("DetachUserPolicy")
}

func (f *fakeIAM) DetachGroupPolicy(_ context.Context, in *iam.DetachGroupPolicyInput, _ ...func(*iam.Options)) (*iam.DetachGroupPolicyOutput, error) {
	f.calls = append(f.calls, "DetachGroupPolicy "+aws.ToString(in.GroupName))
	f.groups = remove(f.groups, aws.ToString(in.GroupName))
	return &iam.DetachGroupPolicyOutput{}, f.err("DetachGroupPolicy")
}

func (f *fakeIAM) DetachRolePolicy(_ context.Context, in *iam.DetachRolePolicyInput, _ ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	f.calls = append(f.calls, "DetachRolePolicy "+aws.ToString(in.RoleName))
	f.roles = remove(f.roles, aws.ToString(in.RoleName))
	return &iam.DetachRolePolicyOutput{}, f.err("DetachRolePolicy")
}

func (f *fakeIAM) DeletePolicy(_ context.Context, _ *iam.DeletePolicyInput, _ ...func(*iam.Options)) (*iam.DeletePolicyOutput, error) {
	f.calls = append(f.calls, "DeletePolicy")
	if err := f.err("DeletePolicy"); err != nil {
		return nil, err
	}
	// IAM's own rules: no attachments, no non-default versions.
	if len(f.users)+len(f.groups)+len(f.roles) > 0 {
		return nil, &iamtypes.DeleteConflictException{Message: aws.String("policy is attached")}
	}
	if len(f.versions) > 1 {
		return nil, &iamtypes.DeleteConflictException{Message: aws.String("policy has non-default versions")}
	}
	f.exists, f.versions = false, nil
	return &iam.DeletePolicyOutput{}, nil
}

func (f *fakeIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	f.calls = append(f.calls, "GetRole "+aws.ToString(in.RoleName))
	if err := f.err("GetRole"); err != nil {
		return nil, err
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{Arn: aws.String(f.roleARN), RoleName: in.RoleName}}, nil
}

func (f *fakeIAM) SimulatePrincipalPolicy(_ context.Context, in *iam.SimulatePrincipalPolicyInput, _ ...func(*iam.Options)) (*iam.SimulatePrincipalPolicyOutput, error) {
	principal := aws.ToString(in.PolicySourceArn)
	f.simulated = append(f.simulated, principal)
	if err, ok := f.simErr[principal]; ok {
		return nil, err
	}
	out := &iam.SimulatePrincipalPolicyOutput{}
	resources := in.ResourceArns
	for _, a := range in.ActionNames {
		ev := iamtypes.EvaluationResult{EvalActionName: aws.String(a)}
		decide := func(r string) iamtypes.PolicyEvaluationDecisionType {
			if f.simulate == nil || f.simulate(principal, a, r, in.ContextEntries) {
				return iamtypes.PolicyEvaluationDecisionTypeAllowed
			}
			return iamtypes.PolicyEvaluationDecisionTypeImplicitDeny
		}
		if len(resources) == 0 {
			ev.EvalResourceName = aws.String("*")
			ev.EvalDecision = decide("")
		} else {
			for _, r := range resources {
				ev.ResourceSpecificResults = append(ev.ResourceSpecificResults, iamtypes.ResourceSpecificResult{
					EvalResourceName: aws.String(r), EvalResourceDecision: decide(r),
				})
			}
		}
		out.EvaluationResults = append(out.EvaluationResults, ev)
	}
	return out, nil
}

// apiErr is a smithy API error with a code, as the SDK returns.
func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: strings.ToLower(code)}
}
