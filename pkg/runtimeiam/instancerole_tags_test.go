package runtimeiam

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/libs/tags"
	"github.com/spore-host/libs/tags/iamtags"

	"github.com/spore-host/lagotto/pkg/managedtags"
)

// tagIAM holds role and instance-profile tags for EnsureInstanceRoleTags.
type tagIAM struct {
	roles, profiles map[string]map[string]string
	err             error // fails every call
	roleTags        int
	profileTags     int
}

func (f *tagIAM) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &iam.GetRoleOutput{Role: &iamtypes.Role{RoleName: in.RoleName, Tags: iamtags.FromMap(f.roles[aws.ToString(in.RoleName)])}}, nil
}

func (f *tagIAM) TagRole(_ context.Context, in *iam.TagRoleInput, _ ...func(*iam.Options)) (*iam.TagRoleOutput, error) {
	f.roleTags++
	for k, v := range iamtags.ToMap(in.Tags) {
		f.roles[aws.ToString(in.RoleName)][k] = v
	}
	return &iam.TagRoleOutput{}, nil
}

func (f *tagIAM) ListInstanceProfileTags(_ context.Context, in *iam.ListInstanceProfileTagsInput, _ ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &iam.ListInstanceProfileTagsOutput{Tags: iamtags.FromMap(f.profiles[aws.ToString(in.InstanceProfileName)])}, nil
}

func (f *tagIAM) TagInstanceProfile(_ context.Context, in *iam.TagInstanceProfileInput, _ ...func(*iam.Options)) (*iam.TagInstanceProfileOutput, error) {
	f.profileTags++
	for k, v := range iamtags.ToMap(in.Tags) {
		f.profiles[aws.ToString(in.InstanceProfileName)][k] = v
	}
	return &iam.TagInstanceProfileOutput{}, nil
}

func TestEnsureInstanceRoleTags(t *testing.T) {
	stamp := managedtags.Stamp{Owner: "AIDAADMIN", Now: func() time.Time { return time.Unix(0, 0) }}
	f := &tagIAM{
		roles: map[string]map[string]string{InstanceRoleName: {"spawn:managed": "true"}, "admins": {}},
		profiles: map[string]map[string]string{
			InstanceRoleName: {ManagedProfileTagKey: ManagedProfileTagValue},
			"admins":         {},                                             // not setup's
			"authorized":     {ManagedProfileTagKey: ManagedProfileTagValue}, // setup's
		},
	}
	ctx := context.Background()
	for _, name := range []string{InstanceRoleName, "admins", "authorized"} {
		if err := EnsureInstanceRoleTags(ctx, f, name, stamp); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for what, m := range map[string]map[string]string{
		"own role": f.roles[InstanceRoleName], "own profile": f.profiles[InstanceRoleName], "setup's profile": f.profiles["authorized"],
	} {
		if !tags.IsManagedBy(m, tags.ToolLagotto) || m[tags.Scope] != managedtags.ScopeGlobal || m[tags.Owner] != "AIDAADMIN" {
			t.Errorf("%s tags = %v", what, m)
		}
	}
	if len(f.roles["admins"]) != 0 || len(f.profiles["admins"]) != 0 {
		t.Errorf("an administrator's role or profile was tagged: %v %v", f.roles["admins"], f.profiles["admins"])
	}

	// Steady state writes nothing.
	f.roleTags, f.profileTags = 0, 0
	if err := EnsureInstanceRoleTags(ctx, f, InstanceRoleName, stamp); err != nil || f.roleTags+f.profileTags != 0 {
		t.Errorf("re-run: err %v, %d role and %d profile tag calls", err, f.roleTags, f.profileTags)
	}

	f.err = errors.New("AccessDenied")
	if err := EnsureInstanceRoleTags(ctx, f, InstanceRoleName, stamp); err == nil {
		t.Error("a GetRole failure was swallowed")
	}
	if err := EnsureInstanceRoleTags(ctx, f, "authorized", stamp); err == nil {
		t.Error("a ListInstanceProfileTags failure was swallowed")
	}
}

func TestInstanceProfileTagsKeepTheManagedMarker(t *testing.T) {
	m := InstanceProfileTags()
	if m[ManagedProfileTagKey] != ManagedProfileTagValue || !tags.IsService(m) || m[tags.Component] != managedtags.ComponentInstanceRole {
		t.Errorf("InstanceProfileTags = %v", m)
	}
}

// Existing poller roles without the spore.host tags gain them; created ones
// carry them at create.
func TestEnsureRoles_TagsRoles(t *testing.T) {
	f := &fakeIAM{existing: map[string]bool{RoleName: true}}
	if err := EnsureRoles(context.Background(), f, "us-east-1", "123456789012", managedtags.Stamp{Owner: "AIDAADMIN"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{RoleName, SchedulerInvokeRoleName} {
		if m := f.tags[name]; !tags.IsManagedBy(m, tags.ToolLagotto) || m[tags.Owner] != "AIDAADMIN" || m[tags.Scope] != managedtags.ScopeGlobal {
			t.Errorf("role %s tags = %v", name, m)
		}
	}
	if len(f.tagged) != 1 || f.tagged[0] != RoleName {
		t.Errorf("TagRole on %v, want only the pre-existing %s", f.tagged, RoleName)
	}
	f.tagged = nil
	if err := EnsureRoles(context.Background(), f, "us-east-1", "123456789012"); err != nil || len(f.tagged) != 0 {
		t.Errorf("re-run: err %v, TagRole on %v; want none", err, f.tagged)
	}
}

// A TagRole failure on an existing poller role is an error naming it.
func TestEnsureRoles_TagFailure(t *testing.T) {
	f := &tagFailIAM{fakeIAM: &fakeIAM{existing: map[string]bool{RoleName: true}}}
	if err := EnsureRoles(context.Background(), f, "us-east-1", "123456789012"); err == nil || !strings.Contains(err.Error(), "tag role "+RoleName) {
		t.Errorf("err = %v", err)
	}
}

type tagFailIAM struct{ *fakeIAM }

func (tagFailIAM) TagRole(context.Context, *iam.TagRoleInput, ...func(*iam.Options)) (*iam.TagRoleOutput, error) {
	return nil, errors.New("AccessDenied")
}
