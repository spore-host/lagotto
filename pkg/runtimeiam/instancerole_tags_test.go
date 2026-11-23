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
	marker := func(extra ...string) map[string]string {
		m := map[string]string{ManagedProfileTagKey: ManagedProfileTagValue}
		for i := 0; i+1 < len(extra); i += 2 {
			m[extra[i]] = extra[i+1]
		}
		return m
	}
	spawnTags := []string{"spawn:managed", "true", "spawn:created-by", "spawn", "spawn:created", "2026-11-13T18:59:19Z"}
	f := &tagIAM{
		// lagotto created its own role and profile through spawn's library,
		// which stamps spawn:* on them: still lagotto's.
		roles: map[string]map[string]string{InstanceRoleName: marker(spawnTags...), "admins": {}},
		profiles: map[string]map[string]string{
			InstanceRoleName: marker(spawnTags...),
			"admins":         {},       // not setup's
			"authorized":     marker(), // setup's
			// lagotto#341: a profile that carries spawn's ownership tags is
			// not lagotto's to claim, marker or not.
			"aws-realign-gpu-instance": marker(spawnTags...),
			"spawn-only":               {"spawn:managed": "true", "spawn:created-by": "spawn"},
			// Another spore.host tool's.
			"calques": marker(tags.ManagedBy, "calque"),
		},
	}
	ctx := context.Background()
	names := []string{InstanceRoleName, "admins", "authorized", "aws-realign-gpu-instance", "spawn-only", "calques"}
	before := map[string]int{}
	for _, n := range names {
		before[n] = len(f.profiles[n])
	}
	for _, name := range names {
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
	if len(f.roles["admins"]) != 0 {
		t.Errorf("an administrator's role was tagged: %v", f.roles["admins"])
	}
	for _, n := range []string{"admins", "aws-realign-gpu-instance", "spawn-only", "calques"} {
		if len(f.profiles[n]) != before[n] {
			t.Errorf("profile %s, not lagotto's alone, was tagged: %v", n, f.profiles[n])
		}
	}
	if f.profiles["spawn-only"][ManagedProfileTagKey] != "" || f.profiles["admins"][ManagedProfileTagKey] != "" {
		t.Error("the setup-created marker was written onto a found profile")
	}

	// Steady state writes nothing.
	f.roleTags, f.profileTags = 0, 0
	if err := EnsureInstanceRoleTags(ctx, f, InstanceRoleName, stamp); err != nil || f.roleTags+f.profileTags != 0 {
		t.Errorf("re-run: err %v, %d role and %d profile tag calls", err, f.roleTags, f.profileTags)
	}

	// Never the marker onto a found own role or profile lacking it.
	g := &tagIAM{roles: map[string]map[string]string{InstanceRoleName: {}}, profiles: map[string]map[string]string{InstanceRoleName: {}}}
	if err := EnsureInstanceRoleTags(ctx, g, InstanceRoleName, stamp); err != nil || g.roleTags+g.profileTags != 0 {
		t.Errorf("a found unmarked %s was tagged: err %v, %v %v", InstanceRoleName, err, g.roles, g.profiles)
	}
	// Nor a different spore-host:managed-by on lagotto's own name.
	g = &tagIAM{roles: map[string]map[string]string{InstanceRoleName: marker(tags.ManagedBy, "calque")},
		profiles: map[string]map[string]string{InstanceRoleName: marker(tags.ManagedBy, "calque")}}
	if err := EnsureInstanceRoleTags(ctx, g, InstanceRoleName, stamp); err != nil || g.roleTags+g.profileTags != 0 {
		t.Errorf("another tool's %s was tagged: err %v", InstanceRoleName, err)
	}

	f.err = errors.New("AccessDenied")
	if err := EnsureInstanceRoleTags(ctx, f, InstanceRoleName, stamp); err == nil {
		t.Error("a GetRole failure was swallowed")
	}
	if err := EnsureInstanceRoleTags(ctx, f, "authorized", stamp); err == nil {
		t.Error("a ListInstanceProfileTags failure was swallowed")
	}
}

// createIAM answers CreateInstanceProfile as IAM does.
type createIAM struct {
	profiles map[string]map[string]string
	err      error
}

func (f *createIAM) CreateInstanceProfile(_ context.Context, in *iam.CreateInstanceProfileInput, _ ...func(*iam.Options)) (*iam.CreateInstanceProfileOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	n := aws.ToString(in.InstanceProfileName)
	if _, ok := f.profiles[n]; ok {
		return nil, &iamtypes.EntityAlreadyExistsException{Message: aws.String("exists")}
	}
	f.profiles[n] = iamtags.ToMap(in.Tags)
	return &iam.CreateInstanceProfileOutput{}, nil
}

// TestCreateInstanceProfile: the marker is written only by lagotto's own
// successful create, in the create request; a found profile is untouched
// (lagotto#341).
func TestCreateInstanceProfile(t *testing.T) {
	ctx := context.Background()
	f := &createIAM{profiles: map[string]map[string]string{"spawns": {"spawn:managed": "true"}}}
	created, err := CreateInstanceProfile(ctx, f, "team-gpu", managedtags.Stamp{Owner: "AIDAADMIN"})
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	if m := f.profiles["team-gpu"]; m[ManagedProfileTagKey] != ManagedProfileTagValue || !tags.IsManagedBy(m, tags.ToolLagotto) ||
		m[tags.Owner] != "AIDAADMIN" || m["spawn:managed"] != "" {
		t.Errorf("created profile tags = %v", m)
	}
	created, err = CreateInstanceProfile(ctx, f, "spawns")
	if err != nil || created {
		t.Errorf("found profile: created %v err %v", created, err)
	}
	if m := f.profiles["spawns"]; len(m) != 1 {
		t.Errorf("found profile was tagged: %v", m)
	}
	f.err = errors.New("AccessDenied")
	if _, err := CreateInstanceProfile(ctx, f, "x"); err == nil || !strings.Contains(err.Error(), "create instance profile x") {
		t.Errorf("err = %v", err)
	}
}

func TestInstanceProfileTagsKeepTheManagedMarker(t *testing.T) {
	m := InstanceProfileTags()
	if m[ManagedProfileTagKey] != ManagedProfileTagValue || (m[tags.Role] != "" || m[tags.Component] == "") || m[tags.Component] != managedtags.ComponentInstanceRole {
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
