package runtimeiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/libs/tags"
)

// fakeProfiles adds instance profiles to fakeRoleDeleter.
type fakeProfiles struct {
	*fakeRoleDeleter
	profiles map[string][]string // profile -> roles
	tags     map[string]map[string]string
	errOn    string
}

func (f *fakeProfiles) GetInstanceProfile(_ context.Context, in *iam.GetInstanceProfileInput, _ ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error) {
	if f.errOn == "GetInstanceProfile" {
		return nil, errors.New("AccessDenied")
	}
	roles, ok := f.profiles[aws.ToString(in.InstanceProfileName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	ip := &iamtypes.InstanceProfile{InstanceProfileName: in.InstanceProfileName}
	for _, r := range roles {
		ip.Roles = append(ip.Roles, iamtypes.Role{RoleName: aws.String(r)})
	}
	return &iam.GetInstanceProfileOutput{InstanceProfile: ip}, nil
}

func (f *fakeProfiles) ListInstanceProfileTags(_ context.Context, in *iam.ListInstanceProfileTagsInput, _ ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error) {
	switch f.errOn {
	case "ListInstanceProfileTags":
		return nil, errors.New("AccessDenied")
	case "ListNoSuchEntity":
		return nil, &iamtypes.NoSuchEntityException{}
	}
	var out []iamtypes.Tag
	for k, v := range f.tags[aws.ToString(in.InstanceProfileName)] {
		out = append(out, iamtypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return &iam.ListInstanceProfileTagsOutput{Tags: out}, nil
}

func (f *fakeProfiles) RemoveRoleFromInstanceProfile(_ context.Context, in *iam.RemoveRoleFromInstanceProfileInput, _ ...func(*iam.Options)) (*iam.RemoveRoleFromInstanceProfileOutput, error) {
	if f.errOn == "RemoveRoleFromInstanceProfile" {
		return nil, errors.New("AccessDenied")
	}
	f.order = append(f.order, "remove "+aws.ToString(in.RoleName)+" from "+aws.ToString(in.InstanceProfileName))
	f.profiles[aws.ToString(in.InstanceProfileName)] = nil
	return &iam.RemoveRoleFromInstanceProfileOutput{}, nil
}

func (f *fakeProfiles) DeleteInstanceProfile(_ context.Context, in *iam.DeleteInstanceProfileInput, _ ...func(*iam.Options)) (*iam.DeleteInstanceProfileOutput, error) {
	if f.errOn == "DeleteInstanceProfile" {
		return nil, errors.New("AccessDenied")
	}
	f.order = append(f.order, "delete-profile "+aws.ToString(in.InstanceProfileName))
	delete(f.profiles, aws.ToString(in.InstanceProfileName))
	return &iam.DeleteInstanceProfileOutput{}, nil
}

// with returns a copy of m with the key/value pairs kv added.
func with(m map[string]string, kv ...string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

func newFakeProfiles() *fakeProfiles {
	return &fakeProfiles{
		fakeRoleDeleter: &fakeRoleDeleter{
			inline:   map[string][]string{InstanceRoleName: {"spored-baseline-policy"}},
			attached: map[string][]string{InstanceRoleName: {"arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"}},
			exists:   map[string]bool{InstanceRoleName: true, "gpu-role": true},
		},
		profiles: map[string][]string{InstanceRoleName: {InstanceRoleName}, "gpu-role": {"gpu-role"}, "theirs": {"theirs"}},
		tags: map[string]map[string]string{
			InstanceRoleName: ManagedProfileTags(),
			"gpu-role":       ManagedProfileTags(),
			"theirs":         {"spawn:managed": "true"},
		},
	}
}

func TestDeleteInstanceRole(t *testing.T) {
	f := newFakeProfiles()
	gone, err := DeleteInstanceRole(context.Background(), f)
	if err != nil || !gone {
		t.Fatalf("DeleteInstanceRole = %v, %v", gone, err)
	}
	got := strings.Join(f.order, "; ")
	for _, want := range []string{"remove " + InstanceRoleName + " from " + InstanceRoleName, "delete-profile " + InstanceRoleName,
		"delete-policy " + InstanceRoleName + "/spored-baseline-policy"} {
		if !strings.Contains(got, want) {
			t.Errorf("order %q lacks %q", got, want)
		}
	}
	// Idempotent: everything already gone.
	f2 := newFakeProfiles()
	delete(f2.profiles, InstanceRoleName)
	f2.exists = map[string]bool{}
	if gone, err := DeleteInstanceRole(context.Background(), f2); err != nil || gone {
		t.Errorf("second run = %v, %v", gone, err)
	}
}

func TestDeleteManagedInstanceProfile(t *testing.T) {
	ctx := context.Background()
	f := newFakeProfiles()
	if gone, err := DeleteManagedInstanceProfile(ctx, f, "gpu-role"); err != nil || !gone {
		t.Errorf("tagged profile: %v, %v", gone, err)
	}
	if !f.exists["gpu-role"] {
		t.Error("the administrator's role was deleted")
	}
	if gone, err := DeleteManagedInstanceProfile(ctx, f, "theirs"); err != nil || gone {
		t.Errorf("untagged profile deleted: %v, %v", gone, err)
	}
	if gone, err := DeleteManagedInstanceProfile(ctx, f, "absent"); err != nil || gone {
		t.Errorf("absent profile: %v, %v", gone, err)
	}
	// lagotto#341: lagotto's marker and another tool's ownership tags on one
	// profile. Refused, named, and nothing is removed from it.
	for name, tg := range map[string]map[string]string{
		"aws-realign-gpu-instance": with(ManagedProfileTags(), "spawn:managed", "true", "spawn:created-by", "spawn", tags.ManagedBy, "lagotto"),
		"created-by-spawn":         with(ManagedProfileTags(), "spawn:created-by", "spawn"),
		"calques":                  with(ManagedProfileTags(), tags.ManagedBy, "calque"),
	} {
		f.profiles[name] = []string{name}
		f.tags[name] = tg
		f.order = nil
		gone, err := DeleteManagedInstanceProfile(ctx, f, name)
		var ce *ClaimedError
		if gone || !errors.Is(err, ErrClaimedByAnotherTool) || !errors.As(err, &ce) || ce.Name != name ||
			!strings.Contains(err.Error(), "left in place") {
			t.Errorf("%s: gone %v err %v", name, gone, err)
		}
		if len(f.order) != 0 || f.profiles[name] == nil {
			t.Errorf("%s: changed %v", name, f.order)
		}
		if gone, err := DeleteInstanceProfile(ctx, f, name); gone || !errors.Is(err, ErrClaimedByAnotherTool) {
			t.Errorf("per-profile deleter, %s: %v %v", name, gone, err)
		}
	}
	// A profile whose tags vanish between the reads is absent, not an error.
	g := newFakeProfiles()
	g.errOn = "ListNoSuchEntity"
	if gone, err := DeleteManagedInstanceProfile(ctx, g, "gpu-role"); gone || err != nil {
		t.Errorf("tags gone mid-delete: %v %v", gone, err)
	}
	for _, op := range []string{"GetInstanceProfile", "ListInstanceProfileTags", "RemoveRoleFromInstanceProfile", "DeleteInstanceProfile"} {
		f := newFakeProfiles()
		f.errOn = op
		if _, err := DeleteManagedInstanceProfile(ctx, f, "gpu-role"); err == nil {
			t.Errorf("%s failure was not reported", op)
		}
	}
}

// TestDeleteRoleOnlyLagottos: the per-role deleter of `teardown --plan`
// (#325) refuses any role that is not lagotto's, before any IAM call.
func TestDeleteRoleOnlyLagottos(t *testing.T) {
	if _, err := DeleteRole(context.Background(), nil, "team-gpu"); err == nil {
		t.Error("DeleteRole accepted an administrator's role")
	}
}

// TestDeleteInstanceProfilePerProfile: lagotto's own profile is deleted
// without its tag being consulted; any other only when setup created it.
func TestDeleteInstanceProfilePerProfile(t *testing.T) {
	ctx := context.Background()
	f := newFakeProfiles()
	f.tags[InstanceRoleName] = nil // an untagged profile of lagotto's own name
	if gone, err := DeleteInstanceProfile(ctx, f, InstanceRoleName); !gone || err != nil {
		t.Errorf("lagotto's profile: %v, %v", gone, err)
	}
	if gone, err := DeleteInstanceProfile(ctx, f, "theirs"); gone || err != nil || f.profiles["theirs"] == nil {
		t.Errorf("someone else's profile: %v, %v (profiles %v)", gone, err, f.profiles)
	}
	if gone, err := DeleteInstanceProfile(ctx, f, "gpu-role"); !gone || err != nil {
		t.Errorf("setup's profile for an authorized role: %v, %v", gone, err)
	}
	if gone, err := DeleteRole(ctx, f, InstanceRoleName); !gone || err != nil {
		t.Errorf("DeleteRole(%s) = %v, %v", InstanceRoleName, gone, err)
	}
}
