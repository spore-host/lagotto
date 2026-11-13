package runtimeiam

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
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
	if f.errOn == "ListInstanceProfileTags" {
		return nil, errors.New("AccessDenied")
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
	for _, op := range []string{"GetInstanceProfile", "ListInstanceProfileTags", "RemoveRoleFromInstanceProfile", "DeleteInstanceProfile"} {
		f := newFakeProfiles()
		f.errOn = op
		if _, err := DeleteManagedInstanceProfile(ctx, f, "gpu-role"); err == nil {
			t.Errorf("%s failure was not reported", op)
		}
	}
}
