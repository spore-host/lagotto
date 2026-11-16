package runtimeiam

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/libs/tags/iamtags"

	"github.com/spore-host/lagotto/pkg/managedtags"
)

// ManagedProfileTagKey / ManagedProfileTagValue mark an instance profile (and
// lagotto's own instance role) that `lagotto setup` created. The deleters
// below remove only what carries it: an administrator's own role, and a
// profile someone else created for it, are left alone.
const (
	ManagedProfileTagKey   = "lagotto:managed"
	ManagedProfileTagValue = "instance-profile"
)

// ManagedProfileTags are the tags `setup` asks spawn to put on what it creates
// for an instance role (spawn adds its own spawn:* tags).
func ManagedProfileTags() map[string]string {
	return map[string]string{ManagedProfileTagKey: ManagedProfileTagValue}
}

// InstanceProfileTags are [ManagedProfileTags] plus the spore.host service tags
// (#324, scope "global"): what `setup` puts on lagotto's instance role, its
// profile, and the profile it creates for an authorized role.
func InstanceProfileTags(stamp ...managedtags.Stamp) map[string]string {
	return managedtags.With(ManagedProfileTags(),
		managedtags.FirstStamp(stamp).Service(managedtags.ScopeGlobal, managedtags.ComponentInstanceRole))
}

// InstanceRoleTagAPI is the slice of IAM [EnsureInstanceRoleTags] needs.
// *iam.Client satisfies it.
type InstanceRoleTagAPI interface {
	GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	TagRole(ctx context.Context, in *iam.TagRoleInput, optFns ...func(*iam.Options)) (*iam.TagRoleOutput, error)
	ListInstanceProfileTags(ctx context.Context, in *iam.ListInstanceProfileTagsInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error)
	TagInstanceProfile(ctx context.Context, in *iam.TagInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.TagInstanceProfileOutput, error)
}

// EnsureInstanceRoleTags adds [InstanceProfileTags] to what `setup` made for
// the instance role name but an older lagotto left without them (#324): the
// tags each lacks are added, and a spore-host:created already there is kept.
// It runs after the role and profile were ensured, so a missing one is an
// error.
//
// Only what is lagotto's is tagged. For lagotto's own [InstanceRoleName] that is
// the role and its profile. For an administrator's authorized role it is the
// profile alone, and only when it carries [ManagedProfileTagKey] (setup
// created it); the role is the administrator's and is never tagged.
func EnsureInstanceRoleTags(ctx context.Context, client InstanceRoleTagAPI, name string, stamp ...managedtags.Stamp) error {
	desired := InstanceProfileTags(stamp...)
	if name == InstanceRoleName {
		out, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
		if err != nil {
			return fmt.Errorf("runtimeiam: read role %s: %w", name, err)
		}
		var have map[string]string
		if out.Role != nil {
			have = iamtags.ToMap(out.Role.Tags)
		}
		if add := managedtags.Converge(have, desired); len(add) > 0 {
			if _, err := client.TagRole(ctx, &iam.TagRoleInput{RoleName: aws.String(name), Tags: iamtags.FromMap(add)}); err != nil {
				return fmt.Errorf("runtimeiam: tag role %s: %w", name, err)
			}
		}
	}
	have, err := profileTags(ctx, client, name)
	if err != nil {
		return err
	}
	if name != InstanceRoleName && have[ManagedProfileTagKey] != ManagedProfileTagValue {
		return nil // not created by setup: not lagotto's to tag
	}
	if add := managedtags.Converge(have, desired); len(add) > 0 {
		if _, err := client.TagInstanceProfile(ctx, &iam.TagInstanceProfileInput{
			InstanceProfileName: aws.String(name), Tags: iamtags.FromMap(add),
		}); err != nil {
			return fmt.Errorf("runtimeiam: tag instance profile %s: %w", name, err)
		}
	}
	return nil
}

// profileTags reads every tag of the named instance profile.
func profileTags(ctx context.Context, client interface {
	ListInstanceProfileTags(ctx context.Context, in *iam.ListInstanceProfileTagsInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error)
}, name string) (map[string]string, error) {
	tags := map[string]string{}
	in := &iam.ListInstanceProfileTagsInput{InstanceProfileName: aws.String(name)}
	for {
		page, err := client.ListInstanceProfileTags(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("runtimeiam: read tags of instance profile %s: %w", name, err)
		}
		for k, v := range iamtags.ToMap(page.Tags) {
			tags[k] = v
		}
		if !page.IsTruncated || page.Marker == nil {
			return tags, nil
		}
		in.Marker = page.Marker
	}
}

// InstanceRoleDeleteAPI is the slice of IAM the instance-role deleters need.
// *iam.Client satisfies it.
type InstanceRoleDeleteAPI interface {
	RoleDeleteAPI
	GetInstanceProfile(ctx context.Context, in *iam.GetInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.GetInstanceProfileOutput, error)
	ListInstanceProfileTags(ctx context.Context, in *iam.ListInstanceProfileTagsInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error)
	RemoveRoleFromInstanceProfile(ctx context.Context, in *iam.RemoveRoleFromInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.RemoveRoleFromInstanceProfileOutput, error)
	DeleteInstanceProfile(ctx context.Context, in *iam.DeleteInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.DeleteInstanceProfileOutput, error)
}

// DeleteInstanceRole is the named deleter for lagotto's own instance role
// ([InstanceRoleName]) and its same-named instance profile, both created by
// `lagotto setup`. It empties the profile, deletes it, then deletes the role's
// inline and managed policies and the role. Absent pieces are skipped, so it
// is idempotent. deleted reports whether the role existed.
//
// Like [DeleteRoles] it must run only when no poller is left in any region:
// instances a poller launches use this profile.
func DeleteInstanceRole(ctx context.Context, client InstanceRoleDeleteAPI) (deleted bool, err error) {
	if _, err := deleteProfile(ctx, client, InstanceRoleName, false); err != nil {
		return false, err
	}
	return deleteRole(ctx, client, InstanceRoleName)
}

// DeleteManagedInstanceProfile deletes the instance profile named after an
// administrator's authorized role, but only when `setup` created it (it
// carries [ManagedProfileTagKey]). The role itself is the administrator's and
// is never touched. Called when `setup --revoke-instance-role` revokes the role,
// and by `teardown --iam-roles` for every role still authorized.
func DeleteManagedInstanceProfile(ctx context.Context, client InstanceRoleDeleteAPI, name string) (deleted bool, err error) {
	return deleteProfile(ctx, client, name, true)
}

// deleteProfile removes every role from the named instance profile and
// deletes it. With requireTag, a profile without lagotto's tag is left alone.
func deleteProfile(ctx context.Context, client InstanceRoleDeleteAPI, name string, requireTag bool) (bool, error) {
	out, err := client.GetInstanceProfile(ctx, &iam.GetInstanceProfileInput{InstanceProfileName: aws.String(name)})
	if err != nil {
		if isNoSuchEntity(err) {
			return false, nil
		}
		return false, fmt.Errorf("runtimeiam: read instance profile %s: %w", name, err)
	}
	if requireTag {
		managed, err := profileIsManaged(ctx, client, name)
		if err != nil {
			return false, err
		}
		if !managed {
			return false, nil
		}
	}
	if out.InstanceProfile != nil {
		for _, r := range out.InstanceProfile.Roles {
			if _, err := client.RemoveRoleFromInstanceProfile(ctx, &iam.RemoveRoleFromInstanceProfileInput{
				InstanceProfileName: aws.String(name), RoleName: r.RoleName,
			}); err != nil && !isNoSuchEntity(err) {
				return false, fmt.Errorf("runtimeiam: remove role %s from instance profile %s: %w", aws.ToString(r.RoleName), name, err)
			}
		}
	}
	if _, err := client.DeleteInstanceProfile(ctx, &iam.DeleteInstanceProfileInput{InstanceProfileName: aws.String(name)}); err != nil {
		if isNoSuchEntity(err) {
			return false, nil
		}
		return false, fmt.Errorf("runtimeiam: delete instance profile %s: %w", name, err)
	}
	return true, nil
}

func profileIsManaged(ctx context.Context, client InstanceRoleDeleteAPI, name string) (bool, error) {
	in := &iam.ListInstanceProfileTagsInput{InstanceProfileName: aws.String(name)}
	for {
		page, err := client.ListInstanceProfileTags(ctx, in)
		if err != nil {
			if isNoSuchEntity(err) {
				return false, nil
			}
			return false, fmt.Errorf("runtimeiam: read tags of instance profile %s: %w", name, err)
		}
		if hasManagedTag(page.Tags) {
			return true, nil
		}
		if !page.IsTruncated || page.Marker == nil {
			return false, nil
		}
		in.Marker = page.Marker
	}
}

func hasManagedTag(tags []iamtypes.Tag) bool {
	for _, t := range tags {
		if aws.ToString(t.Key) == ManagedProfileTagKey && aws.ToString(t.Value) == ManagedProfileTagValue {
			return true
		}
	}
	return false
}
