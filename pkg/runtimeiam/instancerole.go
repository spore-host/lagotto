package runtimeiam

import (
	"context"
	"errors"
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

// InstanceProfileTags are [ManagedProfileTags] plus the spore.host infrastructure tags
// (#324, scope "global"): what `setup` puts on lagotto's instance role, its
// profile, and the profile it creates for an authorized role.
func InstanceProfileTags(stamp ...managedtags.Stamp) map[string]string {
	return managedtags.With(ManagedProfileTags(),
		managedtags.FirstStamp(stamp).Infrastructure(managedtags.ScopeGlobal, managedtags.ComponentInstanceRole))
}

// backfillTags are the tags [EnsureInstanceRoleTags] may add to a role or
// profile lagotto created: [InstanceProfileTags] without the
// [ManagedProfileTagKey] marker, which is written only by lagotto's own
// successful create ([CreateInstanceProfile], or spawn's create of lagotto's
// own role), never onto something found (lagotto#341).
func backfillTags(stamp ...managedtags.Stamp) map[string]string {
	return managedtags.FirstStamp(stamp).Infrastructure(managedtags.ScopeGlobal, managedtags.ComponentInstanceRole)
}

// createdByLagotto reports whether t says lagotto created the role or
// profile: it carries the [ManagedProfileTagKey] marker its create wrote, and
// no other tool's ownership tags. For lagotto's own fixed name (reserved
// true) only a spore-host:managed-by naming another tool counts against it,
// since lagotto created that role and profile through spawn's library, which
// stamps its spawn:* tags on whatever it creates; for any other name spawn's
// tags count too ([managedtags.ForeignOwner]).
func createdByLagotto(t map[string]string, reserved bool) bool {
	if t[ManagedProfileTagKey] != ManagedProfileTagValue {
		return false
	}
	if reserved {
		return managedtags.ForeignManagedBy(t) == ""
	}
	return managedtags.ForeignOwner(t) == ""
}

// InstanceProfileCreateAPI is the slice of IAM [CreateInstanceProfile] needs.
// *iam.Client satisfies it.
type InstanceProfileCreateAPI interface {
	CreateInstanceProfile(ctx context.Context, in *iam.CreateInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.CreateInstanceProfileOutput, error)
}

// CreateInstanceProfile is lagotto's own create of the instance profile
// name, tagged [InstanceProfileTags] in the same request, so the
// [ManagedProfileTagKey] marker exists only on a profile this create made.
// created is false, with no error, when the profile already exists: a found
// profile is never tagged here, whoever made it. The role is added to it
// afterwards by spawn's CreateOrGetInstanceProfile, which finds the profile
// and adds the role to it when it is empty.
//
// Before lagotto#341 setup let spawn's library create the profile, and that
// library stamps spawn:managed=true and spawn:created-by=spawn on every
// profile it creates, so a profile lagotto created carried two tools'
// ownership tags.
func CreateInstanceProfile(ctx context.Context, client InstanceProfileCreateAPI, name string, stamp ...managedtags.Stamp) (created bool, err error) {
	_, err = client.CreateInstanceProfile(ctx, &iam.CreateInstanceProfileInput{
		InstanceProfileName: aws.String(name),
		Tags:                iamtags.FromMap(InstanceProfileTags(stamp...)),
	})
	var exists *iamtypes.EntityAlreadyExistsException
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exists):
		return false, nil
	}
	return false, fmt.Errorf("runtimeiam: create instance profile %s: %w", name, err)
}

// InstanceRoleTagAPI is the slice of IAM [EnsureInstanceRoleTags] needs.
// *iam.Client satisfies it.
type InstanceRoleTagAPI interface {
	GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	TagRole(ctx context.Context, in *iam.TagRoleInput, optFns ...func(*iam.Options)) (*iam.TagRoleOutput, error)
	ListInstanceProfileTags(ctx context.Context, in *iam.ListInstanceProfileTagsInput, optFns ...func(*iam.Options)) (*iam.ListInstanceProfileTagsOutput, error)
	TagInstanceProfile(ctx context.Context, in *iam.TagInstanceProfileInput, optFns ...func(*iam.Options)) (*iam.TagInstanceProfileOutput, error)
}

// EnsureInstanceRoleTags adds the spore.host tags (#324) to what lagotto
// created for the instance role name but an older lagotto left without them:
// the tags each lacks are added, and a spore-host:created already there is
// kept. It runs after the role and profile were ensured, so a missing one is
// an error.
//
// Only what lagotto created is tagged, and that is decided from the marker
// lagotto's create wrote ([ManagedProfileTagKey]), never added here: a role
// or profile without it, or one carrying another tool's ownership tags, is
// left exactly as it is (lagotto#341, where a profile already claimed by
// spawn's tags gained spore-host:managed-by=lagotto). For lagotto's own
// [InstanceRoleName] that is the role and its profile; for an
// administrator's authorized role it is the profile alone, and the role is
// never tagged.
func EnsureInstanceRoleTags(ctx context.Context, client InstanceRoleTagAPI, name string, stamp ...managedtags.Stamp) error {
	desired := backfillTags(stamp...)
	reserved := name == InstanceRoleName
	if reserved {
		out, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
		if err != nil {
			return fmt.Errorf("runtimeiam: read role %s: %w", name, err)
		}
		var have map[string]string
		if out.Role != nil {
			have = iamtags.ToMap(out.Role.Tags)
		}
		if createdByLagotto(have, true) {
			if add := managedtags.Converge(have, desired); len(add) > 0 {
				if _, err := client.TagRole(ctx, &iam.TagRoleInput{RoleName: aws.String(name), Tags: iamtags.FromMap(add)}); err != nil {
					return fmt.Errorf("runtimeiam: tag role %s: %w", name, err)
				}
			}
		}
	}
	have, err := profileTags(ctx, client, name)
	if err != nil {
		return err
	}
	if !createdByLagotto(have, reserved) {
		return nil // not lagotto's to tag
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
// is never touched. A profile that also carries another tool's ownership tags
// ([managedtags.ForeignOwner], such as spawn:managed=true) is refused with an
// error wrapping [ErrClaimedByAnotherTool]: two tools claim it, and lagotto
// does not delete what another tool may still use (lagotto#341). Called when
// `setup --revoke-instance-role` revokes the role, and by `teardown
// --iam-roles` for every role still authorized.
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
		have, err := profileTags(ctx, client, name)
		if err != nil {
			if isNoSuchEntity(err) {
				return false, nil
			}
			return false, err
		}
		if have[ManagedProfileTagKey] != ManagedProfileTagValue {
			return false, nil
		}
		if tool := managedtags.ForeignOwner(have); tool != "" {
			return false, &ClaimedError{Name: name, Tool: tool}
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

// ErrClaimedByAnotherTool is what the instance-profile deleters wrap when a
// profile carries another tool's ownership tags as well as lagotto's.
var ErrClaimedByAnotherTool = errors.New("claimed by another tool")

// ClaimedError is the refusal to delete instance profile Name, whose tags say
// Tool manages it too.
type ClaimedError struct{ Name, Tool string }

func (e *ClaimedError) Error() string {
	return fmt.Sprintf("instance profile %s left in place: its tags say %s manages it as well as lagotto, and lagotto "+
		"does not delete what another tool may still use. Remove lagotto's tags from it (%s, %s) to settle its owner",
		e.Name, e.Tool, ManagedProfileTagKey, "spore-host:*")
}

func (e *ClaimedError) Unwrap() error { return ErrClaimedByAnotherTool }

// DeleteInstanceProfile is the per-profile deleter of `lagotto teardown
// --plan` (#325): lagotto's own instance profile ([InstanceRoleName]) as
// [DeleteInstanceRole] removes it, and any other profile only as
// [DeleteManagedInstanceProfile] does, when `setup` created it. deleted is
// false when the profile was absent or, for another name, lacks lagotto's tag.
func DeleteInstanceProfile(ctx context.Context, client InstanceRoleDeleteAPI, name string) (deleted bool, err error) {
	return deleteProfile(ctx, client, name, name != InstanceRoleName)
}
