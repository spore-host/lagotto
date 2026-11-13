package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Owner-scoped keys.
//
// Every record a CLI user creates is keyed "<owner>/<id>": the owner is the
// creator's principal id (sts GetCallerIdentity UserId, the value IAM exposes
// as ${aws:userid}), and the id is the short, user-facing one ("w-1a2b3c4d",
// "g-<group>", "sl-1a2b3c4d"). The key is the table's partition key, so the
// lagotto-cli-user policy can allow DynamoDB item access only where
// dynamodb:LeadingKeys matches "${aws:userid}/*": IAM, not the CLI, keeps one
// principal's records from another's (pkg/cliiam).
//
// The poller treats a key as opaque, except where it derives one record from
// another (a watch's group, a group's members): those are always taken from
// the same owner scope, so a record can never steer the poller onto a record
// under another owner's prefix.
//
// A key with no "/" is a legacy key, from before owner scoping. `lagotto setup`
// moves legacy records under their owner's prefix (MigrateOwnerKeys).

// MinOwnerKeyPollerVersion is the first poller release that resolves
// owner-scoped keys (groups in the owner's scope, SageMaker job and retry
// schedule names from the short id). Owner keys are breaking for older
// pollers, so this release is a minor bump.
const MinOwnerKeyPollerVersion = "0.70.0"

// KeySeparator separates the owner from the id in a key. It cannot occur in an
// IAM unique id, a role session name or a federated user name.
const KeySeparator = "/"

// OwnerKey returns the key of id under owner, or id unchanged when owner is
// empty (no caller identity: a legacy key).
func OwnerKey(owner, id string) string {
	if owner == "" {
		return id
	}
	return owner + KeySeparator + id
}

// SplitKey splits a key into its owner and short id. A legacy key has no owner.
func SplitKey(key string) (owner, id string) {
	if i := strings.Index(key, KeySeparator); i >= 0 {
		return key[:i], key[i+1:]
	}
	return "", key
}

// KeyOwner is the owner of a key ("" for a legacy key).
func KeyOwner(key string) string {
	o, _ := SplitKey(key)
	return o
}

// ShortID is the user-facing id of a key: what the CLI prints and accepts.
func ShortID(key string) string {
	_, id := SplitKey(key)
	return id
}

// IsLegacyKey reports whether a key predates owner scoping.
func IsLegacyKey(key string) bool { return !strings.Contains(key, KeySeparator) }

// SameScope reports whether two keys belong to the same owner. Two legacy keys
// are in the same (legacy) scope.
func SameScope(a, b string) bool { return KeyOwner(a) == KeyOwner(b) }

// ScopedName qualifies a name a record refers to (a watch's group) with the
// owner of the record's own key, so the reference resolves only inside that
// owner's scope.
func ScopedName(key, name string) string { return OwnerKey(KeyOwner(key), name) }

// OwnerTag is a short, stable, name-safe digest of an owner, for AWS resource
// names that must be unique per account but cannot hold the owner itself
// (SageMaker job names, which allow only [A-Za-z0-9-]). Empty for no owner.
func OwnerTag(owner string) string {
	if owner == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(owner))
	return hex.EncodeToString(sum[:])[:6]
}
