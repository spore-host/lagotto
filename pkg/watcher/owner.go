package watcher

// OwnedBy reports whether the caller identified by callerUserID (sts
// GetCallerIdentity UserId) and callerARN owns a record with the given
// ownerID and userID.
//
// A record that carries an owner id is owned only by that exact principal id:
// the ARN is not consulted, so a role deleted and re-created under the same
// name (a new role id) does not inherit the old one's watches. A legacy record
// with no owner id falls back to the ARN comparison it was always checked
// with, and one with neither is unowned (accessible), as before.
//
// The owner id is the value IAM exposes as ${aws:userid}, so it is the key an
// owner-scoped table design can be authorized on.
func OwnedBy(ownerID, userID, callerUserID, callerARN string) bool {
	if ownerID != "" {
		return callerUserID != "" && ownerID == callerUserID
	}
	return userID == "" || userID == callerARN
}
