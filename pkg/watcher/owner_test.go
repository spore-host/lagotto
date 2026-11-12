package watcher

import "testing"

func TestOwnedBy(t *testing.T) {
	const (
		arn   = "arn:aws:sts::123456789012:assumed-role/Dev/alice"
		alice = "AROAEXAMPLEROLEID:alice"
	)
	cases := []struct {
		name                       string
		ownerID, userID, cid, carn string
		want                       bool
	}{
		{"owner id matches", alice, arn, alice, arn, true},
		{"owner id set, same ARN, different principal id (role re-created)", alice, arn, "AROANEWROLEID:alice", arn, false},
		{"owner id set, caller id unknown", alice, arn, "", arn, false},
		{"legacy: ARN matches", "", arn, alice, arn, true},
		{"legacy: ARN differs", "", arn, "AIDABOB", "arn:aws:iam::123456789012:user/bob", false},
		{"legacy: unowned", "", "", "AIDABOB", "arn:aws:iam::123456789012:user/bob", true},
	}
	for _, c := range cases {
		if got := OwnedBy(c.ownerID, c.userID, c.cid, c.carn); got != c.want {
			t.Errorf("%s: OwnedBy = %v, want %v", c.name, got, c.want)
		}
	}
}
