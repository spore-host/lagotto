package main

import "testing"

// TestIdleTeardownIsReportOnly: this release's poller never deletes in its
// idle teardown (lagotto#326). Reap is lagotto#328, after the report has been
// read on real accounts; this test is what that change has to edit.
func TestIdleTeardownIsReportOnly(t *testing.T) {
	if reapIdle {
		t.Fatal("the poller's idle teardown is in reap mode; this release ships report mode only (lagotto#326, lagotto#328)")
	}
}

func TestAccountFromARN(t *testing.T) {
	for arn, want := range map[string]string{
		"arn:aws:lambda:us-east-1:123456789012:function:lagotto-capacity-poller": "123456789012",
		"": "", "not-an-arn": "", "arn:aws:lambda": "",
	} {
		if got := accountFromARN(arn); got != want {
			t.Errorf("accountFromARN(%q) = %q, want %q", arn, got, want)
		}
	}
}
