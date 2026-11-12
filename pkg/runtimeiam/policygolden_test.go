package runtimeiam

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

var updateRuntimePolicyGolden = flag.Bool("update-runtime-policy", false, "rewrite "+runtimePolicyGolden)

// runtimePolicyGolden is the runtime policy for a fixed account, one
// authorized instance role and one authorized SageMaker execution role,
// pretty-printed.
const runtimePolicyGolden = "testdata/runtime-policy.golden.json"

// TestPolicyDocumentGolden pins the exact runtime policy. Any change to what
// the poller may do shows up as a reviewed diff of this file; regenerate with
// `go test ./pkg/runtimeiam -run TestPolicyDocumentGolden -args -update-runtime-policy`.
func TestPolicyDocumentGolden(t *testing.T) {
	doc, err := PolicyDocumentFor("123456789012", Authorizations{
		InstanceRoles:  []string{"my-instance-role"},
		SageMakerRoles: []string{"arn:aws:iam::123456789012:role/service-role/my-sagemaker-role"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, []byte(doc), "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')
	if *updateRuntimePolicyGolden {
		if err := os.MkdirAll(filepath.Dir(runtimePolicyGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(runtimePolicyGolden, pretty.Bytes(), 0o644); err != nil { //nolint:gosec // a test fixture in the repository
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(runtimePolicyGolden)
	if err != nil {
		t.Fatalf("read %s: %v (generate it with -update-runtime-policy)", runtimePolicyGolden, err)
	}
	if !bytes.Equal(pretty.Bytes(), want) {
		t.Errorf("the runtime policy changed; review and regenerate %s with -update-runtime-policy.\ngot:\n%s", runtimePolicyGolden, pretty.String())
	}
}

// TestPolicyHasNoPolicyVariables: a policy variable such as
// ${aws:PrincipalAccount} in a Resource passes Access Analyzer and the policy
// simulator but fails a real PutRolePolicy ("failed legacy parsing"), which broke
// every spawn launch in spawn#1069. The account is always a literal here.
func TestPolicyHasNoPolicyVariables(t *testing.T) {
	doc, err := PolicyDocument("123456789012", []string{"my-instance-role"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(doc, "${") {
		t.Errorf("the runtime policy contains a policy variable:\n%s", doc)
	}
}

// policyConditionKeys are the condition keys the runtime policy may use. A key
// outside this set fails the test so that a new one is checked against the
// Service Authorization Reference before it ships: an unsupported key is a
// silent implicit deny that PutRolePolicy accepts.
var policyConditionKeys = map[string]bool{
	"iam:PassedToService": true,
	"iam:AWSServiceName":  true,
	"aws:CalledVia":       true,
}

// policyConditionKeyPrefixes are the tag keys, matched by prefix.
var policyConditionKeyPrefixes = []string{"ec2:ResourceTag/", "aws:RequestTag/", "aws:ResourceTag/"}

// TestPolicyConditionKeysMatchActionService: a condition key belongs to the
// ACTION's service. A service-prefixed key (ec2:ResourceTag/…) on an action of
// another service (fsx:, s3:) never matches, so the grant is a silent implicit
// deny, the bug that once disarmed spawn's production reaper (an ec2: key on an
// ssm: action, spawn#665). Global aws: keys may appear on any action.
func TestPolicyConditionKeysMatchActionService(t *testing.T) {
	doc, err := PolicyDocument("123456789012", []string{"my-instance-role"})
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Statement []struct {
			Action    []string
			Condition map[string]map[string]interface{}
		}
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	conditioned := 0
	for i, st := range parsed.Statement {
		for op, kv := range st.Condition {
			for key := range kv {
				conditioned++
				known := policyConditionKeys[key]
				for _, p := range policyConditionKeyPrefixes {
					known = known || strings.HasPrefix(key, p)
				}
				if !known {
					t.Errorf("statement %d: condition key %q (%s) is not one this test knows; check it in the "+
						"Service Authorization Reference for %v and add it", i, key, op, st.Action)
				}
				svc, _, _ := strings.Cut(key, ":")
				if svc == "aws" {
					continue
				}
				for _, a := range st.Action {
					if !strings.HasPrefix(a, svc+":") {
						t.Errorf("statement %d: condition key %q is a %s: key, but it gates %s, an action of another "+
							"service; it would never match (a silent implicit deny)", i, key, svc, a)
					}
				}
			}
		}
	}
	if conditioned < 8 {
		t.Fatalf("found only %d condition keys; the parse is broken", conditioned)
	}
}

// TestDestructiveGrantsAreTagGated (#242): the new destructive grants only reach
// what spawn created, on the tag spawn stamps, with the key of the action's own
// service.
func TestDestructiveGrantsAreTagGated(t *testing.T) {
	doc, err := PolicyDocument("123456789012", nil)
	if err != nil {
		t.Fatal(err)
	}
	grants, err := ParsePolicy(doc)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"ec2:TerminateInstances":         `ec2:ResourceTag/spawn:managed`,
		"ec2:CancelSpotInstanceRequests": `ec2:ResourceTag/spawn:managed`,
		"fsx:DeleteFileSystem":           `aws:ResourceTag/spawn:managed`,
		"fsx:CreateFileSystem":           `aws:RequestTag/spawn:managed`,
		"fsx:TagResource":                `aws:RequestTag/spawn:managed`,
	}
	for action, key := range want {
		found := false
		for _, g := range grants {
			if g.Action != action {
				continue
			}
			found = true
			if !strings.Contains(g.Condition, key) || !strings.Contains(g.Condition, `"true"`) {
				t.Errorf("%s on %s is not gated on %s=true: %q", action, g.Resource, key, g.Condition)
			}
			if g.Resource == "*" || !strings.Contains(g.Resource, "123456789012") {
				t.Errorf("%s is granted on %q, not an account-scoped ARN", action, g.Resource)
			}
		}
		if !found {
			t.Errorf("the runtime policy does not grant %s (#242)", action)
		}
	}
}

// TestS3GrantsAreScopedToTheFSxPrefix (#242): every S3 grant is on
// arn:aws:s3:::lagotto-fsx-* and nothing wider, and the prefix the policy
// grants is the one `lagotto watch` and the poller enforce. Widening a grant
// back to arn:aws:s3:::* (or to an account condition) fails here.
func TestS3GrantsAreScopedToTheFSxPrefix(t *testing.T) {
	if FSxBucketPrefix != watcher.FSxBucketPrefix {
		t.Fatalf("runtimeiam.FSxBucketPrefix %q != watcher.FSxBucketPrefix %q: the poller would be granted one prefix "+
			"and lagotto would enforce another", FSxBucketPrefix, watcher.FSxBucketPrefix)
	}
	doc, err := PolicyDocument("123456789012", []string{"my-instance-role"})
	if err != nil {
		t.Fatal(err)
	}
	grants, err := ParsePolicy(doc)
	if err != nil {
		t.Fatal(err)
	}
	want := "arn:aws:s3:::" + FSxBucketPrefix + "*"
	seen := map[string]bool{}
	for _, g := range grants {
		if !strings.HasPrefix(g.Action, "s3:") {
			continue
		}
		seen[g.Action] = true
		if g.Resource != want {
			t.Errorf("%s is granted on %q; S3 grants must be on %q only", g.Action, g.Resource, want)
		}
	}
	for _, a := range []string{"s3:CreateBucket", "s3:ListBucket", "s3:GetBucketTagging", "s3:PutBucketTagging"} {
		if !seen[a] {
			t.Errorf("the fsx_create bucket action %s is not granted", a)
		}
	}
}
