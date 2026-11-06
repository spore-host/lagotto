package runtimeiam

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/internal/iamcoverage"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The poller coverage gate (#242).
//
// The CLI gate (cmd TestCLIActionInventoryIsCurrent) documents what a USER's
// credentials need. Nothing checked the other principal: the hosted poller,
// which runs as RoleName with the policy PolicyDocument writes. The two drifted —
// spawn's launcher grew Terminate, FSx and S3 calls on the poller's path and the
// policy never followed, so an fsx_create watch launched an instance it could
// neither finish setting up nor terminate.
//
// This gate loads the capacity-poller Lambda (its main and the handler the
// Lambda runtime invokes), discovers every AWS action reachable from it with
// internal/iamcoverage (through lagotto, spawn and truffle), and fails when one
// is neither granted by PolicyDocument nor listed in pollerNotGranted with a
// reason. It checks ACTIONS, not resources or conditions: a grant gated on a
// tag still counts. The one exception is a grant gated on aws:CalledVia, which
// authorizes a call another service makes on the poller's behalf and never the
// poller's own call, so it is not counted.

// pollerNotGranted are actions the poller's code can reach but the runtime
// policy deliberately does not grant, each with why. The discovery is a sound
// over-approximation (it follows every path, including ones lagotto's inputs
// can never take), so these are mostly paths that are unreachable by design.
//
// An entry here must still be in the inventory (else it is stale) and must not
// be granted (else it is pointless); TestPollerPolicyCoversItsCalls checks both.
var pollerNotGranted = map[string]string{
	// Multi-interface EFA launches (spawn#661): spawn's Client.Launch allocates
	// and associates an Elastic IP only when the launch has more than one network
	// interface, and Client.Terminate disassociates/releases one only when it is
	// a spawn-managed EIP tagged to the instance, which only an EFA launch makes.
	// --spawn-config has no EFA key and lagotto never sets LaunchConfig.EFA*
	// (#243). ec2:DescribeAddresses, which every Terminate makes, IS granted.
	"ec2:AllocateAddress":     "EFA-only (spawn attachLaunchElasticIP); no efa spawn_config key (#243)",
	"ec2:AssociateAddress":    "EFA-only (spawn attachLaunchElasticIP); no efa spawn_config key (#243)",
	"ec2:DisassociateAddress": "EFA-only (spawn releaseInstanceElasticIPs, a spawn-managed EIP only an EFA launch creates) (#243)",
	"ec2:ReleaseAddress":      "EFA-only (spawn attachLaunchElasticIP rollback / releaseInstanceElasticIPs) (#243)",

	// spawn's PreflightNoPublicIP → DescribePrivateNetwork, gated on
	// LaunchConfig.NoPublicIP, which lagotto never sets (no no_public_ip key).
	// (The policy grants ec2:DescribeRouteTables only via aws:CalledVia FSx, which
	// does not authorize this call.)
	"ec2:DescribeRouteTables":  "NoPublicIP preflight only; lagotto never sets LaunchConfig.NoPublicIP (#243)",
	"ec2:DescribeVpcEndpoints": "NoPublicIP preflight only; lagotto never sets LaunchConfig.NoPublicIP (#243)",

	// spawn's propagateSnapshotTagsToVolumes runs only for LaunchConfig.AttachVolumes
	// (EBS volumes from snapshots), which ToLaunchConfig never sets; a denial is
	// ignored there anyway.
	"ec2:DescribeSnapshots": "AttachVolumes only; SpawnConfigFile.ToLaunchConfig never sets it",

	// The launch config object (spawn#1359): Client.Launch writes it to the
	// staging bucket only when the launch fixes a spored setting that no longer
	// rides in tags (notify/Slack, webhooks, active ports/processes, app mode,
	// idle CPU %, metrics/tracing). lagotto sets none, so the body is nil and
	// nothing is uploaded (#243). The FSx bucket's own S3 actions are granted.
	"s3:GetLifecycleConfiguration":  "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutLifecycleConfiguration":  "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutEncryptionConfiguration": "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutBucketPublicAccessBlock": "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutBucketOwnershipControls": "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutObject":                  "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",
	"s3:PutObjectTagging":           "launch-config object only (spawn#1359); lagotto sets no launch-fixed spored setting (#243)",

	// Implied by s3:CreateBucket with CreateBucketConfiguration.Tags, which
	// spawn's createBucketIfNotExists never sets: it tags with a separate
	// PutBucketTagging, which is granted.
	"s3:TagResource": "tag-on-create for CreateBucket; spawn never passes CreateBucketConfiguration.Tags",

	// Reachable, and denied on purpose. spawn's NewLaunchTagger reads the
	// account name best-effort for the spawn:account-name DNS alias; denied, the
	// DNS updater uses the base36 account label (spawn#121). Granting it would
	// change the hostname of every poller-launched instance, which #242 does not
	// ask for.
	"account:GetAccountInformation": "best-effort account-name DNS alias; denial falls back to base36 (spawn#121)",

	// Reachable only with AUTO_DELETE_TABLES=true, an opt-in a human adds to the
	// function by hand; deploy never sets it (pkg/deploy PollerEnvVars, #59). With
	// it set and these denied, the poller logs a warning and keeps the tables.
	// The runtime role is not meant to be able to delete lagotto's data tables.
	"dynamodb:DeleteTable":        "AUTO_DELETE_TABLES opt-in only (unset by deploy, #59); the poller role must not delete tables",
	"dynamodb:ListTagsOfResource": "AUTO_DELETE_TABLES opt-in only (unset by deploy, #59): the managed-tag check before DeleteTable",
}

// pollerNotGrantedTripwires: the spawn_config keys whose existence would make an
// "unreachable" entry above reachable. If lagotto ever grows one of these keys,
// the reason above is no longer true and the gate fails here, not in production.
var pollerNotGrantedTripwires = []string{
	"efa", "public_ip", "egress", "attach_volume", "snapshot",
	"notify", "slack", "webhook", "active_port", "active_process", "app_mode", "ready_port",
	"idle_cpu", "metrics", "tracing",
}

// pollerGrantedActions are the actions PolicyDocument grants in a way that
// authorizes the poller's own calls, with the authorized-instance-role
// statements included (a named role only widens resources, not actions).
func pollerGrantedActions(t *testing.T, doc string) []string {
	t.Helper()
	grants, err := ParsePolicy(doc)
	if err != nil {
		t.Fatalf("parse runtime policy: %v", err)
	}
	var out []string
	for _, g := range grants {
		if !strings.EqualFold(g.Effect, "Allow") || strings.Contains(g.Condition, "aws:CalledVia") {
			continue
		}
		out = append(out, g.Action)
	}
	return out
}

// pollerCoverageProblems is the gate's decision, separated from the loading so
// it is tested on its own (TestPollerCoverageProblemsDecision). inventory maps
// each reachable action to a description of where it is called.
func pollerCoverageProblems(inventory map[string]string, granted []string, notGranted map[string]string) []string {
	isGranted := func(action string) bool {
		for _, g := range granted {
			if wildcardMatchFold(g, action) {
				return true
			}
		}
		return false
	}
	var problems []string
	for action, where := range inventory {
		if isGranted(action) {
			continue
		}
		if _, ok := notGranted[action]; ok {
			continue
		}
		problems = append(problems, fmt.Sprintf("the poller calls %s but the runtime policy does not grant it.\n"+
			"    %s\n"+
			"    Grant it in PolicyDocument, or, if the call is unreachable by design, add it to "+
			"pollerNotGranted with the reason.", action, where))
	}
	for action, reason := range notGranted {
		if strings.TrimSpace(reason) == "" {
			problems = append(problems, fmt.Sprintf("pollerNotGranted[%s] has no reason", action))
		}
		if isGranted(action) {
			problems = append(problems, fmt.Sprintf("pollerNotGranted lists %s, which the runtime policy grants; remove the entry", action))
		}
		if _, ok := inventory[action]; !ok {
			problems = append(problems, fmt.Sprintf("pollerNotGranted lists %s, which the poller no longer calls; remove the stale entry", action))
		}
	}
	sort.Strings(problems)
	return problems
}

// TestPollerPolicyCoversItsCalls is the gate.
func TestPollerPolicyCoversItsCalls(t *testing.T) {
	// The Lambda is built for linux/arm64 (pkg/deploy, provided.al2023).
	prog, err := iamcoverage.Load(iamcoverage.LoadConfig{
		Dir: "../../lambda/capacity-poller", GOOS: "linux", GOARCH: "arm64",
		Patterns: []string{"."}, Root: "../..",
	})
	if err != nil {
		t.Fatal(err)
	}
	var sawHandler bool
	for _, m := range prog.Mains() {
		sawHandler = sawHandler || strings.HasSuffix(m.String(), "capacity-poller.handler")
	}
	if !sawHandler {
		t.Fatalf("the Lambda handler is not among the entry points %v; the gate would scan only main", prog.Mains())
	}
	inv := prog.Calls(prog.Mains(), nil)
	if inv.Reached < 1000 {
		t.Fatalf("reached only %d functions from the poller; the call graph is broken", inv.Reached)
	}
	for _, u := range inv.Unmapped {
		t.Errorf("an AWS operation the poller makes that cannot be mapped to an IAM action: %s", u)
	}

	// Documented intent: if the discovery stops seeing these, it is broken, not
	// the poller fixed.
	for _, want := range []string{"ec2:RunInstances", "ec2:TerminateInstances", "fsx:CreateFileSystem", "sns:Publish", "scheduler:CreateSchedule"} {
		if _, ok := inv.Actions[want]; !ok {
			t.Errorf("the poller inventory lacks %s; the discovery is broken", want)
		}
	}

	inventory := map[string]string{}
	for action, sites := range inv.Actions {
		s := sites[0]
		inventory[action] = fmt.Sprintf("%s at %s, via %s", s.Op, s.Pos, strings.Join(inv.PathTo(s.Func), " -> "))
	}

	doc, err := PolicyDocument("123456789012", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pollerCoverageProblems(inventory, pollerGrantedActions(t, doc), pollerNotGranted) {
		t.Error(p)
	}
}

// TestPollerNotGrantedTripwires: the "unreachable by design" reasons rest on
// spawn_config having no key that switches those paths on.
func TestPollerNotGrantedTripwires(t *testing.T) {
	for _, key := range watcher.SpawnConfigKeys {
		for _, wire := range pollerNotGrantedTripwires {
			if strings.Contains(key, wire) {
				t.Errorf("spawn_config key %q may make a pollerNotGranted path reachable (%q); "+
					"re-check those reasons and grant what is now needed", key, wire)
			}
		}
	}
}

// TestPollerCoverageProblemsDecision pins the gate's rules without loading
// the program, including the revert case: the policy losing a grant the poller
// needs fails.
func TestPollerCoverageProblemsDecision(t *testing.T) {
	inventory := map[string]string{
		"ec2:RunInstances":       "launch",
		"ec2:TerminateInstances": "terminate",
		"ec2:AllocateAddress":    "efa",
	}
	notGranted := map[string]string{"ec2:AllocateAddress": "efa only"}

	if p := pollerCoverageProblems(inventory, []string{"ec2:RunInstances", "ec2:TerminateInstances"}, notGranted); len(p) != 0 {
		t.Errorf("a covered inventory reported problems: %v", p)
	}
	if p := pollerCoverageProblems(inventory, []string{"ec2:*"}, map[string]string{}); len(p) != 0 {
		t.Errorf("a wildcard grant did not cover: %v", p)
	}

	// Revert: TerminateInstances not granted and not allowlisted.
	p := pollerCoverageProblems(inventory, []string{"ec2:RunInstances"}, notGranted)
	if len(p) != 1 || !strings.Contains(p[0], "ec2:TerminateInstances") || !strings.Contains(p[0], "terminate") {
		t.Errorf("a missing grant was not reported with its call site: %v", p)
	}

	// Stale, redundant and reasonless allowlist entries.
	p = pollerCoverageProblems(map[string]string{"ec2:RunInstances": "launch"}, []string{"ec2:RunInstances"},
		map[string]string{"ec2:AllocateAddress": "efa only", "ec2:RunInstances": "", "s3:PutObject": "x"})
	joined := strings.Join(p, "\n")
	for _, want := range []string{"ec2:AllocateAddress, which the poller no longer calls", "pollerNotGranted[ec2:RunInstances] has no reason",
		"lists ec2:RunInstances, which the runtime policy grants", "s3:PutObject, which the poller no longer calls"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing problem %q in:\n%s", want, joined)
		}
	}
}

// TestPollerGrantedActionsIgnoresCalledVia: a grant gated on aws:CalledVia
// authorizes what FSx calls on the poller's behalf, never the poller's own call.
func TestPollerGrantedActionsIgnoresCalledVia(t *testing.T) {
	doc := `{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Action":["ec2:DescribeRouteTables"],"Resource":"*",
	   "Condition":{"ForAnyValue:StringEquals":{"aws:CalledVia":["fsx.amazonaws.com"]}}},
	  {"Effect":"Allow","Action":["ec2:DescribeVpcs"],"Resource":"*"}]}`
	got := pollerGrantedActions(t, doc)
	if len(got) != 1 || got[0] != "ec2:DescribeVpcs" {
		t.Errorf("granted = %v, want only ec2:DescribeVpcs", got)
	}
}
