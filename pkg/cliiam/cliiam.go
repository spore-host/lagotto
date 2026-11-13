// Package cliiam owns the customer-managed IAM policy for the people who RUN the
// lagotto CLI (#232), the counterpart of pkg/runtimeiam, which owns the hosted
// poller's roles.
//
// Every lagotto command runs with the caller's own credentials. Before #232 an
// administrator had to work out from docs/reference/cli-iam-actions.md which
// actions the everyday commands need and grant them by hand, which the owner
// principle forbids ("users must never have to hand-create IAM; spore.host
// provisions any IAM a feature needs"). `lagotto setup` now writes
// [PolicyName], built here, and an administrator attaches it to a group, user or
// role. `lagotto teardown --iam-roles` is its named deleter ([DeletePolicy]), and
// `lagotto doctor` simulates the caller against it ([CheckCaller]).
//
// # Held to the inventory
//
// The policy is not a hand-maintained list. cmd's TestCLIUserPolicyMatchesInventory
// compares the actions [PolicyDocument] grants with the actions the CLI's code
// can reach from each of [EverydayCommands] (discovered by internal/iamcoverage,
// the same data as docs/reference/cli-iam-actions.md), in both directions: an
// everyday command that gains an AWS call fails CI until the policy grants it,
// and a grant no everyday command needs fails CI until it is removed.
//
// # No region, no policy variables
//
// Like the runtime policy (#219), the document is a function of the account
// alone: lagotto's regional resources carry a `*` region so one `setup` serves
// every region. The account is always a literal: a policy variable such as
// ${aws:PrincipalAccount} in a Resource passes Access Analyzer and the policy
// simulator but fails a real CreatePolicy ("failed legacy parsing", spawn#1069).
// The one variable used, ${aws:userid}, is in Condition values only (the
// owner-scoped item access), where IAM resolves it per request.
package cliiam

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	// PolicyName is the customer-managed policy `lagotto setup` writes. Fixed, so
	// its ARN is derivable from the account alone ([PolicyARN]).
	PolicyName = "lagotto-cli-user"
	// PolicyPath is the policy's IAM path. The default path keeps the ARN short
	// and predictable.
	PolicyPath = "/"
	// PolicyDescription is what the IAM console shows for the policy.
	PolicyDescription = "lagotto CLI everyday commands (watch, update, ack, extend, cancel, list, status, history, launch, " +
		"notify-gc, doctor). Written by 'lagotto setup'; deleted by 'lagotto teardown --iam-roles'."

	// ManagedTagKey/ManagedTagValue mark the policy as lagotto's. [EnsurePolicy]
	// refuses to rewrite, and [DeletePolicy] refuses to delete, a policy of this
	// name without it: lagotto changes only what it created, as with the
	// scheduled-launches table (lagotto:managed=cli).
	ManagedTagKey   = "lagotto:managed"
	ManagedTagValue = "cli"

	// maxManagedPolicyChars is IAM's size limit for a customer-managed policy
	// document, counted without whitespace.
	maxManagedPolicyChars = 6144
)

// PolicyARN returns the policy's ARN in accountID. IAM is global.
func PolicyARN(accountID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:policy%s%s", accountID, PolicyPath, PolicyName)
}

// EverydayCommands are the commands the policy covers: what a watch user runs
// day to day, and nothing that creates or deletes lagotto's infrastructure. The
// list is the policy's contract with the inventory; cmd's tests require every
// command to be either here or in its administrator list, so a new command
// cannot ship unclassified.
//
// Deliberately NOT here:
//
//   - `setup`, `deploy`, `teardown`: administrator commands. They create and
//     delete IAM roles and policies, Lambda functions, topics and tables.
//   - `poll`: it runs the poller on the caller's machine, so it needs what the
//     hosted poller's role has: ec2:RunInstances and iam:PassRole on the
//     instance roles it launches with, which would let every CLI user launch
//     instances directly. A local `poll --daemon` is run by whoever already has
//     those permissions.
//
// `notify-gc` and `version` are included at no cost: notify-gc needs only SNS
// actions on the alerts topic that `update` and `cancel` already need, and
// `version` makes no AWS call.
var EverydayCommands = []string{
	"lagotto ack",
	"lagotto cancel",
	"lagotto doctor",
	"lagotto extend",
	"lagotto history",
	"lagotto launch",
	"lagotto list",
	"lagotto notify-gc",
	"lagotto status",
	"lagotto update",
	"lagotto version",
	"lagotto watch",
}

// The fixed names of lagotto's resources the everyday commands touch.
const (
	alertsTopicName = "lagotto-capacity-alerts"
	// pollerScheduleName is the poller's EventBridge Scheduler schedule, which
	// watch/update/ack/extend re-enable after the poller disabled itself.
	pollerScheduleName = "schedule/default/lagotto-capacity-poller"
	// pollerFunctionName is the poller Lambda (pkg/deploy.PollerFunctionName;
	// TestNamesMatchTheirOwners asserts they agree).
	pollerFunctionName = "lagotto-capacity-poller"
	// artifactBucketPrefix is the prefix of the poller's per-region artifact
	// buckets, lagotto-lambda-<account>-<region> (pkg/deploy.DefaultBucketName),
	// whose tags doctor reads (#264).
	artifactBucketPrefix = "lagotto-lambda-"
	// legacyStackPattern covers the CloudFormation stack doctor looks for
	// (--stack-name, default "lagotto").
	legacyStackPattern = "stack/lagotto*/*"
	// holdTagKey is the tag the poller stamps on every capacity reservation a hold
	// creates (watcher Holder, lagotto:managed=true), so `cancel` can release
	// lagotto's reservations and no other.
	holdTagKey   = "lagotto:managed"
	holdTagValue = "true"
	// ssoRolePath is the path of every IAM Identity Center permission-set role.
	ssoRolePath = "aws-reserved/sso.amazonaws.com/"
)

// Tables are lagotto's three DynamoDB tables, by their fixed default names
// (the same names the runtime policy grants the poller).
var Tables = []string{"lagotto-watches", "lagotto-match-history", watcher.DefaultScheduledTable}

// launchSchedulePrefix is the name prefix of the per-launch schedules `lagotto
// launch` creates (pkg/deploy.LaunchSchedulePrefix).
const launchSchedulePrefix = "lagotto-launch-sl-"

// statement is one policy statement. Sim, when set, lists the resources doctor
// simulates for it instead of Resource (see [CheckCaller]); it is never written.
type statement struct {
	Sid       string      `json:"Sid"`
	Effect    string      `json:"Effect"`
	Action    []string    `json:"Action"`
	Resource  interface{} `json:"Resource"`
	Condition interface{} `json:"Condition,omitempty"`
	sim       []string
	// noSimulate excludes the statement from the caller check.
	noSimulate bool
	// ownerScoped statements are conditioned on the caller's own key prefix;
	// the caller check simulates them with the caller's principal id and a key
	// under it as context.
	ownerScoped bool
	// simSkip are actions of this statement the caller check does not
	// simulate, because the IAM policy simulator denies them for a principal
	// that real IAM allows (see [SimulatorBlindSpot]). They are reported as
	// not simulated, never as missing.
	simSkip []string
}

// SimulatorBlindSpot says why the caller check skips an action: the IAM policy
// simulator denies these for a principal holding exactly this policy, while the
// same principal's real calls succeed (verified 2026-11-07, lagotto#232, with
// an assumed role holding only lagotto-cli-user: a real tagged
// CancelCapacityReservation, GetSubscriptionAttributes and Unsubscribe on an
// alerts-topic subscription all succeeded; the simulator denied all three).
const SimulatorBlindSpot = "the IAM policy simulator denies it for this policy although real IAM allows it (it does not apply ec2:ResourceTag conditions or SNS subscription ARNs), so doctor cannot check it"

// stringEquals is a StringEquals condition block on one key.
func stringEquals(key, value string) map[string]map[string]string {
	return map[string]map[string]string{"StringEquals": {key: value}}
}

// statements builds the policy's statements for accountID. Every Resource of
// "*" is an action IAM does not authorize at the resource level, and says so.
// OwnKeyPattern is the dynamodb:LeadingKeys pattern of the caller's own
// records ([watcher.OwnerKey]). A policy variable in a Condition value is
// resolved per request; only a Resource cannot hold one (spawn#1069).
const OwnKeyPattern = "${aws:userid}" + watcher.KeySeparator + "*"

var (
	ownKeys = map[string]interface{}{
		"ForAllValues:StringLike": map[string][]string{"dynamodb:LeadingKeys": {OwnKeyPattern}},
		"Null":                    map[string]string{"dynamodb:LeadingKeys": "false"},
	}
	ownIndexKey = map[string]interface{}{
		"ForAllValues:StringEquals": map[string][]string{"dynamodb:LeadingKeys": {"${aws:userid}"}},
	}
)

func statements(accountID string, configBuckets []string) []statement {
	regional := func(service, resource string) string { return runtimeiam.RegionalARN(service, accountID, resource) }
	iamARN := func(resource string) string { return fmt.Sprintf("arn:aws:iam::%s:%s", accountID, resource) }

	var tableARNs []string
	for _, t := range Tables {
		tableARNs = append(tableARNs, regional("dynamodb", "table/"+t))
	}
	topicARN := regional("sns", alertsTopicName)

	sts := []statement{
		// lagotto's own tables, table-level. watch and launch create a missing
		// table (with its TTL and lagotto's tags, and the owner index) on first
		// use in a region, scoped to these three names. No item access here.
		{Sid: "LagottoTables", Action: []string{
			"dynamodb:CreateTable", "dynamodb:DescribeTable", "dynamodb:DescribeTimeToLive",
			"dynamodb:TagResource", "dynamodb:UpdateTimeToLive",
		}, Resource: tableARNs},
		// Item access, only to the caller's own records: every record a CLI
		// user creates is keyed "<principal id>/<id>" (watcher.OwnerKey), and
		// dynamodb:LeadingKeys is the partition key of every item a request
		// touches. ${aws:userid} is the caller's principal id (AIDA… for an IAM
		// user; AROA…:<session name> for a role session). The Null condition
		// refuses a request with no partition key (a Scan), which ForAllValues
		// alone would allow. No Scan, no DeleteItem.
		{Sid: "OwnItems", Action: []string{
			"dynamodb:BatchGetItem", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query", "dynamodb:UpdateItem",
		}, Resource: tableARNs, Condition: ownKeys, ownerScoped: true},
		// The owner index, through which `list` finds the caller's watch keys.
		// When IAM evaluates dynamodb:LeadingKeys for an index Query it is the
		// index's partition key, owner_id, which must be the caller's own. The
		// index projects keys only, and every read of a watch itself goes
		// through OwnItems.
		{Sid: "OwnIndex", Action: []string{"dynamodb:Query"},
			Resource:  regional("dynamodb", "table/"+Tables[0]+"/index/"+watcher.OwnerIndexName),
			Condition: ownIndexKey, ownerScoped: true},

		// The poller's schedule: watch, update, ack and extend re-enable a poller
		// that disabled itself when it ran out of watches; doctor reads it.
		{Sid: "PollerSchedule", Action: []string{"scheduler:GetSchedule", "scheduler:UpdateSchedule"},
			Resource: regional("scheduler", pollerScheduleName)},
		// `lagotto launch` creates one schedule per scheduled launch, and
		// `lagotto cancel sl-…` deletes it and the poller's -r<n> boundary
		// retries of it (#279), which share the prefix. Listing them is
		// scheduler:ListSchedules, below (no resource-level permissions).
		{Sid: "LaunchSchedules", Action: []string{"scheduler:CreateSchedule", "scheduler:DeleteSchedule"},
			Resource: regional("scheduler", "schedule/default/"+launchSchedulePrefix+"*")},
		// Creating or updating a schedule passes the role it assumes. Only
		// lagotto's invoke role, and only to EventBridge Scheduler.
		{Sid: "PassSchedulerInvokeRole", Action: []string{"iam:PassRole"},
			Resource:  runtimeiam.SchedulerInvokeRoleARN(accountID),
			Condition: stringEquals("iam:PassedToService", "scheduler.amazonaws.com")},

		// watch/update/list read the poller's runtime policy to check that a watch's
		// iam_role is authorized (#170); doctor reads both roles and both
		// policies for drift.
		{Sid: "ReadLagottoRoles", Action: []string{"iam:GetRole", "iam:GetRolePolicy"},
			Resource: []string{runtimeiam.RoleARN(accountID), runtimeiam.SchedulerInvokeRoleARN(accountID)}},
		// doctor's caller check (#232) needs the caller's role ARN, which the STS
		// session ARN lacks the path of. IAM Identity Center roles all sit under
		// one path; reading those is how doctor learns the full ARN.
		{Sid: "ReadIdentityCenterRoles", Action: []string{"iam:GetRole"},
			Resource: iamARN("role/" + ssoRolePath + "*"), noSimulate: true},
		// doctor's caller check simulates the caller's own identity. IAM has no
		// "self" resource without a policy variable, which CreatePolicy rejects,
		// so this is every user and role in the account: it reveals what a
		// principal is allowed, and changes nothing.
		{Sid: "SimulateCaller", Action: []string{"iam:SimulatePrincipalPolicy"},
			Resource: []string{iamARN("user/*"), iamARN("role/*")}, noSimulate: true},

		// watch, launch, ack, extend and status compare the poller's version with
		// the CLI's (reminder poller check); doctor inspects it.
		{Sid: "ReadPoller", Action: []string{"lambda:GetFunction"},
			Resource: regional("lambda", "function:"+pollerFunctionName)},

		// --notify email: watch/update/ack/extend subscribe the address to the
		// alerts topic, and only by email (the CLI never subscribes another
		// protocol).
		{Sid: "SubscribeEmail", Action: []string{"sns:Subscribe"}, Resource: topicARN,
			Condition: stringEquals("sns:Protocol", "email")},
		// Reading the topic and its subscriptions, and unsubscribing an address no
		// active watch needs (cancel, update, notify-gc). SNS authorizes
		// subscription calls against the topic; the subscription ARNs
		// (topic:uuid) are listed too so the grant holds whichever is evaluated,
		// as in the poller's policy.
		{Sid: "AlertsTopic", Action: []string{
			"sns:GetSubscriptionAttributes", "sns:GetTopicAttributes", "sns:ListSubscriptionsByTopic", "sns:Unsubscribe",
		}, Resource: []string{topicARN, topicARN + ":*"}, sim: []string{topicARN},
			simSkip: []string{"sns:GetSubscriptionAttributes", "sns:Unsubscribe"}},

		// `cancel` of a fired hold releases its capacity reservation (#203). Only
		// reservations the poller created, which it always tags.
		{Sid: "ReleaseHolds", Action: []string{"ec2:CancelCapacityReservation"},
			Resource:  fmt.Sprintf("arn:aws:ec2:*:%s:capacity-reservation/*", accountID),
			Condition: stringEquals("ec2:ResourceTag/"+holdTagKey, holdTagValue),
			simSkip:   []string{"ec2:CancelCapacityReservation"}},

		// doctor's other reads.
		{Sid: "DoctorLegacyStack", Action: []string{"cloudformation:DescribeStacks"},
			Resource: regional("cloudformation", legacyStackPattern)},
		{Sid: "DoctorArtifactBuckets", Action: []string{"s3:GetBucketTagging"},
			Resource: "arn:aws:s3:::" + artifactBucketPrefix + accountID + "-*"},

		// Actions IAM authorizes only on Resource "*" (no resource-level
		// permissions in the Service Authorization Reference). All are reads.
		{Sid: "NoResourceLevelReads", Action: []string{
			// The quota and default-subnet checks of watch/update and doctor:
			// which AZs offer the type, the account's regions, its default
			// subnets, and running instances for the vCPU usage.
			"ec2:DescribeInstanceTypeOfferings", "ec2:DescribeInstances",
			"ec2:DescribeRegions", "ec2:DescribeSubnets",
			// cancel and launch read a hold's reservation state.
			"ec2:DescribeCapacityReservations",
			// The vCPU quota of --require-quota and the quota check (#157).
			"servicequotas:GetServiceQuota",
			// doctor and cancel: the per-launch schedules (#260, #279); doctor:
			// the artifact buckets (#264) and the Scheduler target-error metric
			// (#218).
			"scheduler:ListSchedules", "s3:ListAllMyBuckets", "cloudwatch:GetMetricStatistics",
		}, Resource: "*"},
	}

	// s3:// spawn configs (`watch --spawn-config s3://...`, `launch`, `update`)
	// are read from buckets the administrator names with `setup
	// --config-bucket`: lagotto imposes no bucket naming on them, and a grant on
	// every bucket would let any CLI user read any object in the account.
	if buckets := NormalizeConfigBuckets(configBuckets); len(buckets) > 0 {
		var res []string
		for _, b := range buckets {
			res = append(res, "arn:aws:s3:::"+b+"/*")
		}
		sts = append(sts, statement{Sid: "SpawnConfigObjects", Action: []string{"s3:GetObject"}, Resource: res})
	}

	for i := range sts {
		sts[i].Effect = "Allow"
		sort.Strings(sts[i].Action)
	}
	return sts
}

// PolicyDocument returns the CLI-user policy for accountID as JSON.
// configBuckets are the buckets s3:// spawn configs may be read from (setup
// --config-bucket); invalid names are dropped (validate them first with
// [ValidateConfigBucketName]).
func PolicyDocument(accountID string, configBuckets []string) (string, error) {
	if !accountIDPattern.MatchString(accountID) {
		return "", fmt.Errorf("cliiam: %q is not a 12-digit AWS account ID", accountID)
	}
	doc := map[string]interface{}{"Version": "2012-10-17", "Statement": statements(accountID, configBuckets)}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("cliiam: build policy: %w", err)
	}
	if n := len(b); n > maxManagedPolicyChars {
		return "", fmt.Errorf("cliiam: the policy is %d characters, over IAM's %d-character limit for a managed policy", n, maxManagedPolicyChars)
	}
	return string(b), nil
}

var accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// configBucketPattern is S3's bucket-name grammar (lower-case letters, digits,
// dots and hyphens, 3 to 63 characters, starting and ending alphanumeric). It
// refuses `*` and `?` in particular: a name is interpolated into a Resource,
// and a wildcard would grant s3:GetObject on every matching bucket.
var configBucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// ValidateConfigBucketName rejects anything that is not a plain S3 bucket name,
// in particular a wildcard, an s3:// URL or an ARN.
func ValidateConfigBucketName(name string) error {
	t := strings.TrimSpace(name)
	switch {
	case t == "":
		return fmt.Errorf("cliiam: config bucket name is empty")
	case strings.ContainsAny(t, "*?"):
		return fmt.Errorf("cliiam: config bucket %q contains a wildcard; name the bucket exactly "+
			"(a wildcard would let every CLI user read every matching bucket)", name)
	case strings.Contains(t, "/") || strings.Contains(t, ":"):
		return fmt.Errorf("cliiam: %q looks like a URL or ARN; pass just the bucket name", name)
	case !configBucketPattern.MatchString(t):
		return fmt.Errorf("cliiam: %q is not a valid S3 bucket name", name)
	}
	return nil
}

// NormalizeConfigBuckets trims, drops empty and invalid names, de-duplicates
// and sorts, so the document is deterministic.
func NormalizeConfigBuckets(names []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range names {
		t := strings.TrimSpace(n)
		if t == "" || seen[t] || ValidateConfigBucketName(t) != nil {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// ConfigBuckets recovers the config buckets a deployed CLI-user policy grants
// s3:GetObject on, so `setup` carries them forward instead of revoking them
// with a wholesale rewrite (the #170 footgun). doc may be URL-encoded, as IAM
// returns it.
func ConfigBuckets(doc string) ([]string, error) {
	grants, err := runtimeiam.ParsePolicy(doc)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, g := range grants {
		if g.Action != "s3:GetObject" || !strings.EqualFold(g.Effect, "Allow") {
			continue
		}
		name, ok := strings.CutPrefix(g.Resource, "arn:aws:s3:::")
		if !ok {
			continue
		}
		name, ok = strings.CutSuffix(name, "/*")
		if ok {
			out = append(out, name)
		}
	}
	return NormalizeConfigBuckets(out), nil
}

// GrantedActions returns the distinct actions [PolicyDocument] grants, sorted.
func GrantedActions(accountID string, configBuckets []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, st := range statements(accountID, configBuckets) {
		for _, a := range st.Action {
			if !seen[a] {
				seen[a] = true
				out = append(out, a)
			}
		}
	}
	sort.Strings(out)
	return out
}
