// Package runtimeiam owns lagotto's hosted capacity-poller IAM roles in Go — the
// single source of truth for the poller's execution role and the EventBridge
// Scheduler invoke role, and for "what the poller Lambda is allowed to call"
// (#16, #145). The two roles are CLI-owned and created here, exactly as the
// DynamoDB tables are (#59); the CFN/SAM stack only REFERENCES them by ARN and
// never creates them. That reconciles two flows that used to collide — a fresh
// `lagotto deploy` and a `deploy` against an account that already has the roles
// (from a prior deploy, a rollback that retained them, or setup) — since a stack
// can't create an explicitly-named IAM role that already exists (CloudFormation
// Early Validation ResourceExistenceCheck, #145). It also keeps the
// privilege-escalation surface (iam:CreateRole/PutRolePolicy) with the human
// admin running deploy/setup, never with the runtime Lambda: the poller may
// create, tag or modify no IAM role at all, and may pass to EC2 and SageMaker
// only the roles setup created or an administrator authorized. It also makes "a new
// SDK call needs a new permission" a one-file code change instead of a template
// edit.
package runtimeiam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const (
	// RoleName is the poller's execution role, CLI-owned (created here) and
	// referenced by the SAM function by ARN. Fixed so the ARN is derivable.
	RoleName = "lagotto-capacity-poller-role"
	// PolicyName is the inline permissions policy written onto RoleName.
	PolicyName = "lagotto-runtime-policy"
	// SchedulerInvokeRoleName is the role EventBridge Scheduler assumes to invoke
	// the poller (and the #49 per-launch schedules), CLI-owned and referenced by
	// the stack's Scheduler target by ARN.
	SchedulerInvokeRoleName = "lagotto-capacity-poller-scheduler-invoke"
	// SchedulerInvokePolicyName is the inline lambda:InvokeFunction policy on it.
	SchedulerInvokePolicyName = "InvokeLambda"
	// SchedulerInvokeAction is the one action that policy grants.
	SchedulerInvokeAction = "lambda:InvokeFunction"
	// pollerFunctionName is the poller Lambda's fixed name. pkg/deploy owns the
	// same constant (deploy.PollerFunctionName) but imports this package, so it
	// cannot be shared by reference; pkg/deploy's tests assert the two agree.
	pollerFunctionName = "lagotto-capacity-poller"
	// lambdaBasicExecutionARN is the AWS-managed policy giving the poller role
	// CloudWatch Logs basic execution (matching the old CFN ManagedPolicyArns).
	lambdaBasicExecutionARN = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
)

// RoleARN returns the constructed ARN of the poller execution role (RoleName).
// IAM is global, so no region is needed. Exported here — next to the name it is
// derived from — so pkg/deploy has a single source of truth for it rather than
// its own copy of the format string. The dependency is one-way (deploy imports
// runtimeiam; runtimeiam imports nothing of deploy's), so there is no cycle.
func RoleARN(accountID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, RoleName)
}

// SchedulerInvokeRoleARN returns the constructed ARN of the EventBridge
// Scheduler invoke role (SchedulerInvokeRoleName).
func SchedulerInvokeRoleARN(accountID string) string {
	return fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, SchedulerInvokeRoleName)
}

// trust returns an sts:AssumeRole trust policy JSON for a single AWS service.
func trust(service string) string {
	doc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{{
			"Effect":    "Allow",
			"Principal": map[string]string{"Service": service},
			"Action":    "sts:AssumeRole",
		}},
	}
	b, _ := json.Marshal(doc)
	return string(b)
}

// IAMAPI is the slice of the IAM API this package needs — an interface so tests
// inject a fake without real AWS. *iam.Client satisfies it.
type IAMAPI interface {
	GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	// GetRolePolicy is read-only and used only by the drift diagnostics
	// (policydiff.go, #156) — nothing in the Ensure* write paths calls it.
	GetRolePolicy(ctx context.Context, in *iam.GetRolePolicyInput, optFns ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error)
	CreateRole(ctx context.Context, in *iam.CreateRoleInput, optFns ...func(*iam.Options)) (*iam.CreateRoleOutput, error)
	AttachRolePolicy(ctx context.Context, in *iam.AttachRolePolicyInput, optFns ...func(*iam.Options)) (*iam.AttachRolePolicyOutput, error)
	PutRolePolicy(ctx context.Context, in *iam.PutRolePolicyInput, optFns ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error)
}

// roleExists reports whether the named role already exists (a NoSuchEntity is
// "absent", not an error).
func roleExists(ctx context.Context, client IAMAPI, name string) (bool, error) {
	_, err := client.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
	if err == nil {
		return true, nil
	}
	var notFound *iamtypes.NoSuchEntityException
	if errors.As(err, &notFound) {
		return false, nil
	}
	return false, err
}

// EnsureRoles creates the two CLI-owned poller roles if absent — the poller
// execution role (Lambda trust + basic-execution logging) and the Scheduler
// invoke role (Scheduler trust + lambda:InvokeFunction on the poller) — so the
// stack can reference them by ARN. Idempotent: an existing role is left as-is,
// apart from the invoke role's inline policy, which is rewritten on every call so
// a deploy repairs a policy written by an older lagotto (#218).
//
// region is validated but writes nothing region-specific: both roles are
// IAM-global and shared by every region's poller.
// It deliberately does NOT attach the full runtime permissions policy; that
// stays with EnsureRuntimeRole (setup), preserving the #16 division where the
// broad spawn/hold/sagemaker grant is applied by the human admin. Called by
// `lagotto deploy` before CreateStack, mirroring how the tables are ensured.
func EnsureRoles(ctx context.Context, client IAMAPI, region, accountID string) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
	}
	if _, err := ensurePollerRole(ctx, client); err != nil {
		return err
	}
	_, err := ensureSchedulerInvokeRole(ctx, client, accountID)
	return err
}

// EnsureSetupRoles is what `lagotto setup` applies (#229): both CLI-owned roles,
// get-or-created, the runtime permissions policy on the poller role
// ([EnsureRuntimeRole]) and the invoke policy on the Scheduler invoke role
// ([SchedulerInvokePolicyDocument]). It returns the names of the roles it
// CREATED, in that order; an existing role is reused and not listed.
//
// Before #229 setup ensured only the poller role, so in an account set up for
// the CloudFormation path no command short of `lagotto deploy` created the
// invoke role the stack's schedule and the Lambda's SCHEDULER_INVOKE_ROLE_ARN
// both name. Both policies are functions of the account alone (#218, #219), so
// setup in any region writes the same bytes and cannot flip-flop another
// region's grants. Idempotent: a re-run creates nothing and rewrites both
// policies in place.
func EnsureSetupRoles(ctx context.Context, client IAMAPI, region, accountID string, auth Authorizations) ([]string, error) {
	if region == "" || accountID == "" {
		return nil, fmt.Errorf("runtimeiam: region and accountID are required")
	}
	var created []string
	made, err := ensurePollerRole(ctx, client)
	if err != nil {
		return nil, err
	}
	if made {
		created = append(created, RoleName)
	}
	if err := putRuntimePolicy(ctx, client, accountID, auth); err != nil {
		return created, err
	}
	made, err = ensureSchedulerInvokeRole(ctx, client, accountID)
	if made {
		created = append(created, SchedulerInvokeRoleName)
	}
	return created, err
}

// ensurePollerRole get-or-creates RoleName with the Lambda trust relationship
// and the AWS-managed basic-execution policy (its permissions policy is added by
// EnsureRuntimeRole).
//
// created reports whether this call created the role.
func ensurePollerRole(ctx context.Context, client IAMAPI) (created bool, err error) {
	exists, err := roleExists(ctx, client, RoleName)
	if err != nil {
		return false, fmt.Errorf("runtimeiam: check role %s: %w", RoleName, err)
	}
	if !exists {
		if _, err := client.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String(RoleName),
			AssumeRolePolicyDocument: aws.String(trust("lambda.amazonaws.com")),
			Tags: []iamtypes.Tag{
				{Key: aws.String("Application"), Value: aws.String("lagotto")},
				{Key: aws.String("Component"), Value: aws.String("capacity-poller")},
			},
		}); err != nil {
			return false, fmt.Errorf("runtimeiam: create role %s: %w", RoleName, err)
		}
	}
	// AttachRolePolicy is idempotent (re-attaching the same managed policy is a no-op).
	if _, err := client.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
		RoleName:  aws.String(RoleName),
		PolicyArn: aws.String(lambdaBasicExecutionARN),
	}); err != nil {
		return !exists, fmt.Errorf("runtimeiam: attach basic-execution to %s: %w", RoleName, err)
	}
	return !exists, nil
}

// ensureSchedulerInvokeRole get-or-creates SchedulerInvokeRoleName with the
// Scheduler trust relationship and writes [SchedulerInvokePolicyDocument] onto it.
//
// It deliberately takes no region (#218). IAM is global, so this one role is
// shared by the poller schedule in EVERY region of the account and by every
// per-launch schedule. It used to be written with this deploy's region in the
// Resource, so the last region deployed silently broke every other region's
// schedule: each scheduled run failed with a Scheduler target error and the
// poller there was never invoked. A document that cannot depend on the region is
// the structural fix — deploying region A then region B converges, with nothing
// to flip-flop.
//
// created reports whether this call created the role.
func ensureSchedulerInvokeRole(ctx context.Context, client IAMAPI, accountID string) (created bool, err error) {
	doc, err := SchedulerInvokePolicyDocument(accountID)
	if err != nil {
		return false, err
	}
	exists, err := roleExists(ctx, client, SchedulerInvokeRoleName)
	if err != nil {
		return false, fmt.Errorf("runtimeiam: check role %s: %w", SchedulerInvokeRoleName, err)
	}
	if !exists {
		if _, err := client.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String(SchedulerInvokeRoleName),
			AssumeRolePolicyDocument: aws.String(trust("scheduler.amazonaws.com")),
		}); err != nil {
			return false, fmt.Errorf("runtimeiam: create role %s: %w", SchedulerInvokeRoleName, err)
		}
	}
	if _, err := client.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:       aws.String(SchedulerInvokeRoleName),
		PolicyName:     aws.String(SchedulerInvokePolicyName),
		PolicyDocument: aws.String(doc),
	}); err != nil {
		return !exists, fmt.Errorf("runtimeiam: put invoke policy on %s: %w", SchedulerInvokeRoleName, err)
	}
	return !exists, nil
}

// SchedulerInvokeFunctionARN is the Resource of the Scheduler invoke policy:
// lagotto's poller function in this account, in ANY region (#218).
//
// The region is a wildcard because the role is IAM-global and shared by every
// region's poller schedule; the function name and the account stay exact, so the
// grant still reaches only lagotto's own poller. Both things this role is used
// for invoke that function: the poller schedule, and the #49 per-launch
// `lagotto-launch-*` schedules (and the poller's #62 re-arm of those), which
// target the same function with a routing payload.
func SchedulerInvokeFunctionARN(accountID string) string {
	return fmt.Sprintf("arn:aws:lambda:*:%s:function:%s", accountID, pollerFunctionName)
}

// SchedulerInvokePolicyDocument returns the inline policy written onto the
// Scheduler invoke role. It is a function of the account alone — never of the
// region the writing command runs in — so every region's deploy writes the same
// bytes (#218).
func SchedulerInvokePolicyDocument(accountID string) (string, error) {
	if accountID == "" {
		return "", fmt.Errorf("runtimeiam: accountID is required for the scheduler-invoke policy")
	}
	doc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{{
			"Effect":   "Allow",
			"Action":   SchedulerInvokeAction,
			"Resource": SchedulerInvokeFunctionARN(accountID),
		}},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("runtimeiam: build scheduler-invoke policy: %w", err)
	}
	return string(b), nil
}

// EnsureRuntimeRole ensures the poller execution role exists (creating it if
// absent, so `lagotto setup` works standalone on a fresh account) and writes the
// runtime permissions policy onto it (idempotent PutRolePolicy — an
// update-in-place on re-run). accountID scopes the policy's ARNs.
//
// region is validated but does not shape what is written (#219): the role is
// IAM-global and shared by every region's poller, so the policy covers lagotto's
// resources in every region and `setup` in any one region serves all of them.
func EnsureRuntimeRole(ctx context.Context, client IAMAPI, region, accountID string, auth Authorizations) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
	}
	if _, err := ensurePollerRole(ctx, client); err != nil {
		return err
	}
	return putRuntimePolicy(ctx, client, accountID, auth)
}

// putRuntimePolicy writes PolicyDocument onto RoleName (a wholesale,
// idempotent PutRolePolicy).
func putRuntimePolicy(ctx context.Context, client IAMAPI, accountID string, auth Authorizations) error {
	doc, err := PolicyDocumentFor(accountID, auth)
	if err != nil {
		return fmt.Errorf("runtimeiam: build policy: %w", err)
	}
	_, err = client.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:       aws.String(RoleName),
		PolicyName:     aws.String(PolicyName),
		PolicyDocument: aws.String(doc),
	})
	if err != nil {
		return fmt.Errorf("runtimeiam: put role policy on %s: %w", RoleName, err)
	}
	return nil
}

// InstanceRoleName is lagotto's own instance role and its identically-named
// instance profile: what a watch launches with when its spawn_config names no
// iam_role. `lagotto setup` creates it ([EnsureInstanceRole]) with spawn's
// spored/SSM baseline and nothing else; `lagotto teardown --iam-roles` deletes
// it ([DeleteInstanceRole]). It equals watcher.DefaultInstanceRoleName.
const InstanceRoleName = watcher.DefaultInstanceRoleName

// InstanceRolePassAction is the one IAM action the poller performs on an
// instance role: passing it to EC2 at RunInstances. The poller creates, tags
// and modifies no role or instance profile. The roles it may pass are lagotto's
// own [InstanceRoleName] and the ones an administrator authorized with `setup
// --instance-role`, each by exact ARN.
//
// Before, the poller held iam:CreateRole, iam:PutRolePolicy and
// iam:AttachRolePolicy on spawn-instance* and spored* and wrote whatever policy
// a stored watch carried. Anyone who could write a watch could therefore have
// the poller build a role with any policy and launch an instance with it.
const InstanceRolePassAction = "iam:PassRole"

// Authorizations are the roles an administrator let the poller pass, carried
// forward from the deployed policy by `setup`.
type Authorizations struct {
	// InstanceRoles are EC2 instance roles (plain names), passed to
	// ec2.amazonaws.com. Each must already have an instance profile of the same
	// name; `setup --instance-role` makes sure of that.
	InstanceRoles []string
	// SageMakerRoles are SageMaker execution-role ARNs (with path), passed to
	// sagemaker.amazonaws.com.
	SageMakerRoles []string
}

// instanceRoleNamePattern is the subset of IAM's role-name grammar we accept for
// an authorized instance role.
//
// IAM itself allows `+=,.@-` and `_`; this deliberately refuses `*` and `?`. An
// authorized name is interpolated straight into a Resource ARN, so accepting a
// wildcard would turn `--instance-role '*'` into iam:PutRolePolicy and
// iam:PassRole on EVERY role in the account — a privilege-escalation path handed
// over by a typo. Refusing is the whole point of naming roles explicitly.
var instanceRoleNamePattern = regexp.MustCompile(`^[A-Za-z0-9+=,.@_-]{1,64}$`)

// ValidateInstanceRoleName rejects anything that is not a plain IAM role name —
// in particular a wildcard or a full ARN — and the reserved roles a watch may
// never launch with (watcher.ReservedInstanceRole: spawn's shared spored*
// role, its nested-spawn roles, lagotto's poller roles). Callers should surface
// the error as-is.
func ValidateInstanceRoleName(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fmt.Errorf("runtimeiam: instance role name is empty")
	}
	if strings.ContainsAny(trimmed, "*?") {
		return fmt.Errorf("runtimeiam: instance role name %q contains a wildcard; name the role exactly "+
			"(a wildcard here would grant iam:PutRolePolicy and iam:PassRole on every matching role)", name)
	}
	if strings.Contains(trimmed, "/") || strings.HasPrefix(trimmed, "arn:") {
		return fmt.Errorf("runtimeiam: %q looks like an ARN or path; pass just the role name", name)
	}
	if !instanceRoleNamePattern.MatchString(trimmed) {
		return fmt.Errorf("runtimeiam: %q is not a valid IAM role name", name)
	}
	if why := watcher.ReservedInstanceRole(trimmed); why != "" {
		return fmt.Errorf("runtimeiam: instance role %q cannot be authorized: %s", trimmed, why)
	}
	if strings.EqualFold(trimmed, InstanceRoleName) {
		return fmt.Errorf("runtimeiam: %q is lagotto's own instance role; it is always authorized", trimmed)
	}
	return nil
}

// ValidateSageMakerRoleARN rejects anything that is not a concrete IAM role
// ARN in accountID, and the reserved roles. An authorized ARN is interpolated
// into a Resource, so a wildcard would let the poller pass every matching role
// to SageMaker.
func ValidateSageMakerRoleARN(roleARN, accountID string) error {
	acct, name, err := watcher.ParseRoleARN(roleARN)
	if err != nil {
		return fmt.Errorf("runtimeiam: --sagemaker-role: %w", err)
	}
	if accountID != "" && acct != accountID {
		return fmt.Errorf("runtimeiam: --sagemaker-role %q is in account %s, not this account (%s)", roleARN, acct, accountID)
	}
	if why := watcher.ReservedInstanceRole(name); why != "" {
		return fmt.Errorf("runtimeiam: --sagemaker-role %q cannot be authorized: %s", roleARN, why)
	}
	return nil
}

// normalizeSageMakerRoles trims, drops invalid ARNs, de-duplicates and sorts.
func normalizeSageMakerRoles(arns []string, accountID string) []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range arns {
		t := strings.TrimSpace(a)
		if t == "" || seen[t] || ValidateSageMakerRoleARN(t, accountID) != nil {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// normalizeInstanceRoles trims, drops empties and invalid names, de-duplicates
// and sorts, so the emitted policy is deterministic (a policy that reorders
// between runs would show as drift in `lagotto doctor`).
//
// Invalid names are dropped rather than erroring because this runs during policy
// construction; callers validate with [ValidateInstanceRoleName] at the point the
// user supplies the name, where a clear error can be shown.
func normalizeInstanceRoles(names []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(names))
	for _, n := range names {
		t := strings.TrimSpace(n)
		if t == "" || seen[t] || ValidateInstanceRoleName(t) != nil {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// anyRegion is the region segment of every regional ARN in the runtime policy.
const anyRegion = "*"

// RegionalARN builds an ARN for one of lagotto's fixed-name regional resources
// in accountID, in ANY region (#219): arn:aws:<service>:*:<account>:<resource>.
// The resource name and account stay exact, so the grant still reaches only
// lagotto's own tables, topic and schedules.
func RegionalARN(service, accountID, resource string) string {
	return fmt.Sprintf("arn:aws:%s:%s:%s:%s", service, anyRegion, accountID, resource)
}

// ManagedTagKey is the tag spawn stamps on every instance it launches (spawn
// pkg/aws buildTags, "spawn:managed"="true"), on the persistent Spot requests it
// tags with the instance's tags, and on every FSx filesystem it creates
// (buildFSxCreateInput). It is the only tag on ALL of the instances lagotto
// launches: lagotto:watch-id is stamped by a watch's Spawn, not by a scheduled
// launch (LaunchScheduled), so it cannot gate `--if-exists replace`. spawn's own
// TTL reaper gates its TerminateInstances on the same tag (spawn
// pkg/reaperiam.ManagedTagCondition).
const ManagedTagKey = "spawn:managed"

// FSxBucketPrefix is the only bucket-name prefix the poller's S3 grants cover:
// the fsx_create backing bucket (#242). It must equal
// watcher.FSxBucketPrefix, which refuses any other name before launch. The two
// packages cannot share it by reference without an import cycle in the tests;
// TestS3GrantsAreScopedToTheFSxPrefix asserts they agree.
const FSxBucketPrefix = "lagotto-fsx-"

// managedTagValue is ManagedTagKey's value.
const managedTagValue = "true"

// statement is a single IAM policy statement (minimal shape we emit).
type statement struct {
	Effect    string      `json:"Effect"`
	Action    []string    `json:"Action"`
	Resource  interface{} `json:"Resource"`
	Condition interface{} `json:"Condition,omitempty"`
}

// PolicyDocument returns the runtime policy as a JSON string, scoped to
// accountID. It is the Go source of truth mirroring the set previously in
// deployment/cloudformation/lagotto-stack.yaml (the poller's Policies: block):
// DynamoDB CRUD (3 tables), SNS publish, EC2/SSM read discovery, scheduler
// manage + PassRole, the spawn launch set (RunInstances/tags/SG + spored* role
// setup + PassRole to ec2), terminating what spawn launched (spawn:managed
// tag-gated, #242), the fsx_create FSx filesystem and its lagotto-fsx-* S3
// backing bucket (#242), capacity reservations, and SageMaker submit + PassRole.
//
// TestPollerPolicyCoversItsCalls holds this policy to the poller Lambda's own
// AWS calls, discovered from the code: a call the policy does not grant fails
// CI unless it is listed, with a reason, as deliberately not granted.
//
// # No region (#219)
//
// The execution role is IAM-global and shared by the poller in every region of
// the account, but `setup` used to write this policy with the region it ran in
// baked into every DynamoDB, SNS and Scheduler ARN. `setup` in region B then
// revoked region A's poller's access to its own tables, topic and schedules, and
// doctor in A prescribed `setup`, which broke B — the regions took turns. The
// regional ARNs therefore carry a `*` region (see [RegionalARN]) while keeping
// lagotto's fixed resource names and the account exact, so every region's
// `setup` writes the same document and one run serves them all.
func PolicyDocument(accountID string, instanceRoles []string) (string, error) {
	return PolicyDocumentFor(accountID, Authorizations{InstanceRoles: instanceRoles})
}

// PolicyDocumentFor is [PolicyDocument] with SageMaker execution roles too.
func PolicyDocumentFor(accountID string, auth Authorizations) (string, error) {
	arn := func(f string, a ...interface{}) string { return fmt.Sprintf(f, a...) }
	schedulerARN := RegionalARN("scheduler", accountID, "schedule/default/lagotto-capacity-poller")
	launchSchedARN := RegionalARN("scheduler", accountID, "schedule/default/lagotto-launch-*")
	schedulerInvokeRoleARN := arn("arn:aws:iam::%s:role/lagotto-capacity-poller-scheduler-invoke", accountID)
	// The instance roles the poller may pass to EC2: lagotto's own, then each
	// one an administrator authorized, by exact ARN. No prefix: spawn-instance*
	// and spored* roles are created by spawn's own users, with policies they
	// chose, so a prefix grant would let every lagotto user launch with them.
	ec2PassRoles := []string{arn("arn:aws:iam::%s:role/%s", accountID, InstanceRoleName)}
	for _, name := range normalizeInstanceRoles(auth.InstanceRoles) {
		ec2PassRoles = append(ec2PassRoles, arn("arn:aws:iam::%s:role/%s", accountID, name))
	}

	passToService := func(svc string) interface{} {
		return map[string]interface{}{"StringEquals": map[string]string{"iam:PassedToService": svc}}
	}
	// A condition key belongs to the ACTION's service: ec2:ResourceTag on an ec2:
	// action; FSx has no fsx:ResourceTag, so its actions use the global aws:
	// keys. An ec2: key on an fsx: or s3: action is a silent implicit deny that
	// still passes PutRolePolicy (the spawn#665 shape).
	// TestPolicyConditionKeysMatchActionService enforces it.
	ec2Managed := map[string]interface{}{"StringEquals": map[string]string{"ec2:ResourceTag/" + ManagedTagKey: managedTagValue}}
	fsxManagedRequest := map[string]interface{}{"StringEquals": map[string]string{"aws:RequestTag/" + ManagedTagKey: managedTagValue}}
	fsxManagedResource := map[string]interface{}{"StringEquals": map[string]string{"aws:ResourceTag/" + ManagedTagKey: managedTagValue}}
	viaFSx := map[string]interface{}{"ForAnyValue:StringEquals": map[string][]string{"aws:CalledVia": {"fsx.amazonaws.com"}}}
	fsxFileSystemARN := RegionalARN("fsx", accountID, "file-system/*")

	statements := []statement{
		// Tables are CLI-owned (#59) — the poller only reads/writes them.
		{Effect: "Allow", Action: []string{
			"dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
			"dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem",
			"dynamodb:DescribeTable", "dynamodb:ConditionCheckItem",
		}, Resource: []string{
			RegionalARN("dynamodb", accountID, "table/lagotto-watches"),
			RegionalARN("dynamodb", accountID, "table/lagotto-watches/index/*"),
			RegionalARN("dynamodb", accountID, "table/lagotto-match-history"),
			RegionalARN("dynamodb", accountID, "table/lagotto-match-history/index/*"),
			RegionalARN("dynamodb", accountID, "table/lagotto-scheduled-launches"),
			RegionalARN("dynamodb", accountID, "table/lagotto-scheduled-launches/index/*"),
		}},
		// SNS capacity alerts.
		{Effect: "Allow", Action: []string{"sns:Publish"},
			Resource: RegionalARN("sns", accountID, "lagotto-capacity-alerts")},
		// Email subscription cleanup (#240): each sweep unsubscribes the
		// per-address subscriptions no active watch names. SNS authorizes
		// subscription calls against the topic; the topic's subscription ARNs
		// (topic:uuid) are listed too so the grant holds whichever ARN is
		// evaluated. Never sns:Subscribe: only the CLI subscribes an address.
		{Effect: "Allow", Action: []string{
			"sns:ListSubscriptionsByTopic", "sns:GetSubscriptionAttributes", "sns:Unsubscribe",
		}, Resource: []string{
			RegionalARN("sns", accountID, "lagotto-capacity-alerts"),
			RegionalARN("sns", accountID, "lagotto-capacity-alerts:*"),
		}},
		// Capacity discovery (truffle) + spawn launcher read APIs. Region-agnostic
		// describe reads — Resource "*" as in the template.
		{Effect: "Allow", Action: []string{
			"ec2:DescribeInstanceTypes", "ec2:DescribeInstanceTypeOfferings", "ec2:DescribeSpotPriceHistory",
			"ec2:DescribeRegions", "ec2:DescribeImages", "ec2:DescribeVpcs", "ec2:DescribeSubnets",
			"ec2:DescribeSecurityGroups", "ec2:DescribeKeyPairs", "ec2:DescribeInstances",
			"ec2:DescribeCapacityReservations", "ssm:GetParameter", "ssm:GetParameters",
			// spawn's Client.Terminate looks up the instance's Elastic IPs after
			// terminating it (releaseInstanceElasticIPs, spawn#661). A denial only
			// warns, but it is on every terminate below (#242/#243). No
			// resource-level permissions, so Resource "*".
			"ec2:DescribeAddresses",
			// pricing:GetProducts backs spawn's --cost-limit enforcement: the launcher
			// looks up the on-demand price before RunInstances, and for newer types
			// (g7/g7e/p5) there's no static-fallback price, so without this the launch
			// fails with AccessDenied — which classifies terminal, killing the watch
			// before any capacity check (lagotto#148). Pricing is a global read-only API.
			"pricing:GetProducts", "pricing:GetAttributeValues",
			// servicequotas reads let the poller put the account's ACTUAL vCPU
			// limit/usage numbers into a quota-cap report (lagotto#153) — e.g. "fleet
			// capped at 2/5; G on-demand quota is 8 vCPU with 8 in use". Read-only,
			// region-agnostic, no write/request-increase action.
			//
			// Explicitly NOT required for the fix: the cap is detected from the
			// RunInstances error itself (pure error inspection), and the lookup seam
			// is nil-safe and fails open. That's deliberate — #148 and #150 were both
			// "a fix that silently no-ops if you forget to re-run `lagotto setup`",
			// and this must not become a third. Re-running setup upgrades the report
			// from "capped at 2/5" to "capped at 2/5, quota is 8 vCPU"; it never
			// decides whether the cap is reported at all. (ec2:DescribeInstances,
			// which GetQuotas also needs for current usage, is already granted above.)
			"servicequotas:GetServiceQuota", "servicequotas:ListServiceQuotas",
		}, Resource: "*"},
		// Scheduler: manage this poller's schedule + the #62 per-launch schedules.
		{Effect: "Allow", Action: []string{"scheduler:UpdateSchedule", "scheduler:GetSchedule"},
			Resource: schedulerARN},
		{Effect: "Allow", Action: []string{"scheduler:CreateSchedule", "scheduler:DeleteSchedule"},
			Resource: launchSchedARN},
		{Effect: "Allow", Action: []string{"iam:PassRole"}, Resource: schedulerInvokeRoleARN,
			Condition: passToService("scheduler.amazonaws.com")},
		// spawn launch set: RunInstances/tags/SG. No IAM writes: the launch names
		// an instance profile an administrator provided, so spawn's launcher does
		// no IAM setup (watcher.SpawnConfigFile.InstanceProfileName).
		{Effect: "Allow", Action: []string{
			"ec2:RunInstances", "ec2:CreateTags", "ec2:CreateSecurityGroup", "ec2:AuthorizeSecurityGroupIngress",
		}, Resource: "*"},
		{Effect: "Allow", Action: []string{InstanceRolePassAction}, Resource: ec2PassRoles,
			Condition: passToService("ec2.amazonaws.com")},
		// Terminating what spawn launched (#242): Provision's compensating
		// teardown after a failed fsx_create setup (spawn pkg/launcher
		// provision.go, "TERMINATE IT MANUALLY" otherwise), `lagotto launch
		// --if-exists replace` (Spawner.LaunchScheduled), and a watch's launch
		// still pending after PendingLandingTimeout (#239, Poller.cancelPending).
		// All go through
		// spotrequest.Terminate, which cancels the instance's Spot request first.
		// Gated on spawn:managed=true, so the poller cannot end an instance spawn
		// did not launch; an untagged one-time Spot request is refused, which is
		// harmless (it closes with its instance; spawn only warns).
		{Effect: "Allow", Action: []string{"ec2:TerminateInstances"},
			Resource: arn("arn:aws:ec2:*:%s:instance/*", accountID), Condition: ec2Managed},
		{Effect: "Allow", Action: []string{"ec2:CancelSpotInstanceRequests"},
			Resource: arn("arn:aws:ec2:*:%s:spot-instances-request/*", accountID), Condition: ec2Managed},
		// fsx_create (#43, #242): spawn's startFSxCreate creates the filesystem
		// WITH tags (spawn:managed=true among them), which FSx authorizes as
		// fsx:TagResource too. Both are gated on that request tag, so the poller
		// can only create filesystems that carry spawn's reaper tag.
		{Effect: "Allow", Action: []string{"fsx:CreateFileSystem", "fsx:TagResource"},
			Resource: fsxFileSystemARN, Condition: fsxManagedRequest},
		// The #210 backstop: Provision deletes the just-created filesystem when it
		// cannot tag the instance with it. Only spawn-managed filesystems.
		{Effect: "Allow", Action: []string{"fsx:DeleteFileSystem"},
			Resource: fsxFileSystemARN, Condition: fsxManagedResource},
		// FSx creates its service-linked role on the first filesystem in an
		// account, as the caller (AmazonFSxFullAccess "CreateSLRForFSx").
		{Effect: "Allow", Action: []string{"iam:CreateServiceLinkedRole"},
			Resource:  arn("arn:aws:iam::%s:role/aws-service-role/fsx.amazonaws.com/*", accountID),
			Condition: map[string]interface{}{"StringEquals": map[string]string{"iam:AWSServiceName": "fsx.amazonaws.com"}}},
		// The VPC reads FSx makes with the caller's credentials while creating a
		// filesystem (AmazonFSxFullAccess "DescribeEC2VpcResources"; the
		// subnet/SG/VPC describes are already granted above). Only through FSx:
		// the poller's own calls to these are not authorized, which the poller
		// coverage gate accounts for.
		{Effect: "Allow", Action: []string{"ec2:DescribeRouteTables", "ec2:GetSecurityGroupsForVpc"},
			Resource: "*", Condition: viaFSx},
		// The fsx_create S3 backing bucket (spawn createBucketIfNotExists, with
		// AutoCreateBucket): HeadBucket (authorized as s3:ListBucket), CreateBucket,
		// then PutBucketTagging; bucketsweep.ClearEmptySince adds the
		// Get/PutBucketTagging pair for a swept name. Only on buckets named
		// FSxBucketPrefix*: spawn imposes no naming rule on fsx_s3_bucket, so
		// lagotto does (#242). `lagotto watch`/`launch` refuse an fsx_create
		// config outside the prefix, and the poller refuses a stored one before
		// launching (watcher.SpawnConfigFile.ValidateFSxBucket).
		{Effect: "Allow", Action: []string{
			"s3:CreateBucket", "s3:ListBucket", "s3:GetBucketTagging", "s3:PutBucketTagging",
		}, Resource: "arn:aws:s3:::" + FSxBucketPrefix + "*"},
		// hold: capacity reservations.
		{Effect: "Allow", Action: []string{
			"ec2:CreateCapacityReservation", "ec2:CancelCapacityReservation", "ec2:DescribeCapacityReservations",
		}, Resource: "*"},
		// SageMaker submit and track, only the jobs lagotto names
		// (lagotto-<watch>-<n>, SageMakerLauncher.submit). SageMaker ARNs carry
		// the job name in lower case; lagotto's names are lower case already.
		{Effect: "Allow", Action: []string{
			"sagemaker:CreateTrainingJob", "sagemaker:AddTags", "sagemaker:DescribeTrainingJob",
		}, Resource: RegionalARN("sagemaker", accountID, "training-job/lagotto-*")},
	}
	// SageMaker execution roles: only those an administrator authorized with
	// `setup --sagemaker-role`, by exact ARN. With none, no SageMaker PassRole
	// at all, so a SageMaker watch fails at submit until one is authorized.
	if smRoles := normalizeSageMakerRoles(auth.SageMakerRoles, accountID); len(smRoles) > 0 {
		statements = append(statements, statement{Effect: "Allow", Action: []string{InstanceRolePassAction},
			Resource: smRoles, Condition: passToService("sagemaker.amazonaws.com")})
	}

	doc := map[string]interface{}{"Version": "2012-10-17", "Statement": statements}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
