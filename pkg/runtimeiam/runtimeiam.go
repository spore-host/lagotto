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
// admin running deploy/setup, never with the runtime Lambda, and makes "a new
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
	// schedulerInvokePolicyName is the inline lambda:InvokeFunction policy on it.
	schedulerInvokePolicyName = "InvokeLambda"
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
// stack can reference them by ARN. Idempotent: an existing role is left as-is.
// It deliberately does NOT attach the full runtime permissions policy; that
// stays with EnsureRuntimeRole (setup), preserving the #16 division where the
// broad spawn/hold/sagemaker grant is applied by the human admin. Called by
// `lagotto deploy` before CreateStack, mirroring how the tables are ensured.
func EnsureRoles(ctx context.Context, client IAMAPI, region, accountID string) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
	}
	if err := ensurePollerRole(ctx, client); err != nil {
		return err
	}
	return ensureSchedulerInvokeRole(ctx, client, region, accountID)
}

// ensurePollerRole get-or-creates RoleName with the Lambda trust relationship
// and the AWS-managed basic-execution policy (its permissions policy is added by
// EnsureRuntimeRole).
func ensurePollerRole(ctx context.Context, client IAMAPI) error {
	exists, err := roleExists(ctx, client, RoleName)
	if err != nil {
		return fmt.Errorf("runtimeiam: check role %s: %w", RoleName, err)
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
			return fmt.Errorf("runtimeiam: create role %s: %w", RoleName, err)
		}
	}
	// AttachRolePolicy is idempotent (re-attaching the same managed policy is a no-op).
	if _, err := client.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
		RoleName:  aws.String(RoleName),
		PolicyArn: aws.String(lambdaBasicExecutionARN),
	}); err != nil {
		return fmt.Errorf("runtimeiam: attach basic-execution to %s: %w", RoleName, err)
	}
	return nil
}

// ensureSchedulerInvokeRole get-or-creates SchedulerInvokeRoleName with the
// Scheduler trust relationship and an inline policy allowing it to invoke the
// poller (referenced by constructed ARN, matching the old template).
func ensureSchedulerInvokeRole(ctx context.Context, client IAMAPI, region, accountID string) error {
	exists, err := roleExists(ctx, client, SchedulerInvokeRoleName)
	if err != nil {
		return fmt.Errorf("runtimeiam: check role %s: %w", SchedulerInvokeRoleName, err)
	}
	if !exists {
		if _, err := client.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String(SchedulerInvokeRoleName),
			AssumeRolePolicyDocument: aws.String(trust("scheduler.amazonaws.com")),
		}); err != nil {
			return fmt.Errorf("runtimeiam: create role %s: %w", SchedulerInvokeRoleName, err)
		}
	}
	invokeDoc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{{
			"Effect":   "Allow",
			"Action":   "lambda:InvokeFunction",
			"Resource": fmt.Sprintf("arn:aws:lambda:%s:%s:function:lagotto-capacity-poller", region, accountID),
		}},
	}
	b, err := json.Marshal(invokeDoc)
	if err != nil {
		return fmt.Errorf("runtimeiam: build scheduler-invoke policy: %w", err)
	}
	if _, err := client.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:       aws.String(SchedulerInvokeRoleName),
		PolicyName:     aws.String(schedulerInvokePolicyName),
		PolicyDocument: aws.String(string(b)),
	}); err != nil {
		return fmt.Errorf("runtimeiam: put invoke policy on %s: %w", SchedulerInvokeRoleName, err)
	}
	return nil
}

// EnsureRuntimeRole ensures the poller execution role exists (creating it if
// absent, so `lagotto setup` works standalone on a fresh account) and writes the
// runtime permissions policy onto it (idempotent PutRolePolicy — an
// update-in-place on re-run). region and accountID scope the policy's ARNs
// (scheduler, PassRole, spored* resources).
func EnsureRuntimeRole(ctx context.Context, client IAMAPI, region, accountID string, instanceRoles []string) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
	}
	if err := ensurePollerRole(ctx, client); err != nil {
		return err
	}
	doc, err := PolicyDocument(region, accountID, instanceRoles)
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

// InstanceRoleActions are the IAM actions spawn's launcher performs on an
// instance role and its identically-named instance profile, in
// CreateOrGetInstanceProfile (spawn pkg/aws/iam.go).
//
// It is exported and used both to BUILD the policy and to CHECK a named role at
// watch-creation time, so the grant and the check cannot drift apart. The order
// matters only for readable output.
//
// Why the whole set rather than just iam:GetRole, which is where #170's
// AccessDenied surfaced: for a pre-existing role the launcher goes on to write
// the spored baseline inline policy (PutRolePolicy, unconditional since spawn#502)
// and attach the SSM managed policy (AttachRolePolicy), then reads/creates the
// instance profile, then passes the role to EC2. Granting these one at a time just
// moves the failure to the next call.
var InstanceRoleActions = []string{
	"iam:GetRole", "iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy",
	"iam:GetInstanceProfile", "iam:CreateInstanceProfile", "iam:AddRoleToInstanceProfile",
}

// InstanceRolePassAction is granted separately from [InstanceRoleActions] because
// it carries a PassedToService condition, which is part of a grant's identity.
const InstanceRolePassAction = "iam:PassRole"

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
// in particular a wildcard or a full ARN. Callers should surface the error as-is.
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
	return nil
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

// statement is a single IAM policy statement (minimal shape we emit).
type statement struct {
	Effect    string      `json:"Effect"`
	Action    []string    `json:"Action"`
	Resource  interface{} `json:"Resource"`
	Condition interface{} `json:"Condition,omitempty"`
}

// PolicyDocument returns the runtime policy as a JSON string, scoped to region
// and accountID. It is the Go source of truth mirroring the set previously in
// deployment/cloudformation/lagotto-stack.yaml (the poller's Policies: block):
// DynamoDB CRUD (3 tables), SNS publish, EC2/SSM read discovery, scheduler
// manage + PassRole, the spawn launch set (RunInstances/tags/SG + spored* role
// setup + PassRole to ec2), capacity reservations, and SageMaker submit + PassRole.
func PolicyDocument(region, accountID string, instanceRoles []string) (string, error) {
	arn := func(f string, a ...interface{}) string { return fmt.Sprintf(f, a...) }
	schedulerARN := arn("arn:aws:scheduler:%s:%s:schedule/default/lagotto-capacity-poller", region, accountID)
	launchSchedARN := arn("arn:aws:scheduler:%s:%s:schedule/default/lagotto-launch-*", region, accountID)
	schedulerInvokeRoleARN := arn("arn:aws:iam::%s:role/lagotto-capacity-poller-scheduler-invoke", accountID)
	sporedRoleARN := arn("arn:aws:iam::%s:role/spored*", accountID)
	sporedProfileARN := arn("arn:aws:iam::%s:instance-profile/spored*", accountID)
	// spawn's launcher names the per-launch instance role/profile it creates
	// "spawn-instance-<hash>" (spawn pkg/aws iam.go generateRoleName), not
	// "spored*". The hosted poller MUST be able to GetRole/CreateRole/…/PassRole
	// on those, or every auto-spawn dies at "set up IAM instance profile" with
	// AccessDenied — which classifies terminal, so the watch fails ~immediately
	// instead of waiting out capacity (lagotto#148). Keep spored* too (spawn's
	// fixed fallback name + older launches).
	spawnRoleARN := arn("arn:aws:iam::%s:role/spawn-instance*", accountID)
	spawnProfileARN := arn("arn:aws:iam::%s:instance-profile/spawn-instance*", accountID)

	passToService := func(svc string) interface{} {
		return map[string]interface{}{"StringEquals": map[string]string{"iam:PassedToService": svc}}
	}

	statements := []statement{
		// Tables are CLI-owned (#59) — the poller only reads/writes them.
		{Effect: "Allow", Action: []string{
			"dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem",
			"dynamodb:Query", "dynamodb:Scan", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem",
			"dynamodb:DescribeTable", "dynamodb:ConditionCheckItem",
		}, Resource: []string{
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-watches", region, accountID),
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-watches/index/*", region, accountID),
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-match-history", region, accountID),
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-match-history/index/*", region, accountID),
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-scheduled-launches", region, accountID),
			arn("arn:aws:dynamodb:%s:%s:table/lagotto-scheduled-launches/index/*", region, accountID),
		}},
		// SNS capacity alerts.
		{Effect: "Allow", Action: []string{"sns:Publish"},
			Resource: arn("arn:aws:sns:%s:%s:lagotto-capacity-alerts", region, accountID)},
		// Capacity discovery (truffle) + spawn launcher read APIs. Region-agnostic
		// describe reads — Resource "*" as in the template.
		{Effect: "Allow", Action: []string{
			"ec2:DescribeInstanceTypes", "ec2:DescribeInstanceTypeOfferings", "ec2:DescribeSpotPriceHistory",
			"ec2:DescribeRegions", "ec2:DescribeImages", "ec2:DescribeVpcs", "ec2:DescribeSubnets",
			"ec2:DescribeSecurityGroups", "ec2:DescribeKeyPairs", "ec2:DescribeInstances",
			"ec2:DescribeCapacityReservations", "ssm:GetParameter", "ssm:GetParameters",
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
		// spawn launch set: RunInstances/tags/SG, spawn-instance*/spored* role/profile setup.
		{Effect: "Allow", Action: []string{
			"ec2:RunInstances", "ec2:CreateTags", "ec2:CreateSecurityGroup", "ec2:AuthorizeSecurityGroupIngress",
		}, Resource: "*"},
		{Effect: "Allow", Action: InstanceRoleActions,
			Resource: []string{spawnRoleARN, spawnProfileARN, sporedRoleARN, sporedProfileARN}},
		{Effect: "Allow", Action: []string{InstanceRolePassAction}, Resource: []string{spawnRoleARN, sporedRoleARN},
			Condition: passToService("ec2.amazonaws.com")},
		// hold: capacity reservations.
		{Effect: "Allow", Action: []string{
			"ec2:CreateCapacityReservation", "ec2:CancelCapacityReservation", "ec2:DescribeCapacityReservations",
		}, Resource: "*"},
		// SageMaker submit + PassRole (the job spec carries its own execution role).
		{Effect: "Allow", Action: []string{
			"sagemaker:CreateTrainingJob", "sagemaker:AddTags", "sagemaker:DescribeTrainingJob",
		}, Resource: "*"},
		{Effect: "Allow", Action: []string{"iam:PassRole"}, Resource: "*",
			Condition: passToService("sagemaker.amazonaws.com")},
	}

	// Operator-authorized instance roles (#170). A watch's spawn_config may name
	// any iam_role, and spawn's launcher uses that name verbatim — so the fixed
	// spawn-instance*/spored* prefixes above cannot cover it, and the watch dies at
	// launch with AccessDenied on iam:GetRole *after* matching capacity.
	//
	// These get exactly the grant set above, on the named role and the
	// identically-named instance profile (CreateOrGetInstanceProfile uses
	// profileName := roleName). Granting the whole set at once is deliberate:
	// granting only iam:GetRole moves the failure to PutRolePolicy, then
	// AttachRolePolicy, then PassRole — the #149/#151/#153 sequence of
	// rediscoveries, each costing a user a matched watch.
	for _, name := range normalizeInstanceRoles(instanceRoles) {
		roleARN := arn("arn:aws:iam::%s:role/%s", accountID, name)
		profileARN := arn("arn:aws:iam::%s:instance-profile/%s", accountID, name)
		statements = append(statements,
			statement{Effect: "Allow", Action: InstanceRoleActions,
				Resource: []string{roleARN, profileARN}},
			statement{Effect: "Allow", Action: []string{InstanceRolePassAction},
				Resource: []string{roleARN}, Condition: passToService("ec2.amazonaws.com")},
		)
	}

	doc := map[string]interface{}{"Version": "2012-10-17", "Statement": statements}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
