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
func EnsureRuntimeRole(ctx context.Context, client IAMAPI, region, accountID string) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
	}
	if err := ensurePollerRole(ctx, client); err != nil {
		return err
	}
	doc, err := PolicyDocument(region, accountID)
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
func PolicyDocument(region, accountID string) (string, error) {
	arn := func(f string, a ...interface{}) string { return fmt.Sprintf(f, a...) }
	schedulerARN := arn("arn:aws:scheduler:%s:%s:schedule/default/lagotto-capacity-poller", region, accountID)
	launchSchedARN := arn("arn:aws:scheduler:%s:%s:schedule/default/lagotto-launch-*", region, accountID)
	schedulerInvokeRoleARN := arn("arn:aws:iam::%s:role/lagotto-capacity-poller-scheduler-invoke", accountID)
	sporedRoleARN := arn("arn:aws:iam::%s:role/spored*", accountID)
	sporedProfileARN := arn("arn:aws:iam::%s:instance-profile/spored*", accountID)

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
		}, Resource: "*"},
		// Scheduler: manage this poller's schedule + the #62 per-launch schedules.
		{Effect: "Allow", Action: []string{"scheduler:UpdateSchedule", "scheduler:GetSchedule"},
			Resource: schedulerARN},
		{Effect: "Allow", Action: []string{"scheduler:CreateSchedule", "scheduler:DeleteSchedule"},
			Resource: launchSchedARN},
		{Effect: "Allow", Action: []string{"iam:PassRole"}, Resource: schedulerInvokeRoleARN,
			Condition: passToService("scheduler.amazonaws.com")},
		// spawn launch set: RunInstances/tags/SG, spored* role/profile setup.
		{Effect: "Allow", Action: []string{
			"ec2:RunInstances", "ec2:CreateTags", "ec2:CreateSecurityGroup", "ec2:AuthorizeSecurityGroupIngress",
		}, Resource: "*"},
		{Effect: "Allow", Action: []string{
			"iam:GetRole", "iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy",
			"iam:GetInstanceProfile", "iam:CreateInstanceProfile", "iam:AddRoleToInstanceProfile",
		}, Resource: []string{sporedRoleARN, sporedProfileARN}},
		{Effect: "Allow", Action: []string{"iam:PassRole"}, Resource: sporedRoleARN,
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

	doc := map[string]interface{}{"Version": "2012-10-17", "Statement": statements}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
