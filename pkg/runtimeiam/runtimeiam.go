// Package runtimeiam owns lagotto's hosted capacity-poller runtime IAM policy in
// Go — the single source of truth for "what the poller Lambda is allowed to
// call" (#16). The CFN/SAM stack creates a MINIMAL execution role (Lambda trust
// + AWSLambdaBasicExecutionRole); `lagotto setup` attaches this permissions
// policy to it, exactly as it already provisions the DynamoDB tables. This keeps
// the privilege-escalation surface (iam:CreateRole/PutRolePolicy) with the human
// admin running setup, never with the runtime Lambda, and makes "a new SDK call
// needs a new permission" a one-file code change instead of a template edit.
package runtimeiam

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
)

const (
	// RoleName is the poller's execution role, created by CFN (minimal) and
	// policy-managed here. Fixed so the SAM function can reference it by ARN.
	RoleName = "lagotto-capacity-poller-role"
	// PolicyName is the inline permissions policy setup writes onto RoleName.
	PolicyName = "lagotto-runtime-policy"
)

// IAMAPI is the slice of the IAM API EnsureRuntimeRole needs — an interface so
// tests inject a fake without real AWS. *iam.Client satisfies it.
type IAMAPI interface {
	PutRolePolicy(ctx context.Context, in *iam.PutRolePolicyInput, optFns ...func(*iam.Options)) (*iam.PutRolePolicyOutput, error)
}

// EnsureRuntimeRole writes the runtime permissions policy onto the poller's
// execution role (idempotent PutRolePolicy — an update-in-place on re-run). The
// role itself is created by CFN; this only manages its inline policy. region and
// accountID scope the policy's ARNs (scheduler, PassRole, spored* resources),
// mirroring the CFN template's ${AWS::Region}/${AWS::AccountId}.
func EnsureRuntimeRole(ctx context.Context, client IAMAPI, region, accountID string) error {
	if region == "" || accountID == "" {
		return fmt.Errorf("runtimeiam: region and accountID are required")
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
