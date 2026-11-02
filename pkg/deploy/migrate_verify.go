package deploy

// Live-state fingerprints of the resources a legacy stack owns (#226), so the
// migration can prove that executing the retain change set and deleting the
// stack left every one of them in place and unchanged.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// iamRoleAPI is the read-only slice of IAM the migration verifies roles with.
//
// It is a PARAMETER of MigrateFromStack, deliberately not a Deployer field:
// TestTeardownCannotTouchIAM pins that the Deployer (and so teardown, which is
// per-region) has no way to reach the IAM-global roles every region shares
// (#218). The migration only reads them.
type iamRoleAPI interface {
	GetRole(ctx context.Context, in *iam.GetRoleInput, optFns ...func(*iam.Options)) (*iam.GetRoleOutput, error)
	ListRolePolicies(ctx context.Context, in *iam.ListRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error)
	GetRolePolicy(ctx context.Context, in *iam.GetRolePolicyInput, optFns ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error)
	ListAttachedRolePolicies(ctx context.Context, in *iam.ListAttachedRolePoliciesInput, optFns ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error)
}

// dynamoTableAPI is the read-only slice of DynamoDB the migration verifies
// tables with.
type dynamoTableAPI interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// Resource types the migration can verify. Every lagotto stack template ever
// shipped declares only these (AWS::Serverless::Function expands to an
// AWS::Lambda::Function, plus an AWS::IAM::Role when it declares Policies). A
// stack owning anything else is refused up front: a resource whose survival
// cannot be checked is not one to delete a stack around.
var verifiableTypes = map[string]bool{
	"AWS::Lambda::Function":    true,
	"AWS::Scheduler::Schedule": true,
	"AWS::SNS::Topic":          true,
	"AWS::IAM::Role":           true,
	"AWS::DynamoDB::Table":     true,
}

// fingerprint reads one owned resource's live state into a canonical string.
// Only identity and configuration go in (never counters or timestamps that move
// on their own), so two reads of an untouched resource are equal.
func (d *Deployer) fingerprint(ctx context.Context, roles iamRoleAPI, r OwnedResource) (string, error) {
	var v any
	switch r.Type {
	case "AWS::Lambda::Function":
		out, err := d.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(r.PhysicalID)})
		if err != nil {
			return "", err
		}
		v = lambdaFingerprint(out)
	case "AWS::Scheduler::Schedule":
		out, err := d.sched.GetSchedule(ctx, &scheduler.GetScheduleInput{Name: aws.String(r.PhysicalID)})
		if err != nil {
			return "", err
		}
		v = map[string]any{
			"Name": out.Name, "GroupName": out.GroupName, "State": out.State,
			"ScheduleExpression": out.ScheduleExpression, "Timezone": out.ScheduleExpressionTimezone,
			"Description": out.Description, "FlexibleTimeWindow": out.FlexibleTimeWindow,
			"Target": out.Target, "KmsKeyArn": out.KmsKeyArn, "StartDate": out.StartDate,
			"EndDate": out.EndDate, "ActionAfterCompletion": out.ActionAfterCompletion,
		}
	case "AWS::SNS::Topic":
		out, err := d.sns.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(r.PhysicalID)})
		if err != nil {
			return "", err
		}
		attrs := map[string]string{}
		for k, val := range out.Attributes {
			switch k {
			case "SubscriptionsConfirmed", "SubscriptionsPending", "SubscriptionsDeleted":
				// counters: they move with subscriptions, not with the stack
			default:
				attrs[k] = val
			}
		}
		v = attrs
	case "AWS::IAM::Role":
		fp, err := roleFingerprint(ctx, roles, r.PhysicalID)
		if err != nil {
			return "", err
		}
		v = fp
	case "AWS::DynamoDB::Table":
		out, err := d.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(r.PhysicalID)})
		if err != nil {
			return "", err
		}
		if out.Table == nil {
			return "", fmt.Errorf("DescribeTable returned no table")
		}
		t := out.Table
		v = map[string]any{
			"TableArn": t.TableArn, "TableId": t.TableId, "CreationDateTime": t.CreationDateTime,
			"KeySchema": t.KeySchema, "BillingModeSummary": t.BillingModeSummary,
		}
	default:
		return "", fmt.Errorf("resource type %s cannot be verified", r.Type)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func lambdaFingerprint(out *lambda.GetFunctionOutput) any {
	fp := map[string]any{"Tags": out.Tags}
	if c := out.Configuration; c != nil {
		var env map[string]string
		if c.Environment != nil {
			env = c.Environment.Variables
		}
		var layers []string
		for _, l := range c.Layers {
			layers = append(layers, aws.ToString(l.Arn))
		}
		fp["FunctionArn"] = c.FunctionArn
		fp["Role"] = c.Role
		fp["Runtime"] = c.Runtime
		fp["Handler"] = c.Handler
		fp["Timeout"] = c.Timeout
		fp["MemorySize"] = c.MemorySize
		fp["Architectures"] = c.Architectures
		fp["Environment"] = env
		fp["CodeSha256"] = c.CodeSha256
		fp["Layers"] = layers
		fp["KMSKeyArn"] = c.KMSKeyArn
		fp["Description"] = c.Description
	}
	return fp
}

// roleFingerprint covers what makes a role work: its identity (RoleId changes
// if it is deleted and recreated under the same name), trust policy, inline
// policies and attached managed policies.
func roleFingerprint(ctx context.Context, roles iamRoleAPI, name string) (map[string]any, error) {
	if roles == nil {
		return nil, fmt.Errorf("no IAM reader")
	}
	role, err := roles.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
	if err != nil {
		return nil, err
	}
	if role.Role == nil {
		return nil, fmt.Errorf("GetRole returned no role")
	}
	fp := map[string]any{
		"RoleId": role.Role.RoleId, "Arn": role.Role.Arn, "Path": role.Role.Path,
		"AssumeRolePolicyDocument": role.Role.AssumeRolePolicyDocument,
		"MaxSessionDuration":       role.Role.MaxSessionDuration,
	}
	if pb := role.Role.PermissionsBoundary; pb != nil {
		fp["PermissionsBoundary"] = pb.PermissionsBoundaryArn
	}

	inline := map[string]string{}
	var marker *string
	for {
		out, err := roles.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: aws.String(name), Marker: marker})
		if err != nil {
			return nil, err
		}
		for _, p := range out.PolicyNames {
			doc, err := roles.GetRolePolicy(ctx, &iam.GetRolePolicyInput{RoleName: aws.String(name), PolicyName: aws.String(p)})
			if err != nil {
				return nil, err
			}
			inline[p] = aws.ToString(doc.PolicyDocument)
		}
		if !out.IsTruncated || out.Marker == nil {
			break
		}
		marker = out.Marker
	}
	fp["InlinePolicies"] = inline

	var attached []string
	marker = nil
	for {
		out, err := roles.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(name), Marker: marker})
		if err != nil {
			return nil, err
		}
		for _, p := range out.AttachedPolicies {
			attached = append(attached, aws.ToString(p.PolicyArn))
		}
		if !out.IsTruncated || out.Marker == nil {
			break
		}
		marker = out.Marker
	}
	sort.Strings(attached)
	fp["AttachedPolicies"] = attached
	return fp, nil
}

// snapshotOwned fingerprints every owned resource. Any read failure is an
// error: before the migration it means survival could not be proven; after it,
// that the resource is gone (or unreadable, which is reported the same way).
func (d *Deployer) snapshotOwned(ctx context.Context, roles iamRoleAPI, owned []OwnedResource) (map[string]string, error) {
	snap := map[string]string{}
	var errs []error
	for _, r := range owned {
		fp, err := d.fingerprint(ctx, roles, r)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s (%s %s): %w", r.LogicalID, r.Type, r.PhysicalID, err))
			continue
		}
		snap[r.LogicalID] = fp
	}
	return snap, errors.Join(errs...)
}

// changedSince compares a fresh read of every owned resource with a snapshot
// and describes each one that is gone or different.
func (d *Deployer) changedSince(ctx context.Context, roles iamRoleAPI, owned []OwnedResource, before map[string]string) []string {
	var problems []string
	for _, r := range owned {
		fp, err := d.fingerprint(ctx, roles, r)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("%s %s (%s) is GONE or unreadable: %v", r.Type, r.PhysicalID, r.LogicalID, err))
		case fp != before[r.LogicalID]:
			problems = append(problems, fmt.Sprintf("%s %s (%s) CHANGED:\n      before: %s\n      after:  %s",
				r.Type, r.PhysicalID, r.LogicalID, before[r.LogicalID], fp))
		}
	}
	return problems
}
