// Package cfn embeds lagotto's CloudFormation/SAM template — the OPTIONAL,
// alternate declarative path for standing up the hosted capacity poller.
//
// `lagotto deploy` does not use it: since #154 the CLI provisions the poller's
// three resources with direct SDK calls, because every deploy outage lagotto had
// was CloudFormation-specific (#67/#68, #59, #143, #145). The template is kept,
// and documented in DEPLOYMENT.md, for IaC and enterprise/governance environments
// (Control Tower, infra review, change management, a CI-driven
// `aws cloudformation deploy`) that expect a declarative artifact.
//
// It stays EMBEDDED for two reasons: the shipped template can't drift out of the
// release, and pkg/deploy still needs the body in-binary — to build the
// `DeletionPolicy: Retain` overlay that `--migrate-from-cloudformation` applies to
// an existing stack, and to create a real stack in the migration tests.
//
// The raw YAML at lagotto-stack.yaml is the source of truth. It is consumed by
// `aws cloudformation deploy` / `create-stack`; there is no SAM CLI path.
package cfn

import _ "embed"

// StackTemplate is the lagotto stack (SNS topic, Lambda, EventBridge Scheduler)
// as a SAM template. Deploy it with CAPABILITY_AUTO_EXPAND only (the
// AWS::Serverless transform) and NO IAM capability: it defines no IAM resources —
// the poller execution role and the Scheduler invoke role are CLI-owned (created
// by pkg/runtimeiam, like the DynamoDB tables) and only referenced by ARN, so
// there is no named-IAM create collision under CloudFormation Early Validation
// (#145). See deploy.deployCapabilities.
//
//go:embed lagotto-stack.yaml
var StackTemplate string
