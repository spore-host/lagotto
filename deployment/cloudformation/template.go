// Package cfn embeds lagotto's CloudFormation/SAM template so the CLI can deploy
// the hosted capacity-poller stack into the user's own account without shipping
// the YAML separately (`lagotto deploy`, #48). Embedding couples the deployed
// template to the binary's version, so there's no drift between the CLI and the
// infra it stands up. The raw YAML remains the source of truth for the internal
// `sam deploy` path documented in DEPLOYMENT.md.
package cfn

import _ "embed"

// StackTemplate is the lagotto stack (SNS topic, Lambda, EventBridge Scheduler)
// as a SAM template. Deploy it with CAPABILITY_AUTO_EXPAND only (the
// AWS::Serverless transform). It defines no IAM resources: the poller execution
// role and the Scheduler invoke role are CLI-owned (created by pkg/runtimeiam,
// like the DynamoDB tables) and only referenced by ARN — so there is no
// named-IAM create collision under CloudFormation Early Validation (#145). See
// deploy.deployCapabilities.
//
//go:embed lagotto-stack.yaml
var StackTemplate string
