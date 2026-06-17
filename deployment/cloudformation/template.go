// Package cfn embeds lagotto's CloudFormation/SAM template so the CLI can deploy
// the hosted capacity-poller stack into the user's own account without shipping
// the YAML separately (`lagotto deploy`, #48). Embedding couples the deployed
// template to the binary's version, so there's no drift between the CLI and the
// infra it stands up. The raw YAML remains the source of truth for the internal
// `sam deploy` path documented in DEPLOYMENT.md.
package cfn

import _ "embed"

// StackTemplate is the lagotto stack (DynamoDB, SNS, Lambda, EventBridge
// Scheduler, IAM) as a SAM template. Deploy it with CAPABILITY_IAM +
// CAPABILITY_AUTO_EXPAND (it uses the AWS::Serverless transform).
//
//go:embed lagotto-stack.yaml
var StackTemplate string
