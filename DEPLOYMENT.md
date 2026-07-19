# Lagotto Deployment Guide

This document describes how to deploy lagotto's infrastructure (DynamoDB, SNS, Lambda, EventBridge) to the `spore-host-infra` AWS account.

## Prerequisites

- AWS CLI configured with `spore-host-infra` profile (account 966362334030)
- Go 1.26+ for building the Lambda binary
- CloudFormation deploy permissions

> **The Lambda zip is now a published release asset.** Each tagged release
> attaches `capacity-poller_lambda_linux_arm64.zip` to its GitHub Release (built
> by GoReleaser, #29), so you can download it directly instead of building +
> uploading it yourself:
>
> ```bash
> gh release download v<version> --repo spore-host/lagotto \
>   --pattern 'capacity-poller_lambda_linux_arm64.zip'
> ```
>
> The manual build/upload steps below remain the internal/`spore-host-infra`
> path and are still valid.

## One-time Setup

### 1. Build the Lambda zip

From the repository root:

```bash
cd lagotto/lambda/capacity-poller
make build
# Produces: function.zip (containing bootstrap binary for linux/arm64)
```

### 2. Upload the Lambda zip to S3

```bash
VERSION=v0.32.0  # match the release tag
AWS_PROFILE=spore-host-infra aws s3 cp function.zip \
  s3://spawn-binaries-us-east-1/lagotto/capacity-poller-${VERSION}.zip
```

### 3. Deploy the CloudFormation stack

```bash
AWS_PROFILE=spore-host-infra aws cloudformation deploy \
  --stack-name lagotto \
  --template-file deployment/cloudformation/lagotto-stack.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
      Environment=production \
      LambdaCodeBucket=spawn-binaries-us-east-1 \
      LambdaCodeKey=lagotto/capacity-poller-${VERSION}.zip \
  --region us-east-1
```

Creates:
- SNS topic: `lagotto-capacity-alerts` (KMS encrypted with `alias/aws/sns`)
- Lambda: `lagotto-capacity-poller` (arm64, provided.al2023, 512MB, 900s timeout)
- EventBridge Schedule: `lagotto-capacity-poller` at `rate(5 minutes)`, starts **DISABLED**
- IAM roles for Lambda execution + EventBridge invocation

**The poller's runtime IAM *permissions* are owned by `lagotto setup`, not the stack** (#16). The stack creates a **minimal** execution role (`lagotto-capacity-poller-role`: Lambda trust + CloudWatch Logs basic execution only); `lagotto setup` attaches the permissions policy (`lagotto-runtime-policy`) that lets the poller read capacity, spawn/hold, submit SageMaker jobs, and manage its schedule. lagotto codifies that policy in Go (`pkg/runtimeiam`) as the single source of truth, the same way it owns its tables — so adding a permission is a code change, not a template edit. **After a fresh `lagotto deploy`, run `lagotto setup`** to grant these permissions; **until then the deployed poller can only send notifications**, not spawn/hold/submit. `setup` is idempotent and re-applies the current policy each run (and reports a clear next-step if the role doesn't exist yet).

**The DynamoDB tables (`lagotto-watches`, `lagotto-match-history`, `lagotto-scheduled-launches`) are NOT created by the stack** (#59). They are owned by the CLI, which auto-creates them on first `lagotto watch`/`launch` (#12); the stack only *references* them by name (the poller's env vars + IAM are scoped to them). `lagotto deploy` ensures they exist before deploying, so the natural "`watch` first, then `deploy`" flow works whether or not the tables already exist. The table names are stack parameters (`WatchesTableName` / `HistoryTableName` / `ScheduledTableName`) defaulting to the standard names, so a non-default table set (`--watches-table`/`--history-table`) still wires the poller correctly.

The schedule starts disabled to avoid billing when no watches are active. The `lagotto watch` CLI enables it on first watch creation; the Lambda disables it when the last active watch is removed. The deployed poller does **not** auto-delete the data tables when they idle to empty (it sets no `AUTO_DELETE_TABLES`) — deployed infra is persistent; tear it down with `lagotto deploy --teardown` (which deletes the stack but retains your tables) or `lagotto teardown` (which deletes the tables).

#### Scheduled launches (`lagotto launch`, #49)

`lagotto launch --at/--after/--cron` (see the README) reuses this stack: the
poller Lambda doubles as the scheduled-launch target, and each `launch` creates a
per-launch EventBridge schedule (`lagotto-launch-sl-<id>`) that invokes the poller
with a routing payload. This relies on two stack outputs — `CapacityPollerFunctionArn`
(the target) and `SchedulerInvokeRoleArn` (the role Scheduler assumes) — so a stack
deployed **without** the Lambda can't run scheduled launches. A third DynamoDB table,
`lagotto-scheduled-launches` (PAY_PER_REQUEST, TTL on `ttl_timestamp`), is created on
first `launch`.

The poller's self-teardown reference-counts pending scheduled launches alongside
active watches: the schedule is only disabled / CLI-managed tables deleted when
there are **neither** active watches **nor** pending scheduled launches, so a
`launch --at next-week` can't have its infrastructure removed out from under it.

### 4. Verify deployment

```bash
AWS_PROFILE=spore-host-infra aws cloudformation describe-stacks \
  --stack-name lagotto \
  --query 'Stacks[0].Outputs'

# Smoke-test the Lambda
AWS_PROFILE=spore-host-infra aws lambda invoke \
  --function-name lagotto-capacity-poller \
  /tmp/lagotto-out.json
cat /tmp/lagotto-out.json
```

## Updating the Lambda (code-only)

When you only change Go code (no CloudFormation changes):

```bash
cd lagotto/lambda/capacity-poller
make build

AWS_PROFILE=spore-host-infra aws lambda update-function-code \
  --function-name lagotto-capacity-poller \
  --zip-file fileb://function.zip
```

## Running Integration Tests

Integration tests hit the real DynamoDB tables in `spore-host-infra`:

```bash
cd lagotto
AWS_PROFILE=spore-host-infra LAGOTTO_INTEGRATION_TEST=1 \
  go test -tags integration -v ./... -run TestIntegration
```

Tests clean up after themselves (cancel their test watches). The `g7e.xlarge` watch may or may not find capacity — both outcomes are valid.

## Teardown

```bash
# Deletes the poller stack (Lambda, SNS, schedule, IAM) but RETAINS the data tables
lagotto deploy --teardown
# or, equivalently, delete the stack directly:
AWS_PROFILE=spore-host-infra aws cloudformation delete-stack --stack-name lagotto --region us-east-1
```

The DynamoDB tables are **not** part of the stack (#59), so deleting the stack leaves them intact — watches and match history survive a redeploy. To delete the tables too (when they're empty, or `--force`), use `lagotto teardown`.

## AWS Account Reference

From `spawn/CLAUDE.md`:

- **spore-host-infra** (966362334030): All lagotto infrastructure lives here — DynamoDB, SNS, Lambda, EventBridge.
- **spore-host-dev** (435415984226): EC2 only; not used by lagotto directly.

The Lambda does not assume a cross-account role — it only reads EC2 metadata (instance types, spot pricing) via the `ec2:Describe*` APIs in the infra account's context.
