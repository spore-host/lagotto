# Lagotto Deployment Guide

This document describes how to deploy lagotto's hosted capacity poller into an AWS
account, so watches are serviced server-side instead of by a foreground
`lagotto poll --daemon` that dies when your laptop sleeps.

There are two paths:

- **`lagotto deploy`** — the primary path, and the one to use unless you have a
  specific reason not to. It provisions everything with direct AWS API calls.
- **CloudFormation** — an optional, alternate declarative path for IaC and
  enterprise/governance environments. See
  [Alternate: deploy via CloudFormation](#alternate-deploy-via-cloudformation-iac--enterprise).

## What gets deployed

| Resource | Name | Owner |
|---|---|---|
| Lambda | `lagotto-capacity-poller` (arm64, `provided.al2023`, 512 MB, 900 s) | `lagotto deploy` |
| SNS topic | `lagotto-capacity-alerts` (KMS encrypted with `alias/aws/sns`) | `lagotto deploy` |
| EventBridge schedule | `lagotto-capacity-poller` at `rate(5 minutes)`, starts **DISABLED** | `lagotto deploy` |
| IAM roles | `lagotto-capacity-poller-role`, `lagotto-capacity-poller-scheduler-invoke` | `pkg/runtimeiam` (`deploy`/`setup`) |
| DynamoDB tables | `lagotto-watches`, `lagotto-match-history`, `lagotto-scheduled-launches` | the CLI (`watch`/`launch`/`deploy`) |
| S3 artifact bucket | `lagotto-lambda-<account>-<region>` | `lagotto deploy` |

Every one of these has a **fixed name**, which is what makes the deploy idempotent
and the ARNs derivable without reading any stack outputs.

## Prerequisites

- AWS credentials for the target account, with permission to create the resources
  above (Lambda, SNS, EventBridge Scheduler, IAM roles, DynamoDB tables, S3).
- A **released** lagotto version: `deploy` downloads the published
  `capacity-poller_lambda_linux_arm64.zip` release asset (#29) rather than
  building it. A `dev` build has no published artifact.

## Deploy

```bash
lagotto deploy --version 0.60.0 --region us-east-1
lagotto setup                     # grant the poller its runtime permissions
```

`deploy` does, in order:

1. Ensures the three DynamoDB tables exist (idempotent — the natural
   "`watch` first, then `deploy`" flow works either way).
2. Ensures the two IAM roles exist.
3. Creates the artifact bucket if absent and uploads the release Lambda zip.
4. Creates or converges the SNS topic, the poller Lambda and its schedule.

It prints the resolved resource ARNs and one line per resource saying what
happened (`created`, `code updated`, `unchanged`).

### Re-running deploy

`deploy` is incremental and safe to re-run. Nothing is recreated, nothing
collides, and there is no stack to strand in a failed state — a half-finished
deploy is fixed by running it again.

**Updating the poller's code is just a `--version` bump:**

```bash
lagotto deploy --version 0.61.0
```

That uploads the new zip and issues a single `UpdateFunctionCode`; the topic and
schedule are left untouched. (There is no longer any need to run
`aws lambda update-function-code` by hand.)

**A deploy never changes the schedule's on/off state.** The schedule's state is
owned out of band — `lagotto watch` enables it, the poller disables itself when no
active watches or pending scheduled launches remain — so a re-deploy of a *running*
poller leaves it running.

### Verify

```bash
lagotto status

aws lambda invoke --function-name lagotto-capacity-poller /tmp/lagotto-out.json
cat /tmp/lagotto-out.json
```

## Runtime permissions: `lagotto setup`

**The poller's runtime IAM *permissions* are owned by `lagotto setup`, not by
deploy** (#16). `deploy` creates the poller's execution role with only a trust
relationship and CloudWatch Logs basic execution; `setup` writes the permissions
policy (`lagotto-runtime-policy`) that lets the poller read capacity, spawn/hold,
submit SageMaker jobs, and manage its schedule. lagotto codifies that policy in Go
(`pkg/runtimeiam`) as the single source of truth, the same way it owns its tables —
so adding a permission is a code change, not a template edit.

Three things to know:

- **After a fresh `lagotto deploy`, run `lagotto setup`.** Until you do, the
  deployed poller can only send notifications — it cannot spawn, hold or submit.
- `setup` is idempotent and re-applies the current policy on each run.
- ⚠️ **`setup` applies the policy with `PutRolePolicy`, which REPLACES
  `lagotto-runtime-policy` wholesale.** Two consequences:
  - **Re-run `setup` after upgrading lagotto**, so the poller gains any
    permissions the new version needs. Several releases have shipped fixes that
    silently no-op without it.
  - **Never run an *older* lagotto's `setup` against a newer-deployed poller.** It
    will overwrite the policy with the older version's set and silently revoke the
    newer grants. (This bit a beta tester during #148.)

## Scheduled launches (`lagotto launch`, #49)

`lagotto launch --at/--after/--cron` (see the README) reuses the deployed poller:
the Lambda doubles as the scheduled-launch target, and each `launch` creates a
per-launch EventBridge schedule (`lagotto-launch-sl-<id>`) that invokes the poller
with a routing payload. It needs the poller function and the
`lagotto-capacity-poller-scheduler-invoke` role, both of which have fixed names, so
`launch` derives their ARNs from your account and region and simply checks the
function exists. A third DynamoDB table, `lagotto-scheduled-launches`
(PAY_PER_REQUEST, TTL on `ttl_timestamp`), is created on first `launch`.

The poller's self-teardown reference-counts pending scheduled launches alongside
active watches: the schedule is only disabled / CLI-managed tables deleted when
there are **neither** active watches **nor** pending scheduled launches, so a
`launch --at next-week` can't have its infrastructure removed out from under it.

## Teardown

```bash
lagotto deploy --teardown
```

This deletes the three poller resources, in the order schedule → function → topic
so nothing can fire into a half-deleted poller. It is idempotent: running it twice
succeeds.

**Explicitly retained** (each is listed in the confirmation prompt):

- **The DynamoDB tables** — they hold your watches and match history. Use
  `lagotto teardown` to delete those (when empty, or with `--force`).
- **Both IAM roles** — `lagotto-capacity-poller-role` and
  `lagotto-capacity-poller-scheduler-invoke`. They are CLI-owned, and the invoke
  role is shared with per-launch scheduled launches. Delete them by hand if you
  really want them gone.
- **The artifact bucket** (`lagotto-lambda-<account>-<region>`) — it is created
  outside the poller's lifecycle. Delete it manually if you want.
- **Per-launch `lagotto-launch-sl-*` schedules.**

## Alternate: deploy via CloudFormation (IaC / enterprise)

`lagotto deploy` does not use CloudFormation. The template is still shipped —
embedded in the binary at `deployment/cloudformation/template.go` so it can't drift
out of the release, with the source at
`deployment/cloudformation/lagotto-stack.yaml` — for environments that expect a
declarative artifact: Control Tower, infra review, change management, or a
CI-driven `aws cloudformation deploy`.

The template declares the same three resources and **references** the IAM roles and
DynamoDB tables by name without creating them, so the CLI still has to prepare
those first:

```bash
# 1. The tables and roles are CLI-owned; create them before the stack.
lagotto setup --region us-east-1

# 2. Get the published Lambda zip and stage it in a bucket you control.
gh release download v0.60.0 --repo spore-host/lagotto \
  --pattern 'capacity-poller_lambda_linux_arm64.zip'
aws s3 cp capacity-poller_lambda_linux_arm64.zip \
  s3://my-artifact-bucket/lagotto/capacity-poller-v0.60.0.zip

# 3. Deploy the stack.
aws cloudformation deploy \
  --stack-name lagotto \
  --template-file deployment/cloudformation/lagotto-stack.yaml \
  --capabilities CAPABILITY_AUTO_EXPAND \
  --parameter-overrides \
      Environment=production \
      LambdaCodeBucket=my-artifact-bucket \
      LambdaCodeKey=lagotto/capacity-poller-v0.60.0.zip \
  --region us-east-1
```

**`CAPABILITY_AUTO_EXPAND` is the only capability required, and
`CAPABILITY_IAM` must NOT be passed.** `AUTO_EXPAND` is for the
`AWS::Serverless-2016-10-31` transform. No IAM capability is needed because the
stack declares no IAM resources at all — both roles are CLI-owned since #146 and
the stack only references them by ARN. (This is also why passing
`CAPABILITY_NAMED_IAM` used to be required and then stopped being: #143 added it,
#145 removed the IAM resources entirely.)

Note that this path is **not** the one lagotto's own CLI exercises, so it gets less
testing. It exists for IaC consumers; `lagotto deploy` is the supported default.

### Migrating off an existing CloudFormation stack

If you previously deployed via CloudFormation (any lagotto before 0.60.0), you do
not need to do anything for the poller to keep working: all three resources have
fixed names, so `lagotto deploy` **adopts** them. Nothing is duplicated and nothing
is recreated.

But the stack still believes it owns them, so `aws cloudformation delete-stack`
would delete your live poller. `lagotto deploy` warns you whenever it sees such a
stack. To detach it safely:

```bash
lagotto deploy --migrate-from-cloudformation --version 0.60.0
```

That runs `UpdateStack` to put `DeletionPolicy: Retain` (and
`UpdateReplacePolicy: Retain`) on the three resources, **reads the deployed template
back to verify the policies actually landed**, and only then deletes the stack —
aborting without deleting anything if the verification fails. It finishes by
confirming the Lambda, schedule and topic all still resolve.

(`delete-stack --retain-resources` is not usable here: CloudFormation accepts it
only for a stack already in `DELETE_FAILED`.)

## Running Integration Tests

Integration tests hit real DynamoDB tables:

```bash
cd lagotto
AWS_PROFILE=spore-host-infra LAGOTTO_INTEGRATION_TEST=1 \
  go test -tags integration -v ./... -run TestIntegration
```

Tests clean up after themselves (cancel their test watches). The `g7e.xlarge` watch
may or may not find capacity — both outcomes are valid.

## AWS Account Reference

From `spawn/CLAUDE.md`:

- **spore-host-infra** (966362334030): All lagotto infrastructure lives here —
  DynamoDB, SNS, Lambda, EventBridge.
- **spore-host-dev** (435415984226): EC2 only; not used by lagotto directly.

The Lambda does not assume a cross-account role — it only reads EC2 metadata
(instance types, spot pricing) via the `ec2:Describe*` APIs in the infra account's
context.
