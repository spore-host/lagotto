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
| SNS email subscriptions | one per `--notify email:` address, on `lagotto-capacity-alerts`, filtered on the `lagotto_email` message attribute so each address receives only its own watches' events (#240) | `lagotto watch`; removed by the poller (next sweep), `lagotto cancel` or `lagotto notify-gc` once no active watch names the address; deleted with the topic |
| EventBridge schedule | `lagotto-capacity-poller` at `rate(5 minutes)`, starts **DISABLED** | `lagotto deploy` |
| IAM role | `lagotto-capacity-poller-role` (execution role; its permissions policy is written by `setup`) | `pkg/runtimeiam`: created by `deploy` or `setup` |
| IAM role | `lagotto-capacity-poller-scheduler-invoke` (assumed by EventBridge Scheduler) | `pkg/runtimeiam`: created by `deploy` **only** |
| DynamoDB tables | `lagotto-watches`, `lagotto-match-history` | the CLI: `deploy`, `setup` or `watch`; deleted by `lagotto teardown` |
| DynamoDB table | `lagotto-scheduled-launches`, tagged `lagotto:managed=cli` | the CLI: `deploy` or the first `launch`; deleted by `lagotto teardown` (#250) |
| EventBridge schedules | `lagotto-launch-sl-<id>`, one per scheduled launch (and the poller's boundary-retry schedules) | `lagotto launch` and the poller; deleted by `lagotto cancel sl-<id>` (#279) or `lagotto teardown` (#260) |
| S3 artifact bucket | `lagotto-lambda-<account>-<region>`, tagged `lagotto:managed=cli` | `lagotto deploy`; deleted by `lagotto deploy --teardown` (#264) |

Every one of these has a **fixed name**, which is what makes the deploy idempotent
and the ARNs derivable without reading any stack outputs.

### More than one region in an account

IAM is global, so the two roles are shared by every region you deploy a poller
to. In particular `lagotto-capacity-poller-scheduler-invoke`, the role EventBridge
Scheduler assumes to invoke the poller, is used by the poller schedule in every
region and by every per-launch `lagotto-launch-sl-*` schedule. Its inline
`InvokeLambda` policy therefore names the poller in **every** region:

```json
{"Effect": "Allow", "Action": "lambda:InvokeFunction",
 "Resource": "arn:aws:lambda:*:<account>:function:lagotto-capacity-poller"}
```

It is still scoped to lagotto's own function in your account. Every `deploy`
writes this same document whichever region it runs in, so deploying region A
and then region B converges.

**Before v0.65.1** `deploy` wrote that policy for the deploying region only, and
the last region deployed silently broke the schedule in every other region
(lagotto#218). Each scheduled run failed with a Scheduler target error and the
poller there was never invoked. **One `lagotto deploy` with v0.65.1 or later, in
any one region, rewrites the shared policy for all regions**; you do not need to
re-deploy the others. `lagotto doctor` reports the problem in each region
(`invoke-policy`), and on a best-effort basis the Scheduler target errors
(`scheduler-errors`).

`lagotto deploy --teardown` deletes only that region's poller resources. It never
deletes or rewrites the shared roles or their policies, so tearing down one region
cannot cut off a poller in another.

The runtime permissions policy that `lagotto setup` writes onto
`lagotto-capacity-poller-role` works the same way since v0.65.1: it names
lagotto's tables, topic and schedules in every region (lagotto#219). See the
multi-region bullet under
[Runtime permissions: `lagotto setup`](#runtime-permissions-lagotto-setup).

**To repair an account that runs pollers in several regions, after upgrading to
v0.65.1: run one `lagotto deploy` and one `lagotto setup`, in any one region.**
Together they rewrite both shared policies for every region. You do not need to
repeat them in the other regions.

## Prerequisites

- AWS credentials for the target account, with permission to create the resources
  above (Lambda, SNS, EventBridge Scheduler, IAM roles, DynamoDB tables, S3).
- A **released** lagotto version: `deploy` downloads the published
  `capacity-poller_lambda_linux_arm64.zip` release asset (#29) rather than
  building it. `--version` defaults to the version of the `lagotto` binary you
  run, so a released binary deploys its own matching poller. A `dev` build has no
  published artifact.

## Deploy

```bash
lagotto deploy --region us-east-1
# Grant the poller its runtime permissions.
lagotto setup
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

**Updating the poller's code is just a new deploy.** Upgrade the `lagotto` CLI,
then run `deploy` again (it defaults to the new binary's version), or name a
release explicitly with `--version X.Y.Z`:

```bash
lagotto deploy
```

That uploads the new zip and issues a single `UpdateFunctionCode`; the topic and
schedule are left untouched. (There is no longer any need to run
`aws lambda update-function-code` by hand.)

**Some features start only once the poller is redeployed.** The poller does
the work, so a new CLI alone is not enough. From v0.68.0:

- **Until-landed reminders** (`lagotto watch --until-landed`) are sent by the
  poller. Until it runs v0.68.0, such a watch still pauses at its
  acknowledgement deadline, but without reminding you first.
  `lagotto watch --until-landed`, `lagotto ack` and `lagotto status` warn when
  the poller in your region is older than that. They read the poller Lambda's
  `Version` tag, as `lagotto doctor` does, so `watch`, `ack`, `extend` and
  `status` call `lambda:GetFunction`; without that permission the check is
  skipped silently.
- **The poller notices a new default subnet within 15 minutes** (#256); before,
  it kept skipping the AZ until a cold start.

None of these needs a `lagotto setup`: the poller's runtime policy is unchanged
in v0.68.0 (reminders' live Price List lookups use a grant it already has).

**A deploy never changes the schedule's on/off state.** The schedule's state is
owned out of band — `lagotto watch` enables it, the poller disables itself when no
active watches or pending scheduled launches remain — so a re-deploy of a *running*
poller leaves it running.

### Verify

```bash
lagotto doctor

aws lambda invoke --function-name lagotto-capacity-poller /tmp/lagotto-out.json
cat /tmp/lagotto-out.json
```

`lagotto doctor` is read-only. It reports drift between the deployed poller and
what this `lagotto` binary expects: a runtime IAM policy that is missing grants
(re-run `lagotto setup`), a poller Lambda older than the CLI, missing tables,
roles, topic or schedule, a schedule that is off while watches are active, and a
leftover CloudFormation stack. It also warns about each active `--action notify`
watch: notify is deprecated (#283) because it reports an offering, not
capacity, and those watches should move to `--action hold` or `--action spawn`.
It exits non-zero if any check fails; warnings never do.
(`lagotto status <watch-id>` shows a single watch, not the deployment.)

## Runtime permissions: `lagotto setup`

**The poller's runtime IAM *permissions* are owned by `lagotto setup`, not by
deploy** (#16). `deploy` creates the poller's execution role with only a trust
relationship and CloudWatch Logs basic execution; `setup` writes the permissions
policy (`lagotto-runtime-policy`) that lets the poller read capacity, spawn/hold,
submit SageMaker jobs, and manage its schedule. Since #242 it also lets the poller
terminate the instances spawn launched (gated on `spawn:managed=true`, for
`--if-exists replace` and the teardown after a failed `fsx_create` setup) and
create the `fsx_create` FSx filesystem and its S3 backing bucket, which must be
named `lagotto-fsx-*` (the S3 grants cover only that prefix). CI checks the
policy against the poller Lambda's own AWS calls, discovered from the code. lagotto codifies that policy in Go
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
- **More than one region in an account:** the execution role is IAM-global and
  shared by the poller in every region, so the policy names lagotto's tables,
  topic and schedules in **any** region:

  ```text
  arn:aws:dynamodb:*:<account>:table/lagotto-watches              (+ /index/*)
  arn:aws:dynamodb:*:<account>:table/lagotto-match-history        (+ /index/*)
  arn:aws:dynamodb:*:<account>:table/lagotto-scheduled-launches   (+ /index/*)
  arn:aws:sns:*:<account>:lagotto-capacity-alerts                 (+ :* for its subscriptions)
  arn:aws:scheduler:*:<account>:schedule/default/lagotto-capacity-poller
  arn:aws:scheduler:*:<account>:schedule/default/lagotto-launch-*
  ```

  The resource names and the account are still exact. Every `setup` writes this
  same document whichever region it runs in, so `setup` in region A and then
  region B converges. **Before v0.65.1** `setup` wrote these for its own region
  only, so each run cut every other region's poller off from its own tables,
  topic and schedules (lagotto#219). **After a v0.65.1 deploy, one `lagotto setup`
  in any one region repairs every region.** `lagotto doctor`'s `runtime-policy`
  check FAILs in a region the policy doesn't cover, and WARNs where it covers
  that region only.

## CLI-user permissions: `lagotto-cli-user`

The poller's role covers the poller, not the people who run the CLI, whose
commands use their own credentials. **`lagotto setup` also writes the
customer-managed policy `lagotto-cli-user`** (#232): what the everyday commands
(`watch`, `update`, `ack`, `extend`, `cancel`, `list`, `status`, `history`,
`launch`, `notify-gc`, `doctor`) need, and nothing else. Attach it to whoever runs
them:

```bash
lagotto setup
aws iam attach-group-policy --group-name <group> \
  --policy-arn arn:aws:iam::<account>:policy/lagotto-cli-user
```

- It is built in Go (`pkg/cliiam`) and CI holds it to the generated inventory
  ([docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md)) in
  both directions: an everyday command cannot gain an AWS call the policy does
  not grant, and the policy cannot grant one no everyday command makes.
- Resource-scoped, like the runtime policy, to lagotto's names in any region of
  the account. `iam:PassRole` only on `lagotto-capacity-poller-scheduler-invoke`
  and only to `scheduler.amazonaws.com`; `sns:Subscribe` only by email;
  `ec2:CancelCapacityReservation` only on reservations tagged
  `lagotto:managed=true`. `*` only for actions IAM does not scope.
- **Not covered:** `poll`, which needs the poller's own launch permissions
  (writing and passing instance roles), and `setup`, `deploy` and `teardown`.
- `s3://` spawn configs: name their buckets with `lagotto setup --config-bucket
  <bucket>` (repeatable). They are carried forward on later runs;
  `--revoke-config-bucket` removes one.
- A re-run writes a new default policy version only when the document changed,
  deleting the oldest non-default version once IAM's limit of five is reached.
- `lagotto doctor`'s `caller-permissions` check simulates your identity against
  the policy and lists what you lack (a warning; "unknown" if you may not call
  `iam:SimulatePrincipalPolicy`).
- `lagotto teardown --iam-roles` deletes it, detaching it from every user, group
  and role first.

## Default subnets

The poller launches spawn and hold watches into each region's **default VPC**
without naming a subnet, so it can only land them in AZs that have a default
subnet. `lagotto watch` and `lagotto update` refuse a watch none of whose
offered AZs has one, and warn per AZ that lacks one (#256); `lagotto doctor`'s
`subnet-coverage` check reports the same for active watches. To fix one:

```bash
aws ec2 create-default-subnet --region us-west-2 --availability-zone us-west-2d
```

The checks run with your credentials and call `ec2:DescribeInstanceTypeOfferings`,
`ec2:DescribeSubnets` and `ec2:DescribeRegions`
([docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md)). A
failed lookup is a warning, never a refusal.

## Scheduled launches (`lagotto launch`, #49)

`lagotto launch --at/--after/--cron` (see the README) reuses the deployed poller:
the Lambda doubles as the scheduled-launch target, and each `launch` creates a
per-launch EventBridge schedule (`lagotto-launch-sl-<id>`) that invokes the poller
with a routing payload. It needs the poller function and the
`lagotto-capacity-poller-scheduler-invoke` role, both of which have fixed names, so
`launch` derives their ARNs from your account and region and simply checks the
function exists. The invoke role is created by `lagotto deploy`, and nothing else
(see the CloudFormation path below). A third DynamoDB table,
`lagotto-scheduled-launches` (PAY_PER_REQUEST, TTL on `ttl_timestamp`), is created
by `lagotto deploy` or on the first `launch`. Until then the poller treats the
missing table as "no launches pending".

The poller's self-teardown reference-counts pending scheduled launches alongside
active watches: the schedule is only disabled / CLI-managed tables deleted when
there are **neither** active watches **nor** pending scheduled launches, so a
`launch --at next-week` can't have its infrastructure removed out from under it.

## Teardown

```bash
lagotto deploy --teardown
```

This deletes the three poller resources, in the order schedule → function → topic
so nothing can fire into a half-deleted poller. Deleting the topic deletes its
email subscriptions with it. It is idempotent: running it twice
succeeds.

**The artifact bucket** `lagotto-lambda-<account>-<region>` is deleted last,
after the poller, with every object version and delete marker in it (#264). Only
when lagotto manages it: this account owns it (every call carries
`ExpectedBucketOwner`) and it carries the `lagotto:managed=cli` tag. `lagotto
deploy` writes the tag when it creates the bucket, and adds it to an existing
default-named bucket this account owns, so a bucket from an older deploy becomes
deletable after one more `deploy`. An untagged bucket is retained and reported. A
bucket you supplied with `--lambda-bucket` is never tagged or deleted, even one
spelled like the default. `lagotto doctor` warns about an artifact bucket in a
region with no poller.

**Explicitly retained** (each is listed in the confirmation prompt):

- **The DynamoDB tables** — `lagotto-watches`, `lagotto-match-history` and
  `lagotto-scheduled-launches` hold your watches, match history and pending
  scheduled launches. Use `lagotto teardown` to delete all three (when empty, or
  with `--force`). It deletes the scheduled-launches table only if it carries
  the `lagotto:managed=cli` tag lagotto sets when it creates it.
- **Both IAM roles** — `lagotto-capacity-poller-role` and
  `lagotto-capacity-poller-scheduler-invoke`. They are CLI-owned and IAM-global:
  shared by the poller in every region of the account, and the invoke role also by
  per-launch scheduled launches. Teardown is per-region, so it cannot know that no
  other region still needs them, and it never touches them (#218). Once no region
  runs a poller, delete them with `lagotto teardown --iam-roles` (#229). It checks
  every region enabled in the account first and refuses, deleting nothing, while a
  poller exists in any of them or a region cannot be checked.
- **Per-launch `lagotto-launch-sl-*` schedules.** Each one fires a scheduled
  launch recorded in the retained `lagotto-scheduled-launches` table, so it is
  kept with that table, and a redeployed poller fires it again. `lagotto
  teardown` deletes them together with the tables (#260): every schedule a
  record names and every `lagotto-launch-sl-*` schedule in the default group,
  before it deletes the table that records them. `lagotto doctor` warns about a
  per-launch schedule with no record.

### Removing everything

To leave nothing behind in an account:

1. `lagotto deploy --teardown --region <region>` in every region that runs a
   poller: the schedule, the Lambda, the topic (with its email subscriptions)
   and the lagotto-managed artifact bucket.
2. `lagotto teardown` (add `--force` if the tables still hold records): the
   per-launch `lagotto-launch-sl-*` schedules, then the three DynamoDB tables.
   It stops before deleting any table if it cannot list or delete the
   schedules.
3. `lagotto teardown --iam-roles`: the two IAM roles and their policies, and the
   `lagotto-cli-user` policy (detached from every user, group and role first).
   It refuses while a poller exists in any enabled region.

`lagotto doctor` warns about what a partial teardown leaves: an artifact bucket
in a region with no poller (`artifact-buckets`) and a per-launch schedule that no
scheduled-launch record names. The new permissions these commands need are in
[docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md).

## Alternate: deploy via CloudFormation (IaC / enterprise)

`lagotto deploy` does not use CloudFormation. The template is still shipped —
embedded in the binary at `deployment/cloudformation/template.go` so it can't drift
out of the release, with the source at
`deployment/cloudformation/lagotto-stack.yaml` — for environments that expect a
declarative artifact: Control Tower, infra review, change management, or a
CI-driven `aws cloudformation deploy`.

The template declares the same three resources and **references** the IAM roles and
DynamoDB tables by name without creating them, so the CLI still has to prepare
those first. `lagotto setup` creates all of them except the scheduled-launches
table, without deploying a poller:

| Referenced by the stack | `lagotto setup` | `lagotto deploy` |
|---|---|---|
| `lagotto-watches`, `lagotto-match-history` tables | creates | creates |
| `lagotto-capacity-poller-role` + its `lagotto-runtime-policy` | creates the role and writes the policy | creates the role; does **not** write the policy |
| `lagotto-capacity-poller-scheduler-invoke` role + its `InvokeLambda` policy | creates (since #229) | creates |
| `lagotto-scheduled-launches` table | does not create | creates |

The scheduled-launches table can wait: the poller treats a missing table as "no
launches pending", and the first `lagotto launch` creates it. The invoke role
cannot: the stack's `PollerSchedule` assumes it to invoke the poller, and the
Lambda's `SCHEDULER_INVOKE_ROLE_ARN` names it. `setup` creates it (lagotto#229;
before that only `deploy` did), and prints each role it created. Both roles are
IAM-global, so one `setup` in any region serves every region; `lagotto doctor`'s
`iam-roles` check confirms they exist.

```bash
# Set VERSION to the lagotto release you want, e.g. the one `lagotto version` prints.
VERSION=X.Y.Z

# 1. Tables, both CLI-owned IAM roles, and their policies.
lagotto setup --region us-east-1

# 2. Get the published Lambda zip and stage it in a bucket you control.
gh release download v$VERSION --repo spore-host/lagotto \
  --pattern 'capacity-poller_lambda_linux_arm64.zip'
aws s3 cp capacity-poller_lambda_linux_arm64.zip \
  s3://my-artifact-bucket/lagotto/capacity-poller-v$VERSION.zip

# 3. Deploy the stack.
aws cloudformation deploy \
  --stack-name lagotto \
  --template-file deployment/cloudformation/lagotto-stack.yaml \
  --capabilities CAPABILITY_AUTO_EXPAND \
  --parameter-overrides \
      Environment=production \
      LambdaCodeBucket=my-artifact-bucket \
      LambdaCodeKey=lagotto/capacity-poller-v$VERSION.zip \
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

But the stack still believes it owns them, and a stack created by an older
lagotto may own more: the account-wide IAM roles `lagotto-capacity-poller-role`
and `lagotto-capacity-poller-scheduler-invoke`, which every region's poller uses,
and (older still) the DynamoDB tables. `aws cloudformation delete-stack` would
delete all of it. `lagotto deploy` warns you whenever it sees such a stack. To
detach it safely:

```bash
lagotto deploy --migrate-from-cloudformation
```

That:

1. lists every resource the stack owns (`ListStackResources`), and reads each
   one's live state;
2. takes the stack's **own** deployed template and adds only
   `DeletionPolicy: Retain` and `UpdateReplacePolicy: Retain` to every resource;
3. creates a **change set** with every parameter at `UsePreviousValue`, and
   refuses unless every change is a Modify with no replacement that touches only
   those two policies. If CloudFormation will re-evaluate a dependent's
   `!GetAtt` at execution (a Dynamic change), it runs drift detection on that
   resource and refuses unless the stack's stored properties match the live
   resource, so the re-evaluation cannot revert anything;
4. confirms the change set's **processed** template (after the SAM transform)
   retains every owned resource, executes it, and confirms the stack's processed
   template does too;
5. deletes the stack, then verifies every resource it owned still exists,
   unchanged.

Every refusal before step 4 deletes the change set unexecuted and leaves the
stack untouched, and prints the manual procedure. A Dynamic re-evaluation
commonly refuses on the schedule's `State`: the legacy template stores it
`DISABLED`, so run the migration while no watch is active.

(`delete-stack --retain-resources` is not usable here: CloudFormation accepts it
only for a stack already in `DELETE_FAILED`.)

## Running Integration Tests

Integration tests hit real DynamoDB tables. From the repository root:

```bash
AWS_PROFILE=spore-host-infra LAGOTTO_INTEGRATION_TEST=1 \
  go test -tags integration -v ./... -run TestIntegration
```

Tests clean up after themselves (cancel their test watches). The `g7e.xlarge` watch
may or may not find capacity — both outcomes are valid.
