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
| CloudWatch Logs log group | `/aws/lambda/lagotto-capacity-poller`, 30-day retention, tagged `lagotto:managed=cli`; created before the function so Lambda never creates an unowned one (an existing group is tagged, and given the retention only if it has none) | `lagotto deploy`; deleted by `lagotto deploy --teardown` (#305) |
| SNS topic | `lagotto-capacity-alerts` (KMS encrypted with `alias/aws/sns`) | `lagotto deploy` |
| SNS email subscriptions | one per `--notify email:` address, on `lagotto-capacity-alerts`, filtered on the `lagotto_email` message attribute so each address receives only its own watches' events (#240) | `lagotto watch`; removed by the poller (next sweep), `lagotto cancel` or `lagotto notify-gc` once no active watch names the address; deleted with the topic |
| EventBridge Scheduler schedule group | `lagotto`, tagged (schedules cannot be tagged, so lagotto's live in this group; see [Tags](#tags)) | `lagotto deploy`; deleted, once empty, by `lagotto deploy --teardown` or `lagotto teardown` (#324) |
| EventBridge schedule | `lagotto-capacity-poller` in schedule group `lagotto` at `rate(5 minutes)`, starts **DISABLED**; an idle poller slows it to `rate(1 hour)` (see [Idle poller](#idle-poller-slow-mode)) | `lagotto deploy` |
| IAM role | `lagotto-capacity-poller-role` (execution role; its permissions policy is written by `setup`) | `pkg/runtimeiam`: created by `deploy` or `setup` |
| IAM role | `lagotto-capacity-poller-scheduler-invoke` (assumed by EventBridge Scheduler) | `pkg/runtimeiam`: created by `deploy` **only** |
| DynamoDB tables | `lagotto-watches`, `lagotto-match-history` | the CLI: `deploy`, or `watch --local` where no poller is deployed (`setup` creates none, #325); deleted by `lagotto teardown` |
| DynamoDB table | `lagotto-scheduled-launches`, tagged `lagotto:managed=cli` | the CLI: `deploy` or the first `launch`; deleted by `lagotto teardown` (#250) |
| EventBridge schedules | `lagotto-launch-sl-<id>` in schedule group `lagotto`, one per scheduled launch (and the poller's boundary-retry schedules) | `lagotto launch` and the poller; deleted by `lagotto cancel sl-<id>` (#279) or `lagotto teardown` (#260) |
| S3 artifact bucket | `lagotto-lambda-<account>-<region>`, tagged `lagotto:managed=cli` | `lagotto deploy`; deleted by `lagotto deploy --teardown` (#264) |

Every one of these has a **fixed name**, which is what makes the deploy idempotent
and the ARNs derivable without reading any stack outputs.

### Tags

Every resource lagotto creates carries the spore.host tags (lagotto#324), so
leave-no-trace cleanup ([spore-host#752](https://github.com/spore-host/spore-host/issues/752))
can find what lagotto made and nothing else:

| Tag | Value |
|---|---|
| `spore-host:managed-by` | `lagotto`: the uniform key cleanup selects on |
| `spore-host:scope` | the region; `global` for IAM roles, instance profiles and the policy |
| `spore-host:created` | when lagotto created it, or first tagged it (RFC 3339, UTC) |
| `spore-host:schema` | `1` |
| `spore-host:owner` | the `aws:userid` of whoever created it, when known |
| `spore-host:component` | on the deployment's infrastructure (everything in the table above, the instance role and the CLI-user policy), the part it belongs to: `capacity-poller`, `alerts`, `store`, `artifacts`, `instance-role`, `cli-policy` |

What the poller creates for a watch (instances launched through spawn, capacity
reservations, SageMaker training jobs) carries the first five and no component,
next to its `lagotto:watch-id` tag.

lagotto never sets `spore-host:role`. Its value `service` is reserved for
spore.host's own live service in the spore.host infra account
([spore-host#750](https://github.com/spore-host/spore-host/issues/750)): cleanup
is denied on anything carrying it. A lagotto deployment is tool infrastructure
in your account, which lagotto's own idle teardown
([lagotto#326](https://github.com/spore-host/lagotto/issues/326)) must be able
to remove.

Resources are tagged when they are created. `lagotto setup` and `lagotto deploy`
also add the tags to anything an older lagotto created without them, keeping
tags already there; a `spore-host:created` or `spore-host:owner` once set is
never changed. EventBridge Scheduler schedules cannot be tagged, so they live in
the tagged schedule group `lagotto`.

**`lagotto:managed`** is the older per-resource marker. Its values are matched on
exactly and are kept as they are; it is not added to anything new:

| Value | On | Read by |
|---|---|---|
| `cli` | the three tables, the artifact bucket, the poller log group, the `lagotto-cli-user` policy | `lagotto teardown`, `deploy --teardown`, the poller's opt-in `AUTO_DELETE_TABLES` |
| `true` | capacity reservations and SageMaker jobs the poller creates | `lagotto cancel` releases only reservations carrying it |
| `instance-profile` | `lagotto-instance-role` and the instance profiles `setup` creates | the instance-profile deleters |

FSx filesystems and `lagotto-fsx-*` buckets created for an `fsx_create` launch
are tagged by spawn, which creates them, not by lagotto.

### Schedule groups (upgrading)

Before lagotto#324 the poller schedule and the per-launch schedules lived in
the `default` schedule group. `lagotto deploy` now creates the group `lagotto`
and moves the poller schedule into it: it creates the schedule there (same
state, cadence and settings) and only then deletes the one in `default`, so the
poller is never without one. If the delete fails both exist for a moment and
`deploy` says so; re-running it deletes the copy in `default`, and `lagotto
doctor` warns while both exist. New per-launch schedules are created in
`lagotto`; existing ones in `default` keep working, and every command looks in
both groups.

`deploy` leaves the schedule in `default` (and says why) while a legacy
CloudFormation stack still owns it, or while the poller's runtime policy or the
`lagotto-cli-user` policy predates the group, since the poller and CLI users
could not then manage the moved schedule. **After upgrading, run `lagotto setup`
and then `lagotto deploy`.**

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

1. Ensures the three DynamoDB tables exist (idempotent). `deploy` is where a
   region's tables come from: `watch` refuses in a region with no poller (unless
   `--local`, for a local `poll --daemon`), and `setup` creates no table (#325).
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

**A deploy never changes the schedule's on/off state or its idle cadence.** Both
are owned out of band — arming a watch (`watch`, `extend`, `update`) sets the
schedule ENABLED every 5 minutes, and an idle poller slows it to hourly — so a
re-deploy of a *running* poller leaves it running.

### Idle poller (slow mode)

When the poller finds no active watch and no pending scheduled launch it does
**not** disable its schedule (lagotto#323). It slows it to `rate(1 hour)`, still
ENABLED: about 720 invocations a month. Arming a watch sets the schedule back to
`rate(5 minutes)`, and the poller restores it too as soon as it finds work.

Earlier pollers disabled the schedule when idle, and that raced with arming: if
the poller counted zero watches, then `lagotto watch` stored one and found the
schedule still ENABLED, the poller then disabled it and the new watch was never
polled. Now arming increments an epoch in a control item of `lagotto-watches`
(key `#lagotto/poller-control`) before it sets the schedule; the poller reads the
epoch before counting, slows down only if a conditional write on that epoch
succeeds, and re-reads it afterwards, restoring `rate(5 minutes)` if it moved.
Since nothing disables the schedule any more, a watch armed by any lagotto
version is polled within the hour even in the worst case.

Mixed versions: an older poller still disables its schedule when idle, and a
current `lagotto watch` re-enables it (at `rate(5 minutes)`) whichever it finds;
`lagotto doctor` reports a DISABLED schedule with active watches as a failure. An
older `lagotto teardown` counts the control item as a record in `lagotto-watches`
and asks for `--force`; the current one ignores it.

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
named `lagotto-fsx-*` (the S3 grants cover only that prefix).

**The poller writes no IAM.** It cannot create, tag or change a role or
instance profile. It may pass to EC2 only `lagotto-instance-role` (which `setup`
creates with spawn's spored/SSM baseline) and the roles named with `setup
--instance-role` (which must exist; `setup` creates their instance profiles),
and to SageMaker only the role ARNs named with `setup --sagemaker-role`. A
watch's `iam_policy*` settings are refused, and the poller re-checks every
stored watch before launching. CI checks the
policy against the poller Lambda's own AWS calls, discovered from the code. lagotto codifies that policy in Go
(`pkg/runtimeiam`) as the single source of truth, the same way it owns its tables —
so adding a permission is a code change, not a template edit.

**Idle teardown (#325, for #326).** The policy also lets the poller remove its own
region once nothing it manages is active, each grant on lagotto's exact,
fixed-name resource (any region): delete and read the tags of the three tables;
read, write, create and delete the shared lease table `spore-host-lifecycle`
(libs#77); empty and delete the artifact bucket `lagotto-lambda-<account>-*`;
delete the `lagotto-capacity-alerts` topic; delete every schedule in the
`lagotto` group and the group; create and delete one-shot schedules in a
`lagotto-teardown` group (passing only the invoke role, to the Scheduler only).
**Nothing account-wide**: no grant lets the poller delete or change a role, an
instance profile or a policy (lagotto#340); the CLI removes those (see
[The account-wide resources](#the-account-wide-resources-one-actor-the-cli)).
The one-shot schedules run as the invoke role, whose second inline policy,
`LagottoTeardown` (written by both `setup` and `deploy`), lets them delete the
poller function and its log group, and nothing else.

To plan that teardown the poller also reads its region's footprint (#326),
read-only and again on lagotto's own resources: its function and its tags
(`lambda:ListTags`: GetFunction returns them only with it), the tags of its
topic, log group and two schedule groups, the legacy stack `lagotto`, the
artifact buckets' tags, the poller role and `lagotto-instance-role` with its
profile; plus, on `*` because they have no resource-level permissions,
`logs:DescribeLogGroups`, `s3:ListAllMyBuckets`, `sagemaker:Search` (the
training jobs running for a watch, part of the idle verdict) and
`scheduler:ListSchedules`. It does not read the authorized instance roles, the
invoke role or `lagotto-cli-user`, which it leaves to `lagotto teardown`.

**Report mode.** In this release the poller's idle teardown only reports. On
every run it judges its region: active while a watch is active, a scheduled
launch is pending or retrying, a hold watch holds a capacity reservation, or a
SageMaker job runs for a watch; any read that fails counts as active. It
records the verdict in `spore-host-lifecycle` and never touches its schedule's
cadence. Once the region has been idle for `LAGOTTO_IDLE_GRACE` (24 h unless
that variable is set on the function, as a Go duration such as `30m`), it
claims the teardown lease, re-reads the region, builds the whole teardown plan,
re-checks every step against the live account and logs it, and gives the lease
back. It deletes nothing (the one exception: a region torn down while the
run observed, below). The log lines start `idle-verdict` and
`idle-teardown REPORT`; the last one of a report is
`idle-teardown REPORT region=<r> would_delete=<n> kept=<m> failed=<f> one_shots=<k> lease=released`.
The plan names, as `phase=one-shot`, the jobs that would remove what the poller
cannot remove from inside itself: its function at +1 h 32 min and its log group
at +1 h 47 min (deleted earlier, Lambda's last log flush recreates it
untagged). Everything account-wide is listed as `phase=kept`, left for the CLI.
`lagotto deploy` does not set `LAGOTTO_IDLE_GRACE`, and a deploy that changes
the function's configuration drops a variable set by hand.

**The lease.** `lagotto deploy`, `watch` (and `watch --local`) and `launch`,
and the poller before it fires a scheduled launch, first mark lagotto active in
the region's `spore-host-lifecycle` table; the CLI's commands then mark
lagotto's global scope too (`tool#lagotto#global` in us-east-1's table,
lagotto#345), as `lagotto setup` does before it writes any IAM. While a
teardown holds either lease they wait (the CLI up to 22 minutes, past the
20 minutes a lease takes to go stale, so the lease a teardown that was killed
left is taken over), then exit non-zero, having created nothing. An
interrupted CLI (Ctrl-C, SIGTERM) gives back the lease it holds, or suspends
it, and ends its marks before it exits. `lagotto-cli-user` grants this on that table only
(`UpdateItem`, `ConditionCheckItem`, `DescribeTable`, `CreateTable`,
`TagResource`, and `DeleteItem` of the closing sentinel's key only). A
creator that runs for long (a `watch --local`) refreshes its marks every 20
minutes, so it is never taken for one that died; `setup`, which creates only
account-wide resources, marks its home region as well as the global scope.
`deploy` settles the region's pending one-shot teardown jobs (lagotto#340)
on every path, once it has replaced the poller: a poller whose region was
torn down leaves its function's job due 92 minutes later, which would delete
a poller deployed in the meantime, in either mode. With the new rate schedule
in place the jobs are deleted; a deploy that failed before it (an artifact it
could not fetch) leaves them, and they finish the region's teardown, so a
failed deploy never leaves a function with no schedule and no job. A job
that fired during the deploy (and so removed the new function) makes the
deploy fail: deploy again.
`watch` and `launch` need the hosted poller: once its rate schedule is gone
(the region was torn down, its function due for deletion) they are refused,
with nothing created, and told to redeploy (lagotto#349); otherwise they
delete the region's pending jobs too. `watch --local` needs no hosted poller,
and leaves them.
`lagotto-cli-user` grants `scheduler:DeleteSchedule` on the
`lagotto-teardown` group's schedules only. A caller whose policy predates
either grant gets a warning and carries on: re-run `lagotto setup`.

Both roles also carry two explicit Denies, which no Allow can outweigh: every
action on a resource tagged `spore-host:role=service` (spore.host's own live
service; lagotto never writes that tag), and every IAM write on the
permissions boundary `spore-host/spawn-instance-boundary`.

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
  arn:aws:scheduler:*:<account>:schedule/lagotto/lagotto-capacity-poller
  arn:aws:scheduler:*:<account>:schedule/lagotto/lagotto-launch-*
  arn:aws:scheduler:*:<account>:schedule/default/lagotto-capacity-poller
  arn:aws:scheduler:*:<account>:schedule/default/lagotto-launch-*
  ```

  Each schedule is granted in lagotto's schedule group and in `default`, where
  everything created before lagotto#324 is (see [Schedule groups](#schedule-groups-upgrading)).

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
- **Owner-scoped table access.** Item actions on the three tables
  (`GetItem`, `PutItem`, `UpdateItem`, `Query`, `BatchGetItem`) only where
  `dynamodb:LeadingKeys` is under `${aws:userid}/`, with a `Null` condition so a
  request without a partition key (a Scan) is refused; no `Scan`, no
  `DeleteItem`, and `Query` on `owner_id-index` only for the caller's own id. Run
  `lagotto setup` after upgrading: it moves older records under their owners'
  ids before it writes this policy.
- **The poller-control item** (lagotto#323): `UpdateItem` on `lagotto-watches`
  only for the exact key `#lagotto/poller-control`, and only of its `epoch`
  attribute (`dynamodb:LeadingKeys` and `dynamodb:Attributes`). `watch`,
  `extend` and `update` increment it so an idle poller that is slowing its
  schedule sees the new watch. A policy written before this does not grant it:
  those commands then warn and still arm the watch, which an idle poller may
  take up to an hour to poll.
- **Not covered:** `poll`, which needs the poller's own launch permissions
  (`ec2:RunInstances` and passing instance roles), and `setup`, `deploy` and `teardown`.
- `s3://` spawn configs: name their buckets with `lagotto setup --config-bucket
  <bucket>` (repeatable). They are carried forward on later runs;
  `--revoke-config-bucket` removes one.
- A re-run writes a new default policy version only when the document changed,
  deleting the oldest non-default version once IAM's limit of five is reached.
- `lagotto doctor`'s `caller-permissions` check simulates your identity against
  the policy and lists what you lack (a warning; "unknown" if you may not call
  `iam:SimulatePrincipalPolicy`).
- `lagotto teardown --iam-roles` deletes it once nothing is attached to it: it
  lists what the policy is attached to right before the delete, refuses if
  anything is, and never detaches it from anyone.

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

This deletes the poller resources in that region, in the order schedule (in
either schedule group) → function → log group → topic → schedule group `lagotto`,
so nothing can fire into a half-deleted poller and Lambda cannot recreate the log
group once it is gone (#305). The schedule group is deleted only when nothing is
left in it: a pending per-launch schedule keeps it until `lagotto teardown`. The log group
`/aws/lambda/lagotto-capacity-poller` is deleted whether or not it carries the
`lagotto:managed=cli` tag: its name belongs to the poller function, so an untagged
one is the group Lambda created implicitly before #305. A teardown that finds
only a leftover log group deletes it. Deleting the topic deletes its
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
  poller exists in any of them or a region cannot be checked; then it removes them
  through [the account-wide teardown](#the-account-wide-resources-one-actor-the-cli),
  under the global lease, re-checking every region before each delete.
- **Per-launch `lagotto-launch-sl-*` schedules.** Each one fires a scheduled
  launch recorded in the retained `lagotto-scheduled-launches` table, so it is
  kept with that table, and a redeployed poller fires it again. `lagotto
  teardown` deletes them together with the tables (#260): every schedule a
  record names and every `lagotto-launch-sl-*` schedule in the `lagotto` and
  `default` schedule groups, before it deletes the table that records them, and
  then the `lagotto` group once nothing is left in it. `lagotto doctor` warns about a
  per-launch schedule with no record.

### Removing everything

To leave nothing behind in an account:

1. `lagotto deploy --teardown --region <region>` in every region that runs a
   poller: the schedule, the Lambda, the topic (with its email subscriptions)
   and the lagotto-managed artifact bucket.
2. `lagotto teardown --region <region>` (add `--force` if the tables still hold
   records) in every region that has lagotto's tables: the per-launch
   `lagotto-launch-sl-*` schedules and the `lagotto` schedule group, then the
   three DynamoDB tables. It works on one region at a time. Older lagotto
   versions created tables in regions with no poller too (`watch` and `setup`,
   before #325), so check regions you never deployed to.
   It stops before deleting any table if it cannot list or delete the
   schedules. Once the tables are gone, with no poller left in the region, it
   also finishes lagotto's item in that region's `spore-host-lifecycle` table
   (left by `deploy` and `watch`), which would otherwise keep the account-wide
   resources (lagotto#340). It claims the item only as it judged it, and never
   while a creator's mark there is fresh (a `watch --local` running): it says
   so and leaves the item, for the next `lagotto teardown`.
3. `lagotto teardown --iam-roles`: the two IAM roles and their policies,
   `lagotto-instance-role` and its profile, the profiles setup made for authorized
   roles, and the `lagotto-cli-user` policy once nothing is attached to it (an
   attached one is kept and reported: detach it first if you mean to remove it).
   It refuses while a poller, a pending one-shot job or a lifecycle item in use
   exists in any enabled region. It prints what it kept and why (`Kept IAM
   policy lagotto-cli-user (in use: attached to ...)`, or `not lagotto's to
   delete` for one it did not tag).

`lagotto doctor` warns about what a partial teardown leaves: an artifact bucket
in a region with no poller (`artifact-buckets`) and a per-launch schedule that no
scheduled-launch record names. The new permissions these commands need are in
[docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md).

### Footprint and the plan-driven teardown

`lagotto footprint --all-regions` lists everything lagotto has in the account
(#325, part of [spore-host#752](https://github.com/spore-host/spore-host/issues/752))
and classifies each resource: `active` (in use, or something in use depends on
it), `residue` (lagotto's own machinery: the shared Scheduler invoke role, and a
poller log group whose function is gone), `permanent` (only what is tagged
`spore-host:permanent=true`), `user-data` and `litter` (removable now). Nothing
lagotto makes is user data: **the match history is deleted with the teardown**,
by owner decision; keep it with [`lagotto history export`](#keep-the-match-history-lagotto-history-export) first. `lagotto-cli-user` is
litter only once nothing is attached to it. A resource matched by name alone
(made before #324 tagged everything) is reported and never deleted. `-o json` is
the shared spore.host footprint schema, and doctor's orphan-bucket and
orphan-schedule checks use the same reader and rule.

`lagotto footprint --all-regions -o json > plan.json`, then `lagotto teardown
--plan plan.json` (report mode, the default) says what would be deleted:
everything not active and not depended on by something active, dependents
first, each re-checked against a fresh footprint. `--mode reap --yes` deletes it
with the same deleters as the steps above, under the teardown lease (#326) of
every region it deletes in: a deploy, watch or launch there waits for it, or,
taking a lease that went stale, stops it before its next delete. A region where
something is in use, or another teardown holds the lease, refuses the run
before anything is deleted. The plan's account-wide steps run last, through
[the account-wide teardown](#the-account-wide-resources-one-actor-the-cli)
(lagotto#346): while any region still has a poller, a pending one-shot job or a
lifecycle item in use, they are kept, and reported as such.

## Idle teardown: what lagotto removes when nothing is active

lagotto follows the spore.host rule (leave no trace,
[spore-host#752](https://github.com/spore-host/spore-host/issues/752)): once
nothing it manages has been active for a grace period, what it created is
removed. lagotto keeps nothing permanent of its own. The only things
spore.host keeps in an account while it is set up are exactly two pieces of
spawn's account setup: the permissions boundary
`spore-host/spawn-instance-boundary` and the user lifecycle policy
`spore-host/spawn-user-lifecycle`, both removed only by
`spawn setup-account --remove`. This section is the whole path, end to end. **In this release the poller only reports it; nothing is deleted**
(lagotto#328 turns deletion on).

### Who decides that a region is idle

Only the deployed poller, in its own region: it is the one component that sees
every user's watches there. On every run it judges the region **active** while
a watch is active, a scheduled launch is pending or retrying, a hold watch holds
a capacity reservation, or a SageMaker training job runs for a watch. A read
that fails counts as active. It records the verdict in the region's
`spore-host-lifecycle` table (the shared lease, libs#77), which is also where
`lagotto deploy`, `watch`, `watch --local` and `launch`, and the poller before
it fires a scheduled launch, mark lagotto active first (see
[The lease](#runtime-permissions-lagotto-setup)). The CLI never judges lagotto
idle, because it cannot see other users' watches.

A region with no poller (one where you only ran `watch --local`) is never judged
idle, so nothing removes lagotto there automatically; use `lagotto teardown
--plan` (below).

### The grace period

The region must stay idle for **24 h**. `LAGOTTO_IDLE_GRACE` on the poller
function overrides it, as a Go duration (`30m`, `72h`); an invalid or
non-positive value is ignored with a warning and 24 h is used. `lagotto deploy`
does not set the variable, and a deploy that changes the function's
configuration drops one you set by hand. Any activity restarts the clock.

### What happens once the grace has passed

Each poller removes **its own region only**. It never deletes, schedules the
deletion of, or decides about anything account-wide, takes no global lease,
and reads no other region.

1. **The lease.** The poller claims the region's teardown lease. While it holds
   it, a `deploy`, `watch` or `launch` in that region waits (the CLI up to 22
   minutes) and then exits non-zero having created nothing; a creator that
   takes over a lease gone stale stops the teardown before its next delete. A
   claim that fails with anything but a throttle is not tried again (it may
   have been applied); a lease found in the poller's own name is named as its
   own, and goes stale for the next run.
2. **The re-check.** With the lease held it reads its region's footprint again.
   If anything there is active now, or cannot be read, it gives the lease back
   and stops.
3. **The plan.** It plans with the same rules as `lagotto footprint` and
   `lagotto teardown --plan`: only a resource tagged
   `spore-host:managed-by=lagotto` with lagotto's name, never one tagged
   `spore-host:role=service` or `spore-host:permanent=true`, never a
   legacy-name or foreign one; dependents before what they depend on. Only its
   region's resources are in the plan; everything account-wide is listed as
   kept, for the CLI.
4. **The self-removal check.** Its own function and log group are re-checked
   against the delete guard first. A function that does not pass it (not
   tagged as lagotto's, or permanent) would never be removed by a job: the
   poller gives the lease back, deletes nothing and commits nothing, and says
   why; the account-wide teardown names that function as what keeps the
   account-wide resources.
5. **Direct deletes**, by the poller, in dependency order: the per-launch
   `lagotto-launch-sl-*` schedules, then the three tables
   (`lagotto-watches`, **`lagotto-match-history`**, `lagotto-scheduled-launches`,
   whatever they hold), the artifact bucket `lagotto-lambda-<account>-<region>`
   and the alerts topic `lagotto-capacity-alerts`.
6. **Self-removal jobs: the commit.** The poller never deletes its own function
   or log group from inside itself. It first deletes any job an earlier run of
   its region left that this plan does not list, then creates one-shot
   EventBridge Scheduler jobs in the schedule group `lagotto-teardown`, which run
   as the Scheduler invoke role (its `LagottoTeardown` policy allows exactly
   these deletes) and delete themselves after they run. A job already there
   (its earlier run crashed) is deleted and created again at this run's time,
   since a job keeps the time it was created with:

   | Job | At | Deletes |
   |---|---|---|
   | `lagotto-teardown-log-group` | +1 h 47 min | the log group `/aws/lambda/lagotto-capacity-poller` |
   | `lagotto-teardown-function` | +1 h 32 min | the poller function `lagotto-capacity-poller` |

   The function's job, created last, commits the poller's removal. Both are due
   after the poller's next run at its idle cadence (hourly), so a run that stops
   after creating them, but before deleting its own schedule, runs once more and
   finishes its region. A run that stops before that (a creator took the lease,
   a delete failed), and every reap run that does not commit (the region is in
   use or within its grace), deletes its region's jobs, so the poller stays.
   It does that under a lock on the region's lifecycle item (no run can claim,
   and so commit, meanwhile; taken as the item was read, which is read again
   if it changed just before), heartbeating it before each delete; and never
   while another run holds the region's live lease, or once the poller's rate
   schedule is gone (the jobs are then a commit: a run finishing, stranded
   after its commit, or finished, and this a retried or duplicate
   invocation). A run that read its region's footprint and found no job makes
   no call for it; one that stopped before reading it (the region in use, or
   within its grace) lists the region's schedules. The
   log group goes late on purpose: while the function is still flushing its
   last logs, Lambda recreates a deleted log group within seconds, untagged and
   with no retention (seen live, lagotto#326).
7. **Final deletes**, once the jobs exist: the poller's own schedule
   `lagotto-capacity-poller` and the schedule group `lagotto`.
8. The poller finishes the lease, and the region's `spore-host-lifecycle` table
   is closed if no spore.host tool's items are left in it. A lease that cannot
   be finished, or a final delete that fails after its schedule is gone, is left
   held to go stale: with no poller left to carry it on, the account-wide
   teardown finishes that dead item (below).

**A run in a region already torn down** (lagotto#350). Every teardown of a
region (this one, `lagotto teardown --plan`, `deploy --teardown`) deletes the
poller's rate schedule before it finishes the region's lifecycle item, and a
run already in flight, or a retried or duplicate invocation, carries on after
the function is deleted. So each run checks that its rate schedule is still
there before it observes, and records nothing if it is not (`observe=skipped`,
outcome `torn-down`). It checks again after observing: an observation that
landed after the teardown finished made the region's item, and the table,
again, so the run finishes that item as it read it (refused while it is in
use: a creator's fresh mark, a hold, another run's live lease) and closes
the table if no tool is left in it. It does this in report mode as well,
the one thing report mode deletes: they are the run's own writes. A run
whose own verdict is active (a watch it saw, a read that failed) leaves the
item, ACTIVE, for the account-wide teardown to judge. A run killed in
between leaves a dead item, which the account-wide teardown finishes. Each
run reads its region's schedules twice for this (each read lists lagotto's
schedule groups and the default group's `lagotto-` schedules); a reap run's
job cancel reuses the second read.

A failed step before the commit gives the lease back and nothing that depends
on it is tried; the next grace period tries again. That includes a commit that
fails after the direct deletes: the poller stays, in a region with no tables,
bucket or topic left, judges it idle, and a grace period later finishes it.

### The account-wide resources: one actor, the CLI

lagotto's account-wide resources are the poller role
`lagotto-capacity-poller-role`, the Scheduler invoke role
`lagotto-capacity-poller-scheduler-invoke`, `lagotto-instance-role` and its
instance profile, the instance profiles `lagotto setup` made for roles authorized
with `--instance-role`, and the `lagotto-cli-user` policy. Exactly one kind of
actor removes them: the CLI (lagotto#340), from its lazy last mile, from
`lagotto teardown --iam-roles`, or for the account-wide steps of `lagotto
teardown --plan --mode reap` (lagotto#346). All three run one code path
(`idle.TearDownAccountWide`):

1. **The global scope.** lagotto's global scope is the item
   `tool#lagotto#global` in us-east-1's `spore-host-lifecycle` table. Every
   creator of something account-wide (`setup`, `deploy`, `watch`, `launch`)
   marks it first (lagotto#345), after its region (setup: its home region),
   adding a mark of its own (a token and the time it was written) to the
   item's set of marks, refreshes it every 20 minutes while it runs (libs
   `RefreshMark`), and removes it when it returns (libs `MarkDone`, on a
   refusal too); its region's mark likewise. A creator's global mark waits
   for a teardown holding the global lease; its regional mark is what stops
   that teardown, at its re-check before the next delete.
   **No writer ever sets an item idle.** Whether it is in use, and since when
   it has been idle, is read from what it holds and its times (libs `InUse`
   and `IdleSince`, with a mark bound of an hour): it is in use while a
   creator's mark is younger than an hour, a hold or a live lease is on it,
   or an older lagotto left it ACTIVE (with no time to age it by); and idle
   since the latest of its last activity (a mark added or removed, a hold, an
   active observation, a release), each mark's time plus an hour, and a lease's
   last heartbeat plus 20 minutes. Overlapping creators keep it in use until
   the last one ends. A mark older than an hour is a creator that died, or
   one whose end failed: it ages out by itself.
2. **Writes only as judged.** The teardown never writes to a lifecycle item,
   global or regional, except on condition that it is still exactly as the
   teardown judged it (its epoch, state, lease, marks and last activity;
   libs#89). Once the scope is not in use and past its grace, it records it
   IDLE and then claims the global lease, each only if nothing changed since
   its read; a creator that marked in between refuses it. A creator that comes while the
   teardown holds the lease waits for it (up to 22 minutes, past the lease's
   20-minute staleness, so a killed teardown's lease is taken over): the teardown re-checks before each delete, and gives the
   lease back at once when it finds something in use.
3. **The conditions.** It may delete only while all of these hold, re-checked
   before **each** delete:
   - no region has a poller function;
   - no `lagotto-teardown` one-shot job is pending in any region (they run as
     the invoke role);
   - no region's lagotto lifecycle item is in use (a creator's fresh mark, a
     hold, a live lease, or an older lagotto's ACTIVE: `lagotto teardown
     --region` clears that); and, once the dead items are finished (below),
     no region has one at all;
   - it holds the global lease, claimed at the epoch it judged.

   A read that fails, or a footprint that is not complete, means "don't".
4. **Dead regional items.** A region's item that is not in use (its lease
   stale, its creators ended or dead), in a region with no poller function, no
   job and nothing in use left, has no poller to carry it on. The
   teardown finishes it, only as it judged it (a `watch
   --local` that marked it since keeps it, and stops the run), closes that
   region's table, and goes ahead. Anything else of lagotto's left there
   (tables, a bucket, a schedule group: a regional teardown that stopped
   partway) needs nothing account-wide; `lagotto teardown --plan` removes it. A
   function left there is a poller, and blocks; one lagotto does not remove
   (untagged, or permanent) is named as such.
5. **The deletes**, in dependency order (a profile before its role; a profile
   setup made for an authorized role before the poller role, whose policy is
   how it is found, and which is otherwise found by setup's
   `lagotto:managed=instance-profile` tag). `lagotto-cli-user` is deleted only
   while nothing is attached to it (owner decision), as listed right before
   the delete, never detaching it; attached, it stays, and does not keep the
   global item. What a run keeps it reports, with why. A re-check that finds
   something in use stops the run: nothing more is re-checked or deleted, and
   a run that had deleted something suspends its lease (the next run finishes
   it). A run that left anything of lagotto's (a delete failed, refused
   because it changed, or skipped behind one that was left) is not complete.
6. **The end.** It finishes the global lease, which deletes the global item,
   and closes us-east-1's table if no tool's item is left in it. **A run that
   has exited never makes a creator wait.** One that fails before deleting
   anything gives the lease back. One that fails partway suspends it (libs
   `Lease.Suspend`): the global item is left IDLE and marked incomplete, with
   no lease, and the next CLI run resumes it without waiting a grace period
   again; a creator in the meantime clears that mark (the scope is in use
   again). Only a run that crashed, or whose own last write failed, leaves a
   live lease, which goes stale in 20 minutes. Each run writes a lease owner of
   its own (`lagotto teardown/<random>`; a poller, one per container), and a
   claim whose answer was lost but which applied is recognised by it, and by
   the client's own random id, and given back (libs `AdoptLease`).

The global scope has a grace of its own, 24 h from when it stopped being in
use (the last creator's end, as read): the
last mile removes the resources only once the scope has been idle that long, so
a `lagotto setup` that deploys no poller is not undone by the next command. An
explicit `lagotto teardown` uses no grace, but never runs while a creator is
running. `lagotto teardown --iam-roles` runs this check, in report mode,
before it deletes anything, so a refusal leaves the account as it was.
Outside the aws partition (GovCloud, China, the ISO partitions, the European
Sovereign Cloud) there is no global scope, and the last mile's account-wide
teardown does not run. There `lagotto teardown --iam-roles` removes the
account-wide resources as it did before lagotto#340, with no lease, once no
poller is left in any region (it says so); `lagotto teardown --plan --mode
reap` does with `--i-understand-no-lease`.

**What remains after the pollers finish, and why.** Once every poller has
removed its region, **the account-wide IAM is still there, until the next
lagotto command runs** (its last mile, with a 24 h grace of its own) or until
`lagotto teardown --iam-roles`. That is deliberate. A poller that removed it
would remove what another region's poller runs as, or what its own one-shot
jobs run as (the invoke role), and what a concurrent `deploy` elsewhere just
converged: deciding that safely took every poller negotiating across regions,
which five reviews found racy. A poller sees only its own region; only the CLI
reads every region, with the caller's own credentials, and nothing it removes
is something a running poller or a pending job needs. The IAM roles and policy
cost nothing while they wait.

**In this release the last mile only reports it** (with `--verbose`, as
`debug: lastmile: account-wide REPORT ...` lines on stderr): it writes and
deletes nothing, and `SPORE_LASTMILE=reap` still reports (libs#79 and
lagotto#328 turn it on). `lagotto teardown --iam-roles` and `--plan --mode
reap` delete.

### Report mode (this release)

The poller runs steps 1 to 7 as a report: it claims the lease, re-checks,
plans, re-checks every step against the live account, logs each one, and gives
the lease back. It creates no job and deletes nothing, writes nothing
account-wide, and it never finishes the lease or closes the table, so the next
report is a grace period later. The lines start `idle-verdict` and
`idle-teardown REPORT`; each job is a `phase=one-shot` line with its `offset=`
and `at=`, each account-wide resource a `phase=kept` line (`left for the CLI`),
and the last line of a report is

```text
idle-teardown REPORT region=<r> would_delete=<n> kept=<m> failed=<f> one_shots=<k> lease=released
```

Read them in the poller's log group, `/aws/lambda/lagotto-capacity-poller`.

### What stays

The poller's teardown leaves, and logs as `phase=kept`:

- **Everything account-wide**, for the CLI (above).
- **The `lagotto-teardown` schedule group**, in each region where a teardown
  ran: it holds the jobs until they have run.
- Capacity reservations, training jobs, the legacy CloudFormation stack
  `lagotto`, and anything matched by name only (made before lagotto#324 tagged
  everything).

The `lagotto-teardown` groups are lagotto's regional **residue**. The CLI's
**lazy last mile** (libs#78) checks for it after a command succeeds, at most
every 6 h per account and within a 30 s budget, with your own credentials: once
no lagotto item is left in any region's `spore-host-lifecycle` table, it
reports each `lagotto-teardown` group as removable, along with a stale lease
and an empty lifecycle table, and then runs the account-wide teardown above
in report mode. **It only reports, and only with `--verbose`** (lines prefixed
`debug: lastmile:` on stderr). Until it reaps, remove the account-wide
resources with `lagotto teardown --iam-roles` and the groups, once their jobs
have run, with `lagotto teardown --plan`.

### Keep the match history: `lagotto history export`

**The match history is deleted with the teardown**, by owner decision: it is
not kept as user data. Keep it first:

```bash
lagotto history export --region <region> > lagotto-match-history.jsonl
lagotto history export --region <region> --out lagotto-match-history.jsonl
```

It writes every row of `lagotto-match-history` in that region, every owner's,
as JSON Lines (one object per row, every attribute, numbers with DynamoDB's
exact digits), to stdout or to a new file created with mode 0600 (an existing
file is never overwritten). The row count goes to stderr. It is read-only: one
call, `dynamodb:Scan` on the history table. `lagotto-cli-user` does not grant
it (item access is owner-scoped, and a Scan reads every owner's rows), so run it
as an administrator. To save only your own rows, use `lagotto history -o json`.
Run it in each region that has a poller or tables.

### Inspect and tear down by hand

- `lagotto footprint --all-regions` lists everything lagotto has in the
  account, with its class (`active`, `residue`, `permanent`, `user-data`,
  `litter`) and whether teardown may delete it.
- `lagotto footprint --all-regions -o json > plan.json` and then
  `lagotto teardown --plan plan.json` reports what would be deleted (the
  default `--mode report`); `--mode reap --yes` deletes it under the lease. See
  [Footprint and the plan-driven teardown](#footprint-and-the-plan-driven-teardown).

### Opting out

- **The CLI last mile:** `SPORE_LASTMILE=off` stops it running at all (it is
  report-only either way).
- **The poller's idle teardown:** there is no off switch. It needs nothing
  lagotto manages to be active for the whole grace period, so a lagotto in use
  is never torn down; to wait longer, set `LAGOTTO_IDLE_GRACE` on the function
  (and set it again after a deploy that changes the function's configuration).

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

The template's `PollerSchedule` is in the `default` schedule group and the
stack's resources carry the template's tags, not the spore.host ones
([Tags](#tags)); every lagotto command still finds a schedule in `default`.

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
