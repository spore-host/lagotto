<p align="center">
  <img src="docs/assets/lagotto-hero.png" alt="lagotto — watch for EC2 capacity and launch when it appears" width="820">
</p>

# lagotto

[![CI](https://github.com/spore-host/lagotto/actions/workflows/ci.yml/badge.svg)](https://github.com/spore-host/lagotto/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/spore-host/lagotto/branch/main/graph/badge.svg)](https://codecov.io/gh/spore-host/lagotto)
[![Go Reference](https://pkg.go.dev/badge/github.com/spore-host/lagotto.svg)](https://pkg.go.dev/github.com/spore-host/lagotto)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.21483804.svg)](https://doi.org/10.5281/zenodo.21483804)

Watch for EC2 instance capacity and act when it appears.

Some instance types — particularly high-demand GPU families — aren't always available. Lagotto runs as a serverless Lambda that polls for capacity on a schedule and acts when it appears.

## Installation

**macOS / Linux (Homebrew)**
```bash
brew install spore-host/tap/lagotto
```

**Windows (Scoop)**
```powershell
scoop bucket add spore-host https://github.com/spore-host/scoop-bucket
scoop install lagotto
```

**Direct download** — pre-built binaries for Linux, macOS, and Windows (amd64/arm64) on the [releases page](https://github.com/spore-host/lagotto/releases/latest).

## Quick Start

```bash
# Watch for any p5 instance and notify when available
lagotto watch "p5.*" --action notify --ttl 7d

# Watch and auto-launch when capacity appears
lagotto watch "g5.xlarge" --action spawn --spawn-config my-job.yaml

# Watch several interchangeable rungs at once — a comma-separated list matches
# ANY of the listed types (first one with capacity wins)
lagotto watch "g6.8xlarge,g6.4xlarge,g6.2xlarge,g6.xlarge" --action spawn --spawn-config my-job.yaml

# Keep hunting with no TTL until the launch actually lands (pauses unless acked)
lagotto watch "p5.48xlarge" --action spawn --spawn-config my-job.yaml \
  --until-landed --notify email:you@example.com

# Manage watches
lagotto list
lagotto status <watch-id>
lagotto extend <watch-id> --ttl 48h
lagotto ack <watch-id>             # an --until-landed watch: move its acknowledgement deadline
lagotto update <watch-id> --add-region us-west-2 --add-type g6.xlarge   # widen the search in place; --dry-run shows the diff
lagotto cancel <watch-id>          # also releases a fired hold's reservation; -y/--yes skips the prompt
lagotto history
```

`my-job.yaml` uses lagotto's own spawn-config schema. It is **not** the file
`spawn launch --config` reads (that takes only a `plugins:` list). snake_case,
kebab-case and CamelCase keys all work:

```yaml
instance_type: g5.xlarge
region: us-west-2
ttl: 24h
command: "bash /opt/run.sh"
on_complete: terminate
completion_file: /tmp/JOB_DONE   # spored watches this path for the completion signal
volume_size: 100                 # root EBS volume size, in GiB
spot_max_price: "0.90"           # cap the Spot bid; omit for on-demand-price default
iam_policy: s3:ReadWrite
tags:                             # extra EC2 tags, alongside spawn's own spawn:* tags
  project: fieldwork
  owner: buckai
```

`user_data` (inline, or `@path` to read a script from disk) / `user_data_file`
add a custom bootstrap payload appended after spored installs; `iam_role` /
`iam_policy_file` grant a pre-built role or a scoped custom policy document
instead of (or alongside) `iam_policy`'s built-in shorthands.

An unknown key is an error at `watch`/`launch` time, naming the key and the
nearest accepted one: `az:` (use `--azs`), `s3_read:`, `instance_profile:` or a
typo is rejected rather than silently ignored. The accepted keys:

<!-- spawn-config-keys:begin -->
- Core: `name`, `instance_type`, `region`, `ami`, `spot`
- Lifecycle: `ttl`, `idle_timeout`, `on_complete`, `pre_stop`, `pre_stop_timeout`, `completion_file`
- Workload: `command`, `user_data`, `user_data_file`
- IAM: `iam_policy`, `iam_role`, `iam_policy_file`, `iam_policy_document`
- Storage: `efs_id`, `fsx_id`, `fsx_create`, `fsx_lifecycle`, `fsx_s3_bucket`, `fsx_import_path`, `fsx_export_path`, `fsx_mount_point`, `fsx_storage_capacity`, `volume_size`
- Capacity: `reservation_id`, `capacity_block`, `spot_max_price`
- Other: `cost_limit`, `key_name`, `session_timeout`, `tags`
<!-- spawn-config-keys:end -->

With `fsx_create`, `fsx_s3_bucket` must be named `lagotto-fsx-<something>` (it is
created if absent): the hosted poller may create, list and tag only buckets with
that prefix, and `watch`/`launch` refuse any other name (#242). Your data can live
elsewhere: point `fsx_import_path` / `fsx_export_path` at it.

## How long a watch lives

A watch hunts until it acts, fails, is cancelled, or reaches its deadline. There
are two kinds of deadline.

**A TTL** (`--ttl`, default `24h`). The watch expires when it runs out.
`lagotto extend <watch-id> --ttl 48h` moves it. `--ttl` must be greater than
zero; to stop a watch, use `lagotto cancel`.

**Until it lands** (`--until-landed`, for `--action spawn` and `--action hold`).
The watch has no TTL and keeps hunting until the launch or hold has actually
landed: the instance is `running`, or the reservation `active`. So that it can't
run forever unnoticed, it has a rolling **acknowledgement deadline** instead, a
dead-man's switch:

```bash
lagotto watch "p5.48xlarge" --action spawn --spawn-config my-job.yaml \
  --regions us-east-1,us-west-2 --until-landed --notify email:you@example.com
lagotto ack <watch-id>        # moves the deadline a full window ahead from now
```

- The deadline is 3 × `--remind` after creation or the last `lagotto ack`.
  `--remind` defaults to `7d` (so 21 days) and can be `1h` to `30d`.
- **Reminders.** The hosted poller reminds you on the watch's `--notify`
  channels after each of the first two intervals (day 7 and day 14 by default)
  and sends a final warning 24 hours before the pause. Each reminder says what
  the watch is hunting, how the hunt is going (attempts, last error, last poll),
  when it pauses, the `lagotto ack` command, and what the watch can cost if it
  lands. Webhook and sns channels get it as JSON with `"event": "reminder"`.
  Each reminder is sent at most once, even with several pollers.
- **Pausing.** Without an ack by the deadline the watch pauses: `status` becomes
  `expired` with `expired_reason: unacknowledged`, and you get a "paused"
  notification instead of the usual expiry one. `lagotto ack` (or
  `lagotto extend`, which acts as `ack` on such a watch) resumes it. Only `ack`
  and `extend` count as an acknowledgement; `list` and `status` do not.
- **Requirements.** It needs a `--notify` channel, and asks for confirmation
  before creating the watch, showing what the watch can cost; pass `--yes` when
  not running in a terminal. It can't be combined with `--ttl`, `--maintain` or
  `--until`. `--action notify` is refused: notify fires when a type is
  *offered*, which doesn't mean a launch would succeed, so the watch would end
  on its first poll.
- **Reminders need an upgraded poller.** They are sent by the hosted poller, so
  they start only after you run `lagotto deploy` with lagotto v0.68.0 or later.
  Until then the watch still pauses safely at its deadline, without reminding
  you. `watch --until-landed`, `ack` and `status` warn when the poller in your
  region is too old (they read its `Version` tag with `lambda:GetFunction`, and
  skip the check silently without that permission).

`lagotto list` shows an until-landed watch's `EXPIRES` as
`until landed (ack in 4d)` or `paused (unacked)`; `lagotto status` shows the
deadline and the command that moves it.

### Changing a watch in place

`lagotto update <watch-id>` changes what a watch hunts for (instance types,
regions, AZs, Spot, `--max-price`), its `--notify` channels, its spawn config or
its project, without cancelling it. The watch keeps its ID, attempts, last error,
match history and confirmed email subscriptions.

```bash
lagotto update <watch-id> --add-region us-west-2 --add-type g6.xlarge --dry-run
```

- The updated watch goes through every check `lagotto watch` makes, including
  default-subnet coverage, so an update can't produce a watch `watch` would
  refuse. `--dry-run` runs the checks and prints the before/after diff without
  writing anything.
- The action, service, owner, ID and deadline can't be changed: use
  `lagotto extend` or `lagotto ack` for the deadline.
- It is refused while a poller is acting on the watch, while a launch or hold
  it made is still landing, and while a SageMaker job it submitted is in flight.
  An expired or failed watch needs `--reactivate`, and must still be before its
  deadline.
- Each update adds an `updated` row to `lagotto history` listing the changed
  fields. It is not a match.

### What a watch can cost

`lagotto watch`, `lagotto status` and the until-landed confirmation show an
`Exposure:` line, and `lagotto list` an `EXPOSURE` column:

| Action | Exposure |
|--------|----------|
| `spawn` | "if this lands: up to $X/hr (≈$Y/day)", × N for a `--maintain N` fleet |
| `hold` | "30 min reserved at up to $Y/hr (≈$Z), billed even if unused" |
| `notify` | launches and reserves nothing |
| SageMaker | unknown |

- The rate is your `--max-price` cap when you set one, labelled as your cap and
  not a quote. Otherwise it is the on-demand rate, which is the ceiling for
  spot too.
- The CLI takes the on-demand rate from the static spore.host price list, marks
  it with `*` and says when the list was collected; it makes no Price List call
  and needs no extra permission. The static list has no GPU or accelerator
  family newer than p4d, so a p5, g6e or g7e watch shows `unknown`. Reminders
  use the live AWS Price List, falling back to the static list, labelled the
  same way.
- A wildcard pattern, or a watch on several types or regions, shows the highest
  rate, labelled with the range.
- `-o json` on `watch`, `list` and `status`, and the reminder's webhook/sns JSON,
  carry an `exposure` object.

## Goal-driven fleets

By default a watch fires its action **once** and retires. A **fleet watch**
instead maintains a target number of workers until an external completion
condition is true — relaunching toward the goal each poll, **including from zero**
if the whole fleet is reclaimed:

```bash
# Keep ~4 workers alive until every manifest entry has a prepared output.
lagotto watch m8g.8xlarge --action spawn --spawn-config prep.yaml \
  --maintain 4 \
  --until 's3-empty: s3://bucket/manifest minus s3://bucket/prepared/' \
  --regions us-west-2 --spot
```

Each poll: if `--until` holds, the fleet retires (`completed`); otherwise lagotto
counts the running workers (by a `lagotto:watch=<id>` tag) and launches enough to
reach `--maintain`. This makes idempotent, spot-friendly batch fire-and-forget:
pull-model workers absorb independent reclaim, and the supervisor revives the
fleet after a correlated total loss.

`--until` conditions:
- **`s3-empty: <wanted-prefix> minus <done-prefix>`** — done when every wanted S3
  key has a corresponding done key (the durable, compute-independent completion
  state for a pull-model job).
- **`http-200: <url>`** — done when a GET returns 2xx.
- **`shell: <cmd>`** — done when the command exits 0. Runs only on a local
  `poll --daemon` (the hosted poller has no shell sandbox).

Requires `--action spawn`. Bound cost with a TTL in the spawn config; workers
still get spored's TTL/idle lifecycle.

## Polling

A watch only fires when something polls it. Two ways:

```bash
# Infra-free: poll in the foreground until the watch fires/expires (no Lambda
# at all). Keep it running — or under your own supervisor/cron.
lagotto poll --daemon --interval 5m

# One-off cycle (testing/debugging)
lagotto poll
```

`--daemon` runs the same poll loop the hosted Lambda does, so
`lagotto watch --action spawn` works hands-off in your own account with zero
extra infrastructure. The hosted Lambda poller (`lagotto deploy`) remains the
option for teams — see [DEPLOYMENT.md](DEPLOYMENT.md).

### Notifications from a local poller

`lagotto poll` and `poll --daemon` deliver a watch's `--notify` channels the way
the hosted poller does:

- **`webhook:` and `sns:` channels are always delivered.** Each names its own
  target (a URL, or your own SNS topic ARN), so no lagotto infrastructure is
  needed.
- **`email:` channels are published to the alerts topic,
  `lagotto-capacity-alerts`**, which `lagotto deploy` creates in your account and
  region. `poll` uses that topic when it exists. It checks with
  `sns:GetTopicAttributes`; if you may not call that, it uses the topic by name.
  To publish to a different topic, set `LAGOTTO_SNS_TOPIC_ARN`:

  ```bash
  LAGOTTO_SNS_TOPIC_ARN=arn:aws:sns:us-west-2:111122223333:my-alerts lagotto poll --daemon
  ```

If no topic is found, `poll` warns at start and names every in-scope watch
whose email channel will not be delivered. Each email it skips is also reported
as a failed notification. Its webhook and sns channels are still delivered.

### Email notifications

An `email:` channel reaches the address it names, and only that address:

```bash
lagotto watch p5.48xlarge --notify email:you@example.com
#   Notify:   email:you@example.com  pending confirmation: check your inbox and click the link; nothing is delivered until you do
```

- **`watch` subscribes the address** to `lagotto-capacity-alerts` in this
  account and region (so run `lagotto deploy` first; with no topic, `watch`
  refuses an email channel). AWS sends an "AWS Notification - Subscription
  Confirmation" email. **Nothing is delivered until you click its link.** An
  address that is already subscribed is reused, so a second watch sends no
  second confirmation.
- **Each address receives only its own watches' events.** Its subscription has
  a filter policy on the `lagotto_email` message attribute, and every email
  lagotto publishes (match, expiry, quota cap, retirement) sets that attribute
  to the watch's address. In a shared account, one user's watch events never
  reach another user's inbox.
- **`lagotto list` and `lagotto status` show each email channel's state**
  (confirmed, pending confirmation, not subscribed), and `lagotto doctor`
  reports pending and unsubscribed addresses, plus any email subscription on
  the topic with no filter policy (one made by hand receives every message).
- **The subscription is removed when no active watch names the address.** The
  poller does it at the start of its next sweep after the last such watch
  matches, expires, fails or completes, so the final email is sent first.
  `lagotto cancel` does it at once for the cancelled watch, and
  `lagotto notify-gc` does it on demand. Only subscriptions lagotto created are
  removed. A subscription still pending confirmation cannot be removed; AWS
  deletes it within 3 days. `lagotto extend` re-subscribes the addresses of a
  watch it reactivates, and `lagotto notify-gc --resubscribe` re-subscribes an
  active watch's address whose confirmation lapsed.
- **After upgrading, re-run `lagotto setup`**: the hosted poller needs
  `sns:ListSubscriptionsByTopic`, `sns:GetSubscriptionAttributes` and
  `sns:Unsubscribe` on the alerts topic for the cleanup. `lagotto doctor`
  reports the missing grants until it is done.

### Scoping a daemon in a shared account

By default `poll --daemon` services **every** active watch in the account. In an
account shared across projects/people, scope it to your own watches so it doesn't
drive (and launch) someone else's (#47):

```bash
lagotto watch "g5.12xlarge" --action spawn --spawn-config job.yaml --project fieldwork
lagotto poll --daemon --project fieldwork   # only fieldwork's watches
lagotto poll --daemon --mine                # only watches you created
lagotto poll --daemon --watch w-aaa,w-bbb   # only these watch IDs
```

`--project` defaults to `$LAGOTTO_PROJECT` (on both `watch` and `poll`). A scoped
daemon exits when *its* watches are done, not the whole account's.

Before acting on a match, a poller claims a short **processing lease** on the
watch, so two daemons — or a local daemon racing the hosted Lambda — can't both
launch the same watch. A crashed poller's lease ages out automatically. Disable
with `--no-lease` (not recommended when more than one poller runs).

## Scheduled launches

Where `watch` fires when *capacity* appears, `lagotto launch` fires at a *time* —
once at a clock time, after a delay, or on a recurring cron. The motivating case
is launching into an [EC2 Capacity Block for ML](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/capacity-blocks.html)
at its reserved start time.

```bash
# Launch into a Capacity Block reliably at its window open — derives the start
# time from the reservation, fires slightly early, and retries through the
# boundary until the instance runs. --reservation-id sets the launch's
# reservation_id, capacity_block, instance type and AZ from the reservation.
lagotto launch --reservation-id cr-0abc123 --at-reservation-start --spawn-config block.yaml

# Launch at an explicit clock time
lagotto launch --at 2026-07-01T08:00:00Z --az us-east-1a --spawn-config block.yaml

# Launch 6 hours from now
lagotto launch --after 6h --spawn-config job.yaml

# Recurring: every weekday at 09:00 UTC
lagotto launch --cron "0 9 ? * MON-FRI *" --spawn-config nightly.yaml
```

For a Capacity Block, prefer **`--reservation-id … --at-reservation-start`** over a
hand-typed `--at`: you've paid up front for a non-refundable window that ends at a
fixed 11:30 UTC, so every minute of late start is wasted. This mode derives the
open time from the reservation, fires `--fire-early` (default 2m) ahead so
EventBridge latency doesn't cost you, and retries on `--retry-interval` (default
30s) through the open — absorbing the transient capacity / not-yet-active blips at
the boundary — until an instance is running, then stops.

Scheduled launches are driven by EventBridge Scheduler against the hosted
poller, so they require `lagotto deploy` first (the schedule targets the poller
Lambda in your account). The launched instance always carries a TTL (#38), and a
one-shot's schedule self-deletes after it fires.

**Overlap policy.** If an instance with the same `Name` tag already exists when a
schedule fires, `--if-exists` decides what happens:

| `--if-exists` | Behavior | Default for |
|---------------|----------|-------------|
| `skip` | Don't launch; treat the existing instance as the fulfillment | `--at` / `--after` (a Capacity Block must not double-book) |
| `launch` | Launch anyway — each fire is a fresh box | `--cron` |
| `replace` | Terminate the existing instance, then launch | — |

The dedup key is the instance `Name` tag (`--name`, or the spawn config's `name`).

## Actions

| Action | Description |
|--------|-------------|
| `notify` | Signal only — send an email/webhook/SNS notification; nothing is launched or reserved |
| `spawn` | Launch an instance from a config file (`RunInstances`) — this is the launchability test |
| `hold` | Reserve capacity via an On-Demand Capacity Reservation (`CreateCapacityReservation`) — billable once held; stricter than launchability |

The three actions differ in what they *do* when a match appears:

- **`notify`** signals only — it never launches or reserves anything. It fires
  when the type is *offered* in a region or AZ, which is not a sign that a
  launch would succeed: for a scarce type that is offered everywhere (p5, say)
  it can fire on the first poll. Use `spawn` or `hold` to wait for real capacity.
- **`spawn`** launches an instance via `RunInstances`. Because the launch is the
  actual capacity test, `--action spawn` is the way to check whether a type will
  really launch right now.
- **`hold`** creates a targeted On-Demand Capacity Reservation
  (`CreateCapacityReservation`, 30-minute window) so the capacity is yours to
  launch into. It **acts**: a successful reservation is **billable from the
  moment it's held**, even before you launch. It is **not** a lightweight
  "is capacity available?" probe — capacity-reservation admission is **stricter
  than launchability**, so `hold` can return `InsufficientInstanceCapacity` and
  retry indefinitely for a type that `--action spawn` (`RunInstances`) would
  launch immediately. Use `spawn` to test launchability; use `hold` only when you
  specifically want reserved capacity.

  Once a hold fires, `lagotto status <watch-id>` and every notification (email,
  SNS and webhook `reservation_id` / `reservation_availability_zone`) name the
  reservation and its AZ. Because it is *targeted*, only a launch that names it
  uses it, e.g. `spawn launch <watch-id> --region <region> --az <az> --instance-type <instance-type> --reservation-id <cr-…>`
  (spawn needs a name and doesn't take the type from the reservation; the
  email and `lagotto status` print this command filled in).
  If you won't use it, `lagotto cancel <watch-id>` releases it early so it stops
  billing. Releasing one that already expired or was already released is not an
  error. Your AWS credentials need `ec2:CancelCapacityReservation` for that;
  without it, cancel says so and the reservation still expires at the end of its
  window.

### Default subnets: where a spawn or hold watch can land

The poller launches into each region's **default VPC** without naming a subnet,
so a spawn or hold watch can only land in an AZ that has a default subnet; it
skips the others. `lagotto watch` (and `lagotto update`) therefore compares, for
each region the watch names, the AZs that offer the type (narrowed by `--azs`)
with the AZs that have a default subnet:

- **None in common:** the watch is refused, naming the AZs and the
  `aws ec2 create-default-subnet --region ... --availability-zone ...` command
  that fixes it. A watch with no `--regions` is refused only if no enabled
  region can serve it.
- **Some offered AZs lack one:** a warning per AZ, e.g. "us-west-2d offers
  p5.4xlarge but has no default subnet; this watch can't land there".
- **The lookup fails** (e.g. a missing permission): a warning, never a refusal.
  Notify and SageMaker watches are not checked.

`lagotto doctor`'s `subnet-coverage` check reports the same for every active
spawn and hold watch. A failed attempt that skipped AZs says so: its
`last_error` starts `no-subnet: <azs>; `, and `lagotto list` shows `no-subnet`
rather than a plain capacity miss. After you create a default subnet, the hosted
poller notices it within 15 minutes (once it runs lagotto v0.68.0 or later).

## Deployment

`lagotto deploy` stands the hosted poller up in your own AWS account with direct
AWS API calls — no CloudFormation, re-runnable, and a `--version` bump is a fast
code-only update. A CloudFormation template is still shipped as an optional
declarative path for IaC/enterprise environments. See
[DEPLOYMENT.md](DEPLOYMENT.md) for both.

### Permissions your own credentials need

Every `lagotto` command runs with **your** AWS credentials. The hosted poller has
its own role, whose permissions `lagotto setup` writes, but that role does not
cover the CLI. [docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md)
lists the IAM actions each command can call. It is generated from the code
(`make cli-iam-actions`), and CI fails when the two disagree. In short:

- `deploy`, `setup` and `teardown` are administrator commands: they create or
  delete IAM roles, policies and tables.
- `poll` runs the poller on your machine, so it needs what the poller's role has,
  including launching instances.
- The everyday commands need `dynamodb:*` on lagotto's three tables plus a few
  more actions, which the generated page lists per command. For example, `watch`
  and `extend` re-enable the poller's schedule (`scheduler:UpdateSchedule`, plus
  `iam:PassRole` on `lagotto-capacity-poller-scheduler-invoke`), `launch` creates
  a schedule, and `cancel` of a fired hold releases its capacity reservation.

lagotto does not yet provision a policy for CLI users (lagotto#232); until it
does, an administrator has to grant these actions. Without the Scheduler
permissions, a new watch can leave a self-disabled poller off without saying so
(lagotto#231); `lagotto doctor` reports it.

### Checking a deployment

`lagotto doctor` reports drift between the poller deployed in your account and
what your lagotto binary expects — most importantly the poller's runtime IAM
policy, which `lagotto setup` replaces wholesale with the grants compiled into
whichever binary ran it. Upgrading lagotto without re-running `setup` therefore
leaves the poller on the old permissions, and the only evidence is an
`AccessDenied` in the poller's CloudWatch Logs. `doctor` also flags a poller
Lambda older than your CLI, missing tables/roles/poller/topic/schedule, a
schedule that's switched off while watches are active, and a leftover
CloudFormation stack.

It is strictly read-only (there is no `--fix`) and exits non-zero only when a
check FAILS, so it can gate CI. `-o json` gives the machine-readable form,
including the exact missing or extra IAM grants.

### Removing it

`lagotto deploy --teardown` removes one region's poller (schedule, Lambda,
topic) and its lagotto-managed artifact bucket. `lagotto teardown` removes the
three DynamoDB tables and the per-launch `lagotto-launch-sl-*` schedules, and
`lagotto teardown --iam-roles` the two account-wide IAM roles once no region
runs a poller. [DEPLOYMENT.md](DEPLOYMENT.md#teardown) has what each one deletes
and what it leaves.

## Go Library

```go
import "github.com/spore-host/lagotto/pkg/watcher"
```

A program that creates watches itself (as spore-host-mcp does) should not copy
`cmd/` code:

- **`watcher.NewWatch(now, watcher.Lifetime{...})`** builds a new watch with a
  TTL or until-landed. It sets the ID, `CreatedAt`, `UpdatedAt`, `ExpiresAt`
  and `TTLTimestamp`, always with `TTLTimestamp` = `RetentionTTL(ExpiresAt)`
  and never the expiry itself. Fill in what to watch, then store it.
- **`github.com/spore-host/lagotto/pkg/arm`** has the steps `lagotto watch` runs
  around storing a watch: `PreflightPollSchedule`, `EnablePollScheduleIfOff`
  and `PollScheduleUpdateRefusal` turn the hosted poller's schedule back on;
  `ResolveAlertsTopic` finds the alerts topic; `SubscribeWatchEmails`,
  `SetChannelStates` and `CleanupWatchEmails` subscribe and unsubscribe email
  channels. They return warnings and results instead of printing them. The
  package documentation gives the order to call them in.
- **`github.com/spore-host/lagotto/pkg/testutil/hermetic`** is the guard that
  keeps a test from reaching real AWS. Import it only from `_test.go` files and
  `go test -exec` wrappers.

## License

Apache 2.0 — Copyright 2025-2026 Scott Friedman.
