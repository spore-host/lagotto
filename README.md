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

# Manage watches
lagotto list
lagotto status <watch-id>
lagotto extend <watch-id> --ttl 48h
lagotto cancel <watch-id>          # prompts for confirmation; -y/--yes to skip
lagotto history
```

`my-job.yaml` is the same shape `spawn launch` reads, with lagotto's own
lifecycle fields layered on top (snake_case, kebab-case, and CamelCase keys all
work):

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

## Goal-driven fleets

By default a watch fires its action **once** and retires. A **fleet watch**
instead maintains a target number of workers until an external completion
condition is true — relaunching toward the goal each poll, **including from zero**
if the whole fleet is reclaimed:

```bash
lagotto watch m8g.8xlarge --action spawn --spawn-config prep.yaml \
  --maintain 4 \                                            # keep ~4 workers alive…
  --until 's3-empty: s3://bucket/manifest minus s3://bucket/prepared/' \  # …until done
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
# boundary until the instance runs (block.yaml sets reservation_id + capacity_block)
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

- **`notify`** signals only — it never launches or reserves anything.
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

## Deployment

`lagotto deploy` stands the hosted poller up in your own AWS account with direct
AWS API calls — no CloudFormation, re-runnable, and a `--version` bump is a fast
code-only update. A CloudFormation template is still shipped as an optional
declarative path for IaC/enterprise environments. See
[DEPLOYMENT.md](DEPLOYMENT.md) for both.

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

## Go Library

```go
import "github.com/spore-host/lagotto/pkg/watcher"
```

## License

Apache 2.0 — Copyright 2025-2026 Scott Friedman.
