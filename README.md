# lagotto

[![CI](https://github.com/spore-host/lagotto/actions/workflows/ci.yml/badge.svg)](https://github.com/spore-host/lagotto/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/spore-host/lagotto)](https://goreportcard.com/report/github.com/spore-host/lagotto)
[![codecov](https://codecov.io/gh/spore-host/lagotto/branch/main/graph/badge.svg)](https://codecov.io/gh/spore-host/lagotto)
[![Go Reference](https://pkg.go.dev/badge/github.com/spore-host/lagotto.svg)](https://pkg.go.dev/github.com/spore-host/lagotto)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

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

# Manage watches
lagotto list
lagotto status <watch-id>
lagotto extend <watch-id> --ttl 48h
lagotto cancel <watch-id>          # prompts for confirmation; -y/--yes to skip
lagotto history
```

## Polling

A watch only fires when something polls it. Two ways:

```bash
# Infra-free: poll in the foreground until the watch fires/expires (no Lambda,
# no CloudFormation). Keep it running — or under your own supervisor/cron.
lagotto poll --daemon --interval 5m

# One-off cycle (testing/debugging)
lagotto poll
```

`--daemon` runs the same poll loop the hosted Lambda does, so
`lagotto watch --action spawn` works hands-off in your own account with zero
extra infrastructure. The hosted, multi-tenant Lambda poller (deployed via
CloudFormation) remains the option for teams — see [DEPLOYMENT.md](DEPLOYMENT.md).

## Scheduled launches

Where `watch` fires when *capacity* appears, `lagotto launch` fires at a *time* —
once at a clock time, after a delay, or on a recurring cron. The motivating case
is launching into an [EC2 Capacity Block for ML](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/capacity-blocks.html)
at its reserved start time.

```bash
# Launch into a Capacity Block at its reserved start time (block.yaml sets
# reservation_id + capacity_block; --az matches the block's AZ)
lagotto launch --at 2026-07-01T08:00:00Z --az us-east-1a --spawn-config block.yaml

# Launch 6 hours from now
lagotto launch --after 6h --spawn-config job.yaml

# Recurring: every weekday at 09:00 UTC
lagotto launch --cron "0 9 ? * MON-FRI *" --spawn-config nightly.yaml
```

Scheduled launches are driven by EventBridge Scheduler in the hosted poller
stack, so they require `lagotto deploy` first (the schedule targets the poller
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
| `notify` | Send email/webhook/SNS notification |
| `spawn` | Auto-launch instance with config file |
| `hold` | Record availability without acting |

## Deployment

Lagotto deploys as a CloudFormation stack. See [DEPLOYMENT.md](DEPLOYMENT.md) for setup.

## Go Library

```go
import "github.com/spore-host/lagotto/pkg/watcher"
```

## License

Apache 2.0 — Copyright 2025-2026 Scott Friedman.
