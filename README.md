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
