# Changelog

All notable changes to **lagotto** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.43.0] - 2026-06-17

### Changed
- Bumped the `spawn` dependency to **v0.61.0**, which carries the FSx hardening
  arc for `--action spawn` watches that auto-create ephemeral FSx (#45): `--az` is
  honored when placing the filesystem (spawn#208), a capacity-failed launch no
  longer orphans the FSx (spawn#210), and — the key one for lagotto's
  retry-on-capacity loop — the ephemeral FSx is now created **only after**
  `RunInstances` succeeds (spawn#213), so a multi-AZ capacity-fail poll creates
  **zero** filesystems instead of one-per-AZ-attempt-per-poll.

### Fixed
- The `--action spawn` AZ retry sweep is now covered by tests (#45): a capacity
  failure tries every candidate AZ in preference order within a poll, stops at the
  first AZ that launches, and a terminal error (bad config/IAM) stops the sweep
  immediately. This is the lagotto-side guard that, paired with spawn#213, keeps a
  capacity-scarce FSx watch from churning filesystems.

### Added
- A watch's `--spawn-config` can now **auto-create ephemeral FSx Lustre storage**
  for the launched instance (#43): `fsx_create: true`, `fsx_lifecycle: ephemeral`,
  `fsx_s3_bucket`, `fsx_import_path`, `fsx_export_path`, `fsx_mount_point`,
  `fsx_storage_capacity`. The filesystem is created asynchronously and mounted by
  spored once AVAILABLE, so the poller never blocks (spawn#194/#202), and it's
  reaped when the instance terminates (spawn#192). Only `ephemeral` is supported
  here — durable storage must be pre-created and mounted via `fsxid`. Requires
  spawn ≥ 0.57.0. See spawn's `docs/durable-storage-fsx.md`.

### Security
- Semgrep SAST is now **enforcing** in CI (`--config=auto --error`) rather than
  report-only (#368). The scan was already clean — no findings to triage.

## [0.41.0] - 2026-06-14

### Added
- `lagotto watch --azs us-west-2b,us-west-2c` pins/orders the availability zones
  eligible within the watched region(s). Empty = all AZs. Widening across AZs is
  free (same-region data locality), so by design all eligible AZs are tried each
  poll (#34).

### Changed
- On an `--action spawn` match, lagotto now tries **every** offered AZ (in
  preference order) within a poll, retrying the next on `InsufficientInstance
  Capacity`, instead of attempting only one AZ and giving up until the next poll.
  For capacity-scarce types this materially shortens the wait at zero extra cost,
  since each AZ is an independent capacity pool reachable with no cross-AZ data
  charge. A terminal failure (bad AMI/IAM/quota) still stops immediately (#34).

### Documentation
- `--regions` help now notes that widening across regions can break data
  co-location (cross-region egress) and to prefer `--azs` within the data's
  region first (#34).

## [0.40.0] - 2026-06-13

### Added
- `lagotto poll --daemon [--interval 5m]`: a built-in foreground polling loop, so
  `watch --action spawn` works hands-off in your own account with **no Lambda /
  EventBridge / CloudFormation** (#30). It runs the same poll path as the hosted
  Lambda, polls immediately then every interval, and exits cleanly when no active
  watches remain or on Ctrl-C / SIGTERM. The hosted multi-tenant Lambda poller
  stays as the team option. Addresses the "nothing polls the watch in an
  end-user account" gap (#29).

## [0.39.2] - 2026-06-12

### Fixed
- Bump spawn to v0.44.2, which stops sending an empty `KeyName` to
  `RunInstances` (spawn#130). This was the second blocker on `--action spawn`
  launches (after the user-data fix in 0.39.1) — with both fixed, the
  watch→launch→run flow completes a headless, SSM-only launch end to end.

## [0.39.1] - 2026-06-12

### Fixed
- Bump spawn to v0.44.1, which fixes user-data base64 encoding in
  `launcher.Provision` (spawn#127). Before this, `--action spawn` launches failed
  at `RunInstances` with "Invalid BASE64 encoding of user data" — so the
  watch→launch→run flow now actually completes its first launch.

## [0.39.0] - 2026-06-12

### Added
- `lagotto version` now reports whether a newer release is available (an
  explicit, on-demand check) (#21).

### Fixed
- `--action spawn` now launches a fully-functional spore via spawn's headless
  launcher (`launcher.Provision`): the AMI is auto-detected, the spored bootstrap
  is installed, and the workload command + on-complete/pre-stop/idle actually
  run. A new `SpawnConfigFile` maps snake_case / kebab-case / CamelCase keys (so
  `on_complete`, `pre_stop`, `idle_timeout`, `iam_policy`, `command` are no longer
  silently dropped) and validates the config at watch-creation. Bumps the spawn
  dependency to v0.41.0 (#19).

## [0.38.1] - 2026-06

### Added
- `lagotto cancel --yes/-y` to skip the confirmation prompt (non-interactive use).

## [0.38.0] - 2026-06

### Changed
- SageMaker watches launch the job directly instead of the EC2-proxy approach,
  retrying on capacity error until provisioned (#14).

## [0.37.0] - 2026-06

### Added
- Auto-create the DynamoDB tables on first use and auto-tear-them-down when empty
  (#12).

## [0.36.0 – 0.36.2] - 2026-06

The 0.36.x series after the move to the standalone repo. Highlights:

### Added
- SageMaker EC2-proxy watch with retry-until-launch-or-TTL (#7).
- Standardized `version` subcommand output.

### Fixed
- Makefile LDFLAGS module path.

## [0.35.0] - 2026-06

Initial tagged release from the standalone `spore-host/lagotto` repository.

---

Older releases are summarized in the
[GitHub Releases](https://github.com/spore-host/lagotto/releases) for this repo.

[Unreleased]: https://github.com/spore-host/lagotto/compare/v0.43.0...HEAD
[0.43.0]: https://github.com/spore-host/lagotto/compare/v0.42.0...v0.43.0
[0.42.0]: https://github.com/spore-host/lagotto/compare/v0.41.0...v0.42.0
[0.41.0]: https://github.com/spore-host/lagotto/compare/v0.40.0...v0.41.0
[0.40.0]: https://github.com/spore-host/lagotto/compare/v0.39.2...v0.40.0
[0.39.2]: https://github.com/spore-host/lagotto/compare/v0.39.1...v0.39.2
[0.39.1]: https://github.com/spore-host/lagotto/compare/v0.39.0...v0.39.1
[0.39.0]: https://github.com/spore-host/lagotto/compare/v0.38.1...v0.39.0
[0.38.1]: https://github.com/spore-host/lagotto/compare/v0.38.0...v0.38.1
[0.38.0]: https://github.com/spore-host/lagotto/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/spore-host/lagotto/compare/v0.36.2...v0.37.0
[0.35.0]: https://github.com/spore-host/lagotto/releases/tag/v0.35.0
