# Changelog

All notable changes to **lagotto** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **`watcher.Snipe` supports optional multi-region fallback** (#76). The
  block-and-wait acquire loop (`SnipeOptions.Fallbacks`) can now take an ordered
  list of additional targets — each a full `SnipeTarget` with its own region, AZs,
  and launch config — tried in order within each round after the primary region's
  AZ sweep hits `InsufficientInstanceCapacity`, before backing off. GPU capacity is
  bursty and region-uneven, so the zone with capacity is often in a different
  region than the one you picked. Opt-in and off by default: cross-region is not
  free like AZ-breadth (each region needs its own AMI id, in-region launch
  artifacts, SG/subnet/IAM), so the caller supplies a complete target per region.
  A terminal failure on any target still stops immediately.
- **Shared spore.host config base.** lagotto now honors the suite-wide
  `libs/sporeconfig` settings: new persistent `--profile`, `--region`, and
  `--account` flags, the `SPORE_PROFILE`/`SPORE_REGION`/`SPORE_ACCOUNT` env vars,
  and the `[spore]` table of `~/.config/spore/config.toml`, resolved
  flag > env > file > default. Every command's AWS config now loads through a
  shared `pkg/awscfg` helper, so a suite-wide profile/region applies consistently
  (previously lagotto used the bare ambient chain with no profile concept — and
  could point at a different account than spawn when reading spawn's infra tables).
  Unset = unchanged (ambient AWS chain); `launch`/`deploy` keep their own
  `--region` which still wins for that command.

### Fixed
- **`parseDuration` now accepts `w` (weeks) and `s` (seconds)** in addition to
  d/h/m, and gives clearer errors naming the offending input and the valid units
  (#41). `1w` / `45s` now work anywhere a short duration is accepted (watch TTL,
  extend).
- **A persistently-broken watch now stops after a bounded number of polls**
  instead of retrying every cycle until its TTL (#41). Launch/hold errors are now
  classified three ways: genuine capacity failures retry uncapped (a watch may
  wait days for scarce GPUs), deterministic serialization errors (a malformed
  stored config) fail immediately, and any other unrecognized error retries but
  counts toward a per-watch cap of 10 consecutive failures — so a single blip
  never kills a watch, but a sustained bad-IAM/region fault does.
- **A matched or launched watch record is now retained for 90 days** rather than
  being deleted at its original watch-expiry TTL (#41). Its `ttl_timestamp` is
  reset to the retention window on the matched/failed transition, mirroring the
  match-history retention, so a resolved watch and its history age out together.

### Changed
- Internal: renamed the SageMaker config loader to `loadSageMakerConfig` and
  documented why it intentionally skips the EC2 spawn-config key-normalization/TTL
  defaulting — a SageMaker job is a `CreateTrainingJobInput`-shaped document with a
  different schema, validated server-side at submit (#41).

## [0.49.0] - 2026-07-15

### Added
- New leaf package **`pkg/failure`** holds the launch-failure classifier
  (`FailureKind`, `ClassifyFailure`, the capacity/terminal code sets), importing
  only `errors`/`strings`/`smithy-go` + spawn's dependency-free `launchererr`
  sentinel — **zero AWS service SDKs**. `pkg/watcher` now aliases these, so every
  existing caller is unchanged, but a stateless consumer that only wants "is this
  launch error retryable?" (e.g. a block-and-wait acquire loop) can import the
  classifier without pulling the poller's 70+ AWS-SDK dependency tree (#75).
  Requires spawn ≥ v0.75.0.
- **`watcher.Snipe(ctx, target, opts)`** — a stateless, blocking, single-target
  capacity acquire for library consumers (#73). It wraps spawn's
  `launcher.Provision` with the capacity classify + backoff-retry loop
  (`ClassifyFailure`), so an embedding consumer that just wants "acquire this
  type here, block until it lands or the deadline passes" no longer needs the
  persisted-`Watch` / DynamoDB machinery or a reimplementation of the retry
  policy. Terminal failures (bad AMI/IAM, quota, post-launch teardown) return
  immediately; capacity failures back off (capped exponential, deadline-bounded).
  Returns a `MatchResult` with the launched instance id/region/AZ. The stateful
  `pkg/watcher` poller remains the path for multi-candidate, scheduled watches.

### Security
- **Pinned the CI/release Go toolchain to 1.26.5** to clear GO-2026-5856, a
  `crypto/tls` standard-library advisory present in go1.26.4. Builds now link the
  patched stdlib and govulncheck is green.
- **Pinned all GitHub Actions to commit SHAs** (with version comments) across
  the CI/security/release workflows, and pinned `trivy-action` from `@master`
  to a release. Clears the Semgrep `github-actions-mutable-action-tag` finding
  and hardens the CI supply chain.

## [0.48.1] - 2026-06-24

### Fixed
- **`lagotto deploy` no longer rolls back with a CloudFormation circular
  dependency** (#67). The v0.48.0 (#62) additions made the poller Lambda (and its
  auto-generated role) reference `SchedulerInvokeRole` (via the
  `SCHEDULER_INVOKE_ROLE_ARN` env var and the `iam:PassRole` grant), while that
  role still referenced the function by `!GetAtt` — closing a cycle among
  `[CapacityPollerFunction, CapacityPollerFunctionRole, SchedulerInvokeRole,
  CapacityPollerSchedule]` so the stack couldn't create in a clean account.
  `SchedulerInvokeRole` now references the poller by its **constructed ARN**
  (`!Sub`, the function name is fixed) instead of `!GetAtt`, breaking the cycle.
  Unblocks the hosted/autonomous poller (`lagotto deploy`).

## [0.48.0] - 2026-06-24

### Added
- **`lagotto launch --reservation-id cr-… --at-reservation-start`** fires a launch
  reliably at a Capacity Block for ML's window open (#62). It derives the start
  time from the reservation itself (no transcription), fires a touch early
  (`--fire-early`, default 2m) so EventBridge latency doesn't burn paid GPU time,
  and **retries through the boundary** (`--retry-interval`, default 30s) until the
  instance is running — absorbing the transient `InsufficientInstanceCapacity` /
  not-yet-active conditions right at the open. It gates on the reservation state
  (a dead/expired/payment-failed reservation fails fast; payment-pending retries),
  pins the launch to the reservation's AZ, and bounds the retry to end ~1h before
  the block closes (all Capacity Blocks end 11:30 UTC). Since EventBridge one-shots
  don't retry themselves, the hosted poller **self-reschedules** a fresh
  tight-interval schedule per attempt until success or the deadline. Requires
  `lagotto deploy`; bumps the spawn dependency to v0.65.0.

## [0.47.2] - 2026-06-24

### Security
- `cancel`, `extend`, `status`, and `history --watch-id` now authorize the caller
  as the watch's owner (#41). Watches are addressable by a guessable ID and the
  tables can be shared across an account, so previously anyone could cancel,
  extend, or read any watch by ID. The caller ARN is compared to the watch's
  recorded owner; a mismatch returns the same "not found" as a missing watch (no
  existence oracle).
- Closed an SSRF hole in webhook delivery (#40). `ValidateWebhookURL` vetted the
  resolved IPs, but `http.Client` re-resolved the host at connect time, so a
  DNS-rebinding attacker could pass validation with a public IP and then have the
  Lambda connect to `169.254.169.254` (the metadata endpoint holding the
  function's credentials). The notifier's HTTP client now re-validates the actual
  dial IP in a custom `DialContext` and pins the connection to it. Also,
  `ValidateWebhookURL` no longer fails open when a host can't be resolved — an
  unresolvable host is now rejected.

### Fixed
- Cancelling a `--action hold` watch now releases its capacity reservation
  (#41). A held ODCR previously billed until its 30-minute auto-expiry even after
  the watch was cancelled; `cancel` now calls `CancelCapacityReservation`
  (best-effort) for the recorded reservation.
- The deployed (hosted) poller can now actually service `--action hold` and
  `--service sagemaker` watches (#39). The poller never wired a `Holder`, so
  `hold` silently degraded to notify; and the CloudFormation policy lacked the
  IAM for both paths. Wire the `Holder`, and grant the poller
  `ec2:CreateCapacityReservation` (+ cancel/describe) for holds and
  `sagemaker:CreateTrainingJob` (+ a SageMaker-scoped `iam:PassRole`) for
  SageMaker jobs. (`--action spawn` IAM was already granted in #51.)
- **A post-launch failure no longer makes the AZ sweep launch (and orphan) an
  instance per AZ** (spawn#220). When a `--action spawn` launch succeeded at
  `RunInstances` but a downstream step failed, spawn now tears the instance down
  and reports it as a post-launch failure; lagotto's per-AZ retry (`launchAcrossAZs`)
  now classifies that as **terminal** and stops, instead of marching to the next
  AZ and launching another instance (each leaking an instance + ephemeral FSx).
  Bumps the spawn dependency to v0.63.1 (which adds the `ErrPostLaunch` sentinel
  and the instance-teardown-on-post-launch-failure fix).

## [0.47.1] - 2026-06-17

### Fixed
- **`lagotto deploy` no longer fails when the DynamoDB tables already exist** from
  prior CLI use (#59). The CLI auto-creates the tables on first `watch`/`launch`
  (#12) and the stack used to *also* create them, so the natural watch-then-deploy
  flow collided on `AlreadyExists` and rolled the whole stack back. The tables are
  now unambiguously **CLI-owned**: `lagotto deploy` ensures they exist, and the
  stack only *references* them by name (env vars + IAM) — it never creates or
  deletes them. The poller's IAM now also covers the **scheduled-launches** table
  (#49), which was previously missing (silent `AccessDenied` on scheduled launches
  from the hosted poller).
- **`lagotto deploy` recovers from a wedged stack**: a previous deploy left in a
  `ROLLBACK_COMPLETE` (or other failed-create) state — which can never be updated —
  is now deleted and recreated automatically instead of failing every retry (#59).

### Changed
- The hosted poller no longer auto-deletes the DynamoDB tables when they idle to
  empty unless `AUTO_DELETE_TABLES=true` (#59). A deployed poller is deliberate,
  persistent infrastructure that references the CLI-owned tables, so it must not
  delete them out from under itself; tear down explicitly with `lagotto deploy
  --teardown` or `lagotto teardown`. (The infra-free `poll --daemon` path is
  unaffected — it never deleted tables.) `lagotto deploy --teardown` now states
  that your data tables are retained.

## [0.47.0] - 2026-06-17

### Added
- **`poll --daemon` can be scoped to your own watches** in a shared account
  (#47): `--project NAME` (or `$LAGOTTO_PROJECT`), `--mine` (only watches you
  created), or `--watch w-aaa,w-bbb`. Previously a local daemon polled and acted
  on *every* watch in the account, so one operator's daemon could launch another
  project's `--action spawn` instance. A scoped daemon also exits when *its*
  watches are done, not the whole account's. `lagotto watch --project NAME` tags
  a watch for this scoping (also from `$LAGOTTO_PROJECT`).

### Fixed
- **Two pollers can no longer double-fire the same watch** (#47): before acting
  on a match, a poller now claims a short processing lease, so a second daemon —
  or a local daemon racing the hosted Lambda — skips a watch already being
  handled instead of duplicating the `RunInstances`/FSx-create. A crashed
  poller's lease ages out (it never blocks the watch). Disable with
  `poll --no-lease`. The hosted poller leases too.

## [0.46.0] - 2026-06-17

### Added
- **`lagotto launch`** schedules a future or recurring instance launch — fire once
  at a clock time (`--at`), after a delay (`--after 6h`), or on a recurring cron
  (`--cron`) — as opposed to `watch`, which fires on *capacity* appearing (#49).
  The motivating case is launching into an EC2 Capacity Block for ML at its
  reserved start time. It's driven by EventBridge Scheduler in the hosted poller
  stack, so it requires `lagotto deploy` first; the launched instance always
  carries a TTL (#38). One-shots self-delete their schedule after firing.
- **Overlap policy** for scheduled launches: when an instance with the same `Name`
  tag already exists at fire time, `--if-exists` decides what happens — `skip`
  (don't double-launch; the default for `--at`/`--after`, so a Capacity Block
  can't double-book), `launch` (launch anyway; the default for `--cron`, so each
  fire is a fresh box), or `replace` (terminate the existing instance, then
  launch). `--name` overrides the dedup key (defaults to the spawn config's name).

### Changed
- The hosted poller's self-teardown now reference-counts **pending scheduled
  launches** alongside active watches: the poller schedule and CLI-managed tables
  are only torn down when there are neither active watches nor pending scheduled
  launches, so a `lagotto launch --at next-week` can't have its infrastructure
  removed out from under it (#49).

## [0.45.0] - 2026-06-17

### Added
- A watch's `--spawn-config` can now target an **EC2 Capacity Reservation /
  Capacity Block for ML**: `reservation_id` launches into an existing reservation
  and `capacity_block: true` consumes a Capacity Block (forwarded to spawn's
  `--reservation-id`/`--capacity-block`, spawn#216). Bumps the spawn dependency to
  v0.62.0. Groundwork for scheduling a launch into a block at its start time (#49).
- **`lagotto deploy`** stands up the hosted capacity-poller stack (DynamoDB, SNS,
  Lambda, EventBridge Scheduler) **in your own AWS account** (#48), so watches are
  serviced server-side — armed once, then hands-off — instead of relying on a
  foreground `poll --daemon` that dies when your laptop sleeps. It downloads the
  published poller Lambda artifact for `--version`, uploads it to a bucket in your
  account (auto-named/created, or `--lambda-bucket`), and deploys the embedded
  CloudFormation template; the poller schedule deploys disabled and the first
  `lagotto watch` arms it. `lagotto deploy --teardown` deletes the stack. Both
  prompt for confirmation (`--yes` to skip).

## [0.44.0] - 2026-06-17

### Added
- The capacity-poller Lambda is now published as a release asset
  (`capacity-poller_lambda_linux_arm64.zip`) attached to each GitHub Release (#29).
  Previously the zip lived only in the `spore-host-infra` S3 bucket, so an end
  user couldn't obtain it — groundwork for a future `lagotto deploy` that stands
  up the poller stack in your own account (#48).

### Fixed
- **The hosted capacity-poller can now actually launch `--action spawn` watches.**
  Its Lambda execution role had Describe-only EC2 permissions, so a deployed
  `--action spawn` watch matched capacity but **silently failed to launch** (no
  `ec2:RunInstances` / `iam:PassRole`). The CloudFormation stack now grants the
  launch permissions spawn's headless launcher needs — `RunInstances`,
  `CreateTags`, security-group create/authorize, the AMI/VPC/subnet/keypair
  describe reads + `ssm:GetParameter`, and scoped `spored*` role/instance-profile
  management with `iam:PassRole` (conditioned to `ec2.amazonaws.com`). Redeploy
  the stack to apply.
- **lagotto now guarantees every spawned instance carries a TTL** (#38). The
  `--spawn-config` `ttl` field had no default or validation, so an `--action
  spawn` watch whose config omitted `ttl` could launch an instance with **no
  death clock** — running unbounded and billing forever, contrary to the
  "everything dies" invariant. lagotto now defaults a missing instance TTL to
  `24h` (matching the watch's own `--ttl` default) and rejects a malformed one at
  watch-create; the spawner also enforces a non-empty TTL as a hard floor at
  launch time, so no path (CLI daemon, hosted poller, or configs written by an
  older CLI) can launch a TTL-less instance.

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

[Unreleased]: https://github.com/spore-host/lagotto/compare/v0.49.0...HEAD
[0.49.0]: https://github.com/spore-host/lagotto/compare/v0.48.1...v0.49.0
[0.48.1]: https://github.com/spore-host/lagotto/compare/v0.48.0...v0.48.1
[0.48.0]: https://github.com/spore-host/lagotto/compare/v0.47.2...v0.48.0
[0.47.2]: https://github.com/spore-host/lagotto/compare/v0.47.1...v0.47.2
[0.47.1]: https://github.com/spore-host/lagotto/compare/v0.47.0...v0.47.1
[0.47.0]: https://github.com/spore-host/lagotto/compare/v0.46.0...v0.47.0
[0.46.0]: https://github.com/spore-host/lagotto/compare/v0.45.0...v0.46.0
[0.45.0]: https://github.com/spore-host/lagotto/compare/v0.44.0...v0.45.0
[0.44.0]: https://github.com/spore-host/lagotto/compare/v0.43.0...v0.44.0
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
