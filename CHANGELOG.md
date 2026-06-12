# Changelog

All notable changes to **lagotto** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/spore-host/lagotto/compare/v0.38.1...HEAD
[0.38.1]: https://github.com/spore-host/lagotto/compare/v0.38.0...v0.38.1
[0.38.0]: https://github.com/spore-host/lagotto/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/spore-host/lagotto/compare/v0.36.2...v0.37.0
[0.35.0]: https://github.com/spore-host/lagotto/releases/tag/v0.35.0
