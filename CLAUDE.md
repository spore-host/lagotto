# CLAUDE.md — lagotto

`lagotto` is the spore.host capacity watcher: it polls for EC2 capacity (via
truffle) and acts when a match appears — notify, hold, or launch via spawn. It
runs both as a CLI and as a capacity-poller Lambda. Part of the spore.host suite
([spawn](https://github.com/spore-host/spawn),
[truffle](https://github.com/spore-host/truffle), lagotto).

## Versioning & changelog (required)

This project follows **[Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html)**
and keeps a **[Keep a Changelog](https://keepachangelog.com/en/1.1.0/)**-format
`CHANGELOG.md` at the repo root.

**Every change that affects users must update `CHANGELOG.md`:**

- Add an entry under the `## [Unreleased]` section, in the right group —
  `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, or `Security`. (Use a
  `Documentation` group for docs-only changes; optional but welcome.)
- Write for humans: describe the user-visible effect, not the implementation.
  Reference the issue/PR where it helps.
- Do this in the **same PR** as the change, so the changelog never lags.

**On release:**

1. Rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and open a fresh empty
   `## [Unreleased]` above it.
2. Choose `X.Y.Z` by SemVer: **MAJOR** for breaking changes, **MINOR** for
   backward-compatible features, **PATCH** for backward-compatible fixes. (Pre-1.0,
   breaking changes bump MINOR.)
3. Update the comparison links at the bottom of the file.
4. Tag `vX.Y.Z` — that triggers the GoReleaser release workflow.

GoReleaser auto-generates the **GitHub Release notes** from commit messages;
`CHANGELOG.md` is the curated, human-facing companion and the source of truth for
"what changed." Keep both — they serve different readers.

**Note:** lagotto's `--action spawn` provisions real, billable EC2 instances via
spawn's launcher. The `spored` lifecycle daemon + TTL still apply; a watch should
carry a TTL so launched instances can't run unbounded.

## Build & test

- `make test` — unit tests
- `make lint` — linters
- `make build` — build the binary

The root module and the `lambda/capacity-poller` module are separate Go modules
(the lambda has its own `go.mod` with a `replace` to the root).
