#!/usr/bin/env bash
#
# Release guard: prove the binary about to be published reports the tag it
# was built from.
#
# Why this exists. lagotto's `cmd.Version` defaults to "dev" (not a plausible
# release number), so this repo doesn't have the drift bug spawn found in
# itself (spawn's default said "0.38.1" while the real release was v0.97.0 —
# see spore-host/spawn#483 and pkg/buildinfo there). What lagotto DOES share
# is the other half of that bug class: nothing verifies the -X ldflag wiring
# still reaches the variable it names. `-X pkg.Var=value` is accepted
# silently by the Go linker even when `pkg.Var` doesn't exist — a renamed
# variable, a moved package, or a renamed module path would build fine,
# release fine, and publish a binary that reports "dev" forever, with no
# signal anywhere until a user pastes `lagotto version` into a bug report.
#
# This script closes that gap: it reads the real ldflag out of
# .goreleaser.yaml (not a restated copy, so drift between the two is
# impossible), builds with it substituting the real tag for
# {{.Version}}, and asks the binary what it thinks it is.
#
# Usage: check-release-version.sh <tag>     e.g. check-release-version.sh v0.54.0
set -euo pipefail

tag="${1:-}"
if [ -z "$tag" ]; then
  echo "usage: $0 <tag>   (e.g. v0.54.0)" >&2
  exit 2
fi

# The tag is vX.Y.Z; the version the binary reports is X.Y.Z. GoReleaser's
# {{.Version}} is the tag without the leading "v", and lagotto's version
# command prints whatever cmd.Version holds verbatim (no "v" trimming of its
# own), so the ldflag must supply the un-prefixed form — exactly what
# {{.Version}} already is.
want="${tag#v}"

# Read the ldflag out of .goreleaser.yaml rather than restating it, so this
# check exercises the real wiring instead of a copy that could drift
# alongside it. A `builds:` entry that loses its -X line makes the grep come
# back empty and fails below.
version_ldflag=$(grep -oE '\-X [^ ]*lagotto/cmd\.Version=\{\{\.Version\}\}' .goreleaser.yaml || true)

fail=0
note() { echo "::error::$*" >&2; fail=1; }

if [ -z "$version_ldflag" ]; then
  note ".goreleaser.yaml has no '-X .../lagotto/cmd.Version={{.Version}}' ldflag — released lagotto binaries would report 'dev'"
fi
[ "$fail" -eq 0 ] || exit 1

# Build with exactly the ldflag the release will use, substituting the real
# tag for {{.Version}}, and ask the binary what it thinks it is. This is the
# part a static check can't do: it catches a ldflag whose variable name no
# longer resolves, which the Go linker accepts silently (it does not error on
# an -X for a symbol that doesn't exist).
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Building lagotto with the release ldflags for ${tag}..."
go build -ldflags "${version_ldflag//\{\{.Version\}\}/$want}" -o "$tmp/lagotto" .

# `lagotto version` prints "Version:    X.Y.Z"; take that field.
got=$("$tmp/lagotto" version | awk '/^Version:/ {print $2}')

if [ "$got" != "$want" ]; then
  note "lagotto reports version '$got' but the tag is '$tag' (expected '$want') — the -X ldflag is not reaching the version the binary prints"
else
  echo "✅ lagotto reports ${got} for tag ${tag}"
fi

[ "$fail" -eq 0 ] || exit 1

# A release must not carry an unreleased-version placeholder in the changelog
# either: the tag is the moment [Unreleased] becomes [X.Y.Z] (see CLAUDE.md),
# and publishing with the section unpromoted means the release notes for
# X.Y.Z say "Unreleased" forever.
if [ -f CHANGELOG.md ] && ! grep -qF "## [${want}]" CHANGELOG.md; then
  note "CHANGELOG.md has no '## [${want}]' section — promote [Unreleased] to [${want}] before tagging"
fi

[ "$fail" -eq 0 ] || exit 1
echo "✅ Release version check passed for ${tag}"
