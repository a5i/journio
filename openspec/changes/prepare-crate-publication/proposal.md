## Why

The journio workspace builds and tests, but none of its crates can be sensibly published to crates.io: there is no LICENSE file despite manifests declaring MIT, no READMEs, missing publication metadata (`repository`, `readme`, `keywords`, `categories`, docs.rs settings), no CI, and no release pipeline. Publishing now would ship crates that look abandoned on arrival and have no automated path from a git tag to the registry.

## What Changes

- Add an MIT LICENSE file at the repository root; manifests already declare `license = "MIT"`.
- Write a root README.md and per-crate READMEs (at minimum for `journio-core`, which is what crates.io/docs.rs will display).
- Complete `[package]` metadata on all five published crates: `repository`, `readme`, `keywords` (max 5), `categories` (from crates.io's official list), `documentation`, and `[package.metadata.docs.rs]`.
- Rewrite internal-facing crate descriptions ("port of journio/system_database.go") into user-facing ones describing what each crate does for a stranger.
- Refresh the stale status header in `crates/journio-core/src/lib.rs` (it claims CLI/admin are "remaining" while those crates already exist).
- Set `publish = false` on workspace members not going to crates.io (Node bindings, example crates) so they can never be published accidentally.
- Add `.github/workflows/ci.yml`: fmt, clippy, tests. Postgres integration tests run via testcontainers, matching the `testcontainers-modules` dev-dependency already used by `journio-postgres`.
- Add `.github/workflows/release.yml`: triggered by `v*.*.*` tags, verifies tag/workspace version consistency, publishes all five crates to crates.io in dependency order (core → sqlite/postgres → cli/admin), modeled on the eregex release workflow.
- Add a CHANGELOG.md (Keep a Changelog format) seeded with the first release entry.
- Verify all five crate names (`journio-core`, `journio-sqlite`, `journio-postgres`, `journio-cli`, `journio-admin`) are available on crates.io before publishing.

Non-goals: no npm/PyPI publishing of language bindings, no API or dependency changes to the crates, no semver checking or docs.rs preview automation.

## Capabilities

### New Capabilities
- `crate-publication`: Requirements for publishing the journio workspace crates to crates.io — licensing, metadata, documentation surfaces, and the tag-driven CI/release pipeline that takes a version tag to published crates.

### Modified Capabilities

(none — no existing spec-level behavior changes; this change adds publication infrastructure only)

## Impact

- All five crate manifests under `crates/` (metadata additions; no dependency or API changes).
- Repository root: new LICENSE, README.md, CHANGELOG.md.
- New `.github/workflows/` CI and release workflows; requires a `CARGO_REGISTRY_TOKEN` secret in GitHub.
- `bindings/nodejs/native` and `examples/*` manifests gain `publish = false`.
- One doc-comment edit in `crates/journio-core/src/lib.rs`.
- Publication must happen in dependency order (see What Changes).
