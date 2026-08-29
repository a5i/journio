## Purpose

Defines the requirements for publishing the journio workspace's five crates to crates.io: licensing, crate metadata, documentation surfaces, and the CI/release pipeline that turns a version tag into published crates.

## ADDED Requirements

### Requirement: Repository carries the declared license
The repository SHALL include an MIT LICENSE file at the root, and every published crate manifest SHALL declare a license that matches it.

#### Scenario: License file present and consistent
- GIVEN the journio repository at any published tag
- WHEN a user or packaging tool looks for license information
- THEN an MIT LICENSE file exists at the repository root
- AND every published crate's `license` field is `MIT`

### Requirement: Published crates carry complete registry metadata
Every crate intended for publication (`journio-core`, `journio-sqlite`, `journio-postgres`, `journio-cli`, `journio-admin`) SHALL declare `repository`, `readme`, up to 5 `keywords`, `categories` from crates.io's official category list, and `documentation`, and SHALL have a description written for someone unfamiliar with the project's internal history.

#### Scenario: Metadata renders on crates.io
- GIVEN a crate published to crates.io
- WHEN a user views its crates.io page
- THEN the page shows the repository link, a README, keywords, and categories
- AND the description describes what the crate does without referencing internal porting history

### Requirement: Documentation surfaces exist
The repository SHALL provide a root README introducing the project and a README for each published crate that crates.io and docs.rs display, and crate-level documentation SHALL accurately reflect the current implementation status.

#### Scenario: Stale status header refreshed
- GIVEN `journio-core`'s crate documentation previously claimed the CLI and admin server were unimplemented
- WHEN those components exist as crates in the workspace
- THEN the crate documentation no longer claims they are missing

### Requirement: Non-published workspace members are guarded
Workspace members not intended for crates.io (language bindings and example crates) SHALL set `publish = false` so they cannot be published accidentally.

#### Scenario: Attempting to publish a guarded member fails
- GIVEN a workspace member marked `publish = false`
- WHEN someone runs `cargo publish` for that member
- THEN cargo refuses to publish it

### Requirement: CI validates every change
The repository SHALL run formatting checks, linting, and the test suite (including Postgres integration tests via testcontainers) on every pull request and push to the main branch.

#### Scenario: Pull request runs the full check suite
- GIVEN a pull request against the main branch
- WHEN CI runs
- THEN formatting, lint, and test jobs all execute, including Postgres-backed integration tests
- AND the pull request only passes when all jobs succeed

### Requirement: Tag-driven release publishes in dependency order
Pushing a tag matching `v*.*.*` SHALL trigger a release pipeline that verifies the tag matches the workspace version and, only if it matches, publishes the five crates to crates.io with `journio-core` first, then `journio-sqlite` and `journio-postgres`, then `journio-cli` and `journio-admin`.

#### Scenario: Matching tag publishes all crates
- GIVEN the workspace version is `X.Y.Z` and all tests pass
- WHEN a tag `vX.Y.Z` is pushed
- THEN the pipeline publishes all five crates to crates.io in dependency order

#### Scenario: Version mismatch aborts the release
- GIVEN the workspace version is `X.Y.Z`
- WHEN a tag `vA.B.C` with a different version is pushed
- THEN the release fails before publishing anything

### Requirement: Changes are recorded in a changelog
The repository SHALL maintain a CHANGELOG in Keep a Changelog format, seeded with an entry for the first release.

#### Scenario: First release has a changelog entry
- GIVEN the first crates.io release is being made
- WHEN a user consults the CHANGELOG
- THEN an entry describes that release's contents

### Requirement: Crate names are verified available before first publish
Before the first release, the five crate names SHALL be confirmed as available (or owned) on crates.io; a name collision SHALL block the release and be resolved before publishing.

#### Scenario: Name collision blocks publishing
- GIVEN one of the five crate names is already taken on crates.io by another owner
- WHEN the release is prepared
- THEN publishing is blocked for that crate until the name conflict is resolved
