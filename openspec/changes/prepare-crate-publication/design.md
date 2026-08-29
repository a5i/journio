## Context

The journio workspace has five crates ready functionally but zero publication infrastructure: no LICENSE file (manifests declare MIT), no READMEs, missing registry metadata, no CI, no release pipeline. A proven reference implementation exists in the eregex repository (tag-driven GitHub Actions release workflow with version-consistency check). See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- A tag-driven release path from git tag `vX.Y.Z` to five crates on crates.io, in dependency order, with a version-consistency gate.
- CI that runs fmt, clippy, and the full test suite (including testcontainers-based Postgres integration tests) on every PR and push to main.
- Complete, verifiable publication metadata without new tooling dependencies.

**Non-Goals:**
- Publishing language bindings (npm/PyPI) — explicitly out of scope.
- API or dependency changes to the crates themselves.
- semver checking, docs.rs preview automation, or commit-message-driven release machinery.

## Decisions

### Release automation: hand-rolled GitHub Actions

```
  push/PR --> ci.yml --> fmt + clippy + test (testcontainers Postgres)

  tag v*.*.* --> release.yml
                    |
                    v
              [check-version]  tag == workspace version?  -- no --> fail
                    | yes
                    v
              [publish]  journio-core
                    v     journio-sqlite
                    v     journio-postgres
                    v     journio-cli, journio-admin
```

The version source of truth remains `[workspace.package].version` in the root Cargo.toml; every member inherits it via `version.workspace = true`. The check-version job extracts that value and compares it to the tag; publish only proceeds on a match. Auth uses a `CARGO_REGISTRY_TOKEN` GitHub secret.

Rationale: mirrors the eregex release.yml already proven by the same author; zero new tooling; explicit, inspectable publish ordering. (CHANGELOG and README content requirements have no architectural dimension and are handled entirely in the spec/tasks phases.)

### Metadata and licensing live in the repo, not in tooling

LICENSE (MIT), READMEs, keywords, categories, docs.rs metadata are manifest/repo files only. No separate publication config; `[package.metadata.docs.rs]` per crate is all docs.rs needs. Non-published members (bindings, examples) get `publish = false` in their manifests as an accidental-publish guard.

### Failure handling

- Tag/version mismatch: check job fails before any publish; no crates touched.
- Partial publish failure (e.g., core publishes, sqlite fails): each crate is a separate workflow step so the failure point is visible; crates.io versions are immutable and re-running the workflow republishes only the missing crates (already-published ones fail with "already exists", which the job tolerates per-crate). No auto-retry.
- Name collision on crates.io: mitigated by a pre-flight manual name-availability check before the first release — renaming after publish is impossible.
- CI testcontainer flake: GitHub runners support Docker; no special mitigation planned.

### Testing approach

Pipeline validation happens without publishing: `cargo publish --dry-run -p <crate>` and `cargo package --list` locally verify manifest completeness and packing. The version-check job is simple shell logic validated by inspection and by the first real tagged run. CI correctness is proven by the first green PR run. No new test infrastructure — CI runs the existing cargo test suite.

## Risks / Trade-offs

- [Published crates are immutable; yanking only after 72-hour window] → dry-run and name verification are the safety gate before the first tag.
- [Hand-maintained publish order can drift if workspace gains crates] → the workflow's publish steps sit next to the workspace member list; adding a crate means editing both (acceptable at this size).
- [testcontainers-based Postgres tests may be slow/flaky on CI runners] → accept initially.

## Migration Plan

All changes are additive (new files, manifest metadata fields, `publish = false` markers); nothing existing breaks and the repo reverts cleanly. Sequence: land changes on main via PR → green CI → manual crates.io name check → push tag `v0.1.0` (or a bumped version if distinguishing from pre-publication history is preferred) → confirm all five crates appear. Rollback after publishing is limited to yanking, hence the dry-run gate first.
