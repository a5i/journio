## 1. Publishable manifests

- [x] 1.1 Add an MIT LICENSE file at the repository root and confirm every published crate manifest declares `license = "MIT"` consistent with it
- [x] 1.2 Write a root README.md introducing the journio project and a README for each published crate (`journio-core`, `journio-sqlite`, `journio-postgres`, `journio-cli`, `journio-admin`), and refresh the stale status header in `crates/journio-core/src/lib.rs` so it reflects the crates that now exist
- [x] 1.3 Complete `[package]` metadata on all five published crates — `repository`, `readme`, up to 5 `keywords`, official crates.io `categories`, `documentation`, and `[package.metadata.docs.rs]` — and rewrite each description in user-facing terms without internal porting references
- [x] 1.4 Set `publish = false` on the Node bindings and example crates, and add a CHANGELOG.md in Keep a Changelog format seeded with the first-release entry
- [x] 1.5 Verify every published crate packs cleanly by running `cargo publish --dry-run` (or `cargo package --list`) for all five crates and confirming no warnings about missing metadata

## 2. CI validation

- [x] 2.1 Add a CI workflow that runs formatting checks, clippy, and the full test suite on every pull request and push to main, with Postgres integration tests executing via testcontainers, and confirm a green run on a pull request

## 3. Tag-driven release

- [x] 3.1 Verify the five crate names are available (or owned) on crates.io and record the result; resolve any collision before proceeding
- [x] 3.2 Add a release workflow triggered by `v*.*.*` tags that verifies the tag matches `[workspace.package].version` and fails before publishing on mismatch, with the `CARGO_REGISTRY_TOKEN` secret configured in GitHub
- [x] 3.3 Implement ordered publication in the release workflow — `journio-core` first, then `journio-sqlite` and `journio-postgres`, then `journio-cli` and `journio-admin` — as separate steps that tolerate "already exists" for previously published crates, and confirm the version-mismatch path fails without publishing anything
- [ ] 3.4 Push the first release tag and confirm all five crates appear on crates.io with correct metadata, READMEs, and license
