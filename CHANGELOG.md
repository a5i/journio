# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-29

First public release of the journio durable workflow orchestration engine.

### Added

- `journio-core`: workflow runtime with journaled steps, durable `Sleep`,
  `Send`/`Recv` signals, events, queues, scheduler, streams, debouncing,
  patching, the standalone `Client`, and crash recovery via replay.
- `journio-sqlite`: SQLite storage backend (`SystemDatabase` implementation).
- `journio-postgres`: Postgres and CockroachDB storage backend with
  `LISTEN`/`NOTIFY`-driven wakeup and deadpool connection pooling.
- `journio-cli`: `journio` command-line interface for inspecting and managing
  workflows in SQLite or Postgres.
- `journio-admin`: admin HTTP server exposing workflow management over axum.
- Node.js native bindings (`journio-node-native`, not published to crates.io).
- Runnable examples: SQLite demo and cross-language Postgres demo.

[0.1.0]: https://github.com/a5i/journio/releases/tag/v0.1.0
