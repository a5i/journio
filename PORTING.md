# Porting `dbos-transact-golang` -> Rust

This document is the working plan for porting
[dbos-transact-golang](../dbos-transact-golang) to Rust with Python and Node
bindings. Keep the Go source open alongside; every Rust unit carries a
`// ported from dbos/<file>.go:<line>` comment so diffs stay auditable.

## What the Go project is

DBOS Transact: durable workflow orchestration on Postgres / CockroachDB /
SQLite. About 22K LOC (about 16K non-test). The interesting files, by size:

| Go file | LOC | Rust target |
|---|---|---|
| `dbos/system_database.go` | 5,916 | `dbos-core::system_db` trait + `dbos-postgres` / `dbos-sqlite` impls |
| `dbos/workflow.go` | 5,269 | `dbos-core::workflow` + `dbos-core::context` |
| `dbos/conductor.go` | 2,052 | `dbos-conductor` |
| `dbos/dbos.go` | 958 | `dbos-core::config` + `context::DbosContext` |
| `dbos/client.go` | 879 | `dbos-core::client` (done) |
| `dbos/queue.go` | 803 | `dbos-core::queue` |
| `dbos/admin_server.go` | 618 | `dbos-admin` (axum) (done) |
| `dbos/debouncer.go` | 549 | `dbos-core::debouncer` |
| `dbos/dialect.go` | 418 | `dbos-core::dialect` (done in scaffold) |
| `dbos/scheduler.go` | 342 | `dbos-core::scheduler` |
| `dbos/serialization.go` | 373 | `dbos-core::value` (done in scaffold) |
| `cmd/dbos/*` | - | `dbos-cli` (clap) — done (version/migrate/reset/workflow/start/init/postgres) |
| `dbos/migrations/*.sql` (40) | - | embedded via `include_str!`, reused verbatim for PG |

## The 5 hard problems

1. **No reflection -> trait objects.** Go registers `Workflow[P,R]` by name via
   `reflect`. Rust uses an erased, object-safe `Workflow` / `Step` trait over
   `serde_json::Value`, plus a typed adapter (`WorkflowFn` / `StepFunc`). This
   erased trait is the seam Python/Node bind to. -> `crates/dbos-core/src/workflow.rs`
2. **Goroutines/channels -> Tokio.** Recovery loop, queue workers, scheduler
   poll, LISTEN/NOTIFY listener, conductor WS = 5+ long-running tasks. Use
   `tokio::task::JoinSet` + `CancellationToken`. Go's durable `Select` / `Go`
   become `tokio::sync::mpsc` + checkpointing: a semantic port, not a
   mechanical one.
3. **DB layer.** Mirror `dialect.go` (already done as `Dialect` trait). Use
   `tokio-postgres` + `deadpool-postgres` (need raw LISTEN/NOTIFY) for PG and
   `sqlx` (sqlite) for SQLite, both behind the `SystemDatabase` trait.
4. **Exactly-once and determinism.** Port SQL verbatim; replicate transaction
   isolation exactly (`FOR UPDATE SKIP LOCKED`, snapshot isolation choices in
   `dialect.go`).
5. **Object safety.** Use `#[async_trait]` for engine traits; the registry and
   FFI need `dyn Workflow`.

## Target workspace layout

```text
durable-wf-rust/
|- crates/
|  |- dbos-core/        present: runtime, registry, context, config, errors, dialect
|  |- dbos-postgres/    present: SystemDatabase impl, migrations, LISTEN/NOTIFY wakeups
|  |- dbos-sqlite/      present: SystemDatabase impl, migrations, examples, integration tests
|  |- dbos-admin/       present: axum HTTP server (workflow CRUD, recovery, queue metadata, global timeout, registered workflows, start, CORS)
|  |- dbos-conductor/   planned: WS client (port `dbos/conductor.go`)
|  `- dbos-cli/         present: `dbos` binary (version/migrate/reset/workflow/start/init/postgres)
|- examples/
|  `- sqlite-demo/      present: interactive demo (SQLite + admin API + seeded workflows)
|- ui/
|  `- (Nuxt 3 app)      present: DBOS Console dashboard (Vue 3 + Tailwind)
|- bindings/
|  |- python/           planned: PyO3 + maturin wheel
|  `- nodejs/           planned: napi-rs package
`- migrations/          embedded in backend crates; PG SQL copied verbatim from Go repo
```

## Dependency map

| Go | Rust | Notes |
|---|---|---|
| `jackc/pgx/v5` | `tokio-postgres` + `deadpool-postgres` | raw LISTEN/NOTIFY access |
| `modernc.org/sqlite` | `sqlx` (sqlite) | async + compile-time checks |
| `gorilla/websocket` | `tokio-tungstenite` | |
| `robfig/cron/v3` | `cron` + `tokio` task | or `tokio-cron-scheduler` |
| `spf13/cobra` | `clap` (derive) | |
| `spf13/viper` | `config` + `serde` | |
| `log/slog` | `tracing` | structured, async-aware |
| `testify` | `rstest` + `testcontainers` (PG) | |
| n/a | `async-trait`, `thiserror`, `serde`, `uuid`, `chrono` | |

## Phased roadmap (~10-14 weeks)

| Phase | Weeks | Deliverable |
|---|---|---|
| **0 Foundations** | 1-2 | done: scaffold, migrations runner (reuse SQL, simple-query protocol), `SystemDatabase` trait + PG impl of lifecycle tables (`workflow_status`, `operation_outputs`) |
| **1 Core engine** | 2-3 | done: `Workflow` / `Step` execution, checkpointing, recovery/replay, `RunWorkflow` / `RunAsStep` / `Sleep` / `GetResult`, durable `Send` / `Recv`, `SetEvent` / `GetEvent`, patching |
| **2 Primitives** | 2 | done: queues (workers, rate-limit, dedup, priority, partitioning), scheduler (cron + backfill present) |
| **3 Management** | 1-2 | done: `Client` API + `dbos-cli` binary + `dbos-admin` HTTP server |
| **4 Conductor** | 1 | missing: WS client + protocol + executor registration |
| **5 SQLite** | 1 | done: `sqlx` impl + sqlite migrations |
| **6 Bindings** | 3-4 | missing: Python (PyO3/maturin), then Node (napi-rs) |

## Bindings - Python and Node

Both bind to the **erased `Workflow` / `Step` traits** in `dbos-core`. The Rust
runtime owns Tokio; the VM only supplies function bodies.

```text
user code (Python/Node)
   |  dbos.workflow(name="x")(async def x(ctx, input): ...)
   v
binding registers a callback by name
   |  implements `dyn Workflow::run` -> marshal Value into VM, call, await, marshal back
   v
dbos-core engine (registry, recovery, queues, scheduler) - pure Rust
   v
dbos-postgres / dbos-sqlite - pure Rust
```

**Python**: `pyo3` + `pyo3-asyncio` (tokio feature) for the asyncio <-> Tokio
bridge. Build wheels with `maturin`. Keep one Tokio runtime owned by the
extension module; never block the asyncio loop on DB I/O.

**Node**: `napi-rs` with the `tokio` runtime feature. Async via napi `Promise`
/ tasks; ship platform-specific prebuilts through napi's release pipeline.

Hard part in both: marshaling `serde_json::Value` <-> VM value and bridging the
VM's async runtime with Tokio. Keep input/output type-erased end-to-end.

## How to start (historical Phase 0 -> Phase 1 notes)

These steps are kept for reference because they guided the initial port:

1. `cargo add` deps into a new `crates/dbos-postgres` crate; copy
   `dbos/migrations/*.sql` into `migrations/` and embed with `include_str!`.
2. Implement `Dialect` for a `PostgresDialect` (port `dialect.go` bodies).
3. Implement `SystemDatabase::migrate` (create schema, run migrations in order,
   track applied in an `application_versions`-style table: port
   `system_database.go` migration logic).
4. Implement `init_workflow` / `record_workflow_result` / `get_workflow_status`
   / `record_step_output` / `get_steps` first; these unblock Phase 1.
5. In `dbos-core::context`, replace `todo!()` bodies for `run_as_step`,
   `run_workflow`, `get_result`, `recover` using the trait + the registry.
6. Add an integration test with `testcontainers` Postgres that mirrors
   `dbos/dbos_test.go`'s simplest workflow.

## Current status

- `dbos-core`: core runtime path is implemented, including recovery/replay,
  durable workflow primitives, patching, queue workers, scheduler loops,
  durable streams, debouncer support, management primitives used directly
  from the runtime (`cancel` / `resume` / `fork` / listing / step inspection),
  and the standalone `Client` API (`dbos-core::client`).
- `dbos-cli`: the `dbos` binary (clap) implements `version`, `migrate`,
  `reset`, `workflow {list,get,steps,cancel,resume,fork,delete}`, `start`,
  `init` (scaffolds a Rust project), and `postgres {start,stop}`.
  Auto-detects Postgres/SQLite from the connection URL; resolves the URL
  from `--db-url`, `dbos-config.yaml`, or `DBOS_SYSTEM_DATABASE_URL`.
- `dbos-admin`: an axum HTTP server exposing the full DBOS Console endpoint
  surface (health, workflow CRUD, steps, recovery, queue metadata, global
  timeout, deactivate, conductor status, GC stub, registered workflows,
  start-workflow, CORS). Started alongside the runtime when
  `Config.admin_server` is set.
- `dbos-postgres`: main backend path is implemented for workflow state,
  checkpoints, notifications, events, streams, recovery queries, migrations,
  and LISTEN/NOTIFY wakeups. Integration coverage runs through
  `testcontainers`.
- `dbos-sqlite`: SQLite backend is in the workspace and reuses the Go SQLite
  migration set as the schema source of truth.
- `examples/sqlite-demo`: an interactive demo app — SQLite backend + admin API
  + four demo workflows (multi-step `checkout`, `greet`, `flaky_task`,
  `long_running`) + seeded history. Run with `cargo run -p sqlite-demo`.
- `ui`: a Nuxt 3 + Vue 3 + Tailwind dashboard ("DBOS Console") that consumes
  the admin API — registered workflows, live execution history, step
  timelines, errors, and start/cancel/resume actions. Run with
  `cd ui && npm run dev`.
- `examples`: SQLite examples cover basic workflow execution plus queue and
  scheduler usage.

What is still missing from the Go project:

- `dbos/conductor.go` + `dbos/conductor_protocol.go`: websocket client,
  reconnection, protocol handlers, export/import, aggregates, and
  queue/schedule control exposed through Conductor.
- `bindings/python` and `bindings/nodejs`: VM adapters over the erased
  `Workflow` / `Step` traits.
- remaining parity suites for metrics/logger behavior, full serialization
  parity, and some Postgres-specific migration/driver-path tests.

What landed since the last status update:

- `dbos/client.go` -> `dbos-core::client::Client`: enqueue, `list_workflows`
  (rich `ListWorkflowsFilter`), send/get_event, retrieve/cancel/resume
  (single + bulk), `set_workflow_delay`, `delete_workflows` (with recursive
  child deletion), fork, `get_workflow_steps`, `get_workflow_children`,
  `read_stream`, full schedule management (`create`/`apply`/`get`/`list`/
  `pause`/`resume`/`delete`/`trigger`/`backfill`), and application-version
  management (`list`/`get_latest`/`set_latest`).
- `cmd/dbos/*` -> `crates/dbos-cli`: the `dbos` binary (clap) with `version`,
  `migrate` (migrations + optional Postgres schema grants + config-file
  migration commands), `reset` (Postgres drop/recreate + SQLite file delete),
  `workflow {list,get,steps,cancel,resume,fork,delete}`, `start` (runs
  `runtimeConfig.start` commands from config with signal forwarding), `init`
  (scaffolds a Rust starter project from embedded templates), and `postgres
  {start,stop}` (manages a local Docker Postgres container via the `docker`
  CLI). URL resolution: `--db-url` flag -> `database_url` in `dbos-config.yaml`
  -> `DBOS_SYSTEM_DATABASE_URL` env. Auto-detects Postgres vs SQLite from the
  URL.
- `dbos/admin_server.go` -> `crates/dbos-admin`: an axum HTTP server over
  `DbosContext` exposing all DBOS Console endpoints: health check, workflow
  CRUD (list/get/steps/cancel/resume/fork), queue metadata, recovery,
  global timeout, deactivate, conductor status, and garbage-collect (stub).
  Response DTOs match Go's PascalCase / epoch-ms format for Console
  compatibility.
- Supporting `SystemDatabase` trait additions implemented by both backends:
  `list_workflows_filtered`, `get_schedule`, `delete_schedule`,
  `update_schedule_status`, `create_application_version`,
  `update_application_version_timestamp`, `list_application_versions`,
  `get_latest_application_version`, `set_workflow_delay`, `delete_workflows`.
- `start_workflow` no longer requires local registration for enqueued/delayed
  launches (matches Go's `Client.Enqueue`, which inserts for a remote
  executor to pick up).
- Added `DbosContext::recover_workflows`, `cancel_all_before`,
  `list_queue_metadata`, `deactivate`, `list_workflows_filtered`, and a public
  `system_db()` accessor for the admin server.
- `SystemDatabase::list_queues` added (lists all registered queue configs).
- Coverage: `dbos-sqlite/tests/client.rs` (9 tests),
  `dbos-postgres/tests/client.rs` (3 tests), `dbos-cli/tests/cli.rs`
  (10 integration + 6 unit tests), and `dbos-admin/tests/admin.rs` (12 tests).

What landed since the last status update:

- `dbos/client.go` -> `dbos-core::client::Client`: enqueue, `list_workflows`
  (rich `ListWorkflowsFilter`), send/get_event, retrieve/cancel/resume
  (single + bulk), `set_workflow_delay`, `delete_workflows` (with recursive
  child deletion), fork, `get_workflow_steps`, `get_workflow_children`,
  `read_stream`, full schedule management (`create`/`apply`/`get`/`list`/
  `pause`/`resume`/`delete`/`trigger`/`backfill`), and application-version
  management (`list`/`get_latest`/`set_latest`).
- Supporting `SystemDatabase` trait additions implemented by both backends:
  `list_workflows_filtered`, `get_schedule`, `delete_schedule`,
  `update_schedule_status`, `create_application_version`,
  `update_application_version_timestamp`, `list_application_versions`,
  `get_latest_application_version`, `set_workflow_delay`, `delete_workflows`.
- `start_workflow` no longer requires local registration for enqueued/delayed
  launches (matches Go's `Client.Enqueue`, which inserts for a remote
  executor to pick up).
- Coverage: `dbos-sqlite/tests/client.rs` (9 tests) and
  `dbos-postgres/tests/client.rs` (3 tests) over the Client + new DB methods.

## SQLite test parity

The Rust workspace now has broad SQLite coverage for the currently implemented
feature surface.

Covered today:

- dialect detection for Postgres/Cockroach/SQLite URL forms;
- SQLite URL normalization and connection setup;
- migrations, reopen/idempotent migrate behavior, PRAGMAs, and schema tables;
- workflow lifecycle persistence, step checkpoints, notifications, events,
  child workflows, recovery/replay, `WorkflowHandle`, workflow listing, and GC;
- replay behavior for `DBOS.recv`, `DBOS.getEvent`, and `DBOS.sleep`;
- error persistence for failing steps and terminal workflow failures;
- queue dequeue, delayed dequeue, deduplication, background queue workers;
- queue registration/config persistence, partitioned queues, and rate limiting;
- schedules, cron triggering, and automatic backfill;
- durable streams: append, snapshot reads, closed-stream semantics, and
  Postgres LISTEN/NOTIFY wakeups;
- debouncer: internal workflow, ACK protocol via events, latest-input
  coalescing, and SQLite/Postgres integration coverage;
- management primitives: cancel/resume, recursive child listing, workflow
  forking with copied checkpoints/events/streams, and SQLite/Postgres
  integration coverage.

Not yet portable from the Go suite because the Rust runtime does not implement
these subsystems yet:

- conductor protocol/handlers;
- metrics, logger, and serialization parity suites;
- Postgres-specific migration and driver-path tests.

The client API surface (enqueue, list/filter, schedule management, application
versions, cancel/resume/fork/delete, streams, events) is now covered by
`dbos-sqlite/tests/client.rs` and `dbos-postgres/tests/client.rs`. The CLI (`version`/`migrate`/`reset`/`workflow`/`start`/`init`/`postgres`) is covered by
`dbos-cli/tests/cli.rs`. The admin HTTP server is covered by
`dbos-admin/tests/admin.rs`.

## Postgres test note

Embedded Postgres via `pglite` was investigated and is intentionally not part
of the active plan right now.

Observed problems during the Windows port:

- the embedded runtime wanted cache/runtime access outside the workspace unless
  patched around;
- startup was sensitive to sandboxing and required unsandboxed runtime access;
- migration execution hung in the `pglite` path while applying later online
  Postgres migrations, which made it a poor fit for the current MVP loop.

Current direction:

- keep Postgres as the production backend;
- run Postgres integration coverage with `testcontainers`;
- keep SQLite as the fast local coverage backend;
- return to embedded Postgres testing later with either a different harness or
  a narrower, explicitly manual setup.

## Porting discipline

- One Go section -> one Rust module; keep the `// ported from` trail.
- Port `system_database.go` and `workflow.go` first; they are about 50% of the
  logic.
- Keep SQL verbatim where possible; diverge only for the SQLite dialect set.
- Add an integration test per Go `*_test.go` as you go (do not batch).
