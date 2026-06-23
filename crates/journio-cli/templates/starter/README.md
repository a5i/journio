# {{PROJECT_NAME}}

A starter **Journio** application in Rust — durable workflows that survive
crashes.

> **Note:** Journio is a Rust workflow-orchestration runtime inspired by
> [DBOS](https://docs.dbos.dev/).

## Quick start

```sh
# Run the app (uses a local SQLite DB by default)
cargo run

# In another terminal, watch a workflow recover after a crash.
# The app prints a workflow id you can inspect:
journio --db-url "sqlite://{{PROJECT_NAME}}.db" workflow get <workflow-id>
```

## What this demonstrates

`src/main.rs` registers a 3-step durable workflow. Each step sleeps 5 seconds
and is checkpointed to the system database. If you kill the process mid-run
(Ctrl+C), restarting it recovers the workflow from the last completed step.

## Switching to Postgres

1. Start a local Postgres: `journio postgres start`
2. Set the URL in `journio-config.yaml` or via `JOURNIO_SYSTEM_DATABASE_URL`.
3. `journio migrate` then `cargo run`.

## Commands

| Command | Description |
|---|---|
| `cargo run` | Start the app (registers workflows + admin server) |
| `journio migrate` | Create / update Journio system tables |
| `journio workflow list` | List workflows |
| `journio workflow get <id>` | Inspect a workflow |
