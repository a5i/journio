# {{PROJECT_NAME}}

A starter DBOS application in Rust — durable workflows that survive crashes.

## Quick start

```sh
# Run the app (uses a local SQLite DB by default)
cargo run

# In another terminal, watch a workflow recover after a crash.
# The app prints a workflow id you can inspect:
dbos --db-url "sqlite://{{PROJECT_NAME}}.db" workflow get <workflow-id>
```

## What this demonstrates

`src/main.rs` registers a 3-step durable workflow. Each step sleeps 5 seconds
and is checkpointed to the system database. If you kill the process mid-run
(Ctrl+C), restarting it recovers the workflow from the last completed step.

## Switching to Postgres

1. Start a local Postgres: `dbos postgres start`
2. Set the URL in `dbos-config.yaml` or via `DBOS_SYSTEM_DATABASE_URL`.
3. `dbos migrate` then `cargo run`.

## Commands

| Command | Description |
|---|---|
| `cargo run` | Start the app (registers workflows + admin server) |
| `dbos migrate` | Create / update DBOS system tables |
| `dbos workflow list` | List workflows |
| `dbos workflow get <id>` | Inspect a workflow |
