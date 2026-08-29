# journio

Journio is a durable workflow orchestration engine for Rust. Workflows are
ordinary async functions whose side effects and timers are journaled to a
pluggable storage backend, so they survive crashes and restarts and replay to
the same state.

## Workspace layout

| Crate | Purpose |
| --- | --- |
| [`journio-core`](crates/journio-core) | Engine traits and runtime: workflows, durable sleep, signals, events, queues, scheduler, streams, debouncing, recovery/replay |
| [`journio-sqlite`](crates/journio-sqlite) | SQLite storage backend for `journio-core` |
| [`journio-postgres`](crates/journio-postgres) | Postgres (and CockroachDB) storage backend for `journio-core` |
| [`journio-cli`](crates/journio-cli) | `journio` command-line interface for managing workflows |
| [`journio-admin`](crates/journio-admin) | Admin HTTP server (axum) exposing workflow management APIs |

The workspace also hosts Node.js bindings (`bindings/nodejs`) and runnable
examples (`examples/`); these are not published to crates.io.

## Quick start

Add the core crate and a backend:

```toml
[dependencies]
journio-core = "0.1"
journio-sqlite = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Define a workflow as an async closure over a `WorkflowContext`:

```rust
use std::sync::Arc;
use journio_core::{StepFunc, Workflow, WorkflowFn};

let checkout: Arc<dyn Workflow> = Arc::new(WorkflowFn::new(
    "checkout",
    |ctx, input: Option<serde_json::Value>| {
        Box::pin(async move {
            // Everything on `ctx` is journaled: durable sleeps, signal
            // send/recv, events, steps, and child workflows. If the process
            // crashes, recovery replays the workflow to the same state.
            let outcome = ctx.run_as_step(Arc::new(StepFunc::new(
                "charge",
                |_ctx| Box::pin(async move { Ok(serde_json::json!({ "charged": true })) }),
            ))))
            .await?;
            Ok(outcome)
        })
    },
));
```

See the crate READMEs linked above and the examples under `examples/` for
complete, runnable setups (SQLite demo, cross-language Postgres).

## License

MIT — see [LICENSE](LICENSE).
