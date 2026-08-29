# journio-core

Durable workflow orchestration core for Rust.

`journio-core` defines the erased, object-safe traits and runtime that storage
backends, the CLI, the admin server, and language bindings all build on:
workflows and steps, durable `Sleep`, `Send`/`Recv` signals, events, queues,
scheduler, streams, debouncing, and crash recovery via journal replay. There is
intentionally no I/O here — concrete storage lives in the `journio-sqlite` and
`journio-postgres` crates.

## Example

```rust
use std::sync::Arc;
use journio_core::{StepFunc, Workflow, WorkflowFn};

let workflow: Arc<dyn Workflow> = Arc::new(WorkflowFn::new(
    "checkout",
    |ctx, input: Option<serde_json::Value>| {
        Box::pin(async move {
            let charge = Arc::new(StepFunc::new(
                "charge",
                |_ctx| Box::pin(async move { Ok(serde_json::json!({ "charged": true })) }),
            ));
            ctx.run_as_step(charge).await
        })
    },
));
```

Pair it with [`journio-sqlite`](https://crates.io/crates/journio-sqlite) or
[`journio-postgres`](https://crates.io/crates/journio-postgres) for storage.

## License

MIT — see the [repository LICENSE](https://github.com/a5i/journio/blob/main/LICENSE).
