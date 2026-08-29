# journio-admin

Admin HTTP server for [journio](https://crates.io/crates/journio-core), the
durable workflow orchestration engine.

`journio-admin` exposes workflow management over HTTP using axum: start and
inspect workflows and their step histories, read durable events, signals,
queues, schedules, and streams, and trigger recovery — operating on the same
system database (SQLite or Postgres) your application uses.

## Example

Embed it next to your journio runtime:

```rust
use std::sync::Arc;
use journio_core::JournioContext;

let server = journio_admin::AdminServer::new(
    Arc::new(JournioContext::new(/* ... */)),
    8080,
);
let addr = server.start().await?;
```

## License

MIT — see the [repository LICENSE](https://github.com/a5i/journio/blob/main/LICENSE).
