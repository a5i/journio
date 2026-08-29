# journio-sqlite

SQLite storage backend for [journio](https://crates.io/crates/journio-core),
the durable workflow orchestration engine.

`journio-sqlite` implements journio's `SystemDatabase` trait on top of SQLite
via SQLx: workflow state, step records, durable timers, signals, events,
queues, schedules, and streams are all journaled to a single SQLite database,
enabling crash recovery and replay for embedded and single-node deployments.

## Example

```rust
use std::sync::Arc;
use journio_sqlite::SqliteSystemDatabase;
use journio_core::SystemDatabase;

let db = SqliteSystemDatabase::connect("sqlite:///tmp/journio.db").await?;
db.migrate().await?;
let handle: Arc<dyn SystemDatabase> = Arc::new(db);
```

## License

MIT — see the [repository LICENSE](https://github.com/a5i/journio/blob/main/LICENSE).
