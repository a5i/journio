# journio-postgres

Postgres (and CockroachDB) storage backend for
[journio](https://crates.io/crates/journio-core), the durable workflow
orchestration engine.

`journio-postgres` implements journio's `SystemDatabase` trait on top of
Postgres via tokio-postgres with deadpool connection pooling: workflow state,
step records, durable timers, signals, events, queues, schedules, and streams
are journaled with `LISTEN`/`NOTIFY`-driven wakeup, enabling crash recovery
and replay for multi-process deployments sharing one database.

## Example

```rust
use std::sync::Arc;
use journio_postgres::PostgresSystemDatabase;
use journio_core::SystemDatabase;

let db = PostgresSystemDatabase::connect("postgres://user:pass@localhost/journio", "journio")?;
db.migrate().await?;
let handle: Arc<dyn SystemDatabase> = Arc::new(db);
```

## License

MIT — see the [repository LICENSE](https://github.com/a5i/journio/blob/main/LICENSE).
