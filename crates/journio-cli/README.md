# journio-cli

`journio` — command-line interface for managing journio workflows.

`journio-cli` inspects and controls workflows journaled by
[journio](https://crates.io/crates/journio-core) in either a SQLite or a
Postgres backend: list workflows and their step histories, inspect durable
timers, signals, events, queues, and schedules, and trigger recovery — all
from the terminal against the same database your application uses.

## Example

```sh
# Inspect a workflow journaled in SQLite
journio --db-url sqlite:///tmp/journio.db workflow get <workflow-id>

# List pending workflows in Postgres
journio --db-url postgres://user:pass@localhost/journio workflow list --status PENDING
```

## License

MIT — see the [repository LICENSE](https://github.com/a5i/journio/blob/main/LICENSE).
