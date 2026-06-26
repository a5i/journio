# Cross-Language Postgres Example

This example runs Journio workflows across separate Rust and TypeScript/Node.js
processes sharing **one Postgres system database**. It is the reference for
polyglot Journio deployments: a workflow registered in one language can be
enqueued and executed by a worker written in another.

It demonstrates:

- Cross-language workflow interoperability (Rust ↔ Node).
- Durable queue handoff through Postgres.
- Separate executor processes with independent workflow registries.
- Shared workflow history visible from either language.
- Durable steps, events, streams, fixed workflow IDs, and idempotent enqueue.
- Cross-process integration tests driven through `testcontainers` Postgres.

## How cross-process execution works

A workflow runs in whichever process has it **registered**. For a caller to run
a workflow that lives in another process, it does not call the function
directly — it **enqueues it by name** onto a durable queue:

1. The caller enqueues a workflow by name, targeting a queue and a workflow ID.
2. A worker process that has registered both the workflow and the queue
   dequeues it and executes it durably (steps are checkpointed).
3. The caller retrieves the durable result by workflow ID — from any process,
   in any language.

Because the handoff goes through Postgres (not in-memory dispatch), the caller
and worker can be different binaries, different languages, on different
machines, and the workflow still survives a worker crash mid-execution.

### `listenQueues` — a worker only consumes what it lists

Each worker passes the set of queues it should poll via `listenQueues`
(`setConfig({ listenQueues: [...] })` in Node, `Config.listen_queues` in Rust).
A worker that is not listening on a queue will never dequeue from it, so two
workers can run side by side without stealing each other's work. This example
uses one queue per language:

- `cross_language_rust_queue` — consumed by the Rust worker.
- `cross_language_node_queue` — consumed by the Node worker.

## Setup

Start a local Postgres via the CLI and export its URL. `journio postgres start`
runs a `pgvector/pgvector:pg16` container on port `5432` with the password
`journio` (override with `PGPASSWORD`):

```sh
journio postgres start
export JOURNIO_SYSTEM_DATABASE_URL="postgres://postgres:journio@localhost:5432/postgres"
export JOURNIO_SYSTEM_DATABASE_SCHEMA="journio_cross_language"
```

On Windows PowerShell:

```powershell
journio postgres start
$env:JOURNIO_SYSTEM_DATABASE_URL="postgres://postgres:journio@localhost:5432/postgres"
$env:JOURNIO_SYSTEM_DATABASE_SCHEMA="journio_cross_language"
```

> **No manual database setup is required.** Schema creation and the full
> migration set run automatically the first time a process connects, and the
> `pgcrypto` extension (used for `gen_random_uuid()`) is installed by the
> migration runner itself — there is nothing to `CREATE EXTENSION` by hand
> from either language.

Build the native Node binding once (it statically links the Rust engine, so it
must be rebuilt whenever `journio-core` / `journio-postgres` change), then
install the example's Node dependencies:

```sh
cd ../../bindings/nodejs
npm install
npm run build        # builds native/index.node (release) + dist/index.js

cd ../../examples/cross-language-postgres/node
npm install
npm run typecheck
```

## Running the demo (four terminals)

### Terminal 1 — Rust worker

```sh
cargo run -p cross-language-postgres --bin rust-worker
```

Registers `rust_price_quote`, listens on `cross_language_rust_queue`, and prints:

```json
{"event":"ready","worker":"rust","queue":"cross_language_rust_queue","workflow":"rust_price_quote"}
```

### Terminal 2 — Node calls Rust

```sh
cd examples/cross-language-postgres/node
npm run node-calls-rust
```

Enqueues `rust_price_quote` for the Rust worker. The result looks like:

```json
{"event":"result","workflowID":"node-calls-rust-...","result":{"engine":"rust","sku":"starter-widget","quantity":3,"totalCents":5997}}
```

### Terminal 3 — Node worker

```sh
cd examples/cross-language-postgres/node
npm run node-worker
```

Registers `node_fraud_check`, listens on `cross_language_node_queue`, and prints:

```json
{"event":"ready","worker":"node","queue":"cross_language_node_queue","workflow":"node_fraud_check"}
```

### Terminal 4 — Rust calls Node

```sh
cargo run -p cross-language-postgres --bin rust-caller
```

Enqueues `node_fraud_check` for the Node worker. The result looks like:

```json
{"event":"result","workflowId":"rust-calls-node-...","result":{"approved":true,"engine":"node","orderId":"order-1001","riskScore":22}}
```

## Integration tests

The cross-process flows are covered by automated tests in `tests/cross_process.rs`:

- `node_caller_executes_rust_worker_through_postgres` — Node caller → Rust worker → `rust_price_quote`.
- `rust_caller_executes_node_worker_through_postgres` — Rust caller → Node worker → `node_fraud_check`.

Each test spins up an isolated Postgres container (unique schema) via
`testcontainers`, spawns the worker and caller as real child processes, and
asserts both the emitted result and the persisted workflow state (status,
queue, and step names).

Prerequisites:

- **Docker** must be running (the tests start Postgres containers).
- The **Node binding must be built** (`bindings/nodejs` `npm run build`) and the
  example's Node dependencies installed (`node/` `npm install`). If either is
  missing the tests skip themselves with a message rather than fail.
- The Rust binaries are compiled automatically by the test target.

Run them with:

```sh
cargo test -p cross-language-postgres --test cross_process -- --nocapture --test-threads=1
```

`--test-threads=1` keeps the worker/caller logs readable; `--nocapture` streams
each child's output live. On any failure the harness prints the captured
**stderr of the child process** that did not produce the expected event, so a
crash or hang surfaces its actual cause instead of a bare exit code. Workers
exit after the target workflow reaches a terminal status when
`JOURNIO_EXIT_AFTER_WORKFLOW_ID` is set, which is how the tests scope a worker
to exactly one run.

## Inspecting workflows

```sh
journio --db-url "$JOURNIO_SYSTEM_DATABASE_URL" workflow list
journio --db-url "$JOURNIO_SYSTEM_DATABASE_URL" workflow get <workflow-id>
journio --db-url "$JOURNIO_SYSTEM_DATABASE_URL" workflow steps <workflow-id>
```

Useful events and streams produced by the demo workflows:

- Rust workflow event: `quote_status` · stream: `quote_updates`
- Node workflow event: `fraud_status` · stream: `fraud_updates`

## Failure demo (durability)

1. Start the relevant worker.
2. Start a caller and copy the printed workflow ID.
3. Kill the worker while it is processing.
4. Restart the same worker.
5. Inspect the workflow steps. Completed steps are reused and execution resumes
   from the next durable operation — no work is repeated.

For integration tests or scripted demos, workers support:

```sh
export JOURNIO_EXIT_AFTER_WORKFLOW_ID="<workflow-id>"
```

When set, the worker exits after that workflow reaches a terminal status.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `JOURNIO_SYSTEM_DATABASE_URL` | required | Postgres connection URL |
| `JOURNIO_SYSTEM_DATABASE_SCHEMA` | `journio_cross_language` | Journio system schema |
| `JOURNIO_RUST_QUEUE` | `cross_language_rust_queue` | Queue consumed by the Rust worker |
| `JOURNIO_NODE_QUEUE` | `cross_language_node_queue` | Queue consumed by the Node worker |
| `JOURNIO_WORKFLOW_ID` | generated | Explicit workflow ID for the caller |
| `JOURNIO_QUOTE_SKU` | `starter-widget` | Node-to-Rust quote SKU |
| `JOURNIO_QUOTE_QUANTITY` | `3` | Node-to-Rust quote quantity |
| `JOURNIO_ORDER_ID` | `order-1001` | Rust-to-Node order ID |
| `JOURNIO_AMOUNT_CENTS` | `12500` | Rust-to-Node amount |
| `JOURNIO_EXIT_AFTER_WORKFLOW_ID` | unset | Worker exits once this workflow is terminal |
