# `@journio/sdk` — Node.js binding

Node.js adapter for the Journio durable-workflow engine. Workflows are written
as plain `async` TypeScript functions; the Rust engine (registry, recovery,
queues, scheduler, Postgres/SQLite backends) runs them durably. The same
system database can be shared with Rust and (future) Python processes, so a
workflow registered in Node can be enqueued and executed from another
language, and vice-versa. See
[`examples/cross-language-postgres`](../../examples/cross-language-postgres)
for a working polyglot setup.

## Build

The binding has two parts that must both be present:

- `native/index.node` — the Rust engine compiled by **napi-rs**. It statically
  links `journio-core` / `journio-postgres`, so **rebuild it whenever those
  crates change**.
- `dist/index.js` — the TypeScript glue, compiled from `src/index.ts`.

```sh
cd bindings/nodejs
npm install
npm run build          # = build:native (release) + build:ts
npm test               # vitest suite against the rebuilt native module
```

Consume it from another package with a `file:` dependency (as the
cross-language example does):

```json
{ "dependencies": { "@journio/sdk": "file:../bindings/nodejs" } }
```

## A minimal workflow

```ts
import { Journio } from "@journio/sdk";

// 1. Register a workflow by name. The function body is the workflow; durable
//    operations inside it (runStep / setEvent / writeStream / ...) are
//    checkpointed and replayed on recovery.
const charge = Journio.registerWorkflow(
  async (input: { orderId: string; cents: number }) => {
    await Journio.setEvent("charge", "charging");
    const receipt = await Journio.runStep(
      async () => ({ id: "rc_" + input.orderId, cents: input.cents }),
      { name: "capture_charge" }
    );
    return { ok: true, receipt };
  },
  { name: "charge_card" }
);

// 2. Configure + launch the runtime. listenQueues restricts this process to
//    polling only the listed queues (it will not dequeue from others).
await Journio.setConfig({
  name: "payments",
  systemDatabaseUrl: process.env.JOURNIO_SYSTEM_DATABASE_URL!,
  systemDatabaseSchemaName: "journio_payments",
  listenQueues: ["payments_queue"],
});
await Journio.launch();

// 3a. Run a workflow locally — calls the function and resolves to its return value.
const result = await charge({ orderId: "o_1", cents: 1999 });

// 3b. Or enqueue it by name for any worker (this process or another) that is
//     listening on the queue. Returns a durable handle.
const handle = await Journio.enqueueWorkflow("charge_card", {
  workflowID: "charge-o_2",
  queueName: "payments_queue",
})({ orderId: "o_2", cents: 2999 });
const outcome = await handle.getResult({ timeoutMS: 20_000 });

await Journio.shutdown();
```

## Lifecycle

| Method | Description |
|---|---|
| `Journio.setConfig(config)` | Configure the runtime. Resets to pre-launch; any previously registered workflows are re-registered on the next `launch()`. Stores the native config as a `Promise`. |
| `Journio.launch()` | Apply config, register all workflows with the native engine, and start the runtime (workers, recovery, scheduler, listeners). |
| `Journio.shutdown(timeoutMS?)` | Stop the runtime. Default grace period 1000 ms. |

`JournioConfig`:

| Field | Required | Description |
|---|---|---|
| `name` | yes | Application name. |
| `systemDatabaseUrl` | yes | `postgres://` / `postgresql://` URL, or a `sqlite:` URL. |
| `systemDatabaseSchemaName` | no | Postgres schema (default `journio`). |
| `applicationVersion` | no | Code version; mismatch drives recovery of in-flight workflows. |
| `executorID` | no | Executor identity. |
| `runAdminServer` | no | Start the Journio Console HTTP server. |
| `adminPort` | no | Admin server port. |
| `listenQueues` | no | Queue names this process will poll. Omit to poll none (e.g. a caller-only process). |
| `logLevel` | no | Log verbosity. |

> Schema creation, the full migration set, and the `pgcrypto` extension are
> applied automatically on first connect — there is no manual DB setup step.

## Defining workflows

`Journio.registerWorkflow(fn, { name })` returns a wrapped function. Calling it
runs the workflow **in this process** and resolves to its return value. Pass an
explicit `name` (an anonymous function has none).

Inside a workflow body these are durable and replay-safe:

| Primitive | Description |
|---|---|
| `Journio.runStep(fn, { name })` | Run `fn` once durably; its result is checkpointed and reused on replay. Steps appear in `listWorkflowSteps`. |
| `Journio.sleep(durationMS)` | Durable sleep; resumes on schedule even after a crash. |
| `Journio.setEvent(key, value)` | Persist a key/value event for this workflow. |
| `Journio.getEvent(targetWorkflowID, key, { timeoutMS })` | Read an event, optionally waiting until it exists. |
| `Journio.writeStream(key, value)` | Append a value to a named stream. |
| `Journio.closeStream(key)` | Mark a stream closed (readers stop waiting). |
| `Journio.readStream(targetWorkflowID, key)` | Async generator over another workflow's stream values (stops at close). |
| `Journio.send(destinationID, message, topic?)` | Send a durable message to another workflow. |
| `Journio.recv(topic?, { timeoutMS })` | Await a durable message. |

The current execution context is available via the static getters
`Journio.workflowID`, `Journio.stepID`, `Journio.applicationVersion`, and
`Journio.executorID` (driven by `AsyncLocalStorage`, so they are correct inside
a step without threading context manually).

## Starting workflows

Two launchers, both returning a `WorkflowHandle` (they do **not** await the
result):

- `Journio.startWorkflow(workflow, params)(...args)` — start a workflow you have
  a reference to. Runs locally if no `queueName` is given, otherwise enqueues it.
- `Journio.enqueueWorkflow(workflowName, params)(...args)` — start a workflow
  **by name** with a `queueName`. This is the cross-process / cross-language
  path: the caller does not need the workflow registered locally — any worker
  listening on that queue with that workflow registered will execute it.

`StartWorkflowParams`: `workflowID`, `queueName`, `timeoutMS`,
`workflowAttributes`, and `enqueueOptions` (`deduplicationID`, `priority`,
`queuePartitionKey`) for idempotent / prioritised enqueue.

A fixed `workflowID` makes the enqueue idempotent: replaying it will not create
a duplicate run.

## Handles, results, and status

`Journio.retrieveWorkflow<R>(workflowID)` returns a handle for any workflow,
independent of where it runs. `WorkflowHandle`:

| Member | Description |
|---|---|
| `workflowID` | The workflow's ID. |
| `getResult({ timeoutMS, pollingIntervalMs })` | Resolve to the workflow's return value, waiting up to `timeoutMS`. |
| `getStatus()` | Current `WorkflowStatus` (or `null`). |
| `cancel()` / `resume({ queueName })` | Cancel or resume the workflow. |
| `fork(options)` | Fork from a step; see `ForkWorkflowOptions`. |

## Queues

Queues decouple **who enqueues** from **who executes**. A worker must do two
things to consume a queue:

1. Pass the queue name in `listenQueues` at `setConfig` time — the runtime will
   only poll queues listed there.
2. `await Journio.registerQueue(name, options)` after `launch()` to persist the
   queue's configuration (concurrency, rate limiting, partitioning).

`QueueOptions`: `concurrency`, `workerConcurrency`, `rateLimit`
(`{ limitPerPeriod, periodSec }`), `priorityEnabled`, `partitionQueue`.

Because a worker only polls its `listenQueues`, several workers (even in
different languages) can coexist without stealing each other's work — give each
language its own queue.

## Management and inspection

| Method | Description |
|---|---|
| `Journio.listWorkflows(input)` / `listQueuedWorkflows(input)` | List/filter workflows; `ListWorkflowsInput` supports filtering by ID, name, status, queue, and pagination. |
| `Journio.listWorkflowSteps(workflowID)` | Step checkpoints (name, output, error, child workflow). |
| `Journio.cancelWorkflow(id)` / `resumeWorkflow(id, { queueName })` | Lifecycle control. |
| `Journio.forkWorkflow(id, startStep, options)` | Fork a workflow from a step. |
| `Journio.patch(name)` / `deprecatePatch(name)` | Progressive rollout / retirement of code branches within a workflow. |

## Error model

Native errors surface as `Error` instances carrying the Journio error code, e.g.
`Error: Journio Error (InitializationError): db error`. The codes mirror
`journio-core` (`InitializationError`, `WorkflowExecutionError`, …). If
`launch()` fails, retry `setConfig` + `launch()` after a short backoff — the
migration/connection step can race a freshly started Postgres. The
cross-language example wraps this in a small retry loop (`configureAndLaunch`).
