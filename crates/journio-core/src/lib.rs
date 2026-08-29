//! # journio-core
//!
//! Pure-Rust port of the core engine of `journio-transact-golang`.
//!
//! This crate defines the *erased, object-safe traits* that the runtime,
//! the Postgres/SQLite backends, and the Python/Node bindings all build on.
//! There is intentionally no I/O here beyond trait definitions — concrete
//! backends live in `journio-postgres` / `journio-sqlite`.
//!
//! Porting source: `journio/journio.go`, `journio/workflow.go`, `journio/dialect.go`,
//! `journio/errors.go`, `journio/serialization.go`, `journio/system_database.go`,
//! `journio/client.go`.
//!
//! Status: the core runtime path and workflow primitives (durable `Sleep`,
//! `Send`/`Recv`, `SetEvent`/`GetEvent`, `RunWorkflow`/`RunAsStep`, recovery/
//! replay) are implemented, alongside queues, scheduler, streams, debouncer,
//! patching, and the standalone `Client`. The SQLite and Postgres backends
//! (`journio-sqlite`, `journio-postgres`), the CLI (`journio-cli`), the admin
//! HTTP server (`journio-admin`), and the Node.js bindings are provided as
//! sibling crates. Remaining: the conductor and further language bindings.

// `JournioError` carries its context fields by value and exceeds clippy's
// 128-byte `result_large_err` threshold; boxing it in `JournioResult` would be
// a cascading public-API break for marginal gain.
#![allow(clippy::result_large_err)]

pub mod client;
pub mod config;
pub mod context;
pub mod dialect;
pub mod error;
pub mod system_db;
pub mod types;
pub mod value;
pub mod workflow;

pub use client::{Client, ClientScheduleInput};
pub use config::Config;
pub use context::{
    DebounceOptions, EnqueueOptions, ForkWorkflowOptions, JournioContext, QueueOptions,
    ReadStreamOptions, ScheduleOptions, WorkflowContext, WorkflowHandle,
};
pub use dialect::{Dialect, DialectName};
pub use error::{JournioError, JournioErrorCode};
pub use system_db::{ForkWorkflow, InitWorkflow, InitWorkflowResult, Notification, SystemDatabase};
pub use types::{
    ListWorkflowsFilter, QueueConfig, ScheduleStatus, ScheduledWorkflowInput, StepRecord,
    StreamEntry, VersionInfo, WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
pub use value::{Interchange, JsonSerializer, Serializer};
pub use workflow::{Registry, Step, StepFunc, Workflow, WorkflowFn};
