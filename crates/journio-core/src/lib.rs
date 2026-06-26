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
//! Status: MVP core runtime path + workflow primitives (durable `Sleep`,
//! `Send`/`Recv`, `SetEvent`/`GetEvent`, `RunWorkflow`/`RunAsStep`, recovery/
//! replay) are implemented, alongside queues, scheduler, streams, debouncer,
//! patching, and the standalone `Client`. Remaining: admin HTTP server, CLI,
//! conductor, and language bindings.

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
