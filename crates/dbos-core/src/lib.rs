//! # dbos-core
//!
//! Pure-Rust port of the core engine of `dbos-transact-golang`.
//!
//! This crate defines the *erased, object-safe traits* that the runtime,
//! the Postgres/SQLite backends, and the Python/Node bindings all build on.
//! There is intentionally no I/O here beyond trait definitions — concrete
//! backends live in `dbos-postgres` / `dbos-sqlite`.
//!
//! Porting source: `dbos/dbos.go`, `dbos/workflow.go`, `dbos/dialect.go`,
//! `dbos/errors.go`, `dbos/serialization.go`, `dbos/system_database.go`.
//!
//! Status: MVP core runtime path + workflow primitives (durable `Sleep`,
//! `Send`/`Recv`, `SetEvent`/`GetEvent`, `RunWorkflow`/`RunAsStep`, recovery/
//! replay) are implemented. Remaining: queues, scheduler, LISTEN/NOTIFY
//! listener, streams, debouncer, patching, client/admin/CLI, conductor, and
//! language bindings.

pub mod config;
pub mod context;
pub mod dialect;
pub mod error;
pub mod system_db;
pub mod types;
pub mod value;
pub mod workflow;

pub use config::Config;
pub use context::{
    DbosContext, DebounceOptions, EnqueueOptions, ForkWorkflowOptions, QueueOptions,
    ReadStreamOptions, ScheduleOptions, WorkflowContext, WorkflowHandle,
};
pub use dialect::{Dialect, DialectName};
pub use error::{DbosError, DbosErrorCode};
pub use system_db::{ForkWorkflow, InitWorkflow, InitWorkflowResult, Notification, SystemDatabase};
pub use types::{
    QueueConfig, ScheduleStatus, ScheduledWorkflowInput, StepRecord, StreamEntry,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
pub use value::{Interchange, JsonSerializer, Serializer};
pub use workflow::{Registry, Step, StepFunc, Workflow, WorkflowFn};
