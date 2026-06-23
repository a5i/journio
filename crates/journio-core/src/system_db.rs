//! System database abstraction — ported from `journio/system_database.go` (5,916 LOC).
//!
//! Every persistence operation the engine needs is a method on this trait.
//! Concrete impls live in `journio-postgres` (tokio-postgres + deadpool, with
//! LISTEN/NOTIFY) and `journio-sqlite` (sqlx). Keeping the trait in core means the
//! engine, recovery, queues, scheduler and FFI never import a backend crate.
//!
//! This is a **representative subset** of the full surface — expand as each
//! phase of the port lands. Group methods by the Go file section they mirror.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::dialect::Dialect;
use crate::error::JournioResult;
use crate::types::{
    ListWorkflowsFilter, QueueConfig, ScheduleStatus, StepRecord, StreamEntry, VersionInfo,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
use crate::value::Interchange;

/// Inputs for `SystemDatabase::init_workflow` — ported from the
/// `insertWorkflowStatusDBInput` / `WorkflowStatus` fields used in
/// `system_database.go:936`.
#[derive(Debug, Clone)]
pub struct InitWorkflow {
    pub workflow_id: String,
    pub name: String,
    pub status: WorkflowStatusType,
    pub executor_id: String,
    pub application_version: Option<String>,
    pub application_id: Option<String>,
    pub input: Option<Interchange>,
    pub queue_name: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub timeout: Option<Duration>,
    pub deadline: Option<DateTime<Utc>>,
    pub delay_until: Option<DateTime<Utc>>,
    pub parent_workflow_id: Option<String>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub serialization: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<Vec<String>>,
    /// Bump `recovery_attempts` on conflict (recovery path).
    pub increment_attempts: bool,
    /// Mark MAX_RECOVERY_ATTEMPTS_EXCEEDED past this many attempts.
    pub max_retries: i32,
}

impl InitWorkflow {
    /// Convenience for a freshly-enqueued PENDING workflow.
    pub fn new_pending(
        workflow_id: impl Into<String>,
        name: impl Into<String>,
        executor_id: impl Into<String>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            name: name.into(),
            status: WorkflowStatusType::Pending,
            executor_id: executor_id.into(),
            application_version: None,
            application_id: None,
            input: None,
            queue_name: None,
            deduplication_id: None,
            priority: 0,
            queue_partition_key: None,
            timeout: None,
            deadline: None,
            delay_until: None,
            parent_workflow_id: None,
            class_name: None,
            config_name: None,
            serialization: None,
            authenticated_user: None,
            assumed_role: None,
            authenticated_roles: None,
            increment_attempts: false,
            max_retries: 0,
        }
    }
}

/// Result of `init_workflow` — ported from `insertWorkflowResult`.
/// The returned `status` reflects the row's *actual* status after the
/// `ON CONFLICT` upsert (may differ from the input on recovery).
#[derive(Debug, Clone)]
pub struct InitWorkflowResult {
    pub status: WorkflowStatusType,
    pub attempts: i64,
    pub name: String,
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
    pub timeout: Option<Duration>,
    pub deadline: Option<DateTime<Utc>>,
}

/// A consumed notification — ported from `recvResult`
/// (`system_database.go:3378`). Carries the decoded message plus the
/// serialization name recorded on the notification row (the sender's format).
#[derive(Debug, Clone)]
pub struct Notification {
    pub message: Interchange,
    pub serialization: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ForkWorkflow {
    pub original_workflow_id: String,
    pub forked_workflow_id: Option<String>,
    pub start_step: i32,
    pub application_version: Option<String>,
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
}

/// The async persistence seam. All methods are transactional where the Go
/// counterpart is; the Postgres impl uses `FOR UPDATE SKIP LOCKED` for dequeue.
#[async_trait]
pub trait SystemDatabase: Send + Sync {
    /// Which dialect this backend speaks.
    fn dialect(&self) -> &dyn Dialect;

    // -- lifecycle ---------------------------------------------------------

    /// Create schema + run any pending migrations (mirrors `runMigrations`).
    async fn migrate(&self) -> JournioResult<()>;

    /// Boot the background loops: notification listener (LISTEN/NOTIFY or
    /// polling), queue pollers, etc. Mirrors `systemDB.launch`.
    async fn launch(&self) -> JournioResult<()>;

    /// Graceful shutdown of background loops.
    async fn shutdown(&self) -> JournioResult<()>;

    // -- workflow_status ---------------------------------------------------

    /// Insert a workflow_status row (or upsert on ID conflict). Returns the
    /// *actual* status after upsert — ported from `insertWorkflowStatus`
    /// (`system_database.go:936`). Surfaces `QueueDeduplicated` on unique
    /// constraint violation of `(queue_name, deduplication_id)` and
    /// `ConflictingWorkflow` if name/queue mismatch an existing row.
    async fn init_workflow(&self, init: InitWorkflow) -> JournioResult<InitWorkflowResult>;

    /// Persist terminal result/error — ported from `recordWorkflowResult`.
    async fn record_workflow_result(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: Option<&Interchange>,
        error: Option<&str>,
    ) -> JournioResult<()>;

    /// Read full status — ported from `getWorkflowStatus`-style single-row
    /// selects scattered through `system_database.go`.
    async fn get_workflow_status(&self, workflow_id: &str) -> JournioResult<Option<WorkflowStatus>>;

    /// List workflows (filters optional) — ported from `listWorkflows`
    /// (`system_database.go:1184`). The `limit`/offset filtering grows as the
    /// port progresses; for now a simple cap.
    async fn list_workflows(&self, limit: i64) -> JournioResult<Vec<WorkflowStatus>>;

    /// List workflows with rich filtering and paging — ported from the
    /// `listWorkflowsOptions` branch of `listWorkflows`
    /// (`system_database.go:1184`). Backends translate the filter into their
    /// native parameterization (Postgres `ANY($n)` arrays vs. SQLite
    /// `QueryBuilder` separated lists).
    async fn list_workflows_filtered(
        &self,
        filter: &ListWorkflowsFilter,
    ) -> JournioResult<Vec<WorkflowStatus>>;

    /// Cancel the provided workflows, leaving terminal rows untouched.
    /// Returns the subset of IDs that already existed.
    async fn cancel_workflows(&self, workflow_ids: &[String]) -> JournioResult<Vec<String>>;

    /// Resume the provided workflows onto `queue_name`, or the internal queue
    /// if `None`. Returns the subset of IDs that already existed.
    async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> JournioResult<Vec<String>>;

    /// Recursively list all descendant workflows of `workflow_id`.
    async fn get_workflow_children(&self, workflow_id: &str) -> JournioResult<Vec<WorkflowStatus>>;

    /// Fork a workflow by cloning its durable state and enqueueing a new run.
    async fn fork_workflow(&self, input: ForkWorkflow) -> JournioResult<String>;

    /// Retrieve persisted queue configuration by name.
    async fn get_queue(&self, queue_name: &str) -> JournioResult<Option<QueueConfig>>;

    /// Create or replace persisted queue configuration.
    async fn upsert_queue(&self, queue: &QueueConfig) -> JournioResult<()>;

    // -- operation_outputs (step checkpointing) ----------------------------

    /// Insert or read-back a completed step — ported from
    /// `recordStepResult` / `checkStepExecution`.
    async fn record_step_output(&self, step: &StepRecord) -> JournioResult<()>;

    /// Fetch all recorded steps for replay — ported from the recovery path
    /// in `workflow.go` + `system_database.go`.
    async fn get_steps(&self, workflow_id: &str) -> JournioResult<Vec<StepRecord>>;

    // -- queues ------------------------------------------------------------

    /// Atomically dequeue the next runnable workflow for `queue_name`
    /// (Postgres: `FOR UPDATE SKIP LOCKED`). Ported from `workflowQueue.loop`.
    async fn dequeue_workflow(
        &self,
        queue_name: &str,
        executor_id: &str,
    ) -> JournioResult<Option<WorkflowStatus>>;

    /// List queue names that currently have runnable or future queued work.
    async fn list_runnable_queues(&self) -> JournioResult<Vec<String>>;

    /// List all registered queues from the `queues` table — ported from
    /// `listQueues` (`system_database.go:4434`). Used by the admin server's
    /// queue-metadata endpoint.
    async fn list_queues(&self) -> JournioResult<Vec<QueueConfig>>;

    // -- inter-workflow communication --------------------------------------
    //
    // Go folds the wait/timeout loop into the DB layer (condition variables
    // woken by a LISTEN/NOTIFY listener). The Rust port splits concerns: these
    // methods are *non-blocking* primitives, and `WorkflowContext` owns the
    // polling/timeout loop + checkpointing. LISTEN/NOTIFY can later collapse
    // the poll interval without changing this trait.

    /// `Send` a message — ported from `send` (`system_database.go:3346`).
    /// Inserts a row into `notifications`. An empty `topic` is stored as the
    /// null topic. Returns `NonExistentWorkflowError` when the destination
    /// workflow does not exist (FK violation).
    async fn send(
        &self,
        destination_id: &str,
        topic: &str,
        message: &Interchange,
    ) -> JournioResult<()>;

    /// Atomically consume the oldest unconsumed message on `topic` destined
    /// for `workflow_id` — ported from the consume half of `recv`
    /// (`system_database.go:3378`). Non-blocking: returns `None` when no
    /// message is available. The polling/timeout loop lives in
    /// [`crate::context::WorkflowContext::recv`].
    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
    ) -> JournioResult<Option<Notification>>;

    /// Optional LISTEN/NOTIFY-backed wait hint for `recv`. Backends without a
    /// wakeup path can ignore it; the caller will re-poll after the timeout.
    async fn wait_for_notification(
        &self,
        _workflow_id: &str,
        _topic: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        tokio::time::sleep(timeout).await;
        Ok(())
    }

    /// `SetEvent` — ported from `setEvent` (`system_database.go:3573`).
    /// Upserts `workflow_events` and appends `workflow_events_history`
    /// (keyed by `function_id`, the calling step id).
    async fn set_event(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Interchange,
        function_id: i32,
    ) -> JournioResult<()>;

    /// Read the current value of event `key` for `workflow_id` — ported from
    /// the query half of `getEvent` (`system_database.go:3615`). Non-blocking:
    /// returns `None` when the key is unset. The polling/timeout loop lives in
    /// [`crate::context::WorkflowContext::get_event`].
    async fn get_event_value(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> JournioResult<Option<Interchange>>;

    /// Optional LISTEN/NOTIFY-backed wait hint for `get_event`.
    async fn wait_for_event(
        &self,
        _workflow_id: &str,
        _key: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        tokio::time::sleep(timeout).await;
        Ok(())
    }

    // -- streams -----------------------------------------------------------

    /// Append one serialized value to a durable stream within a workflow.
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i32,
        serialization: Option<&str>,
    ) -> JournioResult<()>;

    /// Read stream entries from `from_offset`, ordered by offset. Returns the
    /// drained entries plus whether the stream has been closed.
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> JournioResult<(Vec<StreamEntry>, bool)>;

    /// Optional LISTEN/NOTIFY-backed wait hint for `read_stream`.
    async fn wait_for_stream(
        &self,
        _workflow_id: &str,
        _key: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        tokio::time::sleep(timeout).await;
        Ok(())
    }

    // -- recovery ----------------------------------------------------------

    /// Workflows left PENDING for this executor at startup — ported from
    /// `workflowRecovery` in `recovery.go` + `system_database.go`.
    async fn get_workflows_for_recovery(
        &self,
        executor_id: &str,
    ) -> JournioResult<Vec<WorkflowStatus>>;

    // -- scheduler ---------------------------------------------------------

    /// Create or replace a workflow schedule definition.
    async fn upsert_schedule(&self, schedule: &WorkflowSchedule) -> JournioResult<()>;

    /// Fetch a single schedule by name — ported from the
    /// `ScheduleNamePrefixes` branch of `listSchedules`
    /// (`system_database.go:4766`). Returns `None` if no schedule matches.
    async fn get_schedule(&self, schedule_name: &str) -> JournioResult<Option<WorkflowSchedule>>;

    /// List all persisted schedule definitions.
    async fn list_schedules(&self) -> JournioResult<Vec<WorkflowSchedule>>;

    /// Delete a schedule by name — ported from `deleteSchedule`
    /// (`system_database.go:4928`).
    async fn delete_schedule(&self, schedule_name: &str) -> JournioResult<()>;

    /// Update only a schedule's status — ported from `updateSchedule`
    /// (`system_database.go`). Used by pause/resume.
    async fn update_schedule_status(
        &self,
        schedule_name: &str,
        status: ScheduleStatus,
    ) -> JournioResult<()>;

    /// Update the last-fired timestamp after a successful trigger/backfill pass.
    async fn update_schedule_last_fired_at(
        &self,
        schedule_name: &str,
        fired_at: DateTime<Utc>,
    ) -> JournioResult<()>;

    // -- application versions ---------------------------------------------
    //
    // Ported from the `application_versions` helpers in
    // `system_database.go:5174`. The "latest" version is simply the row with
    // the greatest `version_timestamp`.

    /// Register a new application version (no-op if the name already exists).
    /// Ported from `createApplicationVersion`.
    async fn create_application_version(&self, version_name: &str) -> JournioResult<()>;

    /// Bump a version's timestamp, marking it as latest. Ported from
    /// `updateApplicationVersionTimestamp`.
    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp_ms: i64,
    ) -> JournioResult<()>;

    /// All registered versions, newest first — ported from
    /// `listApplicationVersions` (`system_database.go:5174`).
    async fn list_application_versions(&self) -> JournioResult<Vec<VersionInfo>>;

    /// The version with the greatest timestamp — ported from
    /// `getLatestApplicationVersion`. Returns `None` when none are registered.
    async fn get_latest_application_version(&self) -> JournioResult<Option<VersionInfo>>;

    /// Set or update the delay on a DELAYED workflow — ported from
    /// `setWorkflowDelay` (`system_database.go`). No-op for workflows not in
    /// the DELAYED status.
    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay_until: DateTime<Utc>,
    ) -> JournioResult<()>;

    /// Permanently delete the given workflows and all their associated data
    /// (operation outputs, events, event history, streams) — ported from
    /// `deleteWorkflows` (`system_database.go`). When `delete_children` is
    /// true, descendant workflows are deleted recursively first.
    async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> JournioResult<()>;

    // -- GC ---------------------------------------------------------------

    /// Delete completed workflows older than `before` — ported from
    /// `permanentDeleteWorkflows`.
    async fn delete_workflows_before(&self, before: DateTime<Utc>) -> JournioResult<u64>;
}
