//! Runtime contexts & handles — ported from `journio/journio.go` (`JOURNIOContext`,
//! `journioContext`) and `journio/workflow.go` (`workflowState`, `WorkflowHandle`).
//!
//! The Go `JOURNIOContext` is an interface with ~30 methods; here it splits into:
//!   - [`JournioContext`]     — the long-lived runtime (registry, db, loops)
//!   - [`WorkflowContext`] — per-workflow execution state (id, step counter)
//!
//! The MVP execution path is implemented here. Later-phase primitives remain
//! as `todo!()` until their corresponding storage/runtime layers land.

use std::str::FromStr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::Config;
use crate::error::{JournioError, JournioErrorCode, JournioResult};
use crate::system_db::{ForkWorkflow, InitWorkflow, SystemDatabase};
use crate::types::{
    ListWorkflowsFilter, QueueConfig, ScheduleStatus, ScheduledWorkflowInput, StepRecord,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
use crate::value::Interchange;
use crate::workflow::{Registry, Step, Workflow};

/// Poll interval for the context-layer wait loops (`recv` / `get_event` /
/// `get_result`). Go wakes these via LISTEN/NOTIFY; the Rust port polls until
/// the notification listener lands (Phase 4). Matches the `get_result` cadence.
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const LISTENER_WAIT_CAP: Duration = Duration::from_secs(1);
const PATCH_PREFIX: &str = "journio.patch-";
pub(crate) const INTERNAL_QUEUE_NAME: &str = "_journio_internal_queue";
const STREAM_CLOSED_SENTINEL: &str = "__JOURNIO_STREAM_CLOSED__";
const DEBOUNCER_TOPIC: &str = "_journio_debouncer_topic";
const INTERNAL_DEBOUNCER_WORKFLOW_NAME: &str = "__journio_internal_debouncer_workflow";

/// Queue enqueue parameters — a compact Phase 2 subset of Go's
/// `WorkflowOption` / `EnqueueOption` surface.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub workflow_id: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub timeout: Option<Duration>,
    pub delay_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ScheduleOptions {
    pub workflow_class_name: Option<String>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
    pub status: Option<ScheduleStatus>,
    pub last_fired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReadStreamOptions {
    pub snapshot: bool,
    pub from_offset: i64,
}

#[derive(Debug, Clone, Default)]
pub struct DebounceOptions {
    pub workflow_id: Option<String>,
    pub debounce_timeout: Option<Duration>,
    pub queue_name: Option<String>,
    pub application_version: Option<String>,
    pub deduplication_id: Option<String>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub workflow_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    pub concurrency: Option<i32>,
    pub worker_concurrency: Option<i32>,
    pub rate_limit_max: Option<i32>,
    pub rate_limit_period: Option<Duration>,
    pub priority_enabled: bool,
    pub partition_queue: bool,
    pub polling_interval: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
pub struct ForkWorkflowOptions {
    pub workflow_id: Option<String>,
    pub start_step: u32,
    pub application_version: Option<String>,
    pub queue_name: Option<String>,
    pub queue_partition_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DebouncerWorkflowInput {
    initial_input: Interchange,
    target_workflow_name: String,
    target_workflow_id: String,
    delay_ms: u64,
    timeout_ms: Option<u64>,
    queue_name: Option<String>,
    application_version: Option<String>,
    deduplication_id: Option<String>,
    priority: i32,
    queue_partition_key: Option<String>,
    workflow_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DebounceMessage {
    input: Interchange,
    delay_ms: u64,
    id: String,
}

/// Per-workflow execution state — ported from `workflowState` (`workflow.go`).
/// Cheap to clone (all `Arc`).
#[derive(Clone)]
pub struct WorkflowContext {
    pub(crate) inner: Arc<WorkflowCtxInner>,
}

#[allow(dead_code)]
pub(crate) struct WorkflowCtxInner {
    workflow_id: String,
    step_counter: AtomicI32,
    step_execution_depth: AtomicI32,
    /// Weak back-reference to the runtime — avoids reference cycles.
    journio: Weak<JournioContext>,
    /// Durable cancellation (Go's ctx.Done() for the workflow).
    cancel: CancellationToken,
    authenticated_user: RwLock<Option<String>>,
    assumed_role: RwLock<Option<String>>,
}

impl WorkflowContext {
    pub fn workflow_id(&self) -> &str {
        &self.inner.workflow_id
    }

    pub fn current_step_id(&self) -> i32 {
        self.inner.step_counter.load(Ordering::SeqCst)
    }

    pub fn application_version(&self) -> JournioResult<String> {
        Ok(self.journio()?.application_version())
    }

    pub fn executor_id(&self) -> JournioResult<String> {
        Ok(self.journio()?.executor_id())
    }

    /// Allocate the next step id — ported from `workflowState.nextStepID`.
    pub fn next_step_id(&self) -> i32 {
        self.inner.step_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.clone()
    }

    /// `RunAsStep` — ported from `RunAsStep` (`workflow.go:1937`).
    pub async fn run_as_step(&self, step: Arc<dyn Step>) -> JournioResult<Interchange> {
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                step.name(),
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    step.name(),
                    message,
                ));
            }
            return decode_interchange(
                &journio,
                self.workflow_id(),
                step_id,
                step.name(),
                recorded.output,
            );
        }

        let _step_scope = self.enter_step_execution();
        match step.run(self).await {
            Ok(output) => {
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: step.name().to_string(),
                        output: Some(encode_interchange(&journio, &output)?),
                        error: None,
                        child_workflow_id: None,
                    })
                    .await?;
                Ok(output)
            }
            Err(err) => {
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: step.name().to_string(),
                        output: None,
                        error: Some(err.message.clone()),
                        child_workflow_id: None,
                    })
                    .await?;
                Err(err)
            }
        }
    }

    /// `RunWorkflow` (inside a workflow, for child workflows) — `workflow.go:1028`.
    pub async fn run_workflow(
        &self,
        name: &str,
        input: Interchange,
    ) -> JournioResult<WorkflowHandle> {
        let function_id = self.next_step_id();
        let function_name = format!("child::{name}");
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(function_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                function_id,
                &function_name,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    function_id,
                    &function_name,
                    message,
                ));
            }
            if let Some(child_workflow_id) = recorded.child_workflow_id {
                return Ok(WorkflowHandle {
                    workflow_id: child_workflow_id,
                    journio: Arc::downgrade(&journio),
                });
            }
            return Err(JournioError::new(
                JournioErrorCode::WorkflowExecutionError,
                format!(
                    "recorded child workflow step {} in workflow {} is missing child_workflow_id",
                    function_id,
                    self.workflow_id()
                ),
            ));
        }

        journio
            .start_workflow(
                name,
                input,
                WorkflowLaunch::Immediate,
                None,
                None,
                Some(self.workflow_id().to_string()),
                Some(ChildCheckpoint {
                    parent_workflow_id: self.workflow_id().to_string(),
                    function_id,
                    function_name,
                }),
            )
            .await
    }

    /// `Sleep` — durable sleep that survives recovery. Ported from
    /// `sysDB.sleep` (`system_database.go:2964`). Records the wake-up deadline
    /// as a checkpointed step (`journio.sleep`); on replay, decodes the recorded
    /// deadline and sleeps only the remaining duration.
    pub async fn sleep(&self, duration: Duration) -> JournioResult<Duration> {
        const STEP_NAME: &str = "journio.sleep";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            let decoded = decode_interchange(
                &journio,
                self.workflow_id(),
                step_id,
                STEP_NAME,
                recorded.output,
            )?;
            let deadline = parse_deadline(&decoded)?;
            let remaining = remaining_until(deadline);
            tokio::time::sleep(remaining).await;
            return Ok(remaining);
        }

        let deadline = Utc::now()
            + chrono::Duration::from_std(duration)
                .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX / 1_000_000));
        let encoded =
            encode_interchange(&journio, &serde_json::Value::String(deadline.to_rfc3339()))?;
        journio
            .system_db
            .record_step_output(&StepRecord {
                workflow_uuid: self.workflow_id().to_string(),
                function_id: step_id,
                function_name: STEP_NAME.to_string(),
                output: Some(encoded),
                error: None,
                child_workflow_id: None,
            })
            .await?;

        let remaining = remaining_until(deadline);
        tokio::time::sleep(remaining).await;
        Ok(remaining)
    }

    /// `Recv` — ported from `Recv` (`workflow.go:2477`). Polls for a message
    /// on `topic` up to `timeout`. The first message found is recorded as a
    /// checkpointed step (`journio.recv`) so replay returns the same value;
    /// timeouts are recorded as errors for the same reason.
    pub async fn recv(&self, topic: &str, timeout: Duration) -> JournioResult<Interchange> {
        const STEP_NAME: &str = "journio.recv";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return decode_interchange(
                &journio,
                self.workflow_id(),
                step_id,
                STEP_NAME,
                recorded.output,
            );
        }

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(notification) = journio
                .system_db
                .consume_notification(self.workflow_id(), topic)
                .await?
            {
                let encoded = encode_interchange(&journio, &notification.message)?;
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: STEP_NAME.to_string(),
                        output: Some(encoded),
                        error: None,
                        child_workflow_id: None,
                    })
                    .await?;
                return Ok(notification.message);
            }
            if Instant::now() >= deadline {
                let err = ctx_step_error(
                    JournioErrorCode::TimeoutError,
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    format!("no message received on topic {topic:?} within {timeout:?}"),
                );
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: STEP_NAME.to_string(),
                        output: None,
                        error: Some(err.message.clone()),
                        child_workflow_id: None,
                    })
                    .await?;
                return Err(err);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            journio
                .system_db
                .wait_for_notification(self.workflow_id(), topic, remaining.min(LISTENER_WAIT_CAP))
                .await?;
        }
    }

    /// `Send` (from within a workflow) — ported from `Send` (`workflow.go:2402`).
    /// Runs as a checkpointed step (`journio.send`) so the side effect happens
    /// exactly once across replays.
    pub async fn send(&self, dest: &str, msg: Interchange, topic: &str) -> JournioResult<()> {
        const STEP_NAME: &str = "journio.send";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return Ok(());
        }

        journio.system_db.send(dest, topic, &msg).await?;
        journio
            .system_db
            .record_step_output(&StepRecord {
                workflow_uuid: self.workflow_id().to_string(),
                function_id: step_id,
                function_name: STEP_NAME.to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
            })
            .await?;
        Ok(())
    }

    /// `SetEvent` — ported from `SetEvent` (`workflow.go:2576`). Runs as a
    /// checkpointed step (`journio.setEvent`).
    pub async fn set_event(&self, key: &str, value: Interchange) -> JournioResult<()> {
        const STEP_NAME: &str = "journio.setEvent";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return Ok(());
        }

        journio
            .system_db
            .set_event(self.workflow_id(), key, &value, step_id)
            .await?;
        journio
            .system_db
            .record_step_output(&StepRecord {
                workflow_uuid: self.workflow_id().to_string(),
                function_id: step_id,
                function_name: STEP_NAME.to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
            })
            .await?;
        Ok(())
    }

    /// `GetEvent` — ported from `GetEvent` (`workflow.go:2634`). Polls for the
    /// value of `key` on `target` workflow up to `timeout`. The first value
    /// found is recorded as a checkpointed step (`journio.getEvent`); timeouts are
    /// recorded as errors.
    pub async fn get_event(
        &self,
        target: &str,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<Interchange> {
        const STEP_NAME: &str = "journio.getEvent";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return decode_interchange(
                &journio,
                self.workflow_id(),
                step_id,
                STEP_NAME,
                recorded.output,
            );
        }

        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = journio.system_db.get_event_value(target, key).await? {
                let encoded = encode_interchange(&journio, &value)?;
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: STEP_NAME.to_string(),
                        output: Some(encoded),
                        error: None,
                        child_workflow_id: None,
                    })
                    .await?;
                return Ok(value);
            }
            if Instant::now() >= deadline {
                let err = ctx_step_error(
                    JournioErrorCode::TimeoutError,
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    format!("event {key:?} not set for workflow {target:?} within {timeout:?}"),
                );
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: STEP_NAME.to_string(),
                        output: None,
                        error: Some(err.message.clone()),
                        child_workflow_id: None,
                    })
                    .await?;
                return Err(err);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            journio
                .system_db
                .wait_for_event(target, key, remaining.min(LISTENER_WAIT_CAP))
                .await?;
        }
    }

    /// `WriteStream` — append a durable stream value exactly once.
    pub async fn write_stream(&self, key: &str, value: Interchange) -> JournioResult<()> {
        const STEP_NAME: &str = "journio.writeStream";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return Ok(());
        }

        let encoded = encode_interchange(&journio, &value)?;
        journio
            .system_db
            .write_stream(
                self.workflow_id(),
                key,
                &encoded,
                step_id,
                Some(journio.config.serializer.name()),
            )
            .await?;
        journio
            .system_db
            .record_step_output(&StepRecord {
                workflow_uuid: self.workflow_id().to_string(),
                function_id: step_id,
                function_name: STEP_NAME.to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
            })
            .await?;
        Ok(())
    }

    /// `CloseStream` — closes a durable stream by writing the sentinel row.
    pub async fn close_stream(&self, key: &str) -> JournioResult<()> {
        const STEP_NAME: &str = "journio.closeStream";
        let step_id = self.next_step_id();
        let journio = self.journio()?;

        if let Some(recorded) = self.recorded_step(step_id).await? {
            validate_recorded_step(
                self.workflow_id(),
                step_id,
                STEP_NAME,
                &recorded.function_name,
            )?;
            if let Some(message) = recorded.error {
                return Err(recorded_step_error(
                    self.workflow_id(),
                    step_id,
                    STEP_NAME,
                    message,
                ));
            }
            return Ok(());
        }

        journio
            .system_db
            .write_stream(
                self.workflow_id(),
                key,
                STREAM_CLOSED_SENTINEL,
                step_id,
                None,
            )
            .await?;
        journio
            .system_db
            .record_step_output(&StepRecord {
                workflow_uuid: self.workflow_id().to_string(),
                function_id: step_id,
                function_name: STEP_NAME.to_string(),
                output: None,
                error: None,
                child_workflow_id: None,
            })
            .await?;
        Ok(())
    }

    /// Debounce workflow execution by `key`, coalescing repeated invocations
    /// into a single target workflow run that receives the latest input.
    pub async fn debounce_workflow(
        &self,
        workflow_name: &str,
        key: &str,
        delay: Duration,
        input: Interchange,
        mut options: DebounceOptions,
    ) -> JournioResult<WorkflowHandle> {
        let target_workflow_id = match options.workflow_id.take() {
            Some(value) => value,
            None => durable_uuid_step(self, "journio.debounce.assignWorkflowID").await?,
        };
        let message_id = durable_uuid_step(self, "journio.debounce.assignMessageID").await?;
        let journio = self.journio()?;
        journio
            .debounce_workflow_inner(
                workflow_name,
                key,
                delay,
                input,
                options,
                target_workflow_id,
                message_id,
            )
            .await
    }

    /// `Patch` / `DeprecatePatch` — ported from `workflow.go` patching system.
    pub async fn patch(&self, patch_name: &str) -> JournioResult<bool> {
        let journio = self.journio()?;
        validate_patching_enabled(&journio)?;
        validate_patch_name(patch_name)?;
        validate_not_within_step(self)?;

        let step_id = self.peek_next_step_id();
        let prefixed_patch_name = format!("{PATCH_PREFIX}{patch_name}");

        match self.recorded_step(step_id).await? {
            Some(recorded) if recorded.function_name == prefixed_patch_name => {
                self.next_step_id();
                Ok(true)
            }
            Some(_) => Ok(false),
            None => {
                journio
                    .system_db
                    .record_step_output(&StepRecord {
                        workflow_uuid: self.workflow_id().to_string(),
                        function_id: step_id,
                        function_name: prefixed_patch_name,
                        output: None,
                        error: None,
                        child_workflow_id: None,
                    })
                    .await?;
                self.next_step_id();
                Ok(true)
            }
        }
    }

    /// `DeprecatePatch` — ported from `workflow.go` patching system.
    pub async fn deprecate_patch(&self, patch_name: &str) -> JournioResult<()> {
        let journio = self.journio()?;
        validate_patching_enabled(&journio)?;
        validate_patch_name(patch_name)?;
        validate_not_within_step(self)?;

        let step_id = self.peek_next_step_id();
        let prefixed_patch_name = format!("{PATCH_PREFIX}{patch_name}");

        if self
            .recorded_step(step_id)
            .await?
            .is_some_and(|recorded| recorded.function_name == prefixed_patch_name)
        {
            self.next_step_id();
        }

        Ok(())
    }

    fn journio(&self) -> JournioResult<Arc<JournioContext>> {
        self.inner.journio.upgrade().ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                "Journio runtime is no longer available",
            )
        })
    }

    async fn recorded_step(&self, step_id: i32) -> JournioResult<Option<StepRecord>> {
        let journio = self.journio()?;
        let steps = journio.system_db.get_steps(self.workflow_id()).await?;
        Ok(steps.into_iter().find(|step| step.function_id == step_id))
    }

    fn peek_next_step_id(&self) -> i32 {
        self.inner.step_counter.load(Ordering::SeqCst) + 1
    }

    fn is_within_step(&self) -> bool {
        self.inner.step_execution_depth.load(Ordering::SeqCst) > 0
    }

    fn enter_step_execution(&self) -> StepExecutionScope<'_> {
        self.inner
            .step_execution_depth
            .fetch_add(1, Ordering::SeqCst);
        StepExecutionScope { ctx: self }
    }
}

struct StepExecutionScope<'a> {
    ctx: &'a WorkflowContext,
}

impl Drop for StepExecutionScope<'_> {
    fn drop(&mut self) {
        self.ctx
            .inner
            .step_execution_depth
            .fetch_sub(1, Ordering::SeqCst);
    }
}

/// Handle returned by `RunWorkflow` — ported from `WorkflowHandle[R]`
/// (`workflow.go`). Erased (no type param) because inputs/outputs are
/// `Interchange`; typed users downcast via serde.
#[derive(Clone)]
pub struct WorkflowHandle {
    pub workflow_id: String,
    pub(crate) journio: Weak<JournioContext>,
}

impl WorkflowHandle {
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// `GetResult` — ported from `baseWorkflowHandle.GetResult`.
    pub async fn get_result(&self, timeout: Option<Duration>) -> JournioResult<Interchange> {
        let deadline = timeout.and_then(|value| Instant::now().checked_add(value));

        loop {
            let status = match self.get_status().await {
                Ok(status) => status,
                Err(err) if err.code == JournioErrorCode::NonExistentWorkflowError => {
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            return Err(workflow_terminal_error(
                                JournioErrorCode::TimeoutError,
                                &self.workflow_id,
                                format!("timed out waiting for workflow {}", self.workflow_id),
                            ));
                        }
                    }
                    sleep(Duration::from_millis(25)).await;
                    continue;
                }
                Err(err) => return Err(err),
            };
            match status.status {
                WorkflowStatusType::Success => {
                    return Ok(status.output.unwrap_or(Interchange::Null));
                }
                WorkflowStatusType::Error => {
                    return Err(workflow_terminal_error(
                        JournioErrorCode::WorkflowExecutionError,
                        &self.workflow_id,
                        status.error.unwrap_or_else(|| {
                            format!("workflow {} completed with ERROR", self.workflow_id)
                        }),
                    ));
                }
                WorkflowStatusType::Cancelled => {
                    return Err(workflow_terminal_error(
                        JournioErrorCode::WorkflowCancelled,
                        &self.workflow_id,
                        status.error.unwrap_or_else(|| {
                            format!("workflow {} was cancelled", self.workflow_id)
                        }),
                    ));
                }
                WorkflowStatusType::MaxRecoveryAttemptsExceeded => {
                    return Err(workflow_terminal_error(
                        JournioErrorCode::DeadLetterQueueError,
                        &self.workflow_id,
                        status.error.unwrap_or_else(|| {
                            format!(
                                "workflow {} exceeded the maximum number of recovery attempts",
                                self.workflow_id
                            )
                        }),
                    ));
                }
                WorkflowStatusType::Pending
                | WorkflowStatusType::Enqueued
                | WorkflowStatusType::Delayed => {
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            return Err(workflow_terminal_error(
                                JournioErrorCode::TimeoutError,
                                &self.workflow_id,
                                format!("timed out waiting for workflow {}", self.workflow_id),
                            ));
                        }
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }

    /// `GetStatus` — ported from `GetStatus`.
    pub async fn get_status(&self) -> JournioResult<WorkflowStatus> {
        let journio = self.journio()?;
        journio
            .system_db
            .get_workflow_status(&self.workflow_id)
            .await?
            .ok_or_else(|| crate::error::constructors::non_existent_workflow(&self.workflow_id))
    }

    pub async fn cancel(&self) -> JournioResult<bool> {
        let journio = self.journio()?;
        journio.cancel_workflow(&self.workflow_id).await
    }

    pub async fn resume(&self, queue_name: Option<&str>) -> JournioResult<bool> {
        let journio = self.journio()?;
        journio.resume_workflow(&self.workflow_id, queue_name).await
    }

    pub async fn fork(&self, options: ForkWorkflowOptions) -> JournioResult<WorkflowHandle> {
        let journio = self.journio()?;
        journio.fork_workflow(&self.workflow_id, options).await
    }

    fn journio(&self) -> JournioResult<Arc<JournioContext>> {
        self.journio.upgrade().ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                "Journio runtime is no longer available",
            )
        })
    }
}

/// The long-lived Journio runtime — ported from `journioContext` (`journio/journio.go:221`).
///
/// Construct with [`JournioContext::new`] (which calls `process_config`), then
/// [`JournioContext::launch`] to start background loops and run recovery.
pub struct JournioContext {
    pub config: Config,
    pub registry: Registry,
    pub(crate) system_db: Arc<dyn SystemDatabase>,
    pub(crate) cancel: CancellationToken,
    pub(crate) started: std::sync::atomic::AtomicBool,
}

impl JournioContext {
    /// Construct + validate config + resolve backend. Ported from
    /// `NewJOURNIOContext`.
    pub async fn new(mut config: Config) -> JournioResult<Arc<Self>> {
        crate::config::process_config(&mut config)?;
        let system_db = resolve_backend(&config)?;
        let ctx = Arc::new(Self {
            config,
            registry: Registry::new(),
            system_db,
            cancel: CancellationToken::new(),
            started: false.into(),
        });
        ctx.register_workflow(Arc::new(InternalDebouncerWorkflow))?;
        Ok(ctx)
    }

    /// Register a workflow before launch — ported from `RegisterWorkflow`.
    pub fn register_workflow(&self, wf: Arc<dyn Workflow>) -> JournioResult<()> {
        self.registry.register(wf)
    }

    pub fn application_version(&self) -> String {
        self.config.application_version.clone().unwrap_or_default()
    }

    pub fn executor_id(&self) -> String {
        self.config.executor_id.clone().unwrap_or_default()
    }

    /// Start runtime: migrate, launch DB loops, recover local executor's
    /// workflows. Ported from `Launch`.
    pub async fn launch(self: &Arc<Self>) -> JournioResult<()> {
        self.system_db.migrate().await?;
        self.system_db.launch().await?;
        self.recover().await?;
        self.started.store(true, Ordering::SeqCst);
        self.spawn_background_loops();
        Ok(())
    }

    /// Graceful shutdown — ported from `Shutdown`.
    pub async fn shutdown(&self, _timeout: Duration) -> JournioResult<()> {
        self.cancel.cancel();
        self.system_db.shutdown().await
    }

    /// Signal the runtime to stop accepting new work (scheduler/queue loops
    /// observe the cancellation token and exit). Does **not** close the DB
    /// pool — that's `shutdown`. Ported from Go's `/deactivate` handler.
    pub fn deactivate(&self) {
        self.cancel.cancel();
    }

    /// Enqueue a workflow from outside a workflow context. Ported from
    /// `RunWorkflow` at the top level (`workflow.go:1028`).
    pub async fn run_workflow(
        self: &Arc<Self>,
        name: &str,
        input: Interchange,
    ) -> JournioResult<WorkflowHandle> {
        self.start_workflow(
            name,
            input,
            WorkflowLaunch::Immediate,
            None,
            None,
            None,
            None,
        )
        .await
    }

    pub async fn start_workflow_background(
        self: &Arc<Self>,
        name: &str,
        input: Interchange,
        options: EnqueueOptions,
    ) -> JournioResult<WorkflowHandle> {
        self.start_workflow(
            name,
            input,
            WorkflowLaunch::Background,
            None,
            Some(options),
            None,
            None,
        )
        .await
    }

    /// Enqueue a workflow for deferred execution on `queue_name`.
    pub async fn enqueue_workflow(
        self: &Arc<Self>,
        queue_name: &str,
        name: &str,
        input: Interchange,
        options: EnqueueOptions,
    ) -> JournioResult<WorkflowHandle> {
        if queue_name.is_empty() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                "queue_name cannot be empty",
            ));
        }
        let launch = if options.delay_until.is_some() {
            WorkflowLaunch::Delayed
        } else {
            WorkflowLaunch::Enqueued
        };
        self.start_workflow(
            name,
            input,
            launch,
            Some(queue_name.to_string()),
            Some(options),
            None,
            None,
        )
        .await
    }

    /// Register or replace a persisted cron schedule.
    pub async fn register_schedule(
        self: &Arc<Self>,
        schedule_name: &str,
        workflow_name: &str,
        cron_spec: &str,
        context: Interchange,
        options: ScheduleOptions,
    ) -> JournioResult<()> {
        if self.registry.get(workflow_name).is_none() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("workflow {workflow_name} is not registered"),
            ));
        }
        validate_schedule_spec(cron_spec)?;

        let schedule = WorkflowSchedule {
            schedule_id: Uuid::new_v4().to_string(),
            schedule_name: schedule_name.to_string(),
            workflow_name: workflow_name.to_string(),
            workflow_class_name: options.workflow_class_name,
            schedule: cron_spec.to_string(),
            status: options.status.unwrap_or(ScheduleStatus::Active),
            context,
            last_fired_at: options.last_fired_at.or_else(|| Some(Utc::now())),
            automatic_backfill: options.automatic_backfill,
            cron_timezone: options.cron_timezone,
            queue_name: options.queue_name,
        };
        self.system_db.upsert_schedule(&schedule).await
    }

    pub async fn register_queue(
        self: &Arc<Self>,
        queue_name: &str,
        options: QueueOptions,
    ) -> JournioResult<QueueConfig> {
        validate_queue_options(queue_name, &options)?;
        let queue = QueueConfig {
            queue_id: Uuid::new_v4().to_string(),
            name: queue_name.to_string(),
            concurrency: options.concurrency,
            worker_concurrency: options.worker_concurrency,
            rate_limit_max: options.rate_limit_max,
            rate_limit_period_sec: options.rate_limit_period.map(|d| d.as_secs_f64()),
            priority_enabled: options.priority_enabled,
            partition_queue: options.partition_queue,
            polling_interval_sec: options
                .polling_interval
                .unwrap_or(Duration::from_secs(1))
                .as_secs_f64(),
        };
        self.system_db.upsert_queue(&queue).await?;
        self.system_db.get_queue(queue_name).await?.ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                format!("queue {queue_name} missing after registration"),
            )
        })
    }

    pub async fn cancel_workflow(&self, workflow_id: &str) -> JournioResult<bool> {
        Ok(self
            .system_db
            .cancel_workflows(&[workflow_id.to_string()])
            .await?
            .iter()
            .any(|candidate| candidate == workflow_id))
    }

    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> JournioResult<Vec<String>> {
        self.system_db.cancel_workflows(workflow_ids).await
    }

    pub async fn resume_workflow(
        &self,
        workflow_id: &str,
        queue_name: Option<&str>,
    ) -> JournioResult<bool> {
        Ok(self
            .system_db
            .resume_workflows(&[workflow_id.to_string()], queue_name)
            .await?
            .iter()
            .any(|candidate| candidate == workflow_id))
    }

    pub async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> JournioResult<Vec<String>> {
        self.system_db
            .resume_workflows(workflow_ids, queue_name)
            .await
    }

    pub async fn get_workflow_children(
        &self,
        workflow_id: &str,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        self.system_db.get_workflow_children(workflow_id).await
    }

    pub async fn fork_workflow(
        self: &Arc<Self>,
        original_workflow_id: &str,
        options: ForkWorkflowOptions,
    ) -> JournioResult<WorkflowHandle> {
        if original_workflow_id.is_empty() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                "original workflow ID cannot be empty",
            ));
        }
        if options.queue_partition_key.is_some() && options.queue_name.is_none() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                "queue partition key requires a queue name",
            ));
        }
        let start_step = i32::try_from(options.start_step).map_err(|_| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                format!("start step too large: {}", options.start_step),
            )
        })?;
        let workflow_id = self
            .system_db
            .fork_workflow(ForkWorkflow {
                original_workflow_id: original_workflow_id.to_string(),
                forked_workflow_id: options.workflow_id,
                start_step,
                application_version: options.application_version,
                queue_name: options.queue_name,
                queue_partition_key: options.queue_partition_key,
            })
            .await?;
        Ok(self.workflow_handle(workflow_id))
    }

    /// `Send` from outside a workflow — ported from the non-workflow branch of
    /// `Send` (`workflow.go:2402`). Inserts the notification directly (no
    /// checkpoint; the caller is not durably executing).
    pub async fn send(
        self: &Arc<Self>,
        destination_id: &str,
        message: Interchange,
        topic: &str,
    ) -> JournioResult<()> {
        self.system_db.send(destination_id, topic, &message).await
    }

    /// `GetEvent` from outside a workflow — ported from the optional-state
    /// branch of `GetEvent` (`workflow.go:2634`). Polls for the value without
    /// checkpointing (the caller is not durably executing). Returns
    /// `Interchange::Null` on timeout.
    pub async fn get_event(
        self: &Arc<Self>,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<Interchange> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(value) = self
                .system_db
                .get_event_value(target_workflow_id, key)
                .await?
            {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Ok(Interchange::Null);
            }
            sleep(POLL_INTERVAL).await;
        }
    }

    /// Debounce workflow execution by `key`, coalescing repeated invocations
    /// into a single target workflow run that receives the latest input.
    pub async fn debounce_workflow(
        self: &Arc<Self>,
        workflow_name: &str,
        key: &str,
        delay: Duration,
        input: Interchange,
        mut options: DebounceOptions,
    ) -> JournioResult<WorkflowHandle> {
        let target_workflow_id = options
            .workflow_id
            .take()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let message_id = Uuid::new_v4().to_string();
        self.debounce_workflow_inner(
            workflow_name,
            key,
            delay,
            input,
            options,
            target_workflow_id,
            message_id,
        )
        .await
    }

    /// Read a durable stream until it is closed, the workflow becomes
    /// inactive, or snapshot mode drains the current contents.
    pub async fn read_stream(
        self: &Arc<Self>,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> JournioResult<(Vec<Interchange>, bool)> {
        let mut offset = options.from_offset;
        let mut values = Vec::new();

        loop {
            let (entries, closed) = self.system_db.read_stream(workflow_id, key, offset).await?;
            for entry in entries {
                offset = entry.offset + 1;
                values.push(decode_stream_entry(self, workflow_id, key, &entry)?);
            }

            if closed {
                return Ok((values, true));
            }
            if options.snapshot {
                return Ok((values, false));
            }

            let Some(status) = self.system_db.get_workflow_status(workflow_id).await? else {
                return Err(crate::error::constructors::non_existent_workflow(
                    workflow_id,
                ));
            };
            if !stream_workflow_is_active(status.status) {
                return Ok((values, true));
            }

            self.system_db
                .wait_for_stream(workflow_id, key, LISTENER_WAIT_CAP)
                .await?;
        }
    }

    /// Dequeue and execute one queued workflow from `queue_name`.
    pub async fn run_queue_once(
        self: &Arc<Self>,
        queue_name: &str,
    ) -> JournioResult<Option<WorkflowHandle>> {
        let executor_id = self.config.executor_id.clone().unwrap_or_default();
        let Some(status) = self
            .system_db
            .dequeue_workflow(queue_name, &executor_id)
            .await?
        else {
            return Ok(None);
        };

        let handle = self.workflow_handle(status.id.clone());
        let input = status.input.unwrap_or(Interchange::Null);
        self.execute_workflow(&status.id, &status.name, input)
            .await?;
        Ok(Some(handle))
    }

    pub async fn run_all_queues_once(self: &Arc<Self>) -> JournioResult<usize> {
        let queue_names = self.system_db.list_runnable_queues().await?;
        let mut processed = 0usize;
        for queue_name in queue_names {
            while self.run_queue_once(&queue_name).await?.is_some() {
                processed += 1;
            }
        }
        Ok(processed)
    }

    /// Recover PENDING workflows owned by this executor — ported from
    /// `workflowRecovery` in `recovery.go`.
    async fn recover(self: &Arc<Self>) -> JournioResult<()> {
        let pending = self
            .system_db
            .get_workflows_for_recovery(&self.config.executor_id.clone().unwrap_or_default())
            .await?;
        tracing::info!(count = pending.len(), "recovered workflows");
        for workflow in pending {
            let Some(input) = workflow.input.clone() else {
                tracing::warn!(workflow_id = %workflow.id, "skipping recovery for workflow without input");
                continue;
            };
            self.execute_workflow(&workflow.id, &workflow.name, input)
                .await?;
        }
        Ok(())
    }

    /// Expose the steps recorded for a workflow (admin/debug).
    pub async fn get_workflow_steps(&self, workflow_id: &str) -> JournioResult<Vec<StepRecord>> {
        self.system_db.get_steps(workflow_id).await
    }

    /// List workflows with rich filtering — delegates to the system DB.
    /// Public accessor for the admin server.
    pub async fn list_workflows_filtered(
        &self,
        filter: &ListWorkflowsFilter,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        self.system_db.list_workflows_filtered(filter).await
    }

    /// Recover PENDING workflows for the given executors — ported from
    /// `recoverPendingWorkflows` (`recovery.go`). The admin server exposes
    /// this via `POST /journio-workflow-recovery`. Returns the IDs of workflows
    /// that were re-executed.
    pub async fn recover_workflows(
        self: &Arc<Self>,
        executor_ids: &[String],
    ) -> JournioResult<Vec<String>> {
        let mut recovered = Vec::new();
        for executor_id in executor_ids {
            let pending = self
                .system_db
                .get_workflows_for_recovery(executor_id)
                .await?;
            for workflow in pending {
                let Some(input) = workflow.input.clone() else {
                    tracing::warn!(
                        workflow_id = %workflow.id,
                        "skipping recovery for workflow without input"
                    );
                    continue;
                };
                self.execute_workflow(&workflow.id, &workflow.name, input)
                    .await?;
                recovered.push(workflow.id);
            }
        }
        Ok(recovered)
    }

    /// Cancel all PENDING/ENQUEUED/DELAYED workflows created before `cutoff` —
    /// ported from `cancelAllBefore` (`system_database.go`). The admin server
    /// exposes this via `POST /journio-global-timeout`.
    pub async fn cancel_all_before(&self, cutoff: DateTime<Utc>) -> JournioResult<()> {
        let workflows = self
            .system_db
            .list_workflows_filtered(&ListWorkflowsFilter {
                end_time: Some(cutoff),
                statuses: vec![
                    WorkflowStatusType::Pending,
                    WorkflowStatusType::Enqueued,
                    WorkflowStatusType::Delayed,
                ],
                limit: Some(0),
                ..Default::default()
            })
            .await?;
        let ids: Vec<String> = workflows.into_iter().map(|wf| wf.id).collect();
        if !ids.is_empty() {
            self.system_db.cancel_workflows(&ids).await?;
        }
        Ok(())
    }

    /// List all registered queue configurations — ported from
    /// `queueRunner.listQueues`. The admin server exposes this via
    /// `GET /journio-workflow-queues-metadata`.
    pub async fn list_queue_metadata(&self) -> JournioResult<Vec<QueueConfig>> {
        self.system_db.list_queues().await
    }

    /// Construct a handle for an already-known workflow id.
    pub fn workflow_handle(self: &Arc<Self>, workflow_id: impl Into<String>) -> WorkflowHandle {
        WorkflowHandle {
            workflow_id: workflow_id.into(),
            journio: Arc::downgrade(self),
        }
    }

    pub fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    /// Borrow the system database — exposed for the admin server and advanced
    /// callers that need direct persistence access.
    pub fn system_db(&self) -> &Arc<dyn SystemDatabase> {
        &self.system_db
    }

    async fn debounce_workflow_inner(
        self: &Arc<Self>,
        workflow_name: &str,
        key: &str,
        delay: Duration,
        input: Interchange,
        options: DebounceOptions,
        target_workflow_id: String,
        message_id: String,
    ) -> JournioResult<WorkflowHandle> {
        if self.registry.get(workflow_name).is_none() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("workflow {workflow_name} is not registered"),
            ));
        }

        let debouncer_key = debounce_deduplication_key(workflow_name, key);
        let debouncer_input = DebouncerWorkflowInput {
            initial_input: input.clone(),
            target_workflow_name: workflow_name.to_string(),
            target_workflow_id: target_workflow_id.clone(),
            delay_ms: duration_to_millis_u64(delay)?,
            timeout_ms: options
                .debounce_timeout
                .map(duration_to_millis_u64)
                .transpose()?,
            queue_name: options.queue_name,
            application_version: options.application_version,
            deduplication_id: options.deduplication_id,
            priority: options.priority,
            queue_partition_key: options.queue_partition_key,
            workflow_timeout_ms: options
                .workflow_timeout
                .map(duration_to_millis_u64)
                .transpose()?,
        };

        loop {
            let result = self
                .enqueue_workflow(
                    INTERNAL_QUEUE_NAME,
                    INTERNAL_DEBOUNCER_WORKFLOW_NAME,
                    serde_json::to_value(&debouncer_input).map_err(|err| {
                        JournioError::new(
                            JournioErrorCode::InitializationError,
                            format!("failed to serialize debouncer input: {err}"),
                        )
                    })?,
                    EnqueueOptions {
                        deduplication_id: Some(debouncer_key.clone()),
                        ..Default::default()
                    },
                )
                .await;

            match result {
                Ok(_) => {
                    return Ok(self.workflow_handle(target_workflow_id.clone()));
                }
                Err(err) if err.code == JournioErrorCode::QueueDeduplicated => {
                    let Some(existing) = self
                        .find_existing_debouncer_workflow(&debouncer_key)
                        .await?
                    else {
                        continue;
                    };

                    self.send(
                        &existing.id,
                        serde_json::to_value(DebounceMessage {
                            input: input.clone(),
                            delay_ms: duration_to_millis_u64(delay)?,
                            id: message_id.clone(),
                        })
                        .map_err(|ser_err| {
                            JournioError::new(
                                JournioErrorCode::InitializationError,
                                format!("failed to serialize debounce message: {ser_err}"),
                            )
                        })?,
                        DEBOUNCER_TOPIC,
                    )
                    .await?;

                    let ack = self
                        .get_event(&existing.id, &message_id, Duration::from_secs(2))
                        .await?;
                    if ack.is_null() {
                        continue;
                    }

                    let existing_input = parse_debouncer_input(&existing)?;
                    return Ok(self.workflow_handle(existing_input.target_workflow_id));
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn start_workflow(
        self: &Arc<Self>,
        name: &str,
        input: Interchange,
        launch: WorkflowLaunch,
        queue_name: Option<String>,
        enqueue: Option<EnqueueOptions>,
        parent_workflow_id: Option<String>,
        child_checkpoint: Option<ChildCheckpoint>,
    ) -> JournioResult<WorkflowHandle> {
        // Only an immediately-executed workflow needs to be registered locally —
        // an enqueued/delayed workflow is picked up by some executor process
        // (which owns the registration). Mirrors Go's `Client.Enqueue`, which
        // inserts the row directly without consulting the registry.
        if matches!(
            launch,
            WorkflowLaunch::Immediate | WorkflowLaunch::Background
        ) && self.registry.get(name).is_none()
        {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("workflow {name} is not registered"),
            ));
        }
        validate_enqueue_options(&queue_name, &enqueue)?;

        if let Some(queue_name) = queue_name.as_deref() {
            if let Some(queue) = self.system_db.get_queue(queue_name).await? {
                validate_queue_assignment(name, queue_name, &queue, enqueue.as_ref())?;
            }
        }

        let workflow_id = enqueue
            .as_ref()
            .and_then(|opts| opts.workflow_id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let mut init = InitWorkflow::new_pending(
            workflow_id.clone(),
            name.to_string(),
            self.config.executor_id.clone().unwrap_or_default(),
        );
        init.status = match launch {
            WorkflowLaunch::Immediate | WorkflowLaunch::Background => WorkflowStatusType::Pending,
            WorkflowLaunch::Enqueued => WorkflowStatusType::Enqueued,
            WorkflowLaunch::Delayed => WorkflowStatusType::Delayed,
        };
        init.input = Some(input.clone());
        init.parent_workflow_id = parent_workflow_id;
        init.application_version = enqueue
            .as_ref()
            .and_then(|opts| opts.application_version.clone())
            .or_else(|| self.config.application_version.clone());
        init.application_id = Some(self.config.app_name.clone());
        init.serialization = Some(self.config.serializer.name().to_string());
        init.queue_name = queue_name;
        if let Some(opts) = enqueue {
            init.deduplication_id = opts.deduplication_id;
            init.priority = opts.priority;
            init.queue_partition_key = opts.queue_partition_key;
            init.timeout = opts.timeout;
            init.delay_until = opts.delay_until;
        }

        self.system_db.init_workflow(init).await?;

        if let Some(checkpoint) = child_checkpoint {
            self.system_db
                .record_step_output(&StepRecord {
                    workflow_uuid: checkpoint.parent_workflow_id,
                    function_id: checkpoint.function_id,
                    function_name: checkpoint.function_name,
                    output: None,
                    error: None,
                    child_workflow_id: Some(workflow_id.clone()),
                })
                .await?;
        }

        if matches!(launch, WorkflowLaunch::Immediate) {
            self.execute_workflow(&workflow_id, name, input).await?;
        } else if matches!(launch, WorkflowLaunch::Background) {
            let ctx = self.clone();
            let workflow_id = workflow_id.clone();
            let name = name.to_string();
            tokio::spawn(async move {
                if let Err(err) = ctx.execute_workflow(&workflow_id, &name, input).await {
                    tracing::error!(workflow_id = %workflow_id, error = %err, "background workflow failed");
                }
            });
        }

        Ok(WorkflowHandle {
            workflow_id,
            journio: Arc::downgrade(self),
        })
    }

    async fn execute_workflow(
        self: &Arc<Self>,
        workflow_id: &str,
        name: &str,
        input: Interchange,
    ) -> JournioResult<Interchange> {
        let workflow = self.registry.get(name).ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                format!("workflow {name} is not registered"),
            )
        })?;

        let ctx = WorkflowContext {
            inner: Arc::new(WorkflowCtxInner {
                workflow_id: workflow_id.to_string(),
                step_counter: AtomicI32::new(0),
                step_execution_depth: AtomicI32::new(0),
                journio: Arc::downgrade(self),
                cancel: self.cancel.child_token(),
                authenticated_user: RwLock::new(None),
                assumed_role: RwLock::new(None),
            }),
        };

        match workflow.run(&ctx, input).await {
            Ok(output) => {
                self.system_db
                    .record_workflow_result(
                        workflow_id,
                        WorkflowStatusType::Success,
                        Some(&output),
                        None,
                    )
                    .await?;
                Ok(output)
            }
            Err(err) => {
                let terminal_status = if err.code == JournioErrorCode::WorkflowCancelled {
                    WorkflowStatusType::Cancelled
                } else {
                    WorkflowStatusType::Error
                };
                self.system_db
                    .record_workflow_result(workflow_id, terminal_status, None, Some(&err.message))
                    .await?;
                Err(err)
            }
        }
    }

    async fn find_existing_debouncer_workflow(
        &self,
        deduplication_id: &str,
    ) -> JournioResult<Option<WorkflowStatus>> {
        let workflows = self.system_db.list_workflows(10_000).await?;
        Ok(workflows.into_iter().find(|workflow| {
            workflow.name == INTERNAL_DEBOUNCER_WORKFLOW_NAME
                && workflow.queue_name.as_deref() == Some(INTERNAL_QUEUE_NAME)
                && workflow.deduplication_id.as_deref() == Some(deduplication_id)
                && matches!(
                    workflow.status,
                    WorkflowStatusType::Pending
                        | WorkflowStatusType::Enqueued
                        | WorkflowStatusType::Delayed
                )
        }))
    }

    fn spawn_background_loops(self: &Arc<Self>) {
        let queue_ctx = self.clone();
        tokio::spawn(async move {
            queue_ctx.run_queue_worker_loop().await;
        });

        let scheduler_ctx = self.clone();
        tokio::spawn(async move {
            scheduler_ctx.run_scheduler_loop().await;
        });
    }

    async fn run_queue_worker_loop(self: Arc<Self>) {
        loop {
            if self.cancel.is_cancelled() {
                return;
            }

            match self.run_all_queues_once().await {
                Ok(processed) if processed > 0 => continue,
                Ok(_) => sleep(POLL_INTERVAL).await,
                Err(err) => {
                    tracing::warn!(error = %err, "queue worker loop iteration failed");
                    sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    async fn run_scheduler_loop(self: Arc<Self>) {
        loop {
            if self.cancel.is_cancelled() {
                return;
            }

            if let Err(err) = self.run_scheduler_once().await {
                tracing::warn!(error = %err, "scheduler loop iteration failed");
            }

            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = sleep(self.config.scheduler_polling_interval) => {}
            }
        }
    }

    async fn run_scheduler_once(self: &Arc<Self>) -> JournioResult<()> {
        let schedules = self.system_db.list_schedules().await?;
        let now = Utc::now();

        for schedule in schedules {
            if schedule.status != ScheduleStatus::Active {
                continue;
            }

            let spec = parse_schedule(&schedule.schedule)?;
            let Some(last_fired_at) = schedule.last_fired_at else {
                self.system_db
                    .update_schedule_last_fired_at(&schedule.schedule_name, now)
                    .await?;
                continue;
            };

            let due_times =
                due_schedule_times(&spec, last_fired_at, now, schedule.automatic_backfill);
            if due_times.is_empty() {
                continue;
            }

            let queue_name = schedule
                .queue_name
                .clone()
                .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string());

            for scheduled_time in &due_times {
                let workflow_id = format!(
                    "sched-{}-{}",
                    schedule.schedule_name,
                    scheduled_time.to_rfc3339()
                );
                if self
                    .system_db
                    .get_workflow_status(&workflow_id)
                    .await?
                    .is_some()
                {
                    continue;
                }

                let input = serde_json::to_value(ScheduledWorkflowInput {
                    scheduled_time: *scheduled_time,
                    context: schedule.context.clone(),
                })
                .map_err(|err| {
                    JournioError::new(
                        JournioErrorCode::InitializationError,
                        format!(
                            "failed to serialize scheduled workflow input for {}: {err}",
                            schedule.schedule_name
                        ),
                    )
                })?;

                self.enqueue_workflow(
                    &queue_name,
                    &schedule.workflow_name,
                    input,
                    EnqueueOptions {
                        workflow_id: Some(workflow_id),
                        ..Default::default()
                    },
                )
                .await?;
            }

            if let Some(last) = due_times.last().copied() {
                self.system_db
                    .update_schedule_last_fired_at(&schedule.schedule_name, last)
                    .await?;
            }
        }

        Ok(())
    }
}

struct ChildCheckpoint {
    parent_workflow_id: String,
    function_id: i32,
    function_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkflowLaunch {
    Immediate,
    Background,
    Enqueued,
    Delayed,
}

fn validate_schedule_spec(spec: &str) -> JournioResult<()> {
    parse_schedule(spec).map(|_| ())
}

fn parse_schedule(spec: &str) -> JournioResult<Schedule> {
    Schedule::from_str(spec).map_err(|err| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            format!("invalid cron schedule {spec:?}: {err}"),
        )
    })
}

pub(crate) fn parse_cron_schedule(spec: &str) -> JournioResult<Schedule> {
    parse_schedule(spec)
}

fn due_schedule_times(
    schedule: &Schedule,
    last_fired_at: DateTime<Utc>,
    now: DateTime<Utc>,
    automatic_backfill: bool,
) -> Vec<DateTime<Utc>> {
    let mut upcoming = schedule.after(&last_fired_at);
    let mut due = Vec::new();

    while let Some(next) = upcoming.next() {
        if next > now {
            break;
        }
        due.push(next);
        if !automatic_backfill {
            due.truncate(1);
            while let Some(candidate) = upcoming.next() {
                if candidate > now {
                    break;
                }
                due[0] = candidate;
            }
            break;
        }
    }

    due
}

fn resolve_backend(config: &Config) -> JournioResult<Arc<dyn SystemDatabase>> {
    if let Some(db) = config.system_db.clone() {
        return Ok(db);
    }
    Err(JournioError::new(
        JournioErrorCode::InitializationError,
        "no system_db provided: the journio-postgres / journio-sqlite crate must construct the backend \
         and inject it via Config.system_db (core is driver-free)",
    ))
}

fn encode_interchange(journio: &JournioContext, value: &Interchange) -> JournioResult<String> {
    journio.config.serializer.serialize(value)
}

fn decode_interchange(
    journio: &JournioContext,
    workflow_id: &str,
    step_id: i32,
    step_name: &str,
    encoded: Option<String>,
) -> JournioResult<Interchange> {
    match encoded {
        Some(encoded) => journio
            .config
            .serializer
            .deserialize(&encoded)
            .map_err(|mut err| {
                err.workflow_id = Some(workflow_id.to_string());
                err.step_id = Some(step_id);
                err.step_name = Some(step_name.to_string());
                err
            }),
        None => Ok(Interchange::Null),
    }
}

fn decode_stream_entry(
    journio: &JournioContext,
    workflow_id: &str,
    key: &str,
    entry: &crate::types::StreamEntry,
) -> JournioResult<Interchange> {
    journio.config
        .serializer
        .deserialize(&entry.value)
        .map_err(|err| {
            JournioError::new(
                JournioErrorCode::WorkflowUnexpectedTypeError,
                format!(
                    "failed to decode stream entry for workflow {workflow_id}, key {key} with serialization {:?}: {err}",
                    entry.serialization.as_deref().unwrap_or(journio.config.serializer.name())
                ),
            )
        })
}

fn stream_workflow_is_active(status: WorkflowStatusType) -> bool {
    matches!(
        status,
        WorkflowStatusType::Pending | WorkflowStatusType::Enqueued
    )
}

fn duration_to_millis_u64(value: Duration) -> JournioResult<u64> {
    u64::try_from(value.as_millis()).map_err(|_| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            "duration is too large to convert to milliseconds",
        )
    })
}

fn millis_to_duration(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn debounce_deduplication_key(workflow_name: &str, key: &str) -> String {
    format!("debounce::{workflow_name}::{key}")
}

fn validate_queue_options(queue_name: &str, options: &QueueOptions) -> JournioResult<()> {
    if queue_name.is_empty() {
        return Err(JournioError::new(
            JournioErrorCode::InitializationError,
            "queue_name cannot be empty",
        ));
    }
    if let (Some(worker), Some(global)) = (options.worker_concurrency, options.concurrency) {
        if worker > global {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!(
                    "queue {queue_name}: concurrency must be greater than or equal to worker_concurrency"
                ),
            ));
        }
    }
    if let Some(limit) = options.rate_limit_max {
        if limit <= 0 {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("queue {queue_name}: rate limiter limit must be positive"),
            ));
        }
    }
    if let Some(period) = options.rate_limit_period {
        if period.is_zero() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("queue {queue_name}: rate limiter period must be positive"),
            ));
        }
    }
    if let Some(interval) = options.polling_interval {
        if interval.is_zero() {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("queue {queue_name}: polling interval must be positive"),
            ));
        }
    }
    Ok(())
}

fn validate_enqueue_options(
    queue_name: &Option<String>,
    enqueue: &Option<EnqueueOptions>,
) -> JournioResult<()> {
    if let Some(enqueue) = enqueue.as_ref() {
        if enqueue.queue_partition_key.is_some() && queue_name.is_none() {
            return Err(JournioError::new(
                JournioErrorCode::WorkflowExecutionError,
                "queue partition key requires a queue name",
            ));
        }
        if enqueue.queue_partition_key.is_some() && enqueue.deduplication_id.is_some() {
            return Err(JournioError::new(
                JournioErrorCode::WorkflowExecutionError,
                "queue partition key cannot be combined with deduplication_id",
            ));
        }
    }
    Ok(())
}

fn validate_queue_assignment(
    workflow_name: &str,
    queue_name: &str,
    queue: &QueueConfig,
    enqueue: Option<&EnqueueOptions>,
) -> JournioResult<()> {
    let partition_key = enqueue.and_then(|value| value.queue_partition_key.as_deref());
    if queue.partition_queue && partition_key.is_none() {
        return Err(JournioError::new(
            JournioErrorCode::WorkflowExecutionError,
            format!(
                "queue {queue_name} has partitions enabled, but no partition key was provided for workflow {workflow_name}"
            ),
        ));
    }
    if !queue.partition_queue && partition_key.is_some() {
        return Err(JournioError::new(
            JournioErrorCode::WorkflowExecutionError,
            format!(
                "queue {queue_name} is not a partitioned queue, but a partition key was provided for workflow {workflow_name}"
            ),
        ));
    }
    Ok(())
}

fn parse_debouncer_input(workflow: &WorkflowStatus) -> JournioResult<DebouncerWorkflowInput> {
    let input = workflow.input.clone().ok_or_else(|| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            format!("debouncer workflow {} is missing input", workflow.id),
        )
    })?;
    serde_json::from_value(input).map_err(|err| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            format!("failed to decode debouncer workflow input: {err}"),
        )
    })
}

async fn durable_uuid_step(ctx: &WorkflowContext, name: &'static str) -> JournioResult<String> {
    let step = Arc::new(crate::workflow::StepFunc::new(name, |_ctx| {
        Box::pin(async move { Ok(Uuid::new_v4().to_string()) })
    }));
    let value = ctx.run_as_step(step).await?;
    serde_json::from_value(value).map_err(|err| {
        JournioError::new(
            JournioErrorCode::WorkflowUnexpectedTypeError,
            format!("failed to decode durable UUID step output: {err}"),
        )
    })
}

async fn durable_now_step(
    ctx: &WorkflowContext,
    name: &'static str,
) -> JournioResult<DateTime<Utc>> {
    let step = Arc::new(crate::workflow::StepFunc::new(name, |_ctx| {
        Box::pin(async move { Ok(Utc::now().to_rfc3339()) })
    }));
    let value = ctx.run_as_step(step).await?;
    let encoded: String = serde_json::from_value(value).map_err(|err| {
        JournioError::new(
            JournioErrorCode::WorkflowUnexpectedTypeError,
            format!("failed to decode durable timestamp step output: {err}"),
        )
    })?;
    DateTime::parse_from_rfc3339(&encoded)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| {
            JournioError::new(
                JournioErrorCode::WorkflowUnexpectedTypeError,
                format!("failed to parse durable timestamp {encoded}: {err}"),
            )
        })
}

struct InternalDebouncerWorkflow;

#[async_trait::async_trait]
impl Workflow for InternalDebouncerWorkflow {
    fn name(&self) -> &str {
        INTERNAL_DEBOUNCER_WORKFLOW_NAME
    }

    async fn run(&self, ctx: &WorkflowContext, input: Interchange) -> JournioResult<Interchange> {
        let input: DebouncerWorkflowInput = serde_json::from_value(input).map_err(|err| {
            JournioError::new(
                JournioErrorCode::WorkflowUnexpectedTypeError,
                format!("failed to decode internal debouncer input: {err}"),
            )
        })?;

        let start_time = durable_now_step(ctx, "journio.debounce.startTime").await?;
        let max_start_time = input.timeout_ms.map(|timeout_ms| {
            start_time
                + chrono::Duration::from_std(millis_to_duration(timeout_ms))
                    .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX))
        });

        let mut current_input = input.initial_input.clone();
        let mut target_start_time = start_time
            + chrono::Duration::from_std(millis_to_duration(input.delay_ms))
                .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
        if let Some(max_start_time) = max_start_time {
            if target_start_time > max_start_time {
                target_start_time = max_start_time;
            }
        }

        loop {
            let now = durable_now_step(ctx, "journio.debounce.loopTime").await?;
            let remaining = target_start_time
                .signed_duration_since(now)
                .to_std()
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                break;
            }

            match ctx.recv(DEBOUNCER_TOPIC, remaining).await {
                Ok(message) => {
                    let message: DebounceMessage =
                        serde_json::from_value(message).map_err(|err| {
                            JournioError::new(
                                JournioErrorCode::WorkflowUnexpectedTypeError,
                                format!("failed to decode debounce message: {err}"),
                            )
                        })?;
                    current_input = message.input;
                    let mut next_target_start = now
                        + chrono::Duration::from_std(millis_to_duration(message.delay_ms))
                            .unwrap_or_else(|_| chrono::Duration::milliseconds(i64::MAX));
                    if let Some(max_start_time) = max_start_time {
                        if next_target_start > max_start_time {
                            next_target_start = max_start_time;
                        }
                    }
                    target_start_time = next_target_start;
                    if !message.id.is_empty() {
                        ctx.set_event(&message.id, serde_json::json!(true)).await?;
                    }
                }
                Err(err) if err.code == JournioErrorCode::TimeoutError => break,
                Err(err) => return Err(err),
            }
        }

        let journio = ctx.journio()?;
        let mut launch = EnqueueOptions {
            workflow_id: Some(input.target_workflow_id.clone()),
            application_version: input.application_version.clone(),
            deduplication_id: input.deduplication_id.clone(),
            priority: input.priority,
            queue_partition_key: input.queue_partition_key.clone(),
            timeout: input.workflow_timeout_ms.map(millis_to_duration),
            ..Default::default()
        };

        if let Some(queue_name) = input.queue_name.as_deref() {
            journio
                .enqueue_workflow(
                    queue_name,
                    &input.target_workflow_name,
                    current_input,
                    launch,
                )
                .await?;
        } else {
            launch.deduplication_id = None;
            journio
                .start_workflow(
                    &input.target_workflow_name,
                    current_input,
                    WorkflowLaunch::Immediate,
                    None,
                    Some(launch),
                    None,
                    None,
                )
                .await?;
        }

        Ok(Interchange::Null)
    }
}

fn validate_recorded_step(
    workflow_id: &str,
    step_id: i32,
    expected_name: &str,
    recorded_name: &str,
) -> JournioResult<()> {
    if expected_name != recorded_name {
        return Err(crate::error::constructors::unexpected_step(
            workflow_id,
            step_id,
            expected_name,
            recorded_name,
        ));
    }
    Ok(())
}

fn validate_patching_enabled(journio: &JournioContext) -> JournioResult<()> {
    if journio.config.enable_patching {
        return Ok(());
    }
    Err(JournioError::new(
        JournioErrorCode::PatchingNotEnabled,
        "Patching system is not enabled. Set enable_patching to true in the Journio context configuration to use patch and deprecate_patch",
    ))
}

fn validate_patch_name(patch_name: &str) -> JournioResult<()> {
    if patch_name.is_empty() {
        return Err(JournioError::new(
            JournioErrorCode::StepExecutionError,
            "patch name cannot be empty",
        ));
    }
    Ok(())
}

fn validate_not_within_step(ctx: &WorkflowContext) -> JournioResult<()> {
    if !ctx.is_within_step() {
        return Ok(());
    }

    Err(ctx_step_error(
        JournioErrorCode::StepExecutionError,
        ctx.workflow_id(),
        ctx.peek_next_step_id(),
        "patching",
        "cannot call patching APIs within a step".to_string(),
    ))
}

fn recorded_step_error(
    workflow_id: &str,
    step_id: i32,
    step_name: &str,
    message: String,
) -> JournioError {
    ctx_step_error(
        JournioErrorCode::StepExecutionError,
        workflow_id,
        step_id,
        step_name,
        message,
    )
}

/// Build a `JournioError` anchored at a workflow step — mirrors Go's
/// `newStepExecutionError` / `newTimeoutError` constructors (the only
/// difference is the `code`).
fn ctx_step_error(
    code: JournioErrorCode,
    workflow_id: &str,
    step_id: i32,
    step_name: &str,
    message: String,
) -> JournioError {
    let mut err = JournioError::new(code, message);
    err.workflow_id = Some(workflow_id.to_string());
    err.step_id = Some(step_id);
    err.step_name = Some(step_name.to_string());
    err
}

/// Parse an RFC3339 deadline recorded by `sleep`.
fn parse_deadline(value: &Interchange) -> JournioResult<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| {
            JournioError::new(
                JournioErrorCode::StepExecutionError,
                "recorded sleep deadline was not a valid RFC3339 timestamp",
            )
        })
}

/// Duration from now until `deadline`, clamped at zero.
fn remaining_until(deadline: DateTime<Utc>) -> Duration {
    let now = Utc::now();
    if deadline <= now {
        return Duration::ZERO;
    }
    (deadline - now).to_std().unwrap_or(Duration::ZERO)
}

fn workflow_terminal_error(
    code: JournioErrorCode,
    workflow_id: &str,
    message: String,
) -> JournioError {
    let mut err = JournioError::new(code, message);
    err.workflow_id = Some(workflow_id.to_string());
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use async_trait::async_trait;

    use crate::dialect::{Dialect, DialectName};
    use crate::system_db::Notification;
    use crate::types::{ListWorkflowsFilter, VersionInfo};
    use crate::workflow::{StepFunc, WorkflowFn};

    /// A stored notification row in the fake DB.
    #[derive(Clone)]
    struct FakeNotification {
        destination_id: String,
        topic: String,
        message: Interchange,
        consumed: bool,
    }

    #[derive(Clone)]
    struct FakeStreamEntry {
        workflow_id: String,
        key: String,
        value: String,
        offset: i64,
        serialization: Option<String>,
    }

    #[derive(Default)]
    struct FakeState {
        workflows: HashMap<String, WorkflowStatus>,
        steps: HashMap<String, Vec<StepRecord>>,
        notifications: Vec<FakeNotification>,
        /// (workflow_uuid, key) → value.
        events: HashMap<(String, String), Interchange>,
        streams: Vec<FakeStreamEntry>,
        queues: HashMap<String, QueueConfig>,
        schedules: HashMap<String, WorkflowSchedule>,
        application_versions: Vec<VersionInfo>,
    }

    #[derive(Default)]
    struct FakeSystemDatabase {
        state: Mutex<FakeState>,
    }

    /// Matches `_JOURNIO_NULL_TOPIC` / `NULL_TOPIC` in the real backends.
    const FAKE_NULL_TOPIC: &str = "__null__topic__";

    fn normalize_topic(topic: &str) -> &str {
        if topic.is_empty() {
            FAKE_NULL_TOPIC
        } else {
            topic
        }
    }

    #[derive(Clone, Copy)]
    struct TestDialect;

    impl Dialect for TestDialect {
        fn name(&self) -> DialectName {
            DialectName::Postgres
        }

        fn schema_prefix(&self, _schema: &str) -> String {
            String::new()
        }

        fn rewrite_query(&self, query: &str) -> String {
            query.to_string()
        }

        fn lock_skip_locked(&self) -> &str {
            "FOR UPDATE SKIP LOCKED"
        }

        fn lock_nowait(&self) -> &str {
            "FOR UPDATE NOWAIT"
        }

        fn supports_listen_notify(&self) -> bool {
            false
        }

        fn supports_array_parameters(&self) -> bool {
            false
        }

        fn supports_data_modifying_cte(&self) -> bool {
            false
        }
    }

    #[async_trait]
    impl SystemDatabase for FakeSystemDatabase {
        fn dialect(&self) -> &dyn Dialect {
            static DIALECT: TestDialect = TestDialect;
            &DIALECT
        }

        async fn migrate(&self) -> JournioResult<()> {
            Ok(())
        }

        async fn launch(&self) -> JournioResult<()> {
            Ok(())
        }

        async fn shutdown(&self) -> JournioResult<()> {
            Ok(())
        }

        async fn init_workflow(
            &self,
            init: InitWorkflow,
        ) -> JournioResult<crate::system_db::InitWorkflowResult> {
            let mut state = self.state.lock().expect("fake db lock");
            let status = workflow_status_from_init(&init);
            state.workflows.insert(init.workflow_id.clone(), status);
            Ok(crate::system_db::InitWorkflowResult {
                status: init.status,
                attempts: 1,
                name: init.name,
                queue_name: init.queue_name,
                queue_partition_key: init.queue_partition_key,
                timeout: init.timeout,
                deadline: init.deadline,
            })
        }

        async fn record_workflow_result(
            &self,
            workflow_id: &str,
            status: WorkflowStatusType,
            output: Option<&Interchange>,
            error: Option<&str>,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            let existing = state
                .workflows
                .get_mut(workflow_id)
                .expect("workflow exists");
            existing.status = status;
            existing.output = output.cloned();
            existing.error = error.map(ToString::to_string);
            existing.updated_at = Utc::now();
            existing.completed_at = Some(Utc::now());
            Ok(())
        }

        async fn get_workflow_status(
            &self,
            workflow_id: &str,
        ) -> JournioResult<Option<WorkflowStatus>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state.workflows.get(workflow_id).cloned())
        }

        async fn list_workflows(&self, limit: i64) -> JournioResult<Vec<WorkflowStatus>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state
                .workflows
                .values()
                .take(limit as usize)
                .cloned()
                .collect())
        }

        async fn list_workflows_filtered(
            &self,
            filter: &ListWorkflowsFilter,
        ) -> JournioResult<Vec<WorkflowStatus>> {
            let state = self.state.lock().expect("fake db lock");
            let mut rows: Vec<WorkflowStatus> = state
                .workflows
                .values()
                .filter(|wf| matches_filter(wf, filter))
                .cloned()
                .collect();
            rows.sort_by(|left, right| {
                if filter.sort_desc {
                    right.created_at.cmp(&left.created_at)
                } else {
                    left.created_at.cmp(&right.created_at)
                }
            });
            if let Some(offset) = filter.offset {
                let offset = offset as usize;
                if offset >= rows.len() {
                    rows.clear();
                } else {
                    rows.drain(..offset);
                }
            }
            if let Some(limit) = filter.limit {
                rows.truncate(limit as usize);
            }
            Ok(rows)
        }

        async fn set_workflow_delay(
            &self,
            workflow_id: &str,
            delay_until: DateTime<Utc>,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if let Some(workflow) = state.workflows.get_mut(workflow_id) {
                if workflow.status == WorkflowStatusType::Delayed {
                    workflow.delay_until = Some(delay_until);
                }
            }
            Ok(())
        }

        async fn delete_workflows(
            &self,
            workflow_ids: &[String],
            delete_children: bool,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            let ids: std::collections::HashSet<&String> = workflow_ids.iter().collect();
            let to_delete = if delete_children {
                let mut pending: Vec<String> = ids.iter().copied().cloned().collect();
                let mut all: std::collections::HashSet<String> = pending.iter().cloned().collect();
                while let Some(parent) = pending.pop() {
                    for wf in state.workflows.values() {
                        if wf.parent_workflow_id.as_deref() == Some(parent.as_str())
                            && all.insert(wf.id.clone())
                        {
                            pending.push(wf.id.clone());
                        }
                    }
                }
                all
            } else {
                ids.into_iter().cloned().collect()
            };
            for id in &to_delete {
                state.workflows.remove(id);
                state.steps.remove(id);
                state.events.retain(|(workflow_id, _), _| workflow_id != id);
            }
            Ok(())
        }

        async fn cancel_workflows(&self, workflow_ids: &[String]) -> JournioResult<Vec<String>> {
            let mut state = self.state.lock().expect("fake db lock");
            let now = Utc::now();
            let mut found = Vec::new();
            for workflow_id in workflow_ids {
                let Some(workflow) = state.workflows.get_mut(workflow_id) else {
                    continue;
                };
                found.push(workflow_id.clone());
                if matches!(
                    workflow.status,
                    WorkflowStatusType::Success
                        | WorkflowStatusType::Error
                        | WorkflowStatusType::Cancelled
                ) {
                    continue;
                }
                workflow.status = WorkflowStatusType::Cancelled;
                workflow.updated_at = now;
                workflow.completed_at = Some(now);
                workflow.started_at = None;
                workflow.queue_name = None;
                workflow.deduplication_id = None;
            }
            Ok(found)
        }

        async fn resume_workflows(
            &self,
            workflow_ids: &[String],
            queue_name: Option<&str>,
        ) -> JournioResult<Vec<String>> {
            let mut state = self.state.lock().expect("fake db lock");
            let now = Utc::now();
            let queue_name = queue_name.unwrap_or(INTERNAL_QUEUE_NAME).to_string();
            let mut found = Vec::new();
            for workflow_id in workflow_ids {
                let Some(workflow) = state.workflows.get_mut(workflow_id) else {
                    continue;
                };
                found.push(workflow_id.clone());
                if matches!(
                    workflow.status,
                    WorkflowStatusType::Success | WorkflowStatusType::Error
                ) {
                    continue;
                }
                workflow.status = WorkflowStatusType::Enqueued;
                workflow.queue_name = Some(queue_name.clone());
                workflow.attempts = 0;
                workflow.deadline = None;
                workflow.deduplication_id = None;
                workflow.started_at = None;
                workflow.updated_at = now;
                workflow.completed_at = None;
            }
            Ok(found)
        }

        async fn get_workflow_children(
            &self,
            workflow_id: &str,
        ) -> JournioResult<Vec<WorkflowStatus>> {
            let state = self.state.lock().expect("fake db lock");
            let mut queue = vec![workflow_id.to_string()];
            let mut children = Vec::new();

            while let Some(parent_id) = queue.pop() {
                for workflow in state.workflows.values() {
                    if workflow.parent_workflow_id.as_deref() == Some(parent_id.as_str()) {
                        children.push(workflow.clone());
                        queue.push(workflow.id.clone());
                    }
                }
            }

            Ok(children)
        }

        async fn fork_workflow(&self, input: ForkWorkflow) -> JournioResult<String> {
            let mut state = self.state.lock().expect("fake db lock");
            let original = state
                .workflows
                .get(&input.original_workflow_id)
                .cloned()
                .ok_or_else(|| {
                    crate::error::constructors::non_existent_workflow(&input.original_workflow_id)
                })?;
            let forked_workflow_id = input
                .forked_workflow_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let now = Utc::now();

            let mut forked = original.clone();
            forked.id = forked_workflow_id.clone();
            forked.status = WorkflowStatusType::Enqueued;
            forked.output = None;
            forked.error = None;
            forked.created_at = now;
            forked.updated_at = now;
            forked.completed_at = None;
            forked.started_at = None;
            forked.attempts = 0;
            forked.queue_name = Some(
                input
                    .queue_name
                    .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string()),
            );
            forked.queue_partition_key = input.queue_partition_key;
            forked.application_version = input
                .application_version
                .unwrap_or_else(|| original.application_version.clone());
            forked.forked_from = Some(input.original_workflow_id.clone());
            forked.was_forked_from = false;

            state.workflows.insert(forked_workflow_id.clone(), forked);
            if let Some(existing) = state.workflows.get_mut(&input.original_workflow_id) {
                existing.was_forked_from = true;
            }

            if input.start_step > 0 {
                if let Some(steps) = state.steps.get(&input.original_workflow_id).cloned() {
                    let copied: Vec<StepRecord> = steps
                        .into_iter()
                        .filter(|step| step.function_id < input.start_step)
                        .map(|mut step| {
                            step.workflow_uuid = forked_workflow_id.clone();
                            step
                        })
                        .collect();
                    if !copied.is_empty() {
                        state.steps.insert(forked_workflow_id.clone(), copied);
                    }
                }

                let latest_events: Vec<_> = state
                    .events
                    .iter()
                    .filter(|((workflow_id, _), _)| workflow_id == &input.original_workflow_id)
                    .map(|((_, key), value)| (key.clone(), value.clone()))
                    .collect();
                for (key, value) in latest_events {
                    state
                        .events
                        .insert((forked_workflow_id.clone(), key), value);
                }

                let copied_streams: Vec<_> = state
                    .streams
                    .iter()
                    .filter(|entry| {
                        entry.workflow_id == input.original_workflow_id
                            && entry.offset < i64::from(input.start_step)
                    })
                    .cloned()
                    .collect();
                for mut entry in copied_streams {
                    entry.workflow_id = forked_workflow_id.clone();
                    state.streams.push(entry);
                }
            }

            Ok(forked_workflow_id)
        }

        async fn get_queue(&self, queue_name: &str) -> JournioResult<Option<QueueConfig>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state.queues.get(queue_name).cloned())
        }

        async fn upsert_queue(&self, queue: &QueueConfig) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            state.queues.insert(queue.name.clone(), queue.clone());
            Ok(())
        }

        async fn record_step_output(&self, step: &StepRecord) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            let steps = state.steps.entry(step.workflow_uuid.clone()).or_default();
            if let Some(existing) = steps
                .iter_mut()
                .find(|existing| existing.function_id == step.function_id)
            {
                *existing = step.clone();
            } else {
                steps.push(step.clone());
                steps.sort_by_key(|entry| entry.function_id);
            }
            Ok(())
        }

        async fn get_steps(&self, workflow_id: &str) -> JournioResult<Vec<StepRecord>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state.steps.get(workflow_id).cloned().unwrap_or_default())
        }

        async fn dequeue_workflow(
            &self,
            queue_name: &str,
            executor_id: &str,
        ) -> JournioResult<Option<WorkflowStatus>> {
            let mut state = self.state.lock().expect("fake db lock");
            let now = Utc::now();
            let candidate_id = state
                .workflows
                .values()
                .filter(|workflow| {
                    workflow.queue_name.as_deref() == Some(queue_name)
                        && matches!(
                            workflow.status,
                            WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed
                        )
                        && match workflow.status {
                            WorkflowStatusType::Delayed => workflow
                                .delay_until
                                .is_none_or(|delay_until| delay_until <= now),
                            _ => true,
                        }
                })
                .min_by(|left, right| {
                    left.priority
                        .cmp(&right.priority)
                        .then_with(|| left.created_at.cmp(&right.created_at))
                })
                .map(|workflow| workflow.id.clone());

            let Some(candidate_id) = candidate_id else {
                return Ok(None);
            };

            let workflow = state
                .workflows
                .get_mut(&candidate_id)
                .expect("candidate workflow exists");
            workflow.status = WorkflowStatusType::Pending;
            workflow.executor_id = executor_id.to_string();
            workflow.started_at = Some(now);
            workflow.updated_at = now;
            if workflow.deadline.is_none() {
                if let Some(timeout) = workflow.timeout {
                    workflow.deadline = chrono::Duration::from_std(timeout)
                        .ok()
                        .map(|duration| now + duration);
                }
            }

            Ok(Some(workflow.clone()))
        }

        async fn list_runnable_queues(&self) -> JournioResult<Vec<String>> {
            let state = self.state.lock().expect("fake db lock");
            let mut names: Vec<String> = state
                .workflows
                .values()
                .filter_map(|workflow| {
                    if matches!(
                        workflow.status,
                        WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed
                    ) {
                        workflow.queue_name.clone()
                    } else {
                        None
                    }
                })
                .collect();
            names.sort();
            names.dedup();
            Ok(names)
        }

        async fn list_queues(&self) -> JournioResult<Vec<QueueConfig>> {
            let state = self.state.lock().expect("fake db lock");
            let mut queues: Vec<QueueConfig> = state.queues.values().cloned().collect();
            queues.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(queues)
        }

        async fn send(
            &self,
            destination_id: &str,
            topic: &str,
            message: &Interchange,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            // Mirror the FK: a Send to an unknown destination is an error.
            if !state.workflows.contains_key(destination_id) {
                return Err(JournioError::new(
                    JournioErrorCode::NonExistentWorkflowError,
                    format!("destination workflow {destination_id} does not exist"),
                ));
            }
            state.notifications.push(FakeNotification {
                destination_id: destination_id.to_string(),
                topic: normalize_topic(topic).to_string(),
                message: message.clone(),
                consumed: false,
            });
            Ok(())
        }

        async fn consume_notification(
            &self,
            workflow_id: &str,
            topic: &str,
        ) -> JournioResult<Option<Notification>> {
            let mut state = self.state.lock().expect("fake db lock");
            let topic = normalize_topic(topic);
            let candidate = state
                .notifications
                .iter_mut()
                .find(|n| !n.consumed && n.destination_id == workflow_id && n.topic == topic);
            if let Some(n) = candidate {
                n.consumed = true;
                Ok(Some(Notification {
                    message: n.message.clone(),
                    serialization: Some("JOURNIO_JSON".to_string()),
                }))
            } else {
                Ok(None)
            }
        }

        async fn set_event(
            &self,
            workflow_id: &str,
            key: &str,
            value: &Interchange,
            _function_id: i32,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            state
                .events
                .insert((workflow_id.to_string(), key.to_string()), value.clone());
            Ok(())
        }

        async fn get_event_value(
            &self,
            workflow_id: &str,
            key: &str,
        ) -> JournioResult<Option<Interchange>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state
                .events
                .get(&(workflow_id.to_string(), key.to_string()))
                .cloned())
        }

        async fn write_stream(
            &self,
            workflow_id: &str,
            key: &str,
            value: &str,
            _function_id: i32,
            serialization: Option<&str>,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if state.streams.iter().any(|entry| {
                entry.workflow_id == workflow_id
                    && entry.key == key
                    && entry.value == STREAM_CLOSED_SENTINEL
            }) {
                return Err(JournioError::new(
                    JournioErrorCode::WorkflowExecutionError,
                    format!("stream {key:?} is already closed"),
                ));
            }

            let offset = state
                .streams
                .iter()
                .filter(|entry| entry.workflow_id == workflow_id && entry.key == key)
                .map(|entry| entry.offset)
                .max()
                .unwrap_or(-1)
                + 1;
            state.streams.push(FakeStreamEntry {
                workflow_id: workflow_id.to_string(),
                key: key.to_string(),
                value: value.to_string(),
                offset,
                serialization: serialization.map(ToString::to_string),
            });
            Ok(())
        }

        async fn read_stream(
            &self,
            workflow_id: &str,
            key: &str,
            from_offset: i64,
        ) -> JournioResult<(Vec<crate::types::StreamEntry>, bool)> {
            let state = self.state.lock().expect("fake db lock");
            let mut entries = Vec::new();
            let mut closed = false;

            for entry in state.streams.iter().filter(|entry| {
                entry.workflow_id == workflow_id && entry.key == key && entry.offset >= from_offset
            }) {
                if entry.value == STREAM_CLOSED_SENTINEL {
                    closed = true;
                    break;
                }
                entries.push(crate::types::StreamEntry {
                    value: entry.value.clone(),
                    offset: entry.offset,
                    serialization: entry.serialization.clone(),
                });
            }

            Ok((entries, closed))
        }

        async fn get_workflows_for_recovery(
            &self,
            executor_id: &str,
        ) -> JournioResult<Vec<WorkflowStatus>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state
                .workflows
                .values()
                .filter(|workflow| {
                    workflow.executor_id == executor_id
                        && matches!(
                            workflow.status,
                            WorkflowStatusType::Pending
                                | WorkflowStatusType::Enqueued
                                | WorkflowStatusType::Delayed
                        )
                })
                .cloned()
                .collect())
        }

        async fn delete_workflows_before(&self, _before: DateTime<Utc>) -> JournioResult<u64> {
            Ok(0)
        }

        async fn upsert_schedule(&self, schedule: &WorkflowSchedule) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            state
                .schedules
                .insert(schedule.schedule_name.clone(), schedule.clone());
            Ok(())
        }

        async fn get_schedule(
            &self,
            schedule_name: &str,
        ) -> JournioResult<Option<WorkflowSchedule>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state.schedules.get(schedule_name).cloned())
        }

        async fn list_schedules(&self) -> JournioResult<Vec<WorkflowSchedule>> {
            let state = self.state.lock().expect("fake db lock");
            let mut schedules: Vec<_> = state.schedules.values().cloned().collect();
            schedules.sort_by(|left, right| left.schedule_name.cmp(&right.schedule_name));
            Ok(schedules)
        }

        async fn delete_schedule(&self, schedule_name: &str) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            state.schedules.remove(schedule_name);
            Ok(())
        }

        async fn update_schedule_status(
            &self,
            schedule_name: &str,
            status: ScheduleStatus,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if let Some(schedule) = state.schedules.get_mut(schedule_name) {
                schedule.status = status;
            }
            Ok(())
        }

        async fn update_schedule_last_fired_at(
            &self,
            schedule_name: &str,
            fired_at: DateTime<Utc>,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if let Some(schedule) = state.schedules.get_mut(schedule_name) {
                schedule.last_fired_at = Some(fired_at);
            }
            Ok(())
        }

        async fn create_application_version(&self, version_name: &str) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if state
                .application_versions
                .iter()
                .any(|version| version.version_name == version_name)
            {
                return Ok(());
            }
            let now_ms = Utc::now().timestamp_millis();
            state.application_versions.push(VersionInfo {
                version_id: uuid::Uuid::new_v4().to_string(),
                version_name: version_name.to_string(),
                version_timestamp: now_ms,
                created_at: now_ms,
            });
            Ok(())
        }

        async fn update_application_version_timestamp(
            &self,
            version_name: &str,
            timestamp_ms: i64,
        ) -> JournioResult<()> {
            let mut state = self.state.lock().expect("fake db lock");
            if let Some(version) = state
                .application_versions
                .iter_mut()
                .find(|version| version.version_name == version_name)
            {
                version.version_timestamp = timestamp_ms;
            }
            Ok(())
        }

        async fn list_application_versions(&self) -> JournioResult<Vec<VersionInfo>> {
            let state = self.state.lock().expect("fake db lock");
            let mut versions = state.application_versions.clone();
            versions.sort_by(|left, right| right.version_timestamp.cmp(&left.version_timestamp));
            Ok(versions)
        }

        async fn get_latest_application_version(&self) -> JournioResult<Option<VersionInfo>> {
            let state = self.state.lock().expect("fake db lock");
            Ok(state
                .application_versions
                .iter()
                .max_by_key(|version| version.version_timestamp)
                .cloned())
        }
    }

    /// Mirrors the WHERE-clause logic the real backends build from
    /// [`ListWorkflowsFilter`].
    fn matches_filter(wf: &WorkflowStatus, filter: &ListWorkflowsFilter) -> bool {
        if !filter.workflow_ids.is_empty() && !filter.workflow_ids.contains(&wf.id) {
            return false;
        }
        if !filter.workflow_id_prefixes.is_empty()
            && !filter
                .workflow_id_prefixes
                .iter()
                .any(|prefix| wf.id.starts_with(prefix))
        {
            return false;
        }
        if !filter.statuses.is_empty() && !filter.statuses.contains(&wf.status) {
            return false;
        }
        if !filter.names.is_empty() && !filter.names.contains(&wf.name) {
            return false;
        }
        if !filter.application_versions.is_empty()
            && !filter
                .application_versions
                .iter()
                .any(|version| *version == wf.application_version)
        {
            return false;
        }
        if !filter.queue_names.is_empty()
            && !wf
                .queue_name
                .as_deref()
                .is_some_and(|name| filter.queue_names.iter().any(|q| q == name))
        {
            return false;
        }
        if filter.queues_only && wf.queue_name.is_none() {
            return false;
        }
        if !filter.authenticated_users.is_empty()
            && !wf
                .authenticated_user
                .as_deref()
                .is_some_and(|user| filter.authenticated_users.iter().any(|u| u == user))
        {
            return false;
        }
        if !filter.executor_ids.is_empty() && !filter.executor_ids.contains(&wf.executor_id) {
            return false;
        }
        if !filter.forked_from.is_empty()
            && !wf
                .forked_from
                .as_deref()
                .is_some_and(|from| filter.forked_from.iter().any(|f| f == from))
        {
            return false;
        }
        if !filter.parent_workflow_ids.is_empty()
            && !wf
                .parent_workflow_id
                .as_deref()
                .is_some_and(|parent| filter.parent_workflow_ids.iter().any(|p| p == parent))
        {
            return false;
        }
        if !filter.deduplication_ids.is_empty()
            && !wf
                .deduplication_id
                .as_deref()
                .is_some_and(|id| filter.deduplication_ids.iter().any(|d| d == id))
        {
            return false;
        }
        if let Some(start) = filter.start_time {
            if wf.created_at < start {
                return false;
            }
        }
        if let Some(end) = filter.end_time {
            if wf.created_at > end {
                return false;
            }
        }
        if let Some(after) = filter.completed_after {
            if !wf.completed_at.is_some_and(|completed| completed >= after) {
                return false;
            }
        }
        if let Some(before) = filter.completed_before {
            if !wf.completed_at.is_some_and(|completed| completed <= before) {
                return false;
            }
        }
        true
    }

    fn workflow_status_from_init(init: &InitWorkflow) -> WorkflowStatus {
        WorkflowStatus {
            id: init.workflow_id.clone(),
            status: init.status,
            name: init.name.clone(),
            authenticated_user: init.authenticated_user.clone(),
            assumed_role: init.assumed_role.clone(),
            authenticated_roles: init.authenticated_roles.clone(),
            output: None,
            error: None,
            executor_id: init.executor_id.clone(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            application_version: init.application_version.clone().unwrap_or_default(),
            application_id: init.application_id.clone(),
            attempts: 1,
            queue_name: init.queue_name.clone(),
            timeout: init.timeout,
            deadline: init.deadline,
            started_at: None,
            deduplication_id: init.deduplication_id.clone(),
            input: init.input.clone(),
            priority: init.priority,
            queue_partition_key: init.queue_partition_key.clone(),
            forked_from: None,
            was_forked_from: false,
            parent_workflow_id: init.parent_workflow_id.clone(),
            completed_at: None,
            class_name: init.class_name.clone(),
            config_name: init.config_name.clone(),
            serialization: init.serialization.clone(),
            delay_until: init.delay_until,
        }
    }

    async fn test_context() -> (Arc<JournioContext>, Arc<FakeSystemDatabase>) {
        let fake = Arc::new(FakeSystemDatabase::default());
        let mut config = Config::default();
        config.app_name = "test-app".to_string();
        config.system_db = Some(fake.clone());
        config.executor_id = Some("local".to_string());
        let ctx = JournioContext::new(config).await.expect("context");
        (ctx, fake)
    }

    #[tokio::test]
    async fn run_workflow_executes_step_once_and_persists_output() {
        let (ctx, fake) = test_context().await;
        let counter = Arc::new(AtomicUsize::new(0));

        let step_counter = counter.clone();
        let step = Arc::new(StepFunc::new("step", move |_ctx| {
            let step_counter = step_counter.clone();
            Box::pin(async move {
                step_counter.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(21_i64)
            })
        }));

        let workflow_step = step.clone();
        let workflow = Arc::new(WorkflowFn::new("workflow", move |ctx, input: i64| {
            let workflow_step = workflow_step.clone();
            Box::pin(async move {
                let value = ctx.run_as_step(workflow_step).await?;
                let step_output: i64 = serde_json::from_value(value).expect("step output");
                Ok(step_output + input)
            })
        }));

        ctx.register_workflow(workflow).expect("register workflow");
        let handle = ctx
            .run_workflow("workflow", serde_json::json!(21))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(1)))
            .await
            .expect("result");

        assert_eq!(
            serde_json::from_value::<i64>(result).expect("workflow output"),
            42
        );
        assert_eq!(counter.load(AtomicOrdering::SeqCst), 1);

        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].function_name, "step");
    }

    #[tokio::test]
    async fn workflow_handle_times_out_for_pending_workflow() {
        let (ctx, fake) = test_context().await;
        fake.init_workflow(InitWorkflow::new_pending(
            "pending",
            "pending-workflow",
            "local",
        ))
        .await
        .expect("insert pending workflow");

        let handle = WorkflowHandle {
            workflow_id: "pending".to_string(),
            journio: Arc::downgrade(&ctx),
        };

        let err = handle
            .get_result(Some(Duration::from_millis(50)))
            .await
            .expect_err("timeout expected");
        assert_eq!(err.code, JournioErrorCode::TimeoutError);
    }

    #[tokio::test]
    async fn recovery_replays_pending_workflow_without_reexecuting_completed_step() {
        let (ctx, fake) = test_context().await;
        let step_one_counter = Arc::new(AtomicUsize::new(0));
        let step_two_counter = Arc::new(AtomicUsize::new(0));

        let workflow_id = "recover-me".to_string();
        let mut init = InitWorkflow::new_pending(workflow_id.clone(), "recovery-workflow", "local");
        init.input = Some(serde_json::json!(2));
        fake.init_workflow(init).await.expect("seed workflow");
        fake.record_step_output(&StepRecord {
            workflow_uuid: workflow_id.clone(),
            function_id: 1,
            function_name: "step-one".to_string(),
            output: Some(serde_json::json!(40).to_string()),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("seed checkpoint");

        let step_one = Arc::new(StepFunc::new("step-one", {
            let step_one_counter = step_one_counter.clone();
            move |_ctx| {
                let step_one_counter = step_one_counter.clone();
                Box::pin(async move {
                    step_one_counter.fetch_add(1, AtomicOrdering::SeqCst);
                    Ok(40_i64)
                })
            }
        }));

        let step_two = Arc::new(StepFunc::new("step-two", {
            let step_two_counter = step_two_counter.clone();
            move |_ctx| {
                let step_two_counter = step_two_counter.clone();
                Box::pin(async move {
                    step_two_counter.fetch_add(1, AtomicOrdering::SeqCst);
                    Ok(2_i64)
                })
            }
        }));

        let workflow = Arc::new(WorkflowFn::new(
            "recovery-workflow",
            move |ctx, input: i64| {
                let step_one = step_one.clone();
                let step_two = step_two.clone();
                Box::pin(async move {
                    let first = serde_json::from_value::<i64>(ctx.run_as_step(step_one).await?)
                        .expect("first");
                    let second = serde_json::from_value::<i64>(ctx.run_as_step(step_two).await?)
                        .expect("second");
                    Ok(first + second + input)
                })
            },
        ));

        ctx.register_workflow(workflow).expect("register workflow");
        ctx.launch().await.expect("launch and recover");

        let status = fake
            .get_workflow_status(&workflow_id)
            .await
            .expect("workflow status")
            .expect("status row");
        assert_eq!(status.status, WorkflowStatusType::Success);
        assert_eq!(status.output.expect("output"), serde_json::json!(44));
        assert_eq!(step_one_counter.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(step_two_counter.load(AtomicOrdering::SeqCst), 1);
    }

    // ------------------------------------------------------------------
    // Send / Recv
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn send_and_recv_within_workflow_round_trips_message() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("self-recv", |ctx, _input: ()| {
            Box::pin(async move {
                // Send to self, then receive. Both are checkpointed steps.
                ctx.send(ctx.workflow_id(), serde_json::json!("hello"), "greetings")
                    .await?;
                ctx.recv("greetings", Duration::from_secs(1)).await
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("self-recv", serde_json::json!(null))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");

        assert_eq!(result, serde_json::json!("hello"));

        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        let names: Vec<&str> = steps.iter().map(|s| s.function_name.as_str()).collect();
        assert_eq!(names, vec!["journio.send", "journio.recv"]);
        // The recv step carries the consumed message.
        let recv_step = steps
            .iter()
            .find(|s| s.function_name == "journio.recv")
            .expect("recv step");
        assert_eq!(recv_step.output.as_deref(), Some("\"hello\""));
    }

    #[tokio::test]
    async fn recv_times_out_and_records_error_for_replay() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("waiter", |ctx, _input: ()| {
            Box::pin(async move {
                match ctx.recv("empty", Duration::from_millis(60)).await {
                    Ok(value) => Ok(value),
                    // Normalise to a value so the workflow still completes SUCCESS.
                    Err(err) if err.code == JournioErrorCode::TimeoutError => {
                        Ok(serde_json::json!("timed out"))
                    }
                    Err(err) => Err(err),
                }
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("waiter", serde_json::json!(null))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!("timed out"));

        // The timeout must be checkpointed so replay returns the same outcome.
        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        let recv_step = steps
            .iter()
            .find(|s| s.function_name == "journio.recv")
            .expect("recv step");
        assert!(recv_step.error.as_deref().unwrap().contains("no message"));
    }

    #[tokio::test]
    async fn cross_workflow_send_via_top_level_send_delivers_message() {
        let (ctx, _fake) = test_context().await;

        // The receiver runs in the background (it blocks on Recv). It publishes
        // its workflow id through a shared slot so the sender can target it.
        let id_slot: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let receiver_workflow = {
            let id_slot = id_slot.clone();
            Arc::new(WorkflowFn::new("receiver", move |ctx, _input: ()| {
                let id_slot = id_slot.clone();
                Box::pin(async move {
                    *id_slot.lock().expect("slot") = Some(ctx.workflow_id().to_string());
                    ctx.recv("topic", Duration::from_secs(2)).await
                })
            }))
        };
        ctx.register_workflow(receiver_workflow)
            .expect("register receiver");

        let runner_ctx = ctx.clone();
        let receiver_task = tokio::spawn(async move {
            runner_ctx
                .run_workflow("receiver", serde_json::json!(null))
                .await
        });

        // Wait for the receiver to publish its id, then send (top-level, no checkpoint).
        let receiver_id = loop {
            if let Some(id) = id_slot.lock().expect("slot").clone() {
                break id;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        ctx.send(&receiver_id, serde_json::json!("payload"), "topic")
            .await
            .expect("top-level send");

        let handle = receiver_task
            .await
            .expect("receiver task")
            .expect("run receiver");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!("payload"));
    }

    // ------------------------------------------------------------------
    // SetEvent / GetEvent
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn set_and_get_event_within_workflow_round_trips_value() {
        let (ctx, _fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("eventer", |ctx, _input: ()| {
            Box::pin(async move {
                ctx.set_event("status", serde_json::json!("done")).await?;
                ctx.get_event(ctx.workflow_id(), "status", Duration::from_secs(1))
                    .await
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("eventer", serde_json::json!(null))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!("done"));
    }

    #[tokio::test]
    async fn get_event_times_out_and_records_error_for_replay() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("poller", |ctx, _input: ()| {
            Box::pin(async move {
                match ctx
                    .get_event(ctx.workflow_id(), "never", Duration::from_millis(60))
                    .await
                {
                    Ok(value) => Ok(value),
                    Err(err) if err.code == JournioErrorCode::TimeoutError => {
                        Ok(serde_json::json!("timed out"))
                    }
                    Err(err) => Err(err),
                }
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("poller", serde_json::json!(null))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!("timed out"));

        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        let get_step = steps
            .iter()
            .find(|s| s.function_name == "journio.getEvent")
            .expect("getEvent step");
        assert!(get_step.error.as_deref().unwrap().contains("not set"));
    }

    // ------------------------------------------------------------------
    // Sleep
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn sleep_records_deadline_and_completes() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("sleeper", |ctx, _input: ()| {
            Box::pin(async move {
                ctx.sleep(Duration::from_millis(30)).await?;
                Ok(serde_json::json!("woke"))
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("sleeper", serde_json::json!(null))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(2)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!("woke"));

        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        let sleep_step = steps
            .iter()
            .find(|s| s.function_name == "journio.sleep")
            .expect("sleep step recorded");
        assert!(
            sleep_step.output.is_some(),
            "sleep deadline was checkpointed"
        );
    }

    #[tokio::test]
    async fn sleep_replay_does_not_block_when_deadline_has_passed() {
        let (ctx, fake) = test_context().await;
        let workflow_id = "sleep-replay".to_string();
        let mut init = InitWorkflow::new_pending(workflow_id.clone(), "sleeper", "local");
        init.input = Some(serde_json::json!(null));
        fake.init_workflow(init).await.expect("seed workflow");

        // Seed a sleep checkpoint with a deadline already in the past.
        let past = (Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
        fake.record_step_output(&StepRecord {
            workflow_uuid: workflow_id.clone(),
            function_id: 1,
            function_name: "journio.sleep".to_string(),
            output: Some(serde_json::to_string(&serde_json::Value::String(past)).expect("encode")),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("seed sleep checkpoint");

        let workflow = Arc::new(WorkflowFn::new("sleeper", |ctx, _input: ()| {
            Box::pin(async move {
                // 10s sleep — but the recorded deadline is in the past, so replay is instant.
                ctx.sleep(Duration::from_secs(10)).await?;
                Ok(serde_json::json!("done"))
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        // launch() runs recovery; if sleep re-blocked this would hang until the test timeout.
        let launched = tokio::time::timeout(Duration::from_secs(2), ctx.launch()).await;
        launched.expect("launch did not hang").expect("launch ok");

        let status = fake
            .get_workflow_status(&workflow_id)
            .await
            .expect("status")
            .expect("status row");
        assert_eq!(status.status, WorkflowStatusType::Success);
        assert_eq!(status.output.expect("output"), serde_json::json!("done"));
    }

    // ------------------------------------------------------------------
    // Patch / DeprecatePatch
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn patch_records_marker_and_selects_new_code_path() {
        let fake = Arc::new(FakeSystemDatabase::default());
        let mut config = Config::default();
        config.app_name = "test-app".to_string();
        config.system_db = Some(fake.clone());
        config.executor_id = Some("local".to_string());
        config.enable_patching = true;
        let ctx = JournioContext::new(config).await.expect("context");

        let workflow = Arc::new(WorkflowFn::new("patcher", |ctx, input: i64| {
            Box::pin(async move {
                let output = if ctx.patch("my-patch").await? {
                    input + 2
                } else {
                    input + 1
                };
                Ok(output)
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .run_workflow("patcher", serde_json::json!(1))
            .await
            .expect("run workflow");
        let result = handle
            .get_result(Some(Duration::from_secs(1)))
            .await
            .expect("result");

        assert_eq!(serde_json::from_value::<i64>(result).expect("result"), 3);
        let steps = fake.get_steps(handle.workflow_id()).await.expect("steps");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].function_name, "journio.patch-my-patch");
    }

    #[tokio::test]
    async fn patch_replay_preserves_old_code_path_for_existing_workflow() {
        let fake = Arc::new(FakeSystemDatabase::default());
        let mut config = Config::default();
        config.app_name = "test-app".to_string();
        config.system_db = Some(fake.clone());
        config.executor_id = Some("local".to_string());
        config.enable_patching = true;
        let ctx = JournioContext::new(config).await.expect("context");

        let workflow_id = "pre-patch".to_string();
        let mut init = InitWorkflow::new_pending(workflow_id.clone(), "patcher", "local");
        init.input = Some(serde_json::json!(1));
        fake.init_workflow(init).await.expect("seed workflow");
        fake.record_step_output(&StepRecord {
            workflow_uuid: workflow_id.clone(),
            function_id: 1,
            function_name: "legacy-step".to_string(),
            output: Some(serde_json::json!(2).to_string()),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("seed legacy step");

        let legacy_step = Arc::new(StepFunc::new("legacy-step", |_ctx| {
            Box::pin(async move { Ok(2_i64) })
        }));
        let patched_step = Arc::new(StepFunc::new("patched-step", |_ctx| {
            Box::pin(async move { Ok(3_i64) })
        }));

        let workflow = Arc::new(WorkflowFn::new("patcher", move |ctx, _input: i64| {
            let legacy_step = legacy_step.clone();
            let patched_step = patched_step.clone();
            Box::pin(async move {
                let chosen = if ctx.patch("my-patch").await? {
                    ctx.run_as_step(patched_step).await?
                } else {
                    ctx.run_as_step(legacy_step).await?
                };
                Ok(chosen)
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        ctx.launch().await.expect("launch and recover");

        let status = fake
            .get_workflow_status(&workflow_id)
            .await
            .expect("status")
            .expect("status row");
        assert_eq!(status.status, WorkflowStatusType::Success);
        assert_eq!(status.output.expect("output"), serde_json::json!(2));

        let steps = fake.get_steps(&workflow_id).await.expect("steps");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].function_name, "legacy-step");
    }

    #[tokio::test]
    async fn deprecate_patch_consumes_existing_marker_without_nondeterminism() {
        let fake = Arc::new(FakeSystemDatabase::default());
        let mut config = Config::default();
        config.app_name = "test-app".to_string();
        config.system_db = Some(fake.clone());
        config.executor_id = Some("local".to_string());
        config.enable_patching = true;
        let ctx = JournioContext::new(config).await.expect("context");

        let workflow_id = "deprecated-patch".to_string();
        let mut init = InitWorkflow::new_pending(workflow_id.clone(), "patcher", "local");
        init.input = Some(serde_json::json!(1));
        fake.init_workflow(init).await.expect("seed workflow");
        fake.record_step_output(&StepRecord {
            workflow_uuid: workflow_id.clone(),
            function_id: 1,
            function_name: "journio.patch-my-patch".to_string(),
            output: None,
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("seed patch marker");
        fake.record_step_output(&StepRecord {
            workflow_uuid: workflow_id.clone(),
            function_id: 2,
            function_name: "patched-step".to_string(),
            output: Some(serde_json::json!(3).to_string()),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("seed patched step");

        let patched_step = Arc::new(StepFunc::new("patched-step", |_ctx| {
            Box::pin(async move { Ok(3_i64) })
        }));

        let workflow = Arc::new(WorkflowFn::new("patcher", move |ctx, _input: i64| {
            let patched_step = patched_step.clone();
            Box::pin(async move {
                ctx.deprecate_patch("my-patch").await?;
                ctx.run_as_step(patched_step).await
            })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        ctx.launch().await.expect("launch and recover");

        let status = fake
            .get_workflow_status(&workflow_id)
            .await
            .expect("status")
            .expect("status row");
        assert_eq!(status.status, WorkflowStatusType::Success);
        assert_eq!(status.output.expect("output"), serde_json::json!(3));

        let steps = fake.get_steps(&workflow_id).await.expect("steps");
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].function_name, "journio.patch-my-patch");
        assert_eq!(steps[1].function_name, "patched-step");
    }

    #[tokio::test]
    async fn patch_returns_error_when_patching_is_disabled() {
        let (ctx, _fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("patch-disabled", |ctx, _input: ()| {
            Box::pin(async move { Ok(ctx.patch("my-patch").await?) })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let err = match ctx
            .run_workflow("patch-disabled", serde_json::json!(null))
            .await
        {
            Ok(_) => panic!("patching should fail"),
            Err(err) => err,
        };
        assert_eq!(err.code, JournioErrorCode::PatchingNotEnabled);
        assert!(err.message.contains("Patching system is not enabled"));
    }

    #[tokio::test]
    async fn enqueue_workflow_and_run_queue_once_executes_workflow() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("queued-workflow", |_ctx, input: i64| {
            Box::pin(async move { Ok(input + 1) })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .enqueue_workflow(
                "jobs",
                "queued-workflow",
                serde_json::json!(41),
                EnqueueOptions::default(),
            )
            .await
            .expect("enqueue workflow");

        let queued_status = fake
            .get_workflow_status(handle.workflow_id())
            .await
            .expect("queued status")
            .expect("queued row");
        assert_eq!(queued_status.status, WorkflowStatusType::Enqueued);

        let ran = ctx.run_queue_once("jobs").await.expect("run queue");
        assert!(ran.is_some(), "queue worker should find one workflow");

        let result = handle
            .get_result(Some(Duration::from_secs(1)))
            .await
            .expect("result");
        assert_eq!(result, serde_json::json!(42));
    }

    #[tokio::test]
    async fn run_queue_once_skips_not_yet_due_delayed_workflow() {
        let (ctx, fake) = test_context().await;
        let workflow = Arc::new(WorkflowFn::new("delayed-workflow", |_ctx, input: i64| {
            Box::pin(async move { Ok(input + 1) })
        }));
        ctx.register_workflow(workflow).expect("register workflow");

        let handle = ctx
            .enqueue_workflow(
                "jobs",
                "delayed-workflow",
                serde_json::json!(1),
                EnqueueOptions {
                    delay_until: Some(Utc::now() + chrono::Duration::minutes(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("enqueue delayed workflow");

        let ran = ctx.run_queue_once("jobs").await.expect("run queue");
        assert!(
            ran.is_none(),
            "delayed workflow should not run before due time"
        );

        let status = fake
            .get_workflow_status(handle.workflow_id())
            .await
            .expect("status")
            .expect("row");
        assert_eq!(status.status, WorkflowStatusType::Delayed);
    }
}
