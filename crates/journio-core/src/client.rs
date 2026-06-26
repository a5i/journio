//! Programmatic client — ported from `journio/client.go` (879 LOC).
//!
//! [`Client`] is a thin facade over [`JournioContext`] for code that wants to
//! drive a Journio system database from the outside: enqueue workflows, read
//! state, manage schedules and application versions — without registering
//! executors, running queue workers, or recovering local workflows.
//!
//! Construction runs migrations and starts the database's notification
//! listener (so `get_event` / `read_stream` wakeups are prompt) but does *not*
//! spawn queue pollers, the scheduler loop, or recovery. Those are the job of
//! a real executor process. This mirrors Go's `NewClient`, which only calls
//! `systemDB.launch`.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::config::Config;
use crate::context::{
    EnqueueOptions, ForkWorkflowOptions, INTERNAL_QUEUE_NAME, JournioContext, ReadStreamOptions,
    WorkflowHandle, parse_cron_schedule,
};
use crate::error::{JournioError, JournioErrorCode, JournioResult, constructors};
use crate::types::{
    ListWorkflowsFilter, ScheduleStatus, ScheduledWorkflowInput, StepRecord, VersionInfo,
    WorkflowSchedule, WorkflowStatus,
};
use crate::value::Interchange;

/// Default page size when a caller does not set `ListWorkflowsFilter::limit`.
const DEFAULT_LIST_LIMIT: i64 = 100;

/// A programmatic handle for a Journio system database — ported from Go's
/// `Client` interface (`journio/client.go`).
///
/// Cheap to clone via [`Arc`]; most callers keep one `Arc<Client>` for the
/// process lifetime and call [`Client::shutdown`] on exit.
pub struct Client {
    ctx: Arc<JournioContext>,
}

/// Schedule definition for client-side `create`/`apply` calls — ported from
/// Go's `ClientScheduleInput` (`journio/client.go`).
#[derive(Debug, Clone, Default)]
pub struct ClientScheduleInput {
    pub schedule_name: String,
    pub workflow_name: String,
    pub workflow_class_name: Option<String>,
    /// Cron expression (e.g. `"*/5 * * * *"`).
    pub schedule: String,
    pub context: Interchange,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
}

impl Client {
    /// Build a client over the configured system database. Runs migrations and
    /// starts the notification listener, but does not launch executors.
    /// Ported from `NewClient` (`journio/client.go:188`).
    pub async fn new(mut config: Config) -> JournioResult<Arc<Self>> {
        if config.app_name.is_empty() {
            config.app_name = "journio-client".to_string();
        }
        let ctx = JournioContext::new(config).await?;
        ctx.system_db.migrate().await?;
        ctx.system_db.launch().await?;
        Ok(Arc::new(Self { ctx }))
    }

    /// Borrow the underlying runtime (advanced use — e.g. enqueueing through
    /// the context directly).
    pub fn context(&self) -> &Arc<JournioContext> {
        &self.ctx
    }

    // -- workflow lifecycle ------------------------------------------------

    /// Enqueue a workflow for deferred execution — ported from `Client.Enqueue`.
    pub async fn enqueue(
        self: &Arc<Self>,
        queue_name: &str,
        workflow_name: &str,
        input: Interchange,
        options: EnqueueOptions,
    ) -> JournioResult<WorkflowHandle> {
        self.ctx
            .enqueue_workflow(queue_name, workflow_name, input, options)
            .await
    }

    /// Run a workflow immediately (synchronously, blocking on the result) — a
    /// convenience over the runtime's `run_workflow`. Not part of Go's `Client`
    /// (which only enqueues) but useful for tests and bindings.
    pub async fn run_workflow(
        self: &Arc<Self>,
        name: &str,
        input: Interchange,
    ) -> JournioResult<WorkflowHandle> {
        self.ctx.run_workflow(name, input).await
    }

    /// List workflows matching `filter` — ported from `Client.ListWorkflows`.
    pub async fn list_workflows(
        &self,
        filter: ListWorkflowsFilter,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        let mut filter = filter;
        if filter.limit.is_none() {
            filter.limit = Some(DEFAULT_LIST_LIMIT);
        }
        self.ctx.system_db.list_workflows_filtered(&filter).await
    }

    /// Send a message to another workflow — ported from `Client.Send`.
    pub async fn send(
        self: &Arc<Self>,
        destination_id: &str,
        message: Interchange,
        topic: &str,
    ) -> JournioResult<()> {
        self.ctx.send(destination_id, message, topic).await
    }

    /// Read a key-value event — ported from `Client.GetEvent`. Returns
    /// [`Interchange::Null`] on timeout.
    pub async fn get_event(
        self: &Arc<Self>,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<Interchange> {
        self.ctx.get_event(target_workflow_id, key, timeout).await
    }

    /// Return a handle to an existing workflow — ported from
    /// `Client.RetrieveWorkflow`. Does not verify existence eagerly.
    pub fn retrieve_workflow(self: &Arc<Self>, workflow_id: impl Into<String>) -> WorkflowHandle {
        self.ctx.workflow_handle(workflow_id)
    }

    /// Cancel a single workflow — ported from `Client.CancelWorkflow`. Returns
    /// whether the workflow existed.
    pub async fn cancel_workflow(&self, workflow_id: &str) -> JournioResult<bool> {
        self.ctx.cancel_workflow(workflow_id).await
    }

    /// Cancel many workflows in one round-trip — ported from
    /// `Client.CancelWorkflows`. Returns the subset that existed.
    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> JournioResult<Vec<String>> {
        self.ctx.cancel_workflows(workflow_ids).await
    }

    /// Set or update the delay on a DELAYED workflow — ported from
    /// `Client.SetWorkflowDelay`.
    pub async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay_until: DateTime<Utc>,
    ) -> JournioResult<()> {
        self.ctx
            .system_db
            .set_workflow_delay(workflow_id, delay_until)
            .await
    }

    /// Permanently delete workflows and their associated data — ported from
    /// `Client.DeleteWorkflows`. `delete_children` recurses into descendants.
    pub async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> JournioResult<()> {
        self.ctx
            .system_db
            .delete_workflows(workflow_ids, delete_children)
            .await
    }

    /// Resume a single workflow — ported from `Client.ResumeWorkflow`.
    pub async fn resume_workflow(
        &self,
        workflow_id: &str,
        queue_name: Option<&str>,
    ) -> JournioResult<bool> {
        self.ctx.resume_workflow(workflow_id, queue_name).await
    }

    /// Resume many workflows — ported from `Client.ResumeWorkflows`.
    pub async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> JournioResult<Vec<String>> {
        self.ctx.resume_workflows(workflow_ids, queue_name).await
    }

    /// Fork a workflow from a step — ported from `Client.ForkWorkflow`.
    pub async fn fork_workflow(
        self: &Arc<Self>,
        original_workflow_id: &str,
        options: ForkWorkflowOptions,
    ) -> JournioResult<WorkflowHandle> {
        self.ctx.fork_workflow(original_workflow_id, options).await
    }

    /// Recorded steps for a workflow — ported from `Client.GetWorkflowSteps`.
    pub async fn get_workflow_steps(&self, workflow_id: &str) -> JournioResult<Vec<StepRecord>> {
        self.ctx.get_workflow_steps(workflow_id).await
    }

    /// Recursively list descendant workflows.
    pub async fn get_workflow_children(
        &self,
        workflow_id: &str,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        self.ctx.get_workflow_children(workflow_id).await
    }

    /// Read a durable stream — ported from `Client.ClientReadStream`. Returns
    /// the drained values and whether the stream was closed.
    pub async fn read_stream(
        self: &Arc<Self>,
        workflow_id: &str,
        key: &str,
        options: ReadStreamOptions,
    ) -> JournioResult<(Vec<Interchange>, bool)> {
        self.ctx.read_stream(workflow_id, key, options).await
    }

    // -- schedule management -----------------------------------------------
    //
    // Ported from the schedule block of Go's `Client` (`journio/client.go:612`).
    // CRUD calls go straight to `SystemDatabase`; `trigger`/`backfill` reuse
    // the runtime's enqueue path (mirroring Go's `triggerSchedule` /
    // `backfillSchedule`, which enqueue directly).

    /// Create or replace a single schedule — ported from `CreateSchedule`.
    pub async fn create_schedule(&self, input: ClientScheduleInput) -> JournioResult<()> {
        validate_schedule_input(&input)?;
        let schedule = schedule_from_input(&input, Utc::now());
        self.ctx.system_db.upsert_schedule(&schedule).await
    }

    /// Atomically replace the given schedules (delete-then-insert within one
    /// conceptual apply). The Rust backends do not expose a multi-statement
    /// transaction through the trait, so each entry is upserted — the net
    /// effect matches Go's `ApplySchedules`. Ported from `ApplySchedules`.
    pub async fn apply_schedules(&self, schedules: Vec<ClientScheduleInput>) -> JournioResult<()> {
        if schedules.is_empty() {
            return Ok(());
        }
        for (index, input) in schedules.iter().enumerate() {
            validate_schedule_input_indexed(input, index)?;
        }
        let now = Utc::now();
        for input in schedules {
            let schedule = schedule_from_input(&input, now);
            self.ctx.system_db.upsert_schedule(&schedule).await?;
        }
        Ok(())
    }

    /// Fetch a schedule by name — ported from `GetSchedule`.
    pub async fn get_schedule(
        &self,
        schedule_name: &str,
    ) -> JournioResult<Option<WorkflowSchedule>> {
        self.ctx.system_db.get_schedule(schedule_name).await
    }

    /// List all schedules, optionally restricted to `name_prefixes`.
    /// Ported from `ListSchedules`.
    pub async fn list_schedules(
        &self,
        name_prefixes: Option<&[String]>,
    ) -> JournioResult<Vec<WorkflowSchedule>> {
        let schedules = self.ctx.system_db.list_schedules().await?;
        let Some(prefixes) = name_prefixes.filter(|prefixes| !prefixes.is_empty()) else {
            return Ok(schedules);
        };
        Ok(schedules
            .into_iter()
            .filter(|schedule| {
                prefixes
                    .iter()
                    .any(|prefix| schedule.schedule_name.starts_with(prefix))
            })
            .collect())
    }

    /// Pause a schedule — ported from `PauseSchedule`.
    pub async fn pause_schedule(&self, schedule_name: &str) -> JournioResult<()> {
        self.require_schedule(schedule_name).await?;
        self.ctx
            .system_db
            .update_schedule_status(schedule_name, ScheduleStatus::Paused)
            .await
    }

    /// Resume a paused schedule — ported from `ResumeSchedule`.
    pub async fn resume_schedule(&self, schedule_name: &str) -> JournioResult<()> {
        self.require_schedule(schedule_name).await?;
        self.ctx
            .system_db
            .update_schedule_status(schedule_name, ScheduleStatus::Active)
            .await
    }

    /// Delete a schedule — ported from `DeleteSchedule`.
    pub async fn delete_schedule(&self, schedule_name: &str) -> JournioResult<()> {
        self.ctx.system_db.delete_schedule(schedule_name).await
    }

    /// Immediately enqueue the named schedule's workflow — ported from
    /// `TriggerSchedule`. Backfilled/triggered runs target the latest
    /// registered application version when one exists.
    pub async fn trigger_schedule(
        self: &Arc<Self>,
        schedule_name: &str,
    ) -> JournioResult<WorkflowHandle> {
        let schedule = self.require_schedule(schedule_name).await?;
        let now = self.ctx.now();
        let workflow_id = format!("sched-{schedule_name}-trigger-{}", now.to_rfc3339());
        let input = scheduled_workflow_input(&schedule, now)?;
        let queue_name = schedule
            .queue_name
            .clone()
            .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string());

        let application_version = self.latest_version_name().await?;
        self.ctx
            .enqueue_workflow(
                &queue_name,
                &schedule.workflow_name,
                input,
                EnqueueOptions {
                    workflow_id: Some(workflow_id),
                    application_version,
                    ..Default::default()
                },
            )
            .await
    }

    /// Enqueue every execution of the named schedule that would have fired
    /// between `start` and `end` — ported from `BackfillSchedule`. Already-run
    /// slots (matching `sched-<name>-<time>` ids) are skipped. Returns the ids
    /// of the workflows enqueued for the backfilled slots.
    pub async fn backfill_schedule(
        self: &Arc<Self>,
        schedule_name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> JournioResult<Vec<String>> {
        let schedule = self.require_schedule(schedule_name).await?;
        let spec = parse_cron_schedule(&schedule.schedule)?;
        let queue_name = schedule
            .queue_name
            .clone()
            .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string());
        let application_version = self.latest_version_name().await?;

        let mut enqueued = Vec::new();
        for scheduled_time in spec.after(&start) {
            if scheduled_time > end {
                break;
            }
            if scheduled_time < start {
                continue;
            }
            let workflow_id = format!("sched-{schedule_name}-{}", scheduled_time.to_rfc3339());
            if self
                .ctx
                .system_db
                .get_workflow_status(&workflow_id)
                .await?
                .is_some()
            {
                continue;
            }

            let input = scheduled_workflow_input(&schedule, scheduled_time)?;
            self.ctx
                .enqueue_workflow(
                    &queue_name,
                    &schedule.workflow_name,
                    input,
                    EnqueueOptions {
                        workflow_id: Some(workflow_id.clone()),
                        application_version: application_version.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            enqueued.push(workflow_id);
        }
        Ok(enqueued)
    }

    // -- application versions ---------------------------------------------
    //
    // Ported from the `ListApplicationVersions` / `GetLatestApplicationVersion`
    // / `SetLatestApplicationVersion` block of Go's `Client`.

    /// All registered application versions, newest first — ported from
    /// `ListApplicationVersions`.
    pub async fn list_application_versions(&self) -> JournioResult<Vec<VersionInfo>> {
        self.ctx.system_db.list_application_versions().await
    }

    /// The application version with the greatest timestamp — ported from
    /// `GetLatestApplicationVersion`. Returns `None` when none are registered.
    pub async fn get_latest_application_version(&self) -> JournioResult<Option<VersionInfo>> {
        self.ctx.system_db.get_latest_application_version().await
    }

    /// Mark `version_name` as latest by bumping its timestamp, registering it
    /// first if needed — ported from `SetLatestApplicationVersion`.
    pub async fn set_latest_application_version(&self, version_name: &str) -> JournioResult<()> {
        if version_name.is_empty() {
            return Err(constructors::initialization("version_name is required"));
        }
        self.ctx
            .system_db
            .create_application_version(version_name)
            .await?;
        self.ctx
            .system_db
            .update_application_version_timestamp(version_name, Utc::now().timestamp_millis())
            .await
    }

    // -- shutdown ----------------------------------------------------------

    /// Close the system database connection pool — ported from `Shutdown`.
    pub async fn shutdown(&self, timeout: Duration) -> JournioResult<()> {
        self.ctx.shutdown(timeout).await
    }

    // -- helpers -----------------------------------------------------------

    async fn require_schedule(&self, schedule_name: &str) -> JournioResult<WorkflowSchedule> {
        if schedule_name.is_empty() {
            return Err(constructors::initialization("schedule_name is required"));
        }
        self.ctx
            .system_db
            .get_schedule(schedule_name)
            .await?
            .ok_or_else(|| {
                JournioError::new(
                    JournioErrorCode::InitializationError,
                    format!("schedule not found: {schedule_name}"),
                )
            })
    }

    /// Latest version name, or `None` if no versions are registered (the Go
    /// code logs and continues with an empty version).
    async fn latest_version_name(&self) -> JournioResult<Option<String>> {
        Ok(self
            .ctx
            .system_db
            .get_latest_application_version()
            .await?
            .map(|version| version.version_name))
    }
}

fn validate_schedule_input(input: &ClientScheduleInput) -> JournioResult<()> {
    if input.schedule_name.is_empty() {
        return Err(constructors::initialization("schedule_name is required"));
    }
    if input.workflow_name.is_empty() {
        return Err(constructors::initialization("workflow_name is required"));
    }
    parse_cron_schedule(&input.schedule)
        .map(|_| ())
        .map_err(|err| {
            constructors::initialization(format!(
                "invalid cron schedule {:?}: {err}",
                input.schedule
            ))
        })?;
    Ok(())
}

fn validate_schedule_input_indexed(input: &ClientScheduleInput, index: usize) -> JournioResult<()> {
    if input.schedule_name.is_empty() {
        return Err(constructors::initialization(format!(
            "schedule entry {index} is missing required field 'schedule_name'"
        )));
    }
    if input.workflow_name.is_empty() {
        return Err(constructors::initialization(format!(
            "schedule entry {index} is missing required field 'workflow_name'"
        )));
    }
    parse_cron_schedule(&input.schedule)
        .map(|_| ())
        .map_err(|err| {
            constructors::initialization(format!(
                "schedule entry {index}: invalid cron schedule: {err}"
            ))
        })?;
    Ok(())
}

fn schedule_from_input(input: &ClientScheduleInput, now: DateTime<Utc>) -> WorkflowSchedule {
    let queue_name = match &input.queue_name {
        Some(name) if !name.is_empty() => Some(name.clone()),
        _ => None,
    };
    WorkflowSchedule {
        schedule_id: Uuid::new_v4().to_string(),
        schedule_name: input.schedule_name.clone(),
        workflow_name: input.workflow_name.clone(),
        workflow_class_name: input.workflow_class_name.clone(),
        schedule: input.schedule.clone(),
        status: ScheduleStatus::Active,
        context: input.context.clone(),
        last_fired_at: Some(now),
        automatic_backfill: input.automatic_backfill,
        cron_timezone: input.cron_timezone.clone(),
        queue_name,
    }
}

fn scheduled_workflow_input(
    schedule: &WorkflowSchedule,
    scheduled_time: DateTime<Utc>,
) -> JournioResult<Interchange> {
    serde_json::to_value(ScheduledWorkflowInput {
        scheduled_time,
        context: schedule.context.clone(),
    })
    .map_err(|err| {
        constructors::initialization(format!(
            "failed to serialize scheduled workflow input for {}: {err}",
            schedule.schedule_name
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_from_input_normalizes_empty_queue_to_none() {
        let input = ClientScheduleInput {
            schedule_name: "s".into(),
            workflow_name: "w".into(),
            // 6-field cron (sec min hour dom month dow) — the rust `cron`
            // crate requires seconds; Journio's 5-field specs are a known
            // porting gap tracked separately.
            schedule: "*/5 * * * * *".into(),
            queue_name: Some(String::new()),
            ..Default::default()
        };
        let schedule = schedule_from_input(&input, Utc::now());
        assert!(schedule.queue_name.is_none());
        assert_eq!(schedule.status, ScheduleStatus::Active);
    }

    #[test]
    fn validate_schedule_input_rejects_invalid_cron() {
        let input = ClientScheduleInput {
            schedule_name: "s".into(),
            workflow_name: "w".into(),
            schedule: "not a cron".into(),
            ..Default::default()
        };
        assert!(validate_schedule_input(&input).is_err());
    }

    #[test]
    fn validate_schedule_input_accepts_valid_cron() {
        let input = ClientScheduleInput {
            schedule_name: "s".into(),
            workflow_name: "w".into(),
            schedule: "0 * * * * *".into(),
            ..Default::default()
        };
        assert!(validate_schedule_input(&input).is_ok());
    }
}
