//! `SystemDatabase` impl for Postgres/CockroachDB — ported from `sysDB` in
//! `journio/system_database.go` (lifecycle + the first five target methods).
//!
//! All queries are rendered with the canonical Postgres syntax that the Go
//! code uses (`$N` placeholders, schema prefix), matching `renderSQL` +
//! `dialect.SchemaPrefix` exactly. CockroachDB wire compatibility means the
//! same queries run there too.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use journio_core::dialect::Dialect;
use journio_core::error::{JournioError, JournioErrorCode, JournioResult};
use journio_core::system_db::{ForkWorkflow, InitWorkflow, InitWorkflowResult, SystemDatabase};
use journio_core::types::{
    ListWorkflowsFilter, QueueConfig, ScheduleStatus, StepRecord, StreamEntry, VersionInfo,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
use journio_core::value::Interchange;
use deadpool_postgres::{Config, ManagerConfig, Pool, PoolConfig, RecyclingMethod, Runtime};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_postgres::{AsyncMessage, NoTls};
use tokio_util::sync::CancellationToken;

use crate::dialect::PostgresDialect;
use crate::error::{is_foreign_key_violation, is_unique_violation, sqlstate};
use crate::migrations::run_migrations;

/// Topic stored when a `Send`/`Recv` omits the topic — ported from
/// `_JOURNIO_NULL_TOPIC` (`system_database.go:3333`).
const NULL_TOPIC: &str = "__null__topic__";
const NOTIFICATIONS_CHANNEL: &str = "journio_notifications_channel";
const WORKFLOW_EVENTS_CHANNEL: &str = "journio_workflow_events_channel";
const STREAMS_CHANNEL: &str = "journio_streams_channel";
const STREAM_CLOSED_SENTINEL: &str = "__JOURNIO_STREAM_CLOSED__";
const INTERNAL_QUEUE_NAME: &str = "_journio_internal_queue";

/// Serialization name persisted alongside every notification/event row — the
/// Go default is `JOURNIO_JSON` (`serialization.go:51`).
const SERIALIZATION_JSON: &str = "JOURNIO_JSON";

/// Postgres-backed system database. Owns a deadpool connection pool.
///
/// Construct with [`PostgresSystemDatabase::connect`], passing either a
/// `postgres://` URL or the URL handed back by `pglite_oxide::PgliteServer`
/// (same wire protocol — that's why one backend covers dev, test, and prod).
pub struct PostgresSystemDatabase {
    pool: Pool,
    connect_config: tokio_postgres::Config,
    schema: String,
    dialect: PostgresDialect,
    listener_cancel: CancellationToken,
    listener_started: AtomicBool,
    notification_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
    event_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
    stream_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
}

impl PostgresSystemDatabase {
    /// Connect + configure the pool. `database_url` is any libpq URL.
    pub fn connect(database_url: &str, schema: &str) -> Result<Self, JournioError> {
        let mut cfg = Config::new();
        cfg.url = Some(database_url.to_string());
        cfg.options = Some(format!("-c search_path={schema},public"));
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let mut pool_cfg = cfg.pool.unwrap_or_else(PoolConfig::default);
        pool_cfg.max_size = 8;
        cfg.pool = Some(pool_cfg);
        let connect_config = cfg.get_pg_config().map_err(init_err)?;
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(init_err)?;
        Ok(Self {
            pool,
            connect_config,
            schema: schema.to_string(),
            dialect: PostgresDialect,
            listener_cancel: CancellationToken::new(),
            listener_started: AtomicBool::new(false),
            notification_waiters: std::sync::Arc::new(Mutex::new(HashMap::new())),
            event_waiters: std::sync::Arc::new(Mutex::new(HashMap::new())),
            stream_waiters: std::sync::Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    fn q(&self, body: &str) -> String {
        body.to_string()
    }

    fn spawn_listener(&self) {
        let connect_config = self.connect_config.clone();
        let cancel = self.listener_cancel.clone();
        let notifications = self.notification_waiters.clone();
        let events = self.event_waiters.clone();
        let streams = self.stream_waiters.clone();

        tokio::spawn(async move {
            run_listener_loop(connect_config, cancel, notifications, events, streams).await;
        });
    }

    async fn wait_on_map(
        &self,
        waiters: &std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        let notify = {
            let mut map = waiters.lock().expect("waiters lock");
            map.entry(key.to_string())
                .or_insert_with(|| std::sync::Arc::new(Notify::new()))
                .clone()
        };

        tokio::select! {
            _ = notify.notified() => {}
            _ = tokio::time::sleep(timeout) => {}
            _ = self.listener_cancel.cancelled() => {}
        }
        Ok(())
    }
}

async fn run_listener_loop(
    connect_config: tokio_postgres::Config,
    cancel: CancellationToken,
    notification_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
    event_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
    stream_waiters: std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }

        let connect = connect_config.connect(NoTls).await;
        let (client, mut connection) = match connect {
            Ok(parts) => parts,
            Err(err) => {
                tracing::warn!(error = %err, "failed to connect postgres listener");
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
        };

        if let Err(err) = client.batch_execute(&format!("LISTEN {NOTIFICATIONS_CHANNEL}; LISTEN {WORKFLOW_EVENTS_CHANNEL}; LISTEN {STREAMS_CHANNEL};")).await {
            tracing::warn!(error = %err, "failed to register LISTEN channels");
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                message = std::future::poll_fn(|cx| connection.poll_message(cx)) => {
                    match message {
                        Some(Ok(AsyncMessage::Notification(notification))) => {
                            let payload = notification.payload().to_string();
                            match notification.channel() {
                                NOTIFICATIONS_CHANNEL => notify_waiters(&notification_waiters, &payload),
                                WORKFLOW_EVENTS_CHANNEL => notify_waiters(&event_waiters, &payload),
                                STREAMS_CHANNEL => notify_waiters(&stream_waiters, &payload),
                                _ => {}
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(err)) => {
                            tracing::warn!(error = %err, "postgres LISTEN/NOTIFY receive failed");
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    }
}

fn notify_waiters(
    waiters: &std::sync::Arc<Mutex<HashMap<String, std::sync::Arc<Notify>>>>,
    key: &str,
) {
    let notify = waiters
        .lock()
        .expect("waiters lock")
        .get(key)
        .cloned();
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
}

#[async_trait]
impl SystemDatabase for PostgresSystemDatabase {
    fn dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    async fn migrate(&self) -> JournioResult<()> {
        run_migrations(&self.pool, &self.schema).await
    }

    async fn launch(&self) -> JournioResult<()> {
        if self
            .listener_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.spawn_listener();
        }
        Ok(())
    }

    async fn shutdown(&self) -> JournioResult<()> {
        self.listener_cancel.cancel();
        Ok(())
    }

    // ------------------------------------------------------------------
    // workflow_status
    // ------------------------------------------------------------------

    /// Ported from `insertWorkflowStatus` (`system_database.go:936`).
    async fn init_workflow(&self, init: InitWorkflow) -> JournioResult<InitWorkflowResult> {
        let attempts: i64 = match init.status {
            WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed => 0,
            _ => 1,
        };
        let now_ms = Utc::now().timestamp_millis();
        let created_ms = now_ms;
        let timeout_ms: Option<i64> = init.timeout.map(|d| d.as_millis() as i64);
        let deadline_ms: Option<i64> = init.deadline.map(|t| t.timestamp_millis());
        let delay_until_ms: Option<i64> = init.delay_until.map(|t| t.timestamp_millis());
        let inputs_str: Option<String> = init
            .input
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default());
        let roles_json =
            serde_json::to_string(&init.authenticated_roles).unwrap_or_else(|_| "null".into());
        let recovery_increment: i64 = i64::from(i32::from(init.increment_attempts));

        let query = self.q(
            "INSERT INTO workflow_status (
                workflow_uuid, status, name, queue_name, authenticated_user,
                assumed_role, authenticated_roles, executor_id, application_version,
                application_id, created_at, recovery_attempts, updated_at,
                workflow_timeout_ms, workflow_deadline_epoch_ms, inputs,
                deduplication_id, priority, queue_partition_key, owner_xid,
                parent_workflow_id, class_name, config_name, serialization,
                delay_until_epoch_ms
            ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
            ON CONFLICT (workflow_uuid)
                DO UPDATE SET
                    recovery_attempts = CASE
                        WHEN EXCLUDED.status NOT IN ($26, $27) THEN workflow_status.recovery_attempts + $28
                        ELSE workflow_status.recovery_attempts
                    END,
                    updated_at = EXCLUDED.updated_at,
                    executor_id = CASE
                        WHEN EXCLUDED.status IN ($26, $27) THEN workflow_status.executor_id
                        ELSE EXCLUDED.executor_id
                    END
                RETURNING recovery_attempts, status, name, queue_name, queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid",
        );

        let status_str = status_to_str(init.status);
        let enqueued_str = status_to_str(WorkflowStatusType::Enqueued);
        let delayed_str = status_to_str(WorkflowStatusType::Delayed);

        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client
            .query_one(
                &query,
                &[
                    &init.workflow_id, &status_str, &init.name, &init.queue_name,
                    &init.authenticated_user, &init.assumed_role, &roles_json, &init.executor_id,
                    &init.application_version, &init.application_id, &created_ms, &attempts,
                    &now_ms, &timeout_ms, &deadline_ms, &inputs_str, &init.deduplication_id,
                    &init.priority, &init.queue_partition_key, &None::<String>,
                    &init.parent_workflow_id, &init.class_name, &init.config_name,
                    &init.serialization, &delay_until_ms, &enqueued_str, &delayed_str,
                    &recovery_increment,
                ],
            )
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    JournioError {
                        code: JournioErrorCode::QueueDeduplicated,
                        message: format!(
                            "Workflow {} was deduplicated due to an existing workflow in queue {:?} with deduplication ID {:?}",
                            init.workflow_id, init.queue_name, init.deduplication_id
                        ),
                        workflow_id: Some(init.workflow_id.clone()),
                        queue_name: init.queue_name.clone(),
                        deduplication_id: init.deduplication_id.clone(),
                        source: Some(Box::new(e)),
                        ..Default::default()
                    }
                } else {
                    db_err(e)
                }
            })?;

        let attempts_out: i64 = row.get(0);
        let status_out_str: String = row.get(1);
        let name_out: String = row.get(2);
        let queue_name_out: Option<String> = row.get(3);
        let queue_partition_out: Option<String> = row.get(4);
        let timeout_out: Option<i64> = row.get(5);
        let deadline_out: Option<i64> = row.get(6);

        if !init.name.is_empty() && name_out != init.name {
            return Err(JournioError {
                code: JournioErrorCode::ConflictingWorkflowError,
                message: format!(
                    "Conflicting workflow invocation with the same ID ({}): Workflow already exists with a different name: {}, but the provided name is: {}",
                    init.workflow_id, name_out, init.name
                ),
                workflow_id: Some(init.workflow_id),
                ..Default::default()
            });
        }
        if let Some(qn) = &init.queue_name {
            if let Some(existing) = &queue_name_out {
                if qn != existing {
                    return Err(JournioError {
                        code: JournioErrorCode::ConflictingWorkflowError,
                        message: format!(
                            "Conflicting workflow invocation with the same ID ({}): Workflow already exists in a different queue: {}, but the provided queue is: {}",
                            init.workflow_id, existing, qn
                        ),
                        workflow_id: Some(init.workflow_id),
                        ..Default::default()
                    });
                }
            }
        }

        let status_out = parse_status(&status_out_str);
        if !matches!(
            status_out,
            WorkflowStatusType::Success | WorkflowStatusType::Error
        ) && init.max_retries > 0
            && attempts_out > i64::from(init.max_retries) + 1
        {
            let dlq = self.q(
                "UPDATE workflow_status SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL WHERE workflow_uuid = $2 AND status = $3",
            );
            client
                .execute(
                    &dlq,
                    &[
                        &status_to_str(WorkflowStatusType::MaxRecoveryAttemptsExceeded),
                        &init.workflow_id,
                        &status_str,
                    ],
                )
                .await
                .map_err(db_err)?;
            return Err(JournioError {
                code: JournioErrorCode::DeadLetterQueueError,
                message: format!(
                    "Workflow {} has been moved to the dead-letter queue after exceeding the maximum of {} retries",
                    init.workflow_id, init.max_retries
                ),
                workflow_id: Some(init.workflow_id),
                ..Default::default()
            });
        }

        Ok(InitWorkflowResult {
            status: status_out,
            attempts: attempts_out,
            name: name_out,
            queue_name: queue_name_out,
            queue_partition_key: queue_partition_out,
            timeout: timeout_out
                .filter(|&m| m > 0)
                .map(|m| Duration::from_millis(m as u64)),
            deadline: deadline_out.map(timestamp_ms),
        })
    }

    /// Ported from `updateWorkflowOutcome` (`system_database.go:1480`).
    async fn record_workflow_result(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: Option<&Interchange>,
        error: Option<&str>,
    ) -> JournioResult<()> {
        let output_str: Option<String> =
            output.map(|v| serde_json::to_string(v).unwrap_or_default());
        let now_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "UPDATE workflow_status \
             SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL \
             WHERE workflow_uuid = $5 AND NOT (status = $6 AND CAST($1 AS TEXT) IN ($7, $8))",
        );
        let status_str = status_to_str(status);
        let cancelled_str = status_to_str(WorkflowStatusType::Cancelled);
        let success_str = status_to_str(WorkflowStatusType::Success);
        let error_str = status_to_str(WorkflowStatusType::Error);
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[
                    &status_str,
                    &output_str,
                    &error,
                    &now_ms,
                    &workflow_id,
                    &cancelled_str,
                    &success_str,
                    &error_str,
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get_workflow_status(&self, workflow_id: &str) -> JournioResult<Option<WorkflowStatus>> {
        let query = self.q(WORKFLOW_STATUS_BY_ID);
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client
            .query_opt(&query, &[&workflow_id])
            .await
            .map_err(db_err)?;
        Ok(row.map(|r| row_to_workflow_status(&r)))
    }

    async fn list_workflows(&self, limit: i64) -> JournioResult<Vec<WorkflowStatus>> {
        let query = format!(
            "{} ORDER BY created_at DESC LIMIT $1",
            self.q(WORKFLOW_STATUS_SELECT)
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client.query(&query, &[&limit]).await.map_err(db_err)?;
        Ok(rows.iter().map(row_to_workflow_status).collect())
    }

    async fn cancel_workflows(&self, workflow_ids: &[String]) -> JournioResult<Vec<String>> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "WITH existing AS (
                 SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ANY($3)
             ), updated AS (
                 UPDATE workflow_status
                 SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
                     queue_name = NULL, deduplication_id = NULL
                 WHERE workflow_uuid = ANY($3) AND status NOT IN ($4, $5, $6)
                 RETURNING workflow_uuid
             )
             SELECT workflow_uuid FROM existing",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(
                &query,
                &[
                    &status_to_str(WorkflowStatusType::Cancelled),
                    &now_ms,
                    &workflow_ids,
                    &status_to_str(WorkflowStatusType::Success),
                    &status_to_str(WorkflowStatusType::Error),
                    &status_to_str(WorkflowStatusType::Cancelled),
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> JournioResult<Vec<String>> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = Utc::now().timestamp_millis();
        let queue_name = queue_name.unwrap_or(INTERNAL_QUEUE_NAME).to_string();
        let query = self.q(
            "WITH existing AS (
                 SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ANY($5)
             ), updated AS (
                 UPDATE workflow_status
                 SET status = $1, queue_name = $2, recovery_attempts = $3,
                     workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
                     started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
                 WHERE workflow_uuid = ANY($5) AND status NOT IN ($6, $7)
                 RETURNING workflow_uuid
             )
             SELECT workflow_uuid FROM existing",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(
                &query,
                &[
                    &status_to_str(WorkflowStatusType::Enqueued),
                    &queue_name,
                    &0_i64,
                    &now_ms,
                    &workflow_ids,
                    &status_to_str(WorkflowStatusType::Success),
                    &status_to_str(WorkflowStatusType::Error),
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn get_workflow_children(&self, workflow_id: &str) -> JournioResult<Vec<WorkflowStatus>> {
        let query = self.q(
            "WITH RECURSIVE descendants AS (
                 SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
                        output, error, executor_id, created_at, updated_at, application_version, application_id,
                        recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms,
                        started_at_epoch_ms, deduplication_id, inputs, priority, queue_partition_key,
                        forked_from, was_forked_from, parent_workflow_id, completed_at, class_name, config_name,
                        serialization, delay_until_epoch_ms
                 FROM workflow_status
                 WHERE parent_workflow_id = $1
                 UNION ALL
                 SELECT ws.workflow_uuid, ws.status, ws.name, ws.authenticated_user, ws.assumed_role, ws.authenticated_roles,
                        ws.output, ws.error, ws.executor_id, ws.created_at, ws.updated_at, ws.application_version, ws.application_id,
                        ws.recovery_attempts, ws.queue_name, ws.workflow_timeout_ms, ws.workflow_deadline_epoch_ms,
                        ws.started_at_epoch_ms, ws.deduplication_id, ws.inputs, ws.priority, ws.queue_partition_key,
                        ws.forked_from, ws.was_forked_from, ws.parent_workflow_id, ws.completed_at, ws.class_name, ws.config_name,
                        ws.serialization, ws.delay_until_epoch_ms
                 FROM workflow_status ws
                 INNER JOIN descendants d ON ws.parent_workflow_id = d.workflow_uuid
             )
             SELECT * FROM descendants
             ORDER BY created_at ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client.query(&query, &[&workflow_id]).await.map_err(db_err)?;
        Ok(rows.iter().map(row_to_workflow_status).collect())
    }

    async fn fork_workflow(&self, input: ForkWorkflow) -> JournioResult<String> {
        if input.start_step < 0 {
            return Err(JournioError::new(
                JournioErrorCode::InitializationError,
                format!("startStep must be >= 0, got {}", input.start_step),
            ));
        }

        let forked_workflow_id = input
            .forked_workflow_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let queue_name = input
            .queue_name
            .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string());
        let now_ms = Utc::now().timestamp_millis();

        let mut client = self.pool.get().await.map_err(pool_err)?;
        let tx = client.transaction().await.map_err(db_err)?;
        let original_row = tx
            .query_opt(&self.q(WORKFLOW_STATUS_BY_ID), &[&input.original_workflow_id])
            .await
            .map_err(db_err)?;
        let Some(original_row) = original_row else {
            return Err(JournioError::new(
                JournioErrorCode::NonExistentWorkflowError,
                format!("workflow {} does not exist", input.original_workflow_id),
            ));
        };
        let original = row_to_workflow_status(&original_row);
        let authenticated_roles =
            serde_json::to_string(&original.authenticated_roles).unwrap_or_else(|_| "null".into());
        let application_version = input
            .application_version
            .unwrap_or_else(|| original.application_version.clone());
        let inputs = original
            .input
            .as_ref()
            .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "null".into()));

        tx.execute(
            &self.q(
                "INSERT INTO workflow_status (
                     workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
                     executor_id, application_version, application_id, queue_name, queue_partition_key,
                     inputs, created_at, updated_at, recovery_attempts, forked_from, serialization,
                     class_name, config_name
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)",
            ),
            &[
                &forked_workflow_id,
                &status_to_str(WorkflowStatusType::Enqueued),
                &original.name,
                &original.authenticated_user,
                &original.assumed_role,
                &authenticated_roles,
                &original.executor_id,
                &application_version,
                &original.application_id,
                &queue_name,
                &input.queue_partition_key,
                &inputs,
                &now_ms,
                &now_ms,
                &0_i64,
                &input.original_workflow_id,
                &original.serialization,
                &original.class_name,
                &original.config_name,
            ],
        )
        .await
        .map_err(db_err)?;

        tx.execute(
            &self.q("UPDATE workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = $1"),
            &[&input.original_workflow_id],
        )
        .await
        .map_err(db_err)?;

        if input.start_step > 0 {
            tx.execute(
                &self.q(
                    "INSERT INTO operation_outputs
                         (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
                     SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
                     FROM operation_outputs
                     WHERE workflow_uuid = $2 AND function_id < $3",
                ),
                &[&forked_workflow_id, &input.original_workflow_id, &input.start_step],
            )
            .await
            .map_err(db_err)?;

            tx.execute(
                &self.q(
                    "INSERT INTO workflow_events_history
                         (workflow_uuid, function_id, key, value, serialization)
                     SELECT $1, function_id, key, value, serialization
                     FROM workflow_events_history
                     WHERE workflow_uuid = $2 AND function_id < $3",
                ),
                &[&forked_workflow_id, &input.original_workflow_id, &input.start_step],
            )
            .await
            .map_err(db_err)?;

            tx.execute(
                &self.q(
                    "INSERT INTO workflow_events (workflow_uuid, key, value, serialization)
                     SELECT $1, h.key, h.value, h.serialization
                     FROM workflow_events_history h
                     INNER JOIN (
                         SELECT key, MAX(function_id) AS max_fid
                         FROM workflow_events_history
                         WHERE workflow_uuid = $2 AND function_id < $3
                         GROUP BY key
                     ) latest ON h.key = latest.key AND h.function_id = latest.max_fid
                     WHERE h.workflow_uuid = $2 AND h.function_id < $3",
                ),
                &[&forked_workflow_id, &input.original_workflow_id, &input.start_step],
            )
            .await
            .map_err(db_err)?;

            tx.execute(
                &self.q(
                    "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
                     SELECT $1, key, value, \"offset\", function_id, serialization
                     FROM streams
                     WHERE workflow_uuid = $2 AND function_id < $3",
                ),
                &[&forked_workflow_id, &input.original_workflow_id, &input.start_step],
            )
            .await
            .map_err(db_err)?;
        }

        tx.commit().await.map_err(db_err)?;
        Ok(forked_workflow_id)
    }

    async fn get_queue(&self, queue_name: &str) -> JournioResult<Option<QueueConfig>> {
        let query = self.q(
            "SELECT queue_id, name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
                    priority_enabled, partition_queue, polling_interval_sec
             FROM queues WHERE name = $1",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client.query_opt(&query, &[&queue_name]).await.map_err(db_err)?;
        Ok(row.as_ref().map(row_to_queue_config))
    }

    async fn upsert_queue(&self, queue: &QueueConfig) -> JournioResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "INSERT INTO queues (
                 queue_id, name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
                 priority_enabled, partition_queue, polling_interval_sec, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (name)
             DO UPDATE SET
                 concurrency = excluded.concurrency,
                 worker_concurrency = excluded.worker_concurrency,
                 rate_limit_max = excluded.rate_limit_max,
                 rate_limit_period_sec = excluded.rate_limit_period_sec,
                 priority_enabled = excluded.priority_enabled,
                 partition_queue = excluded.partition_queue,
                 polling_interval_sec = excluded.polling_interval_sec,
                 updated_at = excluded.updated_at",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[
                    &queue.queue_id,
                    &queue.name,
                    &queue.concurrency,
                    &queue.worker_concurrency,
                    &queue.rate_limit_max,
                    &queue.rate_limit_period_sec,
                    &queue.priority_enabled,
                    &queue.partition_queue,
                    &queue.polling_interval_sec,
                    &now_ms,
                    &now_ms,
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // operation_outputs
    // ------------------------------------------------------------------

    /// Ported from `recordOperationResult` (`system_database.go:2215`).
    async fn record_step_output(&self, step: &StepRecord) -> JournioResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let query = self.q("INSERT INTO operation_outputs \
                (workflow_uuid, function_id, function_name, output, error, child_workflow_id, \
                 started_at_epoch_ms, completed_at_epoch_ms, serialization) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)");
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[
                    &step.workflow_uuid,
                    &step.function_id,
                    &step.function_name,
                    &step.output,
                    &step.error,
                    &step.child_workflow_id,
                    &now_ms,
                    &now_ms,
                    &"JOURNIO_JSON",
                ],
            )
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    JournioError {
                        code: JournioErrorCode::ConflictingIDError,
                        message: format!("Conflicting workflow ID {}", step.workflow_uuid),
                        workflow_id: Some(step.workflow_uuid.clone()),
                        source: Some(Box::new(e)),
                        ..Default::default()
                    }
                } else {
                    db_err(e)
                }
            })?;
        Ok(())
    }

    /// Ported from `getWorkflowSteps` (`system_database.go:2446`).
    async fn get_steps(&self, workflow_id: &str) -> JournioResult<Vec<StepRecord>> {
        let query = self.q(
            "SELECT workflow_uuid, function_id, function_name, output, error, child_workflow_id \
             FROM operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(&query, &[&workflow_id])
            .await
            .map_err(db_err)?;
        Ok(rows
            .iter()
            .map(|r| StepRecord {
                workflow_uuid: r.get(0),
                function_id: r.get(1),
                function_name: r.get(2),
                output: r.get(3),
                error: r.get(4),
                child_workflow_id: r.get(5),
            })
            .collect())
    }

    async fn dequeue_workflow(
        &self,
        queue_name: &str,
        executor_id: &str,
    ) -> JournioResult<Option<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        let queue = self.get_queue(queue_name).await?;
        let candidates_query = self.q(
            "SELECT workflow_uuid, queue_partition_key
             FROM workflow_status
             WHERE queue_name = $1
               AND status IN ($2, $3)
               AND (
                 status = $2
                 OR delay_until_epoch_ms IS NULL
                 OR delay_until_epoch_ms <= $4
               )
             ORDER BY priority ASC, created_at ASC
             FOR UPDATE SKIP LOCKED",
        );
        let mut client = self.pool.get().await.map_err(pool_err)?;
        let tx = client.transaction().await.map_err(db_err)?;
        let candidates = tx
            .query(
                &candidates_query,
                &[
                    &queue_name,
                    &status_to_str(WorkflowStatusType::Enqueued),
                    &status_to_str(WorkflowStatusType::Delayed),
                    &now_ms,
                ],
            )
            .await
            .map_err(db_err)?;

        let mut selected_id = None;
        for candidate in candidates {
            let workflow_id: String = candidate.get(0);
            let partition_key: Option<String> = candidate.get(1);

            if queue
                .as_ref()
                .is_some_and(|cfg| cfg.partition_queue && partition_key.is_none())
            {
                continue;
            }

            if let Some(cfg) = queue.as_ref() {
                if let Some(limit) = cfg.concurrency {
                    let query = if cfg.partition_queue {
                        self.q(
                            "SELECT COUNT(*) FROM workflow_status
                             WHERE queue_name = $1 AND status = $2 AND queue_partition_key = $3",
                        )
                    } else {
                        self.q(
                            "SELECT COUNT(*) FROM workflow_status
                             WHERE queue_name = $1 AND status = $2",
                        )
                    };
                    let count: i64 = if cfg.partition_queue {
                        tx.query_one(
                            &query,
                            &[&queue_name, &status_to_str(WorkflowStatusType::Pending), &partition_key],
                        )
                        .await
                        .map_err(db_err)?
                        .get(0)
                    } else {
                        tx.query_one(
                            &query,
                            &[&queue_name, &status_to_str(WorkflowStatusType::Pending)],
                        )
                        .await
                        .map_err(db_err)?
                        .get(0)
                    };
                    if count >= i64::from(limit) {
                        continue;
                    }
                }

                if let (Some(limit), Some(period_sec)) = (cfg.rate_limit_max, cfg.rate_limit_period_sec) {
                    let cutoff_ms = now_ms - (period_sec * 1000.0) as i64;
                    let query = if cfg.partition_queue {
                        self.q(
                            "SELECT COUNT(*) FROM workflow_status
                             WHERE queue_name = $1
                               AND rate_limited = TRUE
                               AND status NOT IN ($2, $3)
                               AND started_at_epoch_ms > $4
                               AND queue_partition_key = $5",
                        )
                    } else {
                        self.q(
                            "SELECT COUNT(*) FROM workflow_status
                             WHERE queue_name = $1
                               AND rate_limited = TRUE
                               AND status NOT IN ($2, $3)
                               AND started_at_epoch_ms > $4",
                        )
                    };
                    let count: i64 = if cfg.partition_queue {
                        tx.query_one(
                            &query,
                            &[
                                &queue_name,
                                &status_to_str(WorkflowStatusType::Enqueued),
                                &status_to_str(WorkflowStatusType::Delayed),
                                &cutoff_ms,
                                &partition_key,
                            ],
                        )
                        .await
                        .map_err(db_err)?
                        .get(0)
                    } else {
                        tx.query_one(
                            &query,
                            &[
                                &queue_name,
                                &status_to_str(WorkflowStatusType::Enqueued),
                                &status_to_str(WorkflowStatusType::Delayed),
                                &cutoff_ms,
                            ],
                        )
                        .await
                        .map_err(db_err)?
                        .get(0)
                    };
                    if count >= i64::from(limit) {
                        continue;
                    }
                }
            }

            selected_id = Some(workflow_id);
            break;
        }

        let Some(selected_id) = selected_id else {
            tx.commit().await.map_err(db_err)?;
            return Ok(None);
        };

        let update_query = self.q(
            "UPDATE workflow_status
             SET status = $1,
                 executor_id = $2,
                 started_at_epoch_ms = $3,
                 rate_limited = $4,
                 workflow_deadline_epoch_ms = CASE
                     WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                     THEN $3 + workflow_timeout_ms
                     ELSE workflow_deadline_epoch_ms
                 END
             WHERE workflow_uuid = $5
               AND status IN ($6, $7)
             RETURNING workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
                       output, error, executor_id, created_at, updated_at, application_version, application_id,
                       recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms,
                       started_at_epoch_ms, deduplication_id, inputs, priority, queue_partition_key,
                       forked_from, was_forked_from, parent_workflow_id, completed_at, class_name, config_name,
                       serialization, delay_until_epoch_ms",
        );
        let row = tx
            .query_opt(
                &update_query,
                &[
                    &status_to_str(WorkflowStatusType::Pending),
                    &executor_id,
                    &now_ms,
                    &queue.as_ref().is_some_and(|cfg| cfg.rate_limit_max.is_some()),
                    &selected_id,
                    &status_to_str(WorkflowStatusType::Enqueued),
                    &status_to_str(WorkflowStatusType::Delayed),
                ],
            )
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(row.as_ref().map(row_to_workflow_status))
    }

    async fn list_runnable_queues(&self) -> JournioResult<Vec<String>> {
        let query = self.q(
            "SELECT DISTINCT queue_name
             FROM workflow_status
             WHERE queue_name IS NOT NULL
               AND status IN ($1, $2)
             ORDER BY queue_name ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(
                &query,
                &[
                    &status_to_str(WorkflowStatusType::Enqueued),
                    &status_to_str(WorkflowStatusType::Delayed),
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.get::<_, Option<String>>(0))
            .collect())
    }

    async fn list_queues(&self) -> JournioResult<Vec<QueueConfig>> {
        let query = self.q(
            "SELECT queue_id, name, concurrency, worker_concurrency, rate_limit_max,
                    rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec
             FROM queues
             ORDER BY name ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client.query(&query, &[]).await.map_err(db_err)?;
        Ok(rows.iter().map(row_to_queue_config).collect())
    }

    // ------------------------------------------------------------------
    // notifications (Send / Recv) — ported from `system_database.go:3346`+
    // ------------------------------------------------------------------

    /// Ported from `send` (`system_database.go:3346`).
    async fn send(
        &self,
        destination_id: &str,
        topic: &str,
        message: &Interchange,
    ) -> JournioResult<()> {
        let topic = if topic.is_empty() { NULL_TOPIC } else { topic };
        let message_str = serde_json::to_string(message).unwrap_or_else(|_| "null".into());
        let message_uuid = uuid::Uuid::new_v4().to_string();
        let created_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "INSERT INTO notifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[
                    &destination_id,
                    &topic,
                    &message_str,
                    &SERIALIZATION_JSON,
                    &message_uuid,
                    &created_ms,
                ],
            )
            .await
            .map_err(|e| {
                if is_foreign_key_violation(&e) {
                    JournioError {
                        code: JournioErrorCode::NonExistentWorkflowError,
                        message: format!("destination workflow {destination_id} does not exist"),
                        workflow_id: Some(destination_id.to_string()),
                        source: Some(Box::new(e)),
                        ..Default::default()
                    }
                } else {
                    db_err(e)
                }
            })?;
        // Wake same-process waiters immediately; LISTEN/NOTIFY still covers
        // cross-process wakeups and missed local registrations.
        notify_waiters(
            &self.notification_waiters,
            &format!("{destination_id}::{topic}"),
        );
        Ok(())
    }

    /// Ported from the consume half of `recv` (`system_database.go:3378`).
    /// Atomically marks the oldest unconsumed message `consumed = true` and
    /// returns it. Non-blocking.
    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
    ) -> JournioResult<Option<journio_core::Notification>> {
        let topic = if topic.is_empty() { NULL_TOPIC } else { topic };
        let query = self.q("WITH oldest_entry AS ( \
                SELECT message_uuid FROM notifications \
                WHERE destination_uuid = $1 AND topic = $2 AND consumed = false \
                ORDER BY created_at_epoch_ms ASC LIMIT 1) \
             UPDATE notifications SET consumed = true \
             WHERE message_uuid = (SELECT message_uuid FROM oldest_entry) \
             RETURNING message, serialization");
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client
            .query_opt(&query, &[&workflow_id, &topic])
            .await
            .map_err(db_err)?;
        let Some(row) = row else { return Ok(None) };
        let message_str: Option<String> = row.get(0);
        let serialization: Option<String> = row.get(1);
        let message = message_str
            .and_then(|s| serde_json::from_str::<Interchange>(&s).ok())
            .unwrap_or(Interchange::Null);
        Ok(Some(journio_core::Notification {
            message,
            serialization,
        }))
    }

    async fn wait_for_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        self.wait_on_map(
            &self.notification_waiters,
            &format!("{}::{}", workflow_id, if topic.is_empty() { NULL_TOPIC } else { topic }),
            timeout,
        )
        .await
    }

    // ------------------------------------------------------------------
    // workflow events (SetEvent / GetEvent) — `system_database.go:3573`+
    // ------------------------------------------------------------------

    /// Ported from `setEvent` (`system_database.go:3573`). Upserts
    /// `workflow_events` and appends `workflow_events_history` keyed by
    /// `function_id` (the calling step id).
    async fn set_event(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Interchange,
        function_id: i32,
    ) -> JournioResult<()> {
        let value_str = serde_json::to_string(value).unwrap_or_else(|_| "null".into());
        let mut client = self.pool.get().await.map_err(pool_err)?;
        let tx = client.transaction().await.map_err(db_err)?;

        tx.execute(
            &self.q(
                "INSERT INTO workflow_events (workflow_uuid, key, value, serialization) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (workflow_uuid, key) \
                 DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
            ),
            &[&workflow_id, &key, &value_str, &SERIALIZATION_JSON],
        )
        .await
        .map_err(db_err)?;

        tx.execute(
            &self.q(
                "INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value, serialization) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (workflow_uuid, function_id, key) \
                 DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
            ),
            &[&workflow_id, &function_id, &key, &value_str, &SERIALIZATION_JSON],
        )
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;
        notify_waiters(&self.event_waiters, &format!("{workflow_id}::{key}"));
        Ok(())
    }

    /// Ported from the query half of `getEvent` (`system_database.go:3615`).
    /// Non-blocking: returns the current value, or `None` if unset.
    async fn get_event_value(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> JournioResult<Option<Interchange>> {
        let query =
            self.q("SELECT value FROM workflow_events WHERE workflow_uuid = $1 AND key = $2");
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client
            .query_opt(&query, &[&workflow_id, &key])
            .await
            .map_err(db_err)?;
        let Some(row) = row else { return Ok(None) };
        let value_str: Option<String> = row.get(0);
        Ok(value_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok()))
    }

    async fn wait_for_event(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        self.wait_on_map(
            &self.event_waiters,
            &format!("{workflow_id}::{key}"),
            timeout,
        )
        .await
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i32,
        serialization: Option<&str>,
    ) -> JournioResult<()> {
        let check_closed = self.q(
            "SELECT 1 FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND value = $3
             LIMIT 1",
        );
        let insert = self.q(
            "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
             SELECT $1, $2, $3,
                    COALESCE((SELECT MAX(\"offset\") FROM streams WHERE workflow_uuid = $1 AND key = $2), -1) + 1,
                    $4, $5",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let exists = client
            .query_opt(&check_closed, &[&workflow_id, &key, &STREAM_CLOSED_SENTINEL])
            .await
            .map_err(db_err)?;
        if exists.is_some() {
            return Err(JournioError::new(
                JournioErrorCode::WorkflowExecutionError,
                format!("stream {key:?} is already closed"),
            ));
        }
        client
            .execute(
                &insert,
                &[&workflow_id, &key, &value, &function_id, &serialization],
            )
            .await
            .map_err(db_err)?;
        notify_waiters(&self.stream_waiters, &format!("{workflow_id}::{key}"));
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> JournioResult<(Vec<StreamEntry>, bool)> {
        let from_offset = i32::try_from(from_offset).map_err(|_| {
            JournioError::new(
                JournioErrorCode::InitializationError,
                format!("stream offset {from_offset} does not fit in postgres INTEGER"),
            )
        })?;
        let query = self.q(
            "SELECT value, \"offset\", serialization
             FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3
             ORDER BY \"offset\" ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(&query, &[&workflow_id, &key, &from_offset])
            .await
            .map_err(db_err)?;

        let mut entries = Vec::new();
        let mut closed = false;
        for row in rows {
            let value: String = row.get(0);
            let offset: i32 = row.get(1);
            let serialization: Option<String> = row.get(2);
            if value == STREAM_CLOSED_SENTINEL {
                closed = true;
                break;
            }
            entries.push(StreamEntry {
                value,
                offset: i64::from(offset),
                serialization,
            });
        }
        Ok((entries, closed))
    }

    async fn wait_for_stream(
        &self,
        workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> JournioResult<()> {
        self.wait_on_map(
            &self.stream_waiters,
            &format!("{workflow_id}::{key}"),
            timeout,
        )
        .await
    }

    async fn get_workflows_for_recovery(
        &self,
        executor_id: &str,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        let query = format!(
            "{} WHERE executor_id = $1 AND status IN ('PENDING', 'ENQUEUED', 'DELAYED') ORDER BY created_at ASC",
            self.q(WORKFLOW_STATUS_SELECT)
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(&query, &[&executor_id])
            .await
            .map_err(db_err)?;
        Ok(rows.iter().map(row_to_workflow_status).collect())
    }

    async fn delete_workflows_before(&self, before: DateTime<Utc>) -> JournioResult<u64> {
        let before_ms = before.timestamp_millis();
        let query = self.q(
            "DELETE FROM workflow_status WHERE status IN ('SUCCESS','ERROR','CANCELLED') AND completed_at < $1",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let n = client
            .execute(&query, &[&before_ms])
            .await
            .map_err(db_err)?;
        Ok(n)
    }

    async fn upsert_schedule(&self, schedule: &WorkflowSchedule) -> JournioResult<()> {
        let query = self.q(
            "INSERT INTO workflow_schedules
                 (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (schedule_name)
             DO UPDATE SET
                 schedule_id = excluded.schedule_id,
                 workflow_name = excluded.workflow_name,
                 workflow_class_name = excluded.workflow_class_name,
                 schedule = excluded.schedule,
                 status = excluded.status,
                 context = excluded.context,
                 last_fired_at = excluded.last_fired_at,
                 automatic_backfill = excluded.automatic_backfill,
                 cron_timezone = excluded.cron_timezone,
                 queue_name = excluded.queue_name",
        );
        let context_json = serde_json::to_string(&schedule.context).unwrap_or_else(|_| "null".into());
        let last_fired_at = schedule.last_fired_at.map(|value| value.to_rfc3339());
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[
                    &schedule.schedule_id,
                    &schedule.schedule_name,
                    &schedule.workflow_name,
                    &schedule.workflow_class_name,
                    &schedule.schedule,
                    &schedule_status_to_str(schedule.status),
                    &context_json,
                    &last_fired_at,
                    &schedule.automatic_backfill,
                    &schedule.cron_timezone,
                    &schedule.queue_name,
                ],
            )
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn list_schedules(&self) -> JournioResult<Vec<WorkflowSchedule>> {
        let query = self.q(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
             FROM workflow_schedules
             ORDER BY schedule_name ASC",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client.query(&query, &[]).await.map_err(db_err)?;
        Ok(rows.iter().map(row_to_workflow_schedule).collect())
    }

    async fn update_schedule_last_fired_at(
        &self,
        schedule_name: &str,
        fired_at: DateTime<Utc>,
    ) -> JournioResult<()> {
        let query = self.q(
            "UPDATE workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2",
        );
        let fired_at = fired_at.to_rfc3339();
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(&query, &[&fired_at, &schedule_name])
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get_schedule(
        &self,
        schedule_name: &str,
    ) -> JournioResult<Option<WorkflowSchedule>> {
        let query = self.q(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
             FROM workflow_schedules WHERE schedule_name = $1",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client
            .query_opt(&query, &[&schedule_name])
            .await
            .map_err(db_err)?;
        Ok(row.map(|r| row_to_workflow_schedule(&r)))
    }

    async fn delete_schedule(&self, schedule_name: &str) -> JournioResult<()> {
        let query = self.q("DELETE FROM workflow_schedules WHERE schedule_name = $1");
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(&query, &[&schedule_name])
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn update_schedule_status(
        &self,
        schedule_name: &str,
        status: ScheduleStatus,
    ) -> JournioResult<()> {
        let query =
            self.q("UPDATE workflow_schedules SET status = $1 WHERE schedule_name = $2");
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(
                &query,
                &[&schedule_status_to_str(status), &schedule_name],
            )
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn create_application_version(&self, version_name: &str) -> JournioResult<()> {
        let query = self.q(
            "INSERT INTO application_versions (version_id, version_name, version_timestamp, created_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (version_name) DO NOTHING",
        );
        let now_ms = Utc::now().timestamp_millis();
        let version_id = uuid::Uuid::new_v4().to_string();
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(&query, &[&version_id, &version_name, &now_ms, &now_ms])
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp_ms: i64,
    ) -> JournioResult<()> {
        let query = self.q(
            "UPDATE application_versions SET version_timestamp = $1 WHERE version_name = $2",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(&query, &[&timestamp_ms, &version_name])
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn list_application_versions(&self) -> JournioResult<Vec<VersionInfo>> {
        let query =
            self.q("SELECT version_id, version_name, version_timestamp, created_at FROM application_versions ORDER BY version_timestamp DESC");
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client.query(&query, &[]).await.map_err(db_err)?;
        Ok(rows
            .iter()
            .map(|r| VersionInfo {
                version_id: r.get(0),
                version_name: r.get(1),
                version_timestamp: r.get(2),
                created_at: r.get(3),
            })
            .collect())
    }

    async fn get_latest_application_version(&self) -> JournioResult<Option<VersionInfo>> {
        let query = self.q(
            "SELECT version_id, version_name, version_timestamp, created_at FROM application_versions ORDER BY version_timestamp DESC LIMIT 1",
        );
        let client = self.pool.get().await.map_err(pool_err)?;
        let row = client.query_opt(&query, &[]).await.map_err(db_err)?;
        Ok(row.map(|r| VersionInfo {
            version_id: r.get(0),
            version_name: r.get(1),
            version_timestamp: r.get(2),
            created_at: r.get(3),
        }))
    }

    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay_until: DateTime<Utc>,
    ) -> JournioResult<()> {
        let query = self.q(
            "UPDATE workflow_status SET delay_until_epoch_ms = $1, updated_at = $2 WHERE workflow_uuid = $3 AND status = $4",
        );
        let now_ms = Utc::now().timestamp_millis();
        let delay_ms = delay_until.timestamp_millis();
        let delayed = status_to_str(WorkflowStatusType::Delayed);
        let client = self.pool.get().await.map_err(pool_err)?;
        client
            .execute(&query, &[&delay_ms, &now_ms, &workflow_id, &delayed])
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> JournioResult<()> {
        if workflow_ids.is_empty() {
            return Ok(());
        }

        let mut ids: Vec<String> = workflow_ids.to_vec();
        if delete_children {
            ids = gather_descendants(&self.pool, &ids).await?;
        }

        let query = self.q("DELETE FROM workflow_status WHERE workflow_uuid = ANY($1)");
        let client = self.pool.get().await.map_err(pool_err)?;
        client.execute(&query, &[&ids]).await.map_err(db_err)?;
        Ok(())
    }

    async fn list_workflows_filtered(
        &self,
        filter: &ListWorkflowsFilter,
    ) -> JournioResult<Vec<WorkflowStatus>> {
        use tokio_postgres::types::ToSql;

        // Concrete bind values — only the ones referenced by a non-empty/
        // present filter field are actually appended to `params`. Each
        // reference coerces concrete→trait (`&Vec<String>` →
        // `&(dyn ToSql + Sync)`), which keeps the future `Send` (the boxed
        // trait-object route cannot, because dropping `Send` from a trait
        // object is not an unsizing coercion).
        let workflow_ids = filter.workflow_ids.clone();
        let workflow_id_prefix_patterns: Vec<String> = filter
            .workflow_id_prefixes
            .iter()
            .map(|prefix| format!("{prefix}%"))
            .collect();
        let statuses: Vec<String> = filter
            .statuses
            .iter()
            .map(|status| status_to_str(*status))
            .collect();
        let names = filter.names.clone();
        let application_versions = filter.application_versions.clone();
        let queue_names = filter.queue_names.clone();
        let authenticated_users = filter.authenticated_users.clone();
        let executor_ids = filter.executor_ids.clone();
        let forked_from = filter.forked_from.clone();
        let parent_workflow_ids = filter.parent_workflow_ids.clone();
        let deduplication_ids = filter.deduplication_ids.clone();
        let start_ms = filter.start_time.map(|t| t.timestamp_millis());
        let end_ms = filter.end_time.map(|t| t.timestamp_millis());
        let completed_after_ms = filter.completed_after.map(|t| t.timestamp_millis());
        let completed_before_ms = filter.completed_before.map(|t| t.timestamp_millis());
        let limit = filter.limit.unwrap_or(100);
        let offset = filter.offset.unwrap_or(0);

        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut idx = 1usize;

        macro_rules! bind_array {
            ($values:expr, $predicate:literal) => {{
                if !$values.is_empty() {
                    params.push(&$values);
                    clauses.push(format!(concat!($predicate, " = ANY(${})"), idx));
                    idx += 1;
                }
            }};
        }
        macro_rules! bind_scalar {
            ($value:expr, $predicate:literal) => {{
                if $value.is_some() {
                    params.push(&$value);
                    clauses.push(format!(concat!($predicate, " ${}"), idx));
                    idx += 1;
                }
            }};
        }

        bind_array!(workflow_ids, "workflow_uuid");
        if !workflow_id_prefix_patterns.is_empty() {
            params.push(&workflow_id_prefix_patterns);
            clauses.push(format!("workflow_uuid LIKE ANY(${})", idx));
            idx += 1;
        }
        bind_array!(statuses, "status");
        bind_array!(names, "name");
        bind_array!(application_versions, "application_version");
        if filter.queues_only {
            clauses.push("queue_name IS NOT NULL".to_string());
        } else {
            bind_array!(queue_names, "queue_name");
        }
        bind_array!(authenticated_users, "authenticated_user");
        bind_array!(executor_ids, "executor_id");
        bind_array!(forked_from, "forked_from");
        bind_array!(parent_workflow_ids, "parent_workflow_id");
        bind_array!(deduplication_ids, "deduplication_id");
        bind_scalar!(start_ms, "created_at >=");
        bind_scalar!(end_ms, "created_at <=");
        bind_scalar!(completed_after_ms, "completed_at >=");
        bind_scalar!(completed_before_ms, "completed_at <=");

        let direction = if filter.sort_desc { "DESC" } else { "ASC" };
        let mut query = format!("{WORKFLOW_STATUS_SELECT}");
        if !clauses.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&clauses.join(" AND "));
        }
        query.push_str(&format!(" ORDER BY created_at {direction}"));
        params.push(&limit);
        query.push_str(&format!(" LIMIT ${idx}"));
        idx += 1;
        if offset > 0 {
            params.push(&offset);
            query.push_str(&format!(" OFFSET ${idx}"));
        }

        let query = self.q(&query);
        let client = self.pool.get().await.map_err(pool_err)?;
        let rows = client
            .query(&query, &params)
            .await
            .map_err(db_err)?;
        Ok(rows.iter().map(row_to_workflow_status).collect())
    }
}

// Column list shared by get_workflow_status / list_workflows. Index positions
// are referenced by `row_to_workflow_status`.
const WORKFLOW_STATUS_SELECT: &str = "SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, \
            output, error, executor_id, created_at, updated_at, application_version, application_id, \
            recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, \
            started_at_epoch_ms, deduplication_id, inputs, priority, queue_partition_key, \
            forked_from, was_forked_from, parent_workflow_id, completed_at, class_name, config_name, \
            serialization, delay_until_epoch_ms \
     FROM workflow_status";

const WORKFLOW_STATUS_BY_ID: &str = "SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles, \
            output, error, executor_id, created_at, updated_at, application_version, application_id, \
            recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, \
            started_at_epoch_ms, deduplication_id, inputs, priority, queue_partition_key, \
            forked_from, was_forked_from, parent_workflow_id, completed_at, class_name, config_name, \
            serialization, delay_until_epoch_ms \
     FROM workflow_status WHERE workflow_uuid = $1";

fn init_err(e: impl std::fmt::Display) -> JournioError {
    JournioError::new(JournioErrorCode::InitializationError, e.to_string())
}

pub(crate) fn db_err(e: tokio_postgres::Error) -> JournioError {
    JournioError {
        code: if matches!(
            sqlstate(&e),
            Some(crate::error::sqlstate::SERIALIZATION_FAILURE)
                | Some(crate::error::sqlstate::DEADLOCK_DETECTED)
        ) {
            JournioErrorCode::WorkflowExecutionError
        } else {
            JournioErrorCode::InitializationError
        },
        message: e.to_string(),
        source: Some(Box::new(e)),
        ..Default::default()
    }
}

pub(crate) fn pool_err(e: deadpool_postgres::PoolError) -> JournioError {
    JournioError::new(JournioErrorCode::InitializationError, e.to_string())
}

fn status_to_str(s: WorkflowStatusType) -> String {
    match s {
        WorkflowStatusType::Pending => "PENDING",
        WorkflowStatusType::Enqueued => "ENQUEUED",
        WorkflowStatusType::Delayed => "DELAYED",
        WorkflowStatusType::Success => "SUCCESS",
        WorkflowStatusType::Error => "ERROR",
        WorkflowStatusType::Cancelled => "CANCELLED",
        WorkflowStatusType::MaxRecoveryAttemptsExceeded => "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    }
    .to_string()
}

fn parse_status(s: &str) -> WorkflowStatusType {
    match s {
        "PENDING" => WorkflowStatusType::Pending,
        "ENQUEUED" => WorkflowStatusType::Enqueued,
        "DELAYED" => WorkflowStatusType::Delayed,
        "SUCCESS" => WorkflowStatusType::Success,
        "ERROR" => WorkflowStatusType::Error,
        "CANCELLED" => WorkflowStatusType::Cancelled,
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => WorkflowStatusType::MaxRecoveryAttemptsExceeded,
        other => {
            tracing::warn!(
                status = other,
                "unknown workflow status, defaulting to PENDING"
            );
            WorkflowStatusType::Pending
        }
    }
}

fn schedule_status_to_str(status: ScheduleStatus) -> &'static str {
    match status {
        ScheduleStatus::Active => "ACTIVE",
        ScheduleStatus::Paused => "PAUSED",
    }
}

fn parse_schedule_status(s: &str) -> ScheduleStatus {
    match s {
        "PAUSED" => ScheduleStatus::Paused,
        _ => ScheduleStatus::Active,
    }
}

fn timestamp_ms(ms: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch"))
}

fn row_to_workflow_status(r: &tokio_postgres::Row) -> WorkflowStatus {
    let roles_json: Option<String> = r.get(5);
    let authenticated_roles = roles_json
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .filter(|v| !v.is_empty());

    let output_str: Option<String> = r.get(6);
    let output = output_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok());

    let input_str: Option<String> = r.get(19);
    let input = input_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok());

    let created_ms: i64 = r.get(9);
    let updated_ms: i64 = r.get(10);
    let timeout_ms: Option<i64> = r.get(15);
    let deadline_ms: Option<i64> = r.get(16);
    let started_ms: Option<i64> = r.get(17);
    let completed_ms: Option<i64> = r.get(25);
    let delay_ms: Option<i64> = r.get(29);

    WorkflowStatus {
        id: r.get(0),
        status: parse_status(r.get(1)),
        name: r.get(2),
        authenticated_user: r.get(3),
        assumed_role: r.get(4),
        authenticated_roles,
        output,
        error: r.get(7),
        executor_id: r.get(8),
        created_at: timestamp_ms(created_ms),
        updated_at: timestamp_ms(updated_ms),
        application_version: r.try_get::<_, String>(11).unwrap_or_default(),
        application_id: r.get(12),
        attempts: r.get::<_, i64>(13),
        queue_name: r.get(14),
        timeout: timeout_ms
            .filter(|&m| m > 0)
            .map(|m| Duration::from_millis(m as u64)),
        deadline: deadline_ms.map(timestamp_ms),
        started_at: started_ms.map(timestamp_ms),
        deduplication_id: r.get(18),
        input,
        priority: r.get(20),
        queue_partition_key: r.get(21),
        forked_from: r.get(22),
        was_forked_from: r.get(23),
        parent_workflow_id: r.get(24),
        completed_at: completed_ms.map(timestamp_ms),
        class_name: r.get(26),
        config_name: r.get(27),
        serialization: r.get(28),
        delay_until: delay_ms.map(timestamp_ms),
    }
}

fn row_to_workflow_schedule(r: &tokio_postgres::Row) -> WorkflowSchedule {
    let context_str: String = r.get(6);
    let last_fired_at_str: Option<String> = r.get(7);
    WorkflowSchedule {
        schedule_id: r.get(0),
        schedule_name: r.get(1),
        workflow_name: r.get(2),
        workflow_class_name: r.get(3),
        schedule: r.get(4),
        status: parse_schedule_status(r.get(5)),
        context: serde_json::from_str(&context_str).unwrap_or(Interchange::Null),
        last_fired_at: last_fired_at_str
            .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| value.with_timezone(&Utc)),
        automatic_backfill: r.get(8),
        cron_timezone: r.get(9),
        queue_name: r.get(10),
    }
}

fn row_to_queue_config(r: &tokio_postgres::Row) -> QueueConfig {
    QueueConfig {
        queue_id: r.get(0),
        name: r.get(1),
        concurrency: r.get(2),
        worker_concurrency: r.get(3),
        rate_limit_max: r.get(4),
        rate_limit_period_sec: r.get(5),
        priority_enabled: r.get(6),
        partition_queue: r.get(7),
        polling_interval_sec: r.get(8),
    }
}

/// Recursively gather every descendant workflow id of `roots` (breadth-first,
/// ported from `getWorkflowChildren` in `system_database.go`). Used by
/// `delete_workflows` when `delete_children` is set. Includes the roots
/// themselves in the returned set.
async fn gather_descendants(
    pool: &Pool,
    roots: &[String],
) -> JournioResult<Vec<String>> {
    let client = pool.get().await.map_err(pool_err)?;
    let mut all: Vec<String> = roots.to_vec();
    let mut queue: Vec<String> = roots.to_vec();
    while let Some(parent) = queue.pop() {
        let query = "SELECT workflow_uuid FROM workflow_status WHERE parent_workflow_id = $1";
        let rows = client.query(query, &[&parent]).await.map_err(db_err)?;
        for row in rows {
            let id: String = row.get(0);
            if !all.contains(&id) {
                all.push(id.clone());
                queue.push(id);
            }
        }
    }
    Ok(all)
}

