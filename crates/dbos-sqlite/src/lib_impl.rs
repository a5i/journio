use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dbos_core::dialect::Dialect;
use dbos_core::error::{DbosError, DbosErrorCode, DbosResult};
use dbos_core::system_db::{ForkWorkflow, InitWorkflow, InitWorkflowResult, SystemDatabase};
use dbos_core::types::{
    ListWorkflowsFilter, QueueConfig, ScheduleStatus, StepRecord, StreamEntry, VersionInfo,
    WorkflowSchedule, WorkflowStatus, WorkflowStatusType,
};
use dbos_core::value::Interchange;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use sqlx::{Executor, QueryBuilder, Row, Sqlite, SqlitePool};

use crate::dialect::SqliteDialect;
use crate::error::{db_err, is_foreign_key_violation, is_unique_violation};
use crate::migrations::run_migrations;

const NULL_TOPIC: &str = "__null__topic__";
const SERIALIZATION_JSON: &str = "DBOS_JSON";
const STREAM_CLOSED_SENTINEL: &str = "__DBOS_STREAM_CLOSED__";
const INTERNAL_QUEUE_NAME: &str = "_dbos_internal_queue";

pub struct SqliteSystemDatabase {
    pool: SqlitePool,
    dialect: SqliteDialect,
}

impl SqliteSystemDatabase {
    pub async fn connect(database_url: &str) -> Result<Self, DbosError> {
        let options = sqlite_connect_options(database_url)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    conn.execute("PRAGMA foreign_keys = ON;").await?;
                    conn.execute("PRAGMA busy_timeout = 5000;").await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(db_err)?;
        Ok(Self {
            pool,
            dialect: SqliteDialect,
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    fn q(&self, body: &str) -> String {
        self.dialect.rewrite_query(body)
    }
}

#[async_trait]
impl SystemDatabase for SqliteSystemDatabase {
    fn dialect(&self) -> &dyn Dialect {
        &self.dialect
    }

    async fn migrate(&self) -> DbosResult<()> {
        run_migrations(self.pool.clone()).await
    }

    async fn launch(&self) -> DbosResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> DbosResult<()> {
        self.pool.close().await;
        Ok(())
    }

    async fn init_workflow(&self, init: InitWorkflow) -> DbosResult<InitWorkflowResult> {
        let attempts: i32 = match init.status {
            WorkflowStatusType::Enqueued | WorkflowStatusType::Delayed => 0,
            _ => 1,
        };
        let now_ms = Utc::now().timestamp_millis();
        let timeout_ms: Option<i64> = init.timeout.map(|d| d.as_millis() as i64);
        let deadline_ms: Option<i64> = init.deadline.map(|t| t.timestamp_millis());
        let delay_until_ms: Option<i64> = init.delay_until.map(|t| t.timestamp_millis());
        let inputs_str = init.input.as_ref().map(stringify_json);
        let roles_json =
            serde_json::to_string(&init.authenticated_roles).unwrap_or_else(|_| "null".into());
        let recovery_increment: i32 = i32::from(init.increment_attempts);

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
                        WHEN excluded.status NOT IN ($26, $27) THEN workflow_status.recovery_attempts + $28
                        ELSE workflow_status.recovery_attempts
                    END,
                    updated_at = excluded.updated_at,
                    executor_id = CASE
                        WHEN excluded.status IN ($26, $27) THEN workflow_status.executor_id
                        ELSE excluded.executor_id
                    END
                RETURNING recovery_attempts, status, name, queue_name, queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms",
        );

        let status_str = status_to_str(init.status);
        let enqueued_str = status_to_str(WorkflowStatusType::Enqueued);
        let delayed_str = status_to_str(WorkflowStatusType::Delayed);

        let row = sqlx::query(&query)
            .bind(&init.workflow_id)
            .bind(&status_str)
            .bind(&init.name)
            .bind(&init.queue_name)
            .bind(&init.authenticated_user)
            .bind(&init.assumed_role)
            .bind(&roles_json)
            .bind(&init.executor_id)
            .bind(&init.application_version)
            .bind(&init.application_id)
            .bind(now_ms)
            .bind(attempts)
            .bind(now_ms)
            .bind(timeout_ms)
            .bind(deadline_ms)
            .bind(&inputs_str)
            .bind(&init.deduplication_id)
            .bind(init.priority)
            .bind(&init.queue_partition_key)
            .bind(Option::<String>::None)
            .bind(&init.parent_workflow_id)
            .bind(&init.class_name)
            .bind(&init.config_name)
            .bind(&init.serialization)
            .bind(delay_until_ms)
            .bind(&enqueued_str)
            .bind(&delayed_str)
            .bind(recovery_increment)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| {
                if is_unique_violation(&err) {
                    DbosError {
                        code: DbosErrorCode::QueueDeduplicated,
                        message: format!(
                            "Workflow {} was deduplicated due to an existing workflow in queue {:?} with deduplication ID {:?}",
                            init.workflow_id, init.queue_name, init.deduplication_id
                        ),
                        workflow_id: Some(init.workflow_id.clone()),
                        queue_name: init.queue_name.clone(),
                        deduplication_id: init.deduplication_id.clone(),
                        source: Some(Box::new(err)),
                        ..Default::default()
                    }
                } else {
                    db_err(err)
                }
            })?;

        let attempts_out: i32 = row.get(0);
        let status_out_str: String = row.get(1);
        let name_out: String = row.get(2);
        let queue_name_out: Option<String> = row.get(3);
        let queue_partition_out: Option<String> = row.get(4);
        let timeout_out: Option<i64> = row.get(5);
        let deadline_out: Option<i64> = row.get(6);

        if !init.name.is_empty() && name_out != init.name {
            return Err(DbosError {
                code: DbosErrorCode::ConflictingWorkflowError,
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
                    return Err(DbosError {
                        code: DbosErrorCode::ConflictingWorkflowError,
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
            && attempts_out > init.max_retries + 1
        {
            let dlq = self.q(
                "UPDATE workflow_status SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL WHERE workflow_uuid = $2 AND status = $3",
            );
            sqlx::query(&dlq)
                .bind(status_to_str(
                    WorkflowStatusType::MaxRecoveryAttemptsExceeded,
                ))
                .bind(&init.workflow_id)
                .bind(&status_str)
                .execute(&self.pool)
                .await
                .map_err(db_err)?;
            return Err(DbosError {
                code: DbosErrorCode::DeadLetterQueueError,
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
            attempts: attempts_out as i64,
            name: name_out,
            queue_name: queue_name_out,
            queue_partition_key: queue_partition_out,
            timeout: timeout_out
                .filter(|&m| m > 0)
                .map(|m| Duration::from_millis(m as u64)),
            deadline: deadline_out.map(timestamp_ms),
        })
    }

    async fn record_workflow_result(
        &self,
        workflow_id: &str,
        status: WorkflowStatusType,
        output: Option<&Interchange>,
        error: Option<&str>,
    ) -> DbosResult<()> {
        let output_str = output.map(stringify_json);
        let now_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "UPDATE workflow_status \
             SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL \
             WHERE workflow_uuid = $5 AND NOT (status = $6 AND CAST($1 AS TEXT) IN ($7, $8))",
        );
        sqlx::query(&query)
            .bind(status_to_str(status))
            .bind(output_str)
            .bind(error)
            .bind(now_ms)
            .bind(workflow_id)
            .bind(status_to_str(WorkflowStatusType::Cancelled))
            .bind(status_to_str(WorkflowStatusType::Success))
            .bind(status_to_str(WorkflowStatusType::Error))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get_workflow_status(&self, workflow_id: &str) -> DbosResult<Option<WorkflowStatus>> {
        let query = self.q(WORKFLOW_STATUS_BY_ID);
        let row = sqlx::query(&query)
            .bind(workflow_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(row.map(row_to_workflow_status))
    }

    async fn list_workflows(&self, limit: i64) -> DbosResult<Vec<WorkflowStatus>> {
        let query = format!(
            "{} ORDER BY created_at DESC LIMIT ?1",
            self.q(WORKFLOW_STATUS_SELECT)
        );
        let rows = sqlx::query(&query)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_workflow_status).collect())
    }

    async fn cancel_workflows(&self, workflow_ids: &[String]) -> DbosResult<Vec<String>> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let mut update = QueryBuilder::<Sqlite>::new(
            "UPDATE workflow_status
             SET status = ",
        );
        update.push_bind(status_to_str(WorkflowStatusType::Cancelled));
        update.push(
            ", updated_at = ",
        );
        update.push_bind(now_ms);
        update.push(
            ", completed_at = ",
        );
        update.push_bind(now_ms);
        update.push(
            ", started_at_epoch_ms = NULL, queue_name = NULL, deduplication_id = NULL
             WHERE workflow_uuid IN (",
        );
        {
            let mut separated = update.separated(", ");
            for workflow_id in workflow_ids {
                separated.push_bind(workflow_id);
            }
        }
        update.push(
            ") AND status NOT IN (",
        );
        update.push_bind(status_to_str(WorkflowStatusType::Success));
        update.push(", ");
        update.push_bind(status_to_str(WorkflowStatusType::Error));
        update.push(", ");
        update.push_bind(status_to_str(WorkflowStatusType::Cancelled));
        update.push(")");
        update.build().execute(&mut *tx).await.map_err(db_err)?;

        let mut select = QueryBuilder::<Sqlite>::new(format!("{WORKFLOW_STATUS_ID_SELECT_PREFIX} WHERE workflow_uuid IN ("));
        {
            let mut separated = select.separated(", ");
            for workflow_id in workflow_ids {
                separated.push_bind(workflow_id);
            }
        }
        select.push(")");
        let rows = select.build().fetch_all(&mut *tx).await.map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn resume_workflows(
        &self,
        workflow_ids: &[String],
        queue_name: Option<&str>,
    ) -> DbosResult<Vec<String>> {
        if workflow_ids.is_empty() {
            return Ok(Vec::new());
        }

        let now_ms = Utc::now().timestamp_millis();
        let queue_name = queue_name.unwrap_or(INTERNAL_QUEUE_NAME);
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        let mut update = QueryBuilder::<Sqlite>::new(
            "UPDATE workflow_status
             SET status = ",
        );
        update.push_bind(status_to_str(WorkflowStatusType::Enqueued));
        update.push(", queue_name = ");
        update.push_bind(queue_name);
        update.push(
            ", recovery_attempts = 0, workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
             started_at_epoch_ms = NULL, updated_at = ",
        );
        update.push_bind(now_ms);
        update.push(
            ", completed_at = NULL
             WHERE workflow_uuid IN (",
        );
        {
            let mut separated = update.separated(", ");
            for workflow_id in workflow_ids {
                separated.push_bind(workflow_id);
            }
        }
        update.push(") AND status NOT IN (");
        update.push_bind(status_to_str(WorkflowStatusType::Success));
        update.push(", ");
        update.push_bind(status_to_str(WorkflowStatusType::Error));
        update.push(")");
        update.build().execute(&mut *tx).await.map_err(db_err)?;

        let mut select = QueryBuilder::<Sqlite>::new(format!("{WORKFLOW_STATUS_ID_SELECT_PREFIX} WHERE workflow_uuid IN ("));
        {
            let mut separated = select.separated(", ");
            for workflow_id in workflow_ids {
                separated.push_bind(workflow_id);
            }
        }
        select.push(")");
        let rows = select.build().fetch_all(&mut *tx).await.map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    async fn get_workflow_children(&self, workflow_id: &str) -> DbosResult<Vec<WorkflowStatus>> {
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
        let rows = sqlx::query(&query)
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_workflow_status).collect())
    }

    async fn fork_workflow(&self, input: ForkWorkflow) -> DbosResult<String> {
        if input.start_step < 0 {
            return Err(DbosError::new(
                DbosErrorCode::InitializationError,
                format!("startStep must be >= 0, got {}", input.start_step),
            ));
        }

        let forked_workflow_id = input
            .forked_workflow_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let queue_name = input
            .queue_name
            .unwrap_or_else(|| INTERNAL_QUEUE_NAME.to_string());

        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let original_row = sqlx::query(&self.q(WORKFLOW_STATUS_BY_ID))
            .bind(&input.original_workflow_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
        let Some(original_row) = original_row else {
            return Err(DbosError::new(
                DbosErrorCode::NonExistentWorkflowError,
                format!("workflow {} does not exist", input.original_workflow_id),
            ));
        };
        let original = row_to_workflow_status(original_row);
        let now_ms = Utc::now().timestamp_millis();
        let authenticated_roles =
            serde_json::to_string(&original.authenticated_roles).unwrap_or_else(|_| "null".into());
        let application_version = input
            .application_version
            .unwrap_or_else(|| original.application_version.clone());
        let inputs = original.input.as_ref().map(stringify_json);

        let insert_query = self.q(
            "INSERT INTO workflow_status (
                 workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
                 executor_id, application_version, application_id, queue_name, queue_partition_key,
                 inputs, created_at, updated_at, recovery_attempts, forked_from, serialization,
                 class_name, config_name
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)",
        );
        sqlx::query(&insert_query)
            .bind(&forked_workflow_id)
            .bind(status_to_str(WorkflowStatusType::Enqueued))
            .bind(&original.name)
            .bind(&original.authenticated_user)
            .bind(&original.assumed_role)
            .bind(&authenticated_roles)
            .bind(&original.executor_id)
            .bind(&application_version)
            .bind(&original.application_id)
            .bind(&queue_name)
            .bind(&input.queue_partition_key)
            .bind(&inputs)
            .bind(now_ms)
            .bind(now_ms)
            .bind(0_i64)
            .bind(&input.original_workflow_id)
            .bind(&original.serialization)
            .bind(&original.class_name)
            .bind(&original.config_name)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

        sqlx::query(&self.q(
            "UPDATE workflow_status SET was_forked_from = 1 WHERE workflow_uuid = $1",
        ))
        .bind(&input.original_workflow_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        if input.start_step > 0 {
            sqlx::query(&self.q(
                "INSERT INTO operation_outputs
                     (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
                 SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
                 FROM operation_outputs
                 WHERE workflow_uuid = $2 AND function_id < $3",
            ))
            .bind(&forked_workflow_id)
            .bind(&input.original_workflow_id)
            .bind(input.start_step)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

            sqlx::query(&self.q(
                "INSERT INTO workflow_events_history
                     (workflow_uuid, function_id, key, value, serialization)
                 SELECT $1, function_id, key, value, serialization
                 FROM workflow_events_history
                 WHERE workflow_uuid = $2 AND function_id < $3",
            ))
            .bind(&forked_workflow_id)
            .bind(&input.original_workflow_id)
            .bind(input.start_step)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

            sqlx::query(&self.q(
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
            ))
            .bind(&forked_workflow_id)
            .bind(&input.original_workflow_id)
            .bind(input.start_step)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

            sqlx::query(&self.q(
                "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
                 SELECT $1, key, value, \"offset\", function_id, serialization
                 FROM streams
                 WHERE workflow_uuid = $2 AND function_id < $3",
            ))
            .bind(&forked_workflow_id)
            .bind(&input.original_workflow_id)
            .bind(input.start_step)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        }

        tx.commit().await.map_err(db_err)?;
        Ok(forked_workflow_id)
    }

    async fn get_queue(&self, queue_name: &str) -> DbosResult<Option<QueueConfig>> {
        let query = self.q(
            "SELECT queue_id, name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
                    priority_enabled, partition_queue, polling_interval_sec
             FROM queues WHERE name = $1",
        );
        let row = sqlx::query(&query)
            .bind(queue_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(row.map(row_to_queue_config))
    }

    async fn upsert_queue(&self, queue: &QueueConfig) -> DbosResult<()> {
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
        sqlx::query(&query)
            .bind(&queue.queue_id)
            .bind(&queue.name)
            .bind(queue.concurrency)
            .bind(queue.worker_concurrency)
            .bind(queue.rate_limit_max)
            .bind(queue.rate_limit_period_sec)
            .bind(queue.priority_enabled)
            .bind(queue.partition_queue)
            .bind(queue.polling_interval_sec)
            .bind(now_ms)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn record_step_output(&self, step: &StepRecord) -> DbosResult<()> {
        let now_ms = Utc::now().timestamp_millis();
        let query = self.q("INSERT INTO operation_outputs \
                (workflow_uuid, function_id, function_name, output, error, child_workflow_id, \
                 started_at_epoch_ms, completed_at_epoch_ms, serialization) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)");
        sqlx::query(&query)
            .bind(&step.workflow_uuid)
            .bind(step.function_id)
            .bind(&step.function_name)
            .bind(&step.output)
            .bind(&step.error)
            .bind(&step.child_workflow_id)
            .bind(now_ms)
            .bind(now_ms)
            .bind(SERIALIZATION_JSON)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if is_unique_violation(&err) {
                    DbosError {
                        code: DbosErrorCode::ConflictingIDError,
                        message: format!("Conflicting workflow ID {}", step.workflow_uuid),
                        workflow_id: Some(step.workflow_uuid.clone()),
                        source: Some(Box::new(err)),
                        ..Default::default()
                    }
                } else {
                    db_err(err)
                }
            })?;
        Ok(())
    }

    async fn get_steps(&self, workflow_id: &str) -> DbosResult<Vec<StepRecord>> {
        let query = self.q(
            "SELECT workflow_uuid, function_id, function_name, output, error, child_workflow_id \
             FROM operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id ASC",
        );
        let rows = sqlx::query(&query)
            .bind(workflow_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|row| StepRecord {
                workflow_uuid: row.get(0),
                function_id: row.get(1),
                function_name: row.get(2),
                output: row.get(3),
                error: row.get(4),
                child_workflow_id: row.get(5),
            })
            .collect())
    }

    async fn dequeue_workflow(
        &self,
        queue_name: &str,
        executor_id: &str,
    ) -> DbosResult<Option<WorkflowStatus>> {
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
             ORDER BY priority ASC, created_at ASC",
        );
        let candidates = sqlx::query(&candidates_query)
            .bind(queue_name)
            .bind(status_to_str(WorkflowStatusType::Enqueued))
            .bind(status_to_str(WorkflowStatusType::Delayed))
            .bind(now_ms)
            .fetch_all(&self.pool)
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
                        sqlx::query_scalar(&query)
                            .bind(queue_name)
                            .bind(status_to_str(WorkflowStatusType::Pending))
                            .bind(&partition_key)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(db_err)?
                    } else {
                        sqlx::query_scalar(&query)
                            .bind(queue_name)
                            .bind(status_to_str(WorkflowStatusType::Pending))
                            .fetch_one(&self.pool)
                            .await
                            .map_err(db_err)?
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
                               AND rate_limited = 1
                               AND status NOT IN ($2, $3)
                               AND started_at_epoch_ms > $4
                               AND queue_partition_key = $5",
                        )
                    } else {
                        self.q(
                            "SELECT COUNT(*) FROM workflow_status
                             WHERE queue_name = $1
                               AND rate_limited = 1
                               AND status NOT IN ($2, $3)
                               AND started_at_epoch_ms > $4",
                        )
                    };
                    let count: i64 = if cfg.partition_queue {
                        sqlx::query_scalar(&query)
                            .bind(queue_name)
                            .bind(status_to_str(WorkflowStatusType::Enqueued))
                            .bind(status_to_str(WorkflowStatusType::Delayed))
                            .bind(cutoff_ms)
                            .bind(&partition_key)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(db_err)?
                    } else {
                        sqlx::query_scalar(&query)
                            .bind(queue_name)
                            .bind(status_to_str(WorkflowStatusType::Enqueued))
                            .bind(status_to_str(WorkflowStatusType::Delayed))
                            .bind(cutoff_ms)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(db_err)?
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
            return Ok(None);
        };

        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let query = self.q(
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
        let row = sqlx::query(&query)
            .bind(status_to_str(WorkflowStatusType::Pending))
            .bind(executor_id)
            .bind(now_ms)
            .bind(queue.as_ref().is_some_and(|cfg| cfg.rate_limit_max.is_some()))
            .bind(selected_id)
            .bind(status_to_str(WorkflowStatusType::Enqueued))
            .bind(status_to_str(WorkflowStatusType::Delayed))
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(row.map(row_to_workflow_status))
    }

    async fn list_runnable_queues(&self) -> DbosResult<Vec<String>> {
        let query = self.q(
            "SELECT DISTINCT queue_name
             FROM workflow_status
             WHERE queue_name IS NOT NULL
               AND status IN ($1, $2)
             ORDER BY queue_name ASC",
        );
        let rows = sqlx::query(&query)
            .bind(status_to_str(WorkflowStatusType::Enqueued))
            .bind(status_to_str(WorkflowStatusType::Delayed))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.get::<Option<String>, _>(0))
            .collect())
    }

    async fn list_queues(&self) -> DbosResult<Vec<QueueConfig>> {
        let query = self.q(
            "SELECT queue_id, name, concurrency, worker_concurrency, rate_limit_max,
                    rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec
             FROM queues
             ORDER BY name ASC",
        );
        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_queue_config).collect())
    }

    async fn send(
        &self,
        destination_id: &str,
        topic: &str,
        message: &Interchange,
    ) -> DbosResult<()> {
        let topic = if topic.is_empty() { NULL_TOPIC } else { topic };
        let message_str = stringify_json(message);
        let message_uuid = uuid::Uuid::new_v4().to_string();
        let created_ms = Utc::now().timestamp_millis();
        let query = self.q(
            "INSERT INTO notifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        );
        sqlx::query(&query)
            .bind(destination_id)
            .bind(topic)
            .bind(message_str)
            .bind(SERIALIZATION_JSON)
            .bind(message_uuid)
            .bind(created_ms)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if is_foreign_key_violation(&err) {
                    DbosError {
                        code: DbosErrorCode::NonExistentWorkflowError,
                        message: format!("destination workflow {destination_id} does not exist"),
                        workflow_id: Some(destination_id.to_string()),
                        source: Some(Box::new(err)),
                        ..Default::default()
                    }
                } else {
                    db_err(err)
                }
            })?;
        Ok(())
    }

    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
    ) -> DbosResult<Option<dbos_core::Notification>> {
        let topic = if topic.is_empty() { NULL_TOPIC } else { topic };
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let select = self.q("SELECT message_uuid, message, serialization \
             FROM notifications \
             WHERE destination_uuid = $1 AND topic = $2 AND consumed = false \
             ORDER BY created_at_epoch_ms ASC LIMIT 1");
        let Some(row) = sqlx::query(&select)
            .bind(workflow_id)
            .bind(topic)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?
        else {
            tx.commit().await.map_err(db_err)?;
            return Ok(None);
        };

        let message_uuid: String = row.get(0);
        let message_str: Option<String> = row.get(1);
        let serialization: Option<String> = row.get(2);

        let update = self.q(
            "UPDATE notifications SET consumed = true WHERE message_uuid = $1 AND consumed = false",
        );
        let updated = sqlx::query(&update)
            .bind(&message_uuid)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
        tx.commit().await.map_err(db_err)?;
        if updated == 0 {
            return Ok(None);
        }

        let message = message_str
            .and_then(|s| serde_json::from_str::<Interchange>(&s).ok())
            .unwrap_or(Interchange::Null);
        Ok(Some(dbos_core::Notification {
            message,
            serialization,
        }))
    }

    async fn set_event(
        &self,
        workflow_id: &str,
        key: &str,
        value: &Interchange,
        function_id: i32,
    ) -> DbosResult<()> {
        let value_str = stringify_json(value);
        let mut tx = self.pool.begin().await.map_err(db_err)?;

        sqlx::query(&self.q(
            "INSERT INTO workflow_events (workflow_uuid, key, value, serialization) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (workflow_uuid, key) \
                 DO UPDATE SET value = excluded.value, serialization = excluded.serialization",
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(&value_str)
        .bind(SERIALIZATION_JSON)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        sqlx::query(
            &self.q(
                "INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value, serialization) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (workflow_uuid, function_id, key) \
                 DO UPDATE SET value = excluded.value, serialization = excluded.serialization",
            ),
        )
        .bind(workflow_id)
        .bind(function_id)
        .bind(key)
        .bind(&value_str)
        .bind(SERIALIZATION_JSON)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn get_event_value(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> DbosResult<Option<Interchange>> {
        let query =
            self.q("SELECT value FROM workflow_events WHERE workflow_uuid = $1 AND key = $2");
        let row = sqlx::query(&query)
            .bind(workflow_id)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        let Some(row) = row else { return Ok(None) };
        let value_str: Option<String> = row.get(0);
        Ok(value_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok()))
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: &str,
        function_id: i32,
        serialization: Option<&str>,
    ) -> DbosResult<()> {
        let check_closed = self.q(
            "SELECT 1 FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND value = $3
             LIMIT 1",
        );
        let existing = sqlx::query_scalar::<_, i64>(&check_closed)
            .bind(workflow_id)
            .bind(key)
            .bind(STREAM_CLOSED_SENTINEL)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        if existing.is_some() {
            return Err(DbosError::new(
                DbosErrorCode::WorkflowExecutionError,
                format!("stream {key:?} is already closed"),
            ));
        }

        let query = self.q(
            "INSERT INTO streams (workflow_uuid, key, value, \"offset\", function_id, serialization)
             SELECT $1, $2, $3,
                    COALESCE((SELECT MAX(\"offset\") FROM streams WHERE workflow_uuid = $1 AND key = $2), -1) + 1,
                    $4, $5",
        );
        sqlx::query(&query)
            .bind(workflow_id)
            .bind(key)
            .bind(value)
            .bind(function_id)
            .bind(serialization)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i64,
    ) -> DbosResult<(Vec<StreamEntry>, bool)> {
        let query = self.q(
            "SELECT value, \"offset\", serialization
             FROM streams
             WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3
             ORDER BY \"offset\" ASC",
        );
        let rows = sqlx::query(&query)
            .bind(workflow_id)
            .bind(key)
            .bind(from_offset)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;

        let mut entries = Vec::new();
        let mut closed = false;
        for row in rows {
            let value: String = row.get(0);
            let offset: i64 = row.get(1);
            let serialization: Option<String> = row.get(2);
            if value == STREAM_CLOSED_SENTINEL {
                closed = true;
                break;
            }
            entries.push(StreamEntry {
                value,
                offset,
                serialization,
            });
        }
        Ok((entries, closed))
    }

    async fn get_workflows_for_recovery(
        &self,
        executor_id: &str,
    ) -> DbosResult<Vec<WorkflowStatus>> {
        let query = format!(
            "{} WHERE executor_id = ?1 AND status IN ('PENDING', 'ENQUEUED', 'DELAYED') ORDER BY created_at ASC",
            self.q(WORKFLOW_STATUS_SELECT)
        );
        let rows = sqlx::query(&query)
            .bind(executor_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_workflow_status).collect())
    }

    async fn delete_workflows_before(&self, before: DateTime<Utc>) -> DbosResult<u64> {
        let before_ms = before.timestamp_millis();
        let query = self.q(
            "DELETE FROM workflow_status WHERE status IN ('SUCCESS','ERROR','CANCELLED') AND completed_at < $1",
        );
        let rows = sqlx::query(&query)
            .bind(before_ms)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
        Ok(rows)
    }

    async fn upsert_schedule(&self, schedule: &WorkflowSchedule) -> DbosResult<()> {
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
        sqlx::query(&query)
            .bind(&schedule.schedule_id)
            .bind(&schedule.schedule_name)
            .bind(&schedule.workflow_name)
            .bind(&schedule.workflow_class_name)
            .bind(&schedule.schedule)
            .bind(schedule_status_to_str(schedule.status))
            .bind(stringify_json(&schedule.context))
            .bind(schedule.last_fired_at.map(|value| value.to_rfc3339()))
            .bind(schedule.automatic_backfill)
            .bind(&schedule.cron_timezone)
            .bind(&schedule.queue_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn list_schedules(&self) -> DbosResult<Vec<WorkflowSchedule>> {
        let query = self.q(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
             FROM workflow_schedules
             ORDER BY schedule_name ASC",
        );
        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_workflow_schedule).collect())
    }

    async fn update_schedule_last_fired_at(
        &self,
        schedule_name: &str,
        fired_at: DateTime<Utc>,
    ) -> DbosResult<()> {
        let query = self.q(
            "UPDATE workflow_schedules SET last_fired_at = $1 WHERE schedule_name = $2",
        );
        sqlx::query(&query)
            .bind(fired_at.to_rfc3339())
            .bind(schedule_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get_schedule(
        &self,
        schedule_name: &str,
    ) -> DbosResult<Option<WorkflowSchedule>> {
        let query = self.q(
            "SELECT schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, status, context, last_fired_at, automatic_backfill, cron_timezone, queue_name
             FROM workflow_schedules WHERE schedule_name = $1",
        );
        let row = sqlx::query(&query)
            .bind(schedule_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(row.map(row_to_workflow_schedule))
    }

    async fn delete_schedule(&self, schedule_name: &str) -> DbosResult<()> {
        let query = self.q("DELETE FROM workflow_schedules WHERE schedule_name = $1");
        sqlx::query(&query)
            .bind(schedule_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn update_schedule_status(
        &self,
        schedule_name: &str,
        status: ScheduleStatus,
    ) -> DbosResult<()> {
        let query =
            self.q("UPDATE workflow_schedules SET status = $1 WHERE schedule_name = $2");
        sqlx::query(&query)
            .bind(schedule_status_to_str(status))
            .bind(schedule_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn create_application_version(&self, version_name: &str) -> DbosResult<()> {
        let query = self.q(
            "INSERT INTO application_versions (version_id, version_name, version_timestamp, created_at)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (version_name) DO NOTHING",
        );
        let now_ms = Utc::now().timestamp_millis();
        sqlx::query(&query)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(version_name)
            .bind(now_ms)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn update_application_version_timestamp(
        &self,
        version_name: &str,
        timestamp_ms: i64,
    ) -> DbosResult<()> {
        let query = self.q(
            "UPDATE application_versions SET version_timestamp = $1 WHERE version_name = $2",
        );
        sqlx::query(&query)
            .bind(timestamp_ms)
            .bind(version_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn list_application_versions(&self) -> DbosResult<Vec<VersionInfo>> {
        let query = self.q(
            "SELECT version_id, version_name, version_timestamp, created_at FROM application_versions ORDER BY version_timestamp DESC",
        );
        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|row| VersionInfo {
                version_id: row.get(0),
                version_name: row.get(1),
                version_timestamp: row.get(2),
                created_at: row.get(3),
            })
            .collect())
    }

    async fn get_latest_application_version(&self) -> DbosResult<Option<VersionInfo>> {
        let query = self.q(
            "SELECT version_id, version_name, version_timestamp, created_at FROM application_versions ORDER BY version_timestamp DESC LIMIT 1",
        );
        let row = sqlx::query(&query)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(row.map(|row| VersionInfo {
            version_id: row.get(0),
            version_name: row.get(1),
            version_timestamp: row.get(2),
            created_at: row.get(3),
        }))
    }

    async fn set_workflow_delay(
        &self,
        workflow_id: &str,
        delay_until: DateTime<Utc>,
    ) -> DbosResult<()> {
        let query = self.q(
            "UPDATE workflow_status SET delay_until_epoch_ms = $1, updated_at = $2 WHERE workflow_uuid = $3 AND status = $4",
        );
        sqlx::query(&query)
            .bind(delay_until.timestamp_millis())
            .bind(Utc::now().timestamp_millis())
            .bind(workflow_id)
            .bind(status_to_str(WorkflowStatusType::Delayed))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn delete_workflows(
        &self,
        workflow_ids: &[String],
        delete_children: bool,
    ) -> DbosResult<()> {
        if workflow_ids.is_empty() {
            return Ok(());
        }

        let mut ids: Vec<String> = workflow_ids.to_vec();
        if delete_children {
            ids = gather_descendants_sqlite(&self.pool, &ids).await?;
        }

        let mut delete = QueryBuilder::<Sqlite>::new("DELETE FROM workflow_status WHERE workflow_uuid IN (");
        {
            let mut separated = delete.separated(", ");
            for id in &ids {
                separated.push_bind(id);
            }
        }
        delete.push(")");
        delete.build().execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    async fn list_workflows_filtered(
        &self,
        filter: &ListWorkflowsFilter,
    ) -> DbosResult<Vec<WorkflowStatus>> {
        let statuses: Vec<String> = filter
            .statuses
            .iter()
            .map(|status| status_to_str(*status))
            .collect();

        let mut qb = QueryBuilder::<Sqlite>::new(WORKFLOW_STATUS_SELECT);
        qb.push(" WHERE 1=1");

        if !filter.workflow_ids.is_empty() {
            qb.push(" AND workflow_uuid IN (");
            push_in_list(&mut qb, &filter.workflow_ids);
            qb.push(")");
        }
        if !filter.workflow_id_prefixes.is_empty() {
            qb.push(" AND (");
            let mut sep = qb.separated(" OR ");
            for prefix in &filter.workflow_id_prefixes {
                // Go appends "%" for a prefix match (`addWhereLikeAny`).
                sep.push("workflow_uuid LIKE ")
                    .push_bind_unseparated(format!("{prefix}%"));
            }
            qb.push(")");
        }
        if !statuses.is_empty() {
            qb.push(" AND status IN (");
            push_in_list(&mut qb, &statuses);
            qb.push(")");
        }
        if !filter.names.is_empty() {
            qb.push(" AND name IN (");
            push_in_list(&mut qb, &filter.names);
            qb.push(")");
        }
        if !filter.application_versions.is_empty() {
            qb.push(" AND application_version IN (");
            push_in_list(&mut qb, &filter.application_versions);
            qb.push(")");
        }
        if filter.queues_only {
            qb.push(" AND queue_name IS NOT NULL");
        } else if !filter.queue_names.is_empty() {
            qb.push(" AND queue_name IN (");
            push_in_list(&mut qb, &filter.queue_names);
            qb.push(")");
        }
        if !filter.authenticated_users.is_empty() {
            qb.push(" AND authenticated_user IN (");
            push_in_list(&mut qb, &filter.authenticated_users);
            qb.push(")");
        }
        if !filter.executor_ids.is_empty() {
            qb.push(" AND executor_id IN (");
            push_in_list(&mut qb, &filter.executor_ids);
            qb.push(")");
        }
        if !filter.forked_from.is_empty() {
            qb.push(" AND forked_from IN (");
            push_in_list(&mut qb, &filter.forked_from);
            qb.push(")");
        }
        if !filter.parent_workflow_ids.is_empty() {
            qb.push(" AND parent_workflow_id IN (");
            push_in_list(&mut qb, &filter.parent_workflow_ids);
            qb.push(")");
        }
        if !filter.deduplication_ids.is_empty() {
            qb.push(" AND deduplication_id IN (");
            push_in_list(&mut qb, &filter.deduplication_ids);
            qb.push(")");
        }
        if let Some(start) = filter.start_time {
            qb.push(" AND created_at >= ").push_bind(start.timestamp_millis());
        }
        if let Some(end) = filter.end_time {
            qb.push(" AND created_at <= ").push_bind(end.timestamp_millis());
        }
        if let Some(after) = filter.completed_after {
            qb.push(" AND completed_at >= ").push_bind(after.timestamp_millis());
        }
        if let Some(before) = filter.completed_before {
            qb.push(" AND completed_at <= ").push_bind(before.timestamp_millis());
        }

        let direction = if filter.sort_desc { "DESC" } else { "ASC" };
        qb.push(" ORDER BY created_at ").push(direction);
        let limit = filter.limit.unwrap_or(100);
        if limit > 0 {
            qb.push(" LIMIT ").push_bind(limit);
        }
        if let Some(offset) = filter.offset {
            if offset > 0 {
                qb.push(" OFFSET ").push_bind(offset);
            }
        }

        let rows = qb.build().fetch_all(&self.pool).await.map_err(db_err)?;
        Ok(rows.into_iter().map(row_to_workflow_status).collect())
    }
}

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

const WORKFLOW_STATUS_ID_SELECT_PREFIX: &str =
    "SELECT workflow_uuid FROM workflow_status";

fn sqlite_connect_options(database_url: &str) -> DbosResult<SqliteConnectOptions> {
    let normalized = normalize_sqlite_url(database_url)?;
    let options = SqliteConnectOptions::from_str(&normalized).map_err(|err| {
        DbosError::new(
            DbosErrorCode::InitializationError,
            format!("invalid sqlite connection string {database_url}: {err}"),
        )
    })?;
    Ok(options
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal))
}

fn normalize_sqlite_url(database_url: &str) -> DbosResult<String> {
    let lower = database_url.to_ascii_lowercase();
    if lower == ":memory:" {
        return Ok("sqlite::memory:".to_string());
    }

    let suffix = if lower.starts_with("sqlite3:") {
        &database_url["sqlite3:".len()..]
    } else if lower.starts_with("sqlite:") {
        &database_url["sqlite:".len()..]
    } else {
        return Ok(format!("sqlite://{database_url}"));
    };

    if suffix.is_empty() {
        return Err(DbosError::new(
            DbosErrorCode::InitializationError,
            "invalid sqlite connection string: URL has no path",
        ));
    }

    let normalized = if suffix.starts_with(":memory:") {
        "sqlite::memory:".to_string()
    } else if suffix.starts_with("//") {
        format!("sqlite:{suffix}")
    } else if suffix.starts_with('/') {
        format!("sqlite://{suffix}")
    } else {
        format!("sqlite:{suffix}")
    };
    Ok(normalized)
}

fn stringify_json(value: &Interchange) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".into())
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

fn row_to_workflow_status(row: SqliteRow) -> WorkflowStatus {
    let roles_json: Option<String> = row.get(5);
    let authenticated_roles = roles_json
        .and_then(|s| serde_json::from_str::<Vec<String>>(&s).ok())
        .filter(|v| !v.is_empty());

    let output_str: Option<String> = row.get(6);
    let output = output_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok());

    let input_str: Option<String> = row.get(19);
    let input = input_str.and_then(|s| serde_json::from_str::<Interchange>(&s).ok());

    let created_ms: i64 = row.get(9);
    let updated_ms: i64 = row.get(10);
    let timeout_ms: Option<i64> = row.get(15);
    let deadline_ms: Option<i64> = row.get(16);
    let started_ms: Option<i64> = row.get(17);
    let completed_ms: Option<i64> = row.get(25);
    let delay_ms: Option<i64> = row.get(29);
    let was_forked_from_num: Option<i64> = row.get(23);

    WorkflowStatus {
        id: row.get(0),
        status: parse_status(&row.get::<String, _>(1)),
        name: row.get(2),
        authenticated_user: row.get(3),
        assumed_role: row.get(4),
        authenticated_roles,
        output,
        error: row.get(7),
        executor_id: row.get(8),
        created_at: timestamp_ms(created_ms),
        updated_at: timestamp_ms(updated_ms),
        application_version: row.try_get::<String, _>(11).unwrap_or_default(),
        application_id: row.get(12),
        attempts: row.get::<i64, _>(13),
        queue_name: row.get(14),
        timeout: timeout_ms
            .filter(|&m| m > 0)
            .map(|m| Duration::from_millis(m as u64)),
        deadline: deadline_ms.map(timestamp_ms),
        started_at: started_ms.map(timestamp_ms),
        deduplication_id: row.get(18),
        input,
        priority: row.get(20),
        queue_partition_key: row.get(21),
        forked_from: row.get(22),
        was_forked_from: was_forked_from_num.unwrap_or(0) != 0,
        parent_workflow_id: row.get(24),
        completed_at: completed_ms.map(timestamp_ms),
        class_name: row.get(26),
        config_name: row.get(27),
        serialization: row.get(28),
        delay_until: delay_ms.map(timestamp_ms),
    }
}

fn row_to_workflow_schedule(row: SqliteRow) -> WorkflowSchedule {
    let context_str: String = row.get(6);
    let last_fired_at_str: Option<String> = row.get(7);
    WorkflowSchedule {
        schedule_id: row.get(0),
        schedule_name: row.get(1),
        workflow_name: row.get(2),
        workflow_class_name: row.get(3),
        schedule: row.get(4),
        status: parse_schedule_status(&row.get::<String, _>(5)),
        context: serde_json::from_str(&context_str).unwrap_or(Interchange::Null),
        last_fired_at: last_fired_at_str
            .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
            .map(|value| value.with_timezone(&Utc)),
        automatic_backfill: row.get(8),
        cron_timezone: row.get(9),
        queue_name: row.get(10),
    }
}

fn row_to_queue_config(row: SqliteRow) -> QueueConfig {
    QueueConfig {
        queue_id: row.get(0),
        name: row.get(1),
        concurrency: row.get(2),
        worker_concurrency: row.get(3),
        rate_limit_max: row.get(4),
        rate_limit_period_sec: row.get(5),
        priority_enabled: row.get::<i64, _>(6) != 0,
        partition_queue: row.get::<i64, _>(7) != 0,
        polling_interval_sec: row.get(8),
    }
}

/// Append a comma-separated, bind-parameter list of `values` to `qb`, with no
/// surrounding parentheses — the caller writes the `IN (` / `)`.
fn push_in_list<'a>(qb: &mut QueryBuilder<'a, Sqlite>, values: &'a [String]) {
    let mut separated = qb.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
}

/// Breadth-first gather of every descendant workflow id of `roots` (ported
/// from `getWorkflowChildren` in `system_database.go`). Includes the roots
/// themselves in the returned set.
async fn gather_descendants_sqlite(pool: &SqlitePool, roots: &[String]) -> DbosResult<Vec<String>> {
    let mut all: Vec<String> = roots.to_vec();
    let mut queue: Vec<String> = roots.to_vec();
    while let Some(parent) = queue.pop() {
        let mut qb =
            QueryBuilder::<Sqlite>::new("SELECT workflow_uuid FROM workflow_status WHERE parent_workflow_id = ");
        qb.push_bind(parent.clone());
        let rows = qb.build().fetch_all(pool).await.map_err(db_err)?;
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

#[cfg(test)]
mod tests {
    use super::normalize_sqlite_url;

    #[test]
    fn normalize_sqlite_url_supports_common_forms() {
        let cases = [
            ("sqlite:/tmp/x.db", "sqlite:///tmp/x.db"),
            ("sqlite:///tmp/x.db", "sqlite:///tmp/x.db"),
            ("sqlite::memory:", "sqlite::memory:"),
            ("sqlite:relative.db", "sqlite:relative.db"),
            ("sqlite3:relative.db", "sqlite:relative.db"),
            ("SQLITE:/tmp/x.db", "sqlite:///tmp/x.db"),
            (":memory:", "sqlite::memory:"),
        ];
        for (input, expected) in cases {
            assert_eq!(normalize_sqlite_url(input).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn normalize_sqlite_url_rejects_missing_path() {
        for input in ["sqlite:", "sqlite3:"] {
            assert!(normalize_sqlite_url(input).is_err(), "{input}");
        }
    }
}
