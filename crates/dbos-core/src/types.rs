//! Shared record types — ported from `dbos/workflow.go` (status, steps) and
//! `dbos/system_database.go` row mappings.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::value::Interchange;

/// Mirrors `WorkflowStatusType` in `dbos/workflow.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum WorkflowStatusType {
    Pending,
    Enqueued,
    Delayed,
    Success,
    Error,
    Cancelled,
    MaxRecoveryAttemptsExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleStatus {
    #[serde(rename = "ACTIVE")]
    Active,
    #[serde(rename = "PAUSED")]
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSchedule {
    pub schedule_id: String,
    pub schedule_name: String,
    pub workflow_name: String,
    pub workflow_class_name: Option<String>,
    pub schedule: String,
    pub status: ScheduleStatus,
    pub context: Interchange,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub automatic_backfill: bool,
    pub cron_timezone: Option<String>,
    pub queue_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledWorkflowInput {
    pub scheduled_time: DateTime<Utc>,
    #[serde(skip_serializing_if = "Interchange::is_null", default)]
    pub context: Interchange,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub value: String,
    pub offset: i64,
    pub serialization: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub queue_id: String,
    pub name: String,
    pub concurrency: Option<i32>,
    pub worker_concurrency: Option<i32>,
    pub rate_limit_max: Option<i32>,
    pub rate_limit_period_sec: Option<f64>,
    pub priority_enabled: bool,
    pub partition_queue: bool,
    pub polling_interval_sec: f64,
}

/// Mirrors `WorkflowStatus` in `dbos/workflow.go`. Column-for-column with
/// `workflow_status` in `migrations/1_initial_dbos_schema.sql`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatus {
    pub id: String, // workflow_uuid
    pub status: WorkflowStatusType,
    pub name: String,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<Vec<String>>,
    pub output: Option<Interchange>,
    pub error: Option<String>,
    pub executor_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub application_version: String,
    pub application_id: Option<String>,
    pub attempts: i64,
    pub queue_name: Option<String>,
    pub timeout: Option<Duration>,
    pub deadline: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub deduplication_id: Option<String>,
    pub input: Option<Interchange>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub forked_from: Option<String>,
    pub was_forked_from: bool,
    pub parent_workflow_id: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
    pub serialization: Option<String>,
    pub delay_until: Option<DateTime<Utc>>,
}

/// A recorded step — column-for-column with `operation_outputs`.
/// Mirrors `StepInfo` / the operation_outputs row in Go.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    pub workflow_uuid: String,
    pub function_id: i32,
    pub function_name: String,
    pub output: Option<String>,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
}
