//! Response DTOs for the admin server — ported from Go's
//! `toListWorkflowResponse` / step formatting in `admin_server.go`.
//!
//! These use PascalCase field names and epoch-millisecond timestamp strings
//! because the DBOS Console expects that exact shape. The `Input`/`Output`
//! fields carry raw JSON strings (the serialized interchange value), and
//! `Error` is a JSON-encoded string (Go marshals it via `json.Marshal`).

use chrono::{DateTime, Utc};
use dbos_core::{QueueConfig, StepRecord, WorkflowStatus};
use serde::{Deserialize, Serialize};

/// Format a datetime as an epoch-millisecond string, or `None` when zero.
/// Matches Go's `formatEpochMs`.
fn epoch_ms(t: Option<DateTime<Utc>>) -> Option<String> {
    t.map(|time| time.timestamp_millis().to_string())
}

/// Single workflow row — matches Go's `toListWorkflowResponse` map.
#[derive(Debug, Serialize)]
pub struct WorkflowResponse {
    pub workflow_uuid: String,
    pub status: String,
    pub workflow_name: String,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<Vec<String>>,
    pub output: String,
    pub executor_id: String,
    pub application_version: String,
    pub application_id: Option<String>,
    pub attempts: i64,
    pub queue_name: Option<String>,
    pub timeout: Option<serde_json::Value>,
    pub deduplication_id: Option<String>,
    pub priority: i32,
    pub queue_partition_key: Option<String>,
    pub input: String,
    pub error: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub workflow_deadline_epoch_ms: Option<String>,
    pub started_at: Option<String>,
}

impl From<WorkflowStatus> for WorkflowResponse {
    fn from(ws: WorkflowStatus) -> Self {
        // Serialize input/output to JSON strings (the raw interchange payload).
        let input_str = ws
            .input
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();
        let output_str = ws
            .output
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_default();

        // Error is double-encoded (JSON-string-of-string) to match Go.
        let error_str = ws
            .error
            .as_ref()
            .map(|e| serde_json::to_string(e).unwrap_or_else(|_| "\"\"".into()))
            .unwrap_or_default();

        Self {
            workflow_uuid: ws.id,
            status: format!("{:?}", ws.status).to_uppercase(),
            workflow_name: ws.name,
            authenticated_user: ws.authenticated_user,
            assumed_role: ws.assumed_role,
            authenticated_roles: ws.authenticated_roles,
            output: output_str,
            executor_id: ws.executor_id,
            application_version: ws.application_version,
            application_id: ws.application_id,
            attempts: ws.attempts,
            queue_name: ws.queue_name,
            timeout: ws.timeout.as_ref().map(|d| serde_json::json!(d.as_millis())),
            deduplication_id: ws.deduplication_id,
            priority: ws.priority,
            queue_partition_key: ws.queue_partition_key,
            input: input_str,
            error: error_str,
            created_at: epoch_ms(Some(ws.created_at)),
            updated_at: epoch_ms(Some(ws.updated_at)),
            workflow_deadline_epoch_ms: epoch_ms(ws.deadline),
            started_at: epoch_ms(ws.started_at),
        }
    }
}

/// Single step row — matches Go's step formatting in the steps handler.
#[derive(Debug, Serialize)]
pub struct StepResponse {
    pub function_id: i32,
    pub function_name: String,
    pub output: String,
    pub error: Option<String>,
    pub child_workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_epoch_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_epoch_ms: Option<i64>,
}

impl From<StepRecord> for StepResponse {
    fn from(step: StepRecord) -> Self {
        let output = step.output.unwrap_or_default();
        let error = step.error.map(|e| serde_json::to_string(&e).unwrap_or_else(|_| "\"\"".into()));
        Self {
            function_id: step.function_id,
            function_name: step.function_name,
            output,
            error,
            child_workflow_id: step.child_workflow_id,
            started_at_epoch_ms: None,
            completed_at_epoch_ms: None,
        }
    }
}

/// Queue metadata row — matches Go's `WorkflowQueue` JSON tags.
#[derive(Debug, Serialize)]
pub struct QueueMetadataResponse {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_concurrency: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_queue: Option<bool>,
}

impl From<QueueConfig> for QueueMetadataResponse {
    fn from(q: QueueConfig) -> Self {
        Self {
            name: q.name,
            concurrency: q.concurrency,
            worker_concurrency: q.worker_concurrency,
            priority_enabled: if q.priority_enabled { Some(true) } else { None },
            partition_queue: if q.partition_queue { Some(true) } else { None },
        }
    }
}

/// Health-check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

/// Fork response.
#[derive(Debug, Serialize)]
pub struct ForkResponse {
    pub workflow_id: String,
}

/// Request bodies.

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct ListWorkflowsRequest {
    #[serde(default)]
    pub workflow_uuids: Vec<String>,
    pub authenticated_user: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub status: String,
    pub application_version: Option<String>,
    pub workflow_name: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub sort_desc: Option<bool>,
    pub workflow_id_prefix: Option<String>,
    pub load_input: Option<bool>,
    pub load_output: Option<bool>,
    pub queue_name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GlobalTimeoutRequest {
    pub cutoff_epoch_timestamp_ms: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct GarbageCollectRequest {
    pub cutoff_epoch_timestamp_ms: Option<i64>,
    pub rows_threshold: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ForkRequest {
    pub start_step: Option<u32>,
    pub new_workflow_id: Option<String>,
    pub application_version: Option<String>,
}

/// Parse a status string (case-insensitive, matching Go's `WorkflowStatusType(s)`).
fn parse_status(s: &str) -> Option<dbos_core::WorkflowStatusType> {
    use dbos_core::WorkflowStatusType::*;
    Some(match s.to_ascii_uppercase().as_str() {
        "PENDING" => Pending,
        "SUCCESS" => Success,
        "ERROR" => Error,
        "ENQUEUED" => Enqueued,
        "CANCELLED" => Cancelled,
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => MaxRecoveryAttemptsExceeded,
        _ => return None,
    })
}

/// Convert a `ListWorkflowsRequest` into a core `ListWorkflowsFilter`.
pub fn request_to_filter(req: &ListWorkflowsRequest) -> dbos_core::ListWorkflowsFilter {
    dbos_core::ListWorkflowsFilter {
        workflow_ids: req.workflow_uuids.clone(),
        workflow_id_prefixes: req
            .workflow_id_prefix
            .as_deref()
            .map(|p| vec![p.to_string()])
            .unwrap_or_default(),
        statuses: if req.status.is_empty() {
            Vec::new()
        } else {
            parse_status(&req.status)
                .map(|s| vec![s])
                .unwrap_or_default()
        },
        names: req.workflow_name.as_deref().map(|n| vec![n.to_string()]).unwrap_or_default(),
        application_versions: req
            .application_version
            .as_deref()
            .map(|v| vec![v.to_string()])
            .unwrap_or_default(),
        queue_names: req.queue_name.as_deref().map(|q| vec![q.to_string()]).unwrap_or_default(),
        authenticated_users: req
            .authenticated_user
            .as_deref()
            .map(|u| vec![u.to_string()])
            .unwrap_or_default(),
        limit: req.limit,
        offset: req.offset,
        sort_desc: req.sort_desc.unwrap_or(false),
        load_input: req.load_input.unwrap_or(false),
        load_output: req.load_output.unwrap_or(false),
        ..Default::default()
    }
}
