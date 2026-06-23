//! `journio workflow` subcommands — ported from `cmd/journio/workflow.go`.
//!
//! Each command is a pure function that returns its result; `main.rs` does the
//! JSON printing. This keeps the logic unit-testable without a process
//! boundary.

use chrono::{DateTime, Utc};
use journio_core::{
    Client, ForkWorkflowOptions, ListWorkflowsFilter, StepRecord, WorkflowHandle,
    WorkflowStatus, WorkflowStatusType,
};
use std::time::Duration;

/// Options parsed from CLI flags for `workflow list` — ported from the
/// `workflowListCmd` flag set.
#[derive(Debug, Default, Clone)]
pub struct ListOptions {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub user: Option<String>,
    pub name: Option<String>,
    pub status: Option<WorkflowStatusType>,
    pub application_version: Option<String>,
    pub queue: Option<String>,
    pub queues_only: bool,
    pub sort_desc: bool,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
}

/// `workflow list` — returns workflows matching the filter.
pub async fn list(
    client: &Client,
    opts: ListOptions,
) -> Result<Vec<WorkflowStatus>, String> {
    let mut filter = ListWorkflowsFilter {
        limit: opts.limit,
        offset: opts.offset,
        sort_desc: opts.sort_desc,
        ..Default::default()
    };
    if let Some(user) = opts.user {
        filter.authenticated_users = vec![user];
    }
    if let Some(name) = opts.name {
        filter.names = vec![name];
    }
    if let Some(status) = opts.status {
        filter.statuses = vec![status];
    }
    if let Some(version) = opts.application_version {
        filter.application_versions = vec![version];
    }
    if let Some(queue) = opts.queue {
        filter.queue_names = vec![queue];
    }
    filter.queues_only = opts.queues_only;
    filter.start_time = opts.start_time;
    filter.end_time = opts.end_time;

    client.list_workflows(filter).await.map_err(|e| e.to_string())
}

/// `workflow get <id>` — returns a single workflow's status, or an error if
/// not found.
pub async fn get(client: &Client, workflow_id: &str) -> Result<WorkflowStatus, String> {
    let workflows = client
        .list_workflows(ListWorkflowsFilter {
            workflow_ids: vec![workflow_id.to_string()],
            ..Default::default()
        })
        .await
        .map_err(|e| format!("failed to retrieve workflow: {e}"))?;
    workflows
        .into_iter()
        .next()
        .ok_or_else(|| format!("workflow not found: {workflow_id}"))
}

/// `workflow steps <id>` — returns the steps of a workflow.
pub async fn steps(client: &Client, workflow_id: &str) -> Result<Vec<StepRecord>, String> {
    client
        .get_workflow_steps(workflow_id)
        .await
        .map_err(|e| format!("failed to get workflow steps: {e}"))
}

/// `workflow cancel <id>` — cancel a workflow.
pub async fn cancel(client: &Client, workflow_id: &str) -> Result<(), String> {
    client
        .cancel_workflow(workflow_id)
        .await
        .map_err(|e| e.to_string())?;
    crate::output::info(&format!("Successfully cancelled workflow {workflow_id}"));
    Ok(())
}

/// `workflow resume <id>` — resume a cancelled workflow, returning its status.
pub async fn resume(
    client: &std::sync::Arc<Client>,
    workflow_id: &str,
) -> Result<WorkflowStatus, String> {
    client
        .resume_workflow(workflow_id, None)
        .await
        .map_err(|e| e.to_string())?;
    client
        .retrieve_workflow(workflow_id)
        .get_status()
        .await
        .map_err(|e| format!("failed to get workflow status: {e}"))
}

/// `workflow fork <id>` — fork a workflow from a step, returning the forked
/// workflow's status.
pub async fn fork(
    client: &std::sync::Arc<Client>,
    workflow_id: &str,
    start_step: u32,
    application_version: Option<String>,
    forked_workflow_id: Option<String>,
) -> Result<WorkflowStatus, String> {
    let options = ForkWorkflowOptions {
        workflow_id: forked_workflow_id,
        start_step,
        application_version,
        ..Default::default()
    };
    let handle: WorkflowHandle = client
        .fork_workflow(workflow_id, options)
        .await
        .map_err(|e| e.to_string())?;
    handle
        .get_status()
        .await
        .map_err(|e| format!("failed to get forked workflow status: {e}"))
}

/// `workflow delete <id...>` — permanently delete workflows.
pub async fn delete(client: &Client, ids: &[String], children: bool) -> Result<(), String> {
    client
        .delete_workflows(ids, children)
        .await
        .map_err(|e| e.to_string())?;
    crate::output::info(&format!("Successfully deleted workflow(s): {}", ids.join(", ")));
    Ok(())
}

/// Parse a status string from a CLI flag (case-insensitive, matches Go).
pub fn parse_status(s: &str) -> Result<WorkflowStatusType, String> {
    Ok(match s.to_ascii_uppercase().as_str() {
        "PENDING" => WorkflowStatusType::Pending,
        "SUCCESS" => WorkflowStatusType::Success,
        "ERROR" => WorkflowStatusType::Error,
        "ENQUEUED" => WorkflowStatusType::Enqueued,
        "CANCELLED" => WorkflowStatusType::Cancelled,
        "MAX_RECOVERY_ATTEMPTS_EXCEEDED" => WorkflowStatusType::MaxRecoveryAttemptsExceeded,
        other => return Err(format!("invalid status: {other}")),
    })
}

/// Parse an RFC 3339 timestamp from a CLI flag.
pub fn parse_timestamp(s: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("invalid timestamp {s:?} (expected ISO 8601 / RFC 3339): {e}"))
}

/// Default timeout for workflow-status polling (unused by the read-only
/// subcommands, kept for parity with future `wait` commands).
#[allow(dead_code)]
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
