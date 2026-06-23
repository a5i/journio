//! DBOS Transact admin HTTP server — ported from `dbos/admin_server.go` (618 LOC).
//!
//! An axum router over [`DbosContext`], exposing the same endpoints the DBOS
//! Console expects: health check, workflow CRUD, steps, recovery, queue
//! metadata, global timeout, conductor status, and garbage-collect (stub).
//!
//! The server is started alongside the runtime when `Config.admin_server` is
//! set. It shares the same `Arc<DbosContext>`, so every handler is a thin
//! wrapper over the runtime's public methods.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use dbos_core::DbosContext;
use tokio::net::TcpListener;
use tracing;

mod dto;

use dto::{
    request_to_filter, ForkRequest, ForkResponse, GarbageCollectRequest, GlobalTimeoutRequest,
    HealthResponse, ListWorkflowsRequest, QueueMetadataResponse, StepResponse,
    WorkflowResponse,
};

/// The admin HTTP server — ported from Go's `adminServer`.
pub struct AdminServer {
    ctx: Arc<DbosContext>,
    port: u16,
}

impl AdminServer {
    /// Construct over a running `DbosContext`.
    pub fn new(ctx: Arc<DbosContext>, port: u16) -> Self {
        Self { ctx, port }
    }

    /// Build the axum router — all endpoints match Go's URL patterns.
    pub fn router(&self) -> axum::Router {
        let ctx = self.ctx.clone();
        axum::Router::new()
            .route("/dbos-healthz", get(health))
            .route("/dbos-workflow-recovery", post(recover_workflows))
            .route("/deactivate", get(deactivate))
            .route("/dbos-workflow-queues-metadata", get(queue_metadata))
            .route("/dbos-garbage-collect", post(garbage_collect))
            .route("/dbos-global-timeout", post(global_timeout))
            .route("/queues", post(list_queued_workflows))
            .route("/workflows", post(list_workflows))
            .route("/workflows/{id}", get(get_workflow))
            .route("/workflows/{id}/steps", get(get_workflow_steps))
            .route("/workflows/{id}/cancel", post(cancel_workflow))
            .route("/workflows/{id}/resume", post(resume_workflow))
            .route("/workflows/{id}/fork", post(fork_workflow))
            .route("/conductor", get(conductor_status))
            .with_state(ctx)
    }

    /// Start the server (non-blocking). Returns the bound address so the
    /// caller knows where it's listening. Ported from Go's `Start`.
    pub async fn start(&self) -> Result<SocketAddr, std::io::Error> {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let listener = TcpListener::bind(addr).await?;
        let bound = listener.local_addr()?;
        let router = self.router();
        tokio::spawn(async move {
            let serve = axum::serve(listener, router);
            if let Err(e) = serve.await {
                tracing::error!(error = %e, "admin server error");
            }
        });
        tracing::info!(addr = %bound, "admin server started");
        Ok(bound)
    }
}

// ---------------------------------------------------------------------------
// Handlers — each mirrors a Go mux.HandleFunc closure.
// ---------------------------------------------------------------------------

/// `GET /dbos-healthz`
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "healthy" })
}

/// `POST /dbos-workflow-recovery` — recover workflows for executors.
async fn recover_workflows(
    State(ctx): State<Arc<DbosContext>>,
    Json(executor_ids): Json<Vec<String>>,
) -> Result<Json<Vec<String>>, AppError> {
    let ids = ctx.recover_workflows(&executor_ids).await?;
    Ok(Json(ids))
}

/// `GET /deactivate` — stop the scheduler (idempotent).
async fn deactivate(State(ctx): State<Arc<DbosContext>>) -> &'static str {
    // Cancel the runtime's token — scheduler/queue loops observe it and exit.
    ctx.deactivate();
    "deactivated"
}

/// `GET /dbos-workflow-queues-metadata`
async fn queue_metadata(
    State(ctx): State<Arc<DbosContext>>,
) -> Result<Json<Vec<QueueMetadataResponse>>, AppError> {
    let queues = ctx.list_queue_metadata().await?;
    Ok(Json(queues.into_iter().map(Into::into).collect()))
}

/// `POST /dbos-garbage-collect` — stub (Go marks it TODO too).
async fn garbage_collect(
    _state: State<Arc<DbosContext>>,
    Json(_req): Json<GarbageCollectRequest>,
) -> StatusCode {
    // TODO: implement GC (cutoff / rows threshold).
    StatusCode::NO_CONTENT
}

/// `POST /dbos-global-timeout` — cancel all in-flight workflows before cutoff.
async fn global_timeout(
    State(ctx): State<Arc<DbosContext>>,
    Json(req): Json<GlobalTimeoutRequest>,
) -> Result<StatusCode, AppError> {
    let cutoff = chrono::DateTime::from_timestamp_millis(req.cutoff_epoch_timestamp_ms)
        .ok_or_else(|| AppError::bad("invalid cutoff_epoch_timestamp_ms"))?;
    ctx.cancel_all_before(cutoff).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows` — list workflows with filters.
async fn list_workflows(
    State(ctx): State<Arc<DbosContext>>,
    req: Option<Json<ListWorkflowsRequest>>,
) -> Result<Json<Vec<WorkflowResponse>>, AppError> {
    let filter = match req {
        Some(Json(r)) => request_to_filter(&r),
        None => dbos_core::ListWorkflowsFilter::default(),
    };
    let workflows = ctx.list_workflows_filtered(&filter).await?;
    Ok(Json(workflows.into_iter().map(Into::into).collect()))
}

/// `POST /queues` — list queued workflows (ENQUEUED/PENDING/DELAYED + queues_only).
async fn list_queued_workflows(
    State(ctx): State<Arc<DbosContext>>,
    req: Option<Json<ListWorkflowsRequest>>,
) -> Result<Json<Vec<WorkflowResponse>>, AppError> {
    let mut filter = match req {
        Some(Json(r)) => request_to_filter(&r),
        None => dbos_core::ListWorkflowsFilter::default(),
    };
    if filter.statuses.is_empty() {
        filter.statuses = vec![
            dbos_core::WorkflowStatusType::Enqueued,
            dbos_core::WorkflowStatusType::Pending,
            dbos_core::WorkflowStatusType::Delayed,
        ];
    }
    filter.queues_only = true;
    let workflows = ctx.list_workflows_filtered(&filter).await?;
    Ok(Json(workflows.into_iter().map(Into::into).collect()))
}

/// `GET /workflows/{id}`
async fn get_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowResponse>, AppError> {
    let workflows = ctx
        .list_workflows_filtered(&dbos_core::ListWorkflowsFilter {
            workflow_ids: vec![id.clone()],
            ..Default::default()
        })
        .await?;
    let Some(wf) = workflows.into_iter().next() else {
        return Err(AppError::not_found("workflow not found"));
    };
    Ok(Json(wf.into()))
}

/// `GET /workflows/{id}/steps`
async fn get_workflow_steps(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<StepResponse>>, AppError> {
    let steps = ctx.get_workflow_steps(&id).await?;
    Ok(Json(steps.into_iter().map(Into::into).collect()))
}

/// `POST /workflows/{id}/cancel`
async fn cancel_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ctx.cancel_workflow(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows/{id}/resume`
async fn resume_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ctx.resume_workflow(&id, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows/{id}/fork`
async fn fork_workflow(
    State(ctx): State<Arc<DbosContext>>,
    Path(id): Path<String>,
    Json(req): Json<ForkRequest>,
) -> Result<Json<ForkResponse>, AppError> {
    let options = dbos_core::ForkWorkflowOptions {
        start_step: req.start_step.unwrap_or(1),
        workflow_id: req.new_workflow_id,
        application_version: req.application_version,
        ..Default::default()
    };
    let handle = ctx.fork_workflow(&id, options).await?;
    Ok(Json(ForkResponse {
        workflow_id: handle.workflow_id().to_string(),
    }))
}

/// `GET /conductor`
async fn conductor_status() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": true}))
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

/// Error wrapper that maps to HTTP status codes.
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn bad(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    fn not_found(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: msg.into(),
        }
    }
}

impl From<dbos_core::DbosError> for AppError {
    fn from(e: dbos_core::DbosError) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}
