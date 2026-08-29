//! Journio Transact admin HTTP server — ported from `journio/admin_server.go` (618 LOC).
//!
//! An axum router over [`JournioContext`], exposing the same endpoints the Journio
//! Console expects: health check, workflow CRUD, steps, recovery, queue
//! metadata, global timeout, conductor status, and garbage-collect (stub).
//!
//! The server is started alongside the runtime when `Config.admin_server` is
//! set. It shares the same `Arc<JournioContext>`, so every handler is a thin
//! wrapper over the runtime's public methods.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use journio_core::JournioContext;
use tokio::net::TcpListener;

mod dto;

use dto::{
    ForkRequest, ForkResponse, GarbageCollectRequest, GlobalTimeoutRequest, HealthResponse,
    IndexResponse, ListWorkflowsRequest, QueueMetadataResponse, RegisteredWorkflow,
    StartWorkflowRequest, StartWorkflowResponse, StepResponse, WorkflowResponse, request_to_filter,
};

/// The admin HTTP server — ported from Go's `adminServer`.
pub struct AdminServer {
    ctx: Arc<JournioContext>,
    port: u16,
}

impl AdminServer {
    /// Construct over a running `JournioContext`.
    pub fn new(ctx: Arc<JournioContext>, port: u16) -> Self {
        Self { ctx, port }
    }

    /// Build the axum router — all endpoints match Go's URL patterns, plus
    /// the interactive extensions (`/workflows-registered`, `/workflows/{name}/start`)
    /// used by the Journio Console / a custom UI.
    pub fn router(&self) -> axum::Router {
        let ctx = self.ctx.clone();
        axum::Router::new()
            .route("/", get(index))
            .route("/journio-healthz", get(health))
            .route("/journio-workflow-recovery", post(recover_workflows))
            .route("/deactivate", get(deactivate))
            .route("/journio-workflow-queues-metadata", get(queue_metadata))
            .route("/journio-garbage-collect", post(garbage_collect))
            .route("/journio-global-timeout", post(global_timeout))
            .route("/queues", post(list_queued_workflows))
            .route("/workflows", post(list_workflows))
            .route("/workflows/registered", get(list_registered_workflows))
            .route("/workflows/{name}/start", post(start_workflow))
            .route("/workflows/{id}", get(get_workflow))
            .route("/workflows/{id}/steps", get(get_workflow_steps))
            .route("/workflows/{id}/cancel", post(cancel_workflow))
            .route("/workflows/{id}/resume", post(resume_workflow))
            .route("/workflows/{id}/fork", post(fork_workflow))
            .route("/conductor", get(conductor_status))
            .layer(tower_http::cors::CorsLayer::very_permissive())
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

/// `GET /journio-healthz`
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "healthy" })
}

/// `POST /journio-workflow-recovery` — recover workflows for executors.
async fn recover_workflows(
    State(ctx): State<Arc<JournioContext>>,
    Json(executor_ids): Json<Vec<String>>,
) -> Result<Json<Vec<String>>, AppError> {
    let ids = ctx.recover_workflows(&executor_ids).await?;
    Ok(Json(ids))
}

/// `GET /deactivate` — stop the scheduler (idempotent).
async fn deactivate(State(ctx): State<Arc<JournioContext>>) -> &'static str {
    // Cancel the runtime's token — scheduler/queue loops observe it and exit.
    ctx.deactivate();
    "deactivated"
}

/// `GET /journio-workflow-queues-metadata`
async fn queue_metadata(
    State(ctx): State<Arc<JournioContext>>,
) -> Result<Json<Vec<QueueMetadataResponse>>, AppError> {
    let queues = ctx.list_queue_metadata().await?;
    Ok(Json(queues.into_iter().map(Into::into).collect()))
}

/// `POST /journio-garbage-collect` — stub (Go marks it TODO too).
async fn garbage_collect(
    _state: State<Arc<JournioContext>>,
    Json(_req): Json<GarbageCollectRequest>,
) -> StatusCode {
    // TODO: implement GC (cutoff / rows threshold).
    StatusCode::NO_CONTENT
}

/// `POST /journio-global-timeout` — cancel all in-flight workflows before cutoff.
async fn global_timeout(
    State(ctx): State<Arc<JournioContext>>,
    Json(req): Json<GlobalTimeoutRequest>,
) -> Result<StatusCode, AppError> {
    let cutoff = chrono::DateTime::from_timestamp_millis(req.cutoff_epoch_timestamp_ms)
        .ok_or_else(|| AppError::bad("invalid cutoff_epoch_timestamp_ms"))?;
    ctx.cancel_all_before(cutoff).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows` — list workflows with filters.
async fn list_workflows(
    State(ctx): State<Arc<JournioContext>>,
    req: Option<Json<ListWorkflowsRequest>>,
) -> Result<Json<Vec<WorkflowResponse>>, AppError> {
    let filter = match req {
        Some(Json(r)) => request_to_filter(&r),
        None => journio_core::ListWorkflowsFilter::default(),
    };
    let workflows = ctx.list_workflows_filtered(&filter).await?;
    Ok(Json(workflows.into_iter().map(Into::into).collect()))
}

/// `POST /queues` — list queued workflows (ENQUEUED/PENDING/DELAYED + queues_only).
async fn list_queued_workflows(
    State(ctx): State<Arc<JournioContext>>,
    req: Option<Json<ListWorkflowsRequest>>,
) -> Result<Json<Vec<WorkflowResponse>>, AppError> {
    let mut filter = match req {
        Some(Json(r)) => request_to_filter(&r),
        None => journio_core::ListWorkflowsFilter::default(),
    };
    if filter.statuses.is_empty() {
        filter.statuses = vec![
            journio_core::WorkflowStatusType::Enqueued,
            journio_core::WorkflowStatusType::Pending,
            journio_core::WorkflowStatusType::Delayed,
        ];
    }
    filter.queues_only = true;
    let workflows = ctx.list_workflows_filtered(&filter).await?;
    Ok(Json(workflows.into_iter().map(Into::into).collect()))
}

/// `GET /workflows/{id}`
async fn get_workflow(
    State(ctx): State<Arc<JournioContext>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowResponse>, AppError> {
    let workflows = ctx
        .list_workflows_filtered(&journio_core::ListWorkflowsFilter {
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
    State(ctx): State<Arc<JournioContext>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<StepResponse>>, AppError> {
    let steps = ctx.get_workflow_steps(&id).await?;
    Ok(Json(steps.into_iter().map(Into::into).collect()))
}

/// `POST /workflows/{id}/cancel`
async fn cancel_workflow(
    State(ctx): State<Arc<JournioContext>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ctx.cancel_workflow(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows/{id}/resume`
async fn resume_workflow(
    State(ctx): State<Arc<JournioContext>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ctx.resume_workflow(&id, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /workflows/{id}/fork`
async fn fork_workflow(
    State(ctx): State<Arc<JournioContext>>,
    Path(id): Path<String>,
    Json(req): Json<ForkRequest>,
) -> Result<Json<ForkResponse>, AppError> {
    let options = journio_core::ForkWorkflowOptions {
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

/// `GET /` — service index (handy for browsers).
async fn index(State(ctx): State<Arc<JournioContext>>) -> Json<IndexResponse> {
    Json(IndexResponse {
        service: "journio-admin",
        app_name: ctx.config.app_name.clone(),
        admin_server_port: ctx.config.admin_server_port,
    })
}

/// `GET /workflows/registered` — names of workflows registered in this
/// process's registry (the in-memory set, not persisted state).
async fn list_registered_workflows(
    State(ctx): State<Arc<JournioContext>>,
) -> Json<Vec<RegisteredWorkflow>> {
    let names = ctx.registry.list();
    Json(
        names
            .into_iter()
            .map(|name| RegisteredWorkflow { name })
            .collect(),
    )
}

/// `POST /workflows/{name}/start` — launch a registered workflow by name.
/// With `queue_name` set, enqueues for deferred execution; otherwise runs
/// immediately.
async fn start_workflow(
    State(ctx): State<Arc<JournioContext>>,
    Path(name): Path<String>,
    req: Option<Json<StartWorkflowRequest>>,
) -> Result<Json<StartWorkflowResponse>, AppError> {
    let req = req.map(|Json(r)| r).unwrap_or_default();
    let input = req.input;

    let handle = if let Some(queue) = req.queue_name.as_deref() {
        ctx.enqueue_workflow(
            queue,
            &name,
            input,
            journio_core::EnqueueOptions {
                workflow_id: req.workflow_id,
                ..Default::default()
            },
        )
        .await?
    } else {
        let opts = journio_core::context::EnqueueOptions {
            workflow_id: req.workflow_id,
            ..Default::default()
        };
        // run_workflow doesn't take options; start directly and rely on the
        // runtime to assign an id when workflow_id is unset.
        let _ = opts;
        ctx.run_workflow(&name, input).await?
    };
    Ok(Json(StartWorkflowResponse {
        workflow_id: handle.workflow_id().to_string(),
    }))
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

impl From<journio_core::JournioError> for AppError {
    fn from(e: journio_core::JournioError) -> Self {
        use journio_core::JournioErrorCode as Code;
        // Input-deserialization / validation failures are client errors
        // (the caller sent a bad payload), not server errors. Map them to
        // 400 so the UI surfaces a helpful message instead of a 500.
        let status = match e.code {
            Code::WorkflowUnexpectedTypeError
            | Code::ConflictingIDError
            | Code::ConflictingWorkflowError
            | Code::InitializationError
            | Code::PatchingNotEnabled => StatusCode::BAD_REQUEST,
            Code::NonExistentWorkflowError => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: e.message,
        }
    }
}

/// Error response body — JSON so the UI can parse a structured message.
#[derive(serde::Serialize)]
struct ErrorBody {
    message: String,
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(ErrorBody {
                message: self.message,
            }),
        )
            .into_response()
    }
}
