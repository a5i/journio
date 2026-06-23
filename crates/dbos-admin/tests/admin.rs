//! Admin server integration tests — exercise every endpoint over a real
//! SQLite-backed `DbosContext` with the admin server running on an ephemeral
//! port. Mirrors the Go `TestAdminServer` cases in `admin_server_test.go`.

use std::sync::Arc;

use dbos_admin::AdminServer;
use dbos_core::{
    Config, DbosContext, EnqueueOptions, QueueOptions, SystemDatabase,
    WorkflowStatusType,
};
use dbos_sqlite::SqliteSystemDatabase;

fn temp_db_url() -> String {
    let db_path = std::env::temp_dir().join(format!(
        "dbos-admin-test-{}.db",
        uuid::Uuid::new_v4()
    ));
    format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"))
}

async fn setup() -> (Arc<DbosContext>, String) {
    let url = temp_db_url();
    let db = SqliteSystemDatabase::connect(&url).await.expect("connect");
    db.migrate().await.expect("migrate");
    let db = Arc::new(db);

    let mut config = Config::default();
    config.app_name = "admin-test".to_string();
    config.system_db = Some(db);
    let ctx = DbosContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    // Start the admin server on an ephemeral port.
    let server = AdminServer::new(ctx.clone(), 0);
    let addr = server.start().await.expect("start");
    let base = format!("http://{}", addr);
    (ctx, base)
}

async fn seed_enqueued(ctx: &Arc<DbosContext>, id: &str, name: &str) {
    ctx.enqueue_workflow("q", name, serde_json::json!(null), EnqueueOptions {
        workflow_id: Some(id.to_string()),
        ..Default::default()
    })
    .await
    .expect("enqueue");
}

#[tokio::test]
async fn health_returns_healthy() {
    let (_ctx, base) = setup().await;
    let resp: serde_json::Value = reqwest::get(format!("{base}/dbos-healthz"))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["status"], "healthy");
}

#[tokio::test]
async fn list_workflows_returns_enqueued_workflow() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-1", "test-wf").await;

    let client = reqwest::Client::new();
    let resp: Vec<serde_json::Value> = client
        .post(format!("{base}/workflows"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("send")
        .json()
        .await
        .expect("json");
    assert_eq!(resp.len(), 1);
    assert_eq!(resp[0]["workflow_uuid"], "wf-1");
    assert_eq!(resp[0]["status"], "ENQUEUED");
}

#[tokio::test]
async fn get_workflow_returns_404_for_missing() {
    let (_ctx, base) = setup().await;
    let status = reqwest::get(format!("{base}/workflows/nonexistent"))
        .await
        .expect("request")
        .status();
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_workflow_returns_data_for_existing() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-get", "test-wf").await;

    let resp: serde_json::Value = reqwest::get(format!("{base}/workflows/wf-get"))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["workflow_uuid"], "wf-get");
    assert_eq!(resp["workflow_name"], "test-wf");
}

#[tokio::test]
async fn cancel_and_resume_workflow() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-cr", "test-wf").await;
    let client = reqwest::Client::new();

    // Cancel.
    let status = client
        .post(format!("{base}/workflows/wf-cr/cancel"))
        .send()
        .await
        .expect("cancel")
        .status();
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);

    let status = ctx
        .system_db()
        .get_workflow_status("wf-cr")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.status, WorkflowStatusType::Cancelled);

    // Resume.
    let status = client
        .post(format!("{base}/workflows/wf-cr/resume"))
        .send()
        .await
        .expect("resume")
        .status();
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn fork_workflow_returns_new_id() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-orig", "test-wf").await;
    let backend = ctx.system_db();
    backend
        .record_workflow_result("wf-orig", WorkflowStatusType::Success, Some(&serde_json::json!(1)), None)
        .await
        .expect("result");
    backend
        .record_step_output(&dbos_core::StepRecord {
            workflow_uuid: "wf-orig".to_string(),
            function_id: 1,
            function_name: "s1".to_string(),
            output: Some("42".to_string()),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("step");

    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .post(format!("{base}/workflows/wf-orig/fork"))
        .json(&serde_json::json!({"start_step": 1, "new_workflow_id": "wf-forked"}))
        .send()
        .await
        .expect("fork")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["workflow_id"], "wf-forked");
}

#[tokio::test]
async fn list_queued_workflows_filters_correctly() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-q1", "test-wf").await;
    seed_enqueued(&ctx, "wf-q2", "test-wf").await;

    let client = reqwest::Client::new();
    let resp: Vec<serde_json::Value> = client
        .post(format!("{base}/queues"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("send")
        .json()
        .await
        .expect("json");
    assert_eq!(resp.len(), 2);
}

#[tokio::test]
async fn queue_metadata_lists_registered_queues() {
    let (ctx, base) = setup().await;
    ctx.register_queue("test-queue", QueueOptions {
        concurrency: Some(5),
        ..Default::default()
    })
    .await
    .expect("register queue");

    let resp: Vec<serde_json::Value> = reqwest::get(format!(
        "{base}/dbos-workflow-queues-metadata"
    ))
    .await
    .expect("request")
    .json()
    .await
    .expect("json");
    assert!(resp.iter().any(|q| q["name"] == "test-queue"));
}

#[tokio::test]
async fn global_timeout_cancels_workflows_before_cutoff() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-gt", "test-wf").await;

    let client = reqwest::Client::new();
    let now_ms = chrono::Utc::now().timestamp_millis() + 60000; // future cutoff
    let status = client
        .post(format!("{base}/dbos-global-timeout"))
        .json(&serde_json::json!({"cutoff_epoch_timestamp_ms": now_ms}))
        .send()
        .await
        .expect("send")
        .status();
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);

    let status = ctx
        .system_db()
        .get_workflow_status("wf-gt")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.status, WorkflowStatusType::Cancelled);
}

#[tokio::test]
async fn conductor_status_returns_true() {
    let (_ctx, base) = setup().await;
    let resp: serde_json::Value = reqwest::get(format!("{base}/conductor"))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["status"], true);
}

#[tokio::test]
async fn deactivate_returns_deactivated() {
    let (_ctx, base) = setup().await;
    let resp = reqwest::get(format!("{base}/deactivate"))
        .await
        .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body = resp.text().await.expect("body");
    assert_eq!(body, "deactivated");
}

#[tokio::test]
async fn workflow_steps_endpoint_returns_steps() {
    let (ctx, base) = setup().await;
    seed_enqueued(&ctx, "wf-steps", "test-wf").await;
    ctx.system_db()
        .record_step_output(&dbos_core::StepRecord {
            workflow_uuid: "wf-steps".to_string(),
            function_id: 1,
            function_name: "first-step".to_string(),
            output: Some("99".to_string()),
            error: None,
            child_workflow_id: None,
        })
        .await
        .expect("record step");

    let resp: Vec<serde_json::Value> = reqwest::get(format!(
        "{base}/workflows/wf-steps/steps"
    ))
    .await
    .expect("request")
    .json()
    .await
    .expect("json");
    assert_eq!(resp.len(), 1);
    assert_eq!(resp[0]["function_name"], "first-step");
    assert_eq!(resp[0]["function_id"], 1);
}

#[tokio::test]
async fn registered_workflows_endpoint_lists_registry() {
    use dbos_core::WorkflowFn;

    let url = temp_db_url();
    let db = SqliteSystemDatabase::connect(&url).await.expect("connect");
    db.migrate().await.expect("migrate");
    let db = Arc::new(db);

    let mut config = Config::default();
    config.app_name = "registry-test".to_string();
    config.system_db = Some(db);
    let ctx = DbosContext::new(config).await.expect("context");
    ctx.register_workflow(Arc::new(WorkflowFn::new("alpha", |_ctx, _i: i64| {
        Box::pin(async move { Ok(0) })
    })))
    .expect("register");
    ctx.register_workflow(Arc::new(WorkflowFn::new("beta", |_ctx, _i: i64| {
        Box::pin(async move { Ok(0) })
    })))
    .expect("register");
    ctx.launch().await.expect("launch");

    let server = AdminServer::new(ctx.clone(), 0);
    let addr = server.start().await.expect("start");
    let base = format!("http://{}", addr);

    let resp: Vec<serde_json::Value> = reqwest::get(format!("{base}/workflows/registered"))
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    let names: Vec<&str> = resp.iter().map(|v| v["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha"));
    assert!(names.contains(&"beta"));
}

#[tokio::test]
async fn start_workflow_endpoint_launches_immediately() {
    let (ctx, base) = setup().await;

    // Register a workflow so start has a target.
    use dbos_core::WorkflowFn;
    ctx.register_workflow(Arc::new(WorkflowFn::new("instant", |_ctx, n: i64| {
        Box::pin(async move { Ok(n + 1) })
    })))
    .expect("register");

    let client = reqwest::Client::new();
    let resp: serde_json::Value = client
        .post(format!("{base}/workflows/instant/start"))
        .json(&serde_json::json!({"input": 41}))
        .send()
        .await
        .expect("start")
        .json()
        .await
        .expect("json");
    let id = resp["workflow_id"].as_str().expect("workflow_id");
    assert!(!id.is_empty());

    // The workflow ran immediately; verify it shows up as SUCCESS.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let workflows: Vec<serde_json::Value> = client
        .post(format!("{base}/workflows"))
        .json(&serde_json::json!({"workflow_name": "instant"}))
        .send()
        .await
        .expect("list")
        .json()
        .await
        .expect("json");
    assert!(workflows.iter().any(|w| w["workflow_uuid"] == id));
    assert!(
        workflows
            .iter()
            .any(|w| w["status"] == "SUCCESS" && w["workflow_uuid"] == id)
    );
}

#[tokio::test]
async fn index_endpoint_returns_service_info() {
    let (_ctx, base) = setup().await;
    let resp: serde_json::Value = reqwest::get(&base)
        .await
        .expect("request")
        .json()
        .await
        .expect("json");
    assert_eq!(resp["service"], "dbos-admin");
    assert_eq!(resp["app_name"], "admin-test");
}
