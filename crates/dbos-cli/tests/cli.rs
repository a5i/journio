//! CLI integration tests — exercise the command functions over a real SQLite
//! backend, mirroring the user-facing flows of `cmd/dbos/cli_integration_test.go`.
//!
//! The command functions are pure (return data; `main.rs` does the printing),
//! so these call them directly and assert on the returned values.

use std::sync::Arc;

use dbos_core::{
    Client, Config, EnqueueOptions, SystemDatabase, WorkflowStatusType,
};
use dbos_sqlite::SqliteSystemDatabase;

fn temp_db_url() -> String {
    let db_path = std::env::temp_dir().join(format!(
        "dbos-cli-test-{}.db",
        uuid::Uuid::new_v4()
    ));
    format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"))
}

/// Returns (url, backend, client). The backend is returned so tests can seed
/// terminal statuses and steps directly (the Client only enqueues).
async fn setup() -> (String, Arc<SqliteSystemDatabase>, Arc<Client>) {
    let url = temp_db_url();
    let db = SqliteSystemDatabase::connect(&url).await.expect("connect");
    db.migrate().await.expect("migrate");
    let db = Arc::new(db);

    let mut config = Config::default();
    config.app_name = "dbos-cli-test".to_string();
    config.system_db = Some(db.clone());
    let client = Client::new(config).await.expect("client");
    (url, db, client)
}

async fn enqueue(client: &Arc<Client>, id: &str, name: &str) {
    client
        .enqueue(
            "q",
            name,
            serde_json::json!(null),
            EnqueueOptions {
                workflow_id: Some(id.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");
}

#[tokio::test]
async fn migrate_runs_migrations_idempotently() {
    let url = temp_db_url();
    // First migrate creates the schema.
    dbos_cli::backend::open_system_db(&url, None)
        .await
        .expect("open")
        .migrate()
        .await
        .expect("first migrate");
    // Second migrate is a no-op.
    dbos_cli::backend::open_system_db(&url, None)
        .await
        .expect("open")
        .migrate()
        .await
        .expect("second migrate");
}

#[tokio::test]
async fn workflow_list_get_steps_cancel_resume_delete_round_trip() {
    let (_url, db, client) = setup().await;

    enqueue(&client, "wf-a", "my-workflow").await;
    enqueue(&client, "wf-b", "my-workflow").await;
    // Mark wf-b as SUCCESS via the backend.
    db.record_workflow_result("wf-b", WorkflowStatusType::Success, Some(&serde_json::json!(1)), None)
        .await
        .expect("result");

    // list — both workflows.
    let listed = dbos_cli::commands::workflow::list(
        &client,
        dbos_cli::commands::workflow::ListOptions {
            limit: Some(10),
            ..Default::default()
        },
    )
    .await
    .expect("list");
    assert_eq!(listed.len(), 2);

    // get a single workflow.
    let got = dbos_cli::commands::workflow::get(&client, "wf-a")
        .await
        .expect("get");
    assert_eq!(got.id, "wf-a");

    // get a missing workflow.
    let missing = dbos_cli::commands::workflow::get(&client, "nope").await;
    assert!(missing.is_err());

    // steps (empty).
    let steps = dbos_cli::commands::workflow::steps(&client, "wf-a")
        .await
        .expect("steps");
    assert!(steps.is_empty());

    // cancel.
    dbos_cli::commands::workflow::cancel(&client, "wf-a")
        .await
        .expect("cancel");
    let status = client
        .retrieve_workflow("wf-a")
        .get_status()
        .await
        .expect("status");
    assert_eq!(status.status, WorkflowStatusType::Cancelled);

    // resume.
    let resumed = dbos_cli::commands::workflow::resume(&client, "wf-a")
        .await
        .expect("resume");
    assert_eq!(resumed.status, WorkflowStatusType::Enqueued);

    // delete.
    dbos_cli::commands::workflow::delete(&client, &["wf-a".to_string()], false)
        .await
        .expect("delete");
    let listed = dbos_cli::commands::workflow::list(
        &client,
        dbos_cli::commands::workflow::ListOptions::default(),
    )
    .await
    .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "wf-b");
}

#[tokio::test]
async fn workflow_list_filters_by_status_and_queue() {
    let (_url, db, client) = setup().await;

    enqueue(&client, "f-1", "wf").await;
    enqueue(&client, "f-2", "wf").await;
    db.record_workflow_result("f-2", WorkflowStatusType::Success, Some(&serde_json::json!(1)), None)
        .await
        .expect("result");

    // Filter by status SUCCESS.
    let rows = dbos_cli::commands::workflow::list(
        &client,
        dbos_cli::commands::workflow::ListOptions {
            status: Some(WorkflowStatusType::Success),
            ..Default::default()
        },
    )
    .await
    .expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "f-2");

    // Filter by queue "q" — both are on q.
    let rows = dbos_cli::commands::workflow::list(
        &client,
        dbos_cli::commands::workflow::ListOptions {
            queue: Some("q".to_string()),
            ..Default::default()
        },
    )
    .await
    .expect("list");
    assert_eq!(rows.len(), 2);

    // queues_only.
    let rows = dbos_cli::commands::workflow::list(
        &client,
        dbos_cli::commands::workflow::ListOptions {
            queues_only: true,
            ..Default::default()
        },
    )
    .await
    .expect("list");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn workflow_fork_creates_new_workflow() {
    let (_url, db, client) = setup().await;

    enqueue(&client, "orig", "my-workflow").await;
    db.record_workflow_result("orig", WorkflowStatusType::Success, Some(&serde_json::json!(1)), None)
        .await
        .expect("result");
    db.record_step_output(&dbos_core::StepRecord {
        workflow_uuid: "orig".to_string(),
        function_id: 1,
        function_name: "step-one".to_string(),
        output: Some("42".to_string()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("record step");

    let forked = dbos_cli::commands::workflow::fork(
        &client,
        "orig",
        1,
        None,
        Some("forked-1".to_string()),
    )
    .await
    .expect("fork");
    assert_eq!(forked.id, "forked-1");
}

#[tokio::test]
async fn parse_status_and_timestamp_helpers_work() {
    use dbos_cli::commands::workflow::{parse_status, parse_timestamp};
    assert_eq!(
        parse_status("SUCCESS").unwrap(),
        WorkflowStatusType::Success
    );
    assert_eq!(
        parse_status("pending").unwrap(),
        WorkflowStatusType::Pending
    );
    assert!(parse_status("bogus").is_err());
    assert!(parse_timestamp("2024-01-01T00:00:00Z").is_ok());
    assert!(parse_timestamp("not-a-date").is_err());
}

#[tokio::test]
async fn config_url_resolution_prefers_flag_then_config_then_env() {
    use dbos_cli::config::{resolve_db_url, CliConfig};

    // Flag wins over config.
    let cfg = CliConfig {
        database_url: Some("postgres://from-config".to_string()),
        ..Default::default()
    };
    assert_eq!(
        resolve_db_url(Some("postgres://from-flag"), Some(&cfg)).unwrap(),
        "postgres://from-flag"
    );

    // Config wins over env.
    unsafe { std::env::set_var("DBOS_SYSTEM_DATABASE_URL", "postgres://from-env"); }
    assert_eq!(
        resolve_db_url(None, Some(&cfg)).unwrap(),
        "postgres://from-config"
    );

    // Env is the last resort.
    let empty_cfg = CliConfig::default();
    assert_eq!(
        resolve_db_url(None, Some(&empty_cfg)).unwrap(),
        "postgres://from-env"
    );
    unsafe { std::env::remove_var("DBOS_SYSTEM_DATABASE_URL"); }

    // Nothing set → error.
    assert!(resolve_db_url(None, Some(&empty_cfg)).is_err());
}

#[tokio::test]
async fn mask_password_redacts_credentials() {
    use dbos_cli::config::mask_password;
    assert_eq!(
        mask_password("postgres://user:secret@host:5432/db"),
        "postgres://user:***@host:5432/db"
    );
    assert_eq!(
        mask_password("host=localhost password=secret user=foo"),
        "host=localhost password=*** user=foo"
    );
}
