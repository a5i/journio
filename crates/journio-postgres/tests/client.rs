//! Postgres coverage for the `SystemDatabase` methods added alongside the
//! `Client` port (`list_workflows_filtered`, schedule CRUD, application
//! versions, `delete_workflows`, `set_workflow_delay`) and for `Client`
//! itself. These exercise the Postgres-specific SQL paths (`ANY($n)` arrays,
//! recursive descendant gather) that the SQLite suite does not.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use journio_core::{
    Client, ClientScheduleInput, EnqueueOptions, InitWorkflow, ListWorkflowsFilter, ScheduleStatus,
    SystemDatabase, WorkflowSchedule, WorkflowStatusType,
};
use journio_postgres::PostgresSystemDatabase;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

struct Harness {
    _container: Box<dyn Any>,
    db: Arc<PostgresSystemDatabase>,
}

async fn setup() -> Harness {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("host").to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let schema = format!("journio_{}", uuid::Uuid::new_v4().simple());
    let db =
        PostgresSystemDatabase::connect(&database_url, &schema).expect("connect postgres");
    {
        let client = db.pool().get().await.expect("pool client");
        client
            .execute("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public", &[])
            .await
            .expect("pgcrypto");
    }
    db.migrate().await.expect("migrate");
    Harness {
        _container: Box::new(container),
        db: Arc::new(db),
    }
}

async fn client_over(db: &Arc<PostgresSystemDatabase>) -> Arc<Client> {
    let mut config = journio_core::Config::default();
    config.app_name = "pg-client".to_string();
    config.system_db = Some(db.clone());
    Client::new(config).await.expect("client")
}

async fn seed(db: &Arc<PostgresSystemDatabase>, id: &str, name: &str) {
    let mut init = InitWorkflow::new_pending(id, name, "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed");
}

#[tokio::test]
async fn postgres_client_enqueue_list_filter_and_steps() {
    let harness = setup().await;
    let db = &harness.db;
    let client = client_over(db).await;

    client
        .enqueue(
            "q",
            "wf-a",
            serde_json::json!({"n": 1}),
            EnqueueOptions {
                workflow_id: Some("pg-wf-1".to_string()),
                priority: 3,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    let by_queue = client
        .list_workflows(ListWorkflowsFilter {
            queue_names: vec!["q".to_string()],
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(by_queue.len(), 1);
    assert_eq!(by_queue[0].id, "pg-wf-1");

    // Prefix filter (LIKE ANY) — exercised only by Postgres in this suite.
    let by_prefix = client
        .list_workflows(ListWorkflowsFilter {
            workflow_id_prefixes: vec!["pg-wf".to_string()],
            ..Default::default()
        })
        .await
        .expect("list prefix");
    assert_eq!(by_prefix.len(), 1);

    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: "pg-wf-1".to_string(),
        function_id: 1,
        function_name: "s1".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!(7)).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("step");
    assert_eq!(
        client
            .get_workflow_steps("pg-wf-1")
            .await
            .expect("steps")
            .len(),
        1
    );

    client.shutdown(Duration::from_secs(1)).await.expect("shutdown");
}

#[tokio::test]
async fn postgres_schedule_versions_trigger_backfill_and_delete() {
    let harness = setup().await;
    let db = &harness.db;
    let client = client_over(db).await;

    // Application versions.
    client
        .set_latest_application_version("v1")
        .await
        .expect("v1");
    client
        .set_latest_application_version("v2")
        .await
        .expect("v2");
    let latest = client
        .get_latest_application_version()
        .await
        .expect("latest")
        .expect("present");
    assert_eq!(latest.version_name, "v2");

    // Schedule CRUD + pause/resume.
    client
        .create_schedule(ClientScheduleInput {
            schedule_name: "minutely".to_string(),
            workflow_name: "wf".to_string(),
            schedule: "0 * * * * *".to_string(),
            queue_name: Some("q".to_string()),
            ..Default::default()
        })
        .await
        .expect("create");
    assert!(client.get_schedule("minutely").await.expect("get").is_some());
    client.pause_schedule("minutely").await.expect("pause");
    assert_eq!(
        client.get_schedule("minutely").await.expect("get").expect("row").status,
        ScheduleStatus::Paused
    );
    client.resume_schedule("minutely").await.expect("resume");

    // Trigger enqueues onto the schedule's queue (latest version applied).
    let handle = client.trigger_schedule("minutely").await.expect("trigger");
    let status = handle.get_status().await.expect("status");
    assert_eq!(status.status, WorkflowStatusType::Enqueued);
    assert_eq!(status.application_version, "v2");

    // Backfill a 3-minute window.
    let start = Utc::now() - chrono::Duration::minutes(3);
    let end = Utc::now();
    let enqueued = client
        .backfill_schedule("minutely", start, end)
        .await
        .expect("backfill");
    assert!(!enqueued.is_empty());

    client.delete_schedule("minutely").await.expect("delete");
    assert!(client.get_schedule("minutely").await.expect("get").is_none());

    // delete_workflows with children recurses.
    seed(db, "pg-parent", "w").await;
    let mut child = InitWorkflow::new_pending("pg-child", "w", "local");
    child.parent_workflow_id = Some("pg-parent".to_string());
    db.init_workflow(child).await.expect("seed child");
    client
        .delete_workflows(&["pg-parent".to_string()], true)
        .await
        .expect("delete children");
    assert!(db.get_workflow_status("pg-parent").await.expect("p").is_none());
    assert!(db.get_workflow_status("pg-child").await.expect("c").is_none());

    // set_workflow_delay only affects DELAYED workflows.
    client
        .enqueue(
            "q",
            "wf-d",
            serde_json::json!(null),
            EnqueueOptions {
                workflow_id: Some("pg-delay".to_string()),
                delay_until: Some(Utc::now() + Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue delayed");
    let new_delay = Utc::now() + Duration::from_secs(180);
    client
        .set_workflow_delay("pg-delay", new_delay)
        .await
        .expect("set delay");
    let stored = db
        .get_workflow_status("pg-delay")
        .await
        .expect("status")
        .expect("row");
    assert!((stored.delay_until.expect("delay") - new_delay).num_milliseconds().abs() < 1000);
}

#[tokio::test]
async fn postgres_filtered_list_covers_array_and_time_clauses() {
    let harness = setup().await;
    let db = &harness.db;

    seed(db, "f-1", "alpha").await;
    seed(db, "f-2", "beta").await;
    seed(db, "f-3", "alpha").await;
    db.record_workflow_result("f-2", WorkflowStatusType::Success, Some(&serde_json::json!(1)), None)
        .await
        .expect("result");

    // Filter by ids array.
    let by_ids = db
        .list_workflows_filtered(&ListWorkflowsFilter {
            workflow_ids: vec!["f-1".to_string(), "f-3".to_string()],
            ..Default::default()
        })
        .await
        .expect("list ids");
    assert_eq!(by_ids.len(), 2);

    // Filter by name + status.
    let by_name_status = db
        .list_workflows_filtered(&ListWorkflowsFilter {
            names: vec!["alpha".to_string()],
            statuses: vec![WorkflowStatusType::Pending],
            ..Default::default()
        })
        .await
        .expect("list name+status");
    assert_eq!(by_name_status.len(), 2);

    // completed_after excludes pending rows.
    let completed = db
        .list_workflows_filtered(&ListWorkflowsFilter {
            completed_after: Some(Utc::now() - chrono::Duration::seconds(60)),
            ..Default::default()
        })
        .await
        .expect("list completed");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].id, "f-2");

    // queues_only returns nothing (no queued workflows seeded).
    let queues_only = db
        .list_workflows_filtered(&ListWorkflowsFilter {
            queues_only: true,
            ..Default::default()
        })
        .await
        .expect("list queues_only");
    assert!(queues_only.is_empty());

    // Direct upsert_schedule / list_schedules round-trip (raw trait path).
    db.upsert_schedule(&WorkflowSchedule {
        schedule_id: "id1".to_string(),
        schedule_name: "raw".to_string(),
        workflow_name: "wf".to_string(),
        workflow_class_name: None,
        schedule: "0 * * * * *".to_string(),
        status: ScheduleStatus::Active,
        context: serde_json::json!({}),
        last_fired_at: Some(Utc::now()),
        automatic_backfill: false,
        cron_timezone: None,
        queue_name: Some("q".to_string()),
    })
    .await
    .expect("upsert schedule");
    assert_eq!(db.list_schedules().await.expect("list").len(), 1);
}
