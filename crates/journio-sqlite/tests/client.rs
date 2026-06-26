//! End-to-end coverage for `journio_core::Client` over the SQLite backend —
//! mirrors the schedule/version/client pieces of `journio/client_test.go`. The
//! Client owns no executor, so these tests assert on persisted state rather
//! than workflow execution.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use journio_core::{
    Client, ClientScheduleInput, EnqueueOptions, InitWorkflow, ListWorkflowsFilter, ScheduleStatus,
    SystemDatabase, WorkflowStatusType,
};
use journio_sqlite::{SqliteSystemDatabase, latest_version};

fn temp_db_url(test_name: &str) -> String {
    let db_path = std::env::temp_dir().join(format!(
        "journio-client-{test_name}-{}.db",
        uuid::Uuid::new_v4()
    ));
    format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"))
}

async fn setup() -> (Arc<SqliteSystemDatabase>, Arc<Client>) {
    let db = SqliteSystemDatabase::connect(&temp_db_url("client"))
        .await
        .expect("connect sqlite");
    db.migrate().await.expect("migrate sqlite");
    let db = Arc::new(db);

    let mut config = journio_core::Config::default();
    config.app_name = "client-test".to_string();
    config.system_db = Some(db.clone());
    let client = Client::new(config).await.expect("client");
    (db, client)
}

async fn seed_pending(db: &Arc<SqliteSystemDatabase>, id: &str, name: &str) {
    let mut init = InitWorkflow::new_pending(id, name, "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
}

#[tokio::test]
async fn client_enqueue_lists_retrieves_and_reads_steps() {
    let (db, client) = setup().await;

    let handle = client
        .enqueue(
            "q",
            "workflow-a",
            serde_json::json!({"n": 1}),
            EnqueueOptions {
                workflow_id: Some("wf-client-1".to_string()),
                priority: 5,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue");

    assert_eq!(handle.workflow_id(), "wf-client-1");

    let listed = client
        .list_workflows(ListWorkflowsFilter {
            queue_names: vec!["q".to_string()],
            ..Default::default()
        })
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "wf-client-1");
    assert_eq!(listed[0].status, WorkflowStatusType::Enqueued);
    assert_eq!(listed[0].priority, 5);

    let retrieved = client.retrieve_workflow("wf-client-1");
    let status = retrieved.get_status().await.expect("status");
    assert_eq!(status.name, "workflow-a");

    // Steps: record one directly and read it back through the client.
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: "wf-client-1".to_string(),
        function_id: 1,
        function_name: "step-one".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!(42)).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("record step");
    let steps = client
        .get_workflow_steps("wf-client-1")
        .await
        .expect("steps");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].function_name, "step-one");

    client
        .shutdown(Duration::from_secs(1))
        .await
        .expect("shutdown");
    // shutdown is idempotent enough for the pool — verify the migration version
    // helper still resolves (sanity that nothing panicked).
    let _ = latest_version();
}

#[tokio::test]
async fn client_cancel_resume_and_delete_workflows() {
    let (db, client) = setup().await;

    seed_pending(&db, "wf-c-1", "w").await;
    seed_pending(&db, "wf-c-2", "w").await;

    let cancelled = client
        .cancel_workflows(&[
            "wf-c-1".to_string(),
            "wf-c-2".to_string(),
            "missing".to_string(),
        ])
        .await
        .expect("cancel");
    assert_eq!(cancelled.len(), 2);

    let status = client
        .retrieve_workflow("wf-c-1")
        .get_status()
        .await
        .expect("status");
    assert_eq!(status.status, WorkflowStatusType::Cancelled);

    // Resume one back onto the internal queue.
    assert!(
        client
            .resume_workflow("wf-c-1", None)
            .await
            .expect("resume")
    );
    let resumed = client
        .retrieve_workflow("wf-c-1")
        .get_status()
        .await
        .expect("status");
    assert_eq!(resumed.status, WorkflowStatusType::Enqueued);

    // Delete with children: seed a parent + child, then delete recursively.
    seed_pending(&db, "wf-parent", "w").await;
    let mut child = InitWorkflow::new_pending("wf-child", "w", "local");
    child.parent_workflow_id = Some("wf-parent".to_string());
    db.init_workflow(child).await.expect("seed child");

    client
        .delete_workflows(&["wf-parent".to_string()], true)
        .await
        .expect("delete with children");
    assert!(
        db.get_workflow_status("wf-parent")
            .await
            .expect("parent")
            .is_none()
    );
    assert!(
        db.get_workflow_status("wf-child")
            .await
            .expect("child")
            .is_none()
    );

    // Delete without children leaves the child we re-seed intact.
    seed_pending(&db, "wf-parent-2", "w").await;
    let mut child2 = InitWorkflow::new_pending("wf-child-2", "w", "local");
    child2.parent_workflow_id = Some("wf-parent-2".to_string());
    db.init_workflow(child2).await.expect("seed child 2");
    client
        .delete_workflows(&["wf-parent-2".to_string()], false)
        .await
        .expect("delete without children");
    assert!(
        db.get_workflow_status("wf-parent-2")
            .await
            .expect("parent")
            .is_none()
    );
    assert!(
        db.get_workflow_status("wf-child-2")
            .await
            .expect("child")
            .is_some()
    );
}

#[tokio::test]
async fn client_send_and_get_event() {
    let (db, client) = setup().await;

    // get_event times out returning Null when the target is unknown.
    let value = client
        .get_event("no-such-wf", "k", Duration::from_millis(50))
        .await
        .expect("get event");
    assert!(value.is_null());

    // Seed an event directly through the system db and read it via the client.
    seed_pending(&db, "wf-evt", "w").await;
    db.set_event("wf-evt", "k", &serde_json::json!("hello"), 0)
        .await
        .expect("set event");
    let value = client
        .get_event("wf-evt", "k", Duration::from_secs(1))
        .await
        .expect("get event");
    assert_eq!(value, serde_json::json!("hello"));
}

#[tokio::test]
async fn client_set_workflow_delay_updates_delayed_workflow() {
    let (db, client) = setup().await;

    // Enqueue a delayed workflow.
    let handle = client
        .enqueue(
            "q",
            "workflow-d",
            serde_json::json!(null),
            EnqueueOptions {
                workflow_id: Some("wf-delay".to_string()),
                delay_until: Some(Utc::now() + Duration::from_secs(60)),
                ..Default::default()
            },
        )
        .await
        .expect("enqueue delayed");
    assert_eq!(handle.workflow_id(), "wf-delay");

    let original = db
        .get_workflow_status("wf-delay")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(original.status, WorkflowStatusType::Delayed);

    let new_delay = Utc::now() + Duration::from_secs(120);
    client
        .set_workflow_delay("wf-delay", new_delay)
        .await
        .expect("set delay");
    let updated = db
        .get_workflow_status("wf-delay")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(updated.status, WorkflowStatusType::Delayed);
    let stored = updated.delay_until.expect("delay_until set");
    assert!((stored - new_delay).num_milliseconds().abs() < 1000);
}

#[tokio::test]
async fn client_schedule_crud_pause_resume_and_list() {
    let (_db, client) = setup().await;

    client
        .create_schedule(ClientScheduleInput {
            schedule_name: "nightly".to_string(),
            workflow_name: "wf".to_string(),
            schedule: "0 0 * * * *".to_string(),
            queue_name: Some("q".to_string()),
            ..Default::default()
        })
        .await
        .expect("create schedule");

    client
        .create_schedule(ClientScheduleInput {
            schedule_name: "hourly".to_string(),
            workflow_name: "wf".to_string(),
            schedule: "0 * * * * *".to_string(),
            ..Default::default()
        })
        .await
        .expect("create schedule 2");

    let fetched = client
        .get_schedule("nightly")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched.workflow_name, "wf");
    assert_eq!(fetched.queue_name.as_deref(), Some("q"));
    assert_eq!(fetched.status, ScheduleStatus::Active);

    let all = client.list_schedules(None).await.expect("list all");
    assert_eq!(all.len(), 2);

    let prefixed = client
        .list_schedules(Some(&["night".to_string()]))
        .await
        .expect("list prefix");
    assert_eq!(prefixed.len(), 1);
    assert_eq!(prefixed[0].schedule_name, "nightly");

    client.pause_schedule("nightly").await.expect("pause");
    let paused = client
        .get_schedule("nightly")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(paused.status, ScheduleStatus::Paused);

    client.resume_schedule("nightly").await.expect("resume");
    let resumed = client
        .get_schedule("nightly")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(resumed.status, ScheduleStatus::Active);

    client.delete_schedule("hourly").await.expect("delete");
    assert!(client.get_schedule("hourly").await.expect("get").is_none());
}

#[tokio::test]
async fn client_apply_schedules_replaces_set() {
    let (_db, client) = setup().await;

    client
        .apply_schedules(vec![
            ClientScheduleInput {
                schedule_name: "a".to_string(),
                workflow_name: "wf".to_string(),
                schedule: "0 * * * * *".to_string(),
                ..Default::default()
            },
            ClientScheduleInput {
                schedule_name: "b".to_string(),
                workflow_name: "wf".to_string(),
                schedule: "0 0 * * * *".to_string(),
                ..Default::default()
            },
        ])
        .await
        .expect("apply");

    let all = client.list_schedules(None).await.expect("list");
    assert_eq!(all.len(), 2);

    // Re-apply with a changed definition for "a"; upsert replaces it.
    client
        .apply_schedules(vec![ClientScheduleInput {
            schedule_name: "a".to_string(),
            workflow_name: "wf-renamed".to_string(),
            schedule: "0 0 * * * *".to_string(),
            ..Default::default()
        }])
        .await
        .expect("apply again");
    let a = client
        .get_schedule("a")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(a.workflow_name, "wf-renamed");
}

#[tokio::test]
async fn client_trigger_and_backfill_schedule_enqueue_workflows() {
    let (_db, client) = setup().await;

    client
        .create_schedule(ClientScheduleInput {
            schedule_name: "every-minute".to_string(),
            workflow_name: "wf".to_string(),
            schedule: "0 * * * * *".to_string(),
            queue_name: Some("q".to_string()),
            ..Default::default()
        })
        .await
        .expect("create schedule");

    let handle = client
        .trigger_schedule("every-minute")
        .await
        .expect("trigger");
    let status = handle.get_status().await.expect("status");
    assert_eq!(status.status, WorkflowStatusType::Enqueued);
    assert_eq!(status.queue_name.as_deref(), Some("q"));
    assert!(status.id.starts_with("sched-every-minute-trigger-"));

    // Backfill a 3-minute window (the cron fires at minute boundaries).
    let start = Utc::now() - chrono::Duration::minutes(3);
    let end = Utc::now();
    let enqueued = client
        .backfill_schedule("every-minute", start, end)
        .await
        .expect("backfill");
    assert!(
        !enqueued.is_empty(),
        "backfill should enqueue at least one slot"
    );
    for id in &enqueued {
        assert!(id.starts_with("sched-every-minute-"));
    }
}

#[tokio::test]
async fn client_application_version_management() {
    let (_db, client) = setup().await;

    assert!(
        client
            .get_latest_application_version()
            .await
            .expect("latest")
            .is_none()
    );

    client
        .set_latest_application_version("v1")
        .await
        .expect("set latest v1");
    // Sleep to ensure v2 gets a strictly-later millisecond timestamp;
    // otherwise ORDER BY version_timestamp DESC is ambiguous.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    client
        .set_latest_application_version("v2")
        .await
        .expect("set latest v2");

    let versions = client.list_application_versions().await.expect("list");
    assert_eq!(versions.len(), 2);
    // Newest first.
    assert_eq!(versions[0].version_name, "v2");

    let latest = client
        .get_latest_application_version()
        .await
        .expect("latest")
        .expect("present");
    assert_eq!(latest.version_name, "v2");

    // set_latest is idempotent on an existing version (re-bumps timestamp).
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    client
        .set_latest_application_version("v1")
        .await
        .expect("bump v1");
    let latest = client
        .get_latest_application_version()
        .await
        .expect("latest")
        .expect("present");
    assert_eq!(latest.version_name, "v1");
}

#[tokio::test]
async fn client_list_workflows_filters_by_status_and_uses_paging() {
    let (db, client) = setup().await;

    seed_pending(&db, "p-1", "w").await;
    seed_pending(&db, "p-2", "w").await;
    seed_pending(&db, "p-3", "w").await;
    db.record_workflow_result(
        "p-2",
        WorkflowStatusType::Success,
        Some(&serde_json::json!(1)),
        None,
    )
    .await
    .expect("record result");

    let successful = client
        .list_workflows(ListWorkflowsFilter {
            statuses: vec![WorkflowStatusType::Success],
            ..Default::default()
        })
        .await
        .expect("list success");
    assert_eq!(successful.len(), 1);
    assert_eq!(successful[0].id, "p-2");

    let pending = client
        .list_workflows(ListWorkflowsFilter {
            statuses: vec![WorkflowStatusType::Pending],
            ..Default::default()
        })
        .await
        .expect("list pending");
    assert_eq!(pending.len(), 2);

    // Paging: limit 1 returns one row.
    let paged = client
        .list_workflows(ListWorkflowsFilter {
            limit: Some(1),
            ..Default::default()
        })
        .await
        .expect("list paged");
    assert_eq!(paged.len(), 1);
}
