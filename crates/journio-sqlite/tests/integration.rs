use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::Utc;
use journio_core::{
    Config, DebounceOptions, EnqueueOptions, ForkWorkflowOptions, InitWorkflow, JournioContext,
    JournioErrorCode, QueueOptions, ReadStreamOptions, ScheduleOptions, ScheduleStatus,
    ScheduledWorkflowInput, SystemDatabase, WorkflowFn, WorkflowStatusType,
};
use journio_sqlite::{SqliteSystemDatabase, latest_version};
use sqlx::Row;

fn temp_db_url(test_name: &str) -> String {
    let db_path: PathBuf = std::env::temp_dir().join(format!(
        "journio-sqlite-{test_name}-{}.db",
        uuid::Uuid::new_v4()
    ));
    format!("sqlite://{}", db_path.to_string_lossy().replace('\\', "/"))
}

async fn setup() -> Arc<SqliteSystemDatabase> {
    let db = SqliteSystemDatabase::connect(&temp_db_url("integration"))
        .await
        .expect("connect sqlite");
    db.migrate().await.expect("migrate sqlite");
    Arc::new(db)
}

async fn setup_at(database_url: &str) -> Arc<SqliteSystemDatabase> {
    let db = SqliteSystemDatabase::connect(database_url)
        .await
        .expect("connect sqlite");
    db.migrate().await.expect("migrate sqlite");
    Arc::new(db)
}

async fn seed_workflow(db: &Arc<SqliteSystemDatabase>, id: &str) {
    let mut init = InitWorkflow::new_pending(id, "test-workflow", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
}

#[tokio::test]
async fn sqlite_lifecycle_checkpoints_notifications_and_events_work() {
    let db = setup().await;

    seed_workflow(&db, "wf-1").await;
    let status = db
        .get_workflow_status("wf-1")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.status, WorkflowStatusType::Pending);

    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: "wf-1".to_string(),
        function_id: 1,
        function_name: "step-one".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!(42)).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("record step");
    assert_eq!(db.get_steps("wf-1").await.expect("steps").len(), 1);

    db.send("wf-1", "topic", &serde_json::json!("hello"))
        .await
        .expect("send");
    let msg = db
        .consume_notification("wf-1", "topic")
        .await
        .expect("consume")
        .expect("message");
    assert_eq!(msg.message, serde_json::json!("hello"));
    assert!(
        db.consume_notification("wf-1", "topic")
            .await
            .expect("consume again")
            .is_none()
    );

    db.set_event("wf-1", "status", &serde_json::json!("done"), 2)
        .await
        .expect("set event");
    let value = db
        .get_event_value("wf-1", "status")
        .await
        .expect("get event")
        .expect("event");
    assert_eq!(value, serde_json::json!("done"));

    let err = db
        .send("missing", "topic", &serde_json::json!("x"))
        .await
        .expect_err("foreign key violation");
    assert_eq!(err.code, JournioErrorCode::NonExistentWorkflowError);
}

#[tokio::test]
async fn sqlite_foundation_reopen_pragmas_and_schema_are_correct() {
    let db_path: PathBuf = std::env::temp_dir().join(format!(
        "journio-sqlite-foundation-{}.db",
        uuid::Uuid::new_v4()
    ));
    let url = format!("sqlite:{}", db_path.to_string_lossy().replace('\\', "/"));

    let db1 = setup_at(&url).await;
    let version1: i64 = sqlx::query("SELECT version FROM journio_migrations")
        .fetch_one(db1.pool())
        .await
        .expect("select migration version")
        .get(0);
    assert_eq!(version1, latest_version());

    drop(db1);

    let db2 = setup_at(&url).await;
    let version2: i64 = sqlx::query("SELECT version FROM journio_migrations")
        .fetch_one(db2.pool())
        .await
        .expect("select migration version again")
        .get(0);
    assert_eq!(version2, latest_version());

    let journal_mode: String = sqlx::query("PRAGMA journal_mode")
        .fetch_one(db2.pool())
        .await
        .expect("pragma journal_mode")
        .get(0);
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

    let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
        .fetch_one(db2.pool())
        .await
        .expect("pragma foreign_keys")
        .get(0);
    assert_eq!(foreign_keys, 1);

    for table in [
        "workflow_status",
        "operation_outputs",
        "notifications",
        "workflow_events",
        "workflow_events_history",
        "streams",
        "event_dispatch_kv",
        "workflow_schedules",
        "application_versions",
        "queues",
    ] {
        let name: String =
            sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?1")
                .bind(table)
                .fetch_one(db2.pool())
                .await
                .expect("table exists")
                .get(0);
        assert_eq!(name, table);
    }
}

#[tokio::test]
async fn sqlite_end_to_end_workflow_runs_primitives() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-e2e".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let workflow = Arc::new(WorkflowFn::new("all-primitives", |ctx, _input: ()| {
        Box::pin(async move {
            ctx.sleep(Duration::from_millis(10)).await?;
            ctx.set_event("k", serde_json::json!("v")).await?;
            let event = ctx
                .get_event(ctx.workflow_id(), "k", Duration::from_secs(2))
                .await?;
            ctx.send(ctx.workflow_id(), serde_json::json!("msg"), "t")
                .await?;
            let message = ctx.recv("t", Duration::from_secs(2)).await?;
            Ok(serde_json::json!({ "event": event, "message": message }))
        })
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    let handle = ctx
        .run_workflow("all-primitives", serde_json::json!(null))
        .await
        .expect("run workflow");
    let result = handle
        .get_result(Some(Duration::from_secs(5)))
        .await
        .expect("result");

    assert_eq!(
        result,
        serde_json::json!({ "event": "v", "message": "msg" })
    );

    let status = db
        .get_workflow_status(handle.workflow_id())
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.status, WorkflowStatusType::Success);

    let steps = db.get_steps(handle.workflow_id()).await.expect("steps");
    let names: Vec<&str> = steps.iter().map(|s| s.function_name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "journio.sleep",
            "journio.setEvent",
            "journio.getEvent",
            "journio.send",
            "journio.recv"
        ]
    );
}

#[tokio::test]
async fn sqlite_recovery_replays_completed_step_and_finishes_workflow() {
    let db = setup().await;
    let workflow_id = "sqlite-recover-me".to_string();

    let mut init = InitWorkflow::new_pending(workflow_id.clone(), "recovery-workflow", "local");
    init.input = Some(serde_json::json!(2));
    db.init_workflow(init).await.expect("seed workflow");
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: workflow_id.clone(),
        function_id: 1,
        function_name: "step-one".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!(40)).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("seed checkpoint");

    let config = Config {
        app_name: "sqlite-recovery".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let step_one_counter = Arc::new(AtomicUsize::new(0));
    let step_two_counter = Arc::new(AtomicUsize::new(0));

    let step_one = Arc::new(journio_core::StepFunc::new("step-one", {
        let step_one_counter = step_one_counter.clone();
        move |_ctx| {
            let step_one_counter = step_one_counter.clone();
            Box::pin(async move {
                step_one_counter.fetch_add(1, Ordering::SeqCst);
                Ok(40_i64)
            })
        }
    }));

    let step_two = Arc::new(journio_core::StepFunc::new("step-two", {
        let step_two_counter = step_two_counter.clone();
        move |_ctx| {
            let step_two_counter = step_two_counter.clone();
            Box::pin(async move {
                step_two_counter.fetch_add(1, Ordering::SeqCst);
                Ok(2_i64)
            })
        }
    }));

    let workflow = Arc::new(WorkflowFn::new(
        "recovery-workflow",
        move |ctx, input: i64| {
            let step_one = step_one.clone();
            let step_two = step_two.clone();
            Box::pin(async move {
                let first =
                    serde_json::from_value::<i64>(ctx.run_as_step(step_one).await?).expect("first");
                let second = serde_json::from_value::<i64>(ctx.run_as_step(step_two).await?)
                    .expect("second");
                Ok(first + second + input)
            })
        },
    ));
    ctx.register_workflow(workflow).expect("register workflow");

    ctx.launch().await.expect("launch and recover");

    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status")
        .expect("status row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.output.expect("output"), serde_json::json!(44));
    assert_eq!(step_one_counter.load(Ordering::SeqCst), 0);
    assert_eq!(step_two_counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sqlite_list_workflows_and_gc_delete_only_terminal_rows() {
    let db = setup().await;

    let mut success = InitWorkflow::new_pending("sqlite-success", "success-workflow", "local");
    success.input = Some(serde_json::json!(null));
    db.init_workflow(success)
        .await
        .expect("seed success workflow");
    db.record_workflow_result(
        "sqlite-success",
        WorkflowStatusType::Success,
        Some(&serde_json::json!("done")),
        None,
    )
    .await
    .expect("mark success");

    let mut pending = InitWorkflow::new_pending("sqlite-pending-gc", "pending-workflow", "local");
    pending.input = Some(serde_json::json!(null));
    db.init_workflow(pending)
        .await
        .expect("seed pending workflow");

    let workflows = db.list_workflows(10).await.expect("list workflows");
    assert!(
        workflows.iter().any(|wf| wf.id == "sqlite-success"),
        "success workflow missing"
    );
    assert!(
        workflows.iter().any(|wf| wf.id == "sqlite-pending-gc"),
        "pending workflow missing"
    );

    let deleted = db
        .delete_workflows_before(Utc::now() + chrono::Duration::seconds(1))
        .await
        .expect("gc delete");
    assert_eq!(deleted, 1);

    assert!(
        db.get_workflow_status("sqlite-success")
            .await
            .expect("success status")
            .is_none()
    );
    assert!(
        db.get_workflow_status("sqlite-pending-gc")
            .await
            .expect("pending status")
            .is_some()
    );
}

#[tokio::test]
async fn sqlite_workflow_handle_get_result_covers_success_error_and_timeout() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-handle".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let success_workflow = Arc::new(WorkflowFn::new("success-workflow", |_ctx, input: i64| {
        Box::pin(async move { Ok(input + 1) })
    }));
    ctx.register_workflow(success_workflow)
        .expect("register success workflow");

    let success_handle = ctx
        .run_workflow("success-workflow", serde_json::json!(41))
        .await
        .expect("run success workflow");
    let success_result = success_handle
        .get_result(Some(Duration::from_secs(2)))
        .await
        .expect("success result");
    assert_eq!(success_result, serde_json::json!(42));

    db.init_workflow(InitWorkflow::new_pending(
        "sqlite-failed",
        "failed-workflow",
        "local",
    ))
    .await
    .expect("seed failed workflow");
    db.record_workflow_result(
        "sqlite-failed",
        WorkflowStatusType::Error,
        None,
        Some("boom"),
    )
    .await
    .expect("persist terminal error");
    let failing_handle = ctx.workflow_handle("sqlite-failed");
    let err = failing_handle
        .get_result(Some(Duration::from_secs(2)))
        .await
        .expect_err("failing workflow should surface error");
    assert_eq!(err.code, JournioErrorCode::WorkflowExecutionError);
    assert!(err.message.contains("boom"));

    db.init_workflow(InitWorkflow::new_pending(
        "sqlite-pending",
        "pending-workflow",
        "local",
    ))
    .await
    .expect("seed pending workflow");
    let pending_handle = ctx.workflow_handle("sqlite-pending");
    let timeout = pending_handle
        .get_result(Some(Duration::from_millis(50)))
        .await
        .expect_err("pending workflow should time out");
    assert_eq!(timeout.code, JournioErrorCode::TimeoutError);

    let missing = ctx.workflow_handle("sqlite-missing");
    let missing_err = missing.get_status().await.expect_err("missing workflow");
    assert_eq!(missing_err.code, JournioErrorCode::NonExistentWorkflowError);
}

#[tokio::test]
async fn sqlite_recv_replay_uses_recorded_message_without_consuming_live_notification() {
    let db = setup().await;
    let workflow_id = "sqlite-replay-recv".to_string();

    let mut init = InitWorkflow::new_pending(workflow_id.clone(), "replayer", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: workflow_id.clone(),
        function_id: 1,
        function_name: "journio.recv".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!("recorded")).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("seed recv checkpoint");
    db.send(&workflow_id, "t", &serde_json::json!("live"))
        .await
        .expect("seed live notification");

    let config = Config {
        app_name: "sqlite-recv-replay".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let workflow = Arc::new(WorkflowFn::new("replayer", |ctx, _input: ()| {
        Box::pin(async move { ctx.recv("t", Duration::from_secs(2)).await })
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    ctx.launch().await.expect("launch + recover");

    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status")
        .expect("status row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(
        status.output.expect("output"),
        serde_json::json!("recorded")
    );

    let leftover = db
        .consume_notification(&workflow_id, "t")
        .await
        .expect("consume")
        .expect("live notification still present");
    assert_eq!(leftover.message, serde_json::json!("live"));
}

#[tokio::test]
async fn sqlite_child_workflow_records_child_id_and_parent_completes() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-child".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let child = Arc::new(WorkflowFn::new("child-workflow", |_ctx, input: String| {
        Box::pin(async move { Ok(format!("{input}-child")) })
    }));
    ctx.register_workflow(child).expect("register child");

    let parent = Arc::new(WorkflowFn::new("parent-workflow", |ctx, input: String| {
        Box::pin(async move {
            let handle = ctx
                .run_workflow("child-workflow", serde_json::json!(input))
                .await?;
            handle.get_result(Some(Duration::from_secs(2))).await
        })
    }));
    ctx.register_workflow(parent).expect("register parent");

    let handle = ctx
        .run_workflow("parent-workflow", serde_json::json!("hello"))
        .await
        .expect("run parent");
    let result = handle
        .get_result(Some(Duration::from_secs(2)))
        .await
        .expect("parent result");
    assert_eq!(result, serde_json::json!("hello-child"));

    let parent_steps = db
        .get_steps(handle.workflow_id())
        .await
        .expect("parent steps");
    let child_step = parent_steps
        .iter()
        .find(|step| step.function_name == "child::child-workflow")
        .expect("child workflow checkpoint");
    let child_id = child_step
        .child_workflow_id
        .clone()
        .expect("child workflow id recorded");

    let child_status = db
        .get_workflow_status(&child_id)
        .await
        .expect("child status")
        .expect("child row");
    assert_eq!(child_status.status, WorkflowStatusType::Success);
    assert_eq!(
        child_status.output.expect("child output"),
        serde_json::json!("hello-child")
    );
}

#[tokio::test]
async fn sqlite_step_error_is_checkpointed_and_workflow_status_is_error() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-step-error".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let failing_step = Arc::new(journio_core::StepFunc::new("failing-step", |_ctx| {
        Box::pin(async move {
            Err::<String, _>(journio_core::JournioError::new(
                JournioErrorCode::WorkflowExecutionError,
                "step failure",
            ))
        })
    }));

    let workflow = Arc::new(WorkflowFn::new(
        "step-error-workflow",
        move |ctx, _input: ()| {
            let failing_step = failing_step.clone();
            Box::pin(async move {
                let _ = ctx.run_as_step(failing_step).await?;
                Ok(serde_json::json!("unreachable"))
            })
        },
    ));
    ctx.register_workflow(workflow).expect("register workflow");

    let err = match ctx
        .run_workflow("step-error-workflow", serde_json::json!(null))
        .await
    {
        Ok(_) => panic!("workflow should fail"),
        Err(err) => err,
    };
    assert_eq!(err.code, JournioErrorCode::WorkflowExecutionError);
    assert!(err.message.contains("step failure"));

    let workflows = db.list_workflows(10).await.expect("list workflows");
    let status = workflows
        .iter()
        .find(|wf| wf.name == "step-error-workflow")
        .expect("failed workflow status");
    assert_eq!(status.status, WorkflowStatusType::Error);

    let steps = db
        .get_steps(&status.id)
        .await
        .expect("failed workflow steps");
    let step = steps.first().expect("failing step recorded");
    assert_eq!(step.function_name, "failing-step");
    assert!(step.error.as_deref().unwrap().contains("step failure"));
}

#[tokio::test]
async fn sqlite_get_event_replay_uses_recorded_value_without_live_event() {
    let db = setup().await;
    let workflow_id = "sqlite-replay-event".to_string();

    let mut init = InitWorkflow::new_pending(workflow_id.clone(), "event-replayer", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: workflow_id.clone(),
        function_id: 1,
        function_name: "journio.getEvent".to_string(),
        output: Some(serde_json::to_string(&serde_json::json!("recorded-event")).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("seed getEvent checkpoint");

    let config = Config {
        app_name: "sqlite-event-replay".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let workflow = Arc::new(WorkflowFn::new("event-replayer", |ctx, _input: ()| {
        Box::pin(async move {
            ctx.get_event(ctx.workflow_id(), "missing", Duration::from_secs(2))
                .await
        })
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    ctx.launch().await.expect("launch + recover");

    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status")
        .expect("status row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(
        status.output.expect("output"),
        serde_json::json!("recorded-event")
    );
    assert!(
        db.get_event_value(&workflow_id, "missing")
            .await
            .expect("get event value")
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_sleep_replay_does_not_wait_when_deadline_has_passed() {
    let db = setup().await;
    let workflow_id = "sqlite-sleep-replay".to_string();

    let mut init = InitWorkflow::new_pending(workflow_id.clone(), "sleep-replayer", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
    let past = (Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: workflow_id.clone(),
        function_id: 1,
        function_name: "journio.sleep".to_string(),
        output: Some(serde_json::to_string(&serde_json::Value::String(past)).unwrap()),
        error: None,
        child_workflow_id: None,
    })
    .await
    .expect("seed sleep checkpoint");

    let config = Config {
        app_name: "sqlite-sleep-replay".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let workflow = Arc::new(WorkflowFn::new("sleep-replayer", |ctx, _input: ()| {
        Box::pin(async move {
            ctx.sleep(Duration::from_secs(10)).await?;
            Ok(serde_json::json!("done"))
        })
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    let launched = tokio::time::timeout(Duration::from_secs(2), ctx.launch()).await;
    launched.expect("launch did not hang").expect("launch ok");

    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status")
        .expect("status row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.output.expect("output"), serde_json::json!("done"));
}

#[tokio::test]
async fn sqlite_context_queue_end_to_end_enqueue_and_drain_once() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-queue-e2e".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let workflow = Arc::new(WorkflowFn::new("queued-workflow", |_ctx, input: i64| {
        Box::pin(async move { Ok(input + 1) })
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    let handle = ctx
        .enqueue_workflow(
            "jobs",
            "queued-workflow",
            serde_json::json!(41),
            EnqueueOptions::default(),
        )
        .await
        .expect("enqueue workflow");

    let enqueued = db
        .get_workflow_status(handle.workflow_id())
        .await
        .expect("status")
        .expect("row");
    assert_eq!(enqueued.status, WorkflowStatusType::Enqueued);

    let dequeued = ctx.run_queue_once("jobs").await.expect("drain queue");
    assert!(dequeued.is_some());

    let result = handle
        .get_result(Some(Duration::from_secs(2)))
        .await
        .expect("result");
    assert_eq!(result, serde_json::json!(42));
}

#[tokio::test]
async fn sqlite_launch_background_queue_worker_drains_enqueued_workflow() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-bg-queue".to_string(),
        system_db: Some(db.clone()),
        scheduler_polling_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let workflow = Arc::new(WorkflowFn::new("queued-bg-workflow", |_ctx, input: i64| {
        Box::pin(async move { Ok(input + 1) })
    }));
    ctx.register_workflow(workflow).expect("register workflow");
    ctx.launch().await.expect("launch");

    let handle = ctx
        .enqueue_workflow(
            "jobs",
            "queued-bg-workflow",
            serde_json::json!(41),
            EnqueueOptions::default(),
        )
        .await
        .expect("enqueue workflow");

    let result = handle
        .get_result(Some(Duration::from_secs(3)))
        .await
        .expect("background queue result");
    assert_eq!(result, serde_json::json!(42));
}

#[tokio::test]
async fn sqlite_scheduler_triggers_scheduled_workflow_and_executes_it() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-scheduler".to_string(),
        system_db: Some(db.clone()),
        scheduler_polling_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let executions = Arc::new(AtomicUsize::new(0));
    let scheduled = Arc::new(WorkflowFn::new("scheduled-workflow", {
        let executions = executions.clone();
        move |_ctx, input: ScheduledWorkflowInput| {
            let executions = executions.clone();
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(input.scheduled_time.to_rfc3339())
            })
        }
    }));
    ctx.register_workflow(scheduled).expect("register workflow");
    ctx.launch().await.expect("launch");

    ctx.register_schedule(
        "every-second",
        "scheduled-workflow",
        "*/1 * * * * *",
        serde_json::json!(null),
        ScheduleOptions::default(),
    )
    .await
    .expect("register schedule");

    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if executions.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("schedule should fire");

    let schedules = db.list_schedules().await.expect("list schedules");
    let schedule = schedules
        .iter()
        .find(|schedule| schedule.schedule_name == "every-second")
        .expect("schedule row");
    assert_eq!(schedule.status, ScheduleStatus::Active);
    assert!(schedule.last_fired_at.is_some());
}

#[tokio::test]
async fn sqlite_scheduler_automatic_backfill_executes_missed_ticks() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-scheduler-backfill".to_string(),
        system_db: Some(db.clone()),
        scheduler_polling_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let executions = Arc::new(AtomicUsize::new(0));
    let scheduled = Arc::new(WorkflowFn::new("backfill-workflow", {
        let executions = executions.clone();
        move |_ctx, _input: ScheduledWorkflowInput| {
            let executions = executions.clone();
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(serde_json::json!("ok"))
            })
        }
    }));
    ctx.register_workflow(scheduled).expect("register workflow");
    ctx.launch().await.expect("launch");

    ctx.register_schedule(
        "backfill-secondly",
        "backfill-workflow",
        "*/1 * * * * *",
        serde_json::json!({"kind":"backfill"}),
        ScheduleOptions {
            automatic_backfill: true,
            last_fired_at: Some(Utc::now() - chrono::Duration::seconds(3)),
            ..Default::default()
        },
    )
    .await
    .expect("register backfill schedule");

    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if executions.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("backfill should execute missed ticks");
}

#[tokio::test]
async fn sqlite_streams_snapshot_and_close_semantics_work() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-streams".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let stream_workflow = Arc::new(WorkflowFn::new("stream-workflow", |ctx, _: ()| {
        Box::pin(async move {
            ctx.write_stream("values", serde_json::json!("value1"))
                .await?;
            ctx.write_stream("values", serde_json::json!("value2"))
                .await?;
            let _ = ctx.recv("release", Duration::from_secs(2)).await?;
            ctx.close_stream("values").await?;
            Ok(serde_json::json!("done"))
        })
    }));
    ctx.register_workflow(stream_workflow)
        .expect("register stream workflow");

    let reader_ctx = ctx.clone();
    let handle = ctx
        .enqueue_workflow(
            "jobs",
            "stream-workflow",
            serde_json::json!(null),
            EnqueueOptions::default(),
        )
        .await
        .expect("enqueue workflow");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let (snapshot, closed) = reader_ctx
        .read_stream(
            handle.workflow_id(),
            "values",
            ReadStreamOptions {
                snapshot: true,
                from_offset: 0,
            },
        )
        .await
        .expect("snapshot stream");
    assert_eq!(
        snapshot,
        vec![serde_json::json!("value1"), serde_json::json!("value2")]
    );
    assert!(!closed);

    reader_ctx
        .send(handle.workflow_id(), serde_json::json!("go"), "release")
        .await
        .expect("release workflow");

    let result = handle
        .get_result(Some(Duration::from_secs(3)))
        .await
        .expect("workflow result");
    assert_eq!(result, serde_json::json!("done"));

    let (values, closed) = reader_ctx
        .read_stream(handle.workflow_id(), "values", ReadStreamOptions::default())
        .await
        .expect("read closed stream");
    assert_eq!(
        values,
        vec![serde_json::json!("value1"), serde_json::json!("value2")]
    );
    assert!(closed);
}

#[tokio::test]
async fn sqlite_debouncer_coalesces_multiple_calls_and_runs_latest_input_once() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-debouncer".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let runs = Arc::new(AtomicUsize::new(0));
    let workflow = Arc::new(WorkflowFn::new("debounced-workflow", {
        let runs = runs.clone();
        move |_ctx, input: String| {
            let runs = runs.clone();
            Box::pin(async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok(input)
            })
        }
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    let first = ctx
        .debounce_workflow(
            "debounced-workflow",
            "same-key",
            Duration::from_millis(250),
            serde_json::json!("first"),
            DebounceOptions {
                debounce_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            },
        )
        .await
        .expect("first debounce");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let second = ctx
        .debounce_workflow(
            "debounced-workflow",
            "same-key",
            Duration::from_millis(250),
            serde_json::json!("second"),
            DebounceOptions {
                debounce_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            },
        )
        .await
        .expect("second debounce");

    assert_eq!(first.workflow_id(), second.workflow_id());

    let result = first
        .get_result(Some(Duration::from_secs(4)))
        .await
        .expect("debounced result");
    assert_eq!(result, serde_json::json!("second"));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sqlite_partitioned_queue_requires_key_and_dequeues_per_partition() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-partitioned-queue".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.register_workflow(Arc::new(WorkflowFn::new("noop", |_ctx, input: String| {
        Box::pin(async move { Ok(input) })
    })))
    .expect("register workflow");

    ctx.register_queue(
        "partitioned",
        QueueOptions {
            concurrency: Some(1),
            partition_queue: true,
            ..Default::default()
        },
    )
    .await
    .expect("register queue");

    let err = match ctx
        .enqueue_workflow(
            "partitioned",
            "noop",
            serde_json::json!("missing-key"),
            EnqueueOptions::default(),
        )
        .await
    {
        Ok(_) => panic!("partition key should be required"),
        Err(err) => err,
    };
    assert_eq!(err.code, JournioErrorCode::WorkflowExecutionError);

    let mut a1 = InitWorkflow::new_pending("p1-a", "noop", "local");
    a1.status = WorkflowStatusType::Enqueued;
    a1.input = Some(serde_json::json!("p1-a"));
    a1.queue_name = Some("partitioned".to_string());
    a1.queue_partition_key = Some("p1".to_string());
    db.init_workflow(a1).await.expect("seed p1-a");

    let mut a2 = InitWorkflow::new_pending("p1-b", "noop", "local");
    a2.status = WorkflowStatusType::Enqueued;
    a2.input = Some(serde_json::json!("p1-b"));
    a2.queue_name = Some("partitioned".to_string());
    a2.queue_partition_key = Some("p1".to_string());
    db.init_workflow(a2).await.expect("seed p1-b");

    let mut b1 = InitWorkflow::new_pending("p2-a", "noop", "local");
    b1.status = WorkflowStatusType::Enqueued;
    b1.input = Some(serde_json::json!("p2-a"));
    b1.queue_name = Some("partitioned".to_string());
    b1.queue_partition_key = Some("p2".to_string());
    db.init_workflow(b1).await.expect("seed p2-a");

    let first = db
        .dequeue_workflow("partitioned", "local")
        .await
        .expect("first dequeue")
        .expect("first candidate");
    assert_eq!(first.queue_partition_key.as_deref(), Some("p1"));

    let second = db
        .dequeue_workflow("partitioned", "local")
        .await
        .expect("second dequeue")
        .expect("second candidate");
    assert_eq!(second.queue_partition_key.as_deref(), Some("p2"));
}

#[tokio::test]
async fn sqlite_rate_limited_queue_blocks_second_start_within_window() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-rate-limit".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.register_workflow(Arc::new(WorkflowFn::new("fast", |_ctx, input: i64| {
        Box::pin(async move { Ok(input) })
    })))
    .expect("register workflow");

    ctx.register_queue(
        "rate-limited",
        QueueOptions {
            rate_limit_max: Some(1),
            rate_limit_period: Some(Duration::from_secs(60)),
            ..Default::default()
        },
    )
    .await
    .expect("register queue");

    ctx.enqueue_workflow(
        "rate-limited",
        "fast",
        serde_json::json!(1),
        EnqueueOptions::default(),
    )
    .await
    .expect("enqueue first");
    ctx.enqueue_workflow(
        "rate-limited",
        "fast",
        serde_json::json!(2),
        EnqueueOptions::default(),
    )
    .await
    .expect("enqueue second");

    let first = ctx
        .run_queue_once("rate-limited")
        .await
        .expect("drain first");
    assert!(first.is_some());

    let second = ctx
        .run_queue_once("rate-limited")
        .await
        .expect("drain second");
    assert!(second.is_none());
}

#[tokio::test]
async fn sqlite_cancel_resume_and_children_management_work() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-management".to_string(),
        system_db: Some(db.clone()),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");

    let mut parent = InitWorkflow::new_pending("parent-wf", "parent", "local");
    parent.input = Some(serde_json::json!(null));
    db.init_workflow(parent).await.expect("seed parent");

    let mut child = InitWorkflow::new_pending("child-wf", "child", "local");
    child.status = WorkflowStatusType::Enqueued;
    child.input = Some(serde_json::json!(null));
    child.queue_name = Some("jobs".to_string());
    child.parent_workflow_id = Some("parent-wf".to_string());
    db.init_workflow(child).await.expect("seed child");

    let mut grandchild = InitWorkflow::new_pending("grandchild-wf", "child", "local");
    grandchild.status = WorkflowStatusType::Enqueued;
    grandchild.input = Some(serde_json::json!(null));
    grandchild.queue_name = Some("jobs".to_string());
    grandchild.parent_workflow_id = Some("child-wf".to_string());
    db.init_workflow(grandchild).await.expect("seed grandchild");

    let mut success = InitWorkflow::new_pending("done-wf", "done", "local");
    success.input = Some(serde_json::json!(null));
    db.init_workflow(success).await.expect("seed done");
    db.record_workflow_result(
        "done-wf",
        WorkflowStatusType::Success,
        Some(&serde_json::json!("ok")),
        None,
    )
    .await
    .expect("mark done");

    let children = ctx
        .get_workflow_children("parent-wf")
        .await
        .expect("children");
    let child_ids: Vec<&str> = children
        .iter()
        .map(|workflow| workflow.id.as_str())
        .collect();
    assert_eq!(child_ids, vec!["child-wf", "grandchild-wf"]);

    assert!(ctx.cancel_workflow("child-wf").await.expect("cancel child"));
    let cancelled = db
        .get_workflow_status("child-wf")
        .await
        .expect("cancelled status")
        .expect("cancelled row");
    assert_eq!(cancelled.status, WorkflowStatusType::Cancelled);
    assert!(cancelled.queue_name.is_none());
    assert!(cancelled.started_at.is_none());
    assert!(cancelled.completed_at.is_some());

    let resumed = ctx
        .resume_workflow("child-wf", Some("resumed-jobs"))
        .await
        .expect("resume child");
    assert!(resumed);
    let resumed_status = db
        .get_workflow_status("child-wf")
        .await
        .expect("resumed status")
        .expect("resumed row");
    assert_eq!(resumed_status.status, WorkflowStatusType::Enqueued);
    assert_eq!(resumed_status.queue_name.as_deref(), Some("resumed-jobs"));
    assert!(resumed_status.completed_at.is_none());

    let found = ctx
        .resume_workflow("done-wf", Some("ignored"))
        .await
        .expect("resume done");
    assert!(found);
    let done_status = db
        .get_workflow_status("done-wf")
        .await
        .expect("done status")
        .expect("done row");
    assert_eq!(done_status.status, WorkflowStatusType::Success);
}

#[tokio::test]
async fn sqlite_fork_workflow_reuses_prior_steps_and_copies_events_and_streams() {
    let db = setup().await;

    let config = Config {
        app_name: "sqlite-fork".to_string(),
        system_db: Some(db.clone()),
        scheduler_polling_interval: Duration::from_millis(100),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let step1_runs = Arc::new(AtomicUsize::new(0));
    let step2_runs = Arc::new(AtomicUsize::new(0));

    let step1 = Arc::new(journio_core::StepFunc::new("step1", {
        let step1_runs = step1_runs.clone();
        move |_ctx| {
            let step1_runs = step1_runs.clone();
            Box::pin(async move {
                step1_runs.fetch_add(1, Ordering::SeqCst);
                Ok("first".to_string())
            })
        }
    }));
    let step2 = Arc::new(journio_core::StepFunc::new("step2", {
        let step2_runs = step2_runs.clone();
        move |_ctx| {
            let step2_runs = step2_runs.clone();
            Box::pin(async move {
                step2_runs.fetch_add(1, Ordering::SeqCst);
                Ok(42_i64)
            })
        }
    }));

    let workflow = Arc::new(WorkflowFn::new("forkable", {
        let step1 = step1.clone();
        let step2 = step2.clone();
        move |ctx, _input: ()| {
            let step1 = step1.clone();
            let step2 = step2.clone();
            Box::pin(async move {
                let first: String =
                    serde_json::from_value(ctx.run_as_step(step1).await?).expect("first");
                ctx.set_event("status", serde_json::json!("from-step1"))
                    .await?;
                ctx.write_stream("values", serde_json::json!("stream1"))
                    .await?;
                let second: i64 =
                    serde_json::from_value(ctx.run_as_step(step2).await?).expect("second");
                let event = ctx
                    .get_event(ctx.workflow_id(), "status", Duration::from_secs(1))
                    .await?;
                ctx.write_stream("values", serde_json::json!(format!("stream{second}")))
                    .await?;
                Ok(serde_json::json!({
                    "first": first,
                    "second": second,
                    "event": event,
                }))
            })
        }
    }));
    ctx.register_workflow(workflow).expect("register workflow");

    let original = ctx
        .run_workflow("forkable", serde_json::json!(null))
        .await
        .expect("run original");
    let original_result = original
        .get_result(Some(Duration::from_secs(3)))
        .await
        .expect("original result");
    assert_eq!(
        original_result,
        serde_json::json!({"first":"first","second":42,"event":"from-step1"})
    );

    let forked = ctx
        .fork_workflow(
            original.workflow_id(),
            ForkWorkflowOptions {
                start_step: 4,
                ..Default::default()
            },
        )
        .await
        .expect("fork workflow");
    let forked_result = forked
        .get_result(Some(Duration::from_secs(3)))
        .await
        .expect("forked result");
    assert_eq!(
        forked_result,
        serde_json::json!({"first":"first","second":42,"event":"from-step1"})
    );

    assert_eq!(step1_runs.load(Ordering::SeqCst), 1);
    assert_eq!(step2_runs.load(Ordering::SeqCst), 2);

    let original_status = db
        .get_workflow_status(original.workflow_id())
        .await
        .expect("original status")
        .expect("original row");
    assert!(original_status.was_forked_from);

    let forked_status = db
        .get_workflow_status(forked.workflow_id())
        .await
        .expect("forked status")
        .expect("forked row");
    assert_eq!(
        forked_status.forked_from.as_deref(),
        Some(original.workflow_id())
    );

    let (forked_stream, closed) = ctx
        .read_stream(
            forked.workflow_id(),
            "values",
            ReadStreamOptions {
                snapshot: true,
                from_offset: 0,
            },
        )
        .await
        .expect("forked stream");
    assert_eq!(
        forked_stream,
        vec![serde_json::json!("stream1"), serde_json::json!("stream42")]
    );
    assert!(!closed);
}

#[tokio::test]
async fn sqlite_dequeue_workflow_picks_highest_priority_and_sets_runtime_fields() {
    let db = setup().await;

    let mut low = InitWorkflow::new_pending("sqlite-queue-low", "queue-workflow", "seed");
    low.status = WorkflowStatusType::Enqueued;
    low.queue_name = Some("emails".to_string());
    low.priority = 10;
    low.input = Some(serde_json::json!("low"));
    db.init_workflow(low).await.expect("seed low");

    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut high = InitWorkflow::new_pending("sqlite-queue-high", "queue-workflow", "seed");
    high.status = WorkflowStatusType::Enqueued;
    high.queue_name = Some("emails".to_string());
    high.priority = 1;
    high.timeout = Some(Duration::from_secs(30));
    high.input = Some(serde_json::json!("high"));
    db.init_workflow(high).await.expect("seed high");

    let dequeued = db
        .dequeue_workflow("emails", "worker-a")
        .await
        .expect("dequeue")
        .expect("workflow");
    assert_eq!(dequeued.id, "sqlite-queue-high");
    assert_eq!(dequeued.status, WorkflowStatusType::Pending);
    assert_eq!(dequeued.executor_id, "worker-a");
    assert!(dequeued.started_at.is_some());
    assert!(dequeued.deadline.is_some());

    let low_status = db
        .get_workflow_status("sqlite-queue-low")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(low_status.status, WorkflowStatusType::Enqueued);
}

#[tokio::test]
async fn sqlite_dequeue_workflow_respects_delay_until() {
    let db = setup().await;

    let mut delayed = InitWorkflow::new_pending("sqlite-queue-delayed", "queue-workflow", "seed");
    delayed.status = WorkflowStatusType::Delayed;
    delayed.queue_name = Some("timers".to_string());
    delayed.delay_until = Some(Utc::now() + chrono::Duration::minutes(5));
    db.init_workflow(delayed).await.expect("seed delayed");

    let first = db
        .dequeue_workflow("timers", "worker-a")
        .await
        .expect("dequeue");
    assert!(
        first.is_none(),
        "future-delayed workflow should not dequeue"
    );

    sqlx::query("UPDATE workflow_status SET delay_until_epoch_ms = ?1 WHERE workflow_uuid = ?2")
        .bind((Utc::now() - chrono::Duration::seconds(1)).timestamp_millis())
        .bind("sqlite-queue-delayed")
        .execute(db.pool())
        .await
        .expect("make delayed workflow due");

    let second = db
        .dequeue_workflow("timers", "worker-a")
        .await
        .expect("dequeue due workflow")
        .expect("workflow");
    assert_eq!(second.id, "sqlite-queue-delayed");
    assert_eq!(second.status, WorkflowStatusType::Pending);
}

#[tokio::test]
async fn sqlite_queue_deduplication_rejects_conflicting_enqueue() {
    let db = setup().await;

    let mut first = InitWorkflow::new_pending("sqlite-dedup-one", "queue-workflow", "seed");
    first.status = WorkflowStatusType::Enqueued;
    first.queue_name = Some("dedup".to_string());
    first.deduplication_id = Some("same-key".to_string());
    db.init_workflow(first).await.expect("seed dedup row");

    let mut second = InitWorkflow::new_pending("sqlite-dedup-two", "queue-workflow", "seed");
    second.status = WorkflowStatusType::Enqueued;
    second.queue_name = Some("dedup".to_string());
    second.deduplication_id = Some("same-key".to_string());
    let err = db
        .init_workflow(second)
        .await
        .expect_err("dedup conflict expected");
    assert_eq!(err.code, JournioErrorCode::QueueDeduplicated);
}

#[tokio::test]
async fn sqlite_recovery_attempts_can_move_workflow_to_dead_letter_queue() {
    let db = setup().await;

    let mut init = InitWorkflow::new_pending("sqlite-dlq", "recovery-workflow", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");

    let mut retry_one = InitWorkflow::new_pending("sqlite-dlq", "recovery-workflow", "local");
    retry_one.increment_attempts = true;
    retry_one.max_retries = 1;
    db.init_workflow(retry_one)
        .await
        .expect("first retry should stay pending");

    let mut retry_two = InitWorkflow::new_pending("sqlite-dlq", "recovery-workflow", "local");
    retry_two.increment_attempts = true;
    retry_two.max_retries = 1;
    let err = db
        .init_workflow(retry_two)
        .await
        .expect_err("workflow should dead-letter");
    assert_eq!(err.code, JournioErrorCode::DeadLetterQueueError);

    let status = db
        .get_workflow_status("sqlite-dlq")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(
        status.status,
        WorkflowStatusType::MaxRecoveryAttemptsExceeded
    );
    assert!(status.queue_name.is_none());
}
