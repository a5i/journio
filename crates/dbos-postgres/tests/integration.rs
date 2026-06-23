use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use dbos_core::{
    Config, DbosContext, DbosErrorCode, DebounceOptions, EnqueueOptions, ForkWorkflowOptions,
    InitWorkflow, QueueOptions, ReadStreamOptions, ScheduleOptions, ScheduleStatus,
    ScheduledWorkflowInput, SystemDatabase, WorkflowFn, WorkflowStatusType,
};
use dbos_postgres::{PostgresSystemDatabase, latest_version};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

struct Harness {
    _container: Box<dyn Any>,
    db: Arc<PostgresSystemDatabase>,
    schema: String,
}

async fn setup() -> Harness {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container
        .get_host()
        .await
        .expect("postgres host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port mapping");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let schema = format!("dbos_{}", uuid::Uuid::new_v4().simple());

    let db =
        PostgresSystemDatabase::connect(&database_url, &schema).expect("connect postgres system db");
    {
        let client = db.pool().get().await.expect("pool client");
        client
            .execute("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public", &[])
            .await
            .expect("enable pgcrypto");
    }
    db.migrate().await.expect("migrate postgres");

    Harness {
        _container: Box::new(container),
        db: Arc::new(db),
        schema,
    }
}

async fn seed_workflow(db: &Arc<PostgresSystemDatabase>, id: &str) {
    let mut init = InitWorkflow::new_pending(id, "test-workflow", "local");
    init.input = Some(serde_json::json!(null));
    db.init_workflow(init).await.expect("seed workflow");
}

#[tokio::test]
async fn postgres_foundation_migrations_and_schema_are_correct() {
    let harness = setup().await;

    let client = harness.db.pool().get().await.expect("pool client");
    let version: i64 = client
        .query_one(
            &format!(
                "SELECT version FROM \"{}\".dbos_migrations",
                harness.schema
            ),
            &[],
        )
        .await
        .expect("select migration version")
        .get(0);
    assert_eq!(version, latest_version());

    for table in [
        "workflow_status",
        "operation_outputs",
        "notifications",
        "workflow_events",
        "workflow_events_history",
        "workflow_schedules",
        "application_versions",
        "queues",
    ] {
        let name: String = client
            .query_one(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
                &[&harness.schema, &table],
            )
            .await
            .expect("table exists")
            .get(0);
        assert_eq!(name, table);
    }
}

#[tokio::test]
async fn postgres_lifecycle_checkpoints_notifications_and_events_work() {
    let harness = setup().await;
    let db = harness.db;

    seed_workflow(&db, "wf-1").await;
    let status = db
        .get_workflow_status("wf-1")
        .await
        .expect("status")
        .expect("row");
    assert_eq!(status.status, WorkflowStatusType::Pending);

    db.record_step_output(&dbos_core::StepRecord {
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
    assert_eq!(err.code, DbosErrorCode::NonExistentWorkflowError);
}

#[tokio::test]
async fn postgres_listen_notify_wakes_notification_waiter() {
    let harness = setup().await;
    let db = harness.db;

    db.launch().await.expect("launch postgres listener");
    tokio::time::sleep(Duration::from_millis(250)).await;
    seed_workflow(&db, "notify-waiter").await;

    let started = Instant::now();
    let waiter = tokio::spawn({
        let db = db.clone();
        async move {
            db.wait_for_notification("notify-waiter", "topic", Duration::from_secs(5))
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    db.send("notify-waiter", "topic", &serde_json::json!("hello"))
        .await
        .expect("send notification");

    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter should wake before timeout")
        .expect("join waiter")
        .expect("wait_for_notification");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn postgres_listen_notify_wakes_event_waiter() {
    let harness = setup().await;
    let db = harness.db;

    db.launch().await.expect("launch postgres listener");
    tokio::time::sleep(Duration::from_millis(250)).await;
    seed_workflow(&db, "event-waiter").await;

    let started = Instant::now();
    let waiter = tokio::spawn({
        let db = db.clone();
        async move {
            db.wait_for_event("event-waiter", "status", Duration::from_secs(5))
                .await
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    db.set_event("event-waiter", "status", &serde_json::json!("done"), 1)
        .await
        .expect("set event");

    tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("event waiter should wake before timeout")
        .expect("join waiter")
        .expect("wait_for_event");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn postgres_context_queue_end_to_end_enqueue_and_drain_once() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-queue-e2e".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");

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
async fn postgres_launch_background_queue_worker_drains_enqueued_workflow() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-bg-queue".to_string();
    config.system_db = Some(db.clone());
    config.scheduler_polling_interval = Duration::from_millis(100);
    let ctx = DbosContext::new(config).await.expect("context");

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
async fn postgres_scheduler_triggers_scheduled_workflow_and_executes_it() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-scheduler".to_string();
    config.system_db = Some(db.clone());
    config.scheduler_polling_interval = Duration::from_millis(100);
    let ctx = DbosContext::new(config).await.expect("context");

    let executions = Arc::new(AtomicUsize::new(0));
    let scheduled = Arc::new(WorkflowFn::new(
        "scheduled-workflow",
        {
            let executions = executions.clone();
            move |_ctx, input: ScheduledWorkflowInput| {
                let executions = executions.clone();
                Box::pin(async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                    Ok(input.scheduled_time.to_rfc3339())
                })
            }
        },
    ));
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
async fn postgres_scheduler_automatic_backfill_executes_missed_ticks() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-scheduler-backfill".to_string();
    config.system_db = Some(db.clone());
    config.scheduler_polling_interval = Duration::from_millis(100);
    let ctx = DbosContext::new(config).await.expect("context");

    let executions = Arc::new(AtomicUsize::new(0));
    let scheduled = Arc::new(WorkflowFn::new(
        "backfill-workflow",
        {
            let executions = executions.clone();
            move |_ctx, _input: ScheduledWorkflowInput| {
                let executions = executions.clone();
                Box::pin(async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                    Ok(serde_json::json!("ok"))
                })
            }
        },
    ));
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
async fn postgres_streams_snapshot_and_close_semantics_work() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-streams".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let stream_workflow = Arc::new(WorkflowFn::new("stream-workflow", |ctx, _: ()| {
        Box::pin(async move {
            ctx.write_stream("values", serde_json::json!("value1")).await?;
            ctx.write_stream("values", serde_json::json!("value2")).await?;
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

    tokio::time::sleep(Duration::from_millis(200)).await;

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
    assert_eq!(snapshot, vec![serde_json::json!("value1"), serde_json::json!("value2")]);
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
    assert_eq!(values, vec![serde_json::json!("value1"), serde_json::json!("value2")]);
    assert!(closed);
}

#[tokio::test]
async fn postgres_debouncer_coalesces_multiple_calls_and_runs_latest_input_once() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-debouncer".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");
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
async fn postgres_partitioned_queue_requires_key_and_dequeues_per_partition() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-partitioned-queue".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");
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
    assert_eq!(err.code, DbosErrorCode::WorkflowExecutionError);

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
async fn postgres_rate_limited_queue_blocks_second_start_within_window() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-rate-limit".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");
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
async fn postgres_cancel_resume_and_children_management_work() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-management".to_string();
    config.system_db = Some(db.clone());
    let ctx = DbosContext::new(config).await.expect("context");

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
    let child_ids: Vec<&str> = children.iter().map(|workflow| workflow.id.as_str()).collect();
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
async fn postgres_fork_workflow_reuses_prior_steps_and_copies_events_and_streams() {
    let harness = setup().await;
    let db = harness.db;

    let mut config = Config::default();
    config.app_name = "postgres-fork".to_string();
    config.system_db = Some(db.clone());
    config.scheduler_polling_interval = Duration::from_millis(100);
    let ctx = DbosContext::new(config).await.expect("context");
    ctx.launch().await.expect("launch");

    let step1_runs = Arc::new(AtomicUsize::new(0));
    let step2_runs = Arc::new(AtomicUsize::new(0));

    let step1 = Arc::new(dbos_core::StepFunc::new("step1", {
        let step1_runs = step1_runs.clone();
        move |_ctx| {
            let step1_runs = step1_runs.clone();
            Box::pin(async move {
                step1_runs.fetch_add(1, Ordering::SeqCst);
                Ok("first".to_string())
            })
        }
    }));
    let step2 = Arc::new(dbos_core::StepFunc::new("step2", {
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
                ctx.set_event("status", serde_json::json!("from-step1")).await?;
                ctx.write_stream("values", serde_json::json!("stream1")).await?;
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
