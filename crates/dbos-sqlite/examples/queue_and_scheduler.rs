use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dbos_core::{
    Config, DbosContext, EnqueueOptions, ScheduleOptions, ScheduledWorkflowInput, WorkflowFn,
};
use dbos_sqlite::SqliteSystemDatabase;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = SqliteSystemDatabase::connect("sqlite://dbos-example-queue.db").await?;

    let mut config = Config::default();
    config.app_name = "sqlite-queue-scheduler-example".to_string();
    config.scheduler_polling_interval = Duration::from_millis(200);
    config.system_db = Some(Arc::new(database));

    let ctx = DbosContext::new(config).await?;
    let scheduled_runs = Arc::new(AtomicUsize::new(0));

    ctx.register_workflow(Arc::new(WorkflowFn::new(
        "queued-add-one",
        |_ctx, input: i64| Box::pin(async move { Ok(input + 1) }),
    )))?;
    ctx.register_workflow(Arc::new(WorkflowFn::new(
        "scheduled-log",
        {
            let scheduled_runs = scheduled_runs.clone();
            move |_ctx, input: ScheduledWorkflowInput| {
                let scheduled_runs = scheduled_runs.clone();
                Box::pin(async move {
                    scheduled_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(serde_json::json!({
                        "scheduled_time": input.scheduled_time,
                        "context": input.context,
                    }))
                })
            }
        },
    )))?;

    ctx.launch().await?;

    let queued = ctx
        .enqueue_workflow(
            "jobs",
            "queued-add-one",
            serde_json::json!(41),
            EnqueueOptions::default(),
        )
        .await?;
    let queued_result = queued.get_result(Some(Duration::from_secs(2))).await?;
    println!("queued workflow {} => {}", queued.workflow_id(), queued_result);

    ctx.register_schedule(
        "every-second-example",
        "scheduled-log",
        "*/1 * * * * *",
        serde_json::json!({"source":"example"}),
        ScheduleOptions::default(),
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if scheduled_runs.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    println!(
        "scheduled workflow fired {} time(s)",
        scheduled_runs.load(Ordering::SeqCst)
    );

    ctx.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
