use std::sync::Arc;
use std::time::Duration;

use dbos_core::{Config, DbosContext, StepFunc, WorkflowFn};
use dbos_sqlite::SqliteSystemDatabase;

/// Event key published after each step completes — demonstrates durable
/// inter-workflow events (like the Go starter's STEPS_EVENT).
const STEPS_EVENT: &str = "steps_event";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- connect + migrate (SQLite for zero-config local dev) ------------
    let db = SqliteSystemDatabase::connect("sqlite://{{PROJECT_NAME}}.db").await?;
    db.migrate().await?;

    let mut config = Config::default();
    config.app_name = "{{PROJECT_NAME}}".to_string();
    config.system_db = Some(Arc::new(db));
    let ctx = DbosContext::new(config).await?;

    // ---- register the workflow -------------------------------------------
    //
    // Steps are plain async closures wrapped in StepFunc — no separate
    // registration needed. The workflow calls them via run_as_step, which
    // checkpoints each result so a crash mid-workflow resumes from the last
    // completed step.
    let workflow = Arc::new(WorkflowFn::new("example_workflow", move |ctx, _input: ()| {
        Box::pin(async move {
            ctx.run_as_step(Arc::new(StepFunc::new("step_one", |_ctx| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    println!("Step one completed!");
                    Ok("Step 1 completed")
                })
            })))
            .await?;
            ctx.set_event(STEPS_EVENT, serde_json::json!(1)).await?;

            ctx.run_as_step(Arc::new(StepFunc::new("step_two", |_ctx| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    println!("Step two completed!");
                    Ok("Step 2 completed")
                })
            })))
            .await?;
            ctx.set_event(STEPS_EVENT, serde_json::json!(2)).await?;

            ctx.run_as_step(Arc::new(StepFunc::new("step_three", |_ctx| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    println!("Step three completed!");
                    Ok("Step 3 completed")
                })
            })))
            .await?;
            ctx.set_event(STEPS_EVENT, serde_json::json!(3)).await?;

            Ok::<_, dbos_core::DbosError>("Workflow completed")
        })
    }));
    ctx.register_workflow(workflow)?;

    // ---- launch (migrations, recovery, scheduler, queue workers) --------
    ctx.launch().await?;
    println!("DBOS launched.");
    println!("Try: launch a workflow, then Ctrl+C to crash. Restart recovers it.");

    // ---- kick off a workflow ---------------------------------------------
    let handle = ctx
        .run_workflow("example_workflow", serde_json::json!(null))
        .await?;
    println!("Started workflow: {}", handle.workflow_id());
    println!(
        "Watch it: dbos --db-url sqlite://{{PROJECT_NAME}}.db workflow steps {}",
        handle.workflow_id()
    );

    // Wait for completion or Ctrl+C.
    tokio::signal::ctrl_c().await.ok();
    println!("\nShutting down...");
    ctx.shutdown(Duration::from_secs(10)).await?;
    Ok(())
}
