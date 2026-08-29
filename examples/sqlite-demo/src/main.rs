//! SQLite demo app — runs a Journio runtime on SQLite with the admin HTTP
//! server, registers several demo workflows, and seeds a little history so
//! the UI has something to show immediately.
//!
//! Run it, then open the Nuxt UI (../ui) or hit the API directly:
//!
//! ```sh
//! cargo run -p sqlite-demo
//! # API at http://localhost:3001
//! ```

mod workflows;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use journio_admin::AdminServer;
use journio_core::{
    Config, InitWorkflow, JournioContext, QueueOptions, SystemDatabase, WorkflowStatusType,
};
use journio_sqlite::SqliteSystemDatabase;
use tracing_subscriber::EnvFilter;

const DB_URL: &str = "sqlite://sqlite-demo.db";
const ADMIN_PORT: u16 = 3001;
const QUEUE_NAME: &str = "orders";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    // ---- connect + migrate -----------------------------------------------
    let db = SqliteSystemDatabase::connect(DB_URL).await?;
    db.migrate().await?;
    let db = Arc::new(db);

    let config = Config {
        app_name: "sqlite-demo".to_string(),
        system_db: Some(db.clone()),
        admin_server: true,
        admin_server_port: Some(ADMIN_PORT),
        // Poll the queue quickly so enqueued workflows show up fast in the UI.
        scheduler_polling_interval: Duration::from_millis(500),
        ..Default::default()
    };
    let ctx = JournioContext::new(config).await?;

    // ---- register demo workflows -----------------------------------------
    ctx.register_workflow(workflows::build_checkout_workflow())?;
    ctx.register_workflow(workflows::build_greet_workflow())?;
    ctx.register_workflow(workflows::build_flaky_workflow())?;
    ctx.register_workflow(workflows::build_long_running_workflow())?;

    // Register a queue so checkout can be enqueued.
    ctx.register_queue(
        QUEUE_NAME,
        QueueOptions {
            concurrency: Some(2),
            ..Default::default()
        },
    )
    .await?;

    // ---- seed a little history (so the UI isn't empty on first load) -----
    seed_history(&db).await;

    // ---- launch the runtime ----------------------------------------------
    ctx.launch().await?;

    // ---- start the admin server ------------------------------------------
    let server = AdminServer::new(ctx.clone(), ADMIN_PORT);
    let addr = server.start().await?;

    println!();
    println!("  ╔══════════════════════════════════════════════════╗");
    println!("  ║  Journio SQLite Demo is running                     ║");
    println!("  ╠══════════════════════════════════════════════════╣");
    println!("  ║  Admin API : http://{addr}                    ║");
    println!("  ║  Workflows : checkout, greet, flaky_task,        ║");
    println!("  ║              long_running                        ║");
    println!("  ║                                                  ║");
    println!("  ║  Try:                                            ║");
    println!("  ║    curl http://{addr}/workflows/registered  ║");
    println!("  ║    curl -XPOST http://{addr}/workflows/        ║");
    println!("  ║              greet/start -d '{{\"input\":\"World\"}}'   ║");
    println!("  ║                                                  ║");
    println!("  ║  UI: cd ../ui && npm run dev                     ║");
    println!("  ╚══════════════════════════════════════════════════╝");
    println!();

    // Run forever (Ctrl+C to stop).
    tokio::signal::ctrl_c().await.ok();
    eprintln!("\nShutting down...");
    ctx.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}

/// Seed a handful of completed/failed workflows so the history view in the UI
/// has content on first load.
async fn seed_history(db: &Arc<SqliteSystemDatabase>) {
    let now = Utc::now();

    // A couple of completed checkouts.
    for (id, item, qty, customer) in [
        ("seed-checkout-1", "Widget", 2, "alice"),
        ("seed-checkout-2", "Gadget", 1, "bob"),
        ("seed-checkout-3", "Gizmo", 5, "carol"),
    ] {
        let mut init = InitWorkflow::new_pending(id, "checkout", "seed");
        init.status = WorkflowStatusType::Success;
        init.input = Some(serde_json::json!({
            "item": item, "quantity": qty, "customer": customer
        }));
        init.queue_name = Some(QUEUE_NAME.to_string());
        db.init_workflow(init).await.ok();
        db.record_workflow_result(
            id,
            WorkflowStatusType::Success,
            Some(&serde_json::json!({
                "order_id": id, "total": qty * 1999, "status": "completed"
            })),
            None,
        )
        .await
        .ok();
        // Record the steps too.
        for (step_id, name) in ["validate_order", "charge_card", "ship_order"]
            .iter()
            .enumerate()
        {
            db.record_step_output(&journio_core::StepRecord {
                workflow_uuid: id.to_string(),
                function_id: step_id as i32 + 1,
                function_name: name.to_string(),
                output: Some(format!("\"{name} ok\"").to_string()),
                error: None,
                child_workflow_id: None,
            })
            .await
            .ok();
        }
    }

    // A failed flaky_task.
    let mut init = InitWorkflow::new_pending("seed-flaky-fail", "flaky_task", "seed");
    init.status = WorkflowStatusType::Error;
    init.input = Some(serde_json::json!(3));
    db.init_workflow(init).await.ok();
    db.record_workflow_result(
        "seed-flaky-fail",
        WorkflowStatusType::Error,
        None,
        Some("flaky task failed for seed 3 (odd)"),
    )
    .await
    .ok();
    db.record_step_output(&journio_core::StepRecord {
        workflow_uuid: "seed-flaky-fail".to_string(),
        function_id: 1,
        function_name: "risky_step".to_string(),
        output: None,
        error: Some("flaky task failed for seed 3 (odd)".to_string()),
        child_workflow_id: None,
    })
    .await
    .ok();

    // A successful flaky_task.
    let mut init = InitWorkflow::new_pending("seed-flaky-ok", "flaky_task", "seed");
    init.status = WorkflowStatusType::Success;
    init.input = Some(serde_json::json!(4));
    db.init_workflow(init).await.ok();
    db.record_workflow_result(
        "seed-flaky-ok",
        WorkflowStatusType::Success,
        Some(&serde_json::json!("flaky task succeeded: 8")),
        None,
    )
    .await
    .ok();

    // A couple of greetings.
    for (id, name) in [("seed-greet-1", "World"), ("seed-greet-2", "Journio")] {
        let mut init = InitWorkflow::new_pending(id, "greet", "seed");
        init.status = WorkflowStatusType::Success;
        init.input = Some(serde_json::json!(name));
        db.init_workflow(init).await.ok();
        db.record_workflow_result(
            id,
            WorkflowStatusType::Success,
            Some(&serde_json::json!(format!("Hello, {name}!"))),
            None,
        )
        .await
        .ok();
    }

    tracing::info!("seeded demo history ({} workflows)", 7);
    let _ = now; // (timestamps come from the DB)
}
