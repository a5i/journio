use std::time::Duration;

use cross_language_postgres::{
    exit_after_workflow_id, launch_with_retry, print_json, register_queue, runtime,
    rust_price_quote_workflow, rust_queue, wait_until_terminal,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::try_init().ok();

    let queue = rust_queue();
    let ctx = runtime("cross-language-rust-worker", Some(vec![queue.clone()])).await?;
    ctx.register_workflow(rust_price_quote_workflow())?;
    launch_with_retry(&ctx).await?;
    register_queue(&ctx, &queue).await?;

    print_json(&json!({
        "event": "ready",
        "worker": "rust",
        "queue": queue,
        "workflow": cross_language_postgres::RUST_WORKFLOW,
    }));

    if let Some(workflow_id) = exit_after_workflow_id() {
        wait_until_terminal(&ctx, &workflow_id).await?;
        print_json(&json!({
            "event": "observed-terminal",
            "worker": "rust",
            "workflowID": workflow_id,
        }));
    } else {
        tokio::signal::ctrl_c().await.ok();
    }

    ctx.shutdown(Duration::from_secs(5)).await?;
    Ok(())
}
