use std::sync::Arc;
use std::time::Duration;

use journio_core::{Config, JournioContext, WorkflowFn};
use journio_sqlite::SqliteSystemDatabase;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = SqliteSystemDatabase::connect("sqlite://journio-example-basic.db").await?;

    let config = Config {
        app_name: "sqlite-basic-example".to_string(),
        system_db: Some(Arc::new(database)),
        ..Default::default()
    };

    let ctx = JournioContext::new(config).await?;
    ctx.register_workflow(Arc::new(WorkflowFn::new(
        "double-number",
        |_ctx, input: i64| Box::pin(async move { Ok(input * 2) }),
    )))?;
    ctx.launch().await?;

    let handle = ctx
        .run_workflow("double-number", serde_json::json!(21))
        .await?;
    let result = handle.get_result(Some(Duration::from_secs(2))).await?;
    println!("workflow {} => {}", handle.workflow_id(), result);

    ctx.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
