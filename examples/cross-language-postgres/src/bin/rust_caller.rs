// See journio-core: `JournioError` is slightly over the `result_large_err`
// size threshold and boxing it would be a cascading public-API break.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use cross_language_postgres::{
    NODE_WORKFLOW, enqueue_options, node_queue, postgres_db, print_json, sample_fraud_check_input,
    wait_for_terminal, workflow_id,
};
use journio_core::{Client, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::try_init().ok();

    let config = Config {
        app_name: "cross-language-rust-caller".to_string(),
        system_db: Some(postgres_db().await?),
        ..Default::default()
    };
    let client = client_with_retry(config).await?;

    let id = workflow_id("rust-calls-node");
    let queue = node_queue();
    let input = sample_fraud_check_input();
    let handle = client
        .enqueue(
            &queue,
            NODE_WORKFLOW,
            serde_json::to_value(input)?,
            enqueue_options(id.clone()),
        )
        .await?;

    print_json(&serde_json::json!({
        "event": "started",
        "workflowID": handle.workflow_id(),
        "queue": queue,
        "workflow": NODE_WORKFLOW,
    }));

    let result = wait_for_terminal(&handle, Duration::from_secs(20)).await?;
    print_json(&serde_json::json!({
        "event": "result",
        "workflowId": handle.workflow_id(),
        "result": result,
    }));

    client.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

async fn client_with_retry(
    config: Config,
) -> Result<std::sync::Arc<Client>, journio_core::JournioError> {
    let mut last_error = None;
    for _ in 0..20 {
        match Client::new(config.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        journio_core::JournioError::new(
            journio_core::JournioErrorCode::InitializationError,
            "failed to create Journio client",
        )
    }))
}
