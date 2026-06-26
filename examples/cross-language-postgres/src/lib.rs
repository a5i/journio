use std::env;
use std::sync::Arc;
use std::time::Duration;

use journio_core::{
    Config, EnqueueOptions, JournioContext, JournioError, JournioErrorCode, QueueOptions, StepFunc,
    WorkflowFn, WorkflowHandle, WorkflowStatusType,
};
use journio_postgres::PostgresSystemDatabase;
use serde::{Deserialize, Serialize};

pub const RUST_WORKFLOW: &str = "rust_price_quote";
pub const NODE_WORKFLOW: &str = "node_fraud_check";
pub const DEFAULT_SCHEMA: &str = "journio_cross_language";
pub const DEFAULT_RUST_QUEUE: &str = "cross_language_rust_queue";
pub const DEFAULT_NODE_QUEUE: &str = "cross_language_node_queue";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceQuoteInput {
    pub sku: String,
    pub quantity: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriceQuoteOutput {
    pub engine: String,
    pub sku: String,
    pub quantity: i64,
    pub total_cents: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FraudCheckInput {
    pub order_id: String,
    pub amount_cents: i64,
}

pub fn database_url() -> Result<String, JournioError> {
    env::var("JOURNIO_SYSTEM_DATABASE_URL").map_err(|_| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            "JOURNIO_SYSTEM_DATABASE_URL is required",
        )
    })
}

pub fn schema() -> String {
    env::var("JOURNIO_SYSTEM_DATABASE_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string())
}

pub fn rust_queue() -> String {
    env::var("JOURNIO_RUST_QUEUE").unwrap_or_else(|_| DEFAULT_RUST_QUEUE.to_string())
}

pub fn node_queue() -> String {
    env::var("JOURNIO_NODE_QUEUE").unwrap_or_else(|_| DEFAULT_NODE_QUEUE.to_string())
}

pub fn exit_after_workflow_id() -> Option<String> {
    env::var("JOURNIO_EXIT_AFTER_WORKFLOW_ID")
        .ok()
        .filter(|value| !value.is_empty())
}

pub fn workflow_id(prefix: &str) -> String {
    env::var("JOURNIO_WORKFLOW_ID")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("{prefix}-{}", uuid::Uuid::new_v4()))
}

pub async fn postgres_db() -> Result<Arc<PostgresSystemDatabase>, JournioError> {
    let url = database_url()?;
    let schema = schema();
    let mut last_error = None;
    for _ in 0..20 {
        match PostgresSystemDatabase::connect(&url, &schema) {
            Ok(db) => return Ok(Arc::new(db)),
            Err(err) => last_error = Some(err),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(last_error.unwrap_or_else(|| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            "failed to connect to Postgres",
        )
    }))
}

pub async fn runtime(
    app_name: &str,
    listen_queues: Option<Vec<String>>,
) -> Result<Arc<JournioContext>, JournioError> {
    let mut config = Config::default();
    config.app_name = app_name.to_string();
    config.system_db = Some(postgres_db().await?);
    config.listen_queues = listen_queues;
    config.scheduler_polling_interval = Duration::from_millis(250);
    JournioContext::new(config).await
}

pub async fn launch_with_retry(ctx: &Arc<JournioContext>) -> Result<(), JournioError> {
    let mut last_error = None;
    for _ in 0..20 {
        match ctx.launch().await {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            "failed to launch Journio runtime",
        )
    }))
}

pub fn rust_price_quote_workflow() -> Arc<dyn journio_core::Workflow> {
    Arc::new(WorkflowFn::new(
        RUST_WORKFLOW,
        |ctx, input: PriceQuoteInput| {
            Box::pin(async move {
                ctx.set_event("quote_status", serde_json::json!("validating"))
                    .await?;
                ctx.write_stream(
                    "quote_updates",
                    serde_json::json!({"stage": "received", "sku": input.sku}),
                )
                .await?;

                let validate = Arc::new(StepFunc::new("rust_validate_quote", {
                    let sku = input.sku.clone();
                    let quantity = input.quantity;
                    move |_ctx| {
                        let sku = sku.clone();
                        Box::pin(async move {
                            if quantity <= 0 {
                                return Err(JournioError::new(
                                    JournioErrorCode::StepExecutionError,
                                    "quantity must be positive",
                                ));
                            }
                            Ok(format!("{quantity} units of {sku} validated"))
                        })
                    }
                }));
                let _: serde_json::Value = ctx.run_as_step(validate).await?;

                ctx.set_event("quote_status", serde_json::json!("pricing"))
                    .await?;
                let price = Arc::new(StepFunc::new("rust_calculate_price", {
                    let sku = input.sku.clone();
                    let quantity = input.quantity;
                    move |_ctx| {
                        let sku = sku.clone();
                        Box::pin(async move {
                            let unit_cents = if sku.starts_with("enterprise") {
                                7_500
                            } else {
                                1_999
                            };
                            Ok(unit_cents * quantity)
                        })
                    }
                }));
                let total_cents: i64 = serde_json::from_value(ctx.run_as_step(price).await?)
                    .map_err(|err| {
                        JournioError::new(
                            JournioErrorCode::WorkflowUnexpectedTypeError,
                            err.to_string(),
                        )
                    })?;

                ctx.write_stream(
                    "quote_updates",
                    serde_json::json!({"stage": "priced", "totalCents": total_cents}),
                )
                .await?;
                ctx.close_stream("quote_updates").await?;
                ctx.set_event("quote_status", serde_json::json!("complete"))
                    .await?;

                Ok(PriceQuoteOutput {
                    engine: "rust".to_string(),
                    sku: input.sku,
                    quantity: input.quantity,
                    total_cents,
                })
            })
        },
    ))
}

pub async fn register_queue(
    ctx: &Arc<JournioContext>,
    queue_name: &str,
) -> Result<(), JournioError> {
    ctx.register_queue(
        queue_name,
        QueueOptions {
            concurrency: Some(1),
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

pub async fn wait_for_terminal(
    handle: &WorkflowHandle,
    timeout: Duration,
) -> Result<serde_json::Value, JournioError> {
    handle.get_result(Some(timeout)).await
}

pub async fn wait_until_terminal(
    ctx: &Arc<JournioContext>,
    workflow_id: &str,
) -> Result<(), JournioError> {
    loop {
        let status = match ctx.workflow_handle(workflow_id).get_status().await {
            Ok(status) => status,
            Err(err) if err.code == JournioErrorCode::NonExistentWorkflowError => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(err) => return Err(err),
        };
        match status.status {
            WorkflowStatusType::Success
            | WorkflowStatusType::Error
            | WorkflowStatusType::Cancelled
            | WorkflowStatusType::MaxRecoveryAttemptsExceeded => return Ok(()),
            WorkflowStatusType::Pending
            | WorkflowStatusType::Enqueued
            | WorkflowStatusType::Delayed => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

pub fn print_json<T: Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string(value).expect("serialize json line")
    );
}

pub fn sample_price_quote_input() -> PriceQuoteInput {
    PriceQuoteInput {
        sku: env::var("JOURNIO_QUOTE_SKU").unwrap_or_else(|_| "starter-widget".to_string()),
        quantity: env::var("JOURNIO_QUOTE_QUANTITY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3),
    }
}

pub fn sample_fraud_check_input() -> FraudCheckInput {
    FraudCheckInput {
        order_id: env::var("JOURNIO_ORDER_ID").unwrap_or_else(|_| "order-1001".to_string()),
        amount_cents: env::var("JOURNIO_AMOUNT_CENTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(12_500),
    }
}

pub fn enqueue_options(workflow_id: String) -> EnqueueOptions {
    EnqueueOptions {
        workflow_id: Some(workflow_id),
        ..Default::default()
    }
}
