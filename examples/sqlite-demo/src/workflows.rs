//! Demo workflows for the SQLite demo app.
//!
//! Each workflow exercises a different durable-execution feature so the UI
//! has something interesting to show: multi-step checkpointing, durable
//! events (`set_event`/`get_event`), queue-based execution, and error
//! handling.

use std::sync::Arc;
use std::time::Duration;

use journio_core::value::Interchange;
use journio_core::{StepFunc, WorkflowFn};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Workflow 1: "checkout" — a multi-step e-commerce-style workflow.
// ---------------------------------------------------------------------------

/// Input for the checkout workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutInput {
    pub item: String,
    pub quantity: i64,
    pub customer: String,
}

/// Output of the checkout workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckoutOutput {
    pub order_id: String,
    pub total: i64,
    pub status: String,
}

/// Build and return the "checkout" workflow as an erased trait object.
/// It runs three durable steps (validate → charge → ship), publishing
/// progress via durable events after each step.
pub fn build_checkout_workflow() -> Arc<dyn journio_core::Workflow> {
    Arc::new(WorkflowFn::new("checkout", |ctx, input: Option<CheckoutInput>| {
        Box::pin(async move {
            // Default to a sample order when the UI sends no input (null),
            // so the demo works out-of-the-box by clicking Start.
            let input = input.unwrap_or_else(|| CheckoutInput {
                item: "Widget".to_string(),
                quantity: 1,
                customer: "guest".to_string(),
            });
            let item = input.item.clone();
            let qty = input.quantity;
            let customer = input.customer.clone();

            // Step 1: validate.
            let validate_step = Arc::new(StepFunc::new("validate_order", {
                let item = item.clone();
                move |_ctx| {
                    let item = item.clone();
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        Ok(format!("validated {qty}x {item}"))
                    })
                }
            }));
            let _: Interchange = ctx.run_as_step(validate_step).await?;
            ctx.set_event("progress", serde_json::json!("validated")).await?;

            // Step 2: charge.
            let charge_step = Arc::new(StepFunc::new("charge_card", {
                let customer = customer.clone();
                move |_ctx| {
                    let customer = customer.clone();
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(800)).await;
                        let total = qty * 1999;
                        Ok(format!("charged {customer} ${}", total as f64 / 100.0))
                    })
                }
            }));
            let _: Interchange = ctx.run_as_step(charge_step).await?;
            ctx.set_event("progress", serde_json::json!("charged")).await?;

            // Step 3: ship.
            let ship_step = Arc::new(StepFunc::new("ship_order", {
                let item = item.clone();
                move |_ctx| {
                    let item = item.clone();
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(600)).await;
                        Ok(format!("shipped {item}"))
                    })
                }
            }));
            let _: Interchange = ctx.run_as_step(ship_step).await?;
            ctx.set_event("progress", serde_json::json!("shipped")).await?;

            Ok(CheckoutOutput {
                order_id: uuid::Uuid::new_v4().to_string(),
                total: qty * 1999,
                status: "completed".to_string(),
            })
        })
    }))
}

// ---------------------------------------------------------------------------
// Workflow 2: "greet" — a trivial single-step workflow (fast, for quick demos).
// ---------------------------------------------------------------------------

pub fn build_greet_workflow() -> Arc<dyn journio_core::Workflow> {
    Arc::new(WorkflowFn::new("greet", |_ctx, name: Option<String>| {
        Box::pin(async move {
            // Default to "World" when the UI sends no input (null).
            let name = name.unwrap_or_else(|| "World".to_string());
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(format!("Hello, {name}!"))
        })
    }))
}

// ---------------------------------------------------------------------------
// Workflow 3: "flaky_task" — a workflow that fails ~50% of the time, to
// demonstrate error persistence and the ERROR status in the UI.
// ---------------------------------------------------------------------------

pub fn build_flaky_workflow() -> Arc<dyn journio_core::Workflow> {
    Arc::new(WorkflowFn::new("flaky_task", |ctx, seed: Option<i64>| {
        Box::pin(async move {
            // Default to 4 (even → succeeds) when no input is given.
            let seed = seed.unwrap_or(4);
            let step = Arc::new(StepFunc::new("risky_step", move |_ctx| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    // Fail on odd seeds.
                    if seed % 2 == 1 {
                        return Err(journio_core::JournioError::new(
                            journio_core::JournioErrorCode::StepExecutionError,
                            format!("flaky task failed for seed {seed} (odd)"),
                        ));
                    }
                    Ok(seed * 2)
                })
            }));
            let result: Interchange = ctx.run_as_step(step).await?;
            Ok(format!("flaky task succeeded: {:?}", result))
        })
    }))
}

// ---------------------------------------------------------------------------
// Workflow 4: "long_running" — a workflow with many steps, useful for showing
// in-progress execution in the UI.
// ---------------------------------------------------------------------------

pub fn build_long_running_workflow() -> Arc<dyn journio_core::Workflow> {
    Arc::new(WorkflowFn::new("long_running", |ctx, steps: Option<i64>| {
        Box::pin(async move {
            // Default to 3 steps when no input is given.
            let steps = steps.unwrap_or(3);
            for i in 1..=steps {
                let step = Arc::new(StepFunc::new(format!("step_{i}"), move |_ctx| {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        Ok(format!("step {i} done"))
                    })
                }));
                let _: Interchange = ctx.run_as_step(step).await?;
                ctx.set_event("progress", serde_json::json!(i)).await?;
            }
            Ok(format!("completed {steps} steps"))
        })
    }))
}
