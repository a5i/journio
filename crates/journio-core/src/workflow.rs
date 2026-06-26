//! Workflow & step execution model — ported from `journio/workflow.go`.
//!
//! This is the **central design decision** of the port. Go registers
//! `Workflow[P,R]` / `Step[R]` functions by name using reflection. Rust has no
//! runtime reflection, so we use:
//!
//! 1. An **erased, object-safe** [`Workflow`] / [`Step`] trait whose inputs and
//!    outputs are the [`Interchange`] type (`serde_json::Value`). The registry,
//!    the recovery engine, and the FFI bindings all operate on `dyn Workflow`.
//! 2. A **typed adapter** ([`WorkflowFn`] / [`StepFunc`]) that wraps a user
//!    `Fn` and (de)serializes around it — recovering Go's ergonomic typed API
//!    for pure-Rust users.
//!
//! Python/Node bindings supply their own `dyn Workflow` implementation that
//! marshals `Interchange` into the VM and invokes a stored callable.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::context::WorkflowContext;
use crate::error::{JournioError, JournioErrorCode, JournioResult};
use crate::value::Interchange;

/// Boxed async future returned by typed adapters. Using `Pin<Box<dyn Future>>`
/// keeps the adapter bounds simple and works with plain closures that return
/// `Box::pin(async move { ... })`. (Once `async closures` are ergonomic in
/// trait bounds we can relax this.)
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Erased, object-safe workflow trait — the registry & FFI seam.
///
/// Ported conceptually from `WorkflowFunc` (`workflow.go:814`) and the
/// `WorkflowRegistryEntry` indirection (`workflow.go:456`).
#[async_trait]
pub trait Workflow: Send + Sync {
    /// Registered function name (used as the registry key and persisted as
    /// `workflow_status.name`).
    fn name(&self) -> &str;

    /// Execute the workflow. `input` is the deserialized interchange payload.
    async fn run(&self, ctx: &WorkflowContext, input: Interchange) -> JournioResult<Interchange>;
}

/// Erased, object-safe step trait — ported from `StepFunc` (`workflow.go:1634`).
#[async_trait]
pub trait Step: Send + Sync {
    fn name(&self) -> &str;
    async fn run(&self, ctx: &WorkflowContext) -> JournioResult<Interchange>;
}

// ---------------------------------------------------------------------------
// Typed adapters — recover Go-style ergonomics for pure-Rust users.
// ---------------------------------------------------------------------------

/// Wraps a typed workflow function `F: Fn(WorkflowContext, P) -> BoxFuture<JournioResult<R>>`.
/// The adapter (de)serializes around `F`. Ported conceptually from
/// `RegisterWorkflow[P,R]` (`workflow.go:672`).
pub struct WorkflowFn<P, R, F> {
    name: String,
    func: F,
    _marker: PhantomData<fn() -> (P, R)>,
}

impl<P, R, F> WorkflowFn<P, R, F>
where
    P: DeserializeOwned + Send,
    R: Serialize + Send,
    F: Fn(WorkflowContext, P) -> BoxFuture<JournioResult<R>> + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, R, F> Workflow for WorkflowFn<P, R, F>
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    F: Fn(WorkflowContext, P) -> BoxFuture<JournioResult<R>> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, ctx: &WorkflowContext, input: Interchange) -> JournioResult<Interchange> {
        let typed: P = serde_json::from_value(input).map_err(|e| JournioError {
            code: JournioErrorCode::WorkflowUnexpectedTypeError,
            message: format!("workflow {} input deserialization failed: {e}", self.name),
            source: Some(Box::new(e)),
            ..Default::default()
        })?;
        let fut = (self.func)(ctx.clone(), typed);
        let result: R = fut.await?;
        serde_json::to_value(result).map_err(|e| JournioError {
            code: JournioErrorCode::WorkflowUnexpectedTypeError,
            message: format!("workflow {} output serialization failed: {e}", self.name),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
}

/// Wraps a typed step function `F: Fn(WorkflowContext) -> BoxFuture<JournioResult<R>>`.
pub struct StepFunc<R, F> {
    name: String,
    func: F,
    _marker: PhantomData<fn() -> R>,
}

impl<R, F> StepFunc<R, F>
where
    R: Serialize + Send,
    F: Fn(WorkflowContext) -> BoxFuture<JournioResult<R>> + Send + Sync,
{
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<R, F> Step for StepFunc<R, F>
where
    R: Serialize + Send + 'static,
    F: Fn(WorkflowContext) -> BoxFuture<JournioResult<R>> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, ctx: &WorkflowContext) -> JournioResult<Interchange> {
        let fut = (self.func)(ctx.clone());
        let result: R = fut.await?;
        serde_json::to_value(result).map_err(|e| JournioError {
            code: JournioErrorCode::StepExecutionError,
            message: format!("step {} output serialization failed: {e}", self.name),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
}

// ---------------------------------------------------------------------------
// Registry — ported from `journioContext.workflowRegistry` (sync.Map) +
// `registerWorkflow` / `ListRegisteredWorkflows` in `workflow.go`.
// ---------------------------------------------------------------------------

/// Name-keyed registry of erased workflows — ported from
/// `journioContext.workflowRegistry` (`journio/journio.go:246`).
#[derive(Default)]
pub struct Registry {
    workflows: RwLock<HashMap<String, Arc<dyn Workflow>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workflow. Returns `ConflictingRegistrationError` on duplicate
    /// name — mirrors `registerWorkflow` / Go's behaviour.
    pub fn register(&self, wf: Arc<dyn Workflow>) -> JournioResult<()> {
        let mut map = self
            .workflows
            .write()
            .map_err(|e| JournioError::new(JournioErrorCode::InitializationError, e.to_string()))?;
        if map.contains_key(wf.name()) {
            return Err(JournioError::new(
                JournioErrorCode::ConflictingRegistrationError,
                format!("{} is already registered", wf.name()),
            ));
        }
        map.insert(wf.name().to_string(), wf);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Workflow>> {
        self.workflows
            .read()
            .ok()
            .and_then(|m| m.get(name).cloned())
    }

    pub fn list(&self) -> Vec<String> {
        self.workflows
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopWorkflow {
        name: String,
    }

    #[async_trait]
    impl Workflow for NoopWorkflow {
        fn name(&self) -> &str {
            &self.name
        }

        async fn run(
            &self,
            _ctx: &WorkflowContext,
            input: Interchange,
        ) -> JournioResult<Interchange> {
            Ok(input)
        }
    }

    #[test]
    fn registry_rejects_duplicate_registration() {
        let registry = Registry::new();
        let workflow = Arc::new(NoopWorkflow {
            name: "example".to_string(),
        });

        registry
            .register(workflow.clone())
            .expect("first registration succeeds");
        let err = registry
            .register(workflow)
            .expect_err("second registration should fail");

        assert_eq!(err.code, JournioErrorCode::ConflictingRegistrationError);
    }

    #[test]
    fn registry_can_lookup_registered_workflows() {
        let registry = Registry::new();
        let workflow = Arc::new(NoopWorkflow {
            name: "lookup".to_string(),
        });

        registry.register(workflow).expect("registration succeeds");

        assert!(registry.get("lookup").is_some());
        assert!(registry.get("missing").is_none());
        assert_eq!(registry.list(), vec!["lookup".to_string()]);
    }
}
