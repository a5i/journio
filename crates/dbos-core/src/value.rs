//! Serialization interchange — ported from `dbos/serialization.go`.
//!
//! Workflows and steps are stored in the registry behind erased types, so
//! inputs and outputs flow through a single interchange representation. The
//! default is JSON (`serde_json::Value`), matching Go's default JSON
//! serializer. A `Serializer` trait lets users plug in other formats (the Go
//! code ships a "portable JSON" variant and supports custom serializers).

use crate::error::{DbosError, DbosErrorCode, DbosResult};

/// The erased interchange type for workflow/step inputs & outputs.
/// Stored verbatim in `operation_outputs` / `workflow_status`.
pub type Interchange = serde_json::Value;

/// Pluggable serializer — ported from `Serializer[any]` in `serialization.go`.
pub trait Serializer: Send + Sync {
    fn serialize(&self, value: &Interchange) -> DbosResult<String>;
    fn deserialize(&self, data: &str) -> DbosResult<Interchange>;
    /// Stable format name persisted in `workflow_status.serialization`.
    fn name(&self) -> &'static str;
}

/// Default JSON serializer — matches Go's `JSONSerializer`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize(&self, value: &Interchange) -> DbosResult<String> {
        serde_json::to_string(value).map_err(|e| DbosError {
            code: DbosErrorCode::WorkflowExecutionError,
            message: format!("serialization failed: {e}"),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
    fn deserialize(&self, data: &str) -> DbosResult<Interchange> {
        serde_json::from_str(data).map_err(|e| DbosError {
            code: DbosErrorCode::WorkflowExecutionError,
            message: format!("deserialization failed: {e}"),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
    fn name(&self) -> &'static str {
        "DBOS_JSON"
    }
}

impl Default for DbosError {
    fn default() -> Self {
        Self {
            code: DbosErrorCode::WorkflowExecutionError,
            message: String::new(),
            source: None,
            workflow_id: None,
            step_name: None,
            step_id: None,
            queue_name: None,
            deduplication_id: None,
        }
    }
}
