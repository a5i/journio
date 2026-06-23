//! Serialization interchange — ported from `journio/serialization.go`.
//!
//! Workflows and steps are stored in the registry behind erased types, so
//! inputs and outputs flow through a single interchange representation. The
//! default is JSON (`serde_json::Value`), matching Go's default JSON
//! serializer. A `Serializer` trait lets users plug in other formats (the Go
//! code ships a "portable JSON" variant and supports custom serializers).

use crate::error::{JournioError, JournioErrorCode, JournioResult};

/// The erased interchange type for workflow/step inputs & outputs.
/// Stored verbatim in `operation_outputs` / `workflow_status`.
pub type Interchange = serde_json::Value;

/// Pluggable serializer — ported from `Serializer[any]` in `serialization.go`.
pub trait Serializer: Send + Sync {
    fn serialize(&self, value: &Interchange) -> JournioResult<String>;
    fn deserialize(&self, data: &str) -> JournioResult<Interchange>;
    /// Stable format name persisted in `workflow_status.serialization`.
    fn name(&self) -> &'static str;
}

/// Default JSON serializer — matches Go's `JSONSerializer`.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonSerializer;

impl Serializer for JsonSerializer {
    fn serialize(&self, value: &Interchange) -> JournioResult<String> {
        serde_json::to_string(value).map_err(|e| JournioError {
            code: JournioErrorCode::WorkflowExecutionError,
            message: format!("serialization failed: {e}"),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
    fn deserialize(&self, data: &str) -> JournioResult<Interchange> {
        serde_json::from_str(data).map_err(|e| JournioError {
            code: JournioErrorCode::WorkflowExecutionError,
            message: format!("deserialization failed: {e}"),
            source: Some(Box::new(e)),
            ..Default::default()
        })
    }
    fn name(&self) -> &'static str {
        "JOURNIO_JSON"
    }
}

impl Default for JournioError {
    fn default() -> Self {
        Self {
            code: JournioErrorCode::WorkflowExecutionError,
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
