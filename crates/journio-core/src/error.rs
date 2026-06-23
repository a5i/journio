//! Error model — ported from `journio/errors.go`.
//!
//! Mirrors Go's `JOURNIOError` (struct with a code + message + optional context
//! fields + wrapped error). The set of codes is identical so cross-language
//! behaviour and error mapping in bindings stays 1:1.

use std::fmt;

/// Mirrors `JOURNIOErrorCode` in `journio/errors.go`. Integers kept off the public
/// surface — match by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournioErrorCode {
    ConflictingIDError,
    InitializationError,
    NonExistentWorkflowError,
    ConflictingWorkflowError,
    WorkflowCancelled,
    UnexpectedStep,
    AwaitedWorkflowCancelled,
    ConflictingRegistrationError,
    WorkflowUnexpectedTypeError,
    WorkflowExecutionError,
    StepExecutionError,
    DeadLetterQueueError,
    MaxStepRetriesExceeded,
    QueueDeduplicated,
    PatchingNotEnabled,
    TimeoutError,
    NoApplicationVersions,
}

/// Unified error type — ported from `JOURNIOError` (`journio/errors.go`).
#[derive(Debug)]
pub struct JournioError {
    pub code: JournioErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
    // Optional context fields (only set when relevant — mirrors Go struct).
    pub workflow_id: Option<String>,
    pub step_name: Option<String>,
    pub step_id: Option<i32>,
    pub queue_name: Option<String>,
    pub deduplication_id: Option<String>,
}

impl JournioError {
    pub fn new(code: JournioErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
            workflow_id: None,
            step_name: None,
            step_id: None,
            queue_name: None,
            deduplication_id: None,
        }
    }
}

impl fmt::Display for JournioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Journio Error ({:?}): {}", self.code, self.message)
    }
}

impl std::error::Error for JournioError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &dyn std::error::Error)
    }
}

/// Convenience constructors matching the Go `newXxxError` helpers.
pub mod constructors {
    use super::*;

    pub fn initialization(msg: impl Into<String>) -> JournioError {
        JournioError::new(JournioErrorCode::InitializationError, msg)
    }
    pub fn conflicting_registration(name: &str) -> JournioError {
        JournioError::new(
            JournioErrorCode::ConflictingRegistrationError,
            format!("{name} is already registered"),
        )
    }
    pub fn non_existent_workflow(id: &str) -> JournioError {
        let mut e = JournioError::new(
            JournioErrorCode::NonExistentWorkflowError,
            format!("workflow {id} does not exist"),
        );
        e.workflow_id = Some(id.to_string());
        e
    }
    pub fn unexpected_step(
        workflow_id: &str,
        step_id: i32,
        expected: &str,
        recorded: &str,
    ) -> JournioError {
        let mut e = JournioError::new(
            JournioErrorCode::UnexpectedStep,
            format!(
                "During execution of workflow {workflow_id} step {step_id}, function {recorded} was recorded when {expected} was expected. Check that your workflow is deterministic."
            ),
        );
        e.workflow_id = Some(workflow_id.to_string());
        e.step_id = Some(step_id);
        e
    }
}

pub type JournioResult<T> = Result<T, JournioError>;
