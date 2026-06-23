use journio_core::error::{JournioError, JournioErrorCode};
use sqlx::Error;

pub fn is_unique_violation(err: &Error) -> bool {
    sqlite_code(err)
        .as_deref()
        .is_some_and(|code| matches!(code, "1555" | "2067"))
}

pub fn is_foreign_key_violation(err: &Error) -> bool {
    sqlite_code(err).as_deref() == Some("787")
}

fn sqlite_code(err: &Error) -> Option<String> {
    match err {
        Error::Database(db_err) => db_err.code().map(|code| code.to_string()),
        _ => None,
    }
}

pub fn db_err(err: Error) -> JournioError {
    JournioError {
        code: JournioErrorCode::InitializationError,
        message: err.to_string(),
        source: Some(Box::new(err)),
        ..Default::default()
    }
}
