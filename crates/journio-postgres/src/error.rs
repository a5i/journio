//! SQLSTATE classification for tokio-postgres — stands in for the
//! `IsUniqueViolation` / `IsForeignKeyViolation` methods on Go's `Dialect`
//! (`dialect.go`). Kept off the `Dialect` trait because classification is
//! coupled to the driver error type, not the SQL dialect (Cockroach reuses
//! Postgres SQL but the classifier would be identical).

use tokio_postgres::Error;

/// SQLSTATE codes — subset of `pgerrcode` used by journio.
pub mod sqlstate {
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const DEADLOCK_DETECTED: &str = "40P01";
}

/// Extract the SQLSTATE from a tokio-postgres error, if any.
pub fn sqlstate(err: &Error) -> Option<&str> {
    err.as_db_error().map(|e| e.code().code())
}

pub fn is_unique_violation(err: &Error) -> bool {
    sqlstate(err) == Some(sqlstate::UNIQUE_VIOLATION)
}

pub fn is_foreign_key_violation(err: &Error) -> bool {
    sqlstate(err) == Some(sqlstate::FOREIGN_KEY_VIOLATION)
}
