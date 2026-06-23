//! Backend dialect abstraction — ported from `dbos/dialect.go`.
//!
//! The same `SystemDatabase` impl is reused across Postgres, CockroachDB and
//! SQLite. All per-backend differences (placeholder style, schema prefix, lock
//! clauses, error classification, listen/notify support, migration set) live
//! behind the `Dialect` trait so the engine stays dialect-agnostic.
//!
//! Canonical queries are written in Postgres syntax with `$N` placeholders and
//! a schema-prefix slot; non-Postgres dialects rewrite via [`Dialect::rewrite_query`].

/// Mirrors `DialectName` in `dbos/dialect.go`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DialectName {
    Postgres,
    Cockroach,
    Sqlite,
}

impl DialectName {
    pub fn as_str(self) -> &'static str {
        match self {
            DialectName::Postgres => "postgres",
            DialectName::Cockroach => "cockroach",
            DialectName::Sqlite => "sqlite",
        }
    }
}

/// Detects dialect from a connection URL. Ported from `detectDialect`.
pub fn detect_dialect(database_url: &str) -> Result<DialectName, crate::DbosError> {
    let lower = database_url.to_ascii_lowercase();
    if lower.is_empty() {
        Err(crate::DbosError::new(
            crate::DbosErrorCode::InitializationError,
            "could not detect database dialect from URL: empty database URL",
        ))
    } else if lower.starts_with("postgres://")
        || lower.starts_with("postgresql://")
        || lower.starts_with("crdb://")
    {
        Ok(if lower.starts_with("crdb://") {
            DialectName::Cockroach
        } else {
            DialectName::Postgres
        })
    } else if lower.starts_with("sqlite:")
        || lower.starts_with("sqlite3:")
        || lower == ":memory:"
        || lower.ends_with(".sqlite")
        || lower.ends_with(".db")
    {
        Ok(DialectName::Sqlite)
    } else {
        Err(crate::DbosError::new(
            crate::DbosErrorCode::InitializationError,
            format!("could not detect database dialect from URL: {database_url}"),
        ))
    }
}

/// Per-backend SQL fragments & behaviours — ported 1:1 from the Go `Dialect`
/// interface. Object-safe (no async, no generics).
pub trait Dialect: Send + Sync {
    fn name(&self) -> DialectName;

    /// Qualified-table prefix, e.g. `"dbos".` for Postgres or `""` for SQLite
    /// (no schemas). Includes the trailing dot when non-empty.
    fn schema_prefix(&self, schema: &str) -> String;

    /// Converts a canonical Postgres-style query (with `$N` placeholders and a
    /// rendered schema prefix) into the dialect's native form. No-op for PG;
    /// rewrites `$N -> ?` left-to-right for SQLite.
    fn rewrite_query(&self, query: &str) -> String;

    /// `FOR UPDATE SKIP LOCKED` fragment, or `""` for SQLite.
    fn lock_skip_locked(&self) -> &str;

    /// `FOR UPDATE NOWAIT` fragment, or `""`.
    fn lock_nowait(&self) -> &str;

    /// Whether the dialect supports `LISTEN`/`NOTIFY`. CockroachDB and SQLite
    /// fall back to the polling notification loop.
    fn supports_listen_notify(&self) -> bool;

    /// Whether a slice can bind as one array param (`= ANY($1)`). SQLite must
    /// expand to `IN (?, ?, ...)`.
    fn supports_array_parameters(&self) -> bool;

    /// Whether a CTE term may be an `INSERT/UPDATE/DELETE` (PG yes, SQLite no).
    fn supports_data_modifying_cte(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::{DialectName, detect_dialect};

    #[test]
    fn detect_dialect_recognizes_sqlite_forms() {
        for url in [
            "sqlite:/tmp/x.db",
            "sqlite:///tmp/x.db",
            "sqlite::memory:",
            "sqlite:relative.db",
            "sqlite3:relative.db",
            "SQLITE:/tmp/x.db",
            ":memory:",
            "C:/tmp/dbos.db",
        ] {
            assert_eq!(detect_dialect(url).unwrap(), DialectName::Sqlite, "{url}");
        }
    }

    #[test]
    fn detect_dialect_recognizes_postgres_forms() {
        assert_eq!(
            detect_dialect("postgres://u:p@h:5432/d").unwrap(),
            DialectName::Postgres
        );
        assert_eq!(
            detect_dialect("postgresql://u:p@h/d").unwrap(),
            DialectName::Postgres
        );
        assert_eq!(
            detect_dialect("crdb://u:p@h:26257/d").unwrap(),
            DialectName::Cockroach
        );
    }

    #[test]
    fn detect_dialect_rejects_unknown_forms() {
        for url in ["", "mysql://h/d", "postgress://typo", "justastring"] {
            assert!(detect_dialect(url).is_err(), "{url}");
        }
    }
}
