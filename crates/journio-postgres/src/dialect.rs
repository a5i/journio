//! Postgres dialect — ported 1:1 from `postgresDialect` in `journio/dialect.go`.
//!
//! CockroachDB (`cockroachDialect` in Go) is wire-compatible and shares this
//! dialect; the only differences are `supports_listen_notify() == false` and a
//! runtime probe, neither of which affects SQL generation. That split is added
//! when the conductor / polling paths land.

use journio_core::dialect::{Dialect, DialectName};

/// Postgres-flavoured dialect. Ported from `postgresDialect` (`dialect.go:181`).
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn name(&self) -> DialectName {
        DialectName::Postgres
    }

    /// `"journio".` — mirrors `pgx.Identifier{schema}.Sanitize() + "."`.
    fn schema_prefix(&self, schema: &str) -> String {
        format!("\"{}\".", schema)
    }

    /// Canonical queries are already Postgres syntax — no-op.
    /// Ported from `postgresDialect.RewriteQuery`.
    fn rewrite_query(&self, query: &str) -> String {
        query.to_string()
    }

    fn lock_skip_locked(&self) -> &str {
        "FOR UPDATE SKIP LOCKED"
    }

    fn lock_nowait(&self) -> &str {
        "FOR UPDATE NOWAIT"
    }

    fn supports_listen_notify(&self) -> bool {
        true
    }

    fn supports_array_parameters(&self) -> bool {
        true
    }

    fn supports_data_modifying_cte(&self) -> bool {
        true
    }
}

/// Quote an identifier the way `pgx.Identifier{}.Sanitize()` does — used when
/// rendering schema prefixes outside the `Dialect` trait (e.g. migrations).
pub fn sanitize_identifier(ident: &str) -> String {
    // pgx wraps in double quotes and doubles any embedded double-quote.
    format!("\"{}\"", ident.replace('"', "\"\""))
}
