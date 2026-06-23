//! Backend resolution — opens the correct [`SystemDatabase`] for a URL.
//!
//! `journio-core` is driver-free, so the CLI (which links both backends) is the
//! natural place to turn a connection string into a concrete backend. Ported
//! from the backend-selection half of `createJOURNIOContext` in Go's
//! `cmd/journio/utils.go`.

use std::sync::Arc;

use journio_core::{dialect::detect_dialect, SystemDatabase};
use journio_postgres::PostgresSystemDatabase;
use journio_sqlite::SqliteSystemDatabase;

/// Open a backend by auto-detecting the dialect from `database_url`.
///
/// `schema` only applies to Postgres/Cockroach (SQLite has no schemas).
pub async fn open_system_db(
    database_url: &str,
    schema: Option<&str>,
) -> Result<Arc<dyn SystemDatabase>, journio_core::JournioError> {
    let dialect = detect_dialect(database_url)?;
    let backend: Arc<dyn SystemDatabase> = match dialect {
        journio_core::DialectName::Postgres | journio_core::DialectName::Cockroach => {
            let schema = schema.unwrap_or("journio");
            Arc::new(PostgresSystemDatabase::connect(database_url, schema)?)
        }
        journio_core::DialectName::Sqlite => {
            Arc::new(SqliteSystemDatabase::connect(database_url).await?)
        }
    };
    Ok(backend)
}

/// The dialect detected for a URL (used by `reset` to branch).
pub fn detected_dialect(database_url: &str) -> Result<journio_core::DialectName, journio_core::JournioError> {
    detect_dialect(database_url)
}
