//! Backend resolution — opens the correct [`SystemDatabase`] for a URL.
//!
//! `dbos-core` is driver-free, so the CLI (which links both backends) is the
//! natural place to turn a connection string into a concrete backend. Ported
//! from the backend-selection half of `createDBOSContext` in Go's
//! `cmd/dbos/utils.go`.

use std::sync::Arc;

use dbos_core::{dialect::detect_dialect, SystemDatabase};
use dbos_postgres::PostgresSystemDatabase;
use dbos_sqlite::SqliteSystemDatabase;

/// Open a backend by auto-detecting the dialect from `database_url`.
///
/// `schema` only applies to Postgres/Cockroach (SQLite has no schemas).
pub async fn open_system_db(
    database_url: &str,
    schema: Option<&str>,
) -> Result<Arc<dyn SystemDatabase>, dbos_core::DbosError> {
    let dialect = detect_dialect(database_url)?;
    let backend: Arc<dyn SystemDatabase> = match dialect {
        dbos_core::DialectName::Postgres | dbos_core::DialectName::Cockroach => {
            let schema = schema.unwrap_or("dbos");
            Arc::new(PostgresSystemDatabase::connect(database_url, schema)?)
        }
        dbos_core::DialectName::Sqlite => {
            Arc::new(SqliteSystemDatabase::connect(database_url).await?)
        }
    };
    Ok(backend)
}

/// The dialect detected for a URL (used by `reset` to branch).
pub fn detected_dialect(database_url: &str) -> Result<dbos_core::DialectName, dbos_core::DbosError> {
    detect_dialect(database_url)
}
