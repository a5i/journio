//! `journio reset` — ported from `cmd/journio/reset.go`.
//!
//! Resets the Journio system database. For Postgres this drops and recreates the
//! database; for SQLite it deletes the database file.

use crate::backend;
use crate::config::mask_password;
use crate::output::info;

/// Reset the database. Returns `Ok(false)` when the user declines the
/// confirmation prompt.
pub async fn run(
    database_url: &str,
    skip_confirmation: bool,
) -> Result<bool, String> {
    if !skip_confirmation {
        let prompt = "This command resets your Journio system database, deleting \
                      metadata about past workflows and steps. Are you sure you \
                      want to proceed?";
        if !crate::output::confirm(prompt) {
            info("Operation cancelled.");
            return Ok(false);
        }
    }

    info(&format!(
        "Resetting system database at {}",
        mask_password(database_url)
    ));

    match backend::detected_dialect(database_url).map_err(|e| e.to_string())? {
        journio_core::DialectName::Postgres | journio_core::DialectName::Cockroach => {
            reset_postgres(database_url).await?;
        }
        journio_core::DialectName::Sqlite => {
            reset_sqlite(database_url)?;
        }
    }

    info("System database has been reset successfully");
    Ok(true)
}

/// Connect to the `postgres` maintenance DB, drop the target database
/// (WITH FORCE for Postgres, terminate-then-drop for Cockroach), and recreate
/// it. Ported from `dropDatabaseIfExists` + the reset body in Go.
async fn reset_postgres(database_url: &str) -> Result<(), String> {
    use std::str::FromStr;
    let config = tokio_postgres::Config::from_str(database_url)
        .map_err(|e| format!("failed to parse database URL: {e}"))?;

    let db_name = config
        .get_dbname()
        .ok_or_else(|| "database name not found in connection string".to_string())?
        .to_string();

    // Reconnect to the maintenance database ("postgres") to drop/create.
    let mut maintenance = config.clone();
    maintenance.dbname("postgres");
    let (client, connection) = maintenance
        .connect(tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("failed to connect to PostgreSQL server: {e}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let is_crdb: bool = client
        .query_one("SHOW CRDB_VERSION", &[])
        .await
        .is_ok();
    // The above may fail on vanilla Postgres (no such statement); detect via
    // parameter status as a fallback.
    let is_crdb = is_crdb
        || {
            let row = client
                .query_opt("SELECT current_setting('crdb_version', true)", &[])
                .await
                .ok()
                .flatten();
            row.is_some_and(|r| r.get::<_, Option<String>>(0).is_some())
        };

    let sanitized = journio_postgres::sanitize_identifier(&db_name);
    if is_crdb {
        // CockroachDB: terminate backends first (no WITH FORCE).
        let _ = client
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = $1 AND pid <> pg_backend_pid()",
                &[&db_name],
            )
            .await;
        client
            .execute(&format!("DROP DATABASE IF EXISTS {sanitized}"), &[])
            .await
            .map_err(|e| format!("failed to drop database {db_name}: {e}"))?;
    } else {
        // Postgres: WITH FORCE drops even with active connections.
        client
            .execute(&format!("DROP DATABASE IF EXISTS {sanitized} WITH (FORCE)"), &[])
            .await
            .map_err(|e| format!("failed to drop database {db_name}: {e}"))?;
    }

    client
        .execute(&format!("CREATE DATABASE {sanitized}"), &[])
        .await
        .map_err(|e| format!("failed to create database {db_name}: {e}"))?;

    Ok(())
}

/// SQLite reset: delete the database file (and its -wal/-shm siblings).
fn reset_sqlite(database_url: &str) -> Result<(), String> {
    let path = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite3://"))
        .unwrap_or(database_url);
    let path = path.trim_start_matches("file:");

    if path == ":memory:" || path.is_empty() {
        // Nothing to delete for an in-memory database.
        return Ok(());
    }

    for candidate in [path, &format!("{path}-wal"), &format!("{path}-shm")] {
        if std::path::Path::new(candidate).exists() {
            std::fs::remove_file(candidate)
                .map_err(|e| format!("failed to remove {candidate}: {e}"))?;
        }
    }
    Ok(())
}
