//! `dbos migrate` — ported from `cmd/dbos/migrate.go`.
//!
//! Runs the DBOS system-database migrations, optionally granting schema
//! permissions to an application role (Postgres only) and executing the
//! custom `database.migrate` shell commands from `dbos-config.yaml`.

use std::sync::Arc;

use crate::backend;
use crate::config::{CliConfig};
use crate::output::info;

/// Run the migration. `app_role` grants Postgres schema permissions when set.
pub async fn run(
    database_url: &str,
    schema: Option<&str>,
    app_role: Option<&str>,
    config: Option<&CliConfig>,
) -> Result<(), String> {
    info(&format!(
        "Migrating DBOS system database at {}",
        crate::config::mask_password(database_url)
    ));

    let db = backend::open_system_db(database_url, schema)
        .await
        .map_err(|e| e.to_string())?;
    db.migrate().await.map_err(|e| format!("migration failed: {e}"))?;

    let schema_name = schema.unwrap_or("dbos");
    if let Some(role) = app_role {
        grant_schema_permissions(database_url, role, schema_name).await?;
    }

    // Custom migration commands from config (Go runs them via sh/cmd).
    if let Some(cfg) = config {
        for command in &cfg.database.migrate {
            info(&format!("Executing migration command: {command}"));
            run_shell_command(command)?;
        }
    }

    info("DBOS migrations completed successfully");
    Ok(())
}

/// Execute a single shell command (migrate list), streaming output.
fn run_shell_command(command: &str) -> Result<(), String> {
    let mut cmd = if cfg!(windows) {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", command]);
        c
    } else {
        let mut c = std::process::Command::new("sh");
        c.args(["-c", command]);
        c
    };
    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn migration command {command:?}: {e}"))?;
    if !status.success() {
        return Err(format!(
            "migration command failed: {command} (exit {:?})",
            status.code()
        ));
    }
    Ok(())
}

/// Grant USAGE + privileges on the DBOS schema to `role` — ported from
/// `grantDBOSSchemaPermissions` (Postgres only). No-op for other dialects.
async fn grant_schema_permissions(
    database_url: &str,
    role: &str,
    schema_name: &str,
) -> Result<(), String> {
    if !matches!(
        backend::detected_dialect(database_url).map_err(|e| e.to_string())?,
        dbos_core::DialectName::Postgres | dbos_core::DialectName::Cockroach
    ) {
        return Ok(());
    }

    info(&format!(
        "Granting permissions for schema {schema_name} to role {role}"
    ));

    // Borrow a raw connection to run DDL outside the pool's search_path.
    let grants = build_grant_statements(schema_name, role);
    grant_via_postgres(database_url, &grants).await
}

fn build_grant_statements(schema: &str, role: &str) -> Vec<String> {
    // Sanitize identifiers to prevent injection (mirrors pgx.Identifier.Sanitize).
    let s = dbos_postgres::sanitize_identifier(schema);
    let r = dbos_postgres::sanitize_identifier(role);
    vec![
        format!("GRANT USAGE ON SCHEMA {s} TO {r}"),
        format!("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {s} TO {r}"),
        format!("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {s} TO {r}"),
        format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {s} TO {r}"),
        format!("ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT ALL ON TABLES TO {r}"),
        format!("ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT ALL ON SEQUENCES TO {r}"),
        format!("ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT EXECUTE ON FUNCTIONS TO {r}"),
    ]
}

/// Connect to Postgres directly (outside the pool) to run grant DDL. Uses the
/// `tokio_postgres::Config` URL parser, connecting with the same credentials
/// as the system database.
async fn grant_via_postgres(
    database_url: &str,
    statements: &[String],
) -> Result<(), String> {
    use std::str::FromStr;
    let mut pg_config =
        tokio_postgres::Config::from_str(database_url).map_err(|e| e.to_string())?;
    // Connect without the search_path override the pool applies.
    let (client, connection) = pg_config.connect(tokio_postgres::NoTls).await.map_err(|e| {
        format!("failed to connect for grant: {e}")
    })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for stmt in statements {
        client
            .execute(stmt, &[])
            .await
            .map_err(|e| format!("failed to execute grant {stmt}: {e}"))?;
    }
    // Keep the config referenced to avoid an unused-mut warning above; the
    // `mut` is required by `connect` in some tokio-postgres versions.
    let _ = &mut pg_config;
    Ok(())
}

// Re-export Arc for potential future use by callers that construct backends.
#[allow(dead_code)]
type DbRef = Arc<dyn dbos_core::SystemDatabase>;
