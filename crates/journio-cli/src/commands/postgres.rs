//! `journio postgres` — ported from `cmd/journio/postgres.go`.
//!
//! Manages a local Postgres database via Docker. The Go version uses the
//! Docker API directly; this port shells out to the `docker` CLI — same
//! behaviour, far less code, no heavyweight Docker client dependency.
//!
//! Subcommands: `journio postgres start` / `journio postgres stop`.

use std::process::Command;

use crate::output::info;

const CONTAINER_NAME: &str = "journio-db";
const IMAGE: &str = "pgvector/pgvector:pg16";
const PGDATA_PATH: &str = "/var/lib/postgresql/data";
const VOLUME_NAME: &str = "pgdata";
const PORT: &str = "5432";

/// `journio postgres start` — start a local Postgres (idempotent).
pub async fn start() -> Result<(), String> {
    info("Attempting to create a Docker Postgres container...");

    if !docker_available() {
        return Err(
            "Docker not detected locally. Please install Docker to use this feature".to_string(),
        );
    }

    // Already running?
    if container_state()? == Some("running".to_string()) {
        info(&format!("Container is already running: {CONTAINER_NAME}"));
        return Ok(());
    }

    // Create the named volume (idempotent).
    let _ = run_docker(&["volume", "create", VOLUME_NAME]);

    let password = password();

    // Run the container.
    let env_arg = format!("POSTGRES_PASSWORD={password}");
    let pgdata_arg = format!("PGDATA={PGDATA_PATH}");
    let port_binding = format!("{PORT}:{PORT}");
    let volume_bind = format!("{VOLUME_NAME}:{PGDATA_PATH}");

    // If a stopped container exists, remove it first. A "no such" error is
    // fine — there's nothing to remove.
    if container_exists()? {
        match run_docker(&["rm", "-f", CONTAINER_NAME]) {
            Ok(_) | Err(_) => {}
        }
    }

    let result = run_docker(&[
        "run",
        "-d",
        "--name",
        CONTAINER_NAME,
        "-e",
        &env_arg,
        "-e",
        &pgdata_arg,
        "-p",
        &port_binding,
        "-v",
        &volume_bind,
        IMAGE,
    ]);

    match result {
        Ok(output) => {
            let id = output.trim();
            let short = &id[..id.len().min(12)];
            info(&format!("Created container: {short}"));
        }
        Err(e) => return Err(format!("failed to create container: {e}")),
    }

    // Wait for Postgres to be ready (poll with pg_isready or a connect loop).
    wait_for_ready(&password).await?;

    let masked = mask_password(&password);
    info(&format!(
        "Postgres available: postgres://postgres:{masked}@localhost:{PORT}"
    ));
    Ok(())
}

/// `journio postgres stop` — stop the local Postgres container.
pub async fn stop() -> Result<(), String> {
    info(&format!("Stopping Docker Postgres container: {CONTAINER_NAME}"));

    if !container_exists()? {
        info(&format!("Container does not exist: {CONTAINER_NAME}"));
        return Ok(());
    }

    match run_docker(&["rm", "-f", CONTAINER_NAME]) {
        Ok(_) => {
            info(&format!("Successfully stopped: {CONTAINER_NAME}"));
            Ok(())
        }
        Err(e) if e.contains("no such") => {
            info(&format!("Container does not exist: {CONTAINER_NAME}"));
            Ok(())
        }
        Err(e) => Err(format!("failed to stop container: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Get the password from `PGPASSWORD` (default: "journio").
fn password() -> String {
    std::env::var("PGPASSWORD").unwrap_or_else(|_| "journio".to_string())
}

fn mask_password(pw: &str) -> String {
    if pw.is_empty() {
        String::new()
    } else {
        "***".to_string()
    }
}

/// Is the `docker` CLI available?
fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Returns the container's state ("running", "exited", ...), or `None` if it
/// doesn't exist. A "no such" docker error is treated as non-existence, not
/// a real error.
fn container_state() -> Result<Option<String>, String> {
    match run_docker(&[
        "inspect",
        "-f",
        "{{.State.Status}}",
        CONTAINER_NAME,
    ]) {
        Ok(output) if !output.trim().is_empty() => Ok(Some(output.trim().to_string())),
        Ok(_) => Ok(None),
        Err(e) if e.contains("no such") => Ok(None),
        Err(e) => Err(e),
    }
}

fn container_exists() -> Result<bool, String> {
    Ok(container_state()?.is_some())
}

/// Run a `docker` subcommand, returning stdout on success.
fn run_docker(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("failed to invoke docker: {e}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(stderr.trim().to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Poll the container until Postgres accepts connections (up to 30s).
async fn wait_for_ready(password: &str) -> Result<(), String> {
    info("Waiting for Postgres Docker container to start...");
    let url = format!(
        "postgres://postgres:{password}@localhost:{PORT}/postgres?connect_timeout=2"
    );

    for i in 0..30u32 {
        if i > 0 && i % 5 == 0 {
            info("Still waiting for Postgres Docker container to start...");
        }

        if try_connect(&url) {
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Err(format!(
        "Container {CONTAINER_NAME} did not start in time"
    ))
}

/// Attempt a quick TCP-level readiness check using `docker exec pg_isready`,
/// which avoids needing a Postgres client library.
fn try_connect(_url: &str) -> bool {
    // Use `docker exec` + pg_isready (ships inside the image) so we don't
    // depend on a Postgres client crate.
    run_docker(&["exec", CONTAINER_NAME, "pg_isready", "-U", "postgres"]).is_ok()
}
