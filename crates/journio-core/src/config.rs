//! Configuration — ported from `Config` in `journio/journio.go` and `ClientConfig`
//! in `journio/client.go`.
//!
//! `SystemDBPool` / `SqliteSystemDB` from the Go struct become concrete
//! `SystemDatabase` implementations supplied by `journio-postgres` /
//! `journio-sqlite` (kept out of core to avoid backend deps here).

use std::sync::Arc;
use std::time::Duration;

use crate::dialect::{self, DialectName};
use crate::error::{JournioError, JournioErrorCode};
use crate::value::{JsonSerializer, Serializer};

pub const DEFAULT_ADMIN_SERVER_PORT: u16 = 3001;
pub const DEFAULT_SYSTEM_DB_SCHEMA: &str = "journio";
pub const DEFAULT_SCHEDULER_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Initialization config — ported from `journio.Config`.
#[derive(Clone)]
pub struct Config {
    /// Required. Application name for identification.
    pub app_name: String,
    /// System-database connection string. Exactly one of `database_url` or
    /// `system_db` must be set (`system_db` takes precedence).
    pub database_url: Option<String>,
    /// Pre-built backend (Postgres or SQLite). Optional; takes precedence.
    pub system_db: Option<Arc<dyn crate::SystemDatabase>>,
    /// Schema name (defaults to `journio`).
    pub database_schema: Option<String>,
    /// Enable Transact admin HTTP server (disabled by default).
    pub admin_server: bool,
    pub admin_server_port: Option<u16>,
    /// Conductor service URL / API key (optional).
    pub conductor_url: Option<String>,
    pub conductor_api_key: Option<String>,
    pub conductor_executor_metadata: Option<serde_json::Value>,
    pub application_version: Option<String>,
    pub executor_id: Option<String>,
    pub enable_patching: bool,
    pub serializer: Arc<dyn Serializer>,
    pub scheduler_polling_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            app_name: String::new(),
            database_url: None,
            system_db: None,
            database_schema: None,
            admin_server: false,
            admin_server_port: None,
            conductor_url: None,
            conductor_api_key: None,
            conductor_executor_metadata: None,
            application_version: None,
            executor_id: None,
            enable_patching: false,
            serializer: Arc::new(JsonSerializer),
            scheduler_polling_interval: DEFAULT_SCHEDULER_POLL_INTERVAL,
        }
    }
}

/// Ported from `processConfig` in `journio/journio.go`. Validates + applies defaults
/// + honors `JOURNIO__APPVERSION` / `JOURNIO__VMID` env overrides.
pub fn process_config(input: &mut Config) -> Result<(), JournioError> {
    if input.database_url.is_none() && input.system_db.is_none() {
        return Err(JournioError::new(
            JournioErrorCode::InitializationError,
            "one of database_url or system_db must be provided",
        ));
    }
    if input.app_name.is_empty() {
        return Err(JournioError::new(
            JournioErrorCode::InitializationError,
            "missing required config field: app_name",
        ));
    }
    if let Some(url) = &input.database_url {
        // validate the dialect is detectable (mirrors Go behaviour)
        dialect::detect_dialect(url)?;
    }
    if input.admin_server_port.is_none() && input.admin_server {
        input.admin_server_port = Some(DEFAULT_ADMIN_SERVER_PORT);
    }
    if input
        .database_schema
        .as_deref()
        .is_none_or(|s| s.is_empty())
    {
        input.database_schema = Some(DEFAULT_SYSTEM_DB_SCHEMA.to_string());
    }
    if input.enable_patching && input.application_version.is_none() {
        input.application_version = Some("PATCHING_ENABLED".to_string());
    }
    if let Ok(v) = std::env::var("JOURNIO__APPVERSION") {
        if !v.is_empty() {
            input.application_version = Some(v);
        }
    }
    if let Ok(v) = std::env::var("JOURNIO__VMID") {
        if !v.is_empty() {
            input.executor_id = Some(v);
        }
    }
    if input.application_version.is_none() {
        input.application_version = Some(compute_application_version());
    }
    if input.executor_id.is_none() {
        input.executor_id = Some("local".to_string());
    }
    Ok(())
}

/// Ported from `computeApplicationVersion` — a content hash of the build.
/// Stub for now; replace with a build-script-derived hash in real builds.
fn compute_application_version() -> String {
    "0.0.1".to_string()
}

/// Detects the dialect of a configured URL, returning the default dialect name
/// (used by `JournioContext` to pick a backend).
pub fn detect_configured_dialect(cfg: &Config) -> Result<DialectName, JournioError> {
    if let Some(db) = &cfg.system_db {
        return Ok(db.dialect().name());
    }
    let url = cfg.database_url.as_deref().ok_or_else(|| {
        JournioError::new(
            JournioErrorCode::InitializationError,
            "no database configured",
        )
    })?;
    dialect::detect_dialect(url)
}
