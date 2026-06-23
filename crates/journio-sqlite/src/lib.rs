//! journio-sqlite - SQLite backend for `journio-core`.
//!
//! Port of the SQLite path of `journio/system_database.go` using the Go project's
//! SQLite migration set as the source of truth.

mod dialect;
mod error;
mod lib_impl;
mod migrations;

pub use crate::dialect::SqliteDialect;
pub use crate::lib_impl::SqliteSystemDatabase;
pub use crate::migrations::{latest_version, run_migrations};

pub(crate) use crate::error::db_err;
