//! journio-postgres — Postgres/CockroachDB backend for `journio-core`.
//!
//! Port of the Postgres path of `journio/system_database.go`.

mod dialect;
mod error;
mod lib_impl;
mod migrations;

pub use crate::dialect::{PostgresDialect, sanitize_identifier};
pub use crate::lib_impl::PostgresSystemDatabase;
pub use crate::migrations::{latest_version, run_migrations};

pub(crate) use crate::lib_impl::{db_err, pool_err};
