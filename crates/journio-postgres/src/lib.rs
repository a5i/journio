//! journio-postgres — Postgres/CockroachDB backend for `journio-core`.
//!
//! Port of the Postgres path of `journio/system_database.go`.

// See journio-core: `JournioError` is slightly over the `result_large_err`
// size threshold and boxing it would be a cascading public-API break.
#![allow(clippy::result_large_err)]

mod dialect;
mod error;
mod lib_impl;
mod migrations;

pub use crate::dialect::{PostgresDialect, sanitize_identifier};
pub use crate::lib_impl::PostgresSystemDatabase;
pub use crate::migrations::{latest_version, run_migrations};

pub(crate) use crate::lib_impl::{db_err, pool_err};
