//! Library facade for the `journio` CLI — exposes the command functions and
//! backend/config helpers so they can be reused (by tests, the admin server,
//! or embedded tooling) without spawning the binary.

// See journio-core: `JournioError` is slightly over the `result_large_err`
// size threshold and boxing it would be a cascading public-API break.
#![allow(clippy::result_large_err)]

pub mod backend;
pub mod commands;
pub mod config;
pub mod output;
