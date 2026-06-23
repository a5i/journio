//! Library facade for the `journio` CLI — exposes the command functions and
//! backend/config helpers so they can be reused (by tests, the admin server,
//! or embedded tooling) without spawning the binary.

pub mod backend;
pub mod commands;
pub mod config;
pub mod output;
