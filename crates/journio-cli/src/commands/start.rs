//! `journio start` — ported from `cmd/journio/start.go`.
//!
//! Runs the `runtimeConfig.start` commands from `journio-config.yaml`, each as a
//! child process that inherits stdio. Forwards SIGINT/SIGTERM to the child's
//! process group (Unix) or kills the process tree (Windows).

use std::process::Stdio;

use crate::config::CliConfig;
use crate::output::info;

/// Run each start command sequentially. The first failure stops the chain.
pub async fn run(config: Option<&CliConfig>) -> Result<(), String> {
    let config = config.ok_or_else(|| "no config provided".to_string())?;
    if config.runtime_config.start.is_empty() {
        return Err("no start commands found in config file".to_string());
    }

    info("Executing start commands from config file");

    for command in &config.runtime_config.start {
        info(&format!("Executing command: {command}"));
        run_command(command).await?;
    }
    Ok(())
}

/// Spawn one shell command, wait for it, and forward Ctrl+C if it arrives
/// before the process exits.
async fn run_command(command: &str) -> Result<(), String> {
    let mut child = spawn_shell(command)?;

    // Forward Ctrl+C / SIGTERM to the child.
    let child_id = child.id();
    tokio::select! {
        result = child.wait() => {
            let status = result.map_err(|e| format!("failed to wait on command: {e}"))?;
            if !status.success() {
                return Err(format!("command failed: {command} (exit {:?})", status.code()));
            }
            Ok(())
        }
        _ = tokio::signal::ctrl_c() => {
            info("Received Ctrl+C, stopping...");
            if let Some(pid) = child_id {
                kill_process_tree(pid);
            }
            // Give the child a moment to die from the signal.
            let _ = child.wait().await;
            std::process::exit(0);
        }
    }
}

/// Spawn `sh -c <command>` (Unix) or `cmd /C <command>` (Windows), inheriting
/// stdio so output streams to the user's terminal.
fn spawn_shell(command: &str) -> Result<tokio::process::Child, String> {
    let mut cmd = if cfg!(windows) {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/C", command]);
        c
    } else {
        let mut c = tokio::process::Command::new("sh");
        c.args(["-c", command]);
        c
    };
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // On Unix, put the child in its own process group so we can signal the
    // whole group (mirrors Go's Setpgid: true).
    #[cfg(unix)]
    {
        unsafe {
            cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }

    cmd.spawn()
        .map_err(|e| format!("failed to start command: {e}"))
}

/// Kill the process (and, on Unix, its whole group). Cross-platform.
fn kill_process_tree(pid: u32) {
    #[cfg(unix)]
    {
        // Signal the negative PID to hit the process group.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
    }
    #[cfg(windows)]
    {
        // taskkill /T /F kills the process and its children.
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
    }
}
