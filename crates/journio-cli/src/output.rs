//! Output helpers — JSON to stdout, human messages to stderr. Ported from
//! Go's `outputJSON` / `logger.Info`.

use std::io::Write;

/// Pretty-print `value` as JSON to stdout.
pub fn print_json<T: serde::Serialize>(value: &T) -> Result<(), String> {
    let json = serde_json::to_string_pretty(value)
        .map_err(|e| format!("failed to serialize output: {e}"))?;
    println!("{json}");
    Ok(())
}

/// Print an informational message to stderr (Go logs to stderr via slog).
pub fn info(message: &str) {
    let _ = writeln!(std::io::stderr(), "{message}");
}

/// Prompt the user for y/N confirmation. Ported from `confirmAction`.
pub fn confirm(prompt: &str) -> bool {
    let _ = write!(std::io::stderr(), "{prompt} (y/N): ");
    let _ = std::io::stderr().flush();
    let mut response = String::new();
    if std::io::stdin().read_line(&mut response).is_err() {
        return false;
    }
    let response = response.trim();
    matches!(response.to_ascii_lowercase().as_str(), "y" | "yes")
}
