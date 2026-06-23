//! `journio init` — ported from `cmd/journio/init.go`.
//!
//! Scaffolds a new Rust Journio application from the embedded starter template
//! (the Go version scaffolds Go code; this port scaffolds Rust code that uses
//! `journio-core` + `journio-sqlite`).
//!
//! Templates live in `templates/starter/` and are embedded with `include_str!`.
//! The `{{PROJECT_NAME}}` placeholder is substituted into every file — a
//! minimal substitution that avoids pulling in a template engine.

use std::path::Path;

/// Default project name when none is given.
const DEFAULT_PROJECT_NAME: &str = "journio-rust-starter";

/// Run the init command. Creates `project_dir/` with the scaffolded files.
///
/// `project_dir` may be a relative name or a path; the **last path component**
/// (the basename) is used as the project name substituted into templates.
/// For example, `init /tmp/my-app` creates `/tmp/my-app/` and substitutes
/// `my-app` into every file.
pub fn run(project_dir: Option<&str>) -> Result<(), String> {
    let dir = project_dir.unwrap_or(DEFAULT_PROJECT_NAME);
    let path = Path::new(dir);

    // The project name for substitution = the directory's basename.
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(dir);

    if path.exists() {
        return Err(format!("directory '{dir}' already exists"));
    }

    std::fs::create_dir_all(path)
        .map_err(|e| format!("failed to create directory '{dir}': {e}"))?;

    for (template, output_relative) in TEMPLATE_FILES {
        let content = include_str_template(template);
        let rendered = content.replace("{{PROJECT_NAME}}", name);
        let output_path = path.join(output_relative);
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create directory for {output_relative}: {e}"))?;
        }
        std::fs::write(&output_path, rendered)
            .map_err(|e| format!("failed to write {output_relative}: {e}"))?;
    }

    println!("Created new Journio application: {dir}");
    println!();
    println!("To get started:");
    println!("  cd {dir}");
    println!("  cargo run");
    println!();
    println!("Then try:");
    println!("  journio --db-url sqlite://{name}.db workflow list");
    println!("  journio --db-url sqlite://{name}.db workflow steps <workflow-id>");
    Ok(())
}

/// Template files → output paths. Mirrors Go's `templates` map.
const TEMPLATE_FILES: &[(&str, &str)] = &[
    ("Cargo.toml", "Cargo.toml"),
    ("src/main.rs", "src/main.rs"),
    ("journio-config.yaml", "journio-config.yaml"),
    ("README.md", "README.md"),
];

/// Resolve a template path to its embedded contents.
fn include_str_template(path: &str) -> &'static str {
    match path {
        "Cargo.toml" => include_str!("../../templates/starter/Cargo.toml"),
        "src/main.rs" => include_str!("../../templates/starter/src/main.rs"),
        "journio-config.yaml" => include_str!("../../templates/starter/journio-config.yaml"),
        "README.md" => include_str!("../../templates/starter/README.md"),
        other => panic!("unknown template: {other}"),
    }
}
