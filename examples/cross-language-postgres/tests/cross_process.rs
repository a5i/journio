use std::any::Any;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use journio_core::{SystemDatabase, WorkflowStatusType};
use journio_postgres::PostgresSystemDatabase;
use serde_json::Value;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

struct Harness {
    _container: Box<dyn Any>,
    database_url: String,
    schema: String,
}

struct ChildLines {
    child: Child,
    lines: mpsc::Receiver<String>,
    stderr: Arc<Mutex<String>>,
    label: String,
}

impl Drop for ChildLines {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn setup() -> Harness {
    let container = postgres::Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container
        .get_host()
        .await
        .expect("postgres host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port mapping");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let schema = format!("journio_cross_{}", uuid::Uuid::new_v4().simple());

    Harness {
        _container: Box::new(container),
        database_url,
        schema,
    }
}

fn node_binding_is_built() -> bool {
    repo_root().join("bindings/nodejs/dist/index.js").exists()
        && repo_root()
            .join("bindings/nodejs/native/index.node")
            .exists()
        && tsx_bin().exists()
        && Command::new("node").arg("--version").status().is_ok()
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("examples")
        .parent()
        .expect("repo root")
        .to_path_buf()
}

fn node_example_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("node")
}

fn tsx_bin() -> PathBuf {
    let bin = node_example_dir().join("node_modules").join(".bin");
    if cfg!(windows) {
        bin.join("tsx.cmd")
    } else {
        bin.join("tsx")
    }
}

// Line-reading errors from a dead child are skipped by design; the reader
// threads exit when the channels drop.
#[allow(clippy::lines_filter_map_ok)]
fn spawn_with_lines(label: impl Into<String>, mut command: Command) -> ChildLines {
    let label = label.into();
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child process");
    let stdout = child.stdout.take().expect("child stdout");
    let stderr = child.stderr.take().expect("child stderr");
    let (tx, rx) = mpsc::channel();
    let stderr_buf: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let stderr_buf_writer = stderr_buf.clone();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().flatten() {
            let _ = tx.send(line);
        }
    });
    std::thread::spawn(move || {
        // Append each stderr line to the shared buffer as it arrives, so that
        // wait_for_json can surface it on failure even when the child is still
        // alive (e.g. hung). Bounded so a chatty child cannot exhaust memory.
        for line in BufReader::new(stderr).lines().flatten() {
            if let Ok(mut guard) = stderr_buf_writer.lock() {
                guard.push_str(&line);
                guard.push('\n');
                if guard.len() > 64 * 1024 {
                    let cutoff = guard.len() - 64 * 1024;
                    guard.drain(..cutoff);
                }
            }
        }
    });
    ChildLines {
        child,
        lines: rx,
        stderr: stderr_buf,
        label,
    }
}

impl ChildLines {
    /// Returns the child's stderr captured so far (best-effort snapshot).
    fn stderr_snapshot(&self) -> String {
        self.stderr
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default()
    }
}

fn wait_for_json(child: &mut ChildLines, event: &str, timeout: Duration) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if deadline <= std::time::Instant::now() {
            let stderr = child.stderr_snapshot();
            panic!(
                "timed out waiting for event {event} from {}\n\
                 ----- child stderr -----\n{stderr}-------------------------",
                child.label
            );
        }
        if let Ok(Some(status)) = child.child.try_wait() {
            let stderr = child.stderr_snapshot();
            panic!(
                "child process {} exited with {status} while waiting for event {event}\n\
                 ----- child stderr -----\n{stderr}-------------------------",
                child.label
            );
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = match child
            .lines
            .recv_timeout(remaining.min(Duration::from_millis(250)))
        {
            Ok(line) => line,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                let stderr = child.stderr_snapshot();
                panic!(
                    "child process {} stdout closed while waiting for event {event}\n\
                     ----- child stderr -----\n{stderr}-------------------------",
                    child.label
                )
            }
        };
        let value: Value = serde_json::from_str(&line).unwrap_or_else(|_| {
            let stderr = child.stderr_snapshot();
            panic!(
                "child {} emitted non-json line while waiting for {event}: {line}\n\
                 ----- child stderr -----\n{stderr}-------------------------",
                child.label
            )
        });
        if value.get("event").and_then(Value::as_str) == Some(event) {
            return value;
        }
    }
}

fn base_command_env(command: &mut Command, harness: &Harness, workflow_id: &str) {
    command
        .env("JOURNIO_SYSTEM_DATABASE_URL", &harness.database_url)
        .env("JOURNIO_SYSTEM_DATABASE_SCHEMA", &harness.schema)
        .env("JOURNIO_RUST_QUEUE", "rust-cross-test")
        .env("JOURNIO_NODE_QUEUE", "node-cross-test")
        .env("JOURNIO_WORKFLOW_ID", workflow_id);
}

fn rust_bin(name: &str) -> &'static str {
    match name {
        "rust-worker" => env!("CARGO_BIN_EXE_rust-worker"),
        "rust-caller" => env!("CARGO_BIN_EXE_rust-caller"),
        _ => unreachable!("unknown bin"),
    }
}

fn node_command(script: &str) -> Command {
    let mut command = Command::new(tsx_bin());
    command.arg(script).current_dir(node_example_dir());
    command
}

async fn db(harness: &Harness) -> PostgresSystemDatabase {
    PostgresSystemDatabase::connect(&harness.database_url, &harness.schema).expect("connect db")
}

#[tokio::test]
async fn node_caller_executes_rust_worker_through_postgres() {
    if !node_binding_is_built() {
        eprintln!(
            "skipping cross-process test: build bindings/nodejs and run npm install in examples/cross-language-postgres/node first"
        );
        return;
    }

    let harness = setup().await;
    let workflow_id = format!("node-calls-rust-{}", uuid::Uuid::new_v4());

    let mut worker_command = Command::new(rust_bin("rust-worker"));
    base_command_env(&mut worker_command, &harness, &workflow_id);
    worker_command.env("JOURNIO_EXIT_AFTER_WORKFLOW_ID", &workflow_id);
    let mut worker = spawn_with_lines("rust-worker", worker_command);
    wait_for_json(&mut worker, "ready", Duration::from_secs(20));

    let mut caller_command = node_command("node-caller.ts");
    base_command_env(&mut caller_command, &harness, &workflow_id);
    let mut caller = spawn_with_lines("node-caller", caller_command);
    let result = wait_for_json(&mut caller, "result", Duration::from_secs(20));
    assert_eq!(result["workflowID"], workflow_id);
    assert_eq!(result["result"]["engine"], "rust");
    assert_eq!(result["result"]["totalCents"], 5997);
    wait_for_json(&mut worker, "observed-terminal", Duration::from_secs(20));

    let db = db(&harness).await;
    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status query")
        .expect("workflow row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.queue_name.as_deref(), Some("rust-cross-test"));

    let steps = db.get_steps(&workflow_id).await.expect("steps");
    let names: Vec<&str> = steps
        .iter()
        .map(|step| step.function_name.as_str())
        .collect();
    assert!(names.contains(&"rust_validate_quote"));
    assert!(names.contains(&"rust_calculate_price"));
}

#[tokio::test]
async fn rust_caller_executes_node_worker_through_postgres() {
    if !node_binding_is_built() {
        eprintln!(
            "skipping cross-process test: build bindings/nodejs and run npm install in examples/cross-language-postgres/node first"
        );
        return;
    }

    let harness = setup().await;
    let workflow_id = format!("rust-calls-node-{}", uuid::Uuid::new_v4());

    let mut worker_command = node_command("node-worker.ts");
    base_command_env(&mut worker_command, &harness, &workflow_id);
    worker_command.env("JOURNIO_EXIT_AFTER_WORKFLOW_ID", &workflow_id);
    let mut worker = spawn_with_lines("node-worker", worker_command);
    wait_for_json(&mut worker, "ready", Duration::from_secs(20));

    let mut caller_command = Command::new(rust_bin("rust-caller"));
    base_command_env(&mut caller_command, &harness, &workflow_id);
    let mut caller = spawn_with_lines("rust-caller", caller_command);
    let result = wait_for_json(&mut caller, "result", Duration::from_secs(20));
    assert_eq!(result["workflowId"], workflow_id);
    assert_eq!(result["result"]["engine"], "node");
    assert_eq!(result["result"]["orderId"], "order-1001");
    wait_for_json(&mut worker, "observed-terminal", Duration::from_secs(20));

    let db = db(&harness).await;
    let status = db
        .get_workflow_status(&workflow_id)
        .await
        .expect("status query")
        .expect("workflow row");
    assert_eq!(status.status, WorkflowStatusType::Success);
    assert_eq!(status.queue_name.as_deref(), Some("node-cross-test"));

    let steps = db.get_steps(&workflow_id).await.expect("steps");
    let names: Vec<&str> = steps
        .iter()
        .map(|step| step.function_name.as_str())
        .collect();
    assert!(names.contains(&"node_normalize_order"));
    assert!(names.contains(&"node_score_risk"));
}
