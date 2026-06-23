//! `dbos` CLI — ported from `cmd/dbos/*` (cobra).
//!
//! Subcommands: `version`, `migrate`, `reset`, `workflow`, `start`, `init`,
//! `postgres`.
//!
//! Global flags mirror Go: `--db-url/-D`, `--config`, `--verbose`,
//! `--schema`.

use dbos_cli::{backend, commands, config, output};

use std::sync::Arc;

use clap::{Parser, Subcommand};

/// DBOS CLI — manage DBOS workflows from the command line.
#[derive(Parser)]
#[command(name = "dbos", version, about = "DBOS CLI", long_about = None)]
struct Cli {
    /// Your DBOS system database URL (overrides config/env).
    #[arg(short = 'D', long, global = true)]
    db_url: Option<String>,

    /// Config file path (default: ./dbos-config.yaml).
    #[arg(long, global = true)]
    config: Option<String>,

    /// Enable verbose (DEBUG) logging.
    #[arg(long, global = true)]
    verbose: bool,

    /// Database schema name (defaults to "dbos"; Postgres only).
    #[arg(long, global = true)]
    schema: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show the version and exit.
    Version,

    /// Create DBOS system tables (run migrations).
    Migrate {
        /// The role with which you will run your DBOS application (Postgres
        /// schema grants).
        #[arg(short = 'r', long = "app-role")]
        app_role: Option<String>,
    },

    /// Reset the DBOS system database.
    Reset {
        /// Skip the confirmation prompt.
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Manage DBOS workflows.
    Workflow {
        #[command(subcommand)]
        sub: WorkflowCommand,
    },

    /// Start your DBOS application using the start commands in 'dbos-config.yaml'.
    Start,

    /// Initialize a new DBOS application from a template.
    Init {
        /// Project name (defaults to 'dbos-rust-starter').
        project_name: Option<String>,
    },

    /// Manage a local Postgres database with Docker.
    Postgres {
        #[command(subcommand)]
        sub: PostgresCommand,
    },
}

#[derive(Subcommand)]
enum PostgresCommand {
    /// Start a local Postgres database.
    Start,
    /// Stop the local Postgres database.
    Stop,
}

#[derive(Subcommand)]
enum WorkflowCommand {
    /// List workflows for your application.
    List {
        #[arg(short = 'l', long, default_value_t = 10)]
        limit: i64,
        #[arg(short = 'u', long)]
        user: Option<String>,
        #[arg(short = 'n', long)]
        name: Option<String>,
        /// Status: PENDING, SUCCESS, ERROR, ENQUEUED, CANCELLED,
        /// MAX_RECOVERY_ATTEMPTS_EXCEEDED
        #[arg(short = 'S', long)]
        status: Option<String>,
        #[arg(short = 'v', long = "application-version")]
        application_version: Option<String>,
        #[arg(short = 'q', long)]
        queue: Option<String>,
        /// Retrieve only queued workflows.
        #[arg(short = 'Q', long)]
        queues_only: bool,
        /// Sort descending (older first).
        #[arg(short = 'd', long)]
        sort_desc: bool,
        #[arg(short = 'o', long, default_value_t = 0)]
        offset: i64,
        /// Start after this timestamp (ISO 8601 / RFC 3339).
        #[arg(short = 's', long = "start-time")]
        start_time: Option<String>,
        /// Start before this timestamp (ISO 8601 / RFC 3339).
        #[arg(short = 'e', long = "end-time")]
        end_time: Option<String>,
    },

    /// Retrieve the status of a workflow.
    Get { workflow_id: String },

    /// List the steps of a workflow.
    Steps { workflow_id: String },

    /// Cancel a workflow.
    Cancel { workflow_id: String },

    /// Resume a cancelled workflow.
    Resume { workflow_id: String },

    /// Fork a workflow from the beginning or a specific step.
    Fork {
        workflow_id: String,
        #[arg(short = 's', long, default_value_t = 1)]
        step: u32,
        #[arg(short = 'a', long = "application-version")]
        application_version: Option<String>,
        #[arg(short = 'f', long = "forked-workflow-id")]
        forked_workflow_id: Option<String>,
    },

    /// Permanently delete one or more workflows.
    Delete {
        workflow_ids: Vec<String>,
        /// Also delete all child workflows recursively.
        #[arg(short = 'c', long)]
        children: bool,
    },
}

/// Build metadata — resolved at runtime from compile-time env vars (Go uses
/// `-ldflags`; Rust uses `option_env!`). `unwrap_or` is not yet const-stable,
/// so we resolve lazily.
struct BuildInfo {
    version: &'static str,
    commit: &'static str,
    built_at: &'static str,
}

impl BuildInfo {
    const fn from_env() -> Self {
        Self {
            version: match option_env!("DBOS_BUILD_VERSION") {
                Some(v) => v,
                None => "dev",
            },
            commit: match option_env!("DBOS_BUILD_COMMIT") {
                Some(v) => v,
                None => "",
            },
            built_at: match option_env!("DBOS_BUILD_DATE") {
                Some(v) => v,
                None => "",
            },
        }
    }
}

const BUILD: BuildInfo = BuildInfo::from_env();

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli).await {
        // Errors go to stderr; non-zero exit signals failure.
        output::info(&format!("Error: {e}"));
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    // Load config (for URL resolution + migrate's custom commands).
    let loaded = config::load_config(cli.config.as_deref())?;
    let cfg: Option<config::CliConfig> = loaded.as_ref().map(|(cfg, _path)| cfg.clone());
    let cfg_ref = cfg.as_ref();

    match cli.command {
        Command::Version => {
            let mut info = serde_json::json!({ "version": BUILD.version });
            if !BUILD.commit.is_empty() {
                info["commit"] = BUILD.commit.into();
            }
            if !BUILD.built_at.is_empty() {
                info["built"] = BUILD.built_at.into();
            }
            output::print_json(&info)
        }

        Command::Migrate { app_role } => {
            let url = resolve_url(cli.db_url.as_deref(), cfg_ref)?;
            commands::migrate::run(
                &url,
                cli.schema.as_deref(),
                app_role.as_deref(),
                cfg_ref,
            )
            .await
        }

        Command::Reset { yes } => {
            let url = resolve_url(cli.db_url.as_deref(), cfg_ref)?;
            commands::reset::run(&url, yes).await?;
            Ok(())
        }

        Command::Workflow { sub } => {
            let url = resolve_url(cli.db_url.as_deref(), cfg_ref)?;
            let client = build_client(&url, cli.schema.as_deref()).await?;
            run_workflow(client, sub).await
        }

        Command::Start => commands::start::run(cfg_ref).await,

        Command::Init { project_name } => {
            commands::init::run(project_name.as_deref())
        }

        Command::Postgres { sub } => match sub {
            PostgresCommand::Start => commands::postgres::start().await,
            PostgresCommand::Stop => commands::postgres::stop().await,
        },
    }
}

fn resolve_url(flag: Option<&str>, cfg: Option<&config::CliConfig>) -> Result<String, String> {
    config::resolve_db_url(flag, cfg)
}

async fn build_client(
    database_url: &str,
    schema: Option<&str>,
) -> Result<Arc<dbos_core::Client>, String> {
    let backend = backend::open_system_db(database_url, schema)
        .await
        .map_err(|e| e.to_string())?;
    let mut config = dbos_core::Config::default();
    config.app_name = "dbos-cli".to_string();
    config.system_db = Some(backend);
    if let Some(schema) = schema {
        config.database_schema = Some(schema.to_string());
    }
    dbos_core::Client::new(config).await.map_err(|e| e.to_string())
}

async fn run_workflow(client: Arc<dbos_core::Client>, sub: WorkflowCommand) -> Result<(), String> {
    use commands::workflow as wf;
    match sub {
        WorkflowCommand::List {
            limit,
            user,
            name,
            status,
            application_version,
            queue,
            queues_only,
            sort_desc,
            offset,
            start_time,
            end_time,
        } => {
            let opts = wf::ListOptions {
                limit: (limit > 0).then_some(limit),
                offset: (offset > 0).then_some(offset),
                user,
                name,
                status: status.as_deref().map(wf::parse_status).transpose()?,
                application_version,
                queue,
                queues_only,
                sort_desc,
                start_time: start_time.as_deref().map(wf::parse_timestamp).transpose()?,
                end_time: end_time.as_deref().map(wf::parse_timestamp).transpose()?,
            };
            let rows = wf::list(&client, opts).await?;
            output::print_json(&rows)
        }
        WorkflowCommand::Get { workflow_id } => {
            let status = wf::get(&client, &workflow_id).await?;
            output::print_json(&status)
        }
        WorkflowCommand::Steps { workflow_id } => {
            let steps = wf::steps(&client, &workflow_id).await?;
            output::print_json(&steps)
        }
        WorkflowCommand::Cancel { workflow_id } => {
            wf::cancel(&client, &workflow_id).await
        }
        WorkflowCommand::Resume { workflow_id } => {
            let status = wf::resume(&client, &workflow_id).await?;
            output::print_json(&status)
        }
        WorkflowCommand::Fork {
            workflow_id,
            step,
            application_version,
            forked_workflow_id,
        } => {
            let status = wf::fork(
                &client,
                &workflow_id,
                step,
                application_version,
                forked_workflow_id,
            )
            .await?;
            output::print_json(&status)
        }
        WorkflowCommand::Delete {
            workflow_ids,
            children,
        } => wf::delete(&client, &workflow_ids, children).await,
    }
}
