//! Migration runner — ported from `buildMigrations` + `runMigrations` in
//! `journio/system_database.go`.
//!
//! All 40 Postgres migration files are embedded verbatim (copied from
//! `journio-transact-golang/journio/migrations/`). Each is rendered against the
//! sanitized schema exactly as Go's `fmt.Sprintf` does, preserving per-file
//! `%s` arity and ordering (raw-vs-quoted schema, `CONCURRENTLY` slot for
//! online index migrations).
//!
//! CockroachDB-only files (`*_cockroach.sql`, `10_check_*`) are intentionally
//! NOT embedded — this is the Postgres path.

use deadpool_postgres::Pool;
use journio_core::JournioError;

use crate::db_err;
use crate::dialect::sanitize_identifier;

/// Migration table name — ported from `_JOURNIO_MIGRATION_TABLE`.
const MIGRATION_TABLE: &str = "journio_migrations";

/// How a `%s` in a migration template is substituted. Mirrors the arg lists
/// in Go's `buildMigrations`.
#[derive(Copy, Clone)]
enum Kind {
    /// Every `%s` → sanitized (quoted) schema. The common case.
    AllSan,
    /// First `%s` → raw schema, rest → sanitized. (Migration 10: the first
    /// `%s` lives inside a string literal compared against `nspname`.)
    RawThenSan,
    /// First `%s` → `CONCURRENTLY`, rest → sanitized. (Online index DDL:
    /// migrations 22-37 except 28, 33, 36.)
    ConcurrentlySan,
}

/// One rendered migration. A migration may compose multiple files (migration
/// 1 appends listen-notify; migration 38 appends search-path hardening) —
/// `parts` preserves Go's concatenation.
struct MigrationDef {
    version: i64,
    /// Online migrations run outside a transaction so Postgres accepts
    /// `CREATE/DROP INDEX CONCURRENTLY`. Mirrors `migrationFile.online`.
    online: bool,
    parts: &'static [(&'static str, Kind)],
}

// Embedded migration sources — 1:1 with `journio/migrations/*.sql`.
const M01: &str = include_str!("../migrations/1_initial_journio_schema.sql");
const M01_LN: &str = include_str!("../migrations/1_initial_journio_schema_listen_notify.sql");
const M02: &str = include_str!("../migrations/2_add_queue_partition_key.sql");
const M03: &str = include_str!("../migrations/3_add_workflow_status_index.sql");
const M04: &str = include_str!("../migrations/4_add_forked_from.sql");
const M05: &str = include_str!("../migrations/5_add_step_timestamps.sql");
const M06: &str = include_str!("../migrations/6_add_workflow_events_history.sql");
const M07: &str = include_str!("../migrations/7_add_owner_xid.sql");
const M08: &str = include_str!("../migrations/8_add_parent_workflow_id.sql");
const M09: &str = include_str!("../migrations/9_add_workflow_schedules.sql");
const M10: &str = include_str!("../migrations/10_add_notifications_pkey.sql");
const M11: &str = include_str!("../migrations/11_add_serialization_columns.sql");
const M12: &str = include_str!("../migrations/12_add_notifications_consumed.sql");
const M13: &str = include_str!("../migrations/13_add_application_versions.sql");
const M14: &str = include_str!("../migrations/14_add_pgsql_client_functions.sql");
const M15: &str = include_str!("../migrations/15_add_workflow_schedule_columns.sql");
const M16: &str = include_str!("../migrations/16_add_delay_until.sql");
const M17: &str = include_str!("../migrations/17_add_workflow_schedule_queue_name.sql");
const M18: &str = include_str!("../migrations/18_add_was_forked_from.sql");
const M19: &str = include_str!("../migrations/19_add_operation_outputs_completed_at_index.sql");
const M20: &str = include_str!("../migrations/20_set_function_search_path.sql");
const M21: &str = include_str!("../migrations/21_create_queues_table.sql");
const M22: &str = include_str!("../migrations/22_drop_forked_from_index.sql");
const M23: &str = include_str!("../migrations/23_create_partial_forked_from_index.sql");
const M24: &str = include_str!("../migrations/24_drop_parent_workflow_id_index.sql");
const M25: &str = include_str!("../migrations/25_create_partial_parent_workflow_id_index.sql");
const M26: &str = include_str!("../migrations/26_drop_executor_id_index.sql");
const M27: &str = include_str!("../migrations/27_create_partial_dedup_id_index.sql");
const M28: &str = include_str!("../migrations/28_drop_dedup_id_constraint.sql");
const M29: &str = include_str!("../migrations/29_create_pending_index.sql");
const M30: &str = include_str!("../migrations/30_create_failed_index.sql");
const M31: &str = include_str!("../migrations/31_drop_status_index.sql");
const M32: &str = include_str!("../migrations/32_create_in_flight_index.sql");
const M33: &str = include_str!("../migrations/33_add_rate_limited.sql");
const M34: &str = include_str!("../migrations/34_create_rate_limited_index.sql");
const M35: &str = include_str!("../migrations/35_drop_queue_status_started_index.sql");
const M36: &str = include_str!("../migrations/36_add_completed_at.sql");
const M37: &str = include_str!("../migrations/37_create_started_at_index.sql");
const M38: &str = include_str!("../migrations/38_update_enqueue_workflow.sql");
const M38_SP: &str = include_str!("../migrations/38_set_enqueue_workflow_search_path.sql");
const M39: &str = include_str!("../migrations/39_create_streams_trigger.sql");
const M40: &str = include_str!("../migrations/40_add_attributes.sql");

/// The full ordered migration set — ported 1:1 from `buildMigrations` (the
/// non-Cockroach branch). Cockroach variants land behind a probe later.
const MIGRATIONS: &[MigrationDef] = &[
    MigrationDef {
        version: 1,
        online: false,
        parts: &[(M01, Kind::AllSan), (M01_LN, Kind::AllSan)],
    },
    MigrationDef {
        version: 2,
        online: false,
        parts: &[(M02, Kind::AllSan)],
    },
    MigrationDef {
        version: 3,
        online: false,
        parts: &[(M03, Kind::AllSan)],
    },
    MigrationDef {
        version: 4,
        online: false,
        parts: &[(M04, Kind::AllSan)],
    },
    MigrationDef {
        version: 5,
        online: false,
        parts: &[(M05, Kind::AllSan)],
    },
    MigrationDef {
        version: 6,
        online: false,
        parts: &[(M06, Kind::AllSan)],
    },
    MigrationDef {
        version: 7,
        online: false,
        parts: &[(M07, Kind::AllSan)],
    },
    MigrationDef {
        version: 8,
        online: false,
        parts: &[(M08, Kind::AllSan)],
    },
    MigrationDef {
        version: 9,
        online: false,
        parts: &[(M09, Kind::AllSan)],
    },
    MigrationDef {
        version: 10,
        online: false,
        parts: &[(M10, Kind::RawThenSan)],
    },
    MigrationDef {
        version: 11,
        online: false,
        parts: &[(M11, Kind::AllSan)],
    },
    MigrationDef {
        version: 12,
        online: false,
        parts: &[(M12, Kind::AllSan)],
    },
    MigrationDef {
        version: 13,
        online: false,
        parts: &[(M13, Kind::AllSan)],
    },
    MigrationDef {
        version: 14,
        online: false,
        parts: &[(M14, Kind::AllSan)],
    },
    MigrationDef {
        version: 15,
        online: false,
        parts: &[(M15, Kind::AllSan)],
    },
    MigrationDef {
        version: 16,
        online: false,
        parts: &[(M16, Kind::AllSan)],
    },
    MigrationDef {
        version: 17,
        online: false,
        parts: &[(M17, Kind::AllSan)],
    },
    MigrationDef {
        version: 18,
        online: false,
        parts: &[(M18, Kind::AllSan)],
    },
    MigrationDef {
        version: 19,
        online: false,
        parts: &[(M19, Kind::AllSan)],
    },
    MigrationDef {
        version: 20,
        online: false,
        parts: &[(M20, Kind::AllSan)],
    },
    MigrationDef {
        version: 21,
        online: false,
        parts: &[(M21, Kind::AllSan)],
    },
    // Online index DDL — runs outside a transaction.
    MigrationDef {
        version: 22,
        online: true,
        parts: &[(M22, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 23,
        online: true,
        parts: &[(M23, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 24,
        online: true,
        parts: &[(M24, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 25,
        online: true,
        parts: &[(M25, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 26,
        online: true,
        parts: &[(M26, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 27,
        online: true,
        parts: &[(M27, Kind::ConcurrentlySan)],
    },
    // Migration 28 is a fast catalog op — NOT online (mirrors Go).
    MigrationDef {
        version: 28,
        online: false,
        parts: &[(M28, Kind::AllSan)],
    },
    MigrationDef {
        version: 29,
        online: true,
        parts: &[(M29, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 30,
        online: true,
        parts: &[(M30, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 31,
        online: true,
        parts: &[(M31, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 32,
        online: true,
        parts: &[(M32, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 33,
        online: false,
        parts: &[(M33, Kind::AllSan)],
    },
    MigrationDef {
        version: 34,
        online: true,
        parts: &[(M34, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 35,
        online: true,
        parts: &[(M35, Kind::ConcurrentlySan)],
    },
    MigrationDef {
        version: 36,
        online: false,
        parts: &[(M36, Kind::AllSan)],
    },
    MigrationDef {
        version: 37,
        online: true,
        parts: &[(M37, Kind::ConcurrentlySan)],
    },
    // 38 = base + search-path hardening (Postgres-only).
    MigrationDef {
        version: 38,
        online: false,
        parts: &[(M38, Kind::AllSan), (M38_SP, Kind::AllSan)],
    },
    MigrationDef {
        version: 39,
        online: false,
        parts: &[(M39, Kind::AllSan)],
    },
    MigrationDef {
        version: 40,
        online: false,
        parts: &[(M40, Kind::AllSan)],
    },
];

/// Render one template by substituting `%s` left-to-right per `kind`.
/// Mirrors Go's `fmt.Sprintf(template, args...)`.
fn render(template: &str, schema: &str, kind: Kind) -> String {
    if matches!(kind, Kind::AllSan) {
        return template.replace("%s", &sanitize_identifier(schema));
    }
    let (first, rest) = match template.split_once("%s") {
        Some(pair) => pair,
        None => return template.to_string(),
    };
    let head = match kind {
        Kind::RawThenSan => schema.to_string(),
        Kind::ConcurrentlySan => "CONCURRENTLY".to_string(),
        Kind::AllSan => unreachable!(),
    };
    format!(
        "{}{}{}",
        first,
        head,
        rest.replace("%s", &sanitize_identifier(schema))
    )
}

/// Render all parts of a migration, concatenated with a newline — mirrors
/// Go's `migration1SQLProcessed + "\n" + listenNotify` style.
fn render_migration(def: &MigrationDef, schema: &str) -> String {
    def.parts
        .iter()
        .map(|(tpl, kind)| render(tpl, schema, *kind))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The latest migration version (the last entry in `MIGRATIONS`).
pub fn latest_version() -> i64 {
    MIGRATIONS.last().expect("migration set non-empty").version
}

/// Run all pending migrations against `pool`. Ported from `runMigrations`
/// (`system_database.go:543`).
pub async fn run_migrations(pool: &Pool, schema: &str) -> Result<(), JournioError> {
    let san = sanitize_identifier(schema);

    // 1) Schema + migrations table setup in one short transaction.
    let mut current_version: i64 = {
        let mut client = pool.get().await.map_err(crate::pool_err)?;
        let tx = client.transaction().await.map_err(db_err)?;

        // pgcrypto provides gen_random_uuid(), which the schema (message UUIDs,
        // queue IDs) and client functions rely on from migration 1 onward.
        // Ensure it exists here so every caller of migrate() is self-sufficient.
        tx.execute(
            "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public",
            &[],
        )
        .await
        .map_err(db_err)?;
        let schema_exists: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
                &[&schema],
            )
            .await
            .map_err(db_err)?
            .get(0);
        if !schema_exists {
            tx.execute(format!("CREATE SCHEMA {}", san).as_str(), &[])
                .await
                .map_err(db_err)?;
        }

        let table_exists: bool = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
                &[&schema, &MIGRATION_TABLE],
            )
            .await
            .map_err(db_err)?
            .get(0);
        if !table_exists {
            tx.execute(
                format!(
                    "CREATE TABLE {}.{} (version BIGINT NOT NULL PRIMARY KEY)",
                    san, MIGRATION_TABLE
                )
                .as_str(),
                &[],
            )
            .await
            .map_err(db_err)?;
        }

        let current: i64 = tx
            .query_opt(
                format!("SELECT version FROM {}.{} LIMIT 1", san, MIGRATION_TABLE).as_str(),
                &[],
            )
            .await
            .map_err(db_err)?
            .map(|r| r.get(0))
            .unwrap_or(0);

        tx.commit().await.map_err(db_err)?;
        current
    };

    // 2) Apply pending migrations one at a time.
    for def in MIGRATIONS {
        if def.version <= current_version {
            continue;
        }
        let sql = render_migration(def, schema);

        if def.online {
            // Online migrations run outside a transaction (CONCURRENTLY).
            // `simple_query` because migration files contain multiple
            // statements (the extended/prepared protocol rejects those).
            cleanup_invalid_indexes(pool, schema).await?;
            let client = pool.get().await.map_err(crate::pool_err)?;
            client.simple_query(&sql).await.map_err(db_err)?;
            bump_version(pool, schema, def.version, current_version).await?;
        } else {
            let mut client = pool.get().await.map_err(crate::pool_err)?;
            let tx = client.transaction().await.map_err(db_err)?;
            // `simple_query` — see above (multi-statement DDL).
            tx.simple_query(&sql).await.map_err(db_err)?;
            set_version_tx(&tx, schema, def.version, current_version).await?;
            tx.commit().await.map_err(db_err)?;
        }
        tracing::debug!(version = def.version, "applied migration");
        current_version = def.version;
    }
    Ok(())
}

/// Drop indexes left INVALID by a prior crashed `CREATE INDEX CONCURRENTLY`.
/// Ported from `cleanupInvalidIndexes` (`system_database.go:503`).
async fn cleanup_invalid_indexes(pool: &Pool, schema: &str) -> Result<(), JournioError> {
    let san = sanitize_identifier(schema);
    let client = pool.get().await.map_err(crate::pool_err)?;
    let rows = client
        .query(
            "SELECT i.relname FROM pg_index ix \
             JOIN pg_class i ON i.oid = ix.indexrelid \
             JOIN pg_class t ON t.oid = ix.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE NOT ix.indisvalid AND n.nspname = $1",
            &[&schema],
        )
        .await
        .map_err(db_err)?;
    for row in rows {
        let name: String = row.get(0);
        tracing::warn!(schema = schema, index = %name, "dropping invalid index");
        let drop = format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {}.{}",
            san,
            sanitize_identifier(&name)
        );
        client.execute(&drop, &[]).await.map_err(db_err)?;
    }
    Ok(())
}

/// Insert or update the single version row — mirrors `writeMigrationVersion`.
async fn bump_version(
    pool: &Pool,
    schema: &str,
    version: i64,
    last_applied: i64,
) -> Result<(), JournioError> {
    let san = sanitize_identifier(schema);
    let sql = if last_applied == 0 {
        format!(
            "INSERT INTO {}.{} (version) VALUES ($1)",
            san, MIGRATION_TABLE
        )
    } else {
        format!("UPDATE {}.{} SET version = $1", san, MIGRATION_TABLE)
    };
    let client = pool.get().await.map_err(crate::pool_err)?;
    client.execute(&sql, &[&version]).await.map_err(db_err)?;
    Ok(())
}

/// Same as `bump_version` but inside a transaction.
async fn set_version_tx(
    tx: &deadpool_postgres::Transaction<'_>,
    schema: &str,
    version: i64,
    last_applied: i64,
) -> Result<(), JournioError> {
    let san = sanitize_identifier(schema);
    let sql = if last_applied == 0 {
        format!(
            "INSERT INTO {}.{} (version) VALUES ($1)",
            san, MIGRATION_TABLE
        )
    } else {
        format!("UPDATE {}.{} SET version = $1", san, MIGRATION_TABLE)
    };
    tx.execute(&sql, &[&version]).await.map_err(db_err)?;
    Ok(())
}
