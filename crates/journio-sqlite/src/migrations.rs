use journio_core::JournioError;
use sqlx::{Row, SqlitePool};

use crate::db_err;

const MIGRATION_TABLE: &str = "journio_migrations";

struct MigrationDef {
    version: i64,
    sql: &'static str,
}

const M01: &str = include_str!("../migrations/1_initial_journio_schema.sql");
const M02: &str = include_str!("../migrations/2_add_queue_partition_key.sql");
const M03: &str = include_str!("../migrations/3_add_workflow_status_index.sql");
const M04: &str = include_str!("../migrations/4_add_forked_from.sql");
const M05: &str = include_str!("../migrations/5_add_step_timestamps.sql");
const M06: &str = include_str!("../migrations/6_add_workflow_events_history.sql");
const M07: &str = include_str!("../migrations/7_add_owner_xid.sql");
const M08: &str = include_str!("../migrations/8_add_parent_workflow_id.sql");
const M09: &str = include_str!("../migrations/9_add_workflow_schedules.sql");
const M11: &str = include_str!("../migrations/11_add_serialization_columns.sql");
const M12: &str = include_str!("../migrations/12_add_notifications_consumed.sql");
const M13: &str = include_str!("../migrations/13_add_application_versions.sql");
const M15: &str = include_str!("../migrations/15_add_workflow_schedule_columns.sql");
const M16: &str = include_str!("../migrations/16_add_delay_until.sql");
const M17: &str = include_str!("../migrations/17_add_workflow_schedule_queue_name.sql");
const M18: &str = include_str!("../migrations/18_add_was_forked_from.sql");
const M19: &str = include_str!("../migrations/19_add_operation_outputs_completed_at_index.sql");
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
const M40: &str = include_str!("../migrations/40_add_attributes.sql");

const MIGRATIONS: &[MigrationDef] = &[
    MigrationDef {
        version: 1,
        sql: M01,
    },
    MigrationDef {
        version: 2,
        sql: M02,
    },
    MigrationDef {
        version: 3,
        sql: M03,
    },
    MigrationDef {
        version: 4,
        sql: M04,
    },
    MigrationDef {
        version: 5,
        sql: M05,
    },
    MigrationDef {
        version: 6,
        sql: M06,
    },
    MigrationDef {
        version: 7,
        sql: M07,
    },
    MigrationDef {
        version: 8,
        sql: M08,
    },
    MigrationDef {
        version: 9,
        sql: M09,
    },
    MigrationDef {
        version: 11,
        sql: M11,
    },
    MigrationDef {
        version: 12,
        sql: M12,
    },
    MigrationDef {
        version: 13,
        sql: M13,
    },
    MigrationDef {
        version: 15,
        sql: M15,
    },
    MigrationDef {
        version: 16,
        sql: M16,
    },
    MigrationDef {
        version: 17,
        sql: M17,
    },
    MigrationDef {
        version: 18,
        sql: M18,
    },
    MigrationDef {
        version: 19,
        sql: M19,
    },
    MigrationDef {
        version: 21,
        sql: M21,
    },
    MigrationDef {
        version: 22,
        sql: M22,
    },
    MigrationDef {
        version: 23,
        sql: M23,
    },
    MigrationDef {
        version: 24,
        sql: M24,
    },
    MigrationDef {
        version: 25,
        sql: M25,
    },
    MigrationDef {
        version: 26,
        sql: M26,
    },
    MigrationDef {
        version: 27,
        sql: M27,
    },
    MigrationDef {
        version: 28,
        sql: M28,
    },
    MigrationDef {
        version: 29,
        sql: M29,
    },
    MigrationDef {
        version: 30,
        sql: M30,
    },
    MigrationDef {
        version: 31,
        sql: M31,
    },
    MigrationDef {
        version: 32,
        sql: M32,
    },
    MigrationDef {
        version: 33,
        sql: M33,
    },
    MigrationDef {
        version: 34,
        sql: M34,
    },
    MigrationDef {
        version: 35,
        sql: M35,
    },
    MigrationDef {
        version: 36,
        sql: M36,
    },
    MigrationDef {
        version: 37,
        sql: M37,
    },
    MigrationDef {
        version: 40,
        sql: M40,
    },
];

pub fn latest_version() -> i64 {
    MIGRATIONS.last().expect("migration set non-empty").version
}

pub async fn run_migrations(pool: SqlitePool) -> Result<(), JournioError> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (version INTEGER NOT NULL PRIMARY KEY)"
    ))
    .execute(&pool)
    .await
    .map_err(db_err)?;
    let current_version = sqlx::query(&format!("SELECT version FROM {MIGRATION_TABLE} LIMIT 1"))
        .fetch_optional(&pool)
        .await
        .map_err(db_err)?
        .map(|row| row.get::<i64, _>(0))
        .unwrap_or(0);

    let mut current_version = current_version;
    for migration in MIGRATIONS {
        if migration.version <= current_version {
            continue;
        }
        sqlx::raw_sql(migration.sql)
            .execute(&pool)
            .await
            .map_err(db_err)?;
        let sql = if current_version == 0 {
            format!("INSERT INTO {MIGRATION_TABLE} (version) VALUES (?1)")
        } else {
            format!("UPDATE {MIGRATION_TABLE} SET version = ?1")
        };
        sqlx::query(&sql)
            .bind(migration.version)
            .execute(&pool)
            .await
            .map_err(db_err)?;
        tracing::debug!(version = migration.version, "applied sqlite migration");
        current_version = migration.version;
    }

    Ok(())
}
