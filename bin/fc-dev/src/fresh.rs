//! `fc-dev fresh` — TRUNCATE every FlowCatalyst-owned table.
//!
//! Wipes ALL data (principals, applications, clients, events, dispatch
//! jobs, audit logs, OAuth tokens — everything). Preserves the schema
//! (DDL) and `_schema_migrations` so the database is immediately usable
//! again on next fc-dev startup. Built-in roles + the platform
//! application + default processes are re-seeded automatically when
//! fc-dev starts (those seeders are idempotent).
//!
//! Refuses to run without explicit confirmation. Intended for the local
//! dev loop only — there is no "remote" mode.

use anyhow::{Context, Result};
use sqlx::Row;
use std::io::Write;
use tracing::{info, warn};

/// Table-name prefixes that mark a table as FlowCatalyst-owned. The
/// fresh command will truncate every table in the `public` schema whose
/// name starts with one of these. Update this list when adding a new
/// table prefix (see migrations under `crates/fc-platform/migrations/`).
const FC_TABLE_PREFIXES: &[&str] = &[
    "iam_",
    "msg_",
    "aud_",
    "tnt_",
    "oauth_",
    "webauthn_",
    "outbox_",
    "fc_",
];

#[derive(clap::Args, Debug)]
pub struct FreshArgs {
    /// PostgreSQL database URL. Falls back to FC_DATABASE_URL env var
    /// or the default localhost connection.
    #[arg(
        long,
        env = "FC_DATABASE_URL",
        default_value = "postgresql://localhost:5432/flowcatalyst"
    )]
    pub database_url: String,

    /// Use the embedded PostgreSQL instance (the one fc-dev starts at
    /// :15432 in `~/.cache/flowcatalyst-dev/pgdata/`). Default true so
    /// `fc-dev fresh` matches `fc-dev` (start) without extra flags.
    #[cfg(feature = "embedded-db")]
    #[arg(long, env = "FC_EMBEDDED_DB", default_value = "true")]
    pub embedded_db: bool,

    /// Skip the interactive confirmation. ONLY pass this when scripted —
    /// fresh is destructive and irreversible.
    #[arg(long)]
    pub yes: bool,
}

pub async fn run(args: FreshArgs) -> Result<()> {
    // Embedded PG, if requested. Same data dir as the start path so the
    // truncation hits the right database.
    #[cfg(feature = "embedded-db")]
    let (db_url, mut _embedded) = if args.embedded_db {
        let emb = crate::embedded_pg::start(false).await?;
        let url = emb.url.clone();
        (url, Some(emb))
    } else {
        (args.database_url.clone(), None)
    };
    #[cfg(not(feature = "embedded-db"))]
    let db_url = args.database_url.clone();

    let pool = fc_platform::shared::database::create_pool(&db_url)
        .await
        .context("connect to database")?;

    // Discover which FC tables actually exist. We list them up-front so
    // the confirmation prompt can show the user exactly what's about to
    // be truncated — no surprises.
    let tables = discover_fc_tables(&pool).await?;

    if tables.is_empty() {
        println!("No FlowCatalyst tables found in the database. Nothing to truncate.");
        #[cfg(feature = "embedded-db")]
        if let Some(mut e) = _embedded {
            crate::embedded_pg::stop(&mut e).await;
        }
        return Ok(());
    }

    println!(
        "About to TRUNCATE the following {} FlowCatalyst tables in {}:",
        tables.len(),
        db_url
    );
    for t in &tables {
        println!("  - {}", t);
    }
    println!(
        "\nThis blanks every principal, application, client, event,\n\
         dispatch job, audit log, and OAuth token. The schema is\n\
         preserved; built-in roles + platform application are re-seeded\n\
         automatically when fc-dev next starts.\n"
    );

    if !args.yes {
        print!("Type \"fresh\" to confirm: ");
        std::io::stdout().flush().ok();
        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("read confirmation")?;
        if input.trim() != "fresh" {
            println!("Aborted — confirmation did not match.");
            #[cfg(feature = "embedded-db")]
            if let Some(mut e) = _embedded {
                crate::embedded_pg::stop(&mut e).await;
            }
            return Ok(());
        }
    }

    // One statement per table; CASCADE handles FK chains. RESTART IDENTITY
    // resets sequences so re-seeded ids don't collide with stale ones.
    info!(table_count = tables.len(), "Truncating FlowCatalyst tables");
    let stmt = format!(
        "TRUNCATE TABLE {} RESTART IDENTITY CASCADE",
        tables
            .iter()
            .map(|t| format!("\"{}\"", t))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Err(e) = sqlx::query(&stmt).execute(&pool).await {
        warn!(error = %e, "TRUNCATE failed");
        return Err(anyhow::anyhow!("TRUNCATE failed: {}", e));
    }

    println!("\n✓ All FlowCatalyst tables truncated. Next steps:");
    println!("  • Restart fc-dev (re-seeds built-in roles + platform app), or");
    println!("  • Run `fc-dev init` to bootstrap an admin + application.");

    #[cfg(feature = "embedded-db")]
    if let Some(mut e) = _embedded {
        crate::embedded_pg::stop(&mut e).await;
    }
    Ok(())
}

async fn discover_fc_tables(pool: &sqlx::PgPool) -> Result<Vec<String>> {
    let prefix_predicate = FC_TABLE_PREFIXES
        .iter()
        .map(|p| format!("tablename LIKE '{}%'", p))
        .collect::<Vec<_>>()
        .join(" OR ");
    let sql = format!(
        "SELECT tablename FROM pg_tables \
         WHERE schemaname = 'public' AND ({}) \
         ORDER BY tablename",
        prefix_predicate
    );
    let rows = sqlx::query(&sql)
        .fetch_all(pool)
        .await
        .context("query pg_tables for FC tables")?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.try_get::<String, _>("tablename").ok())
        .collect())
}
