//! `fc-dev fresh` — TRUNCATE every FlowCatalyst-owned table and re-seed
//! the dev defaults.
//!
//! Wipes ALL data (principals, applications, clients, events, dispatch
//! jobs, audit logs, OAuth tokens — everything). Preserves the schema
//! (DDL) and `_schema_migrations`. After the truncate, re-seeds:
//!
//!   - built-in roles
//!   - the `platform` application
//!   - default processes
//!   - a bootstrap admin user (`admin@flowcatalyst.local` /
//!     `DevPassword123!` by default; override with `--admin-email` /
//!     `--admin-password`)
//!
//! So `fc-dev fresh` followed by `fc-dev` lets you log in immediately
//! with the documented dev credentials — no separate `fc-dev init` run
//! required for the simple "I just want a usable local environment" case.
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

    /// Email of the bootstrap admin to recreate after truncate. Defaults
    /// to the documented dev value so `fc-dev fresh` produces a
    /// known-good login.
    #[arg(long, default_value = "admin@flowcatalyst.local")]
    pub admin_email: String,

    /// Password of the bootstrap admin. Defaults to the documented dev
    /// value. Falls back to a relaxed complexity policy if the value
    /// doesn't meet the strict one (matching `bootstrap_admin_user`'s
    /// own fallback so the password you set is the password you can use).
    #[arg(long, default_value = "DevPassword123!")]
    pub admin_password: String,
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

    // Re-seed the same way `fc-dev` does on startup, plus a bootstrap
    // admin so the resulting DB is immediately usable.
    info!("Re-seeding built-in roles, platform application, default processes, admin user");

    fc_platform::shared::database::seed_builtin_roles(&pool)
        .await
        .context("seed built-in roles")?;
    fc_platform::shared::database::seed_platform_application(&pool)
        .await
        .context("seed platform application")?;
    fc_platform::shared::default_processes::seed_default_processes(&pool)
        .await
        .context("seed default processes")?;

    // `bootstrap_admin_user` reads its config from env vars. Setting
    // them here (only for this process) keeps the seeder's existing
    // single source of truth without splitting the API in two.
    std::env::set_var(
        "FLOWCATALYST_BOOTSTRAP_ADMIN_EMAIL",
        &args.admin_email,
    );
    std::env::set_var(
        "FLOWCATALYST_BOOTSTRAP_ADMIN_PASSWORD",
        &args.admin_password,
    );
    fc_platform::shared::bootstrap_admin::bootstrap_admin_user(&pool)
        .await
        .context("bootstrap admin user")?;

    println!("\n✓ All FlowCatalyst tables truncated and re-seeded.");
    println!("  Admin login:  {} / {}", args.admin_email, args.admin_password);
    println!("  Start fc-dev and sign in at http://localhost:8080.");

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
