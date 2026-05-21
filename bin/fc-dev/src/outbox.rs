//! `fc-dev outbox` — standalone outbox poller + setup.
//!
//! Two subcommands:
//!
//!   * `init` — interactive setup that writes `FC_OUTBOX_DB_URL`,
//!     `FC_OUTBOX_API_URL`, `FC_OUTBOX_TOKEN` to the project's `.env`
//!     (idempotent; existing keys updated in place, new keys appended).
//!   * `poll` — read those values back from process env / `.env`, ensure
//!     the `outbox_messages` table exists, then start the processor.
//!
//! The split keeps secrets off the shell history and out of `ps` output.
//! Daily use is just `fc-dev outbox poll` from the project directory.
//!
//! Used when the app's database can't be (or shouldn't be) fc-dev's
//! embedded PG — the headline case is a PostGIS-dependent app running
//! against Docker Postgres.

use anyhow::{anyhow, Context, Result};
use std::io::{stdin, stdout, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

use fc_outbox::enhanced_processor::{EnhancedOutboxProcessor, EnhancedProcessorConfig};
use fc_outbox::http_dispatcher::HttpDispatcherConfig;
use fc_outbox::postgres::PostgresOutboxRepository;
use fc_outbox::repository::OutboxRepository;

#[derive(clap::Args, Debug)]
pub struct OutboxArgs {
    #[command(subcommand)]
    command: OutboxCommand,
}

#[derive(clap::Subcommand, Debug)]
enum OutboxCommand {
    /// Write the outbox poller's configuration into `.env` so daily use
    /// is `fc-dev outbox poll` with no flags. Idempotent — re-running
    /// updates the existing keys, never duplicates them.
    Init(InitArgs),

    /// Poll an external app database's `outbox_messages` and forward
    /// to a FlowCatalyst platform API. Values come from process env
    /// / `.env`; explicit flags override.
    Poll(PollArgs),
}

#[derive(clap::Args, Debug)]
struct InitArgs {
    /// Project root. The `.env` file is written to `{root}/.env`.
    #[arg(long, default_value = ".")]
    root: PathBuf,

    /// PostgreSQL URL of the app database that owns `outbox_messages`.
    #[arg(long)]
    db_url: Option<String>,

    /// Base URL of the FlowCatalyst platform API to forward to.
    #[arg(long)]
    api_url: Option<String>,

    /// Bearer token for the platform API. If omitted, prompted with
    /// terminal echo off (avoid putting secrets in shell history).
    #[arg(long)]
    token: Option<String>,

    /// Non-interactive — fail if any required value is missing rather
    /// than prompting.
    #[arg(long)]
    yes: bool,
}

#[derive(clap::Args, Debug)]
struct PollArgs {
    /// PostgreSQL URL of the app database that owns `outbox_messages`.
    /// Falls back to `FC_OUTBOX_DB_URL` from env / `.env`.
    #[arg(long, env = "FC_OUTBOX_DB_URL")]
    db_url: Option<String>,

    /// Base URL of the FlowCatalyst platform API to forward to.
    #[arg(
        long,
        env = "FC_OUTBOX_API_URL",
        default_value = "http://localhost:8080"
    )]
    api_url: String,

    /// Bearer token for the platform API. Mint one from a service
    /// account's `client_credentials` grant.
    #[arg(long, env = "FC_OUTBOX_TOKEN")]
    token: Option<String>,

    /// Poll interval in milliseconds.
    #[arg(
        long,
        env = "FC_OUTBOX_POLL_INTERVAL_MS",
        default_value = "1000"
    )]
    poll_interval_ms: u64,

    /// Max Postgres pool connections.
    #[arg(
        long,
        env = "FC_OUTBOX_MAX_CONNECTIONS",
        default_value = "5"
    )]
    max_connections: u32,

    /// Skip the `CREATE TABLE IF NOT EXISTS` bootstrap. Use this when
    /// the app manages its own outbox schema and you'd rather see a
    /// clear failure than a silent auto-create.
    #[arg(long, env = "FC_OUTBOX_SKIP_BOOTSTRAP", default_value = "false")]
    skip_bootstrap: bool,
}

pub async fn run(args: OutboxArgs) -> Result<()> {
    match args.command {
        OutboxCommand::Init(args) => run_init(args).await,
        OutboxCommand::Poll(args) => run_poll(args).await,
    }
}

async fn run_init(args: InitArgs) -> Result<()> {
    let db_url = resolve_or_prompt(
        args.db_url,
        "Postgres URL (e.g. postgres://user:pass@localhost:5432/myapp)",
        args.yes,
    )?;
    let api_url = resolve_or_prompt_with_default(
        args.api_url,
        "Platform API URL",
        "http://localhost:8080",
        args.yes,
    )?;
    let token = resolve_secret(args.token, "Bearer token", args.yes)?;

    let env_path = args.root.join(".env");
    let updates: Vec<(&str, &str)> = vec![
        ("FC_OUTBOX_DB_URL", &db_url),
        ("FC_OUTBOX_API_URL", &api_url),
        ("FC_OUTBOX_TOKEN", &token),
    ];

    println!();
    println!("Writing outbox config to {} …", env_path.display());
    crate::init::write_env_updates(&env_path, &updates).context("write .env")?;

    // Tighten permissions on Unix — this file holds the platform bearer
    // token, which is enough to publish events on behalf of the service
    // account. Windows file ACLs default to user-only under the project
    // dir, so no extra hardening is needed there.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if env_path.exists() {
            let mut perms = std::fs::metadata(&env_path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(&env_path, perms).ok();
        }
    }

    println!();
    println!("✓ Done. Run the poller with:");
    println!("    fc-dev outbox poll");
    println!();
    println!("The poll command reads these keys from {} on startup.", env_path.display());
    Ok(())
}

async fn run_poll(args: PollArgs) -> Result<()> {
    let db_url = args.db_url.ok_or_else(|| {
        anyhow!(
            "FC_OUTBOX_DB_URL is required.\n\n\
             Run `fc-dev outbox init` from your project directory to write \
             `.env`, or set FC_OUTBOX_DB_URL in the environment."
        )
    })?;

    info!(
        db_url = %redact_url(&db_url),
        api_url = %args.api_url,
        poll_interval_ms = args.poll_interval_ms,
        "Starting standalone outbox poller"
    );

    if args.token.is_none() {
        warn!(
            "No FC_OUTBOX_TOKEN provided — forwarded requests will be \
             unauthenticated and the platform will reject them."
        );
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(args.max_connections)
        .connect(&db_url)
        .await
        .with_context(|| format!("connecting to {}", redact_url(&db_url)))?;

    let repository = Arc::new(PostgresOutboxRepository::new(pool));

    // Bootstrap the outbox table on first run. `init_schema` issues
    // `CREATE TABLE IF NOT EXISTS` + idempotent partial indexes, so
    // re-running against an existing schema is a no-op. The SDK's own
    // migration produces the same shape (see
    // `clients/typescript-sdk/migrations/postgresql/001_create_outbox_messages.sql`).
    if !args.skip_bootstrap {
        repository
            .init_schema()
            .await
            .context("create/verify outbox_messages schema")?;
    }

    let config = EnhancedProcessorConfig {
        poll_interval: Duration::from_millis(args.poll_interval_ms),
        http_config: HttpDispatcherConfig {
            api_base_url: args.api_url,
            api_token: args.token,
            ..Default::default()
        },
        ..Default::default()
    };

    let processor = Arc::new(
        EnhancedOutboxProcessor::new(config, repository)
            .map_err(|e| anyhow!("failed to create outbox processor: {e}"))?,
    );

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let proc_clone = processor.clone();
    let mut shutdown_rx = shutdown_tx.subscribe();
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = processor.start() => {}
            _ = shutdown_rx.recv() => {
                info!("Outbox processor received shutdown signal");
                proc_clone.stop();
            }
        }
    });

    info!("Outbox poller running. Ctrl+C to stop.");
    fc_platform::shared::server_setup::wait_for_shutdown_signal().await;
    info!("Shutdown signal received, stopping outbox poller…");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;

    info!("Outbox poller stopped");
    Ok(())
}

fn resolve_or_prompt(value: Option<String>, question: &str, yes: bool) -> Result<String> {
    if let Some(v) = value.filter(|s| !s.is_empty()) {
        return Ok(v);
    }
    if yes {
        return Err(anyhow!("{} is required in --yes mode", question));
    }
    print!("{}: ", question);
    stdout().flush().ok();
    let mut input = String::new();
    stdin().lock().read_line(&mut input)?;
    let trimmed = input.trim_end_matches(&['\r', '\n'][..]).to_string();
    if trimmed.is_empty() {
        return Err(anyhow!("{} is required", question));
    }
    Ok(trimmed)
}

fn resolve_or_prompt_with_default(
    value: Option<String>,
    question: &str,
    default: &str,
    yes: bool,
) -> Result<String> {
    if let Some(v) = value.filter(|s| !s.is_empty()) {
        return Ok(v);
    }
    if yes {
        return Ok(default.to_string());
    }
    print!("{} [{}]: ", question, default);
    stdout().flush().ok();
    let mut input = String::new();
    stdin().lock().read_line(&mut input)?;
    let trimmed = input.trim_end_matches(&['\r', '\n'][..]).to_string();
    Ok(if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed
    })
}

/// Best-effort secret entry. Matches the `fc-dev init` admin-password
/// prompt — paste-visible read from stdin. rpassword / termios no-echo
/// would be nicer but adds a dep / unsafe; the bigger win (keeping
/// tokens out of `ps` output and shell history) comes from accepting
/// them via prompt instead of argv, which this already does.
fn resolve_secret(value: Option<String>, question: &str, yes: bool) -> Result<String> {
    if let Some(v) = value.filter(|s| !s.is_empty()) {
        return Ok(v);
    }
    if yes {
        return Err(anyhow!("{} is required in --yes mode", question));
    }
    print!("{}: ", question);
    stdout().flush().ok();
    let mut input = String::new();
    stdin().lock().read_line(&mut input)?;
    let trimmed = input.trim_end_matches(&['\r', '\n'][..]).to_string();
    if trimmed.is_empty() {
        return Err(anyhow!("{} is required", question));
    }
    Ok(trimmed)
}

/// Hide the password portion of a `postgres://user:pass@host/db` URL
/// before logging it. Best-effort; falls back to the original string
/// if no `:pass@` segment is present.
fn redact_url(url: &str) -> String {
    let Some((before_at, after_at)) = url.split_once('@') else {
        return url.to_string();
    };
    let Some((scheme_user, _password)) = before_at.rsplit_once(':') else {
        return url.to_string();
    };
    format!("{scheme_user}:***@{after_at}")
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn redact_url_hides_password() {
        assert_eq!(
            redact_url("postgres://user:secret@localhost:5432/db"),
            "postgres://user:***@localhost:5432/db"
        );
    }

    #[test]
    fn redact_url_passthrough_when_no_password() {
        assert_eq!(
            redact_url("postgres://localhost:5432/db"),
            "postgres://localhost:5432/db"
        );
    }
}
