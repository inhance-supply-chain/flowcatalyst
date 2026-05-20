//! `fc-dev outbox poll` — standalone outbox poller.
//!
//! Polls an external app's `outbox_messages` Postgres table and forwards
//! Events / DispatchJobs / AuditLogs to a FlowCatalyst platform API.
//! Useful when the app runs against a Postgres you can't (or don't want
//! to) use as fc-dev's embedded DB — e.g. PostGIS in Docker.
//!
//! Unlike `fc-dev start --outbox-enabled`, this subcommand boots nothing
//! else: no embedded Postgres, no platform API, no queue, no scheduler.
//! Just the outbox processor and an HTTP client pointing at whatever
//! `--api-url` / `--token` you pass.

use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{info, warn};

use fc_outbox::enhanced_processor::{EnhancedOutboxProcessor, EnhancedProcessorConfig};
use fc_outbox::http_dispatcher::HttpDispatcherConfig;
use fc_outbox::postgres::PostgresOutboxRepository;

#[derive(clap::Args, Debug)]
pub struct OutboxArgs {
    #[command(subcommand)]
    command: OutboxCommand,
}

#[derive(clap::Subcommand, Debug)]
enum OutboxCommand {
    /// Poll an external app database's `outbox_messages` and forward
    /// to a FlowCatalyst platform API.
    Poll(PollArgs),
}

#[derive(clap::Args, Debug)]
struct PollArgs {
    /// PostgreSQL URL of the app database that owns `outbox_messages`.
    #[arg(long, env = "FC_OUTBOX_DB_URL")]
    db_url: String,

    /// Base URL of the FlowCatalyst platform API to forward to.
    #[arg(
        long,
        env = "FC_OUTBOX_API_URL",
        default_value = "http://localhost:8080"
    )]
    api_url: String,

    /// Bearer token for the platform API. Mint one from a service
    /// account's `client_credentials` grant. Required in practice —
    /// the batch ingest endpoints reject unauthenticated requests.
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
}

pub async fn run(args: OutboxArgs) -> Result<()> {
    match args.command {
        OutboxCommand::Poll(args) => run_poll(args).await,
    }
}

async fn run_poll(args: PollArgs) -> Result<()> {
    info!(
        db_url = %redact_url(&args.db_url),
        api_url = %args.api_url,
        poll_interval_ms = args.poll_interval_ms,
        "Starting standalone outbox poller"
    );

    if args.token.is_none() {
        warn!(
            "No --token (FC_OUTBOX_TOKEN) provided — forwarded requests will be \
             unauthenticated and the platform will reject them."
        );
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(args.max_connections)
        .connect(&args.db_url)
        .await
        .with_context(|| format!("connecting to {}", redact_url(&args.db_url)))?;

    let repository = Arc::new(PostgresOutboxRepository::new(pool));

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
