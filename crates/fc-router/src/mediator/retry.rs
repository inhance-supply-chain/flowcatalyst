//! Retry loop around `mediate_once`.
//!
//! Success, config errors, and rate-limit responses bypass retries —
//! they should be terminal for the dispatcher, not consume the retry
//! budget here. Everything else (connection error, process error, 5xx)
//! retries up to `max_retries - 1` times with delays drawn from
//! `retry_delays` (falling back to 3s once the configured list runs out).

use std::future::Future;
use std::time::Duration;

use fc_common::{MediationOutcome, MediationResult};
use tracing::debug;

pub(super) async fn run<F, Fut>(
    message_id: &str,
    max_retries: u32,
    retry_delays: &[Duration],
    mut mediate_once: F,
) -> MediationOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = MediationOutcome>,
{
    let mut attempts: u32 = 0;
    loop {
        let outcome = mediate_once().await;

        if outcome.result == MediationResult::Success
            || outcome.result == MediationResult::ErrorConfig
            || outcome.result == MediationResult::RateLimited
        {
            return outcome;
        }

        attempts += 1;
        if attempts >= max_retries {
            return outcome;
        }

        let delay = retry_delays
            .get(attempts as usize - 1)
            .copied()
            .unwrap_or(Duration::from_secs(3));

        debug!(
            message_id = %message_id,
            attempt = attempts,
            delay_ms = delay.as_millis(),
            "Retrying mediation"
        );
        tokio::time::sleep(delay).await;
    }
}
