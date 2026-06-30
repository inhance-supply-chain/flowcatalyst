//! Map an HTTP response from a mediation target to a `MediationOutcome`.
//!
//! Status-code dispatch:
//! - **2xx with `ack: true`** (or no body) → `Success`.
//! - **2xx with `ack: false`** → `ErrorProcess` with the target's
//!   `delaySeconds` (defaulting to 30) — the target is healthy but
//!   asking us to retry later.
//! - **400 / 401 / 403 / 404 / 501** → `ErrorConfig`. These don't retry
//!   and emit a configuration warning.
//! - **429** → `RateLimited` with the `Retry-After` header (default 30).
//!   The pool nacks with that delay and does NOT consume the retry
//!   budget or trip the circuit breaker.
//! - **Other 4xx** → `ErrorConfig` (no warning).
//! - **5xx** → `ErrorProcess` — retryable transient.
//! - **Anything else** → `ErrorProcess`.

use std::sync::Arc;

use fc_common::{MediationOutcome, MediationResult, Message, WarningCategory, WarningSeverity};
use reqwest::Response;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::warning::WarningService;

#[derive(Debug, Deserialize, Default)]
struct MediationResponse {
    #[serde(default = "default_ack")]
    ack: bool,
    #[serde(rename = "delaySeconds")]
    delay_seconds: Option<u32>,
}

fn default_ack() -> bool {
    true
}

pub(super) async fn classify(
    response: Response,
    message: &Message,
    warning_service: &Arc<WarningService>,
) -> MediationOutcome {
    let status = response.status();
    let status_code = status.as_u16();

    if status.is_success() {
        // Parse response body for ack and delaySeconds.
        if let Ok(body) = response.text().await {
            if let Ok(resp) = serde_json::from_str::<MediationResponse>(&body) {
                if !resp.ack {
                    let delay = resp.delay_seconds.unwrap_or(30);
                    debug!(
                        message_id = %message.id,
                        delay_seconds = delay,
                        "Target returned ack=false with delay"
                    );
                    return MediationOutcome {
                        result: MediationResult::ErrorProcess,
                        delay_seconds: Some(delay),
                        status_code: Some(status_code),
                        error_message: Some("Target returned ack=false".to_string()),
                    };
                }
            }
        }

        debug!(
            message_id = %message.id,
            status_code = status_code,
            "Message delivered successfully"
        );
        return MediationOutcome::success();
    }

    if status_code == 400 {
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Bad request - configuration error"
        );
        emit_config_warning(
            warning_service,
            &message.id,
            &message.mediation_target,
            status_code,
            "Bad Request",
        );
        return MediationOutcome::error_config(status_code, "HTTP 400: Bad request".to_string());
    }

    if status_code == 401 || status_code == 403 {
        let desc = if status_code == 401 {
            "Unauthorized"
        } else {
            "Forbidden"
        };
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Authentication/authorization error"
        );
        emit_config_warning(
            warning_service,
            &message.id,
            &message.mediation_target,
            status_code,
            desc,
        );
        return MediationOutcome::error_config(
            status_code,
            format!("HTTP {}: Auth error", status_code),
        );
    }

    if status_code == 404 {
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Endpoint not found"
        );
        emit_config_warning(
            warning_service,
            &message.id,
            &message.mediation_target,
            status_code,
            "Not Found",
        );
        return MediationOutcome::error_config(status_code, "HTTP 404: Not found".to_string());
    }

    if status_code == 429 {
        // Healthy destination throttling us. Return RateLimited so the
        // pool applies Retry-After without consuming the retry budget or
        // tripping the circuit breaker.
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(30);
        warn!(
            message_id = %message.id,
            status_code = status_code,
            retry_after = retry_after,
            "Rate limited (429) - will retry"
        );
        return MediationOutcome::rate_limited(retry_after);
    }

    if status_code == 501 {
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Not implemented"
        );
        emit_config_warning(
            warning_service,
            &message.id,
            &message.mediation_target,
            status_code,
            "Not Implemented",
        );
        return MediationOutcome::error_config(
            status_code,
            "HTTP 501: Not implemented".to_string(),
        );
    }

    if status.is_client_error() {
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Client error"
        );
        return MediationOutcome::error_config(
            status_code,
            format!("HTTP {}: Client error", status_code),
        );
    }

    if status.is_server_error() {
        warn!(
            message_id = %message.id,
            status_code = status_code,
            "Server error - will retry"
        );
        return MediationOutcome {
            result: MediationResult::ErrorProcess,
            delay_seconds: Some(30),
            status_code: Some(status_code),
            error_message: Some(format!("HTTP {}: Server error", status_code)),
        };
    }

    warn!(
        message_id = %message.id,
        status_code = status_code,
        "Unexpected status code"
    );
    MediationOutcome::error_process(Some(30), format!("HTTP {}: Unexpected status", status_code))
}

/// Push a configuration warning to the `WarningService`. 501 is upgraded
/// to `Critical`; everything else is `Error`.
fn emit_config_warning(
    warning_service: &Arc<WarningService>,
    message_id: &str,
    target: &str,
    status_code: u16,
    description: &str,
) {
    let severity = if status_code == 501 {
        WarningSeverity::Critical
    } else {
        WarningSeverity::Error
    };
    warning_service.add_warning(
        WarningCategory::Configuration,
        severity,
        format!(
            "HTTP {} {} for message {}: Target: {}",
            status_code, description, message_id, target
        ),
        "HttpMediator".to_string(),
    );
}
