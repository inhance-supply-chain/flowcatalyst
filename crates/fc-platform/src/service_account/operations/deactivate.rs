//! Deactivate Service Account Use Case

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::events::ServiceAccountDeactivated;
use crate::usecase::{ExecutionContext, UnitOfWork, UseCase, UseCaseError, UseCaseResult};
use crate::ServiceAccountRepository;

/// Command for deactivating a service account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeactivateServiceAccountCommand {
    /// Service account ID
    pub id: String,
}

/// Use case for deactivating a service account. Flips `active=false` on
/// the SA without touching its OAuth client — that's the caller's
/// responsibility (and the application-deactivate cascade does it
/// explicitly so the audit log records both).
pub struct DeactivateServiceAccountUseCase<U: UnitOfWork> {
    service_account_repo: Arc<ServiceAccountRepository>,
    unit_of_work: Arc<U>,
}

impl<U: UnitOfWork> DeactivateServiceAccountUseCase<U> {
    pub fn new(service_account_repo: Arc<ServiceAccountRepository>, unit_of_work: Arc<U>) -> Self {
        Self {
            service_account_repo,
            unit_of_work,
        }
    }
}

#[async_trait]
impl<U: UnitOfWork> UseCase for DeactivateServiceAccountUseCase<U> {
    type Command = DeactivateServiceAccountCommand;
    type Event = ServiceAccountDeactivated;

    async fn validate(
        &self,
        _command: &DeactivateServiceAccountCommand,
    ) -> Result<(), UseCaseError> {
        Ok(())
    }

    async fn authorize(
        &self,
        _command: &DeactivateServiceAccountCommand,
        _ctx: &ExecutionContext,
    ) -> Result<(), UseCaseError> {
        Ok(())
    }

    async fn execute(
        &self,
        command: DeactivateServiceAccountCommand,
        ctx: ExecutionContext,
    ) -> UseCaseResult<ServiceAccountDeactivated> {
        let mut sa = match self.service_account_repo.find_by_id(&command.id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                return UseCaseResult::failure(UseCaseError::not_found(
                    "SERVICE_ACCOUNT_NOT_FOUND",
                    format!("Service account with ID '{}' not found", command.id),
                ));
            }
            Err(e) => {
                return UseCaseResult::failure(UseCaseError::commit(format!(
                    "Failed to find service account: {}",
                    e
                )));
            }
        };

        // Idempotent: already-inactive SA is a no-op success so the
        // cascade caller doesn't have to filter.
        if !sa.active {
            let event = ServiceAccountDeactivated::new(&ctx, &sa.id, &sa.code);
            return self
                .unit_of_work
                .commit(&sa, &*self.service_account_repo, event, &command)
                .await;
        }

        sa.deactivate();

        let event = ServiceAccountDeactivated::new(&ctx, &sa.id, &sa.code);

        self.unit_of_work
            .commit(&sa, &*self.service_account_repo, event, &command)
            .await
    }
}
