//! The Docker provider's errors in the shared contract's closed set.
//!
//! Classification uses evidence the provider recorded (its own codes, HTTP
//! status codes and the error chain), never message text. Anything whose
//! outcome the provider could not establish stays `Transport` with an unknown
//! outcome, so a caller never reads an uncertain mutation as refused or done.

use sandbox_driver::contract::{Error as ContractError, MutationOutcome, RemainingResource};
use sandbox_driver::{Error, ProviderError};

use crate::fence;

pub(crate) fn to_contract(error: Error) -> ContractError {
    if let Error::Provider(provider) = error {
        return from_provider(provider);
    }
    let message = error.to_string();
    match error {
        Error::NotFound { resource, id } => ContractError::NotFound {
            resource: format!("{resource:?}").to_lowercase(),
            id,
        },
        Error::NotOwned { id, .. } => ContractError::NotOwned { id },
        Error::Unsupported { capability } => ContractError::Unsupported {
            capability: capability.to_string(),
        },
        Error::InvalidSpec { field, reason } => ContractError::InvalidSpec {
            path:    field,
            message: reason,
        },
        // A lifecycle state that refuses the request: nothing was admitted.
        Error::InvalidState { .. } => ContractError::Busy { message },
        Error::Auth(_) => ContractError::Unauthenticated { message },
        Error::RateLimited { retry_after } => ContractError::ResourceExhausted {
            message,
            retry_after_ms: retry_after
                .map(|after| u64::try_from(after.as_millis()).unwrap_or(u64::MAX)),
        },
        Error::Overloaded { .. } => ContractError::Unavailable {
            message,
            outcome: MutationOutcome::NotStarted,
        },
        _ => unknown(message),
    }
}

fn from_provider(provider: ProviderError) -> ContractError {
    let message = provider.to_string();
    match provider.code.as_deref() {
        Some("busy") => return ContractError::Busy { message },
        Some("fence_leaked") => return ContractError::FenceLeaked { message },
        Some("incomplete_cleanup") => {
            let remaining = provider
                .detail
                .as_ref()
                .and_then(|detail| detail.get("remaining"))
                .and_then(|remaining| {
                    serde_json::from_value::<Vec<RemainingResource>>(remaining.clone()).ok()
                })
                .unwrap_or_default();
            return ContractError::IncompleteCleanup { message, remaining };
        }
        Some("transport") => return unknown(message),
        _ => {}
    }
    let code = provider.code.clone();
    if fence::outcome_unknown(&Error::Provider(provider)) {
        return unknown(message);
    }
    match code.as_deref() {
        Some("401") => ContractError::Unauthenticated { message },
        Some("403") => ContractError::PermissionDenied { message },
        // Docker refuses work on a container that is not running.
        Some("409") => ContractError::Busy { message },
        Some("429") => ContractError::ResourceExhausted {
            message,
            retry_after_ms: None,
        },
        Some("503") => ContractError::Unavailable {
            message,
            outcome: MutationOutcome::Unknown,
        },
        // A completed refusal from the daemon or the workload: an image it
        // cannot run, a service that never became ready, or an object it
        // does not have, such as a removed service. A missing primary is
        // already `Error::NotFound`, so a 404 here never means the sandbox
        // is gone.
        _ => ContractError::Unhealthy { message },
    }
}

fn unknown(message: String) -> ContractError {
    ContractError::Transport {
        message,
        outcome: MutationOutcome::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bollard::errors::Error as DockerApiError;
    use sandbox_driver::{ResourceKind, TransportError};
    use serde_json::json;

    use super::*;
    use crate::daemon::{docker_error, docker_kind};

    fn coded(code: &str) -> Error {
        let mut error = ProviderError::new(docker_kind(), "refused");
        error.code = Some(code.to_owned());
        Error::Provider(error)
    }

    #[test]
    fn fence_codes_keep_their_meaning() {
        assert!(matches!(
            to_contract(coded("busy")),
            ContractError::Busy { .. }
        ));
        assert!(matches!(
            to_contract(coded("fence_leaked")),
            ContractError::FenceLeaked { .. }
        ));
        assert!(matches!(
            to_contract(coded("transport")),
            ContractError::Transport {
                outcome: MutationOutcome::Unknown,
                ..
            }
        ));
    }

    #[test]
    fn incomplete_cleanup_keeps_its_remaining_resources() {
        let mut error = ProviderError::new(docker_kind(), "cleanup failed");
        error.code = Some("incomplete_cleanup".to_owned());
        error.detail = Some(json!({
            "code": "incomplete_cleanup",
            "message": "failed-create cleanup could not be confirmed",
            "remaining": [
                {"kind": "container", "id": "abc"},
                {"kind": "fence_container", "id": "def"},
            ],
        }));
        let ContractError::IncompleteCleanup { remaining, .. } =
            to_contract(Error::Provider(error))
        else {
            panic!("expected incomplete cleanup");
        };
        assert_eq!(remaining, vec![
            RemainingResource {
                kind: "container".to_owned(),
                id:   "abc".to_owned(),
            },
            RemainingResource {
                kind: "fence_container".to_owned(),
                id:   "def".to_owned(),
            },
        ]);
    }

    /// A gateway answer or a lost connection does not establish what the
    /// daemon did; a 500 from the daemon itself is a completed refusal.
    #[test]
    fn uncertain_daemon_outcomes_stay_unknown() {
        for status_code in [502, 504] {
            let error = docker_error(
                "starting container",
                DockerApiError::DockerResponseServerError {
                    status_code,
                    message: "gateway".to_owned(),
                },
            );
            assert!(
                matches!(to_contract(error), ContractError::Transport {
                    outcome: MutationOutcome::Unknown,
                    ..
                }),
                "HTTP {status_code}"
            );
        }
        let reset = docker_error("starting container", DockerApiError::IOError {
            err: io::Error::other("connection reset"),
        });
        assert!(matches!(to_contract(reset), ContractError::Transport {
            outcome: MutationOutcome::Unknown,
            ..
        }));
        let refused = docker_error(
            "starting container",
            DockerApiError::DockerResponseServerError {
                status_code: 500,
                message:     "OCI runtime create failed".to_owned(),
            },
        );
        assert!(matches!(
            to_contract(refused),
            ContractError::Unhealthy { .. }
        ));
        assert!(matches!(
            to_contract(Error::Transport(TransportError::new("lost"))),
            ContractError::Transport {
                outcome: MutationOutcome::Unknown,
                ..
            }
        ));
    }

    #[test]
    fn absence_is_not_found() {
        let error = to_contract(Error::NotFound {
            resource: ResourceKind::Sandbox,
            id:       "abc".to_owned(),
        });
        assert_eq!(error, ContractError::NotFound {
            resource: "sandbox".to_owned(),
            id:       "abc".to_owned(),
        });
    }
}
