//! Bounded cleanup of known resources after a failed admission.

use std::time::Duration;

use sandbox_driver::contract::RemainingResource;
use sandbox_driver::{Error, ErrorReport, ProviderError, ProviderKind, Result, SandboxState};
use tokio::time;

use crate::TRANSITION_POLL;
use crate::sdk::{
    DaytonaClient, daytona_error, is_not_found, is_state_change_in_progress, map_state,
};

/// A single admitted delete request, followed by observations of authoritative
/// absence. A delete rejected because a state change is in progress was never
/// admitted, so it is repeated; no uncertain mutation is replayed. A failed
/// observation cannot prove cleanup.
async fn delete_and_confirm(client: &DaytonaClient, id: &str) -> Result<()> {
    loop {
        match client.delete(id).await {
            Ok(()) => break,
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) if is_state_change_in_progress(&error) => {
                time::sleep(TRANSITION_POLL).await;
            }
            Err(error) => return Err(daytona_error("requesting failed-create cleanup", error)),
        }
    }
    loop {
        match client.get(id).await {
            Ok(sandbox) if map_state(sandbox.state) == SandboxState::Deleted => return Ok(()),
            Ok(_) => {}
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) => return Err(daytona_error("confirming failed-create cleanup", error)),
        }
        time::sleep(TRANSITION_POLL).await;
    }
}

pub(crate) async fn failed_create(
    client: &DaytonaClient,
    kind: &ProviderKind,
    id: &str,
    cause: Error,
    budget: Duration,
) -> Error {
    let cleanup = time::timeout(budget, delete_and_confirm(client, id))
        .await
        .unwrap_or_else(|_| {
            Err(Error::Timeout {
                operation: "confirming failed-create cleanup".to_owned(),
                elapsed:   budget,
            })
        });
    let Err(cleanup) = cleanup else {
        return cause;
    };
    // Preserve the legacy error surface and both failures. The code and
    // `remaining` use the shared contract's incomplete-cleanup shape, so the
    // adapter can lift it without matching message text.
    let mut error = ProviderError::with_source(
        kind.clone(),
        format!("failed-create cleanup is unconfirmed for sandbox {id}"),
        cause,
    );
    error.code = Some("incomplete_cleanup".to_owned());
    let remaining = [RemainingResource {
        kind: "sandbox".to_owned(),
        id:   id.to_owned(),
    }];
    error.detail = Some(serde_json::json!({
        "remaining": remaining,
        "cleanup": ErrorReport::from(&cleanup),
    }));
    Error::Provider(error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::fake_daytona::{FakeDaytona, Reply, sandbox_json};

    fn sandbox(state: &str) -> Reply {
        Reply::Json(200, sandbox_json(state))
    }

    /// Runs failed-create cleanup of `sb-1` against a fake answering each
    /// request by method.
    async fn clean_up(
        script: impl Fn(&str) -> Reply + Send + Sync + 'static,
        budget: Duration,
    ) -> (Error, FakeDaytona) {
        let fake = FakeDaytona::start(|_| Arc::new(move |method, _| script(method))).await;
        let client = Arc::new(fake.client().await);
        let kind = ProviderKind::try_new("daytona").expect("provider kind");
        let cause = Error::invalid_spec("service", "readiness failed");
        let error = failed_create(&client, &kind, "sb-1", cause, budget).await;
        (error, fake)
    }

    fn assert_unconfirmed(error: &Error, cleanup_kind: &str) {
        let Error::Provider(provider) = error else {
            panic!("expected unconfirmed cleanup, got {error:?}");
        };
        assert_eq!(provider.code.as_deref(), Some("incomplete_cleanup"));
        assert!(!provider.retryable, "uncertain creates are not replayable");
        let detail = provider.detail.as_ref().unwrap();
        assert_eq!(detail["remaining"][0]["id"], "sb-1");
        assert_eq!(detail["cleanup"]["kind"], cleanup_kind);
        assert!(
            ErrorReport::from(error)
                .causes
                .iter()
                .any(|cause| cause.contains("readiness failed"))
        );
    }

    #[tokio::test]
    async fn accepted_delete_waits_for_absence_without_replaying_delete() {
        let observations = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&observations);
        let (error, fake) = clean_up(
            move |method| {
                if method == "DELETE" {
                    return sandbox("destroying");
                }
                if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                    sandbox("destroying")
                } else {
                    Reply::Json(404, "{}".to_owned())
                }
            },
            Duration::from_secs(5),
        )
        .await;
        assert!(matches!(error, Error::InvalidSpec { .. }), "{error:?}");
        assert_eq!(observations.load(Ordering::SeqCst), 2);
        assert_eq!(fake.deletes(), 1);
    }

    #[tokio::test]
    async fn delete_rejected_during_a_state_change_is_repeated_until_admitted() {
        let rejections = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::clone(&rejections);
        let (error, fake) = clean_up(
            move |method| match method {
                "DELETE" if rejected.fetch_add(1, Ordering::SeqCst) == 0 => Reply::Json(
                    400,
                    r#"{"message":"Sandbox state change in progress"}"#.to_owned(),
                ),
                "DELETE" => sandbox("destroying"),
                _ => Reply::Json(404, "{}".to_owned()),
            },
            Duration::from_secs(5),
        )
        .await;
        assert!(matches!(error, Error::InvalidSpec { .. }), "{error:?}");
        assert_eq!(fake.deletes(), 2);
    }

    #[tokio::test]
    async fn terminal_deleted_state_and_delete_not_found_confirm_cleanup() {
        for missing in [false, true] {
            let (error, fake) = clean_up(
                move |method| match method {
                    "DELETE" if missing => Reply::Json(404, "{}".to_owned()),
                    "DELETE" => sandbox("destroying"),
                    _ => sandbox("destroyed"),
                },
                Duration::from_secs(1),
            )
            .await;
            assert!(matches!(error, Error::InvalidSpec { .. }), "{error:?}");
            assert_eq!(fake.deletes(), 1);
        }
    }

    #[tokio::test]
    async fn pending_deletion_or_stalled_requests_retain_the_resource_id() {
        for hang in ["delete", "observe", "neither"] {
            let (error, fake) = time::timeout(
                Duration::from_secs(2),
                clean_up(
                    move |method| {
                        if (method == "DELETE" && hang == "delete")
                            || (method == "GET" && hang == "observe")
                        {
                            Reply::Hang
                        } else {
                            sandbox("destroying")
                        }
                    },
                    Duration::from_millis(100),
                ),
            )
            .await
            .expect("cleanup has an overall deadline");
            assert_unconfirmed(&error, "timeout");
            assert_eq!(fake.deletes(), 1);
        }
    }

    #[tokio::test]
    async fn delete_or_observation_failure_is_not_absence() {
        for fail_delete in [false, true] {
            let (error, fake) = clean_up(
                move |method| {
                    if method == "DELETE" && !fail_delete {
                        sandbox("destroying")
                    } else {
                        Reply::Json(500, "{}".to_owned())
                    }
                },
                Duration::from_secs(1),
            )
            .await;
            assert_unconfirmed(&error, "provider");
            assert_eq!(fake.deletes(), 1);
        }
    }
}
