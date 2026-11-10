//! Daemon-scoped admission serialization. Docker's unique container names
//! provide the compare-and-create operation shared by independent clients.
//!
//! The marker is never started. A dropped request deliberately retains it:
//! losing the caller cannot prove that the daemon finished its mutation.
//! No lease expiry or automatic takeover can restart work behind a drain.

use std::collections::HashMap;
use std::future::Future;
use std::process;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bollard::Docker;
use bollard::container::{Config, CreateContainerOptions, ListContainersOptions};
use bollard::errors::Error as DockerApiError;
use bollard::models::{ContainerCreateResponse, ContainerInspectResponse};
use futures_util::future::BoxFuture;
use sandbox_driver::contract::RemainingResource;
use sandbox_driver::{Error, ProviderError, Result, TransportError};
use serde_json::Value;
use tokio::time::error::Elapsed;
use tokio::time::{Instant, sleep, timeout};

use crate::MANAGED_LABEL;
use crate::container::{inspect_container, remove_container_forced};
use crate::daemon::{docker_error, docker_kind, is_conflict};
use crate::inspect::label_of;
use crate::one_shot::ONE_SHOT_LABEL;
use crate::sandbox::DockerSandbox;
use crate::sidecars::NETWORK_LABEL;

mod failure;

use failure::{Disposition, diagnostic};
pub(crate) use failure::{Failure, WorkResult, combine, outcome_unknown};

pub(crate) const FENCE_NAME_LABEL: &str = "sh.sandbox-driver.fence-name";
pub(crate) const FENCE_LABEL: &str = "sh.sandbox-driver.fence";
const OPERATION_LABEL: &str = "sh.sandbox-driver.fence-operation";
const SANDBOX_NAME_LABEL: &str = "sh.sandbox-driver.fence-sandbox-name";
const ADMISSION_WAIT: Duration = Duration::from_secs(5);
const MAX_ADMISSION_POLL: Duration = Duration::from_millis(200);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Creation and start wait on service readiness. Images are pulled before
/// the claim is taken.
const PROVISIONING_TIMEOUT: Duration = Duration::from_secs(360);
/// Creation work after the primary exists. It is bounded inside the claim so
/// that a timeout still runs failed-create cleanup; the claim's own bound
/// leaves room for creating the primary and for that cleanup.
pub(crate) const PROVISIONING_WORK: Duration = Duration::from_secs(300);

/// What a fence owner is doing, recorded on its marker.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Admit,
    Create,
    Start,
    Stop,
    Delete,
    Pause,
    Resume,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::Admit => "admit",
            Self::Create => "create",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Delete => "delete",
            Self::Pause => "pause",
            Self::Resume => "resume",
        }
    }

    fn budget(self) -> Duration {
        match self {
            Self::Create | Self::Start => PROVISIONING_TIMEOUT,
            _ => OPERATION_TIMEOUT,
        }
    }

    /// An absent primary cannot admit new work, so only teardown proceeds.
    fn tolerates_absent(self) -> bool {
        matches!(self, Self::Stop | Self::Delete)
    }
}

/// Serializes admissions and lifecycle changes, not the lifetime of a command.
/// Once admission completes, stopping the primary and sweeping its labeled
/// siblings ends the admitted work. The marker is excluded from inventory.
pub(crate) fn run<'a, T: Send + 'a>(
    docker: &'a Docker,
    id: &'a str,
    operation: Operation,
    work: impl Future<Output = WorkResult<T>> + Send + 'a,
) -> BoxFuture<'a, Result<T>> {
    // Box this orchestration boundary: combining daemon inspection, claim
    // allocation, and the operation would otherwise inflate every caller.
    Box::pin(async move {
        // Each daemon call in `acquire` carries its own bound, so a slow
        // marker creation still reports the marker it may have left.
        let Some(marker) = acquire(docker, id, operation).await? else {
            return timeout(operation.budget(), work)
                .await
                .map_err(|_| {
                    unconfirmed(
                        id,
                        "operation on an absent sandbox timed out; outcome is unknown",
                    )
                })?
                .map_err(Failure::into_error);
        };
        finish(docker, &marker, id, operation, work, |_| {
            Some(id.to_owned())
        })
        .await
    })
}

async fn finish<T>(
    docker: &Docker,
    marker: &str,
    sandbox: &str,
    operation: Operation,
    work: impl Future<Output = WorkResult<T>>,
    completed_sandbox: impl FnOnce(&T) -> Option<String>,
) -> Result<T> {
    // A completed response (including a refusal) releases ownership. A lost
    // response does not prove that the daemon stopped processing the request.
    let result = match timeout(operation.budget(), work).await {
        Ok(Err(failure)) if failure.disposition == Disposition::Reconcile => {
            return Err(retain_error(marker, failure.into_error()));
        }
        Ok(result) => result,
        Err(_) => return Err(retained(marker, "operation timed out", None)),
    };
    let sandbox_id = result.as_ref().ok().and_then(completed_sandbox);
    match timeout(
        OPERATION_TIMEOUT,
        remove_container_forced(docker, marker, "releasing sandbox fence"),
    )
    .await
    {
        Ok(Ok(())) => result.map_err(Failure::into_error),
        Ok(Err(error)) => Err(release_failure(
            marker,
            sandbox,
            sandbox_id.as_deref(),
            "fence release failed",
            error,
            &result,
        )),
        Err(_) => Err(release_failure(
            marker,
            sandbox,
            sandbox_id.as_deref(),
            "fence release timed out",
            Error::Transport(TransportError::new("fence release timed out")),
            &result,
        )),
    }
}

/// Release can fail after either a successful operation or a definite refusal.
/// Record both outcomes before converting back to the public error contract.
fn release_failure<T>(
    marker: &str,
    sandbox: &str,
    sandbox_id: Option<&str>,
    reason: &str,
    release: Error,
    result: &WorkResult<T>,
) -> Error {
    let operation_error = result
        .as_ref()
        .err()
        .map(|failure| diagnostic(&failure.error));
    let release_error = diagnostic(&release);
    let mut failure = ProviderError::with_source(
        docker_kind(),
        format!("{reason}; sandbox fence retained for reconciliation"),
        release,
    );
    failure.code = Some("transport".to_owned());
    failure.detail = Some(serde_json::json!({
        "fence_container": marker,
        "sandbox": sandbox,
        "sandbox_id": sandbox_id,
        "outcome": "unknown",
        "operation_outcome": if result.is_ok() { "succeeded" } else { "failed" },
        "operation_error": operation_error,
        "release_error": release_error,
    }));
    Error::Provider(failure)
}

async fn acquire(docker: &Docker, id: &str, operation: Operation) -> Result<Option<String>> {
    // Canonical IDs prevent names and short IDs from producing different locks.
    let inspect = match discover(docker, id, id).await {
        Ok(inspect) => inspect,
        Err(Error::NotFound { .. }) if operation.tolerates_absent() => {
            // An earlier request may still own its marker, though, so
            // absence alone is insufficient.
            ensure_absent(docker, id).await?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let id = inspect
        .id
        .as_deref()
        .ok_or_else(|| Error::Transport(TransportError::new("daemon omitted the sandbox ID")))?;
    let image = inspect
        .image
        .as_deref()
        .ok_or_else(|| Error::Transport(TransportError::new("daemon omitted the sandbox image")))?;
    let name = fence_name(&inspect, id);
    // Keep the name on the claim: the primary may disappear before release,
    // after which Docker can no longer resolve that name to its canonical ID.
    let sandbox_name = inspect
        .name
        .as_deref()
        .unwrap_or_default()
        .trim_start_matches('/');
    let deadline = Instant::now() + ADMISSION_WAIT;
    let mut poll = Duration::from_millis(10);
    loop {
        match create_marker(docker, image, &name, id, sandbox_name, operation).await {
            Ok(Err(error)) if is_conflict(&error) => {
                // Short concurrent admissions can queue, preserving concurrent
                // exec callers. Lifecycle ownership is always a Busy refusal.
                let queued = match discover(docker, id, &name).await {
                    Ok(marker) => {
                        label_of(&marker, OPERATION_LABEL) == Some(Operation::Admit.label())
                    }
                    // Released between the attempt and the inspect: retry, but
                    // still within the admission wait.
                    Err(Error::NotFound { .. }) => true,
                    Err(error) => return Err(error),
                };
                if !queued || Instant::now() >= deadline {
                    return Err(busy("another operation owns the sandbox fence"));
                }
                sleep(poll).await;
                poll = (poll * 2).min(MAX_ADMISSION_POLL);
            }
            attempt => return claimed(&name, attempt).map(Some),
        }
    }
}

/// Whether a stop or delete holds the claim of the inspected primary now.
/// Work admitted earlier that ends while one is held was ended by it.
pub(crate) async fn draining(docker: &Docker, primary: &ContainerInspectResponse) -> Result<bool> {
    let Some(id) = primary.id.as_deref() else {
        return Ok(false);
    };
    match discover(docker, id, &fence_name(primary, id)).await {
        Ok(marker) => Ok(matches!(
            label_of(&marker, OPERATION_LABEL),
            Some(label) if label == Operation::Stop.label() || label == Operation::Delete.label()
        )),
        Err(Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// The name of the inspected primary's fence marker: the one recorded at
/// create, or the name sandboxes created before that was recorded use.
fn fence_name(primary: &ContainerInspectResponse, id: &str) -> String {
    label_of(primary, FENCE_NAME_LABEL)
        .map_or_else(|| format!("sandbox-driver-fence-{id}"), str::to_owned)
}

/// Allocates the gate before the primary is visible to attach/list. Its name
/// is stored in the primary's immutable labels, so all later clients use it.
pub(crate) fn reserve_creation(labels: &mut Option<HashMap<String, String>>) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = format!(
        "sandbox-driver-fence-create-{}-{time}-{}",
        process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    labels
        .get_or_insert_default()
        .insert(FENCE_NAME_LABEL.to_owned(), name.clone());
    name
}

/// Claims creation under `name`. The marker is labeled with `sandbox`, the
/// name the caller requested, so a delete of that name sees the claim before
/// the primary exists.
pub(crate) fn create<'a>(
    docker: &'a Docker,
    image: &'a str,
    name: &'a str,
    sandbox: &'a str,
    work: impl Future<Output = WorkResult<Arc<DockerSandbox>>> + Send + 'a,
) -> BoxFuture<'a, Result<Arc<DockerSandbox>>> {
    // Boxed for the same reason as `run`.
    Box::pin(async move {
        let attempt = create_marker(docker, image, name, sandbox, sandbox, Operation::Create).await;
        let marker = claimed(name, attempt)?;
        finish(
            docker,
            &marker,
            sandbox,
            Operation::Create,
            work,
            |sandbox| Some(sandbox.id.as_str().to_owned()),
        )
        .await
    })
}

type MarkerAttempt = StdResult<StdResult<ContainerCreateResponse, DockerApiError>, Elapsed>;

async fn create_marker(
    docker: &Docker,
    image: &str,
    name: &str,
    id: &str,
    sandbox_name: &str,
    operation: Operation,
) -> MarkerAttempt {
    timeout(
        OPERATION_TIMEOUT,
        docker.create_container(
            Some(CreateContainerOptions {
                name:     name.to_owned(),
                platform: None,
            }),
            Config {
                image: Some(image.to_owned()),
                entrypoint: Some(vec!["/bin/false".to_owned()]),
                labels: Some(HashMap::from([
                    (FENCE_LABEL.to_owned(), id.to_owned()),
                    (SANDBOX_NAME_LABEL.to_owned(), sandbox_name.to_owned()),
                    (MANAGED_LABEL.to_owned(), "false".to_owned()),
                    (OPERATION_LABEL.to_owned(), operation.label().to_owned()),
                    // Image labels are merged by Docker. A claim must never
                    // inherit a sibling-discovery label and be swept while
                    // its critical section is still running.
                    (ONE_SHOT_LABEL.to_owned(), String::new()),
                    (NETWORK_LABEL.to_owned(), String::new()),
                ])),
                ..Default::default()
            },
        ),
    )
    .await
}

/// The marker a creation attempt produced, unless the daemon refused it.
fn claimed(name: &str, attempt: MarkerAttempt) -> Result<String> {
    match attempt {
        Ok(Ok(created)) if created.id.is_empty() => {
            Err(retained(name, "daemon omitted the fence ID", None))
        }
        Ok(Ok(created)) => Ok(created.id),
        Ok(Err(error)) => {
            let failure = Failure::from(docker_error("acquiring sandbox fence", error));
            Err(if failure.disposition == Disposition::Reconcile {
                retained(
                    name,
                    "fence acquisition outcome is unknown",
                    Some(failure.into_error()),
                )
            } else {
                failure.into_error()
            })
        }
        Err(_) => Err(retained(name, "fence acquisition timed out", None)),
    }
}

/// An absent primary is not proof that an earlier request released ownership.
async fn ensure_absent(docker: &Docker, id: &str) -> Result<()> {
    let markers = timeout(
        OPERATION_TIMEOUT,
        docker.list_containers(Some(ListContainersOptions {
            all: true,
            // Short IDs cannot be matched by a Docker label-equality filter.
            // Inspect only claims, then match their preserved identities.
            filters: HashMap::from([("label".to_owned(), vec![FENCE_LABEL.to_owned()])]),
            ..Default::default()
        })),
    )
    .await
    .map_err(|_| acquisition_timeout(id))?
    .map_err(|error| docker_error("discovering sandbox fence", error))?;
    let owns_identifier = markers.iter().any(|marker| {
        let Some(labels) = &marker.labels else {
            return false;
        };
        let owner = labels
            .get(FENCE_LABEL)
            .map(String::as_str)
            .unwrap_or_default();
        owner == id
            || labels
                .get(SANDBOX_NAME_LABEL)
                .is_some_and(|name| name == id)
            || (owner.len() == 64
                && owner.bytes().all(|byte| byte.is_ascii_hexdigit())
                && !id.is_empty()
                && owner.starts_with(id))
    });
    if owns_identifier {
        Err(busy("another operation owns the sandbox fence"))
    } else {
        Ok(())
    }
}

fn busy(message: &str) -> Error {
    let mut error = ProviderError::new(docker_kind(), message);
    error.code = Some("busy".to_owned());
    Error::Provider(error)
}

fn retain_error(marker: &str, error: Error) -> Error {
    let Error::Provider(mut failure) = error else {
        return retained(marker, "daemon outcome is unknown", Some(error));
    };
    if record_marker(&mut failure, marker) {
        return Error::Provider(failure);
    }
    retained(
        marker,
        "daemon outcome is unknown",
        Some(Error::Provider(failure)),
    )
}

/// Adds the retained marker to a failure that already describes what
/// remains, reporting whether its detail had a place for it.
fn record_marker(failure: &mut ProviderError, marker: &str) -> bool {
    match failure.code.as_deref() {
        Some("fence_leaked") => failure
            .detail
            .get_or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .map(|detail| detail.insert("fence_container".to_owned(), marker.into()))
            .is_some(),
        Some("incomplete_cleanup") => failure
            .detail
            .as_mut()
            .and_then(|detail| detail.get_mut("remaining"))
            .and_then(Value::as_array_mut)
            .map(|remaining| {
                let resource = RemainingResource {
                    kind: "fence_container".to_owned(),
                    id:   marker.to_owned(),
                };
                remaining.push(serde_json::to_value(resource).expect("resource serializes"));
            })
            .is_some(),
        _ => false,
    }
}

/// Inspects `container` while acquiring the fence for sandbox `id`.
async fn discover(docker: &Docker, id: &str, container: &str) -> Result<ContainerInspectResponse> {
    timeout(OPERATION_TIMEOUT, inspect_container(docker, container))
        .await
        .map_err(|_| acquisition_timeout(id))?
}

fn acquisition_timeout(id: &str) -> Error {
    unconfirmed(
        id,
        "fence acquisition or discovery timed out; ownership requires reconciliation",
    )
}

fn unconfirmed(id: &str, message: &str) -> Error {
    let mut error = ProviderError::new(docker_kind(), message);
    error.code = Some("transport".to_owned());
    error.detail = Some(serde_json::json!({"sandbox_id": id, "outcome": "unknown"}));
    Error::Provider(error)
}

fn retained(marker: &str, reason: &str, source: Option<Error>) -> Error {
    let message = format!("{reason}; sandbox fence retained for reconciliation");
    let cause = source.as_ref().map(diagnostic);
    let mut error = match source {
        Some(source) => ProviderError::with_source(docker_kind(), message, source),
        None => ProviderError::new(docker_kind(), message),
    };
    error.code = Some("transport".to_owned());
    error.detail =
        Some(serde_json::json!({"fence_container": marker, "outcome": "unknown", "cause": cause}));
    Error::Provider(error)
}
