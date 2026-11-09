//! Sidecar service containers for a Docker sandbox.
//!
//! A sandbox that declares `provider_config.sidecars` gets one user-defined
//! network and one container per sidecar on it, reachable by `name` as a
//! network alias. The main container joins the same network, so a workload
//! reaches every sidecar by name. Sidecars carry a label naming the
//! network, so [`sweep`] and [`set_running`] find them again from a handle
//! rebuilt by `attach`, and `create` tears down what it started when any
//! sidecar fails.
//!
//! # Readiness
//!
//! A sidecar's `health` check is the caller's statement that the service
//! must come up and stay up. `create` waits for every sidecar that has one
//! to report healthy, and fails — naming the sidecar and quoting the tail
//! of its log — if it turns unhealthy or exits first. A sidecar with no
//! health check is started and not watched: it may run for the life of
//! the sandbox or exit at once, and either is fine. That is the shape of a
//! one-shot job such as a database migration, and it matches GitHub
//! Actions, where a service container without a health check is started
//! and never waited on. A service that must be reachable needs a check.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, LogsOptions, NetworkingConfig,
    RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::models::{ContainerStateStatusEnum, EndpointSettings, HealthStatusEnum, HostConfig};
use bollard::network::{CreateNetworkOptions, DisconnectNetworkOptions, InspectNetworkOptions};
use futures_util::StreamExt;
use sandbox_driver::{Error, ProviderError, Result};
use tokio::time;

use crate::config::{Sidecar, to_health_config};
use crate::container::confirm_stopped;
use crate::daemon::{
    docker_error, docker_kind, is_not_found, is_not_modified, listed_id, tolerate_not_modified,
};
use crate::image::{image_present, pull_image};
use crate::{MANAGED_LABEL, fence, non_empty};

/// The label every sidecar carries, naming its network.
pub(crate) const NETWORK_LABEL: &str = "sh.sandbox-driver.network";
/// How long to wait for every sidecar to report healthy, from when they
/// have all been started.
const HEALTH_WAIT: Duration = Duration::from_secs(300);
/// How often to poll a sidecar's health.
const HEALTH_POLL: Duration = Duration::from_millis(250);
/// Log lines quoted in a health failure: enough to show why a service
/// died, bounded so a chatty one cannot bloat the error.
const FAILURE_LOG_LINES: usize = 10;
/// Bound on fetching that tail: the failure is what matters, and a slow
/// daemon must not turn it into a hang.
const FAILURE_LOG_TIMEOUT: Duration = Duration::from_secs(5);

/// The container name of `sidecar` on `network`.
fn container_name(network: &str, sidecar: &Sidecar) -> String {
    format!("{network}-{}", sidecar.name)
}

/// Creates the network and every sidecar on it, then waits for the ones
/// with a health check to report healthy; the ones without are left to
/// run or exit as they will. The creation owner tears down partial resources
/// on failure within its cleanup budget.
pub(crate) async fn realize(docker: &Docker, network: &str, sidecars: &[Sidecar]) -> Result<()> {
    let mut labels = HashMap::new();
    labels.insert(MANAGED_LABEL.to_owned(), "true".to_owned());
    labels.insert(NETWORK_LABEL.to_owned(), network.to_owned());
    docker
        .create_network(CreateNetworkOptions {
            name: network.to_owned(),
            labels: labels.clone(),
            ..Default::default()
        })
        .await
        .map_err(|error| docker_error("creating sidecar network", error))?;

    // The creation owner performs bounded cleanup while retaining the
    // primary container as the discovery anchor if cleanup is incomplete.
    start_all(docker, network, sidecars, &labels).await
}

async fn start_all(
    docker: &Docker,
    network: &str,
    sidecars: &[Sidecar],
    labels: &HashMap<String, String>,
) -> Result<()> {
    for sidecar in sidecars {
        let container = container_name(network, sidecar);
        let env: Vec<String> = sidecar
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        let endpoint = EndpointSettings {
            aliases: Some(vec![sidecar.name.clone()]),
            ..Default::default()
        };
        let mut endpoints = HashMap::new();
        endpoints.insert(network.to_owned(), endpoint);
        let host_config = HostConfig {
            network_mode: Some(network.to_owned()),
            dns: non_empty(sidecar.dns.clone()),
            cap_add: non_empty(sidecar.cap_add.clone()),
            privileged: sidecar.privileged.then_some(true),
            ..Default::default()
        };
        let config = Config {
            image: Some(sidecar.image.clone()),
            env: non_empty(env),
            user: sidecar.user.clone(),
            entrypoint: sidecar.entrypoint.clone(),
            labels: Some(labels.clone()),
            healthcheck: sidecar.health.as_ref().map(to_health_config),
            host_config: Some(host_config),
            networking_config: Some(NetworkingConfig {
                endpoints_config: endpoints,
            }),
            ..Default::default()
        };
        docker
            .create_container(
                Some(CreateContainerOptions {
                    name:     container.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .map_err(|error| docker_error("creating sidecar", error))?;
        docker
            .start_container(&container, None::<StartContainerOptions<String>>)
            .await
            .map_err(|error| docker_error("starting sidecar", error))?;
    }
    // One deadline for all of them: they were started together, so the
    // readiness budget bounds the whole wait, not each service's turn.
    let deadline = Instant::now() + HEALTH_WAIT;
    for sidecar in sidecars {
        if sidecar.health.is_some() {
            await_health(docker, &container_name(network, sidecar), deadline).await?;
        }
    }
    Ok(())
}

/// Pulls missing sidecar images. `create` calls this before claiming the
/// sandbox, as it does for the primary image, so a slow pull never spends
/// the bounded provisioning budget held under the claim.
pub(crate) async fn prepare_images(docker: &Docker, sidecars: &[Sidecar]) -> Result<()> {
    for sidecar in sidecars {
        if !image_present(docker, &sidecar.image).await? {
            pull_image(docker, &sidecar.image, sidecar.registry_auth.as_ref(), None).await?;
        }
    }
    Ok(())
}

async fn await_health(docker: &Docker, container: &str, deadline: Instant) -> Result<()> {
    loop {
        let inspect = docker
            .inspect_container(container, None)
            .await
            .map_err(|error| docker_error("inspecting sidecar health", error))?;
        let state = inspect.state.as_ref();
        let status = state.and_then(|s| s.status);
        let health = state.and_then(|s| s.health.as_ref()).and_then(|h| h.status);
        let failure = match (status, health) {
            (_, Some(HealthStatusEnum::HEALTHY)) => return Ok(()),
            (_, Some(HealthStatusEnum::UNHEALTHY)) => Some("reported unhealthy"),
            (Some(ContainerStateStatusEnum::EXITED | ContainerStateStatusEnum::DEAD), _) => {
                Some("exited before it was healthy")
            }
            _ if Instant::now() >= deadline => Some("never reported healthy"),
            _ => None,
        };
        if let Some(what) = failure {
            return Err(health_failure(docker, container, what).await);
        }
        time::sleep(HEALTH_POLL).await;
    }
}

/// The error for a sidecar that failed its health wait: what happened,
/// then the tail of its log, which is usually the only clue to why.
async fn health_failure(docker: &Docker, container: &str, what: &str) -> Error {
    let mut message = format!("sidecar {container} {what}");
    match log_tail(docker, container).await {
        Some(tail) if !tail.is_empty() => {
            message.push_str("; last log lines:\n");
            message.push_str(&tail);
        }
        Some(_) => message.push_str("; the log is empty"),
        None => message.push_str("; the log could not be read"),
    }
    Error::Provider(ProviderError::new(docker_kind(), message))
}

/// The last [`FAILURE_LOG_LINES`] lines of a container's log, both
/// streams, or `None` when the daemon could not supply them in time.
async fn log_tail(docker: &Docker, container: &str) -> Option<String> {
    let options = LogsOptions {
        stdout: true,
        stderr: true,
        tail: FAILURE_LOG_LINES.to_string(),
        ..Default::default()
    };
    let read = async {
        let mut stream = docker.logs(container, Some(options));
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.ok()?.into_bytes());
        }
        Some(bytes)
    };
    let bytes = time::timeout(FAILURE_LOG_TIMEOUT, read).await.ok()??;
    Some(String::from_utf8_lossy(&bytes).trim_end().to_owned())
}

fn network_filter(network: &str) -> ListContainersOptions<String> {
    let mut filters = HashMap::new();
    filters.insert("label".to_owned(), vec![format!(
        "{NETWORK_LABEL}={network}"
    )]);
    ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    }
}

/// Removes sidecars, disconnects the sandbox when present, then removes
/// the network. Keep the sandbox until this succeeds: its network mode
/// supplies the cleanup identity if any operation must be retried.
pub(crate) async fn sweep(docker: &Docker, network: &str, sandbox: Option<&str>) -> Result<()> {
    let containers = docker
        .list_containers(Some(network_filter(network)))
        .await
        .map_err(|error| docker_error("listing sidecars", error))?;
    for container in containers {
        let id = listed_id(container.id, "sidecar")?;

        match docker
            .remove_container(
                &id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(()) => {}
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(docker_error("removing sidecar", error)),
        }
    }
    if let Some(sandbox) = sandbox {
        let inspected = match docker
            .inspect_network(network, None::<InspectNetworkOptions<String>>)
            .await
        {
            Ok(inspected) => inspected,
            Err(error) if is_not_found(&error) => return Ok(()),
            Err(error) => return Err(docker_error("inspecting sidecar network", error)),
        };
        if inspected
            .containers
            .as_ref()
            .is_some_and(|containers| containers.contains_key(sandbox))
        {
            match docker
                .disconnect_network(network, DisconnectNetworkOptions {
                    container: sandbox,
                    force:     true,
                })
                .await
            {
                Ok(()) => {}
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(docker_error("disconnecting sandbox network", error)),
            }
        }
    }
    match docker.remove_network(network).await {
        Ok(()) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(docker_error("removing sidecar network", error)),
    }
}

/// Stops or starts every sidecar on the network, alongside the main
/// container's own stop/start.
pub(crate) async fn set_running(
    docker: &Docker,
    network: &str,
    running: bool,
) -> fence::WorkResult<()> {
    let containers = docker
        .list_containers(Some(network_filter(network)))
        .await
        .map_err(|error| docker_error("listing sidecars", error))?;
    for container in &containers {
        let id = listed_id(container.id.clone(), "sidecar")?;
        if running {
            match docker
                .start_container(&id, None::<StartContainerOptions<String>>)
                .await
            {
                Ok(()) => {}
                Err(error) if is_not_modified(&error) => {}
                Err(error) => return Err(docker_error("starting sidecar", error).into()),
            }
        } else {
            tolerate_not_modified(
                docker
                    .stop_container(&id, Some(StopContainerOptions { t: 1 }))
                    .await,
                "stopping sidecar",
            )?;
        }
    }
    // Stop every service before confirming any, so one unconfirmed service
    // does not leave the rest running.
    if !running {
        for container in &containers {
            if let Some(id) = &container.id {
                confirm_stopped(docker, id).await?;
            }
        }
        return Ok(());
    }
    // Start every service before waiting: a health check may depend on
    // another service on the same network.
    let deadline = Instant::now() + HEALTH_WAIT;
    for container in containers {
        let id = listed_id(container.id, "sidecar")?;
        let inspect = docker
            .inspect_container(&id, None)
            .await
            .map_err(|error| docker_error("inspecting sidecar health", error))?;
        if inspect
            .state
            .as_ref()
            .and_then(|state| state.health.as_ref())
            .is_some()
        {
            await_health(docker, &id, deadline).await?;
        }
    }
    Ok(())
}
