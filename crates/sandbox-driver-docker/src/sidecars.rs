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
use std::fmt;
use std::time::{Duration, Instant};

use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, LogsOptions, NetworkingConfig,
    RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::models::{
    ContainerStateStatusEnum, EndpointSettings, HealthConfig, HealthStatusEnum, HostConfig,
};
use bollard::network::{
    CreateNetworkOptions, DisconnectNetworkOptions, InspectNetworkOptions, ListNetworksOptions,
};
use futures_util::StreamExt;
use sandbox_driver::{Error, ProviderError, Result};
use tokio::time;

use crate::config::{Sidecar, to_health_config};
use crate::container::{confirm_stopped, remove_container_forced};
use crate::daemon::{
    docker_error, docker_kind, is_not_found, is_not_modified, listed_id, tolerate_not_modified,
};
use crate::image::{image_present, pull_image};
use crate::{MANAGED_LABEL, fence, non_empty};

/// The label every sidecar carries, naming its network.
pub(crate) const NETWORK_LABEL: &str = "sh.sandbox-driver.network";
/// The full ID of the sandbox a sidecar or its network belongs to, so they
/// can be found and removed by that ID after the primary is gone.
pub(crate) const OWNER_LABEL: &str = "sh.sandbox-driver.sandbox";
/// A sidecar's readiness deadline in milliseconds, kept on the container so
/// a restart rechecks readiness with the budget the sandbox was admitted
/// with.
const READY_DEADLINE_LABEL: &str = "sh.sandbox-driver.ready-deadline-ms";
/// The service name a sidecar answers to on its network.
const SERVICE_LABEL: &str = "sh.sandbox-driver.service";
/// The comma-separated TCP ports a service declared reachable.
const PORTS_LABEL: &str = "sh.sandbox-driver.service-ports";
/// The readiness deadline of a sidecar that declares none, from when the
/// sidecars have all been started. Creation's own budget is no longer.
pub(crate) const HEALTH_WAIT: Duration = Duration::from_secs(300);
/// The health check test that turns off an image's own `HEALTHCHECK`.
pub(crate) const NO_HEALTHCHECK: &str = "NONE";
/// How often to poll a sidecar's health.
const HEALTH_POLL: Duration = Duration::from_millis(250);
/// Log lines quoted in a health failure: enough to show why a service
/// died, bounded so a chatty one cannot bloat the error.
const FAILURE_LOG_LINES: usize = 10;
/// Bound on fetching that tail: the failure is what matters, and a slow
/// daemon must not turn it into a hang.
const FAILURE_LOG_TIMEOUT: Duration = Duration::from_secs(5);

/// One sidecar as `create` starts it: the caller's options, the Docker
/// health check that decides readiness, and how long readiness may take.
pub(crate) struct Service {
    pub(crate) sidecar:  Sidecar,
    pub(crate) health:   Option<HealthConfig>,
    pub(crate) deadline: Duration,
    /// Ports the workload reaches the service on, as reported by status.
    pub(crate) ports:    Vec<u16>,
}

// Written by hand: the sidecar's registry credential must never be printed.
impl fmt::Debug for Service {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Service")
            .field("name", &self.sidecar.name)
            .field("image", &self.sidecar.image)
            .field(
                "registry_auth",
                &self.sidecar.registry_auth.as_ref().map(|_| "<redacted>"),
            )
            .field("health", &self.health)
            .field("deadline", &self.deadline)
            .field("ports", &self.ports)
            .finish_non_exhaustive()
    }
}

impl From<Sidecar> for Service {
    fn from(sidecar: Sidecar) -> Self {
        Self {
            health: sidecar.health.as_ref().map(to_health_config),
            sidecar,
            deadline: HEALTH_WAIT,
            ports: Vec::new(),
        }
    }
}

/// The container name of `sidecar` on `network`.
fn container_name(network: &str, sidecar: &Sidecar) -> String {
    format!("{network}-{}", sidecar.name)
}

/// Creates the network and every sidecar on it, then waits for the ones
/// with a health check to report healthy; the ones without are left to
/// run or exit as they will. The creation owner tears down partial resources
/// on failure within its cleanup budget.
pub(crate) async fn realize(
    docker: &Docker,
    network: &str,
    owner: &str,
    services: &[Service],
) -> Result<()> {
    let mut labels = HashMap::new();
    labels.insert(MANAGED_LABEL.to_owned(), "true".to_owned());
    labels.insert(NETWORK_LABEL.to_owned(), network.to_owned());
    labels.insert(OWNER_LABEL.to_owned(), owner.to_owned());
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
    start_all(docker, network, services, &labels).await
}

async fn start_all(
    docker: &Docker,
    network: &str,
    services: &[Service],
    labels: &HashMap<String, String>,
) -> Result<()> {
    for service in services {
        let sidecar = &service.sidecar;
        let container = container_name(network, sidecar);
        let mut labels = labels.clone();
        labels.insert(
            READY_DEADLINE_LABEL.to_owned(),
            service.deadline.as_millis().to_string(),
        );
        labels.insert(SERVICE_LABEL.to_owned(), sidecar.name.clone());
        let ports: Vec<String> = service.ports.iter().map(u16::to_string).collect();
        labels.insert(PORTS_LABEL.to_owned(), ports.join(","));
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
            labels: Some(labels),
            healthcheck: service.health.clone(),
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
    // Deadlines count from when they were all started, so each budget
    // bounds that service's whole wait, not just its turn.
    let started = Instant::now();
    for service in services {
        let watched = service.health.as_ref().is_some_and(|health| {
            health
                .test
                .as_deref()
                .and_then(<[String]>::first)
                .map(String::as_str)
                != Some(NO_HEALTHCHECK)
        });
        if watched {
            let container = container_name(network, &service.sidecar);
            await_health(docker, &container, started + service.deadline).await?;
        }
    }
    Ok(())
}

/// Pulls missing sidecar images. `create` calls this before claiming the
/// sandbox, as it does for the primary image, so a slow pull never spends
/// the bounded provisioning budget held under the claim.
pub(crate) async fn prepare_images(docker: &Docker, services: &[Service]) -> Result<()> {
    for Service { sidecar, .. } in services {
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
    let started = Instant::now();
    for container in containers {
        let deadline = container
            .labels
            .as_ref()
            .and_then(|labels| labels.get(READY_DEADLINE_LABEL))
            .and_then(|millis| millis.parse().ok())
            .map_or(HEALTH_WAIT, Duration::from_millis);
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
            await_health(docker, &id, started + deadline).await?;
        }
    }
    Ok(())
}

/// Removes the sidecars and networks labelled as `owner`'s. Delete calls
/// this when the primary is already gone, so services it left behind are
/// not orphaned; sidecars go before their network.
pub(crate) async fn sweep_owned(docker: &Docker, owner: &str) -> Result<()> {
    let filter = HashMap::from([("label".to_owned(), vec![format!("{OWNER_LABEL}={owner}")])]);
    let containers = docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: filter.clone(),
            ..Default::default()
        }))
        .await
        .map_err(|error| docker_error("listing orphaned sidecars", error))?;
    for container in containers {
        let id = listed_id(container.id, "sidecar")?;
        remove_container_forced(docker, &id, "removing orphaned sidecar").await?;
    }
    let networks = docker
        .list_networks(Some(ListNetworksOptions { filters: filter }))
        .await
        .map_err(|error| docker_error("listing orphaned sidecar networks", error))?;
    for network in networks {
        let id = listed_id(network.id, "network")?;
        match docker.remove_network(&id).await {
            Ok(()) => {}
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(docker_error("removing orphaned sidecar network", error)),
        }
    }
    Ok(())
}

/// The services on `network` that are running now, by the name the workload
/// reaches them at, with their declared ports. Stopped services are omitted:
/// their address is not a current observation.
pub(crate) async fn running(docker: &Docker, network: &str) -> Result<Vec<(String, Vec<u16>)>> {
    let containers = docker
        .list_containers(Some(network_filter(network)))
        .await
        .map_err(|error| docker_error("listing sidecars", error))?;
    Ok(containers
        .into_iter()
        .filter(|container| container.state.as_deref() == Some("running"))
        .filter_map(|container| {
            let labels = container.labels?;
            let name = labels.get(SERVICE_LABEL)?.clone();
            let ports = labels
                .get(PORTS_LABEL)
                .map(|ports| {
                    ports
                        .split(',')
                        .filter_map(|port| port.parse().ok())
                        .collect()
                })
                .unwrap_or_default();
            Some((name, ports))
        })
        .collect())
}
