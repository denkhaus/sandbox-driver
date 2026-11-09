//! The Docker provider: the daemon connection, image readiness, and the
//! verbs that create, attach to, list, and delete sandboxes.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::ListContainersOptions;
use bollard::models::ContainerInspectResponse;
use sandbox_driver::{
    Action, Capabilities, Error, EventContext, EventEmitter, EventSubject, Exec, HealthStatus,
    OperationReporter, Progress, ProgressCode, ProviderError, ProviderHealth, ProviderKind,
    ResourceKind, Result, Sandbox, SandboxFilter, SandboxId, SandboxProvider, SandboxSpec,
    SandboxStatus, TransportError,
};
use tokio::time;

use crate::access::DockerShellCommand;
use crate::config::RegistryAuth;
use crate::container::{ContainerRef, inspect_container, remove_sandbox};
use crate::create::{ContainerPlan, CreatedContainer};
use crate::daemon::{docker_error, docker_kind};
use crate::exec::DockerExec;
use crate::fence::Operation;
use crate::forward::DockerForwards;
use crate::fs::DockerFs;
use crate::image::{image_present, pull_image};
use crate::inspect::{ContainerFacts, is_managed, sidecar_network_of, status_from_inspect};
use crate::one_shot::{self, DockerOneShot};
use crate::pty::DockerPty;
use crate::sandbox::DockerSandbox;
use crate::{MANAGED_LABEL, docker_capabilities, fence, sidecars};

/// The Docker provider, sharing one configured daemon connection.
pub struct DockerProvider {
    kind:         ProviderKind,
    capabilities: Capabilities,
    docker:       Docker,
}

impl DockerProvider {
    /// Connects to the local Docker daemon and verifies it responds.
    #[tracing::instrument(skip_all, fields(provider_kind = "docker"), err)]
    pub async fn connect() -> Result<Self> {
        let provider = Self::connect_unverified()?;
        provider
            .docker
            .ping()
            .await
            .map_err(|error| docker_error("pinging the docker daemon", error))?;
        Ok(provider)
    }

    /// Connects to the local Docker daemon (`DOCKER_HOST` and its TLS
    /// companions honoured) without checking that it answers, so a plugin
    /// can start and report an unreachable daemon through
    /// [`SandboxProvider::health`] instead of failing to launch.
    pub fn connect_unverified() -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()
            .map_err(|error| docker_error("connecting to the docker daemon", error))?;
        Ok(Self::from_client(docker))
    }

    /// Use a caller-configured daemon connection, including an authenticated
    /// custom transport. This does not contact the daemon;
    /// [`SandboxProvider::health`] verifies the connection. The caller owns
    /// endpoint and TLS configuration.
    pub fn from_client(docker: Docker) -> Self {
        Self {
            kind: docker_kind(),
            capabilities: docker_capabilities(),
            docker,
        }
    }

    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn ensure_image(
        &self,
        reference: &str,
        auto_pull: bool,
        auth: Option<&RegistryAuth>,
        platform: Option<&str>,
        reporter: &OperationReporter,
    ) -> Result<()> {
        if image_present(&self.docker, reference).await? {
            return Ok(());
        }
        if !auto_pull {
            return Err(Error::Provider(ProviderError::new(
                self.kind.clone(),
                format!(
                    "image {reference} is not present locally and \
                     provider_config.auto_pull is false"
                ),
            )));
        }
        reporter
            .progress(
                Progress::new(ProgressCode::IMAGE_PULL)
                    .message(format!("pulling image {reference}")),
            )
            .await;
        pull_image(&self.docker, reference, auth, platform).await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, sandbox_id = container_id),
        err
    )]
    async fn inspect(&self, container_id: &str) -> Result<ContainerInspectResponse> {
        inspect_container(&self.docker, container_id).await
    }

    /// Builds a handle from Docker state alone: no exec runs here, so
    /// `attach` works on a stopped container too.
    fn handle(&self, facts: ContainerFacts, events: EventEmitter) -> Arc<DockerSandbox> {
        let ContainerFacts {
            id,
            name,
            working_dir,
            labels,
            env,
            sidecar_network: network,
            workspace,
        } = facts;
        let container = ContainerRef::new(self.docker.clone(), id, working_dir);
        let pty = DockerPty::new(container.clone());
        let one_shot = workspace.map(|workspace| DockerOneShot::new(container.clone(), workspace));
        let shell_command =
            DockerShellCommand::new(container.id.clone(), container.working_dir.clone());
        let exec = Arc::new(DockerExec::new(container.clone(), env));
        let fs = DockerFs::new(container.clone(), Arc::clone(&exec) as Arc<dyn Exec>);
        let forwards = DockerForwards::new(Arc::clone(&exec));
        Arc::new(DockerSandbox {
            id: SandboxId::try_new(container.id.clone())
                .expect("container id is a valid sandbox id"),
            name,
            capabilities: self.capabilities.clone(),
            container,
            labels,
            exec,
            fs,
            pty,
            shell_command,
            one_shot,
            forwards,
            network,
            events,
        })
    }
}

#[async_trait]
impl SandboxProvider for DockerProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn create(
        &self,
        spec: &SandboxSpec,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        // Boxed: the plan carries the whole container request, and a future
        // that held it inline would be large.
        let mut plan = Box::new(ContainerPlan::plan(spec)?);
        let emitter = EventEmitter::new(self.kind.clone(), events);
        let handle_emitter = emitter.clone();
        emitter
            .run(
                EventSubject::pending_sandbox(spec.name.clone()),
                Action::Create,
                |reporter| async move {
                    self.ensure_image(
                        &plan.image,
                        plan.auto_pull,
                        plan.registry_auth.as_ref(),
                        plan.platform.as_deref(),
                        &reporter,
                    )
                    .await?;
                    sidecars::prepare_images(&self.docker, &plan.sidecars).await?;
                    let fence_name = fence::reserve_creation(&mut plan.config.labels);
                    let owner = spec.name.as_deref().unwrap_or(&fence_name);
                    fence::create(&self.docker, &plan.image, &fence_name, owner, async {
                        let created = CreatedContainer::create(&self.docker, &plan).await?;
                        let provisioning = async {
                            let event_id =
                                SandboxId::try_new(created.id.clone()).map_err(|error| {
                                    Error::invalid_spec("sandbox_id", error.to_string())
                                })?;
                            reporter.set_subject(EventSubject::sandbox(Some(event_id)));
                            created.connect_sidecars(&plan.sidecars).await?;
                            created.start().await?;
                            created.verify_running(&self.kind).await
                        };
                        // A timeout here still removes what creation made; one
                        // left to the claim's bound would drop that cleanup.
                        let started =
                            match time::timeout(fence::PROVISIONING_WORK, provisioning).await {
                                Ok(Ok(started)) => started,
                                Ok(Err(error)) => return Err(created.abandon(error).await),
                                Err(_) => {
                                    let error = Error::Transport(TransportError::new(
                                        "sandbox provisioning exceeded its time budget",
                                    ));
                                    return Err(created.abandon(error).await);
                                }
                            };
                        let facts = ContainerFacts::from_inspect(&started, &created.id);
                        Ok(self.handle(facts, handle_emitter) as Arc<dyn Sandbox>)
                    })
                    .await
                },
            )
            .await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, sandbox_id = %id),
        err
    )]
    async fn attach(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> Result<Arc<dyn Sandbox>> {
        let emitter = EventEmitter::new(self.kind.clone(), events);
        let handle_emitter = emitter.clone();
        emitter
            .run(
                EventSubject::sandbox(Some(id.clone())),
                Action::Attach,
                |_| async move {
                    let inspect = self.inspect(id.as_str()).await?;
                    if !is_managed(&inspect) {
                        return Err(Error::NotFound {
                            resource: ResourceKind::Sandbox,
                            id:       id.as_str().to_owned(),
                        });
                    }
                    let facts = ContainerFacts::from_inspect(&inspect, id.as_str());
                    Ok(self.handle(facts, handle_emitter) as Arc<dyn Sandbox>)
                },
            )
            .await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, sandbox_id = %id),
        err
    )]
    async fn delete(&self, id: &SandboxId, events: Option<EventContext>) -> Result<()> {
        let emitter = EventEmitter::new(self.kind.clone(), events);
        emitter
            .run(
                EventSubject::sandbox(Some(id.clone())),
                Action::Delete,
                |_| async move {
                    // Docker state alone decides what to remove: no handle
                    // and no exec, so a stopped or wedged container goes the
                    // same way as a running one.
                    let inspect = match self.inspect(id.as_str()).await {
                        Ok(inspect) => inspect,
                        Err(Error::NotFound { .. }) => {
                            let sweep =
                                async { Ok(one_shot::sweep(&self.docker, id.as_str()).await?) };
                            return fence::run(&self.docker, id.as_str(), Operation::Delete, sweep)
                                .await;
                        }
                        Err(error) => return Err(error),
                    };
                    // A container this provider did not create is unknown to
                    // it, and deleting an unknown id succeeds by doing nothing.
                    if !is_managed(&inspect) {
                        return Ok(());
                    }
                    let network = sidecar_network_of(&inspect);
                    let container_id = inspect.id.clone().unwrap_or_else(|| id.as_str().to_owned());
                    fence::run(&self.docker, &container_id, Operation::Delete, async {
                        Ok(remove_sandbox(&self.docker, &container_id, network.as_deref()).await?)
                    })
                    .await
                },
            )
            .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = %self.kind), err)]
    async fn health(&self) -> Result<ProviderHealth> {
        match self.docker.ping().await {
            Ok(_) => Ok(ProviderHealth::new(HealthStatus::Ok)),
            Err(error) => {
                tracing::warn!("docker provider health check failed");
                let mut health = ProviderHealth::new(HealthStatus::Unreachable);
                health.message = Some(format!("pinging the docker daemon failed: {error}"));
                Ok(health)
            }
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = %self.kind, label_count = filter.labels.len()),
        err
    )]
    async fn list(&self, filter: &SandboxFilter) -> Result<Vec<SandboxStatus>> {
        let mut label_filters = vec![format!("{MANAGED_LABEL}=true")];
        for (key, value) in &filter.labels {
            label_filters.push(format!("{key}={value}"));
        }
        let mut filters = HashMap::new();
        filters.insert("label".to_owned(), label_filters);
        let options = ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        };
        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .map_err(|error| docker_error("listing containers", error))?;
        let mut statuses = Vec::new();
        for container in containers {
            let Some(id) = container.id else { continue };
            // A container deleted between list and inspect is a benign
            // race; any other inspect failure must not silently shrink
            // the listing — a reconciler would treat the missing
            // sandbox as gone.
            let inspect = match self.inspect(&id).await {
                Ok(inspect) => inspect,
                Err(Error::NotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            let sandbox_id = SandboxId::try_new(id)
                .map_err(|error| Error::invalid_spec("id", error.to_string()))?;
            statuses.push(status_from_inspect(sandbox_id, &inspect));
        }
        Ok(statuses)
    }
}
