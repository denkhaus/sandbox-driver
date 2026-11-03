//! The container-backed sandbox handle.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bollard::container::{StartContainerOptions, StopContainerOptions};
use bollard::models::ContainerInspectResponse;
use sandbox_driver::{
    Action, Capabilities, Error, EventEmitter, EventSubject, Exec, ExecSpec, Filesystem, OneShot,
    PlatformInfo, PreviewUrls, ProviderError, Pty, ResourceKind, Result, Sandbox, SandboxId,
    SandboxKind, SandboxState, SandboxStatus, ShellCommand,
};

use crate::access::DockerShellCommand;
use crate::container::{ContainerRef, confirm_stopped, remove_sandbox};
use crate::daemon::{
    docker_error, docker_kind, is_not_found, is_not_modified, tolerate_not_modified,
};
use crate::exec::DockerExec;
use crate::fence::Operation;
use crate::forward::DockerForwards;
use crate::fs::DockerFs;
use crate::inspect::{configured_env, status_from_inspect};
use crate::one_shot::{self, DockerOneShot};
use crate::pty::DockerPty;
use crate::{RUNTIME_DIRECTORY, SIDECAR_NETWORK_LABEL, fence, sidecars};

/// A container-backed sandbox.
pub struct DockerSandbox {
    pub(crate) id:            SandboxId,
    pub(crate) name:          Option<String>,
    pub(crate) capabilities:  Capabilities,
    pub(crate) container:     ContainerRef,
    pub(crate) labels:        BTreeMap<String, String>,
    pub(crate) exec:          Arc<DockerExec>,
    pub(crate) fs:            DockerFs,
    pub(crate) pty:           DockerPty,
    pub(crate) shell_command: DockerShellCommand,
    /// One-shot containers over the sandbox's workspace, when the
    /// container's mounts told where that workspace is.
    pub(crate) one_shot:      Option<DockerOneShot>,
    /// Port forwards into the container, closed with it.
    pub(crate) forwards:      DockerForwards,
    /// The sidecar network to sweep on delete, when the sandbox has one.
    pub(crate) network:       Option<String>,
    pub(crate) events:        EventEmitter,
}

impl DockerSandbox {
    /// Inspects the sandbox's container; a missing one is
    /// [`Error::NotFound`].
    async fn inspect(&self) -> Result<ContainerInspectResponse> {
        self.container.inspect().await
    }

    /// Reports a lifecycle action and runs it under the sandbox fence.
    fn lifecycle<'a>(
        &'a self,
        action: Action,
        operation: Operation,
        work: impl Future<Output = fence::WorkResult<()>> + Send + 'a,
    ) -> impl Future<Output = Result<()>> + 'a {
        // Boxing `work` here keeps it out of every caller's future.
        let fenced = fence::run(&self.container.docker, self.id.as_str(), operation, work);
        self.events
            .run(EventSubject::sandbox(Some(self.id.clone())), action, |_| {
                fenced
            })
    }
}

#[async_trait]
impl Sandbox for DockerSandbox {
    fn id(&self) -> &SandboxId {
        &self.id
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn describe(&self) -> Result<SandboxStatus> {
        match self.inspect().await {
            Ok(inspect) => {
                let mut status = status_from_inspect(self.id.clone(), &inspect);
                if status.labels.is_empty() {
                    status.labels = self.labels.clone();
                }
                Ok(status)
            }
            Err(Error::NotFound { .. }) => {
                let mut status = SandboxStatus::new(self.id.clone(), SandboxState::Deleted);
                status.name.clone_from(&self.name);
                status.sandbox_kind = Some(SandboxKind::Container);
                Ok(status)
            }
            Err(error) => Err(error),
        }
    }

    fn working_directory(&self) -> &str {
        &self.container.working_dir
    }

    fn runtime_directory(&self) -> Option<&str> {
        Some(RUNTIME_DIRECTORY)
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn environment(&self) -> Result<BTreeMap<String, String>> {
        Ok(configured_env(&self.inspect().await?))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn platform_info(&self) -> Result<PlatformInfo> {
        // uname prints its fields in canonical order — sysname, release,
        // machine — regardless of flag order.
        let result = self
            .exec
            .run(
                &ExecSpec::new("uname")
                    .args(["-s", "-r", "-m"])
                    .timeout(Duration::from_secs(30)),
            )
            .await?;
        let text = result.stdout_lossy();
        let mut parts = text.split_whitespace();
        let os = parts.next().unwrap_or("linux").to_lowercase();
        let version = parts.next().unwrap_or("").to_owned();
        let arch = parts.next().unwrap_or("").to_owned();
        Ok(PlatformInfo::new(os, arch, version))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn start(&self) -> Result<()> {
        self.lifecycle(Action::Start, Operation::Start, async {
            let inspect = self.inspect().await?;
            if let Some(network) = &self.network {
                // A crash during create leaves the primary labeled
                // and stopped, but not connected until every service
                // is ready. Do not expose that incomplete allocation
                // as a working sandbox on recovery.
                let pending_connection = inspect.config.as_ref()
                    .and_then(|config| config.labels.as_ref())
                    .is_some_and(|labels| labels.contains_key(SIDECAR_NETWORK_LABEL))
                    && !inspect.network_settings.as_ref()
                        .and_then(|settings| settings.networks.as_ref())
                        .is_some_and(|networks| networks.contains_key(network));
                if pending_connection {
                    return Err(Error::Provider(ProviderError::new(
                        docker_kind(),
                        "sandbox creation did not finish connecting its services; delete the incomplete sandbox",
                    )).into());
                }
                sidecars::set_running(&self.container.docker, network, true).await?;
            }
            if inspect.state.as_ref().and_then(|state| state.paused) == Some(true) {
                return self.container.docker
                    .unpause_container(self.id.as_str())
                    .await
                    .map_err(|error| docker_error("unpausing container", error).into());
            }
            // A daemon or VM crash may have ended the sandbox
            // without running stop's one-shot cleanup. Sweep those
            // resources before restarting it. Starting an already
            // running sandbox must preserve its active one-shots.
            if inspect.state.as_ref().and_then(|state| state.running) != Some(true) {
                one_shot::sweep(&self.container.docker, self.id.as_str()).await?;
            }
            // Already-running (304) is success; a vanished container is
            // not — start's postcondition is a running sandbox, so 404
            // must surface, unlike stop/delete where gone is the goal.
            match self.container.docker
                .start_container(self.id.as_str(), None::<StartContainerOptions<String>>)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) if is_not_modified(&error) => Ok(()),
                Err(error) if is_not_found(&error) => Err(Error::NotFound {
                    resource: ResourceKind::Sandbox,
                    id:       self.id.as_str().to_owned(),
                }.into()),
                Err(error) => Err(docker_error("starting container", error).into()),
            }
        })
        .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn stop(&self) -> Result<()> {
        // Managed containers run the driver's own `sleep infinity` init
        // as PID1, which never installs a SIGTERM handler and (as PID1)
        // never receives the default disposition — so any stop grace is
        // waited out in full, buying nothing. Workload processes get
        // their SIGTERM-grace-SIGKILL sequence from the exec watcher,
        // not from `docker stop`. Keep the grace at fabro's 1 second.
        self.lifecycle(Action::Stop, Operation::Stop, async {
            self.forwards.close_all().await;
            // Whatever a one-shot was doing in the workspace ends
            // with the sandbox's own processes.
            one_shot::sweep(&self.container.docker, self.id.as_str()).await?;
            tolerate_not_modified(
                self.container
                    .docker
                    .stop_container(self.id.as_str(), Some(StopContainerOptions { t: 1 }))
                    .await,
                "stopping container",
            )?;
            // Stop the services even when the primary cannot be confirmed:
            // an early return would leave every one of them running.
            let services = match &self.network {
                Some(network) => {
                    sidecars::set_running(&self.container.docker, network, false).await
                }
                None => Ok(()),
            };
            let primary = confirm_stopped(&self.container.docker, self.id.as_str()).await;
            fence::combine(primary, services)
        })
        .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn delete(&self) -> Result<()> {
        self.lifecycle(Action::Delete, Operation::Delete, async {
            self.forwards.close_all().await;
            remove_sandbox(
                &self.container.docker,
                self.id.as_str(),
                self.network.as_deref(),
            )
            .await
            .map_err(Into::into)
        })
        .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn pause(&self) -> Result<()> {
        self.lifecycle(Action::Pause, Operation::Pause, async {
            self.container
                .docker
                .pause_container(self.id.as_str())
                .await
                .map_err(|error| docker_error("pausing container", error).into())
        })
        .await
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker", sandbox_id = %self.id), err)]
    async fn resume(&self) -> Result<()> {
        self.lifecycle(Action::Resume, Operation::Resume, async {
            self.container
                .docker
                .unpause_container(self.id.as_str())
                .await
                .map_err(|error| docker_error("unpausing container", error).into())
        })
        .await
    }

    fn exec(&self) -> &dyn Exec {
        self.exec.as_ref()
    }

    fn fs(&self) -> &dyn Filesystem {
        &self.fs
    }

    fn pty(&self) -> Option<&dyn Pty> {
        Some(&self.pty)
    }

    fn one_shot(&self) -> Option<&dyn OneShot> {
        self.one_shot
            .as_ref()
            .map(|one_shot| one_shot as &dyn OneShot)
    }

    fn shell_command(&self) -> Option<&dyn ShellCommand> {
        Some(&self.shell_command)
    }

    fn preview_urls(&self) -> Option<&dyn PreviewUrls> {
        Some(&self.forwards)
    }
}
