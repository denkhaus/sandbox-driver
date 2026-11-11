//! One-shot containers beside a Docker sandbox.
//!
//! A one-shot container is created from its own image, mounts the
//! sandbox's workspace volume at the sandbox's working directory, joins
//! the sandbox container's network namespace (so it reaches sidecars by
//! alias and the daemon host by the same names), runs one command, and is
//! removed. It carries [`ONE_SHOT_LABEL`] naming its sandbox, which is how
//! the sandbox's `stop` and `delete` find and end any left running by a
//! caller that went away.
//!
//! Output streams through an attach, stops are signals to the container
//! (`term` sends SIGTERM once, `kill` and the timeout send SIGKILL), and
//! the exit code is the container's own.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::future;
use std::pin::Pin;
use std::result::Result as StdResult;
use std::time::Instant;

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::{
    AttachContainerOptions, Config, CreateContainerOptions, DownloadFromContainerOptions,
    KillContainerOptions, ListContainersOptions, LogOutput, StartContainerOptions,
    WaitContainerOptions,
};
use bollard::errors::Error as DockerApiError;
use bollard::image::BuildImageOptions;
use bollard::models::{HostConfig, Mount};
use futures_util::{Stream, StreamExt};
use sandbox_driver::{
    Error, ExecControls, ExecStreamingResult, OneShot, OneShotImage, OneShotSpec, ProviderError,
    Result, StopLevel, Termination, stop_signal,
};
use tokio::time;

use crate::build_diagnostics::{BuildOutputTail, render_api_failure, with_output_tail};
use crate::container::{ContainerRef, remove_container_forced};
use crate::daemon::{docker_error, docker_kind, is_conflict, is_not_found};
use crate::fs::tar::reroot;
use crate::image::{image_present, pull_image};
use crate::output::{KILL_DRAIN_GRACE, StreamOutput, drain_with_stops};
use crate::{MANAGED_LABEL, non_empty};

/// The label every one-shot container carries, naming the sandbox it
/// belongs to (the sandbox container's id).
pub(crate) const ONE_SHOT_LABEL: &str = "sh.sandbox-driver.one-shot";
/// The one fallback platform: CI images target linux/amd64, so an image
/// with no manifest for the daemon's architecture is pulled as amd64.
const FALLBACK_PLATFORM: &str = "linux/amd64";

/// The one-shot facet of one Docker sandbox.
pub(crate) struct DockerOneShot {
    container: ContainerRef,
    /// The shared workspace mount, including the sandbox's access mode.
    workspace: Mount,
}

impl DockerOneShot {
    pub(crate) fn new(container: ContainerRef, workspace: Mount) -> Self {
        Self {
            container,
            workspace,
        }
    }

    /// Pulls a registry image, retrying once for the fallback platform
    /// when the daemon's own has no manifest.
    async fn ensure_registry_image(&self, reference: &str) -> Result<()> {
        if image_present(&self.container.docker, reference).await? {
            return Ok(());
        }
        let Err(error) = pull_image(&self.container.docker, reference, None, None).await else {
            return Ok(());
        };
        if !is_missing_platform(&error) {
            return Err(error);
        }
        pull_image(
            &self.container.docker,
            reference,
            None,
            Some(FALLBACK_PLATFORM),
        )
        .await
        .map_err(|_| error)?;
        tracing::warn!(
            provider_kind = "docker",
            platform = FALLBACK_PLATFORM,
            "one-shot image pulled for a fallback platform"
        );
        Ok(())
    }

    /// Reads the build context out of the sandbox through the archive
    /// API, re-rooted so its contents sit at the top level.
    async fn download_context(&self, context: &str) -> Result<Vec<u8>> {
        let options = DownloadFromContainerOptions {
            path: self.container.resolve(context),
        };
        let mut stream = self
            .container
            .docker
            .download_from_container(&self.container.id, Some(options));
        let mut archive = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| docker_error("reading build context", error))?;
            archive.extend_from_slice(&chunk);
        }
        reroot(&archive)
    }

    /// Builds an image from a Dockerfile inside the sandbox's workspace.
    async fn build_image(
        &self,
        context: &str,
        dockerfile: Option<&str>,
        tag: &str,
        reuse: bool,
    ) -> Result<()> {
        if reuse && image_present(&self.container.docker, tag).await? {
            return Ok(());
        }
        let context_tar = self.download_context(context).await?;
        let options = BuildImageOptions {
            dockerfile: dockerfile.unwrap_or("Dockerfile").to_owned(),
            t: tag.to_owned(),
            rm: true,
            ..Default::default()
        };
        let mut build = self
            .container
            .docker
            .build_image(options, None, Some(context_tar.into()));
        // Fork feature (work#1253): the daemon's streamed output and the
        // cause chain of a stream-level failure must survive into the
        // error message — a flattened `docker error: building image`
        // names nothing. The chain is interpolated into the message AND
        // kept as the error's source: `ProviderError`'s `Display`
        // renders no chain, so the interpolation is the only render a
        // JSON-RPC consumer sees (a deliberate deviation from the
        // no-flattening guideline, documented here at the bend).
        let mut tail = BuildOutputTail::new();
        while let Some(info) = build.next().await {
            let info = info.map_err(|error| {
                let chain = render_api_failure(&error);
                docker_error(
                    &with_output_tail(format!("building image: {chain}"), &tail),
                    error,
                )
            })?;
            if let Some(line) = info.stream {
                tail.record(line);
            }
            if let Some(message) = info.error {
                let mut provider = ProviderError::new(
                    docker_kind(),
                    with_output_tail(format!("image build failed: {message}"), &tail),
                );
                provider.code = Some("build".to_owned());
                return Err(Error::Provider(provider));
            }
        }
        Ok(())
    }

    async fn prepare_image(&self, image: &OneShotImage) -> Result<String> {
        match image {
            OneShotImage::Registry { reference } => {
                self.ensure_registry_image(reference).await?;
                Ok(reference.clone())
            }
            OneShotImage::Build {
                context,
                dockerfile,
                tag,
                reuse,
            } => {
                self.build_image(context, dockerfile.as_deref(), tag, *reuse)
                    .await?;
                Ok(tag.clone())
            }
            _ => Err(Error::invalid_spec("image", "unsupported one-shot image")),
        }
    }

    /// Readies the image under the operation's own stop controls and
    /// deadline: an already cancelled run pulls nothing and creates
    /// nothing.
    async fn prepare_image_or_stop(
        &self,
        spec: &OneShotSpec,
        controls: &ExecControls,
        started: Instant,
    ) -> Result<StdResult<String, Termination>> {
        let deadline = async {
            match spec.timeout {
                Some(timeout) if started.elapsed() < timeout => {
                    time::sleep(timeout.saturating_sub(started.elapsed())).await;
                }
                Some(_) => {}
                None => future::pending().await,
            }
        };
        Ok(tokio::select! {
            biased;
            () = stop_signal(controls.kill.as_ref()) => Err(Termination::Killed),
            () = stop_signal(controls.term.as_ref()) => Err(Termination::Cancelled),
            () = deadline => Err(Termination::TimedOut),
            image = self.prepare_image(&spec.image) => Ok(image?),
        })
    }

    /// The one-shot container for `spec`: `image`, the sandbox's
    /// workspace at its working directory, and the sandbox container's
    /// network namespace, labeled as this sandbox's.
    fn container_config(&self, spec: &OneShotSpec, image: String) -> Config<String> {
        let mut labels = HashMap::new();
        labels.insert(MANAGED_LABEL.to_owned(), "true".to_owned());
        labels.insert(ONE_SHOT_LABEL.to_owned(), self.container.id.clone());
        let host_config = HostConfig {
            network_mode: Some(format!("container:{}", self.container.id)),
            mounts: Some(vec![self.workspace.clone()]),
            init: Some(true),
            ..Default::default()
        };
        let env: Vec<String> = spec
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        Config {
            image: Some(image),
            entrypoint: spec.entrypoint.clone().map(|entrypoint| vec![entrypoint]),
            cmd: non_empty(spec.args.clone()),
            env: non_empty(env),
            working_dir: Some(
                spec.working_dir
                    .clone()
                    .unwrap_or_else(|| self.container.working_dir.clone()),
            ),
            labels: Some(labels),
            host_config: Some(host_config),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        }
    }
}

/// One-shot output as the daemon streams it.
type ContainerOutput = Pin<Box<dyn Stream<Item = StdResult<LogOutput, DockerApiError>> + Send>>;

/// A one-shot container from creation to removal. Every failure between
/// the two removes it, so a run never leaves a container behind.
struct OneShotContainer {
    docker: Docker,
    id:     String,
}

impl OneShotContainer {
    async fn create(docker: &Docker, config: Config<String>) -> Result<Self> {
        let created = docker
            .create_container(None::<CreateContainerOptions<String>>, config)
            .await
            .map_err(|error| docker_error("creating one-shot container", error))?;
        Ok(Self {
            docker: docker.clone(),
            id:     created.id,
        })
    }

    /// Attaches to both output streams, then starts the container, so no
    /// output is missed. A failure at either step removes the container.
    async fn attach_and_start(&self) -> Result<ContainerOutput> {
        let attached = match self
            .docker
            .attach_container(
                &self.id,
                Some(AttachContainerOptions::<String> {
                    stdout: Some(true),
                    stderr: Some(true),
                    stream: Some(true),
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(attached) => attached,
            Err(error) => {
                self.remove_now().await;
                return Err(docker_error("attaching one-shot container", error));
            }
        };
        if let Err(error) = self
            .docker
            .start_container(&self.id, None::<StartContainerOptions<String>>)
            .await
        {
            self.remove_now().await;
            return Err(docker_error("starting one-shot container", error));
        }
        Ok(attached.output)
    }

    /// Asks the container to stop: a term is SIGTERM, a kill SIGKILL.
    /// Never fails: a container already gone or stopped has nothing to
    /// do, and any other failure is logged, since the removal at the end
    /// of the run ends the container regardless.
    async fn stop(&self, level: StopLevel) -> Result<()> {
        let signal = match level {
            StopLevel::Term => "SIGTERM",
            StopLevel::Kill => "SIGKILL",
        };
        let outcome = self
            .docker
            .kill_container(&self.id, Some(KillContainerOptions { signal }))
            .await;
        match outcome {
            Ok(()) => {}
            // Already gone or already stopped: the stop has nothing to do.
            Err(error) if is_not_found(&error) || is_conflict(&error) => {}
            Err(error) => {
                tracing::warn!(
                    provider_kind = "docker",
                    error = %docker_error("signalling one-shot container", error),
                    "one-shot stop request failed"
                );
            }
        }
        Ok(())
    }

    /// The exit code, from the daemon's own wait; a container the drain
    /// deadline abandoned may still be running, and the removal below
    /// ends it.
    async fn exit_code(&self) -> Option<i32> {
        let mut wait = self.docker.wait_container(
            &self.id,
            Some(WaitContainerOptions {
                condition: "not-running",
            }),
        );
        // Bollard reports a non-zero status as an error variant that
        // still carries the code; both shapes are the container's exit.
        match time::timeout(KILL_DRAIN_GRACE, wait.next()).await {
            Ok(Some(Ok(response))) => i32::try_from(response.status_code).ok(),
            Ok(Some(Err(DockerApiError::DockerContainerWaitError { code, .. }))) => {
                i32::try_from(code).ok()
            }
            _ => None,
        }
    }

    /// Removes the container, running or not; the run is over.
    async fn remove(self) {
        self.remove_now().await;
    }

    async fn remove_now(&self) {
        let _ =
            remove_container_forced(&self.docker, &self.id, "removing one-shot container").await;
    }
}

/// Whether `error`, or any error in its source chain, is a registry's
/// refusal to serve an image for the requested platform ("no matching
/// manifest"). Callers retry the pull with an explicit platform.
#[must_use]
pub fn is_missing_platform(error: &Error) -> bool {
    let mut source: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(current) = source {
        if current.to_string().contains("no matching manifest") {
            return true;
        }
        source = current.source();
    }
    false
}

fn one_shot_filter(sandbox_id: &str) -> ListContainersOptions<String> {
    let mut filters = HashMap::new();
    filters.insert("label".to_owned(), vec![format!(
        "{ONE_SHOT_LABEL}={sandbox_id}"
    )]);
    ListContainersOptions {
        all: true,
        filters,
        ..Default::default()
    }
}

/// Removes every one-shot container of `sandbox_id`, running or not.
/// A missing container is fine; other failures must keep lifecycle cleanup
/// pending so the owner can retry it.
pub(crate) async fn sweep(docker: &Docker, sandbox_id: &str) -> Result<()> {
    let containers = docker
        .list_containers(Some(one_shot_filter(sandbox_id)))
        .await
        .map_err(|error| docker_error("listing one-shot containers", error))?;
    for container in containers {
        if let Some(id) = container.id {
            remove_container_forced(docker, &id, "removing one-shot container").await?;
        }
    }
    Ok(())
}

#[async_trait]
impl OneShot for DockerOneShot {
    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id),
        err
    )]
    async fn run(&self, spec: &OneShotSpec, controls: ExecControls) -> Result<ExecStreamingResult> {
        let started = Instant::now();
        let image = match self.prepare_image_or_stop(spec, &controls, started).await? {
            Ok(image) => image,
            Err(termination) => {
                return Ok(StreamOutput::new(
                    spec.output_sanitization,
                    controls.retained_output_limit,
                )
                .into_result(termination, None, started.elapsed(), false));
            }
        };
        let container =
            OneShotContainer::create(&self.container.docker, self.container_config(spec, image))
                .await?;
        let output = container.attach_and_start().await?;
        let mut captured =
            StreamOutput::new(spec.output_sanitization, controls.retained_output_limit);
        let outcome = match drain_with_stops(
            &mut captured,
            output,
            &controls,
            spec.timeout
                .map(|timeout| timeout.saturating_sub(started.elapsed())),
            |level| container.stop(level),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                container.remove().await;
                return Err(error);
            }
        };
        let exit_code = if outcome.stream.is_ok() {
            container.exit_code().await
        } else {
            None
        };
        container.remove().await;
        outcome
            .stream
            .map_err(|error| docker_error("reading one-shot output", error))?;

        // A container that vanished under the run — the sandbox's stop or
        // delete swept it — ended, but not by exiting: nobody observed how.
        let termination = if outcome.termination == Termination::Exited && exit_code.is_none() {
            Termination::Unknown
        } else {
            outcome.termination
        };
        Ok(captured.into_result(termination, exit_code, started.elapsed(), outcome.truncated))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[tokio::test]
    async fn cancelled_one_shots_do_not_prepare_an_image() {
        let docker =
            Docker::connect_with_http("http://127.0.0.1:1", 1, bollard::API_DEFAULT_VERSION)
                .expect("client");
        let runner = DockerOneShot::new(
            ContainerRef::new(docker, "missing".to_owned(), "/workspace".to_owned()),
            Mount::default(),
        );
        let token = CancellationToken::new();
        token.cancel();
        for (controls, expected) in [
            (
                ExecControls {
                    term: Some(token.clone()),
                    ..ExecControls::default()
                },
                Termination::Cancelled,
            ),
            (
                ExecControls {
                    kill: Some(token.clone()),
                    ..ExecControls::default()
                },
                Termination::Killed,
            ),
        ] {
            let result = runner
                .run(&OneShotSpec::registry("unreachable"), controls)
                .await
                .expect("stopped");
            assert_eq!(result.result.termination, expected);
            assert_eq!(result.result.exit_code, None);
        }
    }

    #[tokio::test]
    async fn image_preparation_obeys_the_operation_deadline() {
        // This socket accepts connections but never answers Docker requests.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let docker = Docker::connect_with_http(
            &format!("http://{}", listener.local_addr().expect("address")),
            60,
            bollard::API_DEFAULT_VERSION,
        )
        .expect("client");
        let runner = DockerOneShot::new(
            ContainerRef::new(docker, "missing".to_owned(), "/workspace".to_owned()),
            Mount::default(),
        );
        let spec = OneShotSpec::registry("unreachable").timeout(Duration::from_millis(20));
        let result = time::timeout(
            Duration::from_secs(2),
            runner.run(&spec, ExecControls::default()),
        )
        .await
        .expect("preparation deadline")
        .expect("timed out run");
        assert_eq!(result.result.termination, Termination::TimedOut);
        assert_eq!(result.result.exit_code, None);
    }
}
