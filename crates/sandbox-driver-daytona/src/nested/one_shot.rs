//! One-shot actions: a fresh container per run inside the nested Docker daemon.

use std::future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use sandbox_driver::{
    Error, ExecControls, ExecResult, ExecStreamingResult, OneShot, OneShotImage, OneShotSpec,
    OutputSink, Result, StopLevel, Termination, drain_with_stops, stop_signal,
};
use sandbox_driver_docker::is_missing_platform;
use tokio::time;
use tokio_util::sync::CancellationToken;

use super::operation::{run_command, run_owned};
use super::{
    CONTAINER_NAME, DRAIN_GRACE, DockerCli, MANAGED_LABEL, NestedDocker, ONE_SHOT_LABEL,
    START_TIMEOUT, checked, flags, words, workspace_mount,
};
use crate::RUNTIME_DIRECTORY;

/// A pull the registry refused for want of a matching platform manifest.
/// The daemon's own message is what [`is_missing_platform`] recognizes;
/// the nested CLI reports the same refusal in the command's output.
fn missing_platform(error: &Error) -> bool {
    if is_missing_platform(error) {
        return true;
    }
    let Error::Exec(failure) = error else {
        return false;
    };
    [failure.stdout(), failure.stderr()]
        .into_iter()
        .any(|output| String::from_utf8_lossy(output).contains("no matching manifest"))
}

impl NestedDocker {
    async fn prepare_action_image(&self, image: &OneShotImage) -> Result<String> {
        match image {
            OneShotImage::Registry { reference } => {
                if let Err(error) = self.cli.pull(reference, None, None).await {
                    if !missing_platform(&error) {
                        return Err(error);
                    }
                    self.cli
                        .pull(reference, None, Some("linux/amd64"))
                        .await
                        .map_err(|_| error)?;
                }
                Ok(reference.clone())
            }
            OneShotImage::Build {
                context,
                dockerfile,
                tag,
                reuse,
            } => {
                if *reuse && self.cli.image_present(tag).await? {
                    return Ok(tag.clone());
                }
                let context = self.cli.resolve(context);
                if dockerfile
                    .as_ref()
                    .is_some_and(|path| path.starts_with('/'))
                {
                    return Err(Error::invalid_spec(
                        "dockerfile",
                        "expected a path within the build context",
                    ));
                }
                let stage = self
                    .targets_container()
                    .then(|| format!("{RUNTIME_DIRECTORY}/build-{:016x}", rand::random::<u64>()));
                let cleanup_stage = stage.clone();
                let cli = Arc::clone(&self.cli);
                let cleanup_fs = Arc::clone(&self.cli.fs);
                let tag = tag.clone();
                let dockerfile = dockerfile.clone();
                run_owned(
                    move |cancel| async move {
                        let context = if let Some(stage) = stage {
                            cli.fs.create_dir(&stage).await?;
                            // Docker resolves the context in the job container. It may
                            // be outside the workspace bind or behind a container symlink.
                            checked(
                                run_command(
                                    &*cli.exec,
                                    &DockerCli::command(words([
                                        "cp",
                                        "--",
                                        &format!("{CONTAINER_NAME}:{context}/."),
                                        &stage,
                                    ])),
                                    &cancel,
                                )
                                .await?,
                                "copying nested Docker build context",
                            )?;
                            stage
                        } else {
                            context
                        };
                        let mut args = words(["build", "--tag", &tag]);
                        if let Some(dockerfile) = &dockerfile {
                            args.extend(words(["--file", &format!("{context}/{dockerfile}")]));
                        }
                        args.push(context);
                        checked(
                            run_command(&*cli.exec, &DockerCli::command(args), &cancel).await?,
                            "building nested Docker action image",
                        )?;
                        Ok(tag)
                    },
                    move || async move {
                        if let Some(stage) = cleanup_stage {
                            cleanup_fs.delete(&stage, true).await?;
                        }
                        Ok(())
                    },
                )
                .await
            }
            _ => Err(Error::invalid_spec("image", "unsupported one-shot image")),
        }
    }

    async fn signal_action(cli: &DockerCli, container: &str, signal: &str) -> Result<()> {
        let command =
            DockerCli::command(words(["kill", "--signal", signal, container])).timeout(DRAIN_GRACE);
        let output = cli.exec.run(&command).await?;
        if output.success() {
            return Ok(());
        }
        let message = format!("{}{}", output.stdout_lossy(), output.stderr_lossy());
        if message.contains("No such container") {
            return Ok(());
        }
        if message.contains("is not running") {
            // Removing a created container fences a Docker start that has not
            // reached the daemon yet. No later attach command can launch it.
            return cli.remove(container).await;
        }
        checked(output, "signalling nested Docker action").map(|_| ())
    }
}

#[async_trait]
impl OneShot for NestedDocker {
    /// Fork feature (work#1463): the cross-process presence probe over
    /// the nested `docker image inspect`.
    async fn image_present(&self, reference: &str) -> Result<bool> {
        self.cli.image_present(reference).await
    }

    async fn run(&self, spec: &OneShotSpec, controls: ExecControls) -> Result<ExecStreamingResult> {
        let started = Instant::now();
        let deadline = async {
            match spec.timeout {
                Some(limit) => time::sleep(limit).await,
                None => future::pending().await,
            }
        };
        let mut deadline = pin!(deadline);
        let prepared = tokio::select! {
            biased;
            () = stop_signal(controls.kill.as_ref()) => Err(Termination::Killed),
            () = stop_signal(controls.term.as_ref()) => Err(Termination::Cancelled),
            () = &mut deadline => Err(Termination::TimedOut),
            image = async { self.ensure_ready().await?; self.prepare_action_image(&spec.image).await } => Ok(image?),
        };
        let image = match prepared {
            Ok(image) => image,
            Err(reason) => {
                return Ok(ExecStreamingResult::new(ExecResult::from_shell_status(
                    reason,
                    None,
                    started.elapsed(),
                )));
            }
        };
        let (primary, network) = if self.targets_container() {
            let inspect = self.cli.inspect(CONTAINER_NAME).await?;
            let id = inspect["Id"]
                .as_str()
                .ok_or_else(|| Error::invalid_spec("docker inspect", "container id missing"))?
                .to_owned();
            let network = format!("container:{id}");
            (id, network)
        } else {
            (CONTAINER_NAME.to_owned(), "host".to_owned())
        };
        let name = format!("sandbox-driver-action-{:016x}", rand::random::<u64>());
        let mut args = words([
            "create",
            "--name",
            &name,
            "--init",
            "--label",
            MANAGED_LABEL,
            "--label",
            &format!("{ONE_SHOT_LABEL}={primary}"),
            "--network",
            &network,
            "--mount",
            &workspace_mount(&self.cli.working_dir),
            "--workdir",
            spec.working_dir.as_deref().unwrap_or(&self.cli.working_dir),
        ]);
        flags(
            &mut args,
            "--env",
            spec.env.iter().map(|(key, value)| format!("{key}={value}")),
        );
        if let Some(entrypoint) = &spec.entrypoint {
            args.extend(words(["--entrypoint", entrypoint]));
        }
        args.push(image);
        args.extend(spec.args.clone());
        let cleanup_cli = Arc::clone(&self.cli);
        let cleanup_name = name.clone();
        let action = Action {
            cli: Arc::clone(&self.cli),
            name,
            spec: spec.clone(),
            started,
        };
        run_owned(
            move |abandoned| action.run(args, controls, abandoned),
            move || async move { cleanup_cli.remove(&cleanup_name).await },
        )
        .await
    }
}

/// One action container, owned by the run that drives it: the CLI it is
/// created through, its container name, the spec it runs, and when the
/// run began (its timeout counts from there, image preparation included).
/// The bundle exists because `run_owned` moves the run onto its own
/// task, which needs everything `'static`.
struct Action {
    cli:     Arc<DockerCli>,
    name:    String,
    spec:    OneShotSpec,
    started: Instant,
}

impl Action {
    async fn run(
        self,
        args: Vec<String>,
        controls: ExecControls,
        abandoned: CancellationToken,
    ) -> Result<ExecStreamingResult> {
        self.cli
            .run_spec(&DockerCli::command(args).timeout(START_TIMEOUT))
            .await?;
        let stopped = if abandoned.is_cancelled()
            || controls
                .kill
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            Some(Termination::Killed)
        } else if controls
            .term
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            Some(Termination::Cancelled)
        } else if self
            .spec
            .timeout
            .is_some_and(|limit| self.started.elapsed() >= limit)
        {
            Some(Termination::TimedOut)
        } else {
            None
        };
        if let Some(reason) = stopped {
            return Ok(ExecStreamingResult::new(ExecResult::from_shell_status(
                reason,
                None,
                self.started.elapsed(),
            )));
        }
        async {
            let sink_failed = CancellationToken::new();
            let sink = controls.sink.clone().map(|sink| {
                let failed = sink_failed.clone();
                Arc::new(move |stream, bytes| {
                    let sink = Arc::clone(&sink);
                    let failed = failed.clone();
                    Box::pin(async move {
                        if sink(stream, bytes).await.is_err() {
                            failed.cancel();
                        }
                        Ok(())
                    })
                        as Pin<Box<dyn future::Future<Output = Result<()>> + Send>>
                }) as OutputSink
            });
            let transport_kill = CancellationToken::new();
            let mut command =
                DockerCli::command(words(["start", "--attach", &self.name])).no_timeout();
            command.output_sanitization = self.spec.output_sanitization;
            let mut run = pin!(self.cli.exec.run_streaming(&command, ExecControls {
                sink,
                kill: Some(transport_kill.clone()),
                retained_output_limit: controls.retained_output_limit,
                ..Default::default()
            }));
            // Abandonment by the owner is a kill like any other: the race
            // sees one kill token, which the caller's own kill also
            // cancels through a link that, like the stop ladder, only
            // drives the token and never resolves.
            let kill = abandoned.child_token();
            let link = async {
                stop_signal(controls.kill.as_ref()).await;
                kill.cancel();
                future::pending::<()>().await;
            };
            let race_controls = ExecControls {
                term: controls.term.clone(),
                kill: Some(kill.clone()),
                ..ExecControls::default()
            };
            let race = drain_with_stops(
                run.as_mut(),
                &sink_failed,
                &race_controls,
                self.spec
                    .timeout
                    .map(|limit| limit.saturating_sub(self.started.elapsed())),
                DRAIN_GRACE,
                |level| {
                    let signal = match level {
                        StopLevel::Term => "TERM",
                        StopLevel::Kill => "KILL",
                    };
                    NestedDocker::signal_action(&self.cli, &self.name, signal)
                },
            );
            let outcome = tokio::select! {
                outcome = race => outcome?,
                () = link => unreachable!("the kill link never resolves"),
            };
            let mut result = if let Some(result) = outcome.drained {
                result?
            } else {
                // The action was killed and its output still has not
                // closed: end the native transport and wait for it.
                transport_kill.cancel();
                run.await?
            };
            if outcome.termination != Termination::Exited {
                result.result.termination = outcome.termination;
            }
            result.result.duration = self.started.elapsed();
            Ok(result)
        }
        .await
    }
}
