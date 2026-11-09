use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{io, process};

use async_trait::async_trait;
use bollard::container::LogOutput;
use bollard::exec::CreateExecOptions;
use futures_util::StreamExt;
use sandbox_driver::{
    BASH_ENV_VAR, Error, Exec, ExecControls, ExecResult, ExecSpec, ExecStreamingResult, Result,
    SpawnSpec, StderrTail, StdioProcess, StdioProcessHandle, StopLevel, Termination, feed_stdin,
    finish_stdin_writer, run_with_stop_grace,
};
use tokio::io::{AsyncWriteExt, duplex};
use tokio::time;

use crate::container::{AttachedExec, ContainerRef, exit_code_of};
use crate::daemon::{POSIX_SH, docker_error, shell_quote};
use crate::output::{StreamOutput, drain_with_stops};

/// `$0` of the wrapper shell, so the user's argv starts at `$1`.
const WRAPPER_NAME: &str = "sandbox-driver";
/// Bound on retrying transient `inspect_exec` failures in stdio `wait`
/// before giving up with `Termination::Unknown`: an end that was never
/// observed is reported only when the daemon stays unreachable for the
/// whole window.
const WAIT_INSPECT_RETRY: Duration = Duration::from_secs(10);
/// The in-container watcher's poll interval for the stop file.
const STOP_POLL_SLEEP_SECONDS: &str = "0.1";
/// Grace between a stdio handle's `terminate` SIGTERM and its SIGKILL,
/// so the process can run traps, flush output, and release locks. The
/// exec path has no ladder of its own: its caller escalates.
const TERM_GRACE: Duration = Duration::from_secs(2);

/// The stop file's contents for `level`; the in-container watcher reads
/// it and signals the command's process group accordingly. A `kill`
/// written after a `term` is honoured — the watcher keeps reading.
fn stop_file_contents(level: StopLevel) -> &'static str {
    match level {
        StopLevel::Term => "term",
        StopLevel::Kill => "kill",
    }
}

/// Command execution inside one container.
///
/// Docker cannot kill an exec instance, so every command runs under a
/// `/bin/sh` wrapper that puts it in its own session and pairs it with an
/// in-container watcher; timeout and cancellation request a stop by
/// creating a stop file through a second exec, and the watcher kills
/// the process group (SIGTERM, grace, SIGKILL) whenever the stop lands
/// — even before the command starts. The wrapper's `wait` forwards the
/// child's exit code.
///
/// The environment and the program reach the wrapper as its positional
/// parameters — `KEY=VALUE`… `program` `args`… — and are executed with
/// `setsid env "$@"`, so no shell ever interprets them. The environment
/// travels through `env` rather than the exec's own environment on
/// purpose: `/bin/sh` is dash on most images, and dash drops variables
/// whose names are not identifiers (`INPUT_INCLUDE-HIDDEN-FILES`, which
/// GitHub Actions passes) when it spawns a child. `env` sets them as
/// given. One consequence: `env` reads a leading word containing `=` as
/// an assignment, so a program whose name contains `=` cannot be run.
pub struct DockerExec {
    container:    ContainerRef,
    base_env:     BTreeMap<String, String>,
    exec_counter: AtomicU64,
}

impl DockerExec {
    pub(crate) fn new(container: ContainerRef, base_env: BTreeMap<String, String>) -> Self {
        Self {
            container,
            base_env,
            exec_counter: AtomicU64::new(0),
        }
    }

    /// The command's environment as `env` arguments: the sandbox's base
    /// env with `BASH_ENV` blanked (an image can carry one, and it would
    /// run inside any `bash -c` a caller sends), then the spec env as
    /// given.
    fn env_entries(&self, extra: &BTreeMap<String, String>) -> Vec<String> {
        let mut entries: Vec<String> = self
            .base_env
            .iter()
            .filter(|(key, _)| key.as_str() != BASH_ENV_VAR)
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        entries.push(format!("{BASH_ENV_VAR}="));
        entries.extend(extra.iter().map(|(key, value)| format!("{key}={value}")));
        entries
    }

    /// The `Cmd` of a wrapped exec: the wrapper under `/bin/sh`, then the
    /// environment, program, and arguments as its positional parameters.
    fn wrapped_cmd(
        wrapper: String,
        env: Vec<String>,
        program: &str,
        args: &[String],
    ) -> Vec<String> {
        let mut cmd = Vec::with_capacity(env.len() + args.len() + 5);
        cmd.extend([POSIX_SH.to_owned(), "-c".to_owned(), wrapper]);
        cmd.push(WRAPPER_NAME.to_owned());
        cmd.extend(env);
        cmd.push(program.to_owned());
        cmd.extend(args.iter().cloned());
        cmd
    }

    /// The exec of one wrapped command: the wrapper under `/bin/sh` with
    /// the environment, program, and arguments as its positional
    /// parameters, attached to both output streams and, when asked, to
    /// stdin, in the resolved working directory. The wrapper shell itself
    /// gets a blank `BASH_ENV`; the command's environment travels through
    /// `env` inside the wrapper.
    fn wrapped_exec_options(
        &self,
        stop_file: &str,
        pid_file: &str,
        program: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        working_dir: Option<&str>,
        attach_stdin: bool,
    ) -> CreateExecOptions<String> {
        let wrapper = Self::command_wrapper(stop_file, pid_file, attach_stdin);
        CreateExecOptions {
            attach_stdin: Some(attach_stdin),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            tty: Some(false),
            cmd: Some(Self::wrapped_cmd(
                wrapper,
                self.env_entries(env),
                program,
                args,
            )),
            working_dir: Some(self.container.resolve_dir(working_dir)),
            env: Some(vec![format!("{BASH_ENV_VAR}=")]),
            ..Default::default()
        }
    }

    /// Allocates a unique stop-file/pid-file pair for one exec. The
    /// nanosecond nonce keeps paths from colliding across driver
    /// restarts (containers outlive drivers, host pids recycle, and the
    /// counter restarts at zero), so a stale control file can never
    /// misdirect a stop.
    fn control_paths(&self) -> (String, String) {
        let count = self.exec_counter.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let prefix = format!(
            "/tmp/.sandbox-driver/exec-{}-{nonce}-{count}",
            process::id()
        );
        (format!("{prefix}.stop"), format!("{prefix}.pid"))
    }

    /// The `/bin/sh` wrapper that runs `env "$@"` — the environment,
    /// program, and arguments — so a stop request is honored at any point.
    ///
    /// An in-container watcher polls for the stop file, so a stop that
    /// lands before the pid file exists — or before the command starts
    /// at all — still takes effect. The stop file carries a rendered
    /// mode: for `term` the watcher SIGTERMs the process group
    /// once and keeps watching, so a later `kill` still lands; for
    /// `kill` it SIGKILLs. There is no escalation in here: the caller
    /// owns that. Control files are cleared before the command starts
    /// and removed on exit. The `wait` runs with stderr closed: dash
    /// reports a signalled background job as `Terminated` on stderr,
    /// which would otherwise land in the command's output.
    pub fn command_wrapper(stop_file: &str, pid_file: &str, forward_stdin: bool) -> String {
        let stop_file = shell_quote(stop_file);
        let pid_file = shell_quote(pid_file);
        let (save_stdin, stdin_redirect, close_stdin) = if forward_stdin {
            ("exec 3<&0\n", "<&3", "exec 3<&-\n")
        } else {
            ("", "< /dev/null", "")
        };
        // The watcher waits for a non-empty stop file so it never reads
        // the mode mid-write. It runs with its own output on /dev/null —
        // a watcher that inherited the exec pipes would hold the stream
        // open after the command had died — and after a `term` it keeps
        // polling the file, in ticks that end when the child is gone,
        // for a `kill` to follow.
        format!(
            "mkdir -p /tmp/.sandbox-driver\n\
             if ! command -v setsid >/dev/null 2>&1 || ! command -v env >/dev/null 2>&1; then\n\
               echo 'sandbox-driver: the container image must provide setsid and env' >&2\n\
               exit 127\n\
             fi\n\
             stop_file={stop_file}\n\
             pid_file={pid_file}\n\
             {save_stdin}rm -f \"$pid_file\"\n\
             if [ -e \"$stop_file\" ]; then\n\
               mode=$(cat \"$stop_file\" 2>/dev/null)\n\
               rm -f \"$stop_file\"\n\
               if [ \"$mode\" = kill ]; then exit 137; fi\n\
               exit 143\n\
             fi\n\
             (\n\
               while [ ! -s \"$stop_file\" ]; do sleep {STOP_POLL_SLEEP_SECONDS}; done\n\
               while [ ! -s \"$pid_file\" ]; do sleep {STOP_POLL_SLEEP_SECONDS}; done\n\
               child=$(cat \"$pid_file\")\n\
               mode=$(cat \"$stop_file\" 2>/dev/null)\n\
               if [ \"$mode\" != kill ]; then\n\
                 kill -TERM \"-$child\" 2>/dev/null || kill -TERM \"$child\" 2>/dev/null || true\n\
                 while kill -0 \"$child\" 2>/dev/null; do\n\
                   [ \"$(cat \"$stop_file\" 2>/dev/null)\" = kill ] && break\n\
                   sleep {STOP_POLL_SLEEP_SECONDS}\n\
                 done\n\
                 kill -0 \"$child\" 2>/dev/null || exit 0\n\
               fi\n\
               kill -KILL \"-$child\" 2>/dev/null || kill -KILL \"$child\" 2>/dev/null || true\n\
             ) >/dev/null 2>&1 & watcher=$!\n\
             setsid env \"$@\" {stdin_redirect} &\n\
             child=$!\n\
             {close_stdin}printf '%s' \"$child\" > \"$pid_file\"\n\
             wait \"$child\" 2>/dev/null\n\
             status=$?\n\
             kill \"$watcher\" 2>/dev/null || true\n\
             wait \"$watcher\" 2>/dev/null || true\n\
             rm -f \"$stop_file\" \"$pid_file\"\n\
             exit \"$status\""
        )
    }
}

/// Requests a stop by writing the rendered `level` into the stop file
/// the in-container watcher polls for; a stop that arrives before
/// the command starts is honored by the wrapper's pre-check. Runs
/// from `/` — the exec being stopped may have removed the working
/// directory the container would otherwise start this one in — with
/// a blank `BASH_ENV`, and reports failure: a stop that could not be
/// requested must never masquerade as a kill.
async fn request_stop(container: &ContainerRef, stop_file: &str, level: StopLevel) -> Result<()> {
    let command = format!(
        "mkdir -p /tmp/.sandbox-driver && printf '%s' {} > {}",
        shell_quote(stop_file_contents(level)),
        shell_quote(stop_file)
    );
    let options = CreateExecOptions {
        cmd: Some(vec![POSIX_SH.to_owned(), "-c".to_owned(), command]),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        tty: Some(false),
        working_dir: Some("/".to_owned()),
        env: Some(vec![format!("{BASH_ENV_VAR}=")]),
        ..Default::default()
    };
    let AttachedExec { id, mut output, .. } = container
        .start_control(options, "stop-request exec")
        .await?;
    // Drain to completion so the exit code below is final.
    while let Some(chunk) = output.next().await {
        if let Err(error) = chunk {
            let error = docker_error("reading stop request output", error);
            tracing::warn!(
                provider_kind = "docker",
                sandbox_id = %container.id,
                error = %error,
                "stop request output stream failed"
            );
            break;
        }
    }
    match container.exec_exit_code(&id).await? {
        None | Some(0) => Ok(()),
        Some(code) => Err(Error::io(
            "requesting exec stop",
            io::Error::other(format!("stop request exited {code}")),
        )),
    }
}

#[async_trait]
impl Exec for DockerExec {
    async fn run(&self, spec: &ExecSpec) -> Result<ExecResult> {
        let streaming = self.run_streaming(spec, ExecControls::buffered()).await?;
        streaming.into_complete()
    }

    #[tracing::instrument(
        skip_all,
        fields(
            provider_kind = "docker",
            sandbox_id = %self.container.id,
            has_stdin = spec.stdin.is_some()
        ),
        err
    )]
    async fn run_streaming(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecStreamingResult> {
        run_with_stop_grace(spec, controls, |spec, controls| async move {
            self.run_signals(&spec, controls).await
        })
        .await
    }

    async fn spawn_stdio(&self, spec: &SpawnSpec) -> Result<StdioProcess> {
        self.spawn_stdio_raw(spec).await
    }
}

impl DockerExec {
    /// Runs the command under raw stop signals; the trait method wraps
    /// this in the spec's stop grace.
    async fn run_signals(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecStreamingResult> {
        let started = Instant::now();
        let (stop_file, pid_file) = self.control_paths();
        let stdin_reader = controls.stdin_reader(spec);
        let options = self.wrapped_exec_options(
            &stop_file,
            &pid_file,
            &spec.program,
            &spec.args,
            &spec.launch_env(),
            spec.working_dir.as_deref(),
            stdin_reader.is_some(),
        );
        let AttachedExec {
            id: exec_id,
            output,
            input,
        } = self.container.start_attached(options, "exec").await?;

        // A command without stdin gets the attached input closed at once.
        let stdin_task = stdin_reader.map(|reader| tokio::spawn(feed_stdin(input, reader)));

        let mut captured =
            StreamOutput::new(spec.output_sanitization, controls.retained_output_limit);
        let outcome = drain_with_stops(
            &mut captured,
            output,
            &controls,
            spec.timeout
                .map(|timeout| timeout.saturating_sub(started.elapsed())),
            |level| request_stop(&self.container, &stop_file, level),
        )
        .await?;

        if let Some(stdin_task) = stdin_task {
            finish_stdin_writer(stdin_task).await?;
        }
        outcome
            .stream
            .map_err(|error| docker_error("reading exec output", error))?;
        let exit_code = self.container.exec_exit_code(&exec_id).await?;
        Ok(captured.into_result(
            outcome.termination,
            exit_code,
            started.elapsed(),
            outcome.truncated,
        ))
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id),
        err
    )]
    async fn spawn_stdio_raw(&self, spec: &SpawnSpec) -> Result<StdioProcess> {
        let (stop_file, pid_file) = self.control_paths();
        let options = self.wrapped_exec_options(
            &stop_file,
            &pid_file,
            &spec.program,
            &spec.args,
            &spec.launch_env(),
            spec.working_dir.as_deref(),
            true,
        );
        let AttachedExec {
            id: exec_id,
            mut output,
            input,
        } = self.container.start_attached(options, "stdio exec").await?;

        // Demux the attached stream into a stdout pipe and a stderr tail.
        let (stdout_writer, stdout_reader) = duplex(64 * 1024);
        let stderr_tail = StderrTail::default();
        let tail = stderr_tail.clone();
        let sandbox_id = self.container.id.clone();
        tokio::spawn(async move {
            let mut stdout_writer = stdout_writer;
            loop {
                match output.next().await {
                    None => break,
                    // Make a transport failure visible in the diagnostic
                    // tail instead of ending the stream silently.
                    Some(Err(error)) => {
                        let diagnostic = error.to_string();
                        let error = docker_error("reading stdio output", error);
                        tracing::warn!(
                            provider_kind = "docker",
                            sandbox_id = %sandbox_id,
                            error = %error,
                            "stdio output stream failed"
                        );
                        tail.push(
                            format!("sandbox-driver: stdio output stream error: {diagnostic}\n")
                                .as_bytes(),
                        );
                        break;
                    }
                    Some(Ok(LogOutput::StdOut { message } | LogOutput::Console { message })) => {
                        if let Err(error) = stdout_writer.write_all(&message).await {
                            tracing::debug!(
                                provider_kind = "docker",
                                sandbox_id = %sandbox_id,
                                error = ?error,
                                "stdio output reader closed"
                            );
                            break;
                        }
                    }
                    Some(Ok(LogOutput::StdErr { message })) => tail.push(&message),
                    Some(Ok(LogOutput::StdIn { .. })) => {}
                }
            }
            if let Err(error) = stdout_writer.shutdown().await {
                tracing::debug!(
                    provider_kind = "docker",
                    sandbox_id = %sandbox_id,
                    error = ?error,
                    "stdio output pipe close failed"
                );
            }
        });

        let handle = DockerStdioHandle {
            container: self.container.clone(),
            exec_id,
            stop_file,
            stop_requested: AtomicBool::new(false),
        };
        Ok(StdioProcess {
            stdin: Box::pin(input),
            stdout: Box::pin(stdout_reader),
            stderr_tail,
            handle: Box::new(handle),
        })
    }
}

struct DockerStdioHandle {
    container:      ContainerRef,
    exec_id:        String,
    stop_file:      String,
    /// At most one stop request per handle: a repeat call, or one after
    /// the wrapper already consumed the stop file, would spawn a fresh
    /// exec and leave a stray stop file behind.
    stop_requested: AtomicBool,
}

impl DockerStdioHandle {
    /// Whether the exec is still running. An inspect failure counts as
    /// running: when in doubt, kill.
    async fn still_running(&self) -> bool {
        match self.container.docker.inspect_exec(&self.exec_id).await {
            Ok(inspect) => inspect.running == Some(true),
            Err(_) => true,
        }
    }
}

#[async_trait]
impl StdioProcessHandle for DockerStdioHandle {
    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id)
    )]
    async fn terminate(&self) {
        if self.stop_requested.swap(true, Ordering::SeqCst) {
            return;
        }
        // An already-exited process needs no stop request: the wrapper
        // that would consume the stop file is gone, so the request
        // would spawn a pointless exec and leave a permanent stray file
        // (fabro gated on observed termination the same way). An
        // inspect failure still sends the stop — when in doubt, kill.
        if !self.still_running().await {
            return;
        }
        // The trait offers no error channel; awaiting at least keeps
        // the request ordered before any caller-side cleanup.
        // TERM, then KILL after the grace: the handle's own ladder, since
        // the trait has one verb. The watcher keeps reading the stop file
        // after a term, so the kill lands on a process that ignored it.
        if let Err(error) = request_stop(&self.container, &self.stop_file, StopLevel::Term).await {
            tracing::warn!(error = %error, "stdio stop request failed");
            return;
        }
        time::sleep(TERM_GRACE).await;
        if !self.still_running().await {
            return;
        }
        if let Err(error) = request_stop(&self.container, &self.stop_file, StopLevel::Kill).await {
            tracing::warn!(error = %error, "stdio kill request failed");
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", sandbox_id = %self.container.id)
    )]
    async fn wait(&self) -> (Termination, Option<i32>) {
        let mut errors_since: Option<Instant> = None;
        let mut retry_delay = Duration::from_millis(100);
        loop {
            match self.container.docker.inspect_exec(&self.exec_id).await {
                Ok(inspect) if inspect.running == Some(true) => {
                    errors_since = None;
                    retry_delay = Duration::from_millis(100);
                    time::sleep(Duration::from_millis(100)).await;
                }
                Ok(inspect) => return (Termination::Exited, exit_code_of(&inspect)),
                // A transient daemon hiccup must not report an end that
                // was never observed; give up with `Unknown` only after
                // the daemon has been unreachable for the whole bound.
                Err(error) => {
                    if errors_since.is_none() {
                        tracing::warn!("stdio status polling failed");
                    }
                    let since = *errors_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= WAIT_INSPECT_RETRY {
                        let error = docker_error("polling stdio status", error);
                        tracing::error!(error = %error, "stdio status remained unavailable");
                        return (Termination::Unknown, None);
                    }
                    time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
                }
            }
        }
    }
}
