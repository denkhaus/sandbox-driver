use std::future::Future;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{io, mem, process};

use async_trait::async_trait;
use daytona_sdk::{DaytonaError, FileSystemService, ProcessService, SessionCommandLogsResult};
use sandbox_driver::{
    Error, Exec, ExecControls, ExecResult, ExecSpec, ExecStreamingResult, IncompleteOperation,
    OutputCaptureBuffer, OutputSanitizer, OutputSink, OutputStream, Result, SpawnSpec,
    StdioProcess, StopLevel, Termination, run_with_stop_grace,
};
use serde::Deserialize;
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::sdk::{DaytonaClient, daytona_error};
use crate::session::{Session, WaitOutcome, dedup_capture, missing_suffix, wait_for_completion};
use crate::shell::{exec_line, shell_quote};
use crate::streamed_stdin::{self, InputPump};
use crate::{encoded_exec, resolve_path, stdio, toolbox};

#[derive(Deserialize)]
struct BufferedResponse {
    #[serde(rename = "exitCode", default)]
    exit_code: Option<i32>,
    result:    String,
}

/// Bound on waiting for the log stream to close after the command has
/// its outcome; a stream that will not end is abandoned.
const STREAM_DRAIN_GRACE: Duration = Duration::from_secs(10);

/// Extra client-side wait beyond the server-side command timeout.
const TIMEOUT_GRACE: Duration = Duration::from_secs(10);

/// The server-side timeout sent when the spec has none. Omitting the
/// field does not mean "no deadline" — the toolbox applies its own
/// 10-second default and kills the command — so an untimed spec must
/// cross the wire as an explicit, effectively unbounded timeout. One
/// year fits comfortably in the API's `i32` seconds.
const UNBOUNDED_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// The server-side timeout for a spec: its own, rounded up to a whole
/// second, or the unbounded stand-in — never an omitted field. The API
/// field is integer seconds; without the ceiling, a sub-second timeout
/// would truncate to `0` on the wire instead of bounding the command.
fn wire_timeout(spec_timeout: Option<Duration>) -> Duration {
    let Some(timeout) = spec_timeout else {
        return UNBOUNDED_TIMEOUT;
    };
    if timeout.subsec_nanos() > 0 {
        Duration::from_secs(timeout.as_secs() + 1)
    } else {
        timeout
    }
}

/// Bound on deleting a stdin temp file, so cleanup can never stall a
/// command that already completed.
const STDIN_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

/// A temporary sandbox file carrying exact stdin bytes.
///
/// The toolbox execute API has no stdin channel, so write-then-EOF is
/// delivered as a file redirection: arbitrary bytes survive verbatim
/// and the command sees EOF, without embedding the data in shell
/// source. fabro's `DaytonaStdinFile`, ported.
struct StdinFile {
    fs:   Option<FileSystemService>,
    path: String,
}

impl StdinFile {
    async fn create(client: &DaytonaClient, sandbox_id: &str, bytes: &[u8]) -> Result<Self> {
        if bytes.len() > sandbox_driver::DEFAULT_BUFFER_BYTES {
            return Err(Error::LimitExceeded {
                limit:     "buffered_value_bytes".into(),
                max_bytes: sandbox_driver::DEFAULT_BUFFER_BYTES,
            });
        }
        let fs = toolbox::fs(client, sandbox_id).await?;
        // Random nonce plus host pid: unique even across concurrent
        // execs in one process, so a stale file from a crashed driver
        // can never feed a later command.
        let nonce: u64 = rand::random();
        let path = format!("/tmp/.sandbox-driver-stdin-{}-{nonce:016x}", process::id());
        fs.upload_file_bytes(&path, bytes)
            .await
            .map_err(|error| daytona_error("uploading exec stdin", error))?;
        Ok(Self { fs: Some(fs), path })
    }

    /// Bounded, best-effort deletion. Failures are swallowed: cleanup
    /// must never fail a command that already ran.
    async fn close(&mut self) {
        let Some(fs) = self.fs.as_ref() else {
            return;
        };
        match time::timeout(STDIN_CLEANUP_TIMEOUT, fs.delete_file(&self.path, false)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let error = daytona_error("deleting exec stdin file", error);
                tracing::warn!(error = %error, "exec stdin file cleanup failed");
            }
            Err(_) => tracing::warn!("exec stdin file cleanup timed out"),
        }
        self.fs.take();
    }
}

impl Drop for StdinFile {
    /// Safety net for a caller that drops the exec future mid-await:
    /// spawn the deletion when a runtime is available.
    fn drop(&mut self) {
        let Some(fs) = self.fs.take() else {
            return;
        };
        let path = mem::take(&mut self.path);
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                match time::timeout(STDIN_CLEANUP_TIMEOUT, fs.delete_file(&path, false)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let error = daytona_error("deleting dropped exec stdin file", error);
                        tracing::warn!(error = %error, "dropped exec stdin cleanup failed");
                    }
                    Err(_) => tracing::warn!("dropped exec stdin cleanup timed out"),
                }
            });
        } else {
            tracing::warn!("exec stdin cleanup skipped without a runtime");
        }
    }
}

/// Native command execution with byte-preserving output framing.
pub struct DaytonaExec {
    transport: DaytonaTransport,
}

impl DaytonaExec {
    pub(crate) fn new(client: DaytonaClient, sandbox_id: String, working_dir: String) -> Self {
        Self {
            transport: DaytonaTransport::new(client, sandbox_id, working_dir),
        }
    }
}

#[async_trait]
impl Exec for DaytonaExec {
    async fn run(&self, spec: &ExecSpec) -> Result<ExecResult> {
        self.run_streaming(spec, ExecControls::buffered())
            .await?
            .into_complete()
    }

    async fn run_streaming(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecStreamingResult> {
        encoded_exec::run(&self.transport, spec, controls).await
    }

    async fn spawn_stdio(&self, spec: &SpawnSpec) -> Result<StdioProcess> {
        self.transport.spawn_stdio(spec).await
    }
}

/// Command execution through the Daytona toolbox, on two transports.
///
/// A plain run uses the one-shot `execute` endpoint: combined output
/// after completion (`streams_separated: false`, `live_streaming:
/// false`, stderr empty) at one API call. A run with a sink or stop
/// token uses a command session instead: logs stream live with
/// server-side stdout/stderr separation, stops and timeouts kill the
/// command by deleting its session, and partial output survives a
/// timeout via a final log fetch. The toolbox takes a shell
/// string on both transports, so the spec's environment, program, and
/// arguments are quoted into one `exec env …` word list — the quoting
/// keeps every word literal, and `env` (not `export`) lets variable names
/// that are not shell identifiers through. The environment crosses in the
/// command because the API's `envs` field is not reliably applied.
/// `BASH_ENV` is unset in the composing shell first.
pub(crate) struct DaytonaTransport {
    client:      DaytonaClient,
    sandbox_id:  String,
    working_dir: String,
    endpoint:    OnceCell<String>,
}

impl DaytonaTransport {
    pub(crate) fn new(client: DaytonaClient, sandbox_id: String, working_dir: String) -> Self {
        Self {
            client,
            sandbox_id,
            working_dir,
            endpoint: OnceCell::new(),
        }
    }

    async fn endpoint(&self) -> Result<&String> {
        self.endpoint
            .get_or_try_init(|| toolbox::endpoint(&self.client, &self.sandbox_id))
            .await
    }

    /// Resolves a relative working directory against the sandbox
    /// working directory, matching the fs facet — the toolbox daemon
    /// would otherwise resolve it against its own cwd.
    fn resolve_dir(&self, dir: Option<&str>) -> String {
        dir.map_or_else(
            || self.working_dir.clone(),
            |dir| resolve_path(&self.working_dir, dir),
        )
    }

    fn compose(spec: &ExecSpec, stdin_path: Option<&str>) -> String {
        // Sandbox-level hygiene: the composing shell must not carry an
        // image's BASH_ENV into a `bash -c` the caller sends. The spec
        // env applies afterwards, as given.
        let mut program = String::from("unset BASH_ENV\n");
        program.push_str(&exec_line(&spec.launch_env(), &spec.program, &spec.args));
        // Write-then-EOF as a file redirection on the program.
        if let Some(path) = stdin_path {
            program.push_str(" < ");
            program.push_str(&shell_quote(path));
        }
        program
    }
}

#[async_trait]
impl Exec for DaytonaTransport {
    async fn run(&self, spec: &ExecSpec) -> Result<ExecResult> {
        let streaming = self.run_streaming(spec, ExecControls::buffered()).await?;
        streaming.into_complete()
    }

    #[tracing::instrument(
        skip_all,
        fields(
            provider_kind = "daytona",
            sandbox_id = %self.sandbox_id,
            has_stdin = spec.stdin.is_some(),
            live_streaming = controls.sink.is_some() || controls.term.is_some() || controls.kill.is_some()
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

impl DaytonaTransport {
    /// Runs the command under raw stop signals; the trait method wraps
    /// this in the spec's stop grace.
    async fn run_signals(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecStreamingResult> {
        // Sessions cost three extra API calls, so plain buffered runs —
        // every derived fs/search/git operation — keep the one-shot
        // endpoint; only streamed stdin, a sink, or a stop token needs the
        // session transport. Daytona ends a command by deleting its session,
        // which is one stop level: a term and a kill end the command the
        // same way, and the result reports whichever was asked for.
        if controls.stdin.is_some()
            || controls.sink.is_some()
            || controls.term.is_some()
            || controls.kill.is_some()
        {
            return self.run_session(spec, controls).await;
        }
        self.run_buffered(spec, &controls).await
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "daytona", sandbox_id = %self.sandbox_id),
        err
    )]
    async fn spawn_stdio_raw(&self, spec: &SpawnSpec) -> Result<StdioProcess> {
        stdio::spawn(
            &self.client,
            &self.sandbox_id,
            &self.resolve_dir(spec.working_dir.as_deref()),
            spec,
        )
        .await
    }
}

impl DaytonaTransport {
    async fn run_buffered(
        &self,
        spec: &ExecSpec,
        controls: &ExecControls,
    ) -> Result<ExecStreamingResult> {
        let started = Instant::now();
        let endpoint = self.endpoint().await?;
        let mut stdin_file = match &spec.stdin {
            Some(bytes) => Some(StdinFile::create(&self.client, &self.sandbox_id, bytes).await?),
            None => None,
        };
        let program = Self::compose(spec, stdin_file.as_ref().map(|file| file.path.as_str()));
        let call = async {
            let (status, bytes) = toolbox::request(
                &self.client,
                endpoint,
                &["process", "execute"],
                Some(json!({
                    "command": program,
                    "cwd": self.resolve_dir(spec.working_dir.as_deref()),
                    "timeout": wire_timeout(spec.timeout).as_secs(),
                })),
            )
            .await?;
            if status == 408 {
                return Ok(None);
            }
            if !(200..300).contains(&status) {
                return Err(Error::io(
                    "executing command",
                    io::Error::other(format!("HTTP {status}")),
                ));
            }
            let response: BufferedResponse = serde_json::from_slice(&bytes)
                .map_err(|error| Error::io("decoding command response", io::Error::other(error)))?;
            Ok::<_, Error>(Some(response))
        };
        let response = match spec.timeout {
            Some(timeout) => time::timeout(timeout + TIMEOUT_GRACE, call)
                .await
                .unwrap_or_else(|_| {
                    Err(Error::Incomplete(IncompleteOperation::new(
                        "Daytona command timeout",
                    )))
                }),
            None => call.await,
        };
        // Clean up before propagating any failure, so an errored command
        // does not strand its stdin file.
        if let Some(file) = stdin_file.as_mut() {
            file.close().await;
        }
        let response = response?;

        // A response is a completed command, whatever its exit code and
        // however close to the deadline it arrived — classifying a
        // genuine failure as TimedOut would trip retry-on-timeout logic
        // on non-idempotent commands. Timeouts are only ever the 408 or
        // client-deadline paths above.
        let (termination, exit_code, stdout) = match response {
            None => (Termination::TimedOut, None, Vec::new()),
            Some(response) => (
                Termination::Exited,
                Some(response.exit_code.unwrap_or(0)),
                spec.output_sanitization
                    .sanitize(response.result.as_bytes()),
            ),
        };

        if let Some(sink) = &controls.sink {
            if !stdout.is_empty() {
                // The buffered transport delivers after completion, so
                // there is nothing left to cancel — but a failed sink
                // must surface: the caller would otherwise believe the
                // output was delivered.
                sink(OutputStream::Stdout, stdout.clone()).await?;
            }
        }
        let mut capture = OutputCaptureBuffer::new(controls.retained_output_limit);
        capture.push(&stdout);
        let (retained, stats) = capture.into_parts();

        let mut result = ExecResult::from_shell_status(termination, exit_code, started.elapsed());
        result.stdout = retained;
        let mut streaming = ExecStreamingResult::new(result);
        streaming.streams_separated = false;
        streaming.live_streaming = false;
        streaming.stdout_capture = stats;
        Ok(streaming)
    }

    /// Streaming/stoppable execution through a command session.
    ///
    /// The command runs asynchronously in a dedicated session; logs
    /// follow live with server-side stream separation; the status poll
    /// races the timeout and the stop tokens; and a non-natural end
    /// kills the command by deleting the session. Partial output on
    /// timeout/stop comes from a final log fetch, deduplicated
    /// against what the stream already delivered.
    async fn run_session(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> Result<ExecStreamingResult> {
        let started = Instant::now();
        let mut run = SessionRun::start(self, spec, &controls).await?;
        let outcome = match run.wait(spec.timeout, controls.stop_requested()).await {
            Ok(outcome) => outcome,
            Err(error) => return Err(run.abort(error).await),
        };
        Ok(run.settle(outcome).await?.into_result(started).await)
    }
}

async fn close_stdin(file: &mut Option<StdinFile>) {
    if let Some(file) = file.as_mut() {
        file.close().await;
    }
}

/// One session-backed command, owned from start to settled.
///
/// Every early exit goes through [`SessionRun::abort`], which ends the
/// stream task and the input pump, deletes the session, and removes the
/// stdin file, so a new return path cannot leak any of them. The awaited paths
/// are the contract: [`Session::close`] reports whether deletion confirmed the
/// command's termination, and [`SessionRun::settle`] turns a refused
/// deletion into [`Error::Incomplete`]. Dropping an unfinished run (the
/// caller cancelled) is the fallback only: [`StreamTask`], [`Session`],
/// and [`StdinFile`] each abandon themselves best-effort on drop.
struct SessionRun {
    session:           Session,
    command_id:        String,
    initial_exit_code: Option<i32>,
    stdin_file:        Option<StdinFile>,
    /// Forwards streamed stdin; idle for fixed or absent stdin.
    stdin:             InputPump,
    stream:            StreamTask,
    output:            Arc<Mutex<SessionOutput>>,
    /// A failing sink cancels the execution (the core contract); the
    /// token routes the failure into the wait loop.
    sink_failed:       CancellationToken,
}

impl SessionRun {
    /// Uploads the stdin file, starts the command in a fresh session, and
    /// spawns the log stream and the input pump.
    async fn start(
        transport: &DaytonaTransport,
        spec: &ExecSpec,
        controls: &ExecControls,
    ) -> Result<Self> {
        let sandbox = toolbox::sandbox(&transport.client, &transport.sandbox_id).await?;

        // A stream wins over fixed bytes (the control-plane contract).
        let streamed = controls
            .stdin
            .is_some()
            .then(|| controls.stdin_reader(spec))
            .flatten();
        let mut stdin_file = match (&streamed, &spec.stdin) {
            (None, Some(bytes)) => {
                Some(StdinFile::create(&transport.client, &transport.sandbox_id, bytes).await?)
            }
            _ => None,
        };

        let mut command = exec_line(&spec.launch_env(), &spec.program, &spec.args);
        if streamed.is_some() {
            command = streamed_stdin::decoded_command(&command);
        } else if let Some(file) = &stdin_file {
            command.push_str(" < ");
            command.push_str(&shell_quote(&file.path));
        }
        let cwd = transport.resolve_dir(spec.working_dir.as_deref());

        let (session, started) =
            match Session::start_command(&transport.client, &sandbox, &cwd, &command).await {
                Ok(started) => started,
                Err(error) => {
                    close_stdin(&mut stdin_file).await;
                    return Err(error);
                }
            };
        let sink_failed = CancellationToken::new();
        let mut run = Self {
            session,
            command_id: started.cmd_id,
            initial_exit_code: started.exit_code,
            stdin_file,
            stdin: InputPump::default(),
            stream: StreamTask::default(),
            output: Arc::new(Mutex::new(SessionOutput::new(
                spec,
                controls,
                sink_failed.clone(),
            ))),
            sink_failed,
        };

        // The stream task needs its own service and 'static state.
        let stream_process = match toolbox::process_of(&sandbox).await {
            Ok(process) => process,
            Err(error) => return Err(run.abort(error).await),
        };
        run.stream = StreamTask::spawn(
            stream_process,
            run.session.id(),
            &run.command_id,
            Arc::clone(&run.output),
        );
        if let Some(reader) = streamed {
            let input_process = match toolbox::process_of(&sandbox).await {
                Ok(process) => process,
                Err(error) => return Err(run.abort(error).await),
            };
            run.stdin = InputPump::spawn(input_process, run.session.id(), &run.command_id, reader);
        }
        Ok(run)
    }

    /// Polls the command to its end, the deadline, a stop request, or a
    /// sink failure. Input that could not be delivered ends the wait with
    /// its error: the command would otherwise wait for it forever.
    async fn wait(
        &mut self,
        timeout: Option<Duration>,
        stop_requested: impl Future<Output = StopLevel>,
    ) -> Result<WaitOutcome> {
        let completion = wait_for_completion(
            &self.session,
            &self.command_id,
            self.initial_exit_code,
            timeout,
            stop_requested,
            self.sink_failed.clone(),
        );
        tokio::select! {
            outcome = completion => outcome,
            error = self.stdin.failed() => Err(error),
        }
    }

    /// Ends everything the run owns and hands back the error that ended
    /// it: the stream task and input pump are aborted and awaited, the
    /// session deleted (best-effort, bounded), and the stdin file removed.
    async fn abort(mut self, error: Error) -> Error {
        self.stdin.stop().await;
        self.stream.stop().await;
        self.session.close().await;
        close_stdin(&mut self.stdin_file).await;
        error
    }

    /// Closes the session, drains the stream, and fetches the final logs
    /// when the stream did not deliver everything. Only a confirmed
    /// deletion settles the run; a refused one is [`Error::Incomplete`]
    /// saying what was and was not confirmed.
    async fn settle(mut self, mut outcome: WaitOutcome) -> Result<Settled> {
        self.stdin.stop().await;
        // A non-natural end must actively kill the command: deleting
        // the session terminates it and closes the log stream.
        if outcome.termination != Termination::Exited && !self.session.close().await {
            return Err(self
                .abort(Error::Incomplete(IncompleteOperation::new(
                    "Daytona session termination",
                )))
                .await);
        }

        let stream_clean = self.stream.drain(STREAM_DRAIN_GRACE).await;
        if !stream_clean {
            if outcome.termination == Termination::Exited {
                tracing::warn!("command log stream ended before a natural exit was drained");
            } else {
                tracing::debug!("command log stream ended during command cancellation");
            }
        }

        let final_logs = match outcome.final_logs.take() {
            Some(logs) => Some(logs),
            None if !stream_clean => self.session.fetch_logs(&self.command_id).await,
            None => None,
        };
        let cleanup_confirmed = self.session.close().await;
        close_stdin(&mut self.stdin_file).await;
        if !cleanup_confirmed {
            let mut incomplete = IncompleteOperation::new("Daytona session cleanup");
            incomplete.termination_confirmed = outcome.termination == Termination::Exited;
            incomplete.output_abandoned = !stream_clean;
            return Err(Error::Incomplete(incomplete));
        }
        Ok(Settled {
            outcome,
            stream_clean,
            final_logs,
            output: self.output,
        })
    }
}

/// A run whose session deletion was confirmed: what the wait decided,
/// whether the stream closed on its own, and the final log fetch when
/// the stream did not deliver everything.
struct Settled {
    outcome:      WaitOutcome,
    stream_clean: bool,
    final_logs:   Option<SessionCommandLogsResult>,
    output:       Arc<Mutex<SessionOutput>>,
}

impl Settled {
    /// Appends what the stream missed, flushes the sanitizers, and
    /// assembles the result.
    async fn into_result(self, started: Instant) -> ExecStreamingResult {
        let mut output = self.output.lock().await;
        // The stream and the final fetch overlap arbitrarily; append
        // only what the stream missed, to the buffers and the sink.
        let mut logs_separated = false;
        if let Some(logs) = &self.final_logs {
            logs_separated = logs.streams_separated;
            output.append_final_logs(logs).await;
        }
        let [stdout, stderr] = output.finish().await;
        let (stdout_bytes, stdout_stats) = stdout.into_parts();
        let (stderr_bytes, stderr_stats) = stderr.into_parts();

        let sink_failed = output.sink_failed.is_cancelled();
        let termination = if sink_failed {
            Termination::Cancelled
        } else {
            self.outcome.termination
        };
        // Exit codes are meaningful only for natural exits.
        let exit_code = (termination == Termination::Exited)
            .then_some(self.outcome.exit_code)
            .flatten();
        let mut result = ExecResult::from_shell_status(termination, exit_code, started.elapsed());
        result.stdout = stdout_bytes;
        result.stderr = stderr_bytes;
        let mut streaming = ExecStreamingResult::new(result);
        streaming.live_streaming = self.stream_clean || output.saw_live;
        streaming.streams_separated = self.stream_clean || logs_separated;
        streaming.stdout_capture = stdout_stats;
        streaming.stderr_capture = stderr_stats;
        if (!self.stream_clean && self.final_logs.is_none()) || sink_failed {
            streaming.stdout_capture.truncated = true;
            streaming.stderr_capture.truncated = true;
        }
        streaming
    }
}

/// The log-stream task, owned so it is always ended explicitly: dropping
/// a bare `JoinHandle` would only detach the task and leave it running.
#[derive(Default)]
struct StreamTask(Option<JoinHandle<StdResult<(), DaytonaError>>>);

impl StreamTask {
    fn spawn(
        process: ProcessService,
        session_id: &str,
        command_id: &str,
        output: Arc<Mutex<SessionOutput>>,
    ) -> Self {
        let session_id = session_id.to_owned();
        let command_id = command_id.to_owned();
        Self(Some(tokio::spawn(async move {
            let stdout = Arc::clone(&output);
            let stderr = output;
            process
                .get_session_command_logs_stream(
                    &session_id,
                    &command_id,
                    move |chunk| {
                        let output = Arc::clone(&stdout);
                        async move {
                            output
                                .lock()
                                .await
                                .deliver(OutputStream::Stdout, chunk)
                                .await
                        }
                    },
                    move |chunk| {
                        let output = Arc::clone(&stderr);
                        async move {
                            output
                                .lock()
                                .await
                                .deliver(OutputStream::Stderr, chunk)
                                .await
                        }
                    },
                )
                .await
        })))
    }

    /// Waits up to `grace` for the stream to close on its own, aborting
    /// one that will not end. True when it closed cleanly.
    async fn drain(&mut self, grace: Duration) -> bool {
        let Some(mut task) = self.0.take() else {
            return false;
        };
        match time::timeout(grace, &mut task).await {
            Ok(Ok(Ok(()))) => true,
            Ok(_) => false,
            Err(_elapsed) => {
                task.abort();
                let _ = task.await;
                false
            }
        }
    }

    /// Aborts the task and waits until it is gone, so nothing it was
    /// holding is touched while it still runs.
    async fn stop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for StreamTask {
    /// Abandonment fallback for a run dropped mid-await: the task is
    /// aborted rather than left following a stream nobody reads.
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// One side (stdout or stderr) of the session's output.
struct Side {
    /// The provider's raw bytes, for matching the final log fetch
    /// against what the stream delivered. Deduplication needs its own
    /// minimum overlap window, whatever the caller retains.
    raw_seen:  OutputCaptureBuffer,
    /// Sanitized bytes at the caller's retention limit.
    seen:      OutputCaptureBuffer,
    sanitizer: OutputSanitizer,
}

/// Everything the session's output passes through, both streams, owned
/// once. The stream task delivers live chunks; after the run settles,
/// the final log fetch appends what the stream missed and the
/// sanitizers flush.
struct SessionOutput {
    sides:       [Side; 2],
    saw_live:    bool,
    sink:        Option<OutputSink>,
    sink_failed: CancellationToken,
}

impl SessionOutput {
    fn new(spec: &ExecSpec, controls: &ExecControls, sink_failed: CancellationToken) -> Self {
        Self {
            sides: [OutputStream::Stdout, OutputStream::Stderr].map(|_| Side {
                raw_seen:  dedup_capture(controls.retained_output_limit),
                seen:      OutputCaptureBuffer::new(controls.retained_output_limit),
                sanitizer: OutputSanitizer::new(spec.output_sanitization),
            }),
            saw_live: false,
            sink: controls.sink.clone(),
            sink_failed,
        }
    }

    /// One live chunk from the log stream. A failing sink cancels the
    /// execution via the wait loop; ending the stream here stops further
    /// deliveries immediately.
    async fn deliver(
        &mut self,
        stream: OutputStream,
        chunk: String,
    ) -> StdResult<(), DaytonaError> {
        let bytes = chunk.into_bytes();
        if bytes.is_empty() {
            return Ok(());
        }
        self.saw_live = true;
        let side = &mut self.sides[stream as usize];
        side.raw_seen.push(&bytes);
        let bytes = side.sanitizer.push(&bytes);
        side.seen.push(&bytes);
        if bytes.is_empty() {
            return Ok(());
        }
        if let Some(sink) = &self.sink {
            if sink(stream, bytes).await.is_err() {
                self.sink_failed.cancel();
                return Err(DaytonaError::general("output sink failed"));
            }
        }
        Ok(())
    }

    /// Appends the bytes of the final log fetch that the stream never
    /// delivered, to the buffers and the sink.
    async fn append_final_logs(&mut self, logs: &SessionCommandLogsResult) {
        for (stream, bytes) in [
            (OutputStream::Stdout, logs.stdout.as_bytes()),
            (OutputStream::Stderr, logs.stderr.as_bytes()),
        ] {
            let side = &mut self.sides[stream as usize];
            let missing = missing_suffix(&mut side.raw_seen, bytes);
            side.raw_seen.push(&missing);
            let sanitized = side.sanitizer.push(&missing);
            side.seen.push(&sanitized);
            self.emit(stream, sanitized).await;
        }
    }

    /// Flushes the sanitizers and takes both captures.
    async fn finish(&mut self) -> [OutputCaptureBuffer; 2] {
        for stream in [OutputStream::Stdout, OutputStream::Stderr] {
            let side = &mut self.sides[stream as usize];
            let final_bytes = side.sanitizer.finish();
            side.seen.push(&final_bytes);
            self.emit(stream, final_bytes).await;
        }
        [OutputStream::Stdout, OutputStream::Stderr].map(|stream| {
            mem::replace(
                &mut self.sides[stream as usize].seen,
                OutputCaptureBuffer::new(None),
            )
        })
    }

    /// Delivers already-captured bytes once the live stream is over. A
    /// failing sink marks the run cancelled, like a failure mid-stream.
    async fn emit(&self, stream: OutputStream, bytes: Vec<u8>) {
        if bytes.is_empty() || self.sink_failed.is_cancelled() {
            return;
        }
        if let Some(sink) = &self.sink {
            if sink(stream, bytes).await.is_err() {
                self.sink_failed.cancel();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Mutex as StdMutex;

    use super::*;
    use crate::fake_daytona::{FakeDaytona, Reply, Script, sandbox_json};

    #[test]
    fn wire_timeout_rounds_sub_second_up() {
        assert_eq!(
            wire_timeout(Some(Duration::from_millis(300))),
            Duration::from_secs(1)
        );
        assert_eq!(
            wire_timeout(Some(Duration::from_secs(10))),
            Duration::from_secs(10)
        );
        assert_eq!(wire_timeout(None), UNBOUNDED_TIMEOUT);
    }

    #[test]
    fn compose_unsets_the_ambient_bash_env_before_the_spec_env() {
        let spec = ExecSpec::bash("true");
        let program = DaytonaTransport::compose(&spec, None);
        // The helper's blank travels like any other spec variable.
        assert_eq!(
            program,
            "unset BASH_ENV\nexec env 'BASH_ENV=' 'bash' '-c' 'true'"
        );
    }

    #[test]
    fn compose_quotes_env_assignments_whole() {
        let spec = ExecSpec::new("true")
            .env_var("X;injected", "a b")
            .env_var("INPUT_INCLUDE-HIDDEN-FILES", "true");
        let program = DaytonaTransport::compose(&spec, None);
        // A metacharacter in a key corrupts nothing: the assignment is one
        // quoted word to `env`, which also admits names that are not shell
        // identifiers.
        assert!(
            program.ends_with("exec env 'INPUT_INCLUDE-HIDDEN-FILES=true' 'X;injected=a b' 'true'"),
            "{program}"
        );
    }

    #[test]
    fn compose_keeps_every_argument_literal() {
        let spec = ExecSpec::new("printf").args(["%s", "$HOME; rm -rf /", "it's"]);
        let program = DaytonaTransport::compose(&spec, None);
        assert!(
            program.ends_with("exec env 'printf' '%s' '$HOME; rm -rf /' 'it'\\''s'"),
            "{program}"
        );
    }

    #[test]
    fn compose_redirects_stdin_from_the_temp_file() {
        let spec = ExecSpec::new("wc").arg("-c");
        let program = DaytonaTransport::compose(&spec, Some("/tmp/.sandbox-driver-stdin-1-2"));
        assert!(
            program.ends_with("exec env 'wc' '-c' < '/tmp/.sandbox-driver-stdin-1-2'"),
            "{program}"
        );
    }

    #[test]
    fn untimed_specs_send_the_unbounded_timeout_explicitly() {
        // An omitted field would let the toolbox kill the command at its
        // 10-second default.
        assert_eq!(wire_timeout(None), UNBOUNDED_TIMEOUT);
        assert_eq!(
            wire_timeout(Some(Duration::from_secs(30))),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn unbounded_timeout_survives_the_wire_conversion() {
        // The SDK converts with `as_secs() as i32`; a value past i32::MAX
        // would truncate into garbage.
        assert!(i32::try_from(UNBOUNDED_TIMEOUT.as_secs()).is_ok());
    }

    /// A sandbox whose command reports `exit_code` at start (`None` keeps
    /// the run polling), whose log stream is refused or held open, and
    /// whose session deletion answers `delete_status`.
    fn script(
        exit_code: Option<i32>,
        hold_stream: bool,
        delete_status: u16,
    ) -> impl FnOnce(SocketAddr) -> Script {
        move |address| {
            Arc::new(move |method: &str, target: &str| match (method, target) {
                ("GET", target) if target.starts_with("/sandbox/sb-1/toolbox-proxy-url") => {
                    Reply::Json(200, format!(r#"{{"url":"http://{address}/toolbox"}}"#))
                }
                ("GET", target) if target.starts_with("/sandbox/sb-1") => {
                    Reply::Json(200, sandbox_json("started"))
                }
                ("POST", "/toolbox/sb-1/process/session") => Reply::Json(201, "{}".to_owned()),
                ("POST", target) if target.ends_with("/exec") => Reply::Json(
                    200,
                    serde_json::json!({"cmdId": "cmd-1", "exitCode": exit_code}).to_string(),
                ),
                ("GET", target) if target.contains("/logs?follow=true") => {
                    if hold_stream {
                        Reply::Hang
                    } else {
                        Reply::Json(404, "{}".to_owned())
                    }
                }
                ("GET", target) if target.ends_with("/logs") => Reply::Json(
                    200,
                    r#"{"stdout":"hello","stderr":"","output":"hello"}"#.to_owned(),
                ),
                ("GET", target) if target.contains("/command/cmd-1") => {
                    Reply::Json(200, r#"{"command":"true","id":"cmd-1"}"#.to_owned())
                }
                ("DELETE", target) if target.starts_with("/toolbox/sb-1/process/session/") => {
                    Reply::Json(delete_status, "{}".to_owned())
                }
                _ => Reply::Json(404, "{}".to_owned()),
            })
        }
    }

    /// Buffered capture with a kill token, so the run takes the session
    /// transport rather than the one-shot endpoint.
    fn stoppable() -> ExecControls {
        ExecControls {
            kill: Some(CancellationToken::new()),
            ..ExecControls::buffered()
        }
    }

    #[tokio::test]
    async fn a_command_whose_stream_failed_completes_from_its_final_logs() {
        let fake = FakeDaytona::start(script(Some(0), false, 200)).await;
        let delivered = Arc::new(StdMutex::new(Vec::new()));
        let sink: OutputSink = Arc::new({
            let delivered = Arc::clone(&delivered);
            move |stream, bytes| {
                let delivered = Arc::clone(&delivered);
                Box::pin(async move {
                    delivered
                        .lock()
                        .expect("delivered lock")
                        .push((stream, bytes));
                    Ok(())
                })
            }
        });
        let result = fake
            .transport()
            .await
            .run_streaming(&ExecSpec::new("true"), ExecControls {
                sink: Some(sink),
                ..stoppable()
            })
            .await
            .expect("the command completed");
        assert_eq!(result.result.termination, Termination::Exited);
        assert_eq!(result.result.exit_code, Some(0));
        // The stream was refused, so everything came from the final fetch.
        assert_eq!(result.result.stdout, b"hello", "{:?}", fake.requests());
        assert!(!result.live_streaming);
        assert!(result.streams_separated);
        assert!(!result.stdout_capture.truncated);
        assert_eq!(*delivered.lock().expect("delivered lock"), [(
            OutputStream::Stdout,
            b"hello".to_vec()
        )]);
        assert_eq!(fake.deletes(), 1);
    }

    #[tokio::test]
    async fn a_refused_session_deletion_after_a_natural_exit_is_incomplete() {
        let fake = FakeDaytona::start(script(Some(0), false, 500)).await;
        let error = fake
            .transport()
            .await
            .run_streaming(&ExecSpec::new("true"), stoppable())
            .await
            .expect_err("the session could not be deleted");
        let Error::Incomplete(incomplete) = error else {
            panic!("expected an incomplete operation, got {error}");
        };
        assert_eq!(incomplete.operation, "Daytona session cleanup");
        assert!(
            incomplete.termination_confirmed,
            "the command exited on its own"
        );
        assert!(incomplete.output_abandoned, "the stream never delivered");
        assert!(!incomplete.cleanup_confirmed);
        assert_eq!(
            fake.deletes(),
            1,
            "a refused deletion is not retried on drop"
        );
    }

    #[tokio::test]
    async fn a_refused_session_deletion_after_a_stop_is_incomplete() {
        let fake = FakeDaytona::start(script(None, false, 500)).await;
        let kill = CancellationToken::new();
        kill.cancel();
        let error = fake
            .transport()
            .await
            .run_streaming(&ExecSpec::new("sleep").arg("60"), ExecControls {
                kill: Some(kill),
                ..ExecControls::buffered()
            })
            .await
            .expect_err("the killed command's session could not be deleted");
        let Error::Incomplete(incomplete) = error else {
            panic!("expected an incomplete operation, got {error}");
        };
        assert_eq!(incomplete.operation, "Daytona session termination");
        assert!(
            !incomplete.termination_confirmed,
            "only a confirmed deletion proves the command stopped"
        );
        assert!(incomplete.output_abandoned);
        assert!(!incomplete.cleanup_confirmed);
        assert_eq!(fake.deletes(), 1);
    }

    #[tokio::test]
    async fn an_abandoned_run_ends_its_stream_and_deletes_its_session() {
        let fake = FakeDaytona::start(script(None, true, 200)).await;
        let transport = Arc::new(fake.transport().await);
        let run = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .run_streaming(&ExecSpec::new("sleep").arg("60"), stoppable())
                    .await
            }
        });
        // The run is waiting: its stream is connected and held open, and
        // it has polled the command at least once.
        fake.saw("/logs?follow=true").await;
        fake.saw("/command/cmd-1").await;
        assert_eq!(fake.deletes(), 0);

        run.abort();
        assert!(run.await.expect_err("aborted").is_cancelled());

        // Dropping the run aborts the stream task (its connection closes)
        // and deletes the session best-effort.
        time::timeout(Duration::from_secs(5), fake.stream_hang_closed())
            .await
            .expect("the abandoned stream connection closed");
        fake.saw("DELETE /toolbox/sb-1/process/session/").await;
    }
}
