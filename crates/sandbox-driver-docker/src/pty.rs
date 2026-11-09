use std::collections::BTreeMap;
use std::pin::Pin;
use std::process;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bollard::container::LogOutput;
use bollard::errors::Error as DockerApiError;
use bollard::exec::{CreateExecOptions, ResizeExecOptions};
use futures_util::{Stream, StreamExt};
use sandbox_driver::{BASH_ENV_VAR, Error, Pty, PtyOptions, PtySession, PtySize, Result};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::container::{AttachedExec, ContainerRef};
use crate::daemon::{docker_error, shell_quote};

const DEFAULT_TERM: &str = "xterm-256color";
const DEFAULT_LANG: &str = "C.UTF-8";

type DockerInput = Pin<Box<dyn AsyncWrite + Send>>;
type DockerOutput = Pin<Box<dyn Stream<Item = StdResult<LogOutput, DockerApiError>> + Send>>;

/// Interactive terminal access for one Docker container.
pub(crate) struct DockerPty {
    container: ContainerRef,
    counter:   AtomicU64,
}

impl DockerPty {
    pub(crate) fn new(container: ContainerRef) -> Self {
        Self {
            container,
            counter: AtomicU64::new(0),
        }
    }

    fn pid_file(&self) -> String {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        format!(
            "/tmp/.sandbox-driver/pty-{}-{nonce}-{count}.pid",
            process::id()
        )
    }
}

#[async_trait]
impl Pty for DockerPty {
    #[tracing::instrument(
        skip_all,
        fields(
            provider_kind = "docker",
            sandbox_id = %self.container.id,
            rows = options.size.rows,
            cols = options.size.cols,
            env_count = options.env.len()
        ),
        err
    )]
    async fn open(&self, options: &PtyOptions) -> Result<Box<dyn PtySession>> {
        let pid_file = self.pid_file();
        let working_dir = self.container.resolve_dir(options.working_dir.as_deref());
        let create = terminal_exec_options(&working_dir, &pid_file, &options.env);
        let AttachedExec {
            id: exec_id,
            output,
            input,
        } = self.container.start_attached(create, "pty exec").await?;
        let session = DockerPtySession {
            container: self.container.clone(),
            exec_id,
            pid_file,
            input: Mutex::new(Some(input)),
            output: Mutex::new(Some(output)),
            closed: AtomicBool::new(false),
        };
        if let Err(error) = session.resize(options.size).await {
            if let Err(close_error) = session.close().await {
                tracing::warn!(error = %close_error, "failed to close pty after resize failure");
            }
            return Err(error);
        }
        Ok(Box::new(session))
    }
}

struct DockerPtySession {
    container: ContainerRef,
    exec_id:   String,
    pid_file:  String,
    input:     Mutex<Option<DockerInput>>,
    output:    Mutex<Option<DockerOutput>>,
    closed:    AtomicBool,
}

impl DockerPtySession {
    async fn kill_shell(&self) -> Result<()> {
        let pid_file = shell_quote(&self.pid_file);
        // TERM the shell's process group (falling back to the pid), wait
        // briefly, then KILL what remains — a shell that ignores TERM
        // must still die. Children in their own job-control groups get
        // the kernel's HUP when the shell and its TTY go away, matching
        // a real terminal close.
        let command = format!(
            "if [ -f {pid_file} ]; then \
             pid=$(cat {pid_file}); rm -f {pid_file}; \
             case \"$pid\" in ''|*[!0-9]*) : ;; *) \
             kill -TERM -- \"-$pid\" 2>/dev/null || kill -TERM -- \"$pid\" 2>/dev/null || true; \
             for _ in 1 2 3 4 5; do \
             kill -0 \"$pid\" 2>/dev/null || break; sleep 0.2; done; \
             if kill -0 \"$pid\" 2>/dev/null; then \
             kill -KILL -- \"-$pid\" 2>/dev/null || true; \
             kill -KILL -- \"$pid\" 2>/dev/null || true; fi ;; \
             esac; fi"
        );
        let options = CreateExecOptions {
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            tty: Some(false),
            // Bash, not sh: dash's `kill` builtin rejects the `--`
            // separator the group kills need ("Illegal number: -"),
            // and a PTY session is an interactive bash already.
            cmd: Some(vec!["bash".to_owned(), "-c".to_owned(), command]),
            working_dir: Some("/".to_owned()),
            env: Some(vec![format!("{BASH_ENV_VAR}=")]),
            ..Default::default()
        };
        let AttachedExec { mut output, .. } = self
            .container
            .start_control(options, "pty cleanup exec")
            .await?;
        while let Some(chunk) = output.next().await {
            chunk.map_err(|error| docker_error("reading pty cleanup output", error))?;
        }
        Ok(())
    }
}

#[async_trait]
impl PtySession for DockerPtySession {
    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", byte_count = bytes.len()),
        err
    )]
    async fn write_input(&self, bytes: &[u8]) -> Result<()> {
        let mut input = self.input.lock().await;
        let Some(input) = input.as_mut() else {
            return Ok(());
        };
        input
            .write_all(bytes)
            .await
            .map_err(|error| Error::io("writing pty input", error))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker"), err)]
    async fn read_output(&self) -> Result<Option<Vec<u8>>> {
        let mut output = self.output.lock().await;
        let Some(output) = output.as_mut() else {
            return Ok(None);
        };
        match output.next().await {
            Some(Ok(chunk)) => Ok(Some(chunk.into_bytes().to_vec())),
            Some(Err(error)) => Err(docker_error("reading pty output", error)),
            None => Ok(None),
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(provider_kind = "docker", rows = size.rows, cols = size.cols),
        err
    )]
    async fn resize(&self, size: PtySize) -> Result<()> {
        self.container
            .docker
            .resize_exec(&self.exec_id, ResizeExecOptions {
                height: size.rows,
                width:  size.cols,
            })
            .await
            .map_err(|error| docker_error("resizing pty exec", error))
    }

    #[tracing::instrument(skip_all, fields(provider_kind = "docker"), err)]
    async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _ = self.input.lock().await.take();
        let _ = self.output.lock().await.take();
        self.kill_shell().await
    }
}

fn terminal_exec_options(
    working_dir: &str,
    pid_file: &str,
    environment: &BTreeMap<String, String>,
) -> CreateExecOptions<String> {
    let pid_file = shell_quote(pid_file);
    let command = format!(
        "mkdir -p /tmp/.sandbox-driver; \
         printf '%s\\n' $$ > {pid_file}; \
         exec sh -l"
    );
    let mut environment = environment.clone();
    environment
        .entry("TERM".to_owned())
        .or_insert_with(|| DEFAULT_TERM.to_owned());
    environment
        .entry("LANG".to_owned())
        .or_insert_with(|| DEFAULT_LANG.to_owned());
    environment.insert(BASH_ENV_VAR.to_owned(), String::new());
    CreateExecOptions {
        attach_stdin: Some(true),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        tty: Some(true),
        cmd: Some(vec!["sh".to_owned(), "-lc".to_owned(), command]),
        working_dir: Some(working_dir.to_owned()),
        env: Some(
            environment
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect(),
        ),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_exec_options_attach_a_tty_and_apply_environment() {
        let environment = BTreeMap::from([
            ("MODE".to_owned(), "test".to_owned()),
            ("TERM".to_owned(), "screen-256color".to_owned()),
            (BASH_ENV_VAR.to_owned(), "/tmp/untrusted".to_owned()),
        ]);
        let options = terminal_exec_options(
            "/workspace/repo",
            "/tmp/.sandbox-driver/pty.pid",
            &environment,
        );

        assert_eq!(options.attach_stdin, Some(true));
        assert_eq!(options.attach_stdout, Some(true));
        assert_eq!(options.attach_stderr, Some(true));
        assert_eq!(options.tty, Some(true));
        assert_eq!(options.working_dir.as_deref(), Some("/workspace/repo"));
        assert_eq!(
            options.env,
            Some(vec![
                "BASH_ENV=".to_owned(),
                "LANG=C.UTF-8".to_owned(),
                "MODE=test".to_owned(),
                "TERM=screen-256color".to_owned(),
            ])
        );
        assert!(
            options
                .cmd
                .expect("terminal command")
                .join(" ")
                .contains("exec sh -l")
        );
    }
}
