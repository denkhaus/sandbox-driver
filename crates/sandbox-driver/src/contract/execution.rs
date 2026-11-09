use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::Error;
use super::runtime::ByteStream;
use super::workload::{validate_argv, validate_env, validate_name};
use crate::OutputLoss;
use crate::redact::EnvKeys;

/// An argv executed in the requested workload, never in its outer runner.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecSpec {
    pub argv:              Vec<String>,
    #[serde(default)]
    pub env:               BTreeMap<String, String>,
    #[serde(default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub timeout_ms:        Option<u64>,
    #[serde(default)]
    pub tty:               bool,
}

impl ExecSpec {
    pub fn argv(args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            argv:              args.into_iter().map(Into::into).collect(),
            env:               BTreeMap::new(),
            working_directory: None,
            timeout_ms:        None,
            tty:               false,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        validate_argv("argv", &self.argv)?;
        if let Some(path) = &self.working_directory {
            validate_name("working_directory", path)?;
        }
        validate_env("env", &self.env)
    }
}

impl fmt::Debug for ExecSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            argv: _,
            env,
            working_directory,
            timeout_ms,
            tty,
        } = self;
        f.debug_struct("ExecSpec")
            .field("argv", &"<redacted>")
            .field("env_keys", &EnvKeys(env))
            .field("working_directory", working_directory)
            .field("timeout_ms", timeout_ms)
            .field("tty", tty)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
    Terminal,
}

/// Awaited in order for each chunk; failure requests bounded execution cleanup.
pub type OutputSink = Arc<
    dyn Fn(OutputStream, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send
        + Sync,
>;

/// Process-local streaming endpoints. Adapters carry these over authenticated
/// data connections, never JSON control. A dropped exec future must request
/// bounded provider-owned cleanup; it is not evidence of termination.
#[derive(Default)]
pub struct ExecControls {
    pub stdin:  Option<ByteStream>,
    pub sink:   Option<OutputSink>,
    pub term:   Option<CancellationToken>,
    pub kill:   Option<CancellationToken>,
    /// Latest size wins. Only meaningful when `ExecSpec.tty` is true.
    pub resize: Option<watch::Receiver<TerminalSize>>,
}

impl fmt::Debug for ExecControls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecControls")
            .field("stdin", &self.stdin.is_some())
            .field("sink", &self.sink.is_some())
            .field("term", &self.term.is_some())
            .field("kill", &self.kill.is_some())
            .field("resize", &self.resize.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalSize {
    pub rows:    NonZeroU16,
    pub columns: NonZeroU16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Termination {
    Exited,
    TimedOut,
    Terminated,
    Killed,
    Lost,
}

/// `Lost` means unknown execution outcome. It never establishes a stop fence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecOutcome {
    pub termination: Termination,
    pub exit_code:   Option<i32>,
    pub signal:      Option<i32>,
    #[serde(default)]
    pub output_loss: OutputLoss,
}
