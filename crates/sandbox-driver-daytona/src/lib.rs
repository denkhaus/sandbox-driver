//! Daytona cloud sandbox provider.
//!
//! A supported library for in-process embedding, and the implementation
//! behind the same-named plugin executable. An application either links
//! this crate and constructs the provider directly, or launches the
//! executable and reaches it through `sandbox-driver-protocol`. Both
//! present the same trait family.
//!
//! VM-backed sandboxes (`Isolation::Vm`) through the Daytona control
//! plane and per-sandbox toolbox daemon, with snapshots and volumes as
//! first-class services and preview-URL/SSH access facets.
//!
//! Lifecycle: archive, undelete (Daytona's "recover" — restore
//! within 24 hours of deletion), refresh-activity, all five timers (TTL
//! and auto-pause included), labels, runtime network updates, and — on
//! VM sandbox classes, narrowed per sandbox — pause/resume, fork, and
//! sandbox-to-snapshot with filesystem and live-process-state modes. The
//! PTY facet rides the toolbox
//! WebSocket; `spawn_stdio` rides command sessions and is UTF-8-only
//! (see the stdio module). The outbound proxy is `provider_config`
//! (`{"outbound_proxy_url": …}`), composing with a domain allow list as
//! upstream intends rather than competing as a network policy. Plain execs run
//! buffered through the toolbox's one-shot endpoint; a sink or stop token
//! routes through a command session, which streams logs live with
//! separated stdout/stderr, kills on stop/timeout by deleting the
//! session, and preserves partial output on timeout. Stdin is delivered
//! through a temp-file redirection inside the sandbox on both paths.
//! Both exec paths encode output before it reaches the toolbox and decode
//! it before delivery, preserving arbitrary bytes and separate streams.
//! Git follows Fabro's hybrid path: clone uses the native toolbox API;
//! worktree and remote operations use the shared exec-derived implementation.
//! Daytona derives Search and background-service management through exec.
//! Snapshots used with these facets must therefore provide the commands
//! documented by [`sandbox_driver::Search`] and [`sandbox_driver::Services`],
//! plus `git`, on `PATH`.
//! Resize remains in the normalized interface, but the current hosted
//! Daytona API and official SDK do not expose a working resize route, so
//! this provider does not declare it.
//!
//! # Failed creation
//!
//! Once Daytona returns a sandbox ID, startup observation is bounded. A
//! failure during startup or provisioning deletes the sandbox, repeating the
//! request only while Daytona rejects it for an in-progress state change, and
//! waits, within a five-minute cleanup deadline, for removal to be confirmed.
//! An accepted delete or a `destroying` status alone does not confirm cleanup.
//! Unconfirmed cleanup returns a legacy [`sandbox_driver::ProviderError`] with
//! code `incomplete_cleanup`, the known sandbox ID in `detail.remaining`, and
//! the cleanup diagnostic in `detail.cleanup`; the original creation failure
//! stays in its source chain. This is not permission to replay the create.
//!
//! # Lifecycle timers
//!
//! Unset timers inherit Daytona's server defaults — notably auto-stop
//! after **15 idle minutes**, which is shorter than a single long
//! inference call. Callers that run long commands should set
//! `timers.auto_stop_after_idle` explicitly. `Duration::ZERO` is the
//! explicit "never", encoded per timer: wire `0` disables auto-stop;
//! auto-delete crosses as `-1` (its wire `0` means delete immediately
//! on stop and is reserved for the ephemeral flag); auto-archive
//! crosses as `0`, which Daytona reads as "the maximum interval" — the
//! closest the API comes to disabling it.
//!
//! # Configuration
//!
//! [`DaytonaProvider::connect`] uses the SDK's environment configuration:
//! `DAYTONA_API_KEY` (or `DAYTONA_JWT_TOKEN` + `DAYTONA_ORGANIZATION_ID`),
//! optional `DAYTONA_API_URL` and `DAYTONA_TARGET`.
//! [`DaytonaProvider::connect_with_config`] accepts the same values explicitly
//! and falls back to the environment for anything left unset.
//! [`DaytonaProvider::connect_explicit`] never reads the environment, so an
//! embedding application that resolves credentials itself can be sure the
//! worker's environment cannot substitute for them.

mod access;
mod cleanup;
mod encoded_exec;
mod exec;
#[cfg(test)]
mod fake_daytona;
mod fs;
mod git;
mod labels;
mod logs;
mod nested;
mod provider;
mod pty;
mod sandbox;
mod sdk;
mod session;
mod shell;
mod snapshots;
mod stdio;
mod streamed_stdin;
mod toolbox;
mod volumes;

use std::time::Duration;

pub use daytona_sdk::DaytonaConfig;
pub use sandbox_driver_daytona_config::{
    DaytonaProviderConfig, DockerExecutionTarget, NestedDockerConfig,
};

pub use crate::access::DaytonaAccess;
pub use crate::exec::DaytonaExec;
pub use crate::fs::DaytonaFs;
pub use crate::git::DaytonaGit;
pub use crate::logs::DaytonaLogs;
pub use crate::provider::DaytonaProvider;
pub use crate::pty::DaytonaPty;
pub use crate::sandbox::DaytonaSandbox;

pub(crate) const FALLBACK_WORKING_DIR: &str = "/home/daytona";
pub(crate) const RUNTIME_DIRECTORY_PARENT: &str = "/tmp/sandbox-driver";
pub(crate) const RUNTIME_DIRECTORY: &str = "/tmp/sandbox-driver/runtime";

/// Resolves `path` against the sandbox working directory: an absolute
/// path stands as given, a relative one is joined onto the working
/// directory. Every facet applies this same rule, so the toolbox daemon
/// never resolves a relative path against its own cwd.
pub(crate) fn resolve_path(working_dir: &str, path: &str) -> String {
    if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("{}/{}", working_dir.trim_end_matches('/'), path)
    }
}
pub(crate) const CREATE_TIMEOUT: Duration = Duration::from_secs(600);
/// Dockerfile sources build the image during create; real builds exceed
/// shorter budgets (fabro-sandbox landed on 30 minutes).
pub(crate) const DOCKERFILE_CREATE_TIMEOUT: Duration = Duration::from_secs(1800);
pub(crate) const CREATE_POLL: Duration = Duration::from_secs(2);
/// Upper bound on cleanup calls (deletes) so a stalled REST call cannot
/// block cancellation or failure paths indefinitely.
pub(crate) const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
/// Budget for confirming that a failed create's sandbox is gone. Live
/// destroys have taken over two minutes; this stays small beside the
/// create budgets.
pub(crate) const FAILED_CREATE_CLEANUP_BUDGET: Duration = Duration::from_secs(300);
/// Budget for waiting out an in-flight Daytona lifecycle transition
/// (for example an auto-stop racing a reactivation).
pub(crate) const TRANSITION_BUDGET: Duration = Duration::from_secs(120);
pub(crate) const TRANSITION_POLL: Duration = Duration::from_secs(1);
pub(crate) const SNAPSHOT_ACTIVATE_BUDGET: Duration = Duration::from_secs(900);
pub(crate) const SNAPSHOT_ACTIVATE_POLL: Duration = Duration::from_secs(5);

/// Items requested per page when listing sandboxes or snapshots. Sandbox
/// listing follows cursors through the SDK; the snapshot endpoint is still
/// page-numbered and truncates an unpaged request to its default page size,
/// so snapshot listing walks `total_pages` explicitly.
pub(crate) const LIST_PAGE_SIZE: i32 = 100;

#[cfg(test)]
mod tests {
    use super::resolve_path;

    #[test]
    fn relative_paths_join_the_working_directory_and_absolute_paths_stand() {
        assert_eq!(
            resolve_path("/workspace/", "src/main.rs"),
            "/workspace/src/main.rs"
        );
        assert_eq!(resolve_path("/workspace", "src"), "/workspace/src");
        assert_eq!(resolve_path("/workspace", "/etc/hosts"), "/etc/hosts");
    }
}
