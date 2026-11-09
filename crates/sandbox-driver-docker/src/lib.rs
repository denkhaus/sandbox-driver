//! Docker container sandbox provider.
//!
//! A supported library for in-process embedding, and the implementation
//! behind the same-named plugin executable. An application either links
//! this crate and constructs the provider directly, or launches the
//! executable and reaches it through `sandbox-driver-protocol`. Both
//! present the same trait family.
//!
//! Containers created from OCI images, with kernel-sharing container
//! isolation (`Isolation::Container`). The Docker socket is
//! host-root-equivalent, so this provider is host-trusted by definition.
//!
//! The container's data plane is hybrid: file content moves through
//! the daemon's archive API (streamed reads that stop at the requested
//! range, writes that carry their missing parents, both working on a
//! stopped container), while metadata operations stay exec-derived — so
//! `Capabilities::fs` still reports `native: false`. Every image must
//! provide `/bin/sh`, `env`, and `setsid` for the exec wrapper (kill
//! semantics need a separate session, and `env` carries the command's
//! environment past the shell; an image without either fails every exec
//! with a clear message). The exec-derived facets are Bash scripts, so an
//! image that serves them must also provide `bash` on `PATH` and a Linux
//! userland with `stat`, `find`, and `base64`. Because Docker advertises
//! the normalized Search, Git, and background-services facets, the image
//! must also provide the commands documented by [`sandbox_driver::Search`]
//! and [`sandbox_driver::Services`], plus `git`, on `PATH`. A program that
//! needs none of that — Petri's step runner, which execs, reads and writes
//! files, and runs one-shot containers — runs on any Linux image with a
//! POSIX userland, Alpine included.
//!
//! # The workspace
//!
//! The sandbox owns its workspace: the working directory is a Docker
//! volume created with the container and removed with it, unless the
//! caller's `provider_config.binds` mounts a host directory there. Every
//! [`sandbox_driver::OneShot`] container the sandbox runs mounts the same
//! volume at the same path and joins the sandbox container's network
//! namespace, so it sees the workspace, the sidecars, and the daemon host
//! exactly as the sandbox does. `stop` and `delete` end the sandbox's
//! one-shot containers first.
//!
//! # Runtime behavior
//!
//! Async on Tokio; the caller owns the runtime. Spawned tasks: stream
//! demux and stdin writers scoped to a running exec, and kill requests
//! on cancellation that run from `/` and fail loudly when the stop
//! cannot be requested. Docker itself is the
//! sandbox registry — handles re-attach by container id across process
//! restarts. Admissions and lifecycle changes coordinate through an unstarted,
//! daemon-owned claim container. Conflicting lifecycle calls return provider
//! code `busy`; cancellation or unknown daemon outcomes retain the claim for
//! explicit reconciliation. See `docs/docker-lifecycle.md` for the scope,
//! operation bounds, and migration limitations.

mod access;
mod config;
mod container;
mod create;
mod daemon;
mod exec;
mod fence;
mod forward;
mod fs;
mod image;
mod inspect;
mod one_shot;
mod output;
mod provider;
mod pty;
mod sandbox;
mod sidecars;

use sandbox_driver::{Capabilities, Isolation, OneShotCaps, PtyCaps};

pub use crate::config::{BindMount, DockerProviderConfig, Health, RegistryAuth, Sidecar};
pub use crate::exec::DockerExec;
pub use crate::one_shot::is_missing_platform;
pub use crate::provider::DockerProvider;
pub use crate::sandbox::DockerSandbox;

pub(crate) const MANAGED_LABEL: &str = "sh.sandbox-driver.managed";
pub(crate) const SIDECAR_NETWORK_LABEL: &str = "sh.sandbox-driver.sidecar-network";
pub(crate) const DEFAULT_WORKING_DIRECTORY: &str = "/workspace";
pub(crate) const RUNTIME_DIRECTORY_PARENT: &str = "/tmp/sandbox-driver";
pub(crate) const RUNTIME_DIRECTORY: &str = "/tmp/sandbox-driver/runtime";

/// `Some(items)` when there are any; Docker's optional list fields read
/// an empty list and an absent one the same way, so the absent form is
/// the cleaner request.
pub(crate) fn non_empty<T>(items: Vec<T>) -> Option<Vec<T>> {
    (!items.is_empty()).then_some(items)
}

/// Docker's capability set, also used by providers that compose a Docker
/// sandbox inside another resource. Reading it does not contact a daemon.
pub fn docker_capabilities() -> Capabilities {
    let mut caps = Capabilities::minimal(Isolation::Container);
    caps.lifecycle.pause = true;
    caps.exec.live_streaming = true;
    caps.exec.streams_separated = true;
    caps.exec.stdin = true;
    caps.exec.stdin_stream = true;
    caps.exec.stop = true;
    caps.exec.stdio_process = true;
    caps.exec.environment = true;
    let mut pty = PtyCaps::default();
    pty.resize = true;
    caps.pty = Some(pty);
    caps.access.shell_command = true;
    // A container port is reached through a forward the plugin opens on
    // its own machine; see `forward.rs`.
    caps.access.preview_urls = true;
    let mut one_shot = OneShotCaps::default();
    one_shot.build = true;
    caps.one_shot = Some(one_shot);
    caps.fs.native = false;
    caps.fs.upload = true;
    caps.fs.download = true;
    caps.fs.permissions = true;
    caps.search.supported = true;
    caps.git.supported = true;
    caps.services.supported = true;
    caps.network.allow_all = true;
    caps.network.block_all = true;
    caps
}
