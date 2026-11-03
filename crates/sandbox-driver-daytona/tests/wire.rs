//! Daytona served over the JSON-RPC protocol: the full conformance
//! suite — including the volume round trip and access-facet consistency
//! — must hold through the wire exactly as it holds in-process.
//!
//! Live test: requires `DAYTONA_API_KEY`; run with `mise run test:live`.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, process};

use sandbox_driver::{
    LogSink, LogSource, SandboxKind, SandboxProvider, SandboxSource, SandboxSpec, SnapshotId,
    SnapshotSource, SnapshotSpec, SnapshotState,
};
use sandbox_driver_conformance::{Conformance, SpecFactory};
use sandbox_driver_daytona::DaytonaProvider;
use sandbox_driver_protocol::{PluginProvider, serve};
use tokio::io::{duplex, split};
use tokio::time;

mod support;

use support::init_diagnostics;

const TEST_SNAPSHOT: &str = "daytona-medium";
const TEST_GIT_REPOSITORY: &str = "https://github.com/octocat/Hello-World.git";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn daytona_passes_conformance_over_the_wire() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect()
        .await
        .expect("connect with credentials");
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    tokio::spawn(serve(Arc::new(provider), plugin_read, plugin_write));
    let remote = PluginProvider::connect(host_read, host_write)
        .await
        .expect("handshake");

    let specs = SpecFactory::new(default_spec)
        .with_git_clone_url(TEST_GIT_REPOSITORY)
        .with_entrypoint_logs(entrypoint_logs_spec);
    let mut conformance = Conformance::new(Arc::new(remote), specs);
    conformance.check_timeout = Duration::from_secs(900);
    conformance.wait.deadline = Some(Duration::from_secs(300));
    let report = conformance.run().await;
    report.assert_pass();
}

fn default_spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .sandbox_kind(SandboxKind::Container)
    .working_directory("/home/daytona/sandbox-driver-conformance")
    .ephemeral(true)
}

fn entrypoint_logs_spec() -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Dockerfile {
        content: r#"FROM debian:stable-slim
ENTRYPOINT ["/bin/sh", "-c", "echo conformance-entrypoint; echo conformance-entrypoint-error >&2; exec sleep 600"]
"#
        .to_owned(),
    })
    .sandbox_kind(SandboxKind::Container)
    .ephemeral(true)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn extended_daytona_streams_and_browser_access_cross_the_wire() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect()
        .await
        .expect("connect with credentials");
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    tokio::spawn(serve(Arc::new(provider), plugin_read, plugin_write));
    let remote = PluginProvider::connect(host_read, host_write)
        .await
        .expect("handshake");

    let snapshots = remote.snapshots().expect("snapshot service declared");
    let name = format!("sd-wire-logs-{}", process::id());
    let mut snapshot_spec = SnapshotSpec::new(SnapshotSource::Dockerfile {
        content: r#"FROM debian:stable-slim
ENTRYPOINT ["/bin/sh", "-c", "echo wire-entrypoint; echo wire-entrypoint-error >&2; sleep 2"]
"#
        .to_owned(),
    });
    snapshot_spec.name = Some(name.clone());
    snapshot_spec.sandbox_kind = Some(SandboxKind::Container);
    snapshot_spec.region = Some("us".to_owned());
    snapshot_spec.resources.cpu_cores = Some(1);
    snapshot_spec.resources.memory_mb = Some(1024);
    snapshot_spec.resources.disk_mb = Some(1024);
    let snapshot_id = snapshots
        .create(&snapshot_spec, None)
        .await
        .expect("create Dockerfile snapshot over wire");

    let outcome = async {
        let build = Arc::new(Mutex::new(Vec::new()));
        snapshots
            .build_logs(&snapshot_id, true, collecting_sink(Arc::clone(&build)))
            .await
            .map_err(|error| format!("snapshot build logs: {error}"))?;
        if build.lock().expect("build output lock").is_empty() {
            return Err("snapshot build logs were empty".to_owned());
        }
        wait_for_snapshot(snapshots, &snapshot_id).await?;

        let sandbox = remote
            .create(
                &SandboxSpec::new(SandboxSource::Snapshot {
                    id: SnapshotId::try_new(name).expect("valid snapshot id"),
                })
                .sandbox_kind(SandboxKind::Container)
                .region("us")
                .ephemeral(true),
                None,
            )
            .await
            .map_err(|error| format!("create log sandbox: {error}"))?;
        let log_outcome = async {
            let terminal = sandbox
                .web_terminal()
                .ok_or("web terminal facet missing")?
                .web_terminal_url()
                .await
                .map_err(|error| format!("web terminal: {error}"))?;
            if !terminal.starts_with("https://") {
                return Err(format!("web terminal URL looks wrong: {terminal}"));
            }
            let entrypoint = Arc::new(Mutex::new(Vec::new()));
            time::timeout(
                Duration::from_secs(60),
                sandbox.logs().ok_or("logs facet missing")?.follow(
                    LogSource::Entrypoint,
                    collecting_sink(Arc::clone(&entrypoint)),
                ),
            )
            .await
            .map_err(|_| "entrypoint logs timed out".to_owned())?
            .map_err(|error| format!("entrypoint logs: {error}"))?;
            let output = entrypoint.lock().expect("entrypoint output lock");
            let output = String::from_utf8_lossy(&output);
            if !output.contains("wire-entrypoint") || !output.contains("wire-entrypoint-error") {
                return Err(format!("entrypoint logs missing markers: {output:?}"));
            }
            Ok(())
        }
        .await;
        let _ = sandbox.delete().await;
        log_outcome?;

        let desktop = remote
            .create(
                &SandboxSpec::new(SandboxSource::Snapshot {
                    id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
                })
                .sandbox_kind(SandboxKind::Container)
                .ephemeral(true),
                None,
            )
            .await
            .map_err(|error| format!("create VNC sandbox: {error}"))?;
        let vnc = match desktop.vnc() {
            Some(vnc) => vnc
                .vnc_connection()
                .await
                .map_err(|error| format!("VNC: {error}")),
            None => Err("VNC facet missing".to_owned()),
        };
        let _ = desktop.delete().await;
        let vnc = vnc?;
        if !vnc.url.contains("/vnc.html") {
            return Err(format!("VNC URL looks wrong: {}", vnc.url));
        }
        Ok(())
    }
    .await;

    snapshots
        .delete(&snapshot_id, None)
        .await
        .expect("delete wire snapshot");
    remote.shutdown().await.expect("shutdown");
    outcome.expect("extended Daytona facets over wire");
}

fn collecting_sink(output: Arc<Mutex<Vec<u8>>>) -> LogSink {
    Arc::new(move |chunk| {
        let output = Arc::clone(&output);
        Box::pin(async move {
            output.lock().expect("log output lock").extend(chunk);
            Ok(())
        })
    })
}

async fn wait_for_snapshot(
    snapshots: &dyn sandbox_driver::SnapshotProvider,
    id: &sandbox_driver::SnapshotId,
) -> Result<(), String> {
    for _ in 0..120 {
        let status = snapshots
            .get(id)
            .await
            .map_err(|error| format!("snapshot get: {error}"))?;
        match status.state {
            SnapshotState::Active => return Ok(()),
            SnapshotState::Error => {
                return Err(format!("snapshot failed: {:?}", status.error_reason));
            }
            _ => time::sleep(Duration::from_secs(5)).await,
        }
    }
    Err("snapshot did not become active".to_owned())
}
