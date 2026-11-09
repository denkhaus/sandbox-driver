//! The Docker sandbox's workspace: a volume the sandbox owns, shared with
//! its one-shot containers, and the archive-backed file operations Petri
//! relies on — over the plugin wire and on an Alpine image, which is the
//! smallest userland the image contract promises to serve.
//!
//! Requires a reachable Docker daemon; tests pass trivially without one,
//! as the conformance run does.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use std::{env, future, io, process};

use bollard::Docker;
use bollard::container::{
    Config, CreateContainerOptions, InspectContainerOptions, KillContainerOptions,
    ListContainersOptions, StartContainerOptions,
};
use sandbox_driver::{
    Error, ExecControls, ExecSpec, OneShotSpec, OutputStream, SandboxFilter, SandboxProvider,
    SandboxSource, SandboxSpec, Termination,
};
use sandbox_driver_docker::{BindMount, DockerProvider, DockerProviderConfig};
use sandbox_driver_protocol::{PluginProvider, serve};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, duplex, split};
use tokio::{fs, time};
use tokio_util::sync::CancellationToken;

const ALPINE: &str = "ghcr.io/fabro-sh/dhi-alpine-base:3.23-dev-2026-06-17";
const WORKSPACE: &str = "/workspace";

#[tokio::test(flavor = "multi_thread")]
async fn restarting_a_stopped_sandbox_sweeps_old_one_shots_but_live_start_preserves_them() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let docker = Docker::connect_with_local_defaults().expect("local Docker connection");
    let sandbox = provider
        .create(&alpine_spec("restart-one-shot"), None)
        .await
        .expect("create sandbox");
    let result = async {
        let lingering = docker
            .create_container(None::<CreateContainerOptions<String>>, Config {
                image: Some(ALPINE.to_owned()),
                cmd: Some(vec!["sleep".to_owned(), "600".to_owned()]),
                labels: Some(HashMap::from([(
                    "sh.sandbox-driver.one-shot".to_owned(),
                    sandbox.id().as_str().to_owned(),
                )])),
                ..Default::default()
            })
            .await?;
        docker
            .start_container(&lingering.id, None::<StartContainerOptions<String>>)
            .await?;
        sandbox.start().await?;
        let live = docker
            .inspect_container(&lingering.id, None::<InspectContainerOptions>)
            .await?;
        docker
            .kill_container(
                sandbox.id().as_str(),
                Some(KillContainerOptions { signal: "KILL" }),
            )
            .await?;
        sandbox.start().await?;
        let swept = !one_shot_containers(sandbox.id().as_str())
            .await
            .contains(&lingering.id);
        Ok::<_, Box<dyn StdError>>((live, swept))
    }
    .await;
    sandbox
        .delete()
        .await
        .expect("delete sandbox and one-shots after any error");
    let (live, swept) = result.expect("restart through the provider");
    assert_eq!(live.state.and_then(|state| state.running), Some(true));
    assert!(swept, "restarting a stopped sandbox left an old one-shot");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_shots_share_a_host_network_helpers_namespace_and_workspace() {
    let Some(provider) = over_the_wire().await else {
        return;
    };
    let mut spec = alpine_spec("host-network");
    spec.provider_config = DockerProviderConfig {
        host_network: true,
        ..Default::default()
    }
    .into_value();
    let sandbox = provider
        .create(&spec, None)
        .await
        .expect("create host-network helper");
    let result = async {
        let helper = sandbox
            .exec()
            .run(&ExecSpec::new("readlink").arg("/proc/self/ns/net"))
            .await?;
        let one_shot = sandbox
            .one_shot()
            .expect("one-shot facet")
            .run(
                &OneShotSpec::registry(ALPINE)
                    .entrypoint("sh")
                    .args(["-c", "echo shared > marker; readlink /proc/self/ns/net"]),
                ExecControls::buffered(),
            )
            .await?;
        let marker = sandbox.fs().read("marker").await?;
        Ok::<_, Error>((helper, one_shot, marker))
    }
    .await;
    sandbox
        .delete()
        .await
        .expect("delete helper after success or error");
    let (helper, one_shot, marker) = result.expect("execute through both facets");
    assert!(helper.success());
    assert!(one_shot.result.success());
    assert_eq!(helper.stdout, one_shot.result.stdout);
    assert_eq!(marker, b"shared\n");
}

/// The Docker provider behind the plugin protocol, as Petri drives it.
async fn over_the_wire() -> Option<PluginProvider> {
    let provider = DockerProvider::connect().await.ok()?;
    let (host_side, plugin_side) = duplex(1024 * 1024);
    let (host_read, host_write) = split(host_side);
    let (plugin_read, plugin_write) = split(plugin_side);
    tokio::spawn(serve(Arc::new(provider), plugin_read, plugin_write));
    Some(
        PluginProvider::connect(host_read, host_write)
            .await
            .expect("handshake succeeds"),
    )
}

/// A label value unique to this test process, so a leak check never sees
/// another run's leftovers.
fn marker(label: &str) -> String {
    format!("{label}-{}", process::id())
}

fn alpine_spec(label: &str) -> SandboxSpec {
    SandboxSpec::new(SandboxSource::Image {
        reference: ALPINE.to_owned(),
    })
    .working_directory(WORKSPACE)
    .label("sandbox-driver-workspace-test", marker(label))
}

type Seen = Arc<Mutex<Vec<(OutputStream, Vec<u8>)>>>;

fn recording_controls() -> (ExecControls, Seen) {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let sink_seen = Arc::clone(&seen);
    let controls = ExecControls {
        sink: Some(Arc::new(move |stream, chunk| {
            let seen = Arc::clone(&sink_seen);
            Box::pin(async move {
                seen.lock().expect("seen lock").push((stream, chunk));
                Ok(())
            })
        })),
        ..ExecControls::buffered()
    };
    (controls, seen)
}

/// Every file operation Petri uses, on Alpine, through the wire: a whole
/// read, a bounded range read, a write into a directory that does not
/// exist yet, binary content, a missing file, and a read past the end.
#[tokio::test(flavor = "multi_thread")]
async fn alpine_file_operations_need_no_bash() {
    let Some(provider) = over_the_wire().await else {
        return;
    };
    let sandbox = provider
        .create(&alpine_spec("files"), None)
        .await
        .expect("create");
    let fs = sandbox.fs();

    let binary: Vec<u8> = (0..70_000u32).map(|i| (i % 253) as u8).collect();
    fs.write("deep/er/dir/blob.bin", &binary)
        .await
        .expect("write into missing parents");
    assert_eq!(fs.read("deep/er/dir/blob.bin").await.expect("read"), binary);
    assert_eq!(
        fs.read_range("deep/er/dir/blob.bin", 65_530, Some(100))
            .await
            .expect("bounded read"),
        binary[65_530..65_630]
    );
    assert_eq!(
        fs.read_range("deep/er/dir/blob.bin", 69_990, None)
            .await
            .expect("read to the end"),
        binary[69_990..]
    );
    assert!(
        fs.read_range("deep/er/dir/blob.bin", 80_000, Some(4))
            .await
            .expect("read past the end")
            .is_empty()
    );
    match fs.read("deep/er/missing.txt").await {
        Err(Error::NotFound { .. }) => {}
        other => panic!("a missing file must be NotFound: {other:?}"),
    }

    // The directories a write created are the container user's, so a
    // later exec can write beside the file without Bash or base64.
    let result = sandbox
        .exec()
        .run(
            &ExecSpec::new("sh")
                .args([
                    "-c",
                    "printf beside > deep/er/dir/beside.txt && ls deep/er/dir",
                ])
                .timeout(Duration::from_secs(30)),
        )
        .await
        .expect("exec");
    assert!(result.success(), "{}", result.stderr_lossy());
    assert_eq!(
        fs.read("deep/er/dir/beside.txt")
            .await
            .expect("read beside"),
        b"beside"
    );
    // An absolute path outside the workspace: the runtime directory is
    // created by the provider's init, its files stay private.
    fs.write("/tmp/sandbox-driver/runtime/new/secret.json", b"{}")
        .await
        .expect("write under the runtime directory");
    let mode = sandbox
        .exec()
        .run(
            &ExecSpec::new("stat")
                .args(["-c", "%a", "/tmp/sandbox-driver/runtime/new/secret.json"])
                .timeout(Duration::from_secs(30)),
        )
        .await
        .expect("stat");
    assert_eq!(mode.stdout_lossy().trim(), "600");

    sandbox.delete().await.expect("delete");
    provider.shutdown().await.expect("shutdown");
}

/// The workspace is the sandbox's own volume: a one-shot container reads
/// what the sandbox wrote and the sandbox reads what the one-shot wrote;
/// `stop` ends a running one-shot; `delete` removes the volume.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_shot_shares_the_workspace_volume() {
    let Some(provider) = over_the_wire().await else {
        return;
    };
    let local = DockerProvider::connect().await.expect("daemon");
    let sandbox = provider
        .create(&alpine_spec("one-shot"), None)
        .await
        .expect("create");
    let one_shot = sandbox.one_shot().expect("docker offers one-shots");

    sandbox
        .fs()
        .write("from-sandbox.txt", b"from-sandbox")
        .await
        .expect("write");
    let (controls, seen) = recording_controls();
    let spec = OneShotSpec::registry(ALPINE)
        .entrypoint("sh")
        .args([
            "-c",
            "cat from-sandbox.txt; printf from-one-shot > from-one-shot.txt; exit 4",
        ])
        .timeout(Duration::from_secs(120));
    let streaming = one_shot.run(&spec, controls).await.expect("one-shot");
    assert_eq!(streaming.result.exit_code, Some(4));
    assert_eq!(streaming.result.termination, Termination::Exited);
    let stdout: Vec<u8> = seen
        .lock()
        .expect("seen lock")
        .iter()
        .filter(|(stream, _)| *stream == OutputStream::Stdout)
        .flat_map(|(_, chunk)| chunk.clone())
        .collect();
    assert_eq!(stdout, b"from-sandbox");
    assert_eq!(
        sandbox
            .fs()
            .read("from-one-shot.txt")
            .await
            .expect("read the one-shot's file"),
        b"from-one-shot"
    );

    // A one-shot still running when the sandbox stops goes with it: the
    // run resolves (its container was killed and removed under it, which
    // reads as a foreign kill or an unobserved end, never a success) and
    // nothing of it is left on the daemon.
    let (controls, _) = recording_controls();
    let sleeper = tokio::spawn({
        let sandbox = Arc::clone(&sandbox);
        async move {
            let spec = OneShotSpec::registry(ALPINE)
                .entrypoint("sleep")
                .args(["300"])
                .timeout(Duration::from_secs(300));
            sandbox
                .one_shot()
                .expect("facet")
                .run(&spec, controls)
                .await
        }
    });
    time::sleep(Duration::from_secs(2)).await;
    assert!(
        !one_shot_containers(sandbox.id().as_str()).await.is_empty(),
        "the sleeping one-shot is running"
    );
    sandbox.stop().await.expect("stop");
    let outcome = time::timeout(Duration::from_secs(60), sleeper)
        .await
        .expect("the one-shot run resolves once its container is gone")
        .expect("join");
    assert!(
        outcome.is_err() || outcome.is_ok_and(|s| !s.result.success()),
        "a swept one-shot does not report a clean exit"
    );
    assert!(
        one_shot_containers(sandbox.id().as_str()).await.is_empty(),
        "stop swept the one-shot container"
    );

    // A term from the caller ends a one-shot the way it ends an exec.
    sandbox.start().await.expect("start");
    let token = CancellationToken::new();
    let stop_after = token.clone();
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(500)).await;
        stop_after.cancel();
    });
    let controls = ExecControls {
        term: Some(token),
        ..ExecControls::buffered()
    };
    let spec = OneShotSpec::registry(ALPINE)
        .entrypoint("sleep")
        .args(["300"])
        .timeout(Duration::from_secs(120));
    let streaming = one_shot
        .run(&spec, controls)
        .await
        .expect("termed one-shot");
    assert_eq!(streaming.result.termination, Termination::Cancelled);

    let volume = workspace_volume(sandbox.id().as_str()).await;
    assert!(volume.is_some(), "the workspace is a volume");
    sandbox.delete().await.expect("delete");
    let remaining = local
        .list(&{
            let mut filter = SandboxFilter::default();
            filter
                .labels
                .insert("sandbox-driver-workspace-test".into(), marker("one-shot"));
            filter
        })
        .await
        .expect("list");
    assert!(
        remaining.is_empty(),
        "the sandbox is gone: {:?}",
        remaining
            .iter()
            .map(|status| (
                status.id.as_str().to_owned(),
                status.state,
                status.name.clone()
            ))
            .collect::<Vec<_>>()
    );
    let volumes = Docker::connect_with_local_defaults()
        .expect("docker")
        .list_volumes::<String>(None)
        .await
        .expect("volumes");
    let name = volume.expect("volume name");
    assert!(
        !volumes
            .volumes
            .unwrap_or_default()
            .iter()
            .any(|v| v.name == name),
        "delete removed the workspace volume"
    );
    provider.shutdown().await.expect("shutdown");
}

/// A one-shot built from a Dockerfile inside the workspace.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_shot_builds_from_the_workspace() {
    let Some(provider) = over_the_wire().await else {
        return;
    };
    let sandbox = provider
        .create(&alpine_spec("build"), None)
        .await
        .expect("create");
    sandbox
        .fs()
        .write(
            "action/Dockerfile",
            format!("FROM {ALPINE}\nCOPY greeting.txt /greeting.txt\nENTRYPOINT [\"cat\", \"/greeting.txt\"]\n").as_bytes(),
        )
        .await
        .expect("write dockerfile");
    sandbox
        .fs()
        .write("action/greeting.txt", b"built-greeting")
        .await
        .expect("write context file");
    let tag = format!("sandbox-driver-one-shot-build:{}", process::id());
    let (controls, seen) = recording_controls();
    let spec = OneShotSpec::new(sandbox_driver::OneShotImage::Build {
        context:    "action".to_owned(),
        dockerfile: None,
        tag:        tag.clone(),
        reuse:      false,
    })
    .timeout(Duration::from_secs(300));
    let streaming = sandbox
        .one_shot()
        .expect("facet")
        .run(&spec, controls)
        .await
        .expect("built one-shot");
    assert!(streaming.result.success(), "{:?}", streaming.result);
    let stdout: Vec<u8> = seen
        .lock()
        .expect("seen lock")
        .iter()
        .flat_map(|(_, chunk)| chunk.clone())
        .collect();
    assert_eq!(stdout, b"built-greeting");
    sandbox.delete().await.expect("delete");
    let _ = Docker::connect_with_local_defaults()
        .expect("docker")
        .remove_image(&tag, None, None)
        .await;
    provider.shutdown().await.expect("shutdown");
}

/// Docker reports a paused container as running, but a paused sandbox must
/// not admit a one-shot into its workspace.
#[tokio::test(flavor = "multi_thread")]
async fn a_paused_sandbox_refuses_one_shots() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let sandbox = provider
        .create(&alpine_spec("paused-one-shot"), None)
        .await
        .expect("create sandbox");
    sandbox.pause().await.expect("pause");
    let spec = OneShotSpec::registry(ALPINE)
        .entrypoint("true")
        .timeout(Duration::from_secs(120));
    let refused = sandbox
        .one_shot()
        .expect("docker offers one-shots")
        .run(&spec, ExecControls::default())
        .await
        .err();
    let leftovers = one_shot_containers(sandbox.id().as_str()).await;
    sandbox.resume().await.expect("resume");
    sandbox.delete().await.expect("delete");
    assert!(
        matches!(&refused, Some(Error::Provider(error))
            if error.message.contains("requires a running sandbox")),
        "paused sandbox admitted a one-shot: {refused:?}"
    );
    assert!(
        leftovers.is_empty(),
        "one-shot containers left: {leftovers:?}"
    );
}

async fn one_shot_containers(sandbox_id: &str) -> Vec<String> {
    let docker = Docker::connect_with_local_defaults().expect("docker");
    let mut filters = HashMap::new();
    filters.insert("label".to_owned(), vec![format!(
        "sh.sandbox-driver.one-shot={sandbox_id}"
    )]);
    docker
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        }))
        .await
        .expect("list")
        .into_iter()
        .filter_map(|c| c.id)
        .collect()
}

async fn workspace_volume(sandbox_id: &str) -> Option<String> {
    let docker = Docker::connect_with_local_defaults().expect("docker");
    let inspect = docker
        .inspect_container(sandbox_id, None)
        .await
        .expect("inspect");
    inspect.mounts?.into_iter().find_map(|mount| {
        (mount.destination.as_deref() == Some(WORKSPACE))
            .then_some(mount.name)
            .flatten()
    })
}

/// A caller's read-only bind has the same access mode in both containers.
#[tokio::test(flavor = "multi_thread")]
async fn a_one_shot_preserves_a_read_only_workspace_bind() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let directory = env::temp_dir().join(marker("sandbox-driver-read-only"));
    fs::create_dir_all(&directory)
        .await
        .expect("workspace directory");
    let config = DockerProviderConfig {
        binds: vec![BindMount {
            host:      directory.to_string_lossy().into_owned(),
            container: WORKSPACE.to_owned(),
            mode:      Some("ro".to_owned()),
        }],
        ..DockerProviderConfig::default()
    };
    let sandbox = provider
        .create(
            &alpine_spec("read-only").provider_config(config.into_value()),
            None,
        )
        .await
        .expect("sandbox");
    let spec = OneShotSpec::registry(ALPINE)
        .entrypoint("sh")
        .args(["-c", "printf forbidden > forbidden.txt"])
        .timeout(Duration::from_secs(30));
    let outcome = sandbox
        .one_shot()
        .expect("facet")
        .run(&spec, ExecControls::buffered())
        .await;
    sandbox.delete().await.expect("delete");
    let written = fs::read(directory.join("forbidden.txt")).await;
    fs::remove_dir_all(directory)
        .await
        .expect("remove workspace");
    let result = outcome.expect("one-shot");
    assert!(
        !result.result.success(),
        "a read-only workspace must reject writes"
    );
    assert!(written.is_err(), "the host workspace was modified");
}

/// Signal handling remains active when a consumer never finishes a chunk.
#[tokio::test(flavor = "multi_thread")]
async fn blocked_output_sinks_do_not_block_kill() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let sandbox = provider
        .create(&alpine_spec("blocked-sink"), None)
        .await
        .expect("sandbox");
    for one_shot in [false, true] {
        let kill = CancellationToken::new();
        let sink_kill = kill.clone();
        let controls = ExecControls {
            kill: Some(kill),
            sink: Some(Arc::new(move |_, _| {
                sink_kill.cancel();
                Box::pin(future::pending())
            })),
            ..ExecControls::buffered()
        };
        let outcome = time::timeout(Duration::from_secs(25), async {
            if one_shot {
                sandbox
                    .one_shot()
                    .expect("facet")
                    .run(
                        &OneShotSpec::registry(ALPINE)
                            .entrypoint("sh")
                            .args(["-c", "printf ready; sleep 300"]),
                        controls,
                    )
                    .await
            } else {
                sandbox
                    .exec()
                    .run_streaming(
                        &ExecSpec::new("sh").args(["-c", "printf ready; sleep 300"]),
                        controls,
                    )
                    .await
            }
        })
        .await;
        if outcome.is_err() {
            sandbox.delete().await.expect("cleanup stalled sandbox");
        }
        let result = outcome
            .expect("kill resolves despite a blocked sink")
            .expect("run");
        assert_eq!(result.result.termination, Termination::Killed);
        assert!(
            result.stdout_capture.truncated,
            "abandoned output is reported"
        );
    }
    sandbox.delete().await.expect("delete");
}

fn pattern_byte(offset: usize) -> u8 {
    u8::try_from(offset % 251).expect("pattern byte fits")
}

/// Generates input without a file-sized allocation and records consumption.
struct PatternInput {
    position: usize,
    length:   usize,
}

impl AsyncRead for PatternInput {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let count = buffer.remaining().min(self.length - self.position);
        for offset in self.position..self.position + count {
            buffer.put_slice(&[pattern_byte(offset)]);
        }
        self.position += count;
        Poll::Ready(Ok(()))
    }
}

/// Validates output as it arrives, retaining only an offset.
#[derive(Default)]
struct PatternOutput {
    position: usize,
    flushed:  bool,
    fail:     bool,
}

impl AsyncWrite for PatternOutput {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fail {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "output rejected",
            )));
        }
        for (index, byte) in bytes.iter().enumerate() {
            assert_eq!(*byte, pattern_byte(self.position + index));
        }
        self.position += bytes.len();
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.flushed = true;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        panic!("read_to must not close the caller's output")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn docker_file_transfers_stream_and_report_input_and_output_failures() {
    let Ok(provider) = DockerProvider::connect().await else {
        return;
    };
    let sandbox = provider
        .create(&alpine_spec("streaming-files"), None)
        .await
        .expect("sandbox");
    let file = format!("missing/parents/{}.bin", "name".repeat(40));
    let length = 4 * 1024 * 1024 + 29;
    let mut input = PatternInput {
        position: 0,
        length:   length + 7,
    };
    sandbox
        .fs()
        .write_from(&file, &mut input, length as u64)
        .await
        .expect("stream upload");
    assert_eq!(input.position, length, "extra input is left unread");
    let mut output = PatternOutput::default();
    sandbox
        .fs()
        .read_to(&file, &mut output)
        .await
        .expect("stream download");
    assert_eq!(output.position, length);
    assert!(output.flushed, "download flushes the caller's output");

    let mut rejected = PatternOutput {
        fail: true,
        ..PatternOutput::default()
    };
    let error = sandbox
        .fs()
        .read_to(&file, &mut rejected)
        .await
        .expect_err("sink failure");
    assert!(
        matches!(error, Error::Io { source, .. } if source.kind() == io::ErrorKind::PermissionDenied)
    );
    let mut short = b"short".as_slice();
    let error = sandbox
        .fs()
        .write_from("short.bin", &mut short, 6)
        .await
        .expect_err("short source");
    assert!(
        matches!(error, Error::Io { source, .. } if source.kind() == io::ErrorKind::UnexpectedEof)
    );

    // Local upload/download exercise the same streaming path on stopped
    // containers and do not require an exec-derived filesystem command.
    let directory = env::temp_dir().join(marker("sandbox-driver-streaming-files"));
    fs::create_dir_all(&directory)
        .await
        .expect("local directory");
    let local = directory.join("download.bin");
    sandbox.stop().await.expect("stop");
    sandbox
        .fs()
        .download(&file, &local)
        .await
        .expect("local download");
    sandbox
        .fs()
        .upload(&local, "other/missing/parents/copy.bin")
        .await
        .expect("local upload");
    let mut copied = PatternOutput::default();
    sandbox
        .fs()
        .read_to("other/missing/parents/copy.bin", &mut copied)
        .await
        .expect("uploaded copy");
    assert_eq!(copied.position, length);
    sandbox.delete().await.expect("delete");
    fs::remove_dir_all(directory).await.expect("local cleanup");
}
