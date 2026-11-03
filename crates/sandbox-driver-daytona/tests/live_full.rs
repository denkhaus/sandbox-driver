//! Deep live exercise of the Daytona provider: the positive paths the
//! conformance suite only presence-checks. Requires a full-access
//! `DAYTONA_API_KEY`; ignored by default and run with
//! `mise run test:live`. Every resource this test
//! creates is deleted before it returns, on success and failure paths.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, process};

use sandbox_driver::{
    Action, Capability, Error, ExecControls, ExecSpec, ExecStreamingResult, Git,
    GitCheckoutOptions, GitCloneOptions, GitCommitOptions, LifecycleTimers, LogSink, LogSource,
    NetworkPolicy, OutputSink, OutputStream, Resources, Sandbox, SandboxKind, SandboxProvider,
    SandboxSnapshotOptions, SandboxSource, SandboxSpec, SandboxState, SnapshotId, SnapshotMode,
    SnapshotSource, SnapshotSpec, SnapshotState, StdinSource, WaitOptions, wait_for_state,
};
use sandbox_driver_daytona::DaytonaProvider;
use tokio::io::{AsyncWriteExt as _, duplex};
use tokio::sync::Notify;
use tokio::time;

mod support;

use support::init_diagnostics;

const TEST_SNAPSHOT: &str = "daytona-medium";

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!("{prefix}-{}-{nanos:x}", process::id())
}

fn wait() -> WaitOptions {
    WaitOptions {
        interval: Duration::from_secs(2),
        deadline: Some(Duration::from_secs(300)),
    }
}

fn snapshot_resources(disk_mb: u64) -> Resources {
    let mut resources = Resources::default();
    resources.cpu_cores = Some(1);
    resources.memory_mb = Some(1024);
    resources.disk_mb = Some(disk_mb);
    resources
}

fn error_chain_contains(error: &(dyn StdError + 'static), needle: &str) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.to_string().contains(needle) {
            return true;
        }
        current = error.source();
    }
    false
}

fn format_error_chain(error: &(dyn StdError + 'static)) -> String {
    let mut report = error.to_string();
    let mut current = error.source();
    while let Some(error) = current {
        write!(report, ": {error}").expect("writing to a String should not fail");
        current = error.source();
    }
    report
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn requested_working_directory_is_created_and_survives_attach() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let working_directory = format!("/home/daytona/{}", unique("sd-working-directory"));
    let display_name = unique("sd-display-name");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .name(&display_name)
    .working_directory(&working_directory)
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        let status = sandbox
            .describe()
            .await
            .map_err(|error| format!("describe created sandbox: {error}"))?;
        if status.name.as_deref() != Some(display_name.as_str()) {
            return Err(format!(
                "created sandbox display name was {:?}",
                status.name
            ));
        }
        if sandbox.working_directory() != working_directory {
            return Err(format!(
                "created handle returned working directory {:?}",
                sandbox.working_directory()
            ));
        }
        let pwd = sandbox
            .exec()
            .run(&ExecSpec::new("pwd").timeout(Duration::from_secs(30)))
            .await
            .map_err(|error| format!("created handle pwd: {error}"))?;
        if pwd.stdout_lossy().trim() != working_directory {
            return Err(format!(
                "created handle used working directory {:?}",
                pwd.stdout_lossy().trim()
            ));
        }
        let runtime_directory = sandbox
            .runtime_directory()
            .ok_or("created handle returned no runtime directory")?;
        let runtime_metadata = sandbox
            .fs()
            .metadata(runtime_directory)
            .await
            .map_err(|error| format!("runtime directory metadata: {error}"))?;
        if runtime_metadata.kind != sandbox_driver::FileKind::Directory {
            return Err(format!("runtime path has kind {:?}", runtime_metadata.kind));
        }
        if runtime_metadata.mode.map(|mode| mode & 0o777) != Some(0o700) {
            return Err(format!(
                "runtime directory mode is {:?}",
                runtime_metadata.mode
            ));
        }

        // Replacing user labels must retain the provider's stored directory.
        let mut labels = BTreeMap::new();
        labels.insert("sd-live".to_owned(), "working-directory".to_owned());
        sandbox
            .set_labels(&labels)
            .await
            .map_err(|error| format!("set_labels: {error}"))?;
        let attached = provider
            .attach(sandbox.id(), None)
            .await
            .map_err(|error| format!("attach: {error}"))?;
        let attached_status = attached
            .describe()
            .await
            .map_err(|error| format!("describe attached sandbox: {error}"))?;
        if attached_status.name.as_deref() != Some(display_name.as_str()) {
            return Err(format!(
                "attached sandbox display name was {:?}",
                attached_status.name
            ));
        }
        if attached.working_directory() != working_directory {
            return Err(format!(
                "attached handle returned working directory {:?}",
                attached.working_directory()
            ));
        }
        if attached.runtime_directory() != Some(runtime_directory) {
            return Err(format!(
                "attached handle returned runtime directory {:?}",
                attached.runtime_directory()
            ));
        }
        let attached_pwd = attached
            .exec()
            .run(&ExecSpec::new("pwd").timeout(Duration::from_secs(30)))
            .await
            .map_err(|error| format!("attached handle pwd: {error}"))?;
        if attached_pwd.stdout_lossy().trim() != working_directory {
            return Err(format!(
                "attached handle used working directory {:?}",
                attached_pwd.stdout_lossy().trim()
            ));
        }
        Ok(())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live requested working directory");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn hybrid_git_uses_native_clone_and_derived_worktree_operations() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let working_directory = format!("/home/daytona/{}", unique("sd-hybrid-git"));
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .working_directory(&working_directory)
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        if !sandbox.capabilities().supports(Capability::Git) || !sandbox.capabilities().git.native {
            return Err("Daytona did not declare its hybrid git facet".to_owned());
        }
        let git = sandbox.git().ok_or("git facet missing")?;
        let mut clone = GitCloneOptions::default();
        clone.depth = Some(1);
        git.clone_repo(
            "https://github.com/octocat/Hello-World.git",
            "hybrid-repo",
            &clone,
        )
        .await
        .map_err(|error| format!("native clone: {error}"))?;

        sandbox
            .fs()
            .write("hybrid-repo/sandbox-driver.txt", b"hybrid git\n")
            .await
            .map_err(|error| format!("write worktree file: {error}"))?;
        let status = git
            .status("hybrid-repo")
            .await
            .map_err(|error| format!("derived status: {error}"))?;
        if !status
            .dirty_paths
            .iter()
            .any(|path| path == "sandbox-driver.txt")
        {
            return Err(format!("status did not report new file: {status:?}"));
        }
        git.add("hybrid-repo", &["sandbox-driver.txt".to_owned()])
            .await
            .map_err(|error| format!("derived add: {error}"))?;
        let sha = git
            .commit(
                "hybrid-repo",
                &GitCommitOptions::new(
                    "test hybrid git",
                    "sandbox-driver",
                    "sandbox-driver@example.com",
                ),
            )
            .await
            .map_err(|error| format!("derived commit: {error}"))?;
        if sha.len() != 40 {
            return Err(format!("commit returned an unexpected SHA: {sha:?}"));
        }
        git.checkout(
            "hybrid-repo",
            &GitCheckoutOptions::new("sandbox-driver-test").create(),
        )
        .await
        .map_err(|error| format!("derived checkout: {error}"))?;
        let branches = git
            .branches("hybrid-repo")
            .await
            .map_err(|error| format!("derived branches: {error}"))?;
        if branches.current.as_deref() != Some("sandbox-driver-test") {
            return Err(format!("checkout did not select new branch: {branches:?}"));
        }
        Ok(())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live hybrid git");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn labels_timers_access_round_trip() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        // Labels: set and read back through describe.
        let mut labels = BTreeMap::new();
        labels.insert("sd-live".to_owned(), "yes".to_owned());
        sandbox
            .set_labels(&labels)
            .await
            .map_err(|error| format!("set_labels: {error}"))?;
        let status = sandbox
            .describe()
            .await
            .map_err(|error| format!("describe: {error}"))?;
        if status.labels.get("sd-live").map(String::as_str) != Some("yes") {
            return Err(format!("labels not visible after set: {:?}", status.labels));
        }

        // Timers: set auto-stop and auto-archive.
        let mut timers = LifecycleTimers::default();
        timers.auto_stop_after_idle = Some(Duration::from_secs(30 * 60));
        timers.auto_archive_after_stop = Some(Duration::from_secs(60 * 60));
        sandbox
            .set_timers(&timers)
            .await
            .map_err(|error| format!("set_timers: {error}"))?;

        // Keepalive.
        sandbox
            .refresh_activity()
            .await
            .map_err(|error| format!("refresh_activity: {error}"))?;

        // Preview URLs.
        let preview = sandbox.preview_urls().expect("facet declared");
        let url = preview
            .preview_url(3000)
            .await
            .map_err(|error| format!("preview_url: {error}"))?;
        if !url.url.starts_with("https://") {
            return Err(format!("preview url looks wrong: {}", url.url));
        }
        if !url.headers.contains_key("x-daytona-preview-token") {
            return Err("preview url missing auth header".to_owned());
        }
        let signed = preview
            .signed_preview_url(3000, Duration::from_secs(120))
            .await
            .map_err(|error| format!("signed_preview_url: {error}"))?;
        if !signed.url.starts_with("https://") {
            return Err(format!("signed preview url looks wrong: {}", signed.url));
        }

        // SSH: mint, sanity-check, revoke.
        let ssh = sandbox.ssh().expect("facet declared");
        let access = ssh
            .ssh_access(Some(Duration::from_secs(10 * 60)))
            .await
            .map_err(|error| format!("ssh_access: {error}"))?;
        if !access.command.contains("ssh") {
            return Err(format!("ssh command looks wrong: {}", access.command));
        }
        let token = access.token.clone().ok_or("ssh access returned no token")?;
        ssh.revoke_ssh_access(&token)
            .await
            .map_err(|error| format!("revoke_ssh_access: {error}"))?;

        // Browser terminal: Daytona exposes the service on port 22222.
        let terminal = sandbox
            .web_terminal()
            .expect("web terminal facet declared")
            .web_terminal_url()
            .await
            .map_err(|error| format!("web_terminal_url: {error}"))?;
        if !terminal.starts_with("https://") {
            return Err(format!("web terminal URL looks wrong: {terminal}"));
        }

        // VNC: starting Computer Use must yield a signed noVNC viewer.
        let vnc = sandbox
            .vnc()
            .expect("vnc facet declared")
            .vnc_connection()
            .await
            .map_err(|error| format!("vnc_connection: {error}"))?;
        if !vnc.url.starts_with("https://") || !vnc.url.contains("/vnc.html") {
            return Err(format!("VNC URL looks wrong: {}", vnc.url));
        }
        Ok(())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live access round trip");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn wall_clock_ttl_and_container_auto_pause_behavior() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        let mut ttl = LifecycleTimers::default();
        ttl.ttl = Some(Duration::from_secs(2 * 60 * 60));
        sandbox
            .set_timers(&ttl)
            .await
            .map_err(|error| format!("set wall-clock ttl: {error}"))?;

        let mut auto_pause = LifecycleTimers::default();
        auto_pause.auto_pause_after_idle = Some(Duration::from_secs(30 * 60));
        match sandbox.set_timers(&auto_pause).await {
            Err(error) if error_chain_contains(&error, "not supported for sandbox class") => Ok(()),
            Err(error) => Err(format!(
                "container auto-pause returned an unexpected error: {}",
                format_error_chain(&error)
            )),
            Ok(()) => Err("container auto-pause unexpectedly succeeded".to_owned()),
        }
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live lifecycle timer behavior");
}

/// A fast run of small writes — a download's progress meter — is where
/// the toolbox tears the encoded output stream. The exec must complete
/// with the command's own exit status either way; whatever the decoder
/// had to discard is reported on the result, so the test prints the loss
/// rather than asserting zero (the toolbox decides that, not the driver).
/// A lossless run must decode exactly, which pins the two-encoder path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
#[expect(
    clippy::print_stderr,
    reason = "the observed loss per run is the test's evidence"
)]
async fn fast_progress_output_completes_and_reports_loss() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .name(unique("sd-live-progress"))
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        // curl repaints its progress bar on stderr as each chunk lands;
        // the rate limit keeps it repainting for a dozen seconds. The
        // byte count on stdout proves the download was the real thing.
        let download = run_fast(
            sandbox.as_ref(),
            "download",
            "curl -# -f -L --limit-rate 8M -o /dev/null -w '%{size_download}' \
             'https://ash-speed.hetzner.com/100MB.bin'",
        )
        .await?;
        if !download.result.success() {
            return Err(format!(
                "download exited {:?} ({:?}): {}",
                download.result.exit_code,
                download.result.termination,
                download.result.stderr_lossy()
            ));
        }
        if download.result.stdout_lossy().trim() != "104857600" && !download.output_loss.is_lossy() {
            return Err(format!(
                "download reported {:?} bytes, expected the whole 100 MiB",
                download.result.stdout_lossy()
            ));
        }

        // A synthetic burst: thirty thousand fifteen-byte writes.
        let burst = run_fast(
            sandbox.as_ref(),
            "burst",
            "for i in $(seq 1 30000); do printf 'progress %5d\\r' \"$i\"; done; printf 'done\\n'",
        )
        .await?;
        let expected: Vec<u8> = (1..=30000)
            .flat_map(|i| format!("progress {i:5}\r").into_bytes())
            .chain(*b"done\n")
            .collect();
        check_burst("burst", &burst, &expected, &[])?;

        // Both encoders write records to the one pipe at once.
        let interleaved = run_fast(
            sandbox.as_ref(),
            "interleaved",
            "for i in $(seq 1 20000); do printf 'out %5d\\r' \"$i\"; printf 'err %5d\\r' \"$i\" >&2; done",
        )
        .await?;
        let expected_out: Vec<u8> = (1..=20000)
            .flat_map(|i| format!("out {i:5}\r").into_bytes())
            .collect();
        let expected_err: Vec<u8> = (1..=20000)
            .flat_map(|i| format!("err {i:5}\r").into_bytes())
            .collect();
        check_burst("interleaved", &interleaved, &expected_out, &expected_err)?;

        // The case that tore in fabro: Playwright's browser download and
        // its progress bar. Skipped, and said so, where the snapshot has
        // no npx.
        let playwright = run_fast(
            sandbox.as_ref(),
            "playwright",
            "if ! command -v npx >/dev/null; then echo 'npx missing' >&2; exit 97; fi; \
             npx -y playwright install chromium 2>&1",
        )
        .await?;
        match (playwright.result.termination, playwright.result.exit_code) {
            (sandbox_driver::Termination::Exited, Some(0)) => {}
            (sandbox_driver::Termination::Exited, Some(97)) => {
                eprintln!("fast-progress playwright: skipped, the snapshot has no npx");
            }
            (termination, code) => {
                return Err(format!(
                    "playwright install exited {code:?} ({termination:?}): {}",
                    playwright.result.stdout_lossy()
                ));
            }
        }
        Ok(())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live fast progress output");
}

/// A burst must exit cleanly; a lossless one must decode exactly, and a
/// lossy one must have fewer bytes than were written, never more.
fn check_burst(
    label: &str,
    result: &ExecStreamingResult,
    expected_stdout: &[u8],
    expected_stderr: &[u8],
) -> Result<(), String> {
    if !result.result.success() {
        return Err(format!(
            "{label} exited {:?} ({:?})",
            result.result.exit_code, result.result.termination
        ));
    }
    let decoded = result.result.stdout.len() + result.result.stderr.len();
    let written = expected_stdout.len() + expected_stderr.len();
    if result.output_loss.is_lossy() {
        if decoded >= written {
            return Err(format!(
                "{label}: a lossy run still decoded {decoded} of {written} bytes"
            ));
        }
    } else if result.result.stdout != expected_stdout || result.result.stderr != expected_stderr {
        return Err(format!(
            "{label}: a lossless run decoded {decoded} bytes, expected {written}"
        ));
    }
    Ok(())
}

/// Runs `script` with buffered retention and reports its exit, sizes,
/// and decoder loss; a transport tear must surface there, not as an error.
#[expect(
    clippy::print_stderr,
    reason = "the observed loss per run is the test's evidence"
)]
async fn run_fast(
    sandbox: &dyn sandbox_driver::Sandbox,
    label: &str,
    script: &str,
) -> Result<ExecStreamingResult, String> {
    let started = Instant::now();
    let result = sandbox
        .exec()
        .run_streaming(
            &ExecSpec::bash(script).timeout(Duration::from_secs(600)),
            ExecControls::buffered(),
        )
        .await
        .map_err(|error| format!("{label}: {error}"))?;
    eprintln!(
        "fast-progress {label}: exit={:?} termination={:?} elapsed={:?} stdout_observed={} \
         stderr_observed={} truncated={} dropped_frames={} dropped_bytes={}",
        result.result.exit_code,
        result.result.termination,
        started.elapsed(),
        result.stdout_capture.observed_bytes,
        result.stderr_capture.observed_bytes,
        result.stdout_capture.truncated || result.stderr_capture.truncated,
        result.output_loss.dropped_frames,
        result.output_loss.dropped_bytes,
    );
    Ok(result)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn cidr_egress_limits_apply_at_create_and_runtime() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .network(NetworkPolicy::CidrAllowList {
        // TEST-NET-3 cannot contain Cloudflare's public endpoint.
        cidrs: vec!["203.0.113.0/24".to_owned()],
    })
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        let blocked = tcp_probe(sandbox.as_ref()).await?;
        if blocked != "blocked" {
            return Err(format!(
                "creation-time CIDR policy did not block egress: {blocked:?}"
            ));
        }

        sandbox
            .update_network(&NetworkPolicy::CidrAllowList {
                cidrs: vec!["1.1.1.1/32".to_owned()],
            })
            .await
            .map_err(|error| format!("update_network: {error}"))?;
        for _ in 0..12 {
            if tcp_probe(sandbox.as_ref()).await? == "reachable" {
                return Ok(());
            }
            time::sleep(Duration::from_secs(5)).await;
        }
        Err("runtime CIDR update did not allow the endpoint within 60 seconds".to_owned())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live CIDR egress policy");
}

async fn tcp_probe(sandbox: &dyn sandbox_driver::Sandbox) -> Result<String, String> {
    let result = sandbox
        .exec()
        .run(
            &ExecSpec::bash(
                "if timeout 5 bash -c 'exec 3<>/dev/tcp/1.1.1.1/443' 2>/dev/null; \
                 then printf reachable; else printf blocked; fi",
            )
            .timeout(Duration::from_secs(15)),
        )
        .await
        .map_err(|error| format!("egress probe: {error}"))?;
    if !result.success() {
        return Err(format!("egress probe failed: {result:?}"));
    }
    String::from_utf8(result.stdout).map_err(|error| format!("egress probe output: {error}"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn stop_resize_archive_restore_lifecycle() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    // Non-ephemeral: an ephemeral sandbox deletes itself on stop, and
    // archive requires a stopped sandbox.
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .name(unique("sd-live-lifecycle"));
    let sandbox = provider.create(&spec, None).await.expect("create");

    let outcome = async {
        sandbox
            .stop()
            .await
            .map_err(|error| format!("stop: {error}"))?;
        wait_for_state(sandbox.as_ref(), SandboxState::Stopped, &wait())
            .await
            .map_err(|error| format!("waiting for Stopped: {error}"))?;

        // Resize: grow disk by 1 GB from the current allocation.
        let current = sandbox
            .describe()
            .await
            .map_err(|error| format!("describe: {error}"))?
            .resources
            .unwrap_or_default();
        let mut resources = Resources::default();
        resources.cpu_cores = current.cpu_cores;
        resources.memory_mb = current.memory_mb;
        resources.disk_mb = current.disk_mb.map(|disk| disk + 1024);
        if resources.disk_mb.is_some() && sandbox.capabilities().lifecycle.resize {
            sandbox
                .resize(&resources)
                .await
                .map_err(|error| format!("resize: {error}"))?;
            let deadline = Instant::now() + Duration::from_secs(300);
            loop {
                let state = sandbox
                    .describe()
                    .await
                    .map_err(|error| format!("describe: {error}"))?
                    .state;
                match state {
                    SandboxState::Stopped => break,
                    SandboxState::Error => return Err("resize entered error state".to_owned()),
                    _ if Instant::now() >= deadline => {
                        return Err(format!("resize never settled (state {state:?})"));
                    }
                    _ => time::sleep(Duration::from_secs(2)).await,
                }
            }
        }

        // Archive, then restore via start. Archiving copies the
        // filesystem to object storage and exceeds the generic wait
        // live (a 300s wait timed out on 2026-08-30), so it gets its
        // own budget.
        sandbox
            .archive()
            .await
            .map_err(|error| format!("archive: {error}"))?;
        let archive_wait = WaitOptions {
            interval: Duration::from_secs(2),
            deadline: Some(Duration::from_secs(900)),
        };
        wait_for_state(sandbox.as_ref(), SandboxState::Archived, &archive_wait)
            .await
            .map_err(|error| format!("waiting for Archived: {error}"))?;
        sandbox
            .start()
            .await
            .map_err(|error| format!("start from archive: {error}"))?;
        wait_for_state(sandbox.as_ref(), SandboxState::Running, &wait())
            .await
            .map_err(|error| format!("waiting for Running after restore: {error}"))?;

        // The sandbox must still work after the full cycle.
        let result = sandbox
            .exec()
            .run(
                &sandbox_driver::ExecSpec::new("echo")
                    .arg("restored")
                    .timeout(Duration::from_secs(60)),
            )
            .await
            .map_err(|error| format!("exec after restore: {error}"))?;
        if !result.success() || !result.stdout_lossy().contains("restored") {
            return Err(format!(
                "exec after restore failed: {}",
                result.stdout_lossy()
            ));
        }
        Ok(())
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("lifecycle round trip");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn fork_and_snapshot_modes_preserve_their_declared_state() {
    // An explicit VM verification must fail when access is unavailable.
    let require_vm = env::var("SANDBOX_DRIVER_DAYTONA_REQUIRE_VM").as_deref() == Ok("1");
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let snapshots = provider.snapshots().expect("snapshot provider declared");
    // Leave the region to the backend unless this account supplies a VM region.
    let vm_region = env::var("SANDBOX_DRIVER_DAYTONA_VM_REGION")
        .or_else(|_| env::var("DAYTONA_TARGET"))
        .ok();
    let mut vm_snapshot_spec = SnapshotSpec::new(SnapshotSource::Image {
        reference: "ubuntu:24.04".to_owned(),
    })
    .name(unique("sd-live-vm-base"))
    .sandbox_kind(SandboxKind::VirtualMachine)
    .resources(snapshot_resources(3072));
    vm_snapshot_spec.region.clone_from(&vm_region);
    let vm_snapshot_id = match snapshots.create(&vm_snapshot_spec, None).await {
        Ok(id) => id,
        Err(error)
            if !require_vm
                && (error_chain_contains(&error, "No runners are configured")
                    || error_chain_contains(&error, "not available to the organization")) =>
        {
            tracing::info!(
                target: "sandbox_driver_daytona",
                reason = "vm_snapshot_unavailable",
                "live VM snapshot test skipped"
            );
            return;
        }
        Err(error) => panic!(
            "create VM snapshot from image: {}",
            format_error_chain(&error)
        ),
    };
    if let Err(error) =
        wait_for_snapshot_state(snapshots, &vm_snapshot_id, SnapshotState::Active).await
    {
        let _ = snapshots.delete(&vm_snapshot_id, None).await;
        panic!("VM snapshot did not become active: {error}");
    }
    let mut source_spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: vm_snapshot_id.clone(),
    })
    .name(unique("sd-live-vm-state"))
    .sandbox_kind(SandboxKind::VirtualMachine);
    source_spec.region.clone_from(&vm_region);
    let source = match provider.create(&source_spec, None).await {
        Ok(source) => source,
        Err(error) => {
            let _ = snapshots.delete(&vm_snapshot_id, None).await;
            panic!("create VM: {error}");
        }
    };
    let mut created_sandboxes: Vec<Arc<dyn sandbox_driver::Sandbox>> = Vec::new();
    let mut created_snapshots = vec![vm_snapshot_id];

    let outcome = async {
        if !source.capabilities().lifecycle.fork
            || !source
                .capabilities()
                .snapshots
                .as_ref()
                .is_some_and(|caps| caps.live_process_state_from_sandbox)
        {
            return Err("VM sandbox did not declare fork and hot snapshots".to_owned());
        }
        let source_status = source
            .describe()
            .await
            .map_err(|error| format!("describe VM: {error}"))?;
        if source_status.sandbox_kind != Some(SandboxKind::VirtualMachine)
            || vm_region
                .as_ref()
                .is_some_and(|region| source_status.region.as_ref() != Some(region))
        {
            return Err(format!(
                "VM status did not preserve kind and region: {source_status:?}"
            ));
        }
        let setup = source
            .exec()
            .run(
                &ExecSpec::bash(
                    "printf preserved > /tmp/sd-state-marker; \
                     nohup bash -c 'exec -a sandbox-driver-live-process sleep 3600' \
                     </dev/null >/dev/null 2>&1 & \
                     echo $! > /tmp/sd-state-pid; cat /tmp/sd-state-pid",
                )
                .timeout(Duration::from_secs(30)),
            )
            .await
            .map_err(|error| format!("start live process: {error}"))?;
        if !setup.success() {
            return Err(format!("start live process: {}", setup.stderr_lossy()));
        }
        let source_pid = setup.stdout_lossy().trim().to_owned();

        let forked = source
            .fork(&sandbox_driver::ForkOptions::default())
            .await
            .map_err(|error| format!("fork: {error}"))?;
        created_sandboxes.push(Arc::clone(&forked));
        assert_live_process(forked.as_ref(), &source_pid).await?;

        let hot_name = unique("sd-live-hot");
        let mut hot_options = SandboxSnapshotOptions::default();
        hot_options.name = Some(hot_name.clone());
        hot_options.mode = SnapshotMode::LiveProcessState;
        let hot_id = source
            .snapshot(&hot_options)
            .await
            .map_err(|error| format!("hot snapshot: {error}"))?;
        created_snapshots.push(hot_id.clone());
        wait_for_snapshot_state(snapshots, &hot_id, SnapshotState::Active).await?;
        let mut hot_restore_spec = SandboxSpec::new(SandboxSource::Snapshot { id: hot_id.clone() })
            .sandbox_kind(SandboxKind::VirtualMachine)
            .ephemeral(true);
        hot_restore_spec.region.clone_from(&source_status.region);
        let hot_restore = provider
            .create(&hot_restore_spec, None)
            .await
            .map_err(|error| format!("restore hot snapshot: {error}"))?;
        created_sandboxes.push(Arc::clone(&hot_restore));
        assert_live_process(hot_restore.as_ref(), &source_pid).await?;

        let mut cold_while_running = SandboxSnapshotOptions::default();
        cold_while_running.mode = SnapshotMode::Filesystem;
        if !matches!(
            source.snapshot(&cold_while_running).await,
            Err(Error::InvalidState {
                current: SandboxState::Running,
                action:  Action::Snapshot,
            })
        ) {
            return Err("filesystem snapshot did not require a stopped sandbox".to_owned());
        }

        source
            .stop()
            .await
            .map_err(|error| format!("stop source: {error}"))?;
        wait_for_state(source.as_ref(), SandboxState::Stopped, &wait())
            .await
            .map_err(|error| format!("waiting for stopped source: {error}"))?;
        if !matches!(
            source.fork(&sandbox_driver::ForkOptions::default()).await,
            Err(Error::InvalidState {
                current: SandboxState::Stopped,
                action:  Action::Fork,
            })
        ) {
            return Err("fork did not require a running source".to_owned());
        }

        let cold_name = unique("sd-live-cold");
        let mut cold_options = SandboxSnapshotOptions::default();
        cold_options.name = Some(cold_name.clone());
        cold_options.mode = SnapshotMode::Filesystem;
        let cold_id = source
            .snapshot(&cold_options)
            .await
            .map_err(|error| format!("filesystem snapshot: {error}"))?;
        created_snapshots.push(cold_id.clone());
        wait_for_snapshot_state(snapshots, &cold_id, SnapshotState::Active).await?;
        let mut cold_restore_spec = SandboxSpec::new(SandboxSource::Snapshot {
            id: cold_id.clone(),
        })
        .sandbox_kind(SandboxKind::VirtualMachine)
        .ephemeral(true);
        cold_restore_spec.region.clone_from(&source_status.region);
        let cold_restore = provider
            .create(&cold_restore_spec, None)
            .await
            .map_err(|error| format!("restore filesystem snapshot: {error}"))?;
        created_sandboxes.push(Arc::clone(&cold_restore));
        let cold_check = cold_restore
            .exec()
            .run(&snapshot_probe(
                "/tmp/sd-state-marker",
                "/tmp/sd-state-pid",
                "sandbox-driver-live-process",
                "cold",
            ))
            .await
            .map_err(|error| format!("check filesystem snapshot: {error}"))?;
        if !cold_check.success() {
            return Err(format!(
                "filesystem snapshot restored a live process or lost its files: {cold_check:?}"
            ));
        }
        Ok(())
    }
    .await;

    for sandbox in created_sandboxes.iter().rev() {
        let _ = sandbox.delete().await;
    }
    let _ = source.delete().await;
    for snapshot in created_snapshots.iter().rev() {
        let _ = snapshots.delete(snapshot, None).await;
    }
    outcome.expect("fork and snapshot mode behavior");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn filesystem_snapshot_restores_files_without_processes() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let source_spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .name(unique("sd-live-filesystem-source"));
    let source = provider.create(&source_spec, None).await.expect("create");
    let snapshots = provider.snapshots().expect("snapshot provider declared");
    let mut restored = None;
    let mut snapshot_id = None;

    let outcome = async {
        let snapshot_caps = source
            .capabilities()
            .snapshots
            .as_ref()
            .ok_or("snapshot capabilities absent")?;
        if !snapshot_caps.filesystem_from_sandbox || snapshot_caps.live_process_state_from_sandbox {
            return Err(format!(
                "container snapshot modes are wrong: filesystem={}, live={}",
                snapshot_caps.filesystem_from_sandbox,
                snapshot_caps.live_process_state_from_sandbox
            ));
        }
        let mut hot_options = SandboxSnapshotOptions::default();
        hot_options.mode = SnapshotMode::LiveProcessState;
        if !matches!(
            source.snapshot(&hot_options).await,
            Err(Error::Unsupported {
                capability: Capability::SnapshotsLiveProcessState,
            })
        ) {
            return Err("container accepted a live-process-state snapshot".to_owned());
        }

        let setup = source
            .exec()
            .run(
                &ExecSpec::bash(
                    "printf preserved > /tmp/sd-cold-marker; \
                     nohup bash -c 'exec -a sandbox-driver-cold-process sleep 3600' \
                     </dev/null >/dev/null 2>&1 & \
                     echo $! > /tmp/sd-cold-pid",
                )
                .timeout(Duration::from_secs(30)),
            )
            .await
            .map_err(|error| format!("start process: {error}"))?;
        if !setup.success() {
            return Err(format!("start process: {}", setup.stderr_lossy()));
        }
        source
            .stop()
            .await
            .map_err(|error| format!("stop source: {error}"))?;
        wait_for_state(source.as_ref(), SandboxState::Stopped, &wait())
            .await
            .map_err(|error| format!("wait for stopped source: {error}"))?;

        let name = unique("sd-live-filesystem");
        let mut options = SandboxSnapshotOptions::default();
        options.name = Some(name.clone());
        options.mode = SnapshotMode::Filesystem;
        let id = source
            .snapshot(&options)
            .await
            .map_err(|error| format!("filesystem snapshot: {error}"))?;
        snapshot_id = Some(id.clone());
        wait_for_snapshot_state(snapshots, &id, SnapshotState::Active).await?;

        let sandbox = provider
            .create(
                &SandboxSpec::new(SandboxSource::Snapshot { id: id.clone() }).ephemeral(true),
                None,
            )
            .await
            .map_err(|error| format!("restore filesystem snapshot: {error}"))?;
        restored = Some(Arc::clone(&sandbox));
        let check = sandbox
            .exec()
            .run(&snapshot_probe(
                "/tmp/sd-cold-marker",
                "/tmp/sd-cold-pid",
                "sandbox-driver-cold-process",
                "cold",
            ))
            .await
            .map_err(|error| format!("check restored filesystem snapshot: {error}"))?;
        if !check.success() {
            return Err(format!(
                "filesystem snapshot lost files or restored a process: {check:?}"
            ));
        }
        Ok(())
    }
    .await;

    if let Some(restored) = restored {
        let _ = restored.delete().await;
    }
    let _ = source.delete().await;
    if let Some(snapshot_id) = snapshot_id {
        let _ = snapshots.delete(&snapshot_id, None).await;
    }
    outcome.expect("filesystem snapshot behavior");
}

fn snapshot_probe(marker: &str, pid: &str, process: &str, mode: &str) -> ExecSpec {
    ExecSpec::new("bash")
        .args([
            "-c",
            include_str!("support/snapshot_state.sh"),
            "snapshot-probe",
            marker,
            pid,
            process,
            mode,
        ])
        .timeout(Duration::from_secs(30))
}

async fn assert_live_process(
    sandbox: &dyn sandbox_driver::Sandbox,
    expected_pid: &str,
) -> Result<(), String> {
    let result = sandbox
        .exec()
        .run(&snapshot_probe(
            "/tmp/sd-state-marker",
            "/tmp/sd-state-pid",
            "sandbox-driver-live-process",
            "live",
        ))
        .await
        .map_err(|error| format!("check live process: {error}"))?;
    let expected = format!("{expected_pid} preserved");
    if !result.success() || result.stdout_lossy() != expected {
        return Err(format!(
            "live process state mismatch: got {:?}, expected {expected:?}, stderr {:?}",
            result.stdout_lossy(),
            result.stderr_lossy()
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn snapshot_provider_round_trip() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let snapshots = provider.snapshots().expect("snapshot provider declared");

    let name = unique("sd-live-snap");
    let spec = SnapshotSpec::new(SnapshotSource::Image {
        reference: "debian:stable-slim".to_owned(),
    })
    .name(name.clone())
    .sandbox_kind(SandboxKind::Container)
    .region("us")
    .resources(snapshot_resources(1024));
    // `ensure` creates the snapshot and waits for the build to settle.
    let id = snapshots
        .ensure(&spec, Duration::from_secs(600), None)
        .await
        .expect("snapshot ensure");

    let outcome = async {
        let status = snapshots
            .get(&id)
            .await
            .map_err(|error| format!("snapshot get: {error}"))?;
        if status.state != SnapshotState::Active
            || status.sandbox_kind != Some(SandboxKind::Container)
            || !status.regions.iter().any(|region| region == "us")
        {
            return Err(format!(
                "snapshot status did not preserve kind and region: {status:?}"
            ));
        }
        // A second ensure finds the active snapshot and returns the same
        // id without creating another.
        let again = snapshots
            .ensure(&spec, Duration::from_secs(60), None)
            .await
            .map_err(|error| format!("second ensure: {error}"))?;
        if again != id {
            return Err(format!(
                "ensure returned {again} for the existing snapshot {id}"
            ));
        }
        // It must appear in a name-filtered list.
        let mut filter = sandbox_driver::SnapshotFilter::default();
        filter.name = Some(name.clone());
        let listed = snapshots
            .list(&filter)
            .await
            .map_err(|error| format!("snapshot list: {error}"))?;
        if listed.len() != 1 {
            return Err(format!("name filter returned {} snapshots", listed.len()));
        }

        // A sandbox must boot from it.
        let spec = SandboxSpec::new(SandboxSource::Snapshot { id: id.clone() })
            .sandbox_kind(SandboxKind::Container)
            .region("us")
            .ephemeral(true);
        let sandbox = provider
            .create(&spec, None)
            .await
            .map_err(|error| format!("create from snapshot: {error}"))?;
        let result = sandbox
            .exec()
            .run(
                &sandbox_driver::ExecSpec::new("cat")
                    .arg("/etc/debian_version")
                    .timeout(Duration::from_secs(60)),
            )
            .await;
        sandbox
            .delete()
            .await
            .map_err(|error| format!("delete snapshot sandbox: {error}"))?;
        wait_for_state(sandbox.as_ref(), SandboxState::Deleted, &wait())
            .await
            .map_err(|error| format!("waiting for snapshot sandbox deletion: {error}"))?;
        let result = result.map_err(|error| format!("exec on snapshot sandbox: {error}"))?;
        if !result.success() {
            return Err(format!(
                "exec on snapshot sandbox failed: {}",
                result.stdout_lossy()
            ));
        }

        snapshots
            .deactivate(&id, None)
            .await
            .map_err(|error| format!("snapshot deactivate: {error}"))?;
        wait_for_snapshot_state(snapshots, &id, SnapshotState::Inactive).await?;
        snapshots
            .activate(&id, None)
            .await
            .map_err(|error| format!("snapshot activate: {error}"))?;
        wait_for_snapshot_state(snapshots, &id, SnapshotState::Active).await?;
        Ok(())
    }
    .await;

    snapshots.delete(&id, None).await.expect("snapshot delete");
    snapshots
        .delete(&id, None)
        .await
        .expect("snapshot delete is idempotent");
    outcome.expect("snapshot round trip");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn dockerfile_snapshot_build_and_entrypoint_logs() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let snapshots = provider.snapshots().expect("snapshot provider declared");
    let name = unique("sd-live-dockerfile");
    let dockerfile = r#"FROM debian:stable-slim
ENTRYPOINT ["/bin/sh", "-c", "echo sandbox-driver-entrypoint; echo sandbox-driver-entrypoint-error >&2; sleep 2"]
"#;
    let vm_spec = SnapshotSpec::new(SnapshotSource::Dockerfile {
        content: dockerfile.to_owned(),
    })
    .sandbox_kind(SandboxKind::VirtualMachine);
    assert!(matches!(
        snapshots.create(&vm_spec, None).await,
        Err(Error::Unsupported {
            capability: Capability::SnapshotsVmFromDockerfile,
        })
    ));

    let spec = SnapshotSpec::new(SnapshotSource::Dockerfile {
        content: dockerfile.to_owned(),
    })
    .name(name.clone())
    .sandbox_kind(SandboxKind::Container)
    .region("us")
    .resources(snapshot_resources(1024));
    let id = snapshots
        .create(&spec, None)
        .await
        .expect("Dockerfile snapshot create");

    let outcome = async {
        let build_output = Arc::new(Mutex::new(Vec::new()));
        snapshots
            .build_logs(&id, true, collecting_sink(Arc::clone(&build_output)))
            .await
            .map_err(|error| format!("snapshot build logs: {error}"))?;
        if build_output.lock().expect("build output lock").is_empty() {
            return Err("snapshot build log stream was empty".to_owned());
        }
        wait_for_snapshot_state(snapshots, &id, SnapshotState::Active).await?;

        let sandbox = provider
            .create(
                &SandboxSpec::new(SandboxSource::Snapshot { id: id.clone() })
                    .sandbox_kind(SandboxKind::Container)
                    .region("us")
                    .ephemeral(true),
                None,
            )
            .await
            .map_err(|error| format!("create from Dockerfile snapshot: {error}"))?;
        let entrypoint_output = Arc::new(Mutex::new(Vec::new()));
        let logs = sandbox.logs().expect("entrypoint logs facet declared");
        let followed = time::timeout(
            Duration::from_secs(60),
            logs.follow(
                LogSource::Entrypoint,
                collecting_sink(Arc::clone(&entrypoint_output)),
            ),
        )
        .await;
        let _ = sandbox.delete().await;
        followed
            .map_err(|_| "entrypoint log stream timed out".to_owned())?
            .map_err(|error| format!("entrypoint logs: {error}"))?;
        let output = entrypoint_output.lock().expect("entrypoint output lock");
        let output = String::from_utf8_lossy(&output);
        if !output.contains("sandbox-driver-entrypoint")
            || !output.contains("sandbox-driver-entrypoint-error")
        {
            return Err(format!("entrypoint logs missing markers: {output:?}"));
        }
        Ok(())
    }
    .await;

    snapshots.delete(&id, None).await.expect("snapshot delete");
    outcome.expect("Dockerfile snapshot and entrypoint logs");
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

async fn wait_for_snapshot_state(
    snapshots: &dyn sandbox_driver::SnapshotProvider,
    id: &sandbox_driver::SnapshotId,
    expected: SnapshotState,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        let status = snapshots
            .get(id)
            .await
            .map_err(|error| format!("snapshot get: {error}"))?;
        if status.state == expected {
            return Ok(());
        }
        if status.state == SnapshotState::Error {
            return Err(format!("snapshot failed: {:?}", status.error_reason));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "snapshot did not reach {expected:?} (state {:?})",
                status.state
            ));
        }
        time::sleep(Duration::from_secs(5)).await;
    }
}

/// Bytes every value of which appears, in an order no simple
/// reordering or dropped chunk preserves.
fn patterned_bytes(len: usize) -> Vec<u8> {
    let mut state: u32 = 0x9e37_79b9;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            state.to_be_bytes()[0]
        })
        .collect()
}

#[expect(
    clippy::print_stderr,
    reason = "the per-turn latency is the test's evidence"
)]
/// A line protocol in the shape ACP uses: each request waits for its
/// reply before the next is written, so the input must stream rather
/// than arrive at EOF.
async fn stdin_streams_turn_by_turn(sandbox: &dyn Sandbox) -> Result<(), String> {
    const TURNS: usize = 12;
    let stdout = Arc::new(Mutex::new(Vec::<u8>::new()));
    let replied = Arc::new(Notify::new());
    let sink: OutputSink = Arc::new({
        let stdout = Arc::clone(&stdout);
        let replied = Arc::clone(&replied);
        move |stream, bytes| {
            if stream == OutputStream::Stdout {
                stdout.lock().expect("stdout").extend_from_slice(&bytes);
                replied.notify_one();
            }
            Box::pin(async { Ok(()) })
        }
    });
    let (mut requests, input) = duplex(4096);
    let controls = ExecControls {
        stdin: Some(StdinSource::new(input)),
        sink: Some(sink),
        ..ExecControls::buffered()
    };
    let spec =
        ExecSpec::bash(r#"while IFS= read -r line; do printf 'ack:%s\n' "$line"; done; echo done"#)
            .timeout(Duration::from_secs(180));
    let run = sandbox.exec().run_streaming(&spec, controls);
    let turns = async {
        let started = Instant::now();
        for turn in 0..TURNS {
            let request = format!("{{\"id\":{turn},\"text\":\"héllo 世界\"}}\n");
            requests
                .write_all(request.as_bytes())
                .await
                .map_err(|error| format!("write turn {turn}: {error}"))?;
            let expected = format!("ack:{}", request.trim_end());
            time::timeout(Duration::from_secs(30), async {
                while !String::from_utf8_lossy(&stdout.lock().expect("stdout")).contains(&expected)
                {
                    replied.notified().await;
                }
            })
            .await
            .map_err(|_| format!("no reply to turn {turn} before EOF"))?;
        }
        eprintln!(
            "streamed stdin: {TURNS} turns, {:?} per turn",
            started.elapsed() / u32::try_from(TURNS).expect("turns fit")
        );
        drop(requests);
        Ok::<_, String>(())
    };
    let (result, turns) = tokio::join!(run, turns);
    turns?;
    let result = result.map_err(|error| format!("turn-by-turn exec: {error}"))?;
    if !result.result.success() || !result.result.stdout_lossy().ends_with("done\n") {
        return Err(format!(
            "EOF did not end the loop cleanly: {:?} {:?}",
            result.result.termination,
            result.result.stdout_lossy()
        ));
    }
    Ok(())
}

#[expect(
    clippy::print_stderr,
    reason = "the transfer time is the test's evidence"
)]
/// Every byte value, a mebibyte of them, arrives in order and ends at EOF.
async fn stdin_delivers_exact_bytes(sandbox: &dyn Sandbox) -> Result<(), String> {
    let bytes = patterned_bytes(1 << 20);
    let expected = "/tmp/sandbox-driver-streamed-stdin-expected";
    sandbox
        .fs()
        .write(expected, &bytes)
        .await
        .map_err(|error| format!("write expected bytes: {error}"))?;
    let controls = ExecControls {
        stdin: Some(StdinSource::new(Cursor::new(bytes))),
        ..ExecControls::buffered()
    };
    let spec = ExecSpec::new("cmp")
        .args(["-", expected])
        .timeout(Duration::from_secs(300));
    let started = Instant::now();
    let result = sandbox
        .exec()
        .run_streaming(&spec, controls)
        .await
        .map_err(|error| format!("exact-bytes exec: {error}"))?;
    eprintln!("streamed stdin: 1 MiB in {:?}", started.elapsed());
    if !result.result.success() {
        return Err(format!(
            "streamed bytes differ: {:?} {}{}",
            result.result.exit_code,
            result.result.stdout_lossy(),
            result.result.stderr_lossy()
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires DAYTONA_API_KEY; run with `mise run test:live`"]
async fn streamed_stdin_is_interactive_exact_and_ends_at_eof() {
    env::var("DAYTONA_API_KEY").expect("live gate requires DAYTONA_API_KEY");
    init_diagnostics();
    let provider = DaytonaProvider::connect().await.expect("connect");
    let spec = SandboxSpec::new(SandboxSource::Snapshot {
        id: SnapshotId::try_new(TEST_SNAPSHOT).expect("valid snapshot id"),
    })
    .name(unique("sd-streamed-stdin"))
    .ephemeral(true);
    let sandbox = provider.create(&spec, None).await.expect("create");
    if !sandbox.capabilities().exec.stdin_stream {
        sandbox.delete().await.expect("delete");
        panic!("Daytona sandboxes declare exec.stdin_stream");
    }

    let outcome = async {
        stdin_streams_turn_by_turn(sandbox.as_ref()).await?;
        stdin_delivers_exact_bytes(sandbox.as_ref()).await
    }
    .await;

    sandbox.delete().await.expect("delete");
    outcome.expect("live streamed stdin");
}
